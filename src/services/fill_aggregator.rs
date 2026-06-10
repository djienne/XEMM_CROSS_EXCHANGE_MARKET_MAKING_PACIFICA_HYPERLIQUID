use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::services::metrics;
use crate::strategy::OrderSide;

/// Exposure accumulator for a single Pacifica maker order.
///
/// The important invariant is residual based:
/// cumulative_filled - cumulative_hedged_confirmed - cumulative_hedge_pending
/// - unverified_unknown_qty is the only quantity eligible for a new hedge
/// intent. Unknown-settled quantity is quarantined out of the residual so the
/// order-based re-emission paths (idle flush / try_reserve) can never
/// double-hedge an uncertain attempt that actually filled; the position
/// reconciler, which reasons from venue-position truth, owns its recovery.
#[derive(Debug, Clone, Copy)]
pub struct OrderFillState {
    pub order_id: u64,
    pub side: OrderSide,
    pub cumulative_filled: f64,
    pub cumulative_hedged_confirmed: f64,
    pub cumulative_hedge_pending: f64,
    /// Quantity whose hedge outcome is uncertain (submit ack lost and
    /// orderStatus could not confirm either way). Excluded from `residual()`;
    /// resolved by `resolve_unknowns_on_neutral` once positions read neutral.
    pub unverified_unknown_qty: f64,
    pub avg_price: f64,
    pub terminal: bool,
    pub last_updated: Instant,
    pub created_at: Instant,
    pub next_hedge_seq: u64,
    pub last_unknown_hedge: Option<Instant>,
    pub target_size: f64,
}

impl OrderFillState {
    #[inline]
    pub fn residual(&self) -> f64 {
        (self.cumulative_filled
            - self.cumulative_hedged_confirmed
            - self.cumulative_hedge_pending
            - self.unverified_unknown_qty)
            .max(0.0)
    }
}

/// Reservation returned when a residual hedge should fire.
///
/// Creating this reservation increments `cumulative_hedge_pending`. The caller
/// must either enqueue the matching hedge intent or release the reservation.
#[derive(Debug, Clone, Copy)]
pub struct HedgeReservation {
    pub source_order_id: u64,
    pub hedge_seq: u64,
    pub side: OrderSide,
    pub size: f64,
    pub avg_price: f64,
    pub detected_at: Instant,
    pub terminal: bool,
}

pub type HedgeDecision = HedgeReservation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeSettlementStatus {
    Filled,
    Rejected,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub struct HedgeSettlement {
    pub order_id: u64,
    pub target_qty: f64,
    pub confirmed_qty: f64,
    pub status: HedgeSettlementStatus,
}

impl HedgeSettlement {
    pub fn filled(order_id: u64, target_qty: f64, confirmed_qty: f64) -> Self {
        Self {
            order_id,
            target_qty,
            confirmed_qty,
            status: HedgeSettlementStatus::Filled,
        }
    }

    pub fn rejected(order_id: u64, target_qty: f64, confirmed_qty: f64) -> Self {
        Self {
            order_id,
            target_qty,
            confirmed_qty,
            status: HedgeSettlementStatus::Rejected,
        }
    }

    pub fn unknown(order_id: u64, target_qty: f64, confirmed_qty: f64) -> Self {
        Self {
            order_id,
            target_qty,
            confirmed_qty,
            status: HedgeSettlementStatus::Unknown,
        }
    }
}

/// Aggregates fill observations from multiple detectors into residual hedge
/// decisions. Unlike the previous one-shot order dedup, this allows a later
/// residual fill on the same maker order to generate another hedge.
pub struct FillAggregator {
    inner: Mutex<HashMap<u64, OrderFillState>>,
    pub emergency_notional_usd: f64,
    pub min_notional_usd: f64,
    pub min_fraction: f64,
    pub idle_timeout: Duration,
    pub min_hedge_qty: f64,
    pub max_entries: usize,
    /// Residuals at or below this are un-hedgeable dust (HL would round them to 0)
    /// and are treated as neutral/done — never re-emitted, and GC-eligible. Should
    /// match the position reconciler's effective dust.
    pub neutral_dust: f64,
}

impl FillAggregator {
    pub fn new(emergency_notional_usd: f64) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd,
            idle_timeout: Duration::from_secs(2),
            min_notional_usd: emergency_notional_usd,
            min_fraction: 0.0,
            min_hedge_qty: 0.0,
            max_entries: 10_000,
            neutral_dust: 0.0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_thresholds(
        emergency_notional_usd: f64,
        min_notional_usd: f64,
        min_fraction: f64,
        idle_timeout: Duration,
        min_hedge_qty: f64,
        max_entries: usize,
        neutral_dust: f64,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd,
            min_notional_usd,
            min_fraction,
            idle_timeout,
            min_hedge_qty,
            max_entries: max_entries.max(1),
            neutral_dust: neutral_dust.max(0.0),
        })
    }

    /// The effective un-hedgeable dust floor (never below a tiny epsilon).
    #[inline]
    fn dust(&self) -> f64 {
        self.neutral_dust.max(1e-9)
    }

    /// Record an authoritative cumulative fill observation for `order_id`.
    ///
    /// The detector must pass the exchange's cumulative filled size, not the
    /// per-event delta. Out-of-order observations are tolerated by keeping the
    /// largest cumulative size observed so far.
    pub fn on_fill(
        &self,
        order_id: u64,
        side: OrderSide,
        absolute_filled_size: f64,
        avg_price: f64,
        is_terminal: bool,
    ) -> Option<HedgeDecision> {
        self.on_fill_with_target(
            order_id,
            side,
            absolute_filled_size,
            avg_price,
            is_terminal,
            None,
        )
    }

    pub fn on_fill_with_target(
        &self,
        order_id: u64,
        side: OrderSide,
        absolute_filled_size: f64,
        avg_price: f64,
        is_terminal: bool,
        target_size: Option<f64>,
    ) -> Option<HedgeDecision> {
        if !self.observe_fill_with_target(
            order_id,
            side,
            absolute_filled_size,
            avg_price,
            is_terminal,
            target_size,
        ) {
            return None;
        }
        self.try_reserve_hedge(order_id)
    }

    /// Record a cumulative fill observation without reserving pending hedge
    /// quantity. Returns true when the residual is eligible for reservation.
    pub fn observe_fill(
        &self,
        order_id: u64,
        side: OrderSide,
        absolute_filled_size: f64,
        avg_price: f64,
        is_terminal: bool,
    ) -> bool {
        self.observe_fill_with_target(
            order_id,
            side,
            absolute_filled_size,
            avg_price,
            is_terminal,
            None,
        )
    }

    /// Record a cumulative fill observation without reserving pending hedge
    /// quantity. Detectors should deduplicate before calling `try_reserve_hedge`.
    pub fn observe_fill_with_target(
        &self,
        order_id: u64,
        side: OrderSide,
        absolute_filled_size: f64,
        avg_price: f64,
        is_terminal: bool,
        target_size: Option<f64>,
    ) -> bool {
        let now = Instant::now();
        let dust = self.dust();
        let mut g = self.inner.lock();
        if !g.contains_key(&order_id) && g.len() >= self.max_entries {
            // Only evict a FULLY-SETTLED entry. Never drop a live accumulator:
            // doing so would lose its reserved-hedge accounting (pending qty /
            // settle_hedge / unknown quarantine), which could cause a duplicate
            // or re-emitted hedge.
            let evictable = g
                .iter()
                .filter(|(_, s)| {
                    s.terminal
                        && s.residual() <= dust
                        && s.cumulative_hedge_pending <= f64::EPSILON
                        && s.unverified_unknown_qty <= f64::EPSILON
                })
                .min_by_key(|(_, s)| s.created_at)
                .map(|(id, _)| *id);
            if let Some(oldest) = evictable {
                g.remove(&oldest);
            }
            // else: at capacity with all entries live — accept temporary growth
            // rather than dropping live hedge accounting.
        }
        let entry = g.entry(order_id).or_insert_with(|| OrderFillState {
            order_id,
            side,
            cumulative_filled: 0.0,
            cumulative_hedged_confirmed: 0.0,
            cumulative_hedge_pending: 0.0,
            unverified_unknown_qty: 0.0,
            avg_price: 0.0,
            terminal: false,
            last_updated: now,
            created_at: now,
            next_hedge_seq: 0,
            last_unknown_hedge: None,
            target_size: target_size.unwrap_or(0.0).max(0.0),
        });

        if let Some(target_size) = target_size {
            if target_size > entry.target_size {
                entry.target_size = target_size;
            }
        }
        if absolute_filled_size > entry.cumulative_filled {
            entry.cumulative_filled = absolute_filled_size;
            // Preserve a previously-recorded maker price across a cancel-residual
            // observation that carries a 0.0 placeholder (poisons profit auditing).
            if avg_price > 0.0 {
                entry.avg_price = avg_price;
            }
        }
        entry.terminal |= is_terminal;
        entry.last_updated = now;

        self.should_emit(entry)
    }

    pub fn try_reserve_hedge(&self, order_id: u64) -> Option<HedgeReservation> {
        let now = Instant::now();
        let mut g = self.inner.lock();
        let entry = g.get_mut(&order_id)?;
        if self.should_emit(entry) {
            Some(Self::emit_pending(entry, now, entry.terminal))
        } else {
            None
        }
    }

    fn should_emit(&self, entry: &OrderFillState) -> bool {
        let residual = entry.residual();
        let notional = residual * entry.avg_price;
        let fraction = if entry.target_size > 0.0 {
            residual / entry.target_size
        } else {
            0.0
        };
        // A sub-dust residual is un-hedgeable (HL rounds it to 0), so never emit
        // it — even when terminal — to avoid a perpetual failing-hedge loop.
        residual > self.dust()
            && (entry.terminal
                || (self.min_hedge_qty > 0.0 && residual >= self.min_hedge_qty)
                || (self.min_notional_usd > 0.0 && notional >= self.min_notional_usd)
                || (self.min_fraction > 0.0 && fraction >= self.min_fraction)
                || notional >= self.emergency_notional_usd)
    }

    fn emit_pending(entry: &mut OrderFillState, now: Instant, terminal: bool) -> HedgeReservation {
        let residual = entry.residual();
        let hedge_seq = entry.next_hedge_seq;
        entry.next_hedge_seq += 1;
        entry.cumulative_hedge_pending += residual;
        metrics::store_f64(
            &metrics::risk_metrics().aggregator_reserved_qty_bits,
            entry.cumulative_hedge_pending,
        );

        HedgeReservation {
            source_order_id: entry.order_id,
            hedge_seq,
            side: entry.side,
            size: residual,
            avg_price: entry.avg_price,
            detected_at: now,
            terminal,
        }
    }

    pub fn commit_queued(&self, _reservation: HedgeReservation) {}

    pub fn release_reservation(&self, reservation: HedgeReservation) {
        self.release_pending(
            reservation.source_order_id,
            reservation.hedge_seq,
            reservation.size,
        );
    }

    pub fn release_pending(&self, order_id: u64, _hedge_seq: u64, qty: f64) {
        let mut g = self.inner.lock();
        if let Some(entry) = g.get_mut(&order_id) {
            let release = qty.max(0.0).min(entry.cumulative_hedge_pending);
            entry.cumulative_hedge_pending -= release;
            metrics::store_f64(
                &metrics::risk_metrics().aggregator_unreserved_residual_qty_bits,
                entry.residual(),
            );
        }
    }

    /// Mark a hedge intent as confirmed.
    ///
    /// This compatibility method is kept for older tests and call sites. New
    /// code should call `settle_hedge`, which releases the full reserved target
    /// and confirms only the quantity that actually filled.
    pub fn mark_hedge_confirmed(&self, order_id: u64, filled_qty: f64) {
        self.settle_hedge(HedgeSettlement::filled(order_id, filled_qty, filled_qty));
    }

    pub fn settle_hedge(&self, settlement: HedgeSettlement) {
        let mut g = self.inner.lock();
        if let Some(entry) = g.get_mut(&settlement.order_id) {
            let reserved = settlement
                .target_qty
                .max(0.0)
                .min(entry.cumulative_hedge_pending);
            let filled = settlement.confirmed_qty.max(0.0).min(reserved);
            entry.cumulative_hedge_pending -= reserved;
            entry.cumulative_hedged_confirmed += filled;
            if settlement.status == HedgeSettlementStatus::Unknown {
                // Quarantine the unconfirmed remainder instead of letting it
                // fall back into the residual: the hedge may actually have
                // filled (orderStatus lag), so an order-based re-emit would
                // risk a double hedge. The position reconciler nets genuine
                // exposure and `resolve_unknowns_on_neutral` retires the
                // quarantine once venue positions read neutral.
                entry.unverified_unknown_qty += reserved - filled;
                entry.last_unknown_hedge = Some(Instant::now());
            }
        }
    }

    /// Retire all unknown-settled quarantine once venue positions confirm
    /// neutrality: whether the uncertain hedge filled or the reconciler netted
    /// the exposure, the order-based ledger must stop tracking it. Called by
    /// the position reconciler after two consecutive neutral reads.
    pub fn resolve_unknowns_on_neutral(&self) {
        let mut g = self.inner.lock();
        for entry in g.values_mut() {
            if entry.unverified_unknown_qty > 0.0 {
                entry.cumulative_hedged_confirmed += entry.unverified_unknown_qty;
                entry.unverified_unknown_qty = 0.0;
                entry.last_unknown_hedge = None;
            }
        }
    }

    /// Scan accumulators and emit residuals that have gone idle.
    pub fn flush_idle(&self) -> Vec<(u64, HedgeReservation)> {
        let now = Instant::now();
        let mut out = Vec::new();
        let mut g = self.inner.lock();
        let dust = self.dust();
        for (order_id, acc) in g.iter_mut() {
            if acc.residual() > dust && now.duration_since(acc.last_updated) >= self.idle_timeout {
                let decision = Self::emit_pending(acc, now, acc.terminal);
                out.push((*order_id, decision));
            }
        }
        out
    }

    /// Remove terminal accumulators whose exposure has been fully hedged.
    /// Entries with quarantined unknown quantity are never collected — they
    /// must survive until `resolve_unknowns_on_neutral` retires them.
    pub fn gc(&self, ttl: Duration) {
        let now = Instant::now();
        let mut g = self.inner.lock();
        let dust = self.dust();
        g.retain(|_, acc| {
            !(acc.terminal
                && acc.residual() <= dust
                && acc.cumulative_hedge_pending <= f64::EPSILON
                && acc.unverified_unknown_qty <= f64::EPSILON
                && now.duration_since(acc.created_at) >= ttl)
        });
    }

    /// Total base quantity currently reserved for in-flight hedges across all
    /// orders. The position reconciler subtracts this from observed net exposure
    /// so it does not enqueue a duplicate hedge while a primary hedge is still
    /// executing.
    pub fn total_pending_qty(&self) -> f64 {
        self.inner
            .lock()
            .values()
            .map(|state| state.cumulative_hedge_pending.max(0.0))
            .sum()
    }

    /// Pending hedge qty reserved for maker fills of `maker_side` — i.e. hedges
    /// that reduce net exposure in that side's direction. A maker Buy fill (long,
    /// net > 0) reserves a Sell hedge, so a net-long exposure is covered by
    /// `pending_qty_for_side(Buy)`. Side-filtering avoids subtracting an
    /// opposite-side reservation from the current net exposure.
    pub fn pending_qty_for_side(&self, maker_side: OrderSide) -> f64 {
        self.inner
            .lock()
            .values()
            .filter(|state| state.side == maker_side)
            .map(|state| state.cumulative_hedge_pending.max(0.0))
            .sum()
    }

    /// Current accumulator count (diagnostic / test use).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    pub fn snapshot(&self, order_id: u64) -> Option<OrderFillState> {
        self.inner.lock().get(&order_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_terminal_partial_does_not_emit_by_default() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 0.5, 100.0, false);
        assert!(d.is_none());
    }

    #[test]
    fn terminal_emits_with_accumulated_total() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 0.5, 100.0, false).is_none());
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 101.0, true).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
        assert!((d.avg_price - 101.0).abs() < 1e-9);
        assert_eq!(d.side, OrderSide::Buy);
        assert!(d.terminal);
    }

    #[test]
    fn later_residual_after_idle_flush_emits_again() {
        let agg = Arc::new(FillAggregator {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd: 1_000_000.0,
            min_notional_usd: 1_000_000.0,
            min_fraction: 1.0,
            idle_timeout: Duration::from_millis(10),
            min_hedge_qty: 0.0,
            max_entries: 10_000,
            neutral_dust: 0.0,
        });
        assert!(agg.on_fill(1, OrderSide::Buy, 0.4, 100.0, false).is_none());
        std::thread::sleep(Duration::from_millis(20));
        let first = agg.flush_idle();
        assert_eq!(first.len(), 1);
        assert!((first[0].1.size - 0.4).abs() < 1e-9);
        assert!(!first[0].1.terminal);

        let second = agg.on_fill(1, OrderSide::Buy, 1.0, 101.0, true).unwrap();
        assert!((second.size - 0.6).abs() < 1e-9);
        assert_eq!(second.hedge_seq, 1);
        assert!(second.terminal);
    }

    #[test]
    fn duplicate_cumulative_observation_does_not_emit_duplicate() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_some());
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_none());
    }

    #[test]
    fn emergency_notional_emits_without_terminal() {
        let agg = FillAggregator::new(50.0);
        let d = agg.on_fill(1, OrderSide::Sell, 1.0, 100.0, false).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
        assert!(!d.terminal);
    }

    #[test]
    fn out_of_order_cumulative_keeps_larger() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 0.8, 100.0, false).is_none());
        assert!(agg.on_fill(1, OrderSide::Buy, 0.5, 101.0, false).is_none());
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 102.0, true).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
        assert!((d.avg_price - 102.0).abs() < 1e-9);
    }

    #[test]
    fn mark_confirmed_moves_pending_to_confirmed() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.mark_hedge_confirmed(1, d.size);
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_none());
    }

    #[test]
    fn configured_partial_threshold_uses_original_target_size() {
        let agg = FillAggregator::with_thresholds(
            1_000_000.0,
            1_000_000.0,
            0.35,
            Duration::from_secs(60),
            0.0,
            100,
            0.0,
        );
        assert!(agg
            .on_fill_with_target(1, OrderSide::Buy, 0.1, 100.0, false, Some(1.0))
            .is_none());
        let d = agg
            .on_fill_with_target(1, OrderSide::Buy, 0.4, 100.0, false, Some(1.0))
            .unwrap();
        assert!((d.size - 0.4).abs() < 1e-9);
    }

    #[test]
    fn settlement_releases_full_reserved_target_but_confirms_only_filled() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.settle_hedge(HedgeSettlement::rejected(1, d.size, 0.4));
        let residual = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        assert!((residual.size - 0.6).abs() < 1e-9);
    }

    #[test]
    fn observe_does_not_reserve_until_try_reserve() {
        let agg = FillAggregator::new(50.0);
        assert!(agg.observe_fill(1, OrderSide::Buy, 1.0, 100.0, false));
        let state = agg.snapshot(1).unwrap();
        assert!((state.cumulative_hedge_pending - 0.0).abs() < 1e-9);

        let reservation = agg.try_reserve_hedge(1).unwrap();
        assert!((reservation.size - 1.0).abs() < 1e-9);
        let state = agg.snapshot(1).unwrap();
        assert!((state.cumulative_hedge_pending - 1.0).abs() < 1e-9);
    }

    #[test]
    fn release_reservation_reexposes_residual() {
        let agg = FillAggregator::new(50.0);
        assert!(agg.observe_fill(1, OrderSide::Buy, 1.0, 100.0, false));
        let reservation = agg.try_reserve_hedge(1).unwrap();
        agg.release_reservation(reservation);

        let state = agg.snapshot(1).unwrap();
        assert!((state.residual() - 1.0).abs() < 1e-9);
        let reservation = agg.try_reserve_hedge(1).unwrap();
        assert!((reservation.size - 1.0).abs() < 1e-9);
    }

    #[test]
    fn pending_qty_for_side_filters_by_maker_side() {
        let agg = FillAggregator::new(1000.0);
        // Reserve a Buy-side hedge on order 1 and a Sell-side on order 2.
        let _ = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        let _ = agg.on_fill(2, OrderSide::Sell, 0.5, 100.0, true).unwrap();

        assert!((agg.pending_qty_for_side(OrderSide::Buy) - 1.0).abs() < 1e-9);
        assert!((agg.pending_qty_for_side(OrderSide::Sell) - 0.5).abs() < 1e-9);
        // Sanity: the side-agnostic total is the sum of both.
        assert!((agg.total_pending_qty() - 1.5).abs() < 1e-9);

        // Settling the Buy hedge leaves only the Sell reservation for its side.
        agg.settle_hedge(HedgeSettlement::filled(1, 1.0, 1.0));
        assert!(agg.pending_qty_for_side(OrderSide::Buy).abs() < 1e-9);
        assert!((agg.pending_qty_for_side(OrderSide::Sell) - 0.5).abs() < 1e-9);
    }

    #[test]
    fn total_pending_qty_sums_reserved_across_orders() {
        let agg = FillAggregator::new(1000.0);
        // Reserve on two different orders.
        let a = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        let b = agg.on_fill(2, OrderSide::Sell, 0.5, 100.0, true).unwrap();
        assert!((a.size - 1.0).abs() < 1e-9);
        assert!((b.size - 0.5).abs() < 1e-9);
        assert!((agg.total_pending_qty() - 1.5).abs() < 1e-9);

        // Settling one releases its reservation.
        agg.settle_hedge(HedgeSettlement::filled(1, 1.0, 1.0));
        assert!((agg.total_pending_qty() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn gc_removes_old_terminal_hedged_entries() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.mark_hedge_confirmed(1, d.size);
        assert_eq!(agg.len(), 1);
        std::thread::sleep(Duration::from_millis(5));
        agg.gc(Duration::from_millis(1));
        assert_eq!(agg.len(), 0);
    }

    #[test]
    fn unknown_settlement_quarantines_qty_from_reemission() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);

        // Uncertain outcome: nothing confirmed, qty must NOT reappear as residual.
        agg.settle_hedge(HedgeSettlement::unknown(1, d.size, 0.0));
        let state = agg.snapshot(1).unwrap();
        assert!((state.unverified_unknown_qty - 1.0).abs() < 1e-9);
        assert!(state.residual().abs() < 1e-9);
        assert!(agg.try_reserve_hedge(1).is_none());
        assert!(agg.flush_idle().is_empty());
        // A duplicate cumulative observation must not re-emit either.
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_none());
    }

    #[test]
    fn unknown_settlement_with_partial_confirm_quarantines_only_remainder() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.settle_hedge(HedgeSettlement::unknown(1, d.size, 0.4));
        let state = agg.snapshot(1).unwrap();
        assert!((state.cumulative_hedged_confirmed - 0.4).abs() < 1e-9);
        assert!((state.unverified_unknown_qty - 0.6).abs() < 1e-9);
        assert!(state.residual().abs() < 1e-9);
    }

    #[test]
    fn resolve_unknowns_on_neutral_retires_quarantine_and_allows_gc() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.settle_hedge(HedgeSettlement::unknown(1, d.size, 0.0));

        // Quarantined entries must survive GC.
        std::thread::sleep(Duration::from_millis(5));
        agg.gc(Duration::from_millis(1));
        assert_eq!(agg.len(), 1);

        agg.resolve_unknowns_on_neutral();
        let state = agg.snapshot(1).unwrap();
        assert!(state.unverified_unknown_qty.abs() < 1e-9);
        assert!((state.cumulative_hedged_confirmed - 1.0).abs() < 1e-9);
        assert!(state.last_unknown_hedge.is_none());

        agg.gc(Duration::from_millis(1));
        assert_eq!(agg.len(), 0);
    }

    #[test]
    fn new_fill_after_unknown_settlement_exposes_only_new_qty() {
        // Emergency notional 50 lets the 0.6 @ $100 partial emit non-terminally.
        let agg = FillAggregator::new(50.0);
        let d = agg.on_fill(1, OrderSide::Buy, 0.6, 100.0, false).unwrap();
        assert!((d.size - 0.6).abs() < 1e-9);
        agg.settle_hedge(HedgeSettlement::unknown(1, d.size, 0.0));

        // Later cumulative growth re-exposes ONLY the new quantity.
        let second = agg.on_fill(1, OrderSide::Buy, 1.0, 101.0, true).unwrap();
        assert!((second.size - 0.4).abs() < 1e-9);
    }
}
