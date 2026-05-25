use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::strategy::OrderSide;

/// Exposure accumulator for a single Pacifica maker order.
///
/// The important invariant is residual based:
/// cumulative_filled - cumulative_hedged_confirmed - cumulative_hedge_pending
/// is the only quantity eligible for a new hedge intent.
#[derive(Debug, Clone, Copy)]
pub struct OrderFillState {
    pub order_id: u64,
    pub side: OrderSide,
    pub cumulative_filled: f64,
    pub cumulative_hedged_confirmed: f64,
    pub cumulative_hedge_pending: f64,
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
        (self.cumulative_filled - self.cumulative_hedged_confirmed - self.cumulative_hedge_pending)
            .max(0.0)
    }
}

/// Decision returned by `on_fill` / `flush_idle` when a residual hedge should
/// fire.
#[derive(Debug, Clone, Copy)]
pub struct HedgeDecision {
    pub source_order_id: u64,
    pub hedge_seq: u64,
    pub side: OrderSide,
    pub size: f64,
    pub avg_price: f64,
    pub detected_at: Instant,
    pub terminal: bool,
}

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
        })
    }

    pub fn with_thresholds(
        emergency_notional_usd: f64,
        min_notional_usd: f64,
        min_fraction: f64,
        idle_timeout: Duration,
        min_hedge_qty: f64,
        max_entries: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd,
            min_notional_usd,
            min_fraction,
            idle_timeout,
            min_hedge_qty,
            max_entries: max_entries.max(1),
        })
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
        let now = Instant::now();
        let mut g = self.inner.lock();
        if !g.contains_key(&order_id) && g.len() >= self.max_entries {
            if let Some(oldest) = g
                .iter()
                .min_by_key(|(_, state)| state.created_at)
                .map(|(id, _)| *id)
            {
                g.remove(&oldest);
            }
        }
        let entry = g.entry(order_id).or_insert_with(|| OrderFillState {
            order_id,
            side,
            cumulative_filled: 0.0,
            cumulative_hedged_confirmed: 0.0,
            cumulative_hedge_pending: 0.0,
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
            entry.avg_price = avg_price;
        }
        entry.terminal |= is_terminal;
        entry.last_updated = now;

        let residual = entry.residual();
        let notional = residual * entry.avg_price;
        let fraction = if entry.target_size > 0.0 {
            residual / entry.target_size
        } else {
            0.0
        };
        let should_emit = residual > 0.0
            && (entry.terminal
                || (self.min_hedge_qty > 0.0 && residual >= self.min_hedge_qty)
                || (self.min_notional_usd > 0.0 && notional >= self.min_notional_usd)
                || (self.min_fraction > 0.0 && fraction >= self.min_fraction)
                || notional >= self.emergency_notional_usd);

        if should_emit {
            Some(Self::emit_pending(entry, now, entry.terminal))
        } else {
            None
        }
    }

    fn emit_pending(entry: &mut OrderFillState, now: Instant, terminal: bool) -> HedgeDecision {
        let residual = entry.residual();
        let hedge_seq = entry.next_hedge_seq;
        entry.next_hedge_seq += 1;
        entry.cumulative_hedge_pending += residual;

        HedgeDecision {
            source_order_id: entry.order_id,
            hedge_seq,
            side: entry.side,
            size: residual,
            avg_price: entry.avg_price,
            detected_at: now,
            terminal,
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
                entry.last_unknown_hedge = Some(Instant::now());
            }
        }
    }

    /// Scan accumulators and emit residuals that have gone idle.
    pub fn flush_idle(&self) -> Vec<(u64, HedgeDecision)> {
        let now = Instant::now();
        let mut out = Vec::new();
        let mut g = self.inner.lock();
        for (order_id, acc) in g.iter_mut() {
            if acc.residual() > 0.0 && now.duration_since(acc.last_updated) >= self.idle_timeout {
                let decision = Self::emit_pending(acc, now, acc.terminal);
                out.push((*order_id, decision));
            }
        }
        out
    }

    /// Remove terminal accumulators whose exposure has been fully hedged.
    pub fn gc(&self, ttl: Duration) {
        let now = Instant::now();
        let mut g = self.inner.lock();
        g.retain(|_, acc| {
            !(acc.terminal
                && acc.residual() <= f64::EPSILON
                && acc.cumulative_hedge_pending <= f64::EPSILON
                && now.duration_since(acc.created_at) >= ttl)
        });
    }

    /// Current accumulator count (diagnostic / test use).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
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
    fn gc_removes_old_terminal_hedged_entries() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.mark_hedge_confirmed(1, d.size);
        assert_eq!(agg.len(), 1);
        std::thread::sleep(Duration::from_millis(5));
        agg.gc(Duration::from_millis(1));
        assert_eq!(agg.len(), 0);
    }
}
