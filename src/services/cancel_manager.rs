use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use fast_float::parse;

use crate::bot::{BotState, BotStatus, RunState};
use crate::config::Config;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::fill_aggregator::{FillAggregator, HedgeReservation};
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::metrics;
use crate::services::order_monitor::SharedOrderSnapshot;
use crate::services::trade_gate::{GateReason, TradeGate};
use crate::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use crate::util::cancel::dual_cancel;
use crate::util::rate_limit::is_rate_limit_error;

#[derive(Debug, Clone)]
pub enum CancelReason {
    AgeExpiry {
        age_ms: u64,
    },
    ProfitDeviation {
        current_profit_bps: f64,
        deviation_bps: f64,
    },
    PartialFill,
    Shutdown,
    Safety,
    Coalesced {
        reason_bits: u64,
    },
}

impl CancelReason {
    pub const fn bit(&self) -> u64 {
        match self {
            Self::AgeExpiry { .. } => 1 << 0,
            Self::ProfitDeviation { .. } => 1 << 1,
            Self::PartialFill => 1 << 2,
            Self::Shutdown => 1 << 3,
            Self::Safety => 1 << 4,
            Self::Coalesced { reason_bits } => *reason_bits,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CancelIntent {
    pub symbol: Arc<str>,
    pub reason: CancelReason,
}

impl CancelIntent {
    pub fn new(symbol: impl Into<Arc<str>>, reason: CancelReason) -> Self {
        Self {
            symbol: symbol.into(),
            reason,
        }
    }
}

#[derive(Debug, Default)]
pub struct CancelDemand {
    generation: AtomicU64,
    reason_bits: AtomicU64,
}

impl CancelDemand {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn request(&self, reason: &CancelReason) {
        self.reason_bits.fetch_or(reason.bit(), Ordering::AcqRel);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn clear(&self) {
        self.reason_bits.store(0, Ordering::Release);
    }

    fn pending_intent(&self, symbol: String) -> Option<CancelIntent> {
        let bits = self.reason_bits.swap(0, Ordering::AcqRel);
        if bits == 0 {
            return None;
        }
        Some(CancelIntent::new(
            Arc::<str>::from(symbol),
            CancelReason::Coalesced { reason_bits: bits },
        ))
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
}

pub fn request_cancel(
    cancel_tx: &mpsc::Sender<CancelIntent>,
    cancel_demand: &Arc<CancelDemand>,
    symbol: impl Into<Arc<str>>,
    reason: CancelReason,
) -> bool {
    cancel_demand.request(&reason);
    metrics::risk_metrics()
        .cancel_demand_generation
        .store(cancel_demand.generation(), Ordering::Release);
    cancel_tx
        .try_send(CancelIntent::new(symbol, reason))
        .is_ok()
}

pub struct CancelManagerService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub atomic_status: Arc<AtomicU8>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    pub rest: Arc<PacificaTrading>,
    pub ws: Arc<PacificaWsTrading>,
    pub trade_gate: Arc<TradeGate>,
    pub cancel_demand: Arc<CancelDemand>,
    pub config: Config,
    /// Shared fill pipeline, so a fill that races a cancel can be routed to the
    /// hedge path instead of being silently dropped as a no-op cancel.
    pub fill_aggregator: Arc<FillAggregator>,
    pub processed_fills: Arc<FillDedup>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
}

impl CancelManagerService {
    pub fn channel() -> (mpsc::Sender<CancelIntent>, mpsc::Receiver<CancelIntent>) {
        mpsc::channel(256)
    }

    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<CancelIntent>) {
        let mut demand_tick = tokio::time::interval(Duration::from_millis(50));
        demand_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                Some(intent) = rx.recv() => {
                    self.process_intent(intent).await;
                }
                _ = demand_tick.tick() => {
                    if let Some(intent) = self.cancel_demand.pending_intent(self.config.symbol.clone()) {
                        self.process_intent(intent).await;
                    }
                }
                else => break,
            }
        }
    }

    async fn process_intent(&self, intent: CancelIntent) {
        self.trade_gate.block(GateReason::CancelPending);
        self.trade_gate.mark_cancel_now();
        let verify_start = std::time::Instant::now();

        let status = RunState::load(&self.atomic_status);
        if matches!(status, RunState::OrderPlaced | RunState::CancelPending) {
            RunState::store(&self.atomic_status, RunState::Cancelling);
            let mut state = self.bot_state.write();
            if matches!(
                state.status,
                BotStatus::OrderPlaced | BotStatus::CancelPending
            ) {
                state.mark_cancelling();
            }
        }

        if !self.config.low_latency_mode {
            match &intent.reason {
                CancelReason::AgeExpiry { age_ms } => info!(
                    "[CANCEL] Age expiry: age {}ms > {}s threshold",
                    age_ms, self.config.order_refresh_interval_secs
                ),
                CancelReason::ProfitDeviation {
                    current_profit_bps,
                    deviation_bps,
                } => info!(
                    "[CANCEL] Profit deviation: current={:.2} bps, deviation={:.2} bps",
                    current_profit_bps, deviation_bps
                ),
                CancelReason::PartialFill => {
                    info!("[CANCEL] Partial fill policy: cancelling remainder")
                }
                CancelReason::Shutdown => info!("[CANCEL] Shutdown cancellation requested"),
                CancelReason::Safety => info!("[CANCEL] Safety cancellation requested"),
                CancelReason::Coalesced { reason_bits } => info!(
                    "[CANCEL] Coalesced cancellation requested (reason_bits={})",
                    reason_bits
                ),
            }
        }

        let mut attempts = 0u32;
        loop {
            attempts += 1;
            match dual_cancel(&self.rest, &self.ws, &intent.symbol).await {
                Ok((rest_count, ws_count)) => {
                    debug!(
                        "[CANCEL] Dual cancel submitted (REST: {}, WS: {}, attempt {})",
                        rest_count, ws_count, attempts
                    );
                }
                Err(e) => {
                    warn!("[CANCEL] Dual cancel submission failed: {}", e);
                }
            }

            match self.verify_no_open_orders(&intent.symbol).await {
                Ok(true) => {
                    // The order is gone from the book. Before concluding it was
                    // cancelled, verify it did not actually FILL (cancel-vs-fill
                    // race): a filled order disappearing would otherwise be cleared
                    // as a no-op cancel, dropping the fast hedge path and leaving
                    // naked exposure. If a fill is found we route it to the hedge
                    // pipeline and let that path own the state (do not clear here).
                    let routed = self.route_fill_if_any(&intent.symbol).await;
                    if !routed {
                        self.clear_after_verified_cancel();
                    }
                    self.trade_gate.allow(GateReason::OpenOrderExists);
                    self.trade_gate.allow(GateReason::OpenOrderUnknown);
                    self.trade_gate.allow(GateReason::CancelPending);
                    metrics::risk_metrics().cancel_verify_latency_ms.store(
                        verify_start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        Ordering::Release,
                    );
                    self.cancel_demand.clear();
                    return;
                }
                Ok(false) => {
                    warn!(
                        "[CANCEL] Open orders still present after cancel attempt {}; retrying",
                        attempts
                    );
                }
                Err(e) => {
                    self.trade_gate.block(GateReason::OpenOrderUnknown);
                    if is_rate_limit_error(&e) {
                        warn!(
                            "[CANCEL] Verification hit rate limit; retaining cancel intent and retrying"
                        );
                    } else {
                        warn!("[CANCEL] Verification failed: {}", e);
                    }
                }
            }

            let delay = Duration::from_millis((100 * attempts as u64).min(1_000));
            tokio::time::sleep(delay).await;
            if attempts >= 5 {
                warn!(
                    "[CANCEL] Fast cancel verification exhausted; retaining demand for slow retry"
                );
                self.cancel_demand.request(&intent.reason);
                return;
            }
        }
    }

    async fn verify_no_open_orders(&self, symbol: &str) -> anyhow::Result<bool> {
        let orders = self.rest.get_open_orders().await?;
        Ok(!orders.iter().any(|order| order.symbol == symbol))
    }

    /// Cancel-vs-fill guard: when the active order has vanished from the book,
    /// check trade history for the order's CLOID. If it actually filled, route the
    /// fill to the hedge pipeline and return `true` so the caller does NOT clear
    /// the active order (the fill/hedge path owns the state from here).
    ///
    /// Idempotent against the WS/REST/position fill layers via the shared
    /// `FillAggregator` (cumulative-keyed) and `processed_fills` dedup, so this can
    /// never double-hedge a fill another layer already claimed.
    async fn route_fill_if_any(&self, symbol: &str) -> bool {
        let Some((cloid, order_id_opt, side, size)) = ({
            let state = self.bot_state.read();
            state
                .active_order
                .as_ref()
                .map(|o| (o.client_order_id.clone(), o.order_id, o.side, o.size))
        }) else {
            return false;
        };

        let trades = match self
            .rest
            .get_trade_history(Some(symbol), Some(100), None, None)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                // Could not confirm; treat as not-routed so the slow REST fill
                // detector / reconciler remain the backstop. Do not clear yet:
                // leave the verify loop's caller to decide (it will clear only on a
                // genuine no-fill cancel). Returning false here means we may clear,
                // but the REST fill detector + reconciler still catch a missed fill.
                warn!(
                    "[CANCEL] Trade-history fill check failed for {}: {}",
                    cloid, e
                );
                return false;
            }
        };

        let mut cumulative = 0.0;
        let mut notional = 0.0;
        let mut order_id = order_id_opt.unwrap_or(0);
        for trade in trades
            .iter()
            .filter(|t| t.client_order_id.as_deref() == Some(cloid.as_str()))
        {
            let qty: f64 = parse(&trade.amount).unwrap_or(0.0);
            let px: f64 = parse(&trade.entry_price).unwrap_or(0.0);
            if qty <= 0.0 {
                continue;
            }
            cumulative += qty;
            notional += qty * px.max(0.0);
            order_id = trade.order_id;
        }

        if cumulative <= 0.0 {
            return false; // genuine cancel, no fill
        }
        let avg_price = if notional > 0.0 {
            notional / cumulative
        } else {
            0.0
        };

        // If another layer already accounted for this cumulative, defer to it: do
        // not clear (it owns the fill/hedge state), but do not re-hedge either.
        if !self.fill_aggregator.observe_fill_with_target(
            order_id,
            side,
            cumulative,
            avg_price,
            true,
            Some(size),
        ) {
            return true;
        }
        if !self
            .processed_fills
            .insert_if_new(FillKey::from_cloid_cumulative(cloid.clone(), cumulative))
        {
            return true;
        }
        let Some(reservation) = self.fill_aggregator.try_reserve_hedge(order_id) else {
            return true;
        };

        {
            let mut state = self.bot_state.write();
            state.mark_filled(reservation.size, reservation.side);
        }
        warn!(
            "[CANCEL] Cancel-vs-fill race: order {} actually filled {} @ ${:.4}; routing to hedge",
            cloid, reservation.size, reservation.avg_price
        );
        enqueue_reserved_hedge(
            &self.fill_aggregator,
            &self.hedge_tx,
            &self.bot_state,
            reservation,
            "cancel-vs-fill",
        )
        .await;
        true
    }

    fn clear_after_verified_cancel(&self) {
        self.trade_gate.mark_cancel_now();
        self.order_snapshot.set(None);
        let mut state = self.bot_state.write();
        if matches!(
            state.status,
            BotStatus::OrderPlaced | BotStatus::Cancelling | BotStatus::CancelPending
        ) {
            state.clear_active_order();
        }
    }
}

async fn enqueue_reserved_hedge(
    aggregator: &FillAggregator,
    hedge_tx: &mpsc::Sender<HedgeIntent>,
    bot_state: &Arc<RwLock<BotState>>,
    reservation: HedgeReservation,
    context: &str,
) -> bool {
    let intent: HedgeIntent = reservation.into();
    match enqueue_hedge_intent(hedge_tx, bot_state, intent).await {
        Ok(HedgeEnqueueResult::Queued) => {
            aggregator.commit_queued(reservation);
            true
        }
        Ok(HedgeEnqueueResult::PersistedButNotQueued { reason }) => {
            aggregator.release_reservation(reservation);
            warn!(
                "[CANCEL] Hedge not queued for {}; released reservation: {}",
                context, reason
            );
            false
        }
        Err(e) => {
            aggregator.release_reservation(reservation);
            warn!(
                "[CANCEL] Hedge enqueue failed for {}; released reservation: {}",
                context, e
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_full_still_records_atomic_cancel_demand() {
        let demand = CancelDemand::new();
        let (tx, _rx) = mpsc::channel(1);
        assert!(tx
            .try_send(CancelIntent::new("SOL", CancelReason::Safety))
            .is_ok());

        assert!(!request_cancel(
            &tx,
            &demand,
            "SOL",
            CancelReason::PartialFill
        ));
        assert!(demand.generation() > 0);
        let pending = demand.pending_intent("SOL".to_string()).unwrap();
        assert!(matches!(
            pending.reason,
            CancelReason::Coalesced { reason_bits }
                if reason_bits & CancelReason::PartialFill.bit() != 0
        ));
    }
}
