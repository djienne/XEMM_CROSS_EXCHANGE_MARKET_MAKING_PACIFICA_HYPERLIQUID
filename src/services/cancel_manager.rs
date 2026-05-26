use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus, RunState};
use crate::config::Config;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::metrics;
use crate::services::order_monitor::SharedOrderSnapshot;
use crate::services::trade_gate::{GateReason, TradeGate};
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
                    self.clear_after_verified_cancel();
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
