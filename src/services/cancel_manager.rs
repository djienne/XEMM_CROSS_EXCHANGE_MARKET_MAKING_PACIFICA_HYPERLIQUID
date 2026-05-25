use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus, RunState};
use crate::config::Config;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
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
}

#[derive(Debug, Clone)]
pub struct CancelIntent {
    pub symbol: String,
    pub reason: CancelReason,
}

impl CancelIntent {
    pub fn new(symbol: impl Into<String>, reason: CancelReason) -> Self {
        Self {
            symbol: symbol.into(),
            reason,
        }
    }
}

pub struct CancelManagerService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub atomic_status: Arc<AtomicU8>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    pub rest: Arc<PacificaTrading>,
    pub ws: Arc<PacificaWsTrading>,
    pub trade_gate: Arc<TradeGate>,
    pub config: Config,
}

impl CancelManagerService {
    pub fn channel() -> (mpsc::Sender<CancelIntent>, mpsc::Receiver<CancelIntent>) {
        mpsc::channel(256)
    }

    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<CancelIntent>) {
        while let Some(intent) = rx.recv().await {
            self.process_intent(intent).await;
        }
    }

    async fn process_intent(&self, intent: CancelIntent) {
        self.trade_gate.block(GateReason::CancelPending);
        self.trade_gate.mark_cancel_now();

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
