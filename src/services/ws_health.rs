use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{watch, RwLock};
use tracing::{info, warn};

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::market_event::MarketSource;
use crate::services::order_monitor::{AtomicBotStatus, SharedOrderSnapshot, sync_atomic_status};
use crate::util::cancel::dual_cancel;

const WS_HEALTHY: u8 = 1;
const WS_UNHEALTHY: u8 = 0;

#[derive(Debug)]
pub struct WsHealth {
    pacifica_last_ms: AtomicU64,
    hyperliquid_last_ms: AtomicU64,
    status: AtomicU8,
}

impl WsHealth {
    pub fn new() -> Self {
        Self {
            pacifica_last_ms: AtomicU64::new(0),
            hyperliquid_last_ms: AtomicU64::new(0),
            status: AtomicU8::new(WS_UNHEALTHY),
        }
    }

    #[inline]
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    #[inline]
    pub fn record_update(&self, source: MarketSource, now_ms: u64) {
        match source {
            MarketSource::Pacifica => {
                self.pacifica_last_ms.store(now_ms, Ordering::Release);
            }
            MarketSource::Hyperliquid => {
                self.hyperliquid_last_ms.store(now_ms, Ordering::Release);
            }
        }
    }

    #[inline]
    pub fn last_update_ms(&self, source: MarketSource) -> u64 {
        match source {
            MarketSource::Pacifica => self.pacifica_last_ms.load(Ordering::Acquire),
            MarketSource::Hyperliquid => self.hyperliquid_last_ms.load(Ordering::Acquire),
        }
    }

    #[inline]
    pub fn is_healthy(&self) -> bool {
        self.status.load(Ordering::Acquire) == WS_HEALTHY
    }

    #[inline]
    pub fn mark_healthy(&self) {
        self.status.store(WS_HEALTHY, Ordering::Release);
    }

    #[inline]
    pub fn mark_unhealthy(&self) {
        self.status.store(WS_UNHEALTHY, Ordering::Release);
    }
}

impl Default for WsHealth {
    fn default() -> Self {
        Self::new()
    }
}

pub struct WsHealthMonitor {
    pub symbol: String,
    pub health: Arc<WsHealth>,
    pub bot_state: Arc<RwLock<BotState>>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub atomic_status: Arc<AtomicU8>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    pub pacifica_reconnect_tx: watch::Sender<u64>,
    pub hyperliquid_reconnect_tx: watch::Sender<u64>,
    pub stale_threshold_ms: u64,
}

impl WsHealthMonitor {
    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(200));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut was_healthy = self.health.is_healthy();

        loop {
            interval.tick().await;

            let now_ms = WsHealth::now_ms();
            let pac_ms = self.health.last_update_ms(MarketSource::Pacifica);
            let hl_ms = self.health.last_update_ms(MarketSource::Hyperliquid);

            let pac_stale = pac_ms == 0 || now_ms.saturating_sub(pac_ms) > self.stale_threshold_ms;
            let hl_stale = hl_ms == 0 || now_ms.saturating_sub(hl_ms) > self.stale_threshold_ms;
            let healthy = !(pac_stale || hl_stale);

            if healthy {
                if !was_healthy {
                    self.health.mark_healthy();
                    info!(
                        "[WS_HEALTH] Feeds healthy again (pacifica_age={}ms, hyperliquid_age={}ms)",
                        now_ms.saturating_sub(pac_ms),
                        now_ms.saturating_sub(hl_ms)
                    );
                    self.reconcile_after_recovery().await;
                }
            } else if was_healthy {
                self.health.mark_unhealthy();
                warn!(
                    "[WS_HEALTH] Feed unhealthy (pacifica_age={}ms, hyperliquid_age={}ms) - canceling orders and reconnecting",
                    now_ms.saturating_sub(pac_ms),
                    now_ms.saturating_sub(hl_ms)
                );

                let _ = self.pacifica_reconnect_tx.send(now_ms);
                let _ = self.hyperliquid_reconnect_tx.send(now_ms);

                self.cancel_all_orders().await;
            }

            was_healthy = healthy;
        }
    }

    async fn cancel_all_orders(&self) {
        match dual_cancel(&self.pacifica_trading, &self.pacifica_ws_trading, &self.symbol).await {
            Ok((rest_count, ws_count)) => {
                info!(
                    "[WS_HEALTH] Cancelled orders (REST: {}, WS: {})",
                    rest_count, ws_count
                );
            }
            Err(e) => {
                warn!("[WS_HEALTH] Failed to cancel orders during WS outage: {}", e);
            }
        }
    }

    async fn reconcile_after_recovery(&self) {
        let status = self.atomic_status.load(Ordering::Acquire);
        if status != AtomicBotStatus::OrderPlaced as u8 {
            return;
        }

        let active_order = {
            let state = self.bot_state.read().await;
            if !matches!(state.status, BotStatus::OrderPlaced) {
                return;
            }
            state.active_order.clone()
        };

        let Some(order) = active_order else {
            return;
        };

        let client_order_id = order.client_order_id.clone();

        match self.pacifica_trading.get_open_orders().await {
            Ok(open_orders) => {
                let still_open = open_orders
                    .iter()
                    .any(|o| o.client_order_id == client_order_id);
                if !still_open {
                    let mut state = self.bot_state.write().await;
                    let matches_active = state
                        .active_order
                        .as_ref()
                        .map(|o| o.client_order_id == client_order_id)
                        .unwrap_or(false);
                    if matches!(state.status, BotStatus::OrderPlaced) && matches_active {
                        state.clear_active_order_no_grace();
                        sync_atomic_status(&self.atomic_status, &state.status);
                        self.order_snapshot.set(None);
                        info!("[WS_HEALTH] Cleared stale active order after recovery");
                    }
                }
            }
            Err(e) => {
                warn!("[WS_HEALTH] Failed to sync open orders after recovery: {}", e);
            }
        }
    }
}
