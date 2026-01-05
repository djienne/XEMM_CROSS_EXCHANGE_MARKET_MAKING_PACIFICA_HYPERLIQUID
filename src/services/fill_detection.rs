use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, error};
use colored::Colorize;
use fast_float::parse;

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent, PacificaTrading, PacificaWsTrading,
    PositionBaselineUpdater,
};
use crate::services::{FillHandler, FillSource, FillType, HedgeEvent};

/// WebSocket-based fill detection service (primary fill detection method)
pub struct FillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub fill_config: FillDetectionConfig,
    pub symbol: String,
    pub baseline_updater: PositionBaselineUpdater,
    pub atomic_status: Arc<std::sync::atomic::AtomicU8>,
    pub order_snapshot: Arc<crate::services::order_monitor::SharedOrderSnapshot>,
    /// Minimum notional value (USD) to trigger hedge on partial fills
    pub min_hedge_notional: f64,
    /// Expected client_order_id for fast ownership check (shared with FillHandler)
    pub expected_cloid: Arc<parking_lot::Mutex<Option<String>>>,
}

impl FillDetectionService {
    pub async fn run(self) {
        info!("{} Starting fill detection client", "[FILL_DETECTION]".magenta().bold());

        let mut fill_client = match FillDetectionClient::new(self.fill_config, false) {
            Ok(client) => client,
            Err(e) => {
                error!("{} {} Failed to create fill detection client: {}",
                    "[FILL_DETECTION]".magenta().bold(),
                    "✗".red().bold(),
                    e
                );
                return;
            }
        };

        // Create the unified fill handler with atomic fast-path support
        let fill_handler = FillHandler::with_atomic_status(
            self.bot_state.clone(),
            self.hedge_tx.clone(),
            self.pacifica_trading.clone(),
            self.pacifica_ws_trading.clone(),
            self.symbol.clone(),
            Some(self.baseline_updater.clone()),
            self.atomic_status.clone(),
            self.expected_cloid.clone(),
        );

        // Clone dependencies for the callback closure
        let bot_state = self.bot_state.clone();
        let atomic_status = self.atomic_status.clone();
        let order_snapshot = self.order_snapshot.clone();

        fill_client
            .start(move |fill_event| {
                match fill_event {
                    FillEvent::FullFill {
                        symbol: fill_symbol,
                        side,
                        filled_amount,
                        avg_price,
                        client_order_id,
                        ..
                    } => {
                        // Parse before spawning to avoid cloning strings unnecessarily
                        let filled_size: f64 = parse(&filled_amount).unwrap_or(0.0);
                        let fill_price: f64 = parse(&avg_price).unwrap_or(0.0);

                        // Minimal hot-path logging (colored formatting is expensive)
                        debug!(
                            "[FILL_DETECTION] FULL FILL: {} {} {} @ {}",
                            side, filled_amount, fill_symbol, avg_price
                        );

                        let handler = fill_handler.clone();
                        // Only clone what we need: side string and client_order_id
                        let side_str = side;
                        let cloid = client_order_id;

                        tokio::spawn(async move {
                            handler.handle_fill(
                                FillSource::WebSocket,
                                FillType::Full,
                                cloid.as_deref(),
                                &side_str,
                                filled_size,
                                fill_price,
                            ).await;
                        });
                    }
                    FillEvent::Cancelled { client_order_id, reason, .. } => {
                        debug!(
                            "[FILL_DETECTION] Order cancelled: {} (reason: {})",
                            client_order_id.as_deref().unwrap_or("None"),
                            reason
                        );

                        let bot_state_clone = bot_state.clone();
                        let cloid = client_order_id.clone();
                        let atomic_status_clone = atomic_status.clone();
                        let order_snapshot_clone = order_snapshot.clone();

                        tokio::spawn(async move {
                            let mut state = bot_state_clone.write().await;
                            let is_our_order = state
                                .active_order
                                .as_ref()
                                .and_then(|o| cloid.as_ref().map(|id| &o.client_order_id == id))
                                .unwrap_or(false);

                            if is_our_order {
                                match &state.status {
                                    BotStatus::OrderPlaced => {
                                        state.clear_active_order();
                                        crate::services::order_monitor::sync_atomic_status(&atomic_status_clone, &state.status);
                                        order_snapshot_clone.set(None);
                                        debug!("[BOT] Active order cancelled, returning to Idle");
                                    }
                                    BotStatus::Filled | BotStatus::Hedging | BotStatus::Complete => {
                                        debug!(
                                            "[BOT] Cancellation confirmed for order in {:?} state (ignoring, hedge in progress)",
                                            state.status
                                        );
                                    }
                                    BotStatus::Idle => {
                                        debug!("[BOT] Cancellation received but state already Idle");
                                    }
                                    BotStatus::Error(_) => {
                                        debug!("[BOT] Cancellation received in Error state (ignoring)");
                                    }
                                }
                            }
                        });
                    }
                    FillEvent::PartialFill {
                        symbol: fill_symbol,
                        side,
                        filled_amount,
                        original_amount,
                        avg_price,
                        client_order_id,
                        ..
                    } => {
                        let filled_size: f64 = parse(&filled_amount).unwrap_or(0.0);
                        let fill_price: f64 = parse(&avg_price).unwrap_or(0.0);
                        let notional_value = filled_size * fill_price;

                        // Minimal hot-path logging (colored formatting is expensive)
                        debug!(
                            "[FILL_DETECTION] PARTIAL FILL: {} {} {} @ {} | {}/{} | ${:.2}",
                            side, filled_amount, fill_symbol, avg_price,
                            filled_amount, original_amount, notional_value
                        );

                        // Only hedge if notional value exceeds threshold
                        let min_notional = self.min_hedge_notional;
                        if notional_value > min_notional {
                            debug!(
                                "[FILL_DETECTION] Partial fill ${:.2} > ${:.2}, hedging",
                                notional_value, min_notional
                            );

                            let handler = fill_handler.clone();
                            // Move strings directly, no clone needed
                            let side_str = side;
                            let cloid = client_order_id;

                            tokio::spawn(async move {
                                handler.handle_fill(
                                    FillSource::WebSocket,
                                    FillType::Partial,
                                    cloid.as_deref(),
                                    &side_str,
                                    filled_size,
                                    fill_price,
                                ).await;
                            });
                        } else {
                            debug!(
                                "[FILL_DETECTION] Partial fill ${:.2} < ${:.2}, skipping",
                                notional_value, min_notional
                            );
                        }
                    }
                    FillEvent::PositionFill { .. } => {
                        debug!("[FILL_DETECTION] Position fill event (handled by PositionMonitorService)");
                    }
                }
            })
            .await
            .map_err(|e| error!("{} {} Fill detection client exited: {}",
                "[FILL_DETECTION]".magenta().bold(), "✗".red().bold(), e))
            .ok();
    }
}
