use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};
use colored::Colorize;
use fast_float::parse;

use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::{FillHandler, FillSource, FillType, HedgeEvent};
use crate::strategy::OrderSide;
use crate::util::rate_limit::is_rate_limit_error;

/// REST API fill detection service (backup/fallback method)
///
/// Polls open_orders via REST API on a configurable interval (fast when active,
/// slower when idle) to detect fills missed by WebSocket. This provides redundancy
/// and recovery capabilities.
pub struct RestFillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub symbol: String,
    pub processed_fills: Arc<parking_lot::Mutex<HashSet<String>>>,
    pub min_hedge_notional: f64,
    pub poll_interval_ms: u64,
}

impl RestFillDetectionService {
    pub async fn run(self) {
        // Create the unified fill handler (no baseline updater for REST)
        let fill_handler = FillHandler::new(
            self.bot_state.clone(),
            self.hedge_tx.clone(),
            self.pacifica_trading.clone(),
            self.pacifica_ws_trading.clone(),
            self.symbol.clone(),
            self.processed_fills.clone(),
            None, // No baseline updater for REST detection
        );

        let mut consecutive_errors = 0u32;
        let mut last_known_filled_amount: f64 = 0.0;

        loop {
            // Adaptive polling: fast when order active, slow when idle
            let has_active_order = {
                let state = self.bot_state.read().await;
                state.has_active_order_fast() || matches!(state.status, crate::bot::BotStatus::Filled | crate::bot::BotStatus::Hedging)
            };

            let poll_ms = if has_active_order { self.poll_interval_ms } else { 1000 };
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;

            // Get active order info, with recovery logic for recent cancellations
            let active_order_info = {
                let state = self.bot_state.read().await;

                // Skip only for terminal states
                if matches!(
                    state.status,
                    crate::bot::BotStatus::Complete | crate::bot::BotStatus::Error(_)
                ) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                if let Some(ref order) = state.active_order {
                    Some((order.client_order_id.clone(), order.side))
                } else {
                    if matches!(state.status, crate::bot::BotStatus::Filled | crate::bot::BotStatus::Hedging) {
                        None // Recovery mode
                    } else {
                        last_known_filled_amount = 0.0;
                        continue;
                    }
                }
            };

            let client_order_id_opt = active_order_info.as_ref().map(|(id, _)| id.clone());
            let order_side_opt = active_order_info.as_ref().map(|(_, side)| *side);

            // Fetch open orders via REST API
            let open_orders_result = self.pacifica_trading.get_open_orders().await;

            match open_orders_result {
                Ok(orders) => {
                    consecutive_errors = 0;

                    let our_order = if let Some(ref cloid) = client_order_id_opt {
                        orders.iter().find(|o| &o.client_order_id == cloid)
                    } else {
                        debug!(
                            "[REST_FILL_DETECTION] Recovery mode: searching {} orders for filled orders",
                            orders.len()
                        );
                        orders.iter().filter(|o| o.symbol == self.symbol).find(|o| {
                            let filled: f64 = parse(&o.filled_amount).unwrap_or(0.0);
                            filled > 0.0
                        })
                    };

                    if let Some(order) = our_order {
                        let filled_amount: f64 = parse(&order.filled_amount).unwrap_or(0.0);
                        let initial_amount: f64 = parse(&order.initial_amount).unwrap_or(0.0);
                        let price: f64 = parse(&order.price).unwrap_or(0.0);

                        if filled_amount > last_known_filled_amount && filled_amount > 0.0 {
                            let new_fill_amount = filled_amount - last_known_filled_amount;
                            let notional_value = new_fill_amount * price;

                            debug!(
                                "[REST_FILL_DETECTION] Fill detected: {} -> {} (new: {}) | Notional: ${:.2}",
                                last_known_filled_amount, filled_amount, new_fill_amount, notional_value
                            );

                            last_known_filled_amount = filled_amount;

                            let is_full_fill = (filled_amount - initial_amount).abs() < 0.0001;

                            if is_full_fill || notional_value > self.min_hedge_notional {
                                let fill_type = if is_full_fill { FillType::Full } else { FillType::Partial };
                                let cloid = &order.client_order_id;

                                // Check if already processed using FillHandler
                                if !fill_handler.try_mark_processed(fill_type, cloid) {
                                    debug!("[REST_FILL_DETECTION] Fill already processed by WebSocket, skipping");
                                    continue;
                                }

                                // Determine order side
                                let order_side = if let Some(side) = order_side_opt {
                                    side
                                } else {
                                    match order.side.as_str() {
                                        "bid" | "buy" => OrderSide::Buy,
                                        "ask" | "sell" => OrderSide::Sell,
                                        _ => {
                                            debug!(
                                                "[REST_FILL_DETECTION] Unknown order side: {}, skipping",
                                                order.side
                                            );
                                            continue;
                                        }
                                    }
                                };

                                info!(
                                    "{} {} {} FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {} {}",
                                    "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                    "✓".green().bold(),
                                    if is_full_fill { "FULL" } else { "PARTIAL" },
                                    order.side.bright_yellow(),
                                    filled_amount,
                                    self.symbol.bright_white().bold(),
                                    format!("${:.6}", price).cyan(),
                                    filled_amount,
                                    initial_amount,
                                    format!("${:.2}", notional_value).cyan().bold(),
                                    "(REST API)".bright_black()
                                );

                                // Process using unified handler
                                let side_str = order.side.clone();
                                fill_handler.process_fill(
                                    FillSource::RestApi,
                                    fill_type,
                                    order_side,
                                    filled_amount,
                                    price,
                                    Some(&side_str),
                                ).await;
                            } else {
                                debug!(
                                    "[REST_FILL_DETECTION] Fill notional ${:.2} < ${:.2} threshold, skipping",
                                    notional_value, self.min_hedge_notional
                                );
                            }
                        }
                    } else if client_order_id_opt.is_some() {
                        debug!("[REST_FILL_DETECTION] Active order not found in open_orders (might be fully filled)");
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;

                    if is_rate_limit_error(&e) {
                        let backoff_secs = std::cmp::min(2u64.pow(consecutive_errors - 1), 32);
                        warn!(
                            "{} {} Rate limit hit, backing off for {} seconds...",
                            "[REST_FILL_DETECTION]".bright_cyan().bold(),
                            "⚠".yellow().bold(),
                            backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    } else {
                        debug!(
                            "[REST_FILL_DETECTION] Error fetching open orders (attempt {}): {}",
                            consecutive_errors, e
                        );

                        if consecutive_errors >= 5 {
                            warn!(
                                "{} {} {} consecutive errors fetching open orders",
                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                "⚠".yellow().bold(),
                                consecutive_errors
                            );
                        }
                    }
                }
            }
        }
    }
}
