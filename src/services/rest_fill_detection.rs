use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::debug;
use colored::Colorize;
use fast_float::parse;

use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;
use crate::util::rate_limit::is_rate_limit_error;

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}

/// REST API fill detection service (backup/fallback method)
///
/// Polls open_orders via REST API every 500ms to detect fills that may have been
/// missed by WebSocket. This provides redundancy and recovery capabilities.
pub struct RestFillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub symbol: String,
    pub processed_fills: Arc<parking_lot::Mutex<HashSet<String>>>,
    pub min_hedge_notional: f64,
}

impl RestFillDetectionService {
    pub async fn run(self) {
        let mut consecutive_errors = 0u32;
        let mut last_known_filled_amount: f64 = 0.0;

        loop {
            // Adaptive polling: fast when order active, slow when idle
            let has_active_order = {
                let state = self.bot_state.read().await;
                state.has_active_order_fast() || matches!(state.status, crate::bot::BotStatus::Filled | crate::bot::BotStatus::Hedging)
            };

            let poll_ms = if has_active_order { 100 } else { 1000 };
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
                    // No active order - check if this is a recent cancellation
                    if matches!(state.status, crate::bot::BotStatus::Filled | crate::bot::BotStatus::Hedging) {
                        // Recent fill/hedge - keep polling briefly for catch-up
                        None // Will query all open orders below
                    } else {
                        // Old cancellation, truly idle
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
                    consecutive_errors = 0; // Reset error counter on success

                    // Find our order - either by client_order_id or any filled order in recovery mode
                    let our_order = if let Some(ref cloid) = client_order_id_opt {
                        // Normal mode: Find by client_order_id
                        orders.iter().find(|o| &o.client_order_id == cloid)
                    } else {
                        // Recovery mode: Find any order with fills for our symbol
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

                        // Check if there's a NEW fill (filled_amount increased since last check)
                        if filled_amount > last_known_filled_amount && filled_amount > 0.0 {
                            let new_fill_amount = filled_amount - last_known_filled_amount;
                            let notional_value = new_fill_amount * price;

                            debug!(
                                "[REST_FILL_DETECTION] Fill detected: {} -> {} (new: {}) | Notional: ${:.2}",
                                last_known_filled_amount, filled_amount, new_fill_amount, notional_value
                            );

                            // Update last known amount
                            last_known_filled_amount = filled_amount;

                            // Check if this is a full fill or significant partial fill
                            let is_full_fill = (filled_amount - initial_amount).abs() < 0.0001;

                            if is_full_fill || notional_value > self.min_hedge_notional {
                                let fill_type = if is_full_fill { "full" } else { "partial" };
                                let cloid = &order.client_order_id;
                                let fill_id = format!("{}_{}_rest", fill_type, cloid);

                                // Check if already processed (prevent duplicate hedges from WebSocket)
                                let mut processed = self.processed_fills.lock();
                                if processed.contains(&fill_id)
                                    || processed.contains(&format!("full_{}", cloid))
                                    || processed.contains(&format!("partial_{}", cloid))
                                {
                                    debug!("[REST_FILL_DETECTION] Fill already processed by WebSocket, skipping");
                                    continue;
                                }
                                processed.insert(fill_id);
                                drop(processed);

                                // Determine order side from order data or state
                                let order_side = if let Some(side) = order_side_opt {
                                    side
                                } else {
                                    // Recovery mode: parse from order.side string
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

                                tprintln!(
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

                                // Trigger hedge (same flow as WebSocket)
                                let bot_state_clone = self.bot_state.clone();
                                let hedge_tx_clone = self.hedge_tx.clone();
                                let pac_trading_clone = self.pacifica_trading.clone();
                                let pac_ws_trading_clone = self.pacifica_ws_trading.clone();
                                let symbol_clone = self.symbol.clone();

                                tokio::spawn(async move {
                                    // Update state
                                    {
                                        let mut state = bot_state_clone.write().await;
                                        state.mark_filled(filled_amount, order_side);
                                    }

                                    tprintln!(
                                        "{} {} State updated to Filled (REST)",
                                        "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                        "✓".green().bold()
                                    );

                                    // Dual cancellation
                                    tprintln!(
                                        "{} {} Dual cancellation (REST + WebSocket)...",
                                        "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                        "⚡".yellow().bold()
                                    );

                                    match dual_cancel(&pac_trading_clone, &pac_ws_trading_clone, &symbol_clone).await
                                    {
                                        Ok((rest_count, ws_count)) => {
                                            tprintln!(
                                                "{} {} Dual cancellation complete (REST: {}, WS: {})",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "✓✓".green().bold(),
                                                rest_count,
                                                ws_count
                                            );
                                        }
                                        Err(e) => {
                                            tprintln!(
                                                "{} {} Dual cancellation failed: {}",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "✗".red().bold(),
                                                e
                                            );
                                        }
                                    }

                                    tprintln!(
                                        "{} {}, triggering hedge (REST)",
                                        format!("[{}]", symbol_clone).bright_white().bold(),
                                        "Order filled".green().bold()
                                    );

                                    // Trigger hedge (with current timestamp since REST detection detects fills retroactively)
                                    let _ = hedge_tx_clone.send((order_side, filled_amount, price, std::time::Instant::now()));
                                });
                            } else {
                                debug!(
                                    "[REST_FILL_DETECTION] Fill notional ${:.2} < ${:.2} threshold, skipping",
                                    notional_value, self.min_hedge_notional
                                );
                            }
                        }
                    } else if client_order_id_opt.is_some() {
                        // Order not found but we expect it - might be filled and removed
                        debug!("[REST_FILL_DETECTION] Active order not found in open_orders (might be fully filled)");
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;

                    // Check if it's a rate limit error
                    let is_rate_limit = is_rate_limit_error(&e);

                    if is_rate_limit {
                        // Exponential backoff for rate limits: 1s, 2s, 4s, 8s, 16s, 32s (max)
                        let backoff_secs = std::cmp::min(2u64.pow(consecutive_errors - 1), 32);
                        tprintln!(
                            "{} {} Rate limit hit, backing off for {} seconds...",
                            "[REST_FILL_DETECTION]".bright_cyan().bold(),
                            "⚠".yellow().bold(),
                            backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    } else {
                        // Other errors: log and continue with normal polling
                        debug!(
                            "[REST_FILL_DETECTION] Error fetching open orders (attempt {}): {}",
                            consecutive_errors, e
                        );

                        // If too many consecutive errors, log warning
                        if consecutive_errors >= 5 {
                            tprintln!(
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
