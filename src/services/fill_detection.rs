use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::debug;
use colored::Colorize;

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent, PacificaTrading, PacificaWsTrading,
    PositionBaselineUpdater,
};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}

/// WebSocket-based fill detection service (primary fill detection method)
pub struct FillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub fill_config: FillDetectionConfig,
    pub symbol: String,
    pub processed_fills: Arc<tokio::sync::Mutex<HashSet<String>>>,
    pub baseline_updater: PositionBaselineUpdater,
}

impl FillDetectionService {
    pub async fn run(self) {
        tprintln!("{} Starting fill detection client", "[FILL_DETECTION]".magenta().bold());

        let mut fill_client = match FillDetectionClient::new(self.fill_config, false) {
            Ok(client) => client,
            Err(e) => {
                tprintln!("{} {} Failed to create fill detection client: {}",
                    "[FILL_DETECTION]".magenta().bold(),
                    "✗".red().bold(),
                    e
                );
                return;
            }
        };

        // Clone dependencies for the callback closure
        let bot_state = self.bot_state.clone();
        let hedge_tx = self.hedge_tx.clone();
        let pacifica_trading = self.pacifica_trading.clone();
        let pacifica_ws_trading = self.pacifica_ws_trading.clone();
        let symbol = self.symbol.clone();
        let processed_fills = self.processed_fills.clone();
        let baseline_updater = self.baseline_updater.clone();

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
                        tprintln!(
                            "{} {} FULL FILL: {} {} {} @ {} (cloid: {})",
                            "[FILL_DETECTION]".magenta().bold(),
                            "✓".green().bold(),
                            side.bright_yellow(),
                            filled_amount.bright_white(),
                            fill_symbol.bright_white().bold(),
                            avg_price.cyan(),
                            client_order_id.as_deref().unwrap_or("None")
                        );

                        // Spawn async task to handle the fill
                        let bot_state_clone = bot_state.clone();
                        let hedge_tx = hedge_tx.clone();
                        let side_str = side.clone();
                        let filled_amount_str = filled_amount.clone();
                        let avg_price_str = avg_price.clone();
                        let cloid = client_order_id.clone();
                        let pac_trading_clone = pacifica_trading.clone();
                        let pac_ws_trading_clone = pacifica_ws_trading.clone();
                        let symbol_clone = symbol.clone();
                        let processed_fills_clone = processed_fills.clone();
                        let baseline_updater_clone = baseline_updater.clone();

                        tokio::spawn(async move {
                            // Check if this is our order
                            let state = bot_state_clone.read().await;
                            let is_our_order = state
                                .active_order
                                .as_ref()
                                .and_then(|o| cloid.as_ref().map(|id| &o.client_order_id == id))
                                .unwrap_or(false);
                            drop(state);

                            if is_our_order {
                                // Check if this fill was already processed (prevent duplicate hedges)
                                let fill_id = cloid.as_ref().map(|id| format!("full_{}", id)).unwrap_or_default();
                                {
                                    let mut processed = processed_fills_clone.lock().await;
                                    if processed.contains(&fill_id) {
                                        debug!("[FILL_DETECTION] Full fill already processed (duplicate), skipping");
                                        return;
                                    }
                                    processed.insert(fill_id);
                                }

                                // *** CRITICAL: UPDATE STATE FIRST ***
                                // Mark as filled IMMEDIATELY to prevent main loop from placing new orders
                                // State machine provides race condition protection - cancellation can run async
                                let fill_detect_start = std::time::Instant::now();

                                let order_side = match side_str.as_str() {
                                    "buy" | "bid" => OrderSide::Buy,
                                    "sell" | "ask" => OrderSide::Sell,
                                    _ => {
                                        tprintln!("{} {} Unknown side: {}", "[FILL_DETECTION]".magenta().bold(), "✗".red().bold(), side_str);
                                        return;
                                    }
                                };

                                let filled_size: f64 = filled_amount_str.parse().unwrap_or(0.0);

                                {
                                    let mut state = bot_state_clone.write().await;
                                    state.mark_filled(filled_size, order_side);
                                }

                                tprintln!("{} {} FILL DETECTED - State updated to Filled",
                                    "[FILL_DETECTION]".magenta().bold(),
                                    "✓".green().bold()
                                );

                                // *** PARALLEL EXECUTION: Cancellation + Hedge Trigger ***
                                // State machine (mark_filled) already prevents new orders
                                // Dual cancel runs async while hedge triggers immediately
                                // Pre-hedge cancellation in hedge.rs provides defensive redundancy
                                tprintln!("{} {} Spawning async dual cancellation (REST + WebSocket)...",
                                    "[FILL_DETECTION]".magenta().bold(),
                                    "⚡".yellow().bold()
                                );

                                // Clone for async cancellation task
                                let pac_trading_bg = pac_trading_clone.clone();
                                let pac_ws_trading_bg = pac_ws_trading_clone.clone();
                                let symbol_bg = symbol_clone.clone();

                                // Spawn dual cancel in background (don't await)
                                tokio::spawn(async move {
                                    match dual_cancel(
                                        &pac_trading_bg,
                                        &pac_ws_trading_bg,
                                        &symbol_bg
                                    ).await {
                                        Ok((rest_count, ws_count)) => {
                                            tprintln!("{} {} Background dual cancellation complete (REST: {}, WS: {})",
                                                "[FILL_DETECTION]".magenta().bold(),
                                                "✓✓".green().bold(),
                                                rest_count,
                                                ws_count
                                            );
                                        }
                                        Err(e) => {
                                            tprintln!("{} {} Background dual cancellation failed: {}",
                                                "[FILL_DETECTION]".magenta().bold(),
                                                "✗".red().bold(),
                                                e
                                            );
                                        }
                                    }
                                });

                                // *** CRITICAL: UPDATE POSITION BASELINE ***
                                // This prevents position-based detection from triggering duplicate hedge
                                let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                baseline_updater_clone.update_baseline(
                                    &symbol_clone,
                                    &side_str,
                                    filled_size,
                                    avg_px
                                );

                                // *** TRIGGER HEDGE IMMEDIATELY (PARALLEL WITH CANCELLATION) ***
                                let hedge_trigger_latency = fill_detect_start.elapsed();
                                tprintln!("{} {} ⚡ PARALLEL EXECUTION: Hedge triggered in {:.1}ms (cancellation running async)",
                                    format!("[{}]", symbol_clone).bright_white().bold(),
                                    "Order filled".green().bold(),
                                    hedge_trigger_latency.as_secs_f64() * 1000.0
                                );

                                // Trigger hedge immediately (runs in parallel with background cancellation)
                                let _ = hedge_tx.send((order_side, filled_size, avg_px, fill_detect_start));
                            }
                        });
                    }
                    FillEvent::Cancelled { client_order_id, reason, .. } => {
                        // Log at debug level since monitor already logs cancellations
                        debug!(
                            "[FILL_DETECTION] Order cancelled: {} (reason: {})",
                            client_order_id.as_deref().unwrap_or("None"),
                            reason
                        );

                        // Spawn async task to handle the cancellation
                        let bot_state_clone = bot_state.clone();
                        let cloid = client_order_id.clone();

                        tokio::spawn(async move {
                            let mut state = bot_state_clone.write().await;
                            let is_our_order = state
                                .active_order
                                .as_ref()
                                .and_then(|o| cloid.as_ref().map(|id| &o.client_order_id == id))
                                .unwrap_or(false);

                            if is_our_order {
                                // *** CRITICAL FIX: Only reset to Idle if in OrderPlaced state ***
                                // Prevents race condition where post-fill cancellation confirmations
                                // (from dual-cancel safety mechanism) reset state while hedge executes
                                match &state.status {
                                    BotStatus::OrderPlaced => {
                                        // Normal cancellation (monitor refresh, profit deviation, etc.)
                                        state.clear_active_order();
                                        debug!("[BOT] Active order cancelled, returning to Idle");
                                    }
                                    BotStatus::Filled | BotStatus::Hedging | BotStatus::Complete => {
                                        // Post-fill cancellation confirmation (from dual-cancel safety)
                                        // DO NOT reset state - hedge is in progress or complete
                                        debug!(
                                            "[BOT] Cancellation confirmed for order in {:?} state (ignoring, hedge in progress)",
                                            state.status
                                        );
                                    }
                                    BotStatus::Idle => {
                                        // Already idle, no action needed
                                        debug!("[BOT] Cancellation received but state already Idle");
                                    }
                                    BotStatus::Error(_) => {
                                        // Error state, don't change anything
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
                        // Calculate notional value of partial fill
                        let filled_size: f64 = filled_amount.parse().unwrap_or(0.0);
                        let fill_price: f64 = avg_price.parse().unwrap_or(0.0);
                        let notional_value = filled_size * fill_price;

                        tprintln!(
                            "{} {} PARTIAL FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {}",
                            "[FILL_DETECTION]".magenta().bold(),
                            "⚡".yellow().bold(),
                            side.bright_yellow(),
                            filled_amount.bright_white(),
                            fill_symbol.bright_white().bold(),
                            avg_price.cyan(),
                            filled_amount.bright_white(),
                            original_amount,
                            format!("${:.2}", notional_value).cyan().bold()
                        );

                        // Only hedge if notional value > $10
                        if notional_value > 10.0 {
                            tprintln!(
                                "{} {} Partial fill notional ${:.2} > $10.00 threshold, initiating hedge",
                                "[FILL_DETECTION]".magenta().bold(),
                                "✓".green().bold(),
                                notional_value
                            );

                            // Spawn async task to handle the partial fill (same as full fill)
                            let bot_state_clone = bot_state.clone();
                            let hedge_tx = hedge_tx.clone();
                            let side_str = side.clone();
                            let filled_amount_str = filled_amount.clone();
                            let avg_price_str = avg_price.clone();
                            let cloid = client_order_id.clone();
                            let pac_trading_clone = pacifica_trading.clone();
                            let pac_ws_trading_clone = pacifica_ws_trading.clone();
                            let symbol_clone = symbol.clone();
                            let processed_fills_clone = processed_fills.clone();
                            let baseline_updater_clone = baseline_updater.clone();

                            tokio::spawn(async move {
                                // Check if this is our order
                                let state = bot_state_clone.read().await;
                                let is_our_order = state
                                    .active_order
                                    .as_ref()
                                    .and_then(|o| cloid.as_ref().map(|id| &o.client_order_id == id))
                                    .unwrap_or(false);
                                drop(state);

                                if is_our_order {
                                    // Check if this fill was already processed (prevent duplicate hedges)
                                    let fill_id = cloid.as_ref().map(|id| format!("partial_{}", id)).unwrap_or_default();
                                    {
                                        let mut processed = processed_fills_clone.lock().await;
                                        if processed.contains(&fill_id) {
                                            debug!("[FILL_DETECTION] Partial fill already processed (duplicate), skipping");
                                            return;
                                        }
                                        processed.insert(fill_id);
                                    }

                                    // *** CRITICAL: UPDATE STATE FIRST ***
                                    // State machine provides race condition protection - cancellation can run async
                                    let fill_detect_start = std::time::Instant::now();

                                    let order_side = match side_str.as_str() {
                                        "buy" | "bid" => OrderSide::Buy,
                                        "sell" | "ask" => OrderSide::Sell,
                                        _ => {
                                            tprintln!("{} {} Unknown side: {}", "[FILL_DETECTION]".magenta().bold(), "✗".red().bold(), side_str);
                                            return;
                                        }
                                    };

                                    let filled_size: f64 = filled_amount_str.parse().unwrap_or(0.0);

                                    {
                                        let mut state = bot_state_clone.write().await;
                                        state.mark_filled(filled_size, order_side);
                                    }

                                    tprintln!("{} {} PARTIAL FILL DETECTED - State updated to Filled",
                                        "[FILL_DETECTION]".magenta().bold(),
                                        "✓".green().bold()
                                    );

                                    // *** PARALLEL EXECUTION: Cancellation + Hedge Trigger ***
                                    // State machine (mark_filled) already prevents new orders
                                    // Dual cancel runs async while hedge triggers immediately
                                    // Pre-hedge cancellation in hedge.rs provides defensive redundancy
                                    tprintln!("{} {} Spawning async dual cancellation (REST + WebSocket)...",
                                        "[FILL_DETECTION]".magenta().bold(),
                                        "⚡".yellow().bold()
                                    );

                                    // Clone for async cancellation task
                                    let pac_trading_bg = pac_trading_clone.clone();
                                    let pac_ws_trading_bg = pac_ws_trading_clone.clone();
                                    let symbol_bg = symbol_clone.clone();

                                    // Spawn dual cancel in background (don't await)
                                    tokio::spawn(async move {
                                        match dual_cancel(
                                            &pac_trading_bg,
                                            &pac_ws_trading_bg,
                                            &symbol_bg
                                        ).await {
                                            Ok((rest_count, ws_count)) => {
                                                tprintln!("{} {} Background dual cancellation complete (REST: {}, WS: {})",
                                                    "[FILL_DETECTION]".magenta().bold(),
                                                    "✓✓".green().bold(),
                                                    rest_count,
                                                    ws_count
                                                );
                                            }
                                            Err(e) => {
                                                tprintln!("{} {} Background dual cancellation failed: {}",
                                                    "[FILL_DETECTION]".magenta().bold(),
                                                    "✗".red().bold(),
                                                    e
                                                );
                                            }
                                        }
                                    });

                                    // *** CRITICAL: UPDATE POSITION BASELINE ***
                                    let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                    baseline_updater_clone.update_baseline(
                                        &symbol_clone,
                                        &side_str,
                                        filled_size,
                                        avg_px
                                    );

                                    // *** TRIGGER HEDGE IMMEDIATELY (PARALLEL WITH CANCELLATION) ***
                                    let hedge_trigger_latency = fill_detect_start.elapsed();
                                    tprintln!("{} {} ⚡ PARALLEL EXECUTION: Hedge triggered in {:.1}ms (cancellation running async)",
                                        format!("[{}]", symbol_clone).bright_white().bold(),
                                        "Partial fill".green().bold(),
                                        hedge_trigger_latency.as_secs_f64() * 1000.0
                                    );

                                    // Trigger hedge immediately (runs in parallel with background cancellation)
                                    let _ = hedge_tx.send((order_side, filled_size, avg_px, fill_detect_start));
                                }
                            });
                        } else {
                            tprintln!(
                                "{} {} Partial fill notional ${:.2} < $10.00 threshold, skipping hedge",
                                "[FILL_DETECTION]".magenta().bold(),
                                "→".bright_black(),
                                notional_value
                            );
                        }
                    }
                    FillEvent::PositionFill { .. } => {
                        // Position-based fills are handled by PositionMonitorService
                        // This is logged at debug level to avoid spam
                        debug!("[FILL_DETECTION] Position fill event (handled by PositionMonitorService)");
                    }
                }
            })
            .await
            .ok();
    }
}
