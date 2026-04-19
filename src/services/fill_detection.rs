use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn, error};
use colored::Colorize;
use fast_float::parse;

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent, PacificaTrading, PacificaWsTrading,
    PositionBaselineUpdater,
};
use crate::services::fill_aggregator::FillAggregator;
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;
use crate::util::log::{tag_static, Color};

/// WebSocket-based fill detection service (primary fill detection method)
pub struct FillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub fill_config: FillDetectionConfig,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub baseline_updater: PositionBaselineUpdater,
    pub atomic_status: Arc<std::sync::atomic::AtomicU8>,
    pub order_snapshot: Arc<crate::services::order_monitor::SharedOrderSnapshot>,
}

impl FillDetectionService {
    pub async fn run(self) {
        info!("{} Starting fill detection client", tag_static("FILL_DETECTION", Color::Magenta));

        let mut fill_client = match FillDetectionClient::new(self.fill_config, false) {
            Ok(client) => client,
            Err(e) => {
                error!("{} {} Failed to create fill detection client: {}",
                    tag_static("FILL_DETECTION", Color::Magenta),
                    "✗".red().bold(),
                    e
                );
                return;
            }
        };

        // REST reconcile hook: on every (re)connect, scan open_orders for any
        // fills we missed during the WS outage and push them into the
        // aggregator (which routes them to the hedge queue on terminal events).
        {
            let pac_for_hook = self.pacifica_trading.clone();
            let symbol_for_hook = self.symbol.clone();
            let aggregator_for_hook = self.fill_aggregator.clone();
            let processed_for_hook = self.processed_fills.clone();
            let hedge_tx_for_hook = self.hedge_tx.clone();

            let hook: crate::connector::pacifica::ReconcileHook = Arc::new(move || {
                let pac = pac_for_hook.clone();
                let symbol = symbol_for_hook.clone();
                let aggregator = aggregator_for_hook.clone();
                let processed = processed_for_hook.clone();
                let hedge_tx = hedge_tx_for_hook.clone();
                Box::pin(async move {
                    match pac.get_open_orders().await {
                        Ok(orders) => {
                            for order in orders.into_iter().filter(|o| o.symbol == symbol) {
                                let filled: f64 = parse(&order.filled_amount).unwrap_or(0.0);
                                if filled <= 0.0 {
                                    continue;
                                }
                                let initial: f64 = parse(&order.initial_amount).unwrap_or(0.0);
                                let price: f64 = parse(&order.price).unwrap_or(0.0);
                                let is_terminal = (filled - initial).abs() < 1e-9;
                                let side = match order.side.as_str() {
                                    "bid" | "buy" => OrderSide::Buy,
                                    "ask" | "sell" => OrderSide::Sell,
                                    _ => continue,
                                };
                                if let Some(d) = aggregator.on_fill(order.order_id, side, filled, price, is_terminal) {
                                    if processed.insert_if_new(FillKey::OrderId(order.order_id)) {
                                        warn!(
                                            "[FILL_DETECTION] Reconcile after reconnect: emitting missed fill (order_id={}, size={}, price=${:.4})",
                                            order.order_id, d.size, d.avg_price
                                        );
                                        let _ = hedge_tx.try_send((d.side, d.size, d.avg_price, std::time::Instant::now()));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            warn!("[FILL_DETECTION] Reconcile REST call failed: {}", e);
                        }
                    }
                })
            });
            fill_client.set_reconcile_hook(hook);
        }

        // Clone dependencies for the callback closure
        let bot_state = self.bot_state.clone();
        let hedge_tx = self.hedge_tx.clone();
        let pacifica_trading = self.pacifica_trading.clone();
        let pacifica_ws_trading = self.pacifica_ws_trading.clone();
        let symbol = self.symbol.clone();
        let processed_fills = self.processed_fills.clone();
        let fill_aggregator = self.fill_aggregator.clone();
        let baseline_updater = self.baseline_updater.clone();
        let atomic_status = self.atomic_status.clone();
        let order_snapshot = self.order_snapshot.clone();

        fill_client
            .start(move |fill_event| {
                match fill_event {
                    FillEvent::FullFill {
                        order_id,
                        symbol: fill_symbol,
                        side,
                        filled_amount,
                        avg_price,
                        client_order_id,
                        ..
                    } => {
                        info!(
                            "{} {} FULL FILL: {} {} {} @ {} (cloid: {})",
                            tag_static("FILL_DETECTION", Color::Magenta),
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
                        let fill_aggregator_clone = fill_aggregator.clone();
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
                                let fill_detect_start = std::time::Instant::now();

                                let order_side = match side_str.as_str() {
                                    "buy" | "bid" => OrderSide::Buy,
                                    "sell" | "ask" => OrderSide::Sell,
                                    _ => {
                                        error!("{} {} Unknown side: {}", tag_static("FILL_DETECTION", Color::Magenta), "✗".red().bold(), side_str);
                                        return;
                                    }
                                };

                                let filled_size: f64 = parse(&filled_amount_str).unwrap_or(0.0);
                                let avg_px_for_agg: f64 = parse(&avg_price_str).unwrap_or(0.0);

                                // Route through the aggregator first. A previous partial-fill
                                // emergency emit may have already hedged — in that case,
                                // is_terminal=true here returns None and we skip the hedge.
                                let decision = fill_aggregator_clone.on_fill(
                                    order_id,
                                    order_side,
                                    filled_size,
                                    avg_px_for_agg,
                                    true, // terminal
                                );
                                if decision.is_none() {
                                    debug!(
                                        "[FILL_DETECTION] Full fill already emitted by aggregator (order_id={}), skipping",
                                        order_id
                                    );
                                    return;
                                }
                                // Second guard via dedup set — defends against the same
                                // aggregator being reused across processes / restarts.
                                if !processed_fills_clone.insert_if_new(FillKey::OrderId(order_id)) {
                                    debug!(
                                        "[FILL_DETECTION] Full fill already processed (order_id={}), skipping",
                                        order_id
                                    );
                                    return;
                                }

                                {
                                    let mut state = bot_state_clone.write().await;
                                    state.mark_filled(filled_size, order_side);
                                }

                                info!("{} {} FILL DETECTED - State updated to Filled",
                                    tag_static("FILL_DETECTION", Color::Magenta),
                                    "✓".green().bold()
                                );

                                // *** PARALLEL EXECUTION: Cancellation + Hedge Trigger ***
                                // State machine (mark_filled) already prevents new orders
                                // Dual cancel runs async while hedge triggers immediately
                                // Pre-hedge cancellation in hedge.rs provides defensive redundancy
                                info!("{} {} Spawning async dual cancellation (REST + WebSocket)...",
                                    tag_static("FILL_DETECTION", Color::Magenta),
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
                                            info!("{} {} Background dual cancellation complete (REST: {}, WS: {})",
                                                tag_static("FILL_DETECTION", Color::Magenta),
                                                "✓✓".green().bold(),
                                                rest_count,
                                                ws_count
                                            );
                                        }
                                        Err(e) => {
                                            error!("{} {} Background dual cancellation failed: {}",
                                                tag_static("FILL_DETECTION", Color::Magenta),
                                                "✗".red().bold(),
                                                e
                                            );
                                        }
                                    }
                                });

                                // *** CRITICAL: UPDATE POSITION BASELINE ***
                                // This prevents position-based detection from triggering duplicate hedge
                                let avg_px: f64 = parse(&avg_price_str).unwrap_or(0.0);
                                baseline_updater_clone.update_baseline(
                                    &symbol_clone,
                                    &side_str,
                                    filled_size,
                                    avg_px
                                );

                                // *** TRIGGER HEDGE IMMEDIATELY (PARALLEL WITH CANCELLATION) ***
                                let hedge_trigger_latency = fill_detect_start.elapsed();
                                info!("{} {} ⚡ PARALLEL EXECUTION: Hedge triggered in {:.1}ms (cancellation running async)",
                                    format!("[{}]", symbol_clone).bright_white().bold(),
                                    "Order filled".green().bold(),
                                    hedge_trigger_latency.as_secs_f64() * 1000.0
                                );

                                // Trigger hedge immediately (runs in parallel with background cancellation)
                                if let Err(e) = hedge_tx.try_send((order_side, filled_size, avg_px, fill_detect_start)) {
                                    error!(
                                        "{} {} Hedge queue send failed — BACKLOG: {}",
                                        tag_static("FILL_DETECTION", Color::Magenta),
                                        "✗".red().bold(),
                                        e
                                    );
                                }
                            }
                        });
                    }
                    FillEvent::Cancelled { order_id, client_order_id, side, filled_amount, reason, .. } => {
                        debug!(
                            "[FILL_DETECTION] Order cancelled: {} (reason: {}, filled_amount: {})",
                            client_order_id.as_deref().unwrap_or("None"),
                            reason,
                            filled_amount
                        );

                        // Cancellation is a terminal event from the aggregator's perspective.
                        // If any fills accumulated before the cancel landed, push them to the
                        // hedge queue now so the remaining exposure is neutralised.
                        let filled_so_far: f64 = parse(&filled_amount).unwrap_or(0.0);
                        if filled_so_far > 0.0 {
                            let order_side = match side.as_str() {
                                "buy" | "bid" => OrderSide::Buy,
                                "sell" | "ask" => OrderSide::Sell,
                                _ => OrderSide::Buy, // unknown; aggregator only emits if we already have accumulator with a proper side
                            };
                            // Aggregator may already have the correct cumulative+price from WS partials;
                            // we pass `filled_so_far` as a lower bound.
                            if let Some(d) = fill_aggregator.on_fill(order_id, order_side, filled_so_far, 0.0, true) {
                                if processed_fills.insert_if_new(FillKey::OrderId(order_id)) {
                                    warn!(
                                        "[FILL_DETECTION] Cancellation left {} filled on order_id={}, emitting hedge",
                                        d.size, order_id
                                    );
                                    let _ = hedge_tx.try_send((d.side, d.size, d.avg_price, std::time::Instant::now()));
                                }
                            }
                        }

                        // Spawn async task to handle the cancellation
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
                                // *** CRITICAL FIX: Only reset to Idle if in OrderPlaced state ***
                                // Prevents race condition where post-fill cancellation confirmations
                                // (from dual-cancel safety mechanism) reset state while hedge executes
                                match &state.status {
                                    BotStatus::OrderPlaced => {
                                        // Normal cancellation (monitor refresh, profit deviation, etc.)
                                        state.clear_active_order();
                                        // Sync atomic status and clear order snapshot
                                        crate::services::order_monitor::sync_atomic_status(&atomic_status_clone, &state.status);
                                        order_snapshot_clone.set(None);
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
                        order_id,
                        symbol: fill_symbol,
                        side,
                        filled_amount,
                        original_amount,
                        avg_price,
                        client_order_id,
                        ..
                    } => {
                        // Calculate notional value of partial fill
                        let filled_size: f64 = parse(&filled_amount).unwrap_or(0.0);
                        let fill_price: f64 = parse(&avg_price).unwrap_or(0.0);
                        let notional_value = filled_size * fill_price;

                        info!(
                            "{} {} PARTIAL FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {}",
                            tag_static("FILL_DETECTION", Color::Magenta),
                            "⚡".yellow().bold(),
                            side.bright_yellow(),
                            filled_amount.bright_white(),
                            fill_symbol.bright_white().bold(),
                            avg_price.cyan(),
                            filled_amount.bright_white(),
                            original_amount,
                            format!("${:.2}", notional_value).cyan().bold()
                        );

                        // Translate side string into OrderSide and accumulate into the
                        // aggregator. The aggregator emits a hedge only when a terminal
                        // event arrives OR the emergency notional is breached — see
                        // fill_aggregator.rs.
                        let order_side = match side.as_str() {
                            "buy" | "bid" => OrderSide::Buy,
                            "sell" | "ask" => OrderSide::Sell,
                            _ => {
                                error!("{} {} Unknown side: {}", tag_static("FILL_DETECTION", Color::Magenta), "✗".red().bold(), side);
                                return;
                            }
                        };

                        let decision = fill_aggregator.on_fill(
                            order_id,
                            order_side,
                            filled_size,
                            fill_price,
                            false, // not terminal — still partial
                        );

                        if let Some(d) = decision {
                            warn!(
                                "{} {} Partial-fill notional breach (order_id={}): hedging {} @ ${:.4}",
                                tag_static("FILL_DETECTION", Color::Magenta),
                                "⚠".yellow().bold(),
                                order_id,
                                d.size,
                                d.avg_price
                            );

                            // Spawn async task to handle the partial fill (same as full fill)
                            let bot_state_clone = bot_state.clone();
                            let hedge_tx = hedge_tx.clone();
                            let side_str = side.clone();
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
                                    if !processed_fills_clone.insert_if_new(FillKey::OrderId(order_id)) {
                                        debug!(
                                            "[FILL_DETECTION] Partial fill already processed (order_id={}), skipping",
                                            order_id
                                        );
                                        return;
                                    }

                                    let fill_detect_start = std::time::Instant::now();

                                    {
                                        let mut state = bot_state_clone.write().await;
                                        state.mark_filled(d.size, d.side);
                                    }

                                    info!("{} {} PARTIAL FILL DETECTED - State updated to Filled",
                                        tag_static("FILL_DETECTION", Color::Magenta),
                                        "✓".green().bold()
                                    );

                                    let pac_trading_bg = pac_trading_clone.clone();
                                    let pac_ws_trading_bg = pac_ws_trading_clone.clone();
                                    let symbol_bg = symbol_clone.clone();

                                    tokio::spawn(async move {
                                        match dual_cancel(
                                            &pac_trading_bg,
                                            &pac_ws_trading_bg,
                                            &symbol_bg
                                        ).await {
                                            Ok((rest_count, ws_count)) => {
                                                info!("{} {} Background dual cancellation complete (REST: {}, WS: {})",
                                                    tag_static("FILL_DETECTION", Color::Magenta),
                                                    "✓✓".green().bold(),
                                                    rest_count,
                                                    ws_count
                                                );
                                            }
                                            Err(e) => {
                                                error!("{} {} Background dual cancellation failed: {}",
                                                    tag_static("FILL_DETECTION", Color::Magenta),
                                                    "✗".red().bold(),
                                                    e
                                                );
                                            }
                                        }
                                    });

                                    baseline_updater_clone.update_baseline(
                                        &symbol_clone,
                                        &side_str,
                                        d.size,
                                        d.avg_price,
                                    );

                                    if let Err(e) = hedge_tx.try_send((d.side, d.size, d.avg_price, fill_detect_start)) {
                                        error!(
                                            "{} {} Hedge queue send failed — BACKLOG: {}",
                                            tag_static("FILL_DETECTION", Color::Magenta),
                                            "✗".red().bold(),
                                            e
                                        );
                                    }
                                }
                            });
                        } else {
                            debug!(
                                "[FILL_DETECTION] Partial fill accumulated in aggregator (order_id={}, cumulative ${:.2}), waiting for terminal event",
                                order_id, notional_value
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
