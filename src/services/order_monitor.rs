use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::debug;
use colored::Colorize;
use fast_float::parse;

use crate::bot::{BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::strategy::{Opportunity, OpportunityEvaluator, OrderSide};
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}

/// Order monitoring service
///
/// Monitors active orders for:
/// 1. Age - refreshes order if age > order_refresh_interval_secs
/// 2. Profit deviation - cancels if profit drops > profit_cancel_threshold_bps
/// 3. Periodic profit logging every 2 seconds
///
/// Checks every 25ms (40 Hz) for fast response to market changes.
pub struct OrderMonitorService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub pacifica_prices: Arc<Mutex<(f64, f64)>>,
    pub hyperliquid_prices: Arc<Mutex<(f64, f64)>>,
    pub config: Config,
    pub evaluator: OpportunityEvaluator,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
}

impl OrderMonitorService {
    pub async fn run(self) {
        let mut monitor_interval = interval(Duration::from_millis(1)); // Check every 1ms (1000 Hz)
        let mut log_interval = interval(Duration::from_secs(2)); // Log profit every 2 seconds

        // Rate limit tracking for cancellations
        let mut cancel_rate_limit = RateLimitTracker::new();

        loop {
            tokio::select! {
                _ = monitor_interval.tick() => {
                    let state = self.bot_state.read().await;

                    // Only monitor orders that are actively placed (not filled/hedging/complete)
                    if !matches!(state.status, BotStatus::OrderPlaced) {
                        continue;
                    }

                    let active_order = match &state.active_order {
                        Some(order) => order.clone(),
                        None => continue,
                    };
                    drop(state);

            // Check 1: Order age (refresh if > order_refresh_interval_secs)
            if active_order.placed_at.elapsed() > Duration::from_secs(self.config.order_refresh_interval_secs) {
                // Check if we're in rate limit backoff period
                if cancel_rate_limit.should_skip() {
                    let remaining = cancel_rate_limit.remaining_backoff_secs();
                    debug!("[MONITOR] Skipping age cancellation (rate limit backoff, {:.1}s remaining)", remaining);
                    continue;
                }

                let (hl_bid, hl_ask) = *self.hyperliquid_prices.lock().unwrap();
                let age_ms = active_order.placed_at.elapsed().as_millis();

                // *** CRITICAL FIX: Check if order has fills BEFORE cancelling ***
                // If order is filled but WebSocket missed it, don't cancel - let REST fill detection handle it
                let filled_check_result = self.pacifica_trading.get_open_orders().await;

                match filled_check_result {
                    Ok(orders) => {
                        if let Some(order) = orders.iter().find(|o| o.client_order_id == active_order.client_order_id) {
                            let filled_amount: f64 = parse(&order.filled_amount).unwrap_or(0.0);

                            if filled_amount > 0.0 {
                                // Order has fills - DON'T cancel, let fill detection handle it
                                tprintln!(
                                    "{} Order age {}s but has FILLS ({}) - skipping cancellation, waiting for fill detection",
                                    "[MONITOR]".yellow().bold(),
                                    format!("{:.3}", age_ms as f64 / 1000.0).bright_white(),
                                    filled_amount
                                );
                                continue;
                            }
                        } else {
                            // Order not in open orders anymore (filled or already cancelled)
                            debug!("[MONITOR] Order not in open orders, might be filled/cancelled already");
                            continue;
                        }
                    }
                    Err(e) => {
                        debug!("[MONITOR] Failed to check order fills before age cancellation: {}", e);
                        // Continue with cancellation attempt (safer than leaving hanging orders)
                    }
                }

                tprintln!(
                    "{} Order age {}s (max {}s) | PAC: {} | HL: {}/{} → {}",
                    "[MONITOR]".yellow().bold(),
                    format!("{:.3}", age_ms as f64 / 1000.0).bright_white(),
                    self.config.order_refresh_interval_secs,
                    format!("${:.4}", active_order.price).cyan(),
                    format!("${:.4}", hl_bid).cyan(),
                    format!("${:.4}", hl_ask).cyan(),
                    "Refreshing".yellow()
                );

                // Cancel all orders for this symbol (only if no fills detected above)
                let cancel_result = self.pacifica_trading
                    .cancel_all_orders(false, Some(&active_order.symbol), false)
                    .await;

                match cancel_result {
                    Ok(_) => {
                        // Success - reset rate limit tracking
                        cancel_rate_limit.record_success();
                    }
                    Err(e) => {
                        // Check if it's a rate limit error
                        if is_rate_limit_error(&e) {
                            cancel_rate_limit.record_error();
                            let backoff_secs = cancel_rate_limit.get_backoff_secs();
                            tprintln!(
                                "{} {} Failed to cancel: Rate limit exceeded. Backing off for {}s (attempt #{})",
                                "[MONITOR]".yellow().bold(),
                                "⚠".yellow().bold(),
                                backoff_secs,
                                cancel_rate_limit.consecutive_errors()
                            );
                        } else {
                            // Other error - log but don't backoff
                            tprintln!("{} {} Failed to cancel order: {}", "[MONITOR]".yellow().bold(), "✗".red().bold(), e);
                        }

                        // Check if order is still active (might have been filled/cancelled)
                        let state = self.bot_state.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        continue;
                    }
                }

                // Clear active order ONLY if not filled/hedging
                let mut state = self.bot_state.write().await;
                match &state.status {
                    BotStatus::OrderPlaced => {
                        // Normal cancellation - safe to clear
                        state.clear_active_order();
                    }
                    BotStatus::Filled | BotStatus::Hedging | BotStatus::Complete => {
                        // Order was filled during cancellation - DO NOT reset to Idle
                        debug!("[MONITOR] Order age check: state is {:?}, not clearing", state.status);
                    }
                    _ => {}
                }
                drop(state);

                // Force fresh price fetch from REST API after cancellation (both exchanges)
                // Pacifica REST API
                {
                    match self.pacifica_trading.get_best_bid_ask_rest(&self.config.symbol, self.config.agg_level).await {
                        Ok(Some((bid, ask))) => {
                            *self.pacifica_prices.lock().unwrap() = (bid, ask);
                            tprintln!(
                                "{} Refreshed Pacifica via REST: bid={}, ask={}",
                                "[MONITOR]".yellow().bold(),
                                format!("${:.6}", bid).cyan(),
                                format!("${:.6}", ask).cyan()
                            );
                        }
                        Ok(None) => {
                            debug!("[MONITOR] No Pacifica bid/ask available from REST API");
                        }
                        Err(e) => {
                            debug!("[MONITOR] Failed to fetch Pacifica prices: {}", e);
                        }
                    }
                }

                // Hyperliquid REST API
                {
                    match self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
                        Ok(Some((bid, ask))) => {
                            *self.hyperliquid_prices.lock().unwrap() = (bid, ask);
                            tprintln!(
                                "{} Refreshed Hyperliquid via REST: bid={}, ask={}",
                                "[MONITOR]".yellow().bold(),
                                format!("${:.6}", bid).cyan(),
                                format!("${:.6}", ask).cyan()
                            );
                        }
                        Ok(None) => {
                            debug!("[MONITOR] No Hyperliquid bid/ask available from REST API");
                        }
                        Err(e) => {
                            debug!("[MONITOR] Failed to fetch Hyperliquid prices: {}", e);
                        }
                    }
                }

                continue;
            }

            // Check 2: Profit monitoring (cancel if drops > profit_cancel_threshold_bps)
            let (hl_bid, hl_ask) = *self.hyperliquid_prices.lock().unwrap();

            if hl_bid == 0.0 || hl_ask == 0.0 {
                continue;
            }

            // Create a temporary opportunity to recalculate profit
            let temp_opp = Opportunity {
                direction: active_order.side,
                pacifica_price: active_order.price,
                hyperliquid_price: 0.0, // Not used in recalculation
                size: active_order.size,
                initial_profit_bps: active_order.initial_profit_bps,
                timestamp: 0,
            };

            let current_profit = self.evaluator.recalculate_profit(&temp_opp, hl_bid, hl_ask);
            let profit_change = active_order.initial_profit_bps - current_profit;
            let profit_deviation = profit_change.abs();

            if profit_deviation > self.config.profit_cancel_threshold_bps {
                // Check if we're in rate limit backoff period
                if cancel_rate_limit.should_skip() {
                    let remaining = cancel_rate_limit.remaining_backoff_secs();
                    debug!("[MONITOR] Skipping profit cancellation (rate limit backoff, {:.1}s remaining)", remaining);
                    continue;
                }

                let hedge_price = match active_order.side {
                    OrderSide::Buy => hl_bid,
                    OrderSide::Sell => hl_ask,
                };

                let age_ms = active_order.placed_at.elapsed().as_millis();
                let change_direction = if profit_change > 0.0 { "dropped" } else { "increased" };
                // *** CRITICAL FIX: Check if order has fills BEFORE cancelling ***
                // If order is filled but WebSocket missed it, don't cancel - let REST fill detection handle it
                let filled_check_result = self.pacifica_trading.get_open_orders().await;

                match filled_check_result {
                    Ok(orders) => {
                        if let Some(order) = orders.iter().find(|o| o.client_order_id == active_order.client_order_id) {
                            let filled_amount: f64 = parse(&order.filled_amount).unwrap_or(0.0);

                            if filled_amount > 0.0 {
                                // Order has fills - DON'T cancel, let fill detection handle it
                                tprintln!(
                                    "{} Profit deviation {:.2} bps but order has FILLS ({}) - skipping cancellation",
                                    "[MONITOR]".yellow().bold(),
                                    profit_deviation,
                                    filled_amount
                                );
                                continue;
                            }
                        } else {
                            // Order not in open orders anymore (filled or already cancelled)
                            debug!("[MONITOR] Order not in open orders during profit check, might be filled/cancelled");
                            continue;
                        }
                    }
                    Err(e) => {
                        debug!("[MONITOR] Failed to check order fills before profit cancellation: {}", e);
                        // Continue with cancellation attempt (safer than leaving hanging orders)
                    }
                }

                tprintln!(
                    "{} Profit: {} bps ({} {}) | PAC: {} | HL: {} | Age: {}s → {}",
                    "[MONITOR]".yellow().bold(),
                    if profit_change > 0.0 { format!("{:.2}", current_profit).red() } else { format!("{:.2}", current_profit).green() },
                    change_direction,
                    format!("{:.2}", profit_deviation).bright_white(),
                    format!("${:.4}", active_order.price).cyan(),
                    format!("${:.4}", hedge_price).cyan(),
                    format!("{:.3}", age_ms as f64 / 1000.0).bright_white(),
                    "Cancelling".yellow()
                );

                // Cancel all orders for this symbol (only if no fills detected above)
                let cancel_result = self.pacifica_trading
                    .cancel_all_orders(false, Some(&active_order.symbol), false)
                    .await;

                match cancel_result {
                    Ok(_) => {
                        // Success - reset rate limit tracking
                        cancel_rate_limit.record_success();
                    }
                    Err(e) => {
                        // Check if it's a rate limit error
                        if is_rate_limit_error(&e) {
                            cancel_rate_limit.record_error();
                            let backoff_secs = cancel_rate_limit.get_backoff_secs();
                            tprintln!(
                                "{} {} Failed to cancel: Rate limit exceeded. Backing off for {}s (attempt #{})",
                                "[MONITOR]".yellow().bold(),
                                "⚠".yellow().bold(),
                                backoff_secs,
                                cancel_rate_limit.consecutive_errors()
                            );
                        } else {
                            // Other error - log but don't backoff
                            tprintln!("{} {} Failed to cancel order: {}", "[MONITOR]".yellow().bold(), "✗".red().bold(), e);
                        }

                        // Check if order is still active (might have been filled/cancelled)
                        let state = self.bot_state.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        continue;
                    }
                }

                // Clear active order ONLY if not filled/hedging
                let mut state = self.bot_state.write().await;
                match &state.status {
                    BotStatus::OrderPlaced => {
                        // Normal cancellation - safe to clear
                        state.clear_active_order();
                    }
                    BotStatus::Filled | BotStatus::Hedging | BotStatus::Complete => {
                        // Order was filled during cancellation - DO NOT reset to Idle
                        debug!("[MONITOR] Profit check: state is {:?}, not clearing", state.status);
                    }
                    _ => {}
                }
                drop(state);

                // Force fresh price fetch from REST API after cancellation (both exchanges)
                // Pacifica REST API
                {
                    match self.pacifica_trading.get_best_bid_ask_rest(&self.config.symbol, self.config.agg_level).await {
                        Ok(Some((bid, ask))) => {
                            *self.pacifica_prices.lock().unwrap() = (bid, ask);
                            tprintln!(
                                "{} Refreshed Pacifica via REST: bid={}, ask={}",
                                "[MONITOR]".yellow().bold(),
                                format!("${:.6}", bid).cyan(),
                                format!("${:.6}", ask).cyan()
                            );
                        }
                        Ok(None) => {
                            debug!("[MONITOR] No Pacifica bid/ask available from REST API");
                        }
                        Err(e) => {
                            debug!("[MONITOR] Failed to fetch Pacifica prices: {}", e);
                        }
                    }
                }

                // Hyperliquid REST API
                {
                    match self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
                        Ok(Some((bid, ask))) => {
                            *self.hyperliquid_prices.lock().unwrap() = (bid, ask);
                            tprintln!(
                                "{} Refreshed Hyperliquid via REST: bid={}, ask={}",
                                "[MONITOR]".yellow().bold(),
                                format!("${:.6}", bid).cyan(),
                                format!("${:.6}", ask).cyan()
                            );
                        }
                        Ok(None) => {
                            debug!("[MONITOR] No Hyperliquid bid/ask available from REST API");
                        }
                        Err(e) => {
                            debug!("[MONITOR] Failed to fetch Hyperliquid prices: {}", e);
                        }
                    }
                }
            }
                }

                _ = log_interval.tick() => {
                    // Periodic profit logging every 2 seconds
                    let state = self.bot_state.read().await;

                    // Only log profit for actively placed orders
                    if !matches!(state.status, BotStatus::OrderPlaced) {
                        continue;
                    }

                    let active_order = match &state.active_order {
                        Some(order) => order.clone(),
                        None => continue,
                    };
                    drop(state);

                    let (hl_bid, hl_ask) = *self.hyperliquid_prices.lock().unwrap();
                    if hl_bid == 0.0 || hl_ask == 0.0 {
                        continue;
                    }

                    let temp_opp = Opportunity {
                        direction: active_order.side,
                        pacifica_price: active_order.price,
                        hyperliquid_price: 0.0,
                        size: active_order.size,
                        initial_profit_bps: active_order.initial_profit_bps,
                        timestamp: 0,
                    };

                    let current_profit = self.evaluator.recalculate_profit(&temp_opp, hl_bid, hl_ask);
                    let profit_change = current_profit - active_order.initial_profit_bps;
                    let age_ms = active_order.placed_at.elapsed().as_millis();

                    let hedge_price = match active_order.side {
                        OrderSide::Buy => hl_bid,
                        OrderSide::Sell => hl_ask,
                    };

                    tprintln!(
                        "{} Current: {} bps (initial: {}, change: {}) | PAC: {} | HL: {} | Age: {}s",
                        "[PROFIT]".bright_blue().bold(),
                        format!("{:.2}", current_profit).bright_white().bold(),
                        format!("{:.2}", active_order.initial_profit_bps).bright_white(),
                        if profit_change >= 0.0 { format!("{:+.2}", profit_change).green() } else { format!("{:+.2}", profit_change).red() },
                        format!("${:.4}", active_order.price).cyan(),
                        format!("${:.4}", hedge_price).cyan(),
                        format!("{:.3}", age_ms as f64 / 1000.0).bright_white()
                    );
                }
            }
        }
    }
}
