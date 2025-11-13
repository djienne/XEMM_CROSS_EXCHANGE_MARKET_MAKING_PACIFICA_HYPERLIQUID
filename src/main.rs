use anyhow::{Context, Result};
use colored::Colorize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, info, warn};
use tokio::signal;

use xemm_rust::app::PositionSnapshot;
use xemm_rust::bot::{ActiveOrder, BotStatus};
use xemm_rust::trade_fetcher;
use xemm_rust::util::cancel::dual_cancel;
use xemm_rust::util::rate_limit::{is_rate_limit_error, RateLimitTracker};

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}
use xemm_rust::connector::hyperliquid::{
    OrderbookClient as HyperliquidOrderbookClient,
    OrderbookConfig as HyperliquidOrderbookConfig,
};
use xemm_rust::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent,
    OrderbookClient as PacificaOrderbookClient, OrderbookConfig as PacificaOrderbookConfig, OrderSide as PacificaOrderSide,
    PacificaTrading,
};
use xemm_rust::strategy::{Opportunity, OpportunityEvaluator, OrderSide};

/// XEMM Bot - Cross-Exchange Market Making Bot
///
/// Single-cycle arbitrage bot that:
/// 1. Evaluates opportunities between Pacifica and Hyperliquid
/// 2. Places limit order on Pacifica
/// 3. Monitors profitability (cancels if profit drops >3 bps)
/// 4. Auto-refreshes orders based on configured interval (default 60 seconds)
/// 5. Hedges on Hyperliquid when filled (WebSocket + REST API detection)
/// 6. Hedges partial fills above $10 notional
/// 7. Exits after successful hedge
///
/// Tasks:
/// 1. Pacifica Orderbook (WebSocket - real-time push)
/// 2. Hyperliquid Orderbook (WebSocket - 99ms request/response)
/// 3. Fill Detection (WebSocket - primary, real-time)
/// 4. Pacifica REST Polling (orderbook fallback, every 2s)
/// 4.5. Hyperliquid REST Polling (orderbook fallback, every 2s)
/// 5. REST API Fill Detection (backup, 500ms polling with rate limit handling)
/// 5.5. Position Monitor (4th layer, ground truth, 500ms polling)
/// 6. Order Monitoring (profit/age checks, every 25ms)
/// 7. Hedge Execution
/// 8. Main Opportunity Loop (every 100ms)
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Create and initialize bot (all wiring happens in XemmBot::new())
    let bot = xemm_rust::app::XemmBot::new().await?;

    // Get references to variables needed for the remaining code
    let config = &bot.config;
    let bot_state = bot.bot_state.clone();
    let pacifica_prices = bot.pacifica_prices.clone();
    let hyperliquid_prices = bot.hyperliquid_prices.clone();
    let pacifica_trading_main = bot.pacifica_trading_main.clone();
    let pacifica_trading_fill = bot.pacifica_trading_fill.clone();
    let pacifica_trading_rest_fill = bot.pacifica_trading_rest_fill.clone();
    let pacifica_trading_monitor = bot.pacifica_trading_monitor.clone();
    let pacifica_trading_hedge = bot.pacifica_trading_hedge.clone();
    let pacifica_trading_rest_poll = bot.pacifica_trading_rest_poll.clone();
    let pacifica_ws_trading = bot.pacifica_ws_trading.clone();
    let hyperliquid_trading = bot.hyperliquid_trading.clone();
    let evaluator = bot.evaluator.clone();
    let processed_fills = bot.processed_fills.clone();
    let last_position_snapshot = bot.last_position_snapshot.clone();
    let hedge_tx = bot.hedge_tx.clone();
    let mut hedge_rx = bot.hedge_rx.unwrap();
    let shutdown_tx = bot.shutdown_tx.clone();
    let mut shutdown_rx = bot.shutdown_rx.unwrap();
    let pacifica_credentials = bot.pacifica_credentials.clone();

    // ═══════════════════════════════════════════════════
    // Task 1: Pacifica Orderbook
    // ═══════════════════════════════════════════════════

    let pac_prices_clone = pacifica_prices.clone();
    let pacifica_ob_config = PacificaOrderbookConfig {
        symbol: config.symbol.clone(),
        agg_level: config.agg_level,
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };

    let mut pacifica_ob_client = PacificaOrderbookClient::new(pacifica_ob_config)
        .context("Failed to create Pacifica orderbook client")?;

    tokio::spawn(async move {
        tprintln!("{} Starting orderbook client", "[PACIFICA_OB]".magenta().bold());
        pacifica_ob_client
            .start(move |bid, ask, _symbol, _ts| {
                let bid_price: f64 = bid.parse().unwrap_or(0.0);
                let ask_price: f64 = ask.parse().unwrap_or(0.0);
                *pac_prices_clone.lock().unwrap() = (bid_price, ask_price);
            })
            .await
            .ok();
    });

    // ═══════════════════════════════════════════════════
    // Task 2: Hyperliquid Orderbook
    // ═══════════════════════════════════════════════════

    let hl_prices_clone = hyperliquid_prices.clone();
    let hyperliquid_ob_config = HyperliquidOrderbookConfig {
        coin: config.symbol.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
        request_interval_ms: 99, // ~10 Hz updates (10 req/s)
    };

    let mut hyperliquid_ob_client = HyperliquidOrderbookClient::new(hyperliquid_ob_config)
        .context("Failed to create Hyperliquid orderbook client")?;

    tokio::spawn(async move {
        tprintln!("{} Starting orderbook client", "[HYPERLIQUID_OB]".magenta().bold());
        hyperliquid_ob_client
            .start(move |bid, ask, _coin, _ts| {
                let bid_price: f64 = bid.parse().unwrap_or(0.0);
                let ask_price: f64 = ask.parse().unwrap_or(0.0);
                *hl_prices_clone.lock().unwrap() = (bid_price, ask_price);
            })
            .await
            .ok();
    });

    // ═══════════════════════════════════════════════════
    // Task 3: Fill Detection
    // ═══════════════════════════════════════════════════

    let fill_config = FillDetectionConfig {
        account: pacifica_credentials.account.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
        enable_position_fill_detection: true,  // Enable position-based fill detection redundancy
    };

    let fill_client = FillDetectionClient::new(fill_config.clone(), false)
        .context("Failed to create fill detection client")?;

    // Get baseline updater BEFORE starting (to prevent duplicate hedge triggers)
    let baseline_updater = fill_client.get_baseline_updater();

    let fill_service = xemm_rust::services::fill_detection::FillDetectionService {
        bot_state: bot_state.clone(),
        hedge_tx: hedge_tx.clone(),
        pacifica_trading: pacifica_trading_fill.clone(),
        pacifica_ws_trading: pacifica_ws_trading.clone(),
        fill_config,
        symbol: config.symbol.clone(),
        processed_fills: processed_fills.clone(),
        baseline_updater,
    };

    tokio::spawn(async move {
        fill_service.run().await;
    });
    // ═══════════════════════════════════════════════════
    // Task 4: Pacifica REST API Polling (Complement WebSocket)
    // ═══════════════════════════════════════════════════

    let pac_prices_rest_clone = pacifica_prices.clone();
    // pacifica_trading_rest_poll already defined at top level (no clone needed)
    let rest_symbol = config.symbol.clone();
    let rest_agg_level = config.agg_level;
    let rest_poll_interval = config.pacifica_rest_poll_interval_secs;

    tokio::spawn(async move {
        let mut interval_timer = interval(Duration::from_secs(rest_poll_interval));
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;

            match pacifica_trading_rest_poll.get_best_bid_ask_rest(&rest_symbol, rest_agg_level).await {
                Ok(Some((bid, ask))) => {
                    // Update shared orderbook prices
                    *pac_prices_rest_clone.lock().unwrap() = (bid, ask);
                    debug!(
                        "[PACIFICA_REST] Updated prices via REST: bid=${:.4}, ask=${:.4}",
                        bid, ask
                    );
                }
                Ok(None) => {
                    debug!("[PACIFICA_REST] No bid/ask available from REST API");
                }
                Err(e) => {
                    debug!("[PACIFICA_REST] Failed to fetch prices: {}", e);
                }
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // Task 4.5: Hyperliquid REST API Polling (Complement WebSocket)
    // ═══════════════════════════════════════════════════

    let hl_prices_rest_clone = hyperliquid_prices.clone();
    let hyperliquid_trading_rest_clone = hyperliquid_trading.clone();
    let hl_rest_symbol = config.symbol.clone();
    let hl_rest_poll_interval = 2u64; // 2 seconds (same as Pacifica)

    tokio::spawn(async move {
        let mut interval_timer = interval(Duration::from_secs(hl_rest_poll_interval));
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;

            match hyperliquid_trading_rest_clone.get_l2_snapshot(&hl_rest_symbol).await {
                Ok(Some((bid, ask))) => {
                    // Update shared orderbook prices
                    *hl_prices_rest_clone.lock().unwrap() = (bid, ask);
                    debug!(
                        "[HYPERLIQUID_REST] Updated prices via REST: bid=${:.4}, ask=${:.4}",
                        bid, ask
                    );
                }
                Ok(None) => {
                    debug!("[HYPERLIQUID_REST] No bid/ask available from REST API");
                }
                Err(e) => {
                    debug!("[HYPERLIQUID_REST] Failed to fetch prices: {}", e);
                }
            }
        }
    });

    // Wait for initial orderbook data
    tprintln!("{} Waiting for orderbook data...", "[INIT]".cyan().bold());
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ═══════════════════════════════════════════════════
    // Task 5: REST API Fill Detection (Backup/Complement WebSocket)
    // ═══════════════════════════════════════════════════

    let rest_fill_service = xemm_rust::services::rest_fill_detection::RestFillDetectionService {
        bot_state: bot_state.clone(),
        hedge_tx: hedge_tx.clone(),
        pacifica_trading: pacifica_trading_rest_fill.clone(),
        pacifica_ws_trading: pacifica_ws_trading.clone(),
        symbol: config.symbol.clone(),
        processed_fills: processed_fills.clone(),
        min_hedge_notional: 10.0, // Same as partial fill threshold
    };

    tokio::spawn(async move {
        rest_fill_service.run().await;
    });

    // Small delay to let REST fill detection initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ═══════════════════════════════════════════════════
    // Task 5.5: Position-Based Fill Detection (4th Layer - Ground Truth)
    // ═══════════════════════════════════════════════════

    let pacifica_trading_position = Arc::new(
        PacificaTrading::new(pacifica_credentials.clone())
            .context("Failed to create position monitor trading client")?
    );

    let position_monitor_service = xemm_rust::services::position_monitor::PositionMonitorService {
        bot_state: bot_state.clone(),
        hedge_tx: hedge_tx.clone(),
        pacifica_trading: pacifica_trading_position,
        pacifica_ws_trading: pacifica_ws_trading.clone(),
        symbol: config.symbol.clone(),
        processed_fills: processed_fills.clone(),
        last_position_snapshot: last_position_snapshot.clone(),
    };

    tokio::spawn(async move {
        position_monitor_service.run().await;
    });

    // Small delay to let position monitor initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ═══════════════════════════════════════════════════
    // Task 6: Order Monitoring (Profit Check & Refresh)
    // ═══════════════════════════════════════════════════


    let order_monitor_service = xemm_rust::services::order_monitor::OrderMonitorService {
        bot_state: bot_state.clone(),
        pacifica_prices: pacifica_prices.clone(),
        hyperliquid_prices: hyperliquid_prices.clone(),
        config: config.clone(),
        evaluator: evaluator.clone(),
        pacifica_trading: pacifica_trading_monitor.clone(),
        hyperliquid_trading: Arc::clone(&hyperliquid_trading),
    };

    tokio::spawn(async move {
        order_monitor_service.run().await;
    });

    // ═══════════════════════════════════════════════════
    // ═══════════════════════════════════════════════════
    // Task 7: Hedge Execution
    // ═══════════════════════════════════════════════════

    let bot_state_hedge = bot_state.clone();
    let hl_prices_hedge = hyperliquid_prices.clone();
    let config_hedge = config.clone();
    let hl_trading_hedge = Arc::clone(&hyperliquid_trading);
    // pacifica_trading_hedge already defined at top level (no clone needed)
    let shutdown_tx_hedge = shutdown_tx.clone();

    tokio::spawn(async move {
        while let Some((side, size, avg_price)) = hedge_rx.recv().await {
            tprintln!("{} Received trigger: {} {} @ {}",
                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                side.as_str().bright_yellow(),
                size,
                format!("${:.4}", avg_price).cyan()
            );

            // *** CRITICAL: CANCEL ALL ORDERS BEFORE HEDGE ***
            // Extra safety: cancel again in case fill detection missed anything
            // or there was a race condition
            tprintln!("{} {} Pre-hedge safety: Cancelling all Pacifica orders...",
                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                "⚡".yellow().bold()
            );

            if let Err(e) = pacifica_trading_hedge
                .cancel_all_orders(false, Some(&config_hedge.symbol), false)
                .await
            {
                tprintln!("{} {} Failed to cancel orders before hedge: {}",
                    format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                    "⚠".yellow().bold(),
                    e
                );
            } else {
                tprintln!("{} {} Pre-hedge cancellation complete",
                    format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                    "✓".green().bold()
                );
            }

            // Update status
            {
                let mut state = bot_state_hedge.write().await;
                state.mark_hedging();
            }

            // Execute opposite direction on Hyperliquid
            let is_buy = match side {
                OrderSide::Buy => false, // Filled buy on Pacifica → sell on Hyperliquid
                OrderSide::Sell => true, // Filled sell on Pacifica → buy on Hyperliquid
            };

            let (hl_bid, hl_ask) = *hl_prices_hedge.lock().unwrap();

            tprintln!(
                "{} Executing {} {} on Hyperliquid",
                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                if is_buy { "BUY".green().bold() } else { "SELL".red().bold() },
                size
            );

            let hedge_result = hl_trading_hedge
                .place_market_order(
                    &config_hedge.symbol,
                    is_buy,
                    size,
                    config_hedge.hyperliquid_slippage,
                    false, // reduce_only
                    Some(hl_bid),
                    Some(hl_ask),
                )
                .await;

            match hedge_result {
                Ok(response) => {
                    tprintln!("{} {} Hedge executed successfully",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "✓".green().bold()
                    );

                    // Extract hedge fill price from response
                    let hedge_fill_price = if let Some(status) = response.response.data.statuses.first() {
                        match status {
                            xemm_rust::connector::hyperliquid::OrderStatus::Filled { filled } => {
                                filled.avgPx.parse::<f64>().ok()
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };

                    // Get expected profit from active order before marking complete
                    let expected_profit_bps = {
                        let state = bot_state_hedge.read().await;
                        state.active_order.as_ref().map(|o| o.initial_profit_bps)
                    };

                    // Wait for trades to propagate to exchange APIs (20 seconds)
                    tprintln!("{} Waiting 20 seconds for trades to propagate to APIs...",
                        format!("[{} PROFIT]", config_hedge.symbol).bright_blue().bold()
                    );
                    tokio::time::sleep(Duration::from_secs(20)).await;

                    // Get client_order_id from bot state
                    let client_order_id = {
                        let state = bot_state_hedge.read().await;
                        state.active_order.as_ref().map(|o| o.client_order_id.clone())
                    };

                    // Fetch Pacifica trade history with retry logic
                    let (pacifica_fill_price, pacifica_actual_fee, pacifica_notional): (Option<f64>, Option<f64>, Option<f64>) = if let Some(cloid) = &client_order_id {
                        let result = trade_fetcher::fetch_pacifica_trade(
                            pacifica_trading_hedge.clone(),
                            &config_hedge.symbol,
                            &cloid,
                            3, // max_attempts
                            |msg| {
                                tprintln!("{} {}",
                                    format!("[{} PROFIT]", config_hedge.symbol).bright_blue().bold(),
                                    msg
                                );
                            }
                        ).await;
                        (result.fill_price, result.actual_fee, result.total_notional)
                    } else {
                        (None, None, None)
                    };

                    // Fetch Hyperliquid user fills with retry logic
                    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
                    let (hl_fill_price, hl_actual_fee, hl_notional): (Option<f64>, Option<f64>, Option<f64>) = {
                        let result = trade_fetcher::fetch_hyperliquid_fills(
                            &hl_trading_hedge,
                            &hl_wallet,
                            &config_hedge.symbol,
                            3, // max_attempts
                            30, // time_window_secs
                            |msg| {
                                tprintln!("{} {}",
                                    format!("[{} PROFIT]", config_hedge.symbol).bright_blue().bold(),
                                    msg
                                );
                            }
                        ).await;
                        (result.fill_price, result.actual_fee, result.total_notional)
                    };

                    // Calculate actual profitability using real fill data and actual fees
                    let (actual_profit_bps, actual_profit_usd, pacifica_actual_price, hl_actual_price, pac_fee_usd, hl_fee_usd) =
                        match (pacifica_notional, hl_notional, pacifica_fill_price, hl_fill_price) {
                            (Some(pac_notional), Some(hl_notional), pac_price_opt, hl_price_opt) => {
                                // Use ACTUAL notional values from exchanges (not recalculated!)
                                // This handles multi-fill trades correctly and avoids Pacifica API bugs

                                // Use actual fees from trade history, or fall back to theoretical
                                let pac_fee = pacifica_actual_fee.unwrap_or_else(|| {
                                    // Fallback: 1.5 bps on notional
                                    pac_notional * (config_hedge.pacifica_maker_fee_bps / 10000.0)
                                });

                                let hl_fee = hl_actual_fee.unwrap_or_else(|| {
                                    // Fallback: 4 bps on notional
                                    hl_notional * (config_hedge.hyperliquid_taker_fee_bps / 10000.0)
                                });

                                // Use the shared profit calculation function (same as test utility!)
                                let is_pacifica_buy = matches!(side, OrderSide::Buy);
                                let profit = trade_fetcher::calculate_hedge_profit(
                                    pac_notional,
                                    hl_notional,
                                    pac_fee,
                                    hl_fee,
                                    is_pacifica_buy,
                                );

                                (profit.profit_bps, profit.net_profit, pac_price_opt, hl_price_opt, pac_fee, hl_fee)
                            }
                            _ => {
                                // Fallback to fill event data if trade history unavailable
                                tprintln!("{} {} Using fill event data (trade history unavailable)",
                                    format!("[{} PROFIT]", config_hedge.symbol).bright_blue().bold(),
                                    "⚠".yellow().bold()
                                );

                                // Calculate profit using fill event prices and estimated fees
                                if let Some(hl_price) = hedge_fill_price {
                                    let pac_price = avg_price;

                                    // Estimate fees using configured rates
                                    let pac_fee = pac_price * size * (config_hedge.pacifica_maker_fee_bps / 10000.0);
                                    let hl_fee = hl_price * size * (config_hedge.hyperliquid_taker_fee_bps / 10000.0);

                                    // Calculate profit
                                    let (profit_usd, cost, _revenue) = match side {
                                        OrderSide::Buy => {
                                            // Bought on Pacifica (maker), Sold on Hyperliquid (taker)
                                            let cost = (pac_price * size) + pac_fee;
                                            let revenue = (hl_price * size) - hl_fee;
                                            (revenue - cost, cost, revenue)
                                        }
                                        OrderSide::Sell => {
                                            // Sold on Pacifica (maker), Bought on Hyperliquid (taker)
                                            let revenue = (pac_price * size) - pac_fee;
                                            let cost = (hl_price * size) + hl_fee;
                                            (revenue - cost, cost, revenue)
                                        }
                                    };

                                    let profit_rate = if cost > 0.0 { profit_usd / cost } else { 0.0 };
                                    let profit_bps = profit_rate * 10000.0;

                                    (profit_bps, profit_usd, Some(pac_price), Some(hl_price), pac_fee, hl_fee)
                                } else {
                                    // No hedge price available at all
                                    (0.0, 0.0, Some(avg_price), None, 0.0, 0.0)
                                }
                            }
                        };

                    // Display comprehensive summary
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());
                    tprintln!("{}", "  BOT CYCLE COMPLETE!".green().bold());
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());
                    tprintln!("");
                    tprintln!("{}", "📊 TRADE SUMMARY:".bright_white().bold());
                    if let Some(pac_price) = pacifica_actual_price {
                        tprintln!("  {}: {} {} {} @ {} {}",
                            "Pacifica".bright_magenta(),
                            side.as_str().bright_yellow(),
                            format!("{:.4}", size).bright_white(),
                            config_hedge.symbol.bright_white().bold(),
                            format!("${:.6}", pac_price).cyan().bold(),
                            "(actual fill)".bright_black()
                        );
                    }
                    if let Some(hl_price) = hl_actual_price {
                        tprintln!("  {}: {} {} {} @ {} {}",
                            "Hyperliquid".bright_magenta(),
                            if is_buy { "BUY".green() } else { "SELL".red() },
                            format!("{:.4}", size).bright_white(),
                            config_hedge.symbol.bright_white().bold(),
                            format!("${:.6}", hl_price).cyan().bold(),
                            "(actual fill)".bright_black()
                        );
                    }
                    tprintln!("");
                    tprintln!("{}", "💰 PROFITABILITY:".bright_white().bold());
                    if let Some(expected) = expected_profit_bps {
                        tprintln!("  Expected: {} bps", format!("{:.2}", expected).bright_white());
                    }
                    if pacifica_actual_price.is_some() && hl_actual_price.is_some() {
                        let profit_color = if actual_profit_bps > 0.0 { format!("{:.2}", actual_profit_bps).green().bold() } else { format!("{:.2}", actual_profit_bps).red().bold() };
                        let usd_color = if actual_profit_usd > 0.0 { format!("${:.4}", actual_profit_usd).green().bold() } else { format!("${:.4}", actual_profit_usd).red().bold() };
                        tprintln!("  Actual:   {} bps ({})", profit_color, usd_color);
                        if let Some(expected) = expected_profit_bps {
                            let diff = actual_profit_bps - expected;
                            let diff_sign = if diff >= 0.0 { "+" } else { "" };
                            let diff_color = if diff >= 0.0 { format!("{}{:.2}", diff_sign, diff).green() } else { format!("{:.2}", diff).red() };
                            tprintln!("  Difference: {} bps", diff_color);
                        }
                    } else {
                        tprintln!("  {} Unable to calculate actual profit (trade history unavailable)", "⚠".yellow().bold());
                    }
                    tprintln!("");
                    tprintln!("{}", "📈 FEES:".bright_white().bold());
                    if pacifica_actual_price.is_some() && hl_actual_price.is_some() {
                        // Show actual fees paid
                        tprintln!("  Pacifica: {} {}",
                            format!("${:.4}", pac_fee_usd).yellow(),
                            if pacifica_actual_fee.is_some() { "(actual)" } else { "(estimated)" }.bright_black()
                        );
                        tprintln!("  Hyperliquid: {} {}",
                            format!("${:.4}", hl_fee_usd).yellow(),
                            if hl_actual_fee.is_some() { "(actual)" } else { "(estimated)" }.bright_black()
                        );
                        tprintln!("  Total: {}", format!("${:.4}", pac_fee_usd + hl_fee_usd).yellow().bold());
                    } else {
                        // Fallback to theoretical fees
                        tprintln!("  Pacifica (maker): {} bps", format!("{:.2}", config_hedge.pacifica_maker_fee_bps).yellow());
                        tprintln!("  Hyperliquid (taker): {} bps", format!("{:.2}", config_hedge.hyperliquid_taker_fee_bps).yellow());
                        tprintln!("  Total fees: {} bps", format!("{:.2}", config_hedge.pacifica_maker_fee_bps + config_hedge.hyperliquid_taker_fee_bps).yellow().bold());
                    }
                    tprintln!("{}", "═══════════════════════════════════════════════════".green().bold());

                    // *** CRITICAL: FINAL SAFETY CANCELLATION ***
                    // Cancel all orders one last time before marking complete
                    // This ensures no stray orders remain active
                    tprintln!("");
                    tprintln!("{} {} Post-hedge safety: Final cancellation of all Pacifica orders...",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    if let Err(e) = pacifica_trading_hedge
                        .cancel_all_orders(false, Some(&config_hedge.symbol), false)
                        .await
                    {
                        tprintln!("{} {} Failed to cancel orders after hedge completion: {}",
                            format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                    } else {
                        tprintln!("{} {} Final cancellation complete",
                            format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                            "✓".green().bold()
                        );
                    }

                    // *** POST-HEDGE POSITION VERIFICATION ***
                    // Wait for positions to propagate and verify net position is neutral
                    tprintln!("");
                    tprintln!("{} {} Post-hedge verification: Waiting 8 seconds for positions to propagate...",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "⏱".cyan().bold()
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;

                    tprintln!("{} Verifying final positions on both exchanges...",
                        format!("[{} VERIFY]", config_hedge.symbol).cyan().bold()
                    );

                    // Check Pacifica position
                    let pacifica_position = match pacifica_trading_hedge.get_positions().await {
                        Ok(positions) => {
                            if let Some(pos) = positions.iter().find(|p| p.symbol == config_hedge.symbol) {
                                let amount: f64 = pos.amount.parse().unwrap_or(0.0);
                                let signed_amount = if pos.side == "bid" { amount } else { -amount };

                                tprintln!("{} Pacifica: {} {} (signed: {:.4})",
                                    format!("[{} VERIFY]", config_hedge.symbol).cyan().bold(),
                                    amount,
                                    pos.side,
                                    signed_amount
                                );
                                Some(signed_amount)
                            } else {
                                tprintln!("{} Pacifica: No position (flat)",
                                    format!("[{} VERIFY]", config_hedge.symbol).cyan().bold()
                                );
                                Some(0.0)
                            }
                        }
                        Err(e) => {
                            tprintln!("{} {} Failed to fetch Pacifica position: {}",
                                format!("[{} VERIFY]", config_hedge.symbol).yellow().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                            None
                        }
                    };

                    // Check Hyperliquid position
                    let hl_wallet = std::env::var("HL_WALLET").unwrap_or_default();
                    let mut hyperliquid_position: Option<f64> = None;

                    // Try up to 3 times with delays if position not found
                    for retry in 0..3 {
                        if retry > 0 {
                            tprintln!("{} Retry {} - waiting 3 more seconds for Hyperliquid position...",
                                format!("[{} VERIFY]", config_hedge.symbol).cyan().bold(),
                                retry
                            );
                            tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        }

                        match hl_trading_hedge.get_user_state(&hl_wallet).await {
                            Ok(user_state) => {
                                if let Some(asset_pos) = user_state.asset_positions.iter().find(|ap| ap.position.coin == config_hedge.symbol) {
                                    let szi: f64 = asset_pos.position.szi.parse().unwrap_or(0.0);
                                    tprintln!("{} Hyperliquid: {} (signed: {:.4})",
                                        format!("[{} VERIFY]", config_hedge.symbol).cyan().bold(),
                                        if szi > 0.0 { "LONG".green() } else if szi < 0.0 { "SHORT".red() } else { "FLAT".bright_white() },
                                        szi
                                    );
                                    hyperliquid_position = Some(szi);
                                    break;
                                } else if retry == 2 {
                                    tprintln!("{} Hyperliquid: No position found after 3 attempts (flat)",
                                        format!("[{} VERIFY]", config_hedge.symbol).cyan().bold()
                                    );
                                    hyperliquid_position = Some(0.0);
                                }
                            }
                            Err(e) => {
                                if retry == 2 {
                                    tprintln!("{} {} Failed to fetch Hyperliquid position after 3 attempts: {}",
                                        format!("[{} VERIFY]", config_hedge.symbol).yellow().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                    hyperliquid_position = None;
                                }
                            }
                        }
                    }

                    // Calculate net position across both exchanges
                    if let (Some(pac_pos), Some(hl_pos)) = (pacifica_position, hyperliquid_position) {
                        let net_position = pac_pos + hl_pos;

                        tprintln!("");
                        tprintln!("{} Net Position: {:.4} (Pacifica: {:.4} + Hyperliquid: {:.4})",
                            format!("[{} VERIFY]", config_hedge.symbol).cyan().bold(),
                            net_position,
                            pac_pos,
                            hl_pos
                        );

                        // Check if net position is close to neutral
                        if net_position.abs() < 0.01 {
                            tprintln!("{} {} Net position is NEUTRAL (properly hedged across both exchanges)",
                                format!("[{} VERIFY]", config_hedge.symbol).cyan().bold(),
                                "✓".green().bold()
                            );
                        } else {
                            tprintln!("");
                            tprintln!("{}", "⚠".repeat(80).yellow());
                            tprintln!("{} {} WARNING: Net position NOT neutral!",
                                format!("[{} VERIFY]", config_hedge.symbol).red().bold(),
                                "⚠".yellow().bold()
                            );
                            tprintln!("{} Position delta: {:.4} {}",
                                format!("[{} VERIFY]", config_hedge.symbol).red().bold(),
                                net_position.abs(),
                                config_hedge.symbol
                            );
                            tprintln!("{} This indicates a potential hedge failure or partial fill.",
                                format!("[{} VERIFY]", config_hedge.symbol).red().bold()
                            );
                            tprintln!("{} Please check positions manually and rebalance if needed!",
                                format!("[{} VERIFY]", config_hedge.symbol).red().bold()
                            );
                            tprintln!("{}", "⚠".repeat(80).yellow());
                            tprintln!("");
                        }
                    } else {
                        tprintln!("");
                        tprintln!("{} {} WARNING: Could not verify net position!",
                            format!("[{} VERIFY]", config_hedge.symbol).yellow().bold(),
                            "⚠".yellow().bold()
                        );
                        tprintln!("{} Failed to fetch positions from one or both exchanges.",
                            format!("[{} VERIFY]", config_hedge.symbol).yellow().bold()
                        );
                        tprintln!("{} Please check positions manually!",
                            format!("[{} VERIFY]", config_hedge.symbol).yellow().bold()
                        );
                        tprintln!("");
                    }

                    // Mark cycle as complete AFTER displaying profit AND final cancellation
                    let mut state = bot_state_hedge.write().await;
                    state.mark_complete();
                    drop(state);

                    // Signal shutdown
                    shutdown_tx_hedge.send(()).await.ok();
                }
                Err(e) => {
                    tprintln!("{} {} Hedge failed: {}",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "✗".red().bold(),
                        e.to_string().red()
                    );

                    // *** CRITICAL: CANCEL ALL ORDERS ON ERROR ***
                    // Even if hedge fails, cancel all orders to prevent stray positions
                    tprintln!("{} {} Error recovery: Cancelling all Pacifica orders...",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    if let Err(cancel_err) = pacifica_trading_hedge
                        .cancel_all_orders(false, Some(&config_hedge.symbol), false)
                        .await
                    {
                        tprintln!("{} {} Failed to cancel orders after hedge error: {}",
                            format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            cancel_err
                        );
                    } else {
                        tprintln!("{} {} Error recovery cancellation complete",
                            format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                            "✓".green().bold()
                        );
                    }

                    let mut state = bot_state_hedge.write().await;
                    state.set_error(format!("Hedge failed: {}", e));

                    // Signal shutdown with error
                    shutdown_tx_hedge.send(()).await.ok();
                }
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // Task 8: Main Opportunity Evaluation Loop
    // ═══════════════════════════════════════════════════

    tprintln!("{} Starting opportunity evaluation loop",
        format!("[{} MAIN]", config.symbol).bright_white().bold()
    );
    tprintln!("");

    let mut eval_interval = interval(Duration::from_millis(100)); // Evaluate every 100ms

    // Rate limit tracking for order placement
    let mut order_placement_rate_limit = RateLimitTracker::new();

    // Helper async function to wait for SIGTERM (Docker shutdown)
    async fn wait_for_sigterm() {
        #[cfg(unix)]
        {
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to setup SIGTERM handler");
            sigterm.recv().await;
        }

        #[cfg(not(unix))]
        {
            // On non-Unix platforms, never resolve (only Ctrl+C will work)
            std::future::pending::<()>().await;
        }
    }

    loop {
        tokio::select! {
            _ = signal::ctrl_c() => {
                tprintln!("{} {} Received SIGINT (Ctrl+C), initiating graceful shutdown...",
                    format!("[{} MAIN]", config.symbol).bright_white().bold(),
                    "⚠".yellow().bold()
                );
                break;
            }

            _ = wait_for_sigterm() => {
                tprintln!("{} {} Received SIGTERM (Docker shutdown), initiating graceful shutdown...",
                    format!("[{} MAIN]", config.symbol).bright_white().bold(),
                    "⚠".yellow().bold()
                );
                break;
            }

            _ = eval_interval.tick() => {
                // Check if we should exit
                let state = bot_state.read().await;
                if state.is_terminal() {
                    break;
                }

                // Only evaluate if idle
                if !state.is_idle() {
                    continue;
                }

                // Check if grace period has elapsed (3 seconds after cancellation - gives REST time to detect fills)
                if !state.grace_period_elapsed(3) {
                    continue;
                }
                drop(state);

                // Check if we're in rate limit backoff period for order placement
                if order_placement_rate_limit.should_skip() {
                    let remaining = order_placement_rate_limit.remaining_backoff_secs();
                    // Only log occasionally to avoid spam
                    if remaining as u64 % 5 == 0 || remaining < 1.0 {
                        debug!("[MAIN] Skipping order placement (rate limit backoff, {:.1}s remaining)", remaining);
                    }
                    continue;
                }

                // Get current prices
                let (pac_bid, pac_ask) = *pacifica_prices.lock().unwrap();
                let (hl_bid, hl_ask) = *hyperliquid_prices.lock().unwrap();

                // Validate prices
                if pac_bid == 0.0 || pac_ask == 0.0 || hl_bid == 0.0 || hl_ask == 0.0 {
                    continue;
                }

                // Evaluate both directions
                let buy_opp = evaluator.evaluate_buy_opportunity(hl_bid, config.order_notional_usd);
                let sell_opp = evaluator.evaluate_sell_opportunity(hl_ask, config.order_notional_usd);

                // Pick best opportunity
                let pac_mid = (pac_bid + pac_ask) / 2.0;
                let best_opp = OpportunityEvaluator::pick_best_opportunity(buy_opp, sell_opp, pac_mid);

                if let Some(opp) = best_opp {
                    // Verify bot is still idle (double-check for race conditions)
                    let mut state = bot_state.write().await;
                    if !state.is_idle() {
                        continue;
                    }

                    tprintln!(
                        "{} {} @ {} → HL {} | Size: {} | Profit: {} | PAC: {}/{} | HL: {}/{}",
                        format!("[{} OPPORTUNITY]", config.symbol).bright_green().bold(),
                        opp.direction.as_str().bright_yellow().bold(),
                        format!("${:.6}", opp.pacifica_price).cyan().bold(),
                        format!("${:.6}", opp.hyperliquid_price).cyan(),
                        format!("{:.4}", opp.size).bright_white(),
                        format!("{:.2} bps", opp.initial_profit_bps).green().bold(),
                        format!("${:.6}", pac_bid).cyan(),
                        format!("${:.6}", pac_ask).cyan(),
                        format!("${:.6}", hl_bid).cyan(),
                        format!("${:.6}", hl_ask).cyan()
                    );

                    // Place order on Pacifica
                    tprintln!("{} Placing {} on Pacifica...",
                        format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                        opp.direction.as_str().bright_yellow().bold()
                    );

                    // Convert OrderSide from strategy to pacifica
                    let pacifica_side = match opp.direction {
                        OrderSide::Buy => PacificaOrderSide::Buy,
                        OrderSide::Sell => PacificaOrderSide::Sell,
                    };

                    match pacifica_trading_main
                        .place_limit_order(
                            &config.symbol,
                            pacifica_side,
                            opp.size,
                            Some(opp.pacifica_price),
                            0.0, // No mid price offset
                            Some(pac_bid),
                            Some(pac_ask),
                        )
                        .await
                    {
                        Ok(order_data) => {
                            // Success - reset rate limit tracking
                            order_placement_rate_limit.record_success();

                            // Extract client_order_id from response
                            if let Some(client_order_id) = order_data.client_order_id {
                                let order_id = order_data.order_id.unwrap_or(0);
                                tprintln!(
                                    "{} {} Placed {} #{} @ {} | cloid: {}...{}",
                                    format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                    "✓".green().bold(),
                                    opp.direction.as_str().bright_yellow(),
                                    order_id,
                                    format!("${:.4}", opp.pacifica_price).cyan().bold(),
                                    &client_order_id[..8],
                                    &client_order_id[client_order_id.len()-4..]
                                );

                                // Update state
                                let active_order = ActiveOrder {
                                    client_order_id,
                                    symbol: config.symbol.clone(),
                                    side: opp.direction,
                                    price: opp.pacifica_price,
                                    size: opp.size,
                                    initial_profit_bps: opp.initial_profit_bps,
                                    placed_at: Instant::now(),
                                };

                                state.set_active_order(active_order);
                            } else {
                                tprintln!("{} {} Order placed but no client_order_id returned",
                                    format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                    "✗".red().bold()
                                );
                            }
                        }
                        Err(e) => {
                            // Check if it's a rate limit error
                            if is_rate_limit_error(&e) {
                                order_placement_rate_limit.record_error();
                                let backoff_secs = order_placement_rate_limit.get_backoff_secs();
                                tprintln!(
                                    "{} {} Failed to place order: Rate limit exceeded. Backing off for {}s (attempt #{})",
                                    format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                    "⚠".yellow().bold(),
                                    backoff_secs,
                                    order_placement_rate_limit.consecutive_errors()
                                );
                            } else {
                                // Other error - log but don't backoff
                                tprintln!("{} {} Failed to place order: {}",
                                    format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                    "✗".red().bold(),
                                    e.to_string().red()
                                );
                            }
                            // Stay in Idle state to try again
                        }
                    }
                }
            }

            _ = shutdown_rx.recv() => {
                tprintln!("{} Shutdown signal received",
                    format!("[{} MAIN]", config.symbol).bright_white().bold()
                );
                break;
            }
        }
    }

    // Cancel any remaining orders on Pacifica at shutdown
    tprintln!("");
    tprintln!("{} Cancelling any remaining orders...",
        format!("[{} SHUTDOWN]", config.symbol).yellow().bold()
    );
    {
        match pacifica_trading_main.cancel_all_orders(false, Some(&config.symbol), false).await {
            Ok(count) => tprintln!("{} {} Cancelled {} order(s)",
                format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => tprintln!("{} {} Failed to cancel orders: {}",
                format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }
    }

    // Final state check
    let final_state = bot_state.read().await;
    match &final_state.status {
        BotStatus::Complete => {
            tprintln!("");
            tprintln!("{} {}", "✓".green().bold(), "Bot completed successfully!".green().bold());
            tprintln!("Final position: {}", final_state.position);
            Ok(())
        }
        BotStatus::Error(e) => {
            tprintln!("");
            tprintln!("{} {}: {}", "✗".red().bold(), "Bot terminated with error".red().bold(), e.to_string().red());
            anyhow::bail!("Bot failed: {}", e)
        }
        _ => {
            tprintln!("");
            tprintln!("{} Bot terminated in unexpected state: {:?}", "⚠".yellow().bold(), final_state.status);
            Ok(())
        }
    }
}
