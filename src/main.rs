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

    let pacifica_ob_service = xemm_rust::services::orderbook::PacificaOrderbookService {
        prices: pacifica_prices.clone(),
        symbol: config.symbol.clone(),
        agg_level: config.agg_level,
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };

    tokio::spawn(async move {
        pacifica_ob_service.run().await.ok();
    });

    // ═══════════════════════════════════════════════════
    // Task 2: Hyperliquid Orderbook
    // ═══════════════════════════════════════════════════

    let hyperliquid_ob_service = xemm_rust::services::orderbook::HyperliquidOrderbookService {
        prices: hyperliquid_prices.clone(),
        symbol: config.symbol.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
        request_interval_ms: 99, // ~10 Hz updates (10 req/s)
    };

    tokio::spawn(async move {
        hyperliquid_ob_service.run().await.ok();
    });

    // ═══════════════════════════════════════════════════
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


    let hedge_service = xemm_rust::services::hedge::HedgeService {
        bot_state: bot_state.clone(),
        hedge_rx,
        hyperliquid_prices: hyperliquid_prices.clone(),
        config: config.clone(),
        hyperliquid_trading: Arc::clone(&hyperliquid_trading),
        pacifica_trading: pacifica_trading_hedge.clone(),
        shutdown_tx: shutdown_tx.clone(),
    };

    tokio::spawn(async move {
        hedge_service.run().await;
    });

    // ═══════════════════════════════════════════════════
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
