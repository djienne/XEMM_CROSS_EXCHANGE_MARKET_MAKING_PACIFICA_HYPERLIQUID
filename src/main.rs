use anyhow::{Context, Result};
use colored::Colorize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::debug;
use tokio::signal;

use xemm_rust::bot::{ActiveOrder, BotState, BotStatus};
use xemm_rust::config::Config;
use xemm_rust::trade_fetcher;
use xemm_rust::csv_logger;

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
    HyperliquidCredentials, HyperliquidTrading, OrderbookClient as HyperliquidOrderbookClient,
    OrderbookConfig as HyperliquidOrderbookConfig,
};
use xemm_rust::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, FillEvent, OrderbookClient as PacificaOrderbookClient,
    OrderbookConfig as PacificaOrderbookConfig, OrderSide as PacificaOrderSide,
    PacificaCredentials, PacificaTrading, PacificaWsTrading,
};
use xemm_rust::strategy::{Opportunity, OpportunityEvaluator, OrderSide};

/// XEMM Bot - Cross-Exchange Market Making Bot
///
/// Single-cycle arbitrage bot that:
/// 1. Evaluates opportunities between Pacifica and Hyperliquid
/// 2. Places limit order on Pacifica
/// 3. Monitors profitability (cancels if profit drops >3 bps)
/// 4. Auto-refreshes orders older than 30 seconds
/// 5. Hedges on Hyperliquid when filled
/// 6. Exits after successful hedge
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    tprintln!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    tprintln!("{}", "  XEMM Bot - Cross-Exchange Market Making".bright_cyan().bold());
    tprintln!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    tprintln!("");

    // Load configuration
    let config = Config::load_default().context("Failed to load config.json")?;
    config.validate().context("Invalid configuration")?;

    tprintln!("{} Symbol: {}", "[CONFIG]".blue().bold(), config.symbol.bright_white().bold());
    tprintln!("{} Order Notional: {}", "[CONFIG]".blue().bold(), format!("${:.2}", config.order_notional_usd).bright_white());
    tprintln!("{} Pacifica Maker Fee: {}", "[CONFIG]".blue().bold(), format!("{} bps", config.pacifica_maker_fee_bps).bright_white());
    tprintln!("{} Hyperliquid Taker Fee: {}", "[CONFIG]".blue().bold(), format!("{} bps", config.hyperliquid_taker_fee_bps).bright_white());
    tprintln!("{} Target Profit: {}", "[CONFIG]".blue().bold(), format!("{} bps", config.profit_rate_bps).green().bold());
    tprintln!("{} Profit Cancel Threshold: {}", "[CONFIG]".blue().bold(), format!("{} bps", config.profit_cancel_threshold_bps).yellow());
    tprintln!("{} Order Refresh Interval: {}", "[CONFIG]".blue().bold(), format!("{} secs", config.order_refresh_interval_secs).bright_white());
    tprintln!("{} Pacifica REST Poll Interval: {}", "[CONFIG]".blue().bold(), format!("{} secs", config.pacifica_rest_poll_interval_secs).bright_white());
    tprintln!("{} Hyperliquid Market Order maximum allowed Slippage: {}", "[CONFIG]".blue().bold(), format!("{}%", config.hyperliquid_slippage * 100.0).bright_white());
    tprintln!("");

    // Load credentials
    dotenv::dotenv().ok();
    let pacifica_credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials from environment")?;
    let hyperliquid_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials from environment")?;

    tprintln!("{} {}", "[INIT]".cyan().bold(), "Credentials loaded successfully".green());

    // Initialize trading clients
    let pacifica_trading = Arc::new(tokio::sync::Mutex::new(PacificaTrading::new(pacifica_credentials.clone())));

    // Initialize WebSocket trading client for ultra-fast cancellations (no rate limits)
    let pacifica_ws_trading = Arc::new(PacificaWsTrading::new(pacifica_credentials.clone(), false)); // false = mainnet

    let hyperliquid_trading = Arc::new(HyperliquidTrading::new(hyperliquid_credentials, false)
        .context("Failed to create Hyperliquid trading client")?);

    tprintln!("{} {}", "[INIT]".cyan().bold(), "Trading clients initialized (REST + WebSocket)".green());

    // Pre-fetch Hyperliquid metadata (szDecimals, etc.) to reduce hedge latency
    tprintln!("{} Pre-fetching Hyperliquid metadata for {}...", "[INIT]".cyan().bold(), config.symbol.bright_white());
    hyperliquid_trading.get_meta().await
        .context("Failed to pre-fetch Hyperliquid metadata")?;
    tprintln!("{} {} Hyperliquid metadata cached", "[INIT]".cyan().bold(), "✓".green().bold());

    // Cancel any existing orders on Pacifica at startup with dual cancellation
    tprintln!("{} Dual cancellation of any existing orders (REST + WebSocket)...", "[INIT]".cyan().bold());

    // First: REST API cancel
    {
        let trading = pacifica_trading.lock().await;
        match trading.cancel_all_orders(false, Some(&config.symbol), false).await {
            Ok(count) => tprintln!("{} {} REST API cancelled {} existing order(s)", "[INIT]".cyan().bold(), "✓".green().bold(), count),
            Err(e) => tprintln!("{} {} REST API cancel failed: {}", "[INIT]".cyan().bold(), "⚠".yellow().bold(), e),
        }
    }

    // Second: WebSocket cancel
    match pacifica_ws_trading.cancel_all_orders_ws(false, Some(&config.symbol), false).await {
        Ok(count) => tprintln!("{} {} WebSocket cancelled {} existing order(s)", "[INIT]".cyan().bold(), "✓".green().bold(), count),
        Err(e) => tprintln!("{} {} WebSocket cancel failed: {}", "[INIT]".cyan().bold(), "⚠".yellow().bold(), e),
    }

    tprintln!("{} {} Startup dual cancellation complete", "[INIT]".cyan().bold(), "✓✓".green().bold());

    // Get market info to determine tick size
    let pacifica_tick_size: f64 = {
        let mut trading = pacifica_trading.lock().await;
        let market_info = trading.get_market_info().await.context("Failed to fetch Pacifica market info")?;
        let symbol_info = market_info
            .get(&config.symbol)
            .with_context(|| format!("Symbol {} not found in market info", config.symbol))?;
        symbol_info
            .tick_size
            .parse()
            .context("Failed to parse tick size")?
    };

    tprintln!("{} Pacifica tick size for {}: {}", "[INIT]".cyan().bold(), config.symbol.bright_white(), format!("{}", pacifica_tick_size).bright_white());

    // Create opportunity evaluator
    let evaluator = OpportunityEvaluator::new(
        config.pacifica_maker_fee_bps,
        config.hyperliquid_taker_fee_bps,
        config.profit_rate_bps,
        pacifica_tick_size,
    );

    tprintln!("{} {}", "[INIT]".cyan().bold(), "Opportunity evaluator created".green());

    // Shared state for orderbook prices
    let pacifica_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let hyperliquid_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)

    // Shared bot state
    let bot_state = Arc::new(RwLock::new(BotState::new()));

    // Order placement mutex - ensures only ONE order can be placed at a time
    // Prevents race conditions where multiple evaluation loops try to place orders simultaneously
    let order_placement_lock = Arc::new(tokio::sync::Mutex::new(()));

    // Channels for communication
    let (hedge_tx, mut hedge_rx) = mpsc::channel::<(OrderSide, f64, f64)>(1); // (side, size, avg_price)
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

    tprintln!("{} {}", "[INIT]".cyan().bold(), "State and channels initialized".green());
    tprintln!("");

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

    let bot_state_fill = bot_state.clone();
    let hedge_tx_clone = hedge_tx.clone();
    let pacifica_trading_fill = pacifica_trading.clone();
    let pacifica_ws_trading_fill = pacifica_ws_trading.clone();
    let symbol_fill = config.symbol.clone();
    let fill_config = FillDetectionConfig {
        account: pacifica_credentials.account.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };

    let mut fill_client = FillDetectionClient::new(fill_config, false)
        .context("Failed to create fill detection client")?;

    tokio::spawn(async move {
        tprintln!("{} Starting fill detection client", "[FILL_DETECTION]".magenta().bold());
        fill_client
            .start(move |fill_event| {
                match fill_event {
                    FillEvent::FullFill {
                        symbol,
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
                            symbol.bright_white().bold(),
                            avg_price.cyan(),
                            client_order_id.as_deref().unwrap_or("None")
                        );

                        // Spawn async task to handle the fill
                        let bot_state_clone = bot_state_fill.clone();
                        let hedge_tx = hedge_tx_clone.clone();
                        let side_str = side.clone();
                        let filled_amount_str = filled_amount.clone();
                        let avg_price_str = avg_price.clone();
                        let cloid = client_order_id.clone();

                        let pac_trading_clone = pacifica_trading_fill.clone();
                        let pac_ws_trading_clone = pacifica_ws_trading_fill.clone();
                        let symbol_clone = symbol_fill.clone();
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
                                // *** CRITICAL: UPDATE STATE FIRST ***
                                // Mark as filled IMMEDIATELY to prevent main loop from placing new orders
                                // during the cancellation window
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

                                // *** CRITICAL: DUAL CANCELLATION (REST + WebSocket) ***
                                // This prevents race conditions where:
                                // 1. Monitor task might place a new order
                                // 2. Multiple orders might be active
                                // 3. Stale orders might fill after hedge
                                // Using BOTH methods provides maximum safety
                                tprintln!("{} {} Dual cancellation (REST + WebSocket)...",
                                    "[FILL_DETECTION]".magenta().bold(),
                                    "⚡".yellow().bold()
                                );

                                // First: REST API cancel (fast, reliable)
                                let rest_result = pac_trading_clone
                                    .lock()
                                    .await
                                    .cancel_all_orders(false, Some(&symbol_clone), false)
                                    .await;

                                match rest_result {
                                    Ok(count) => {
                                        tprintln!("{} {} REST API cancelled {} order(s)",
                                            "[FILL_DETECTION]".magenta().bold(),
                                            "✓".green().bold(),
                                            count
                                        );
                                    }
                                    Err(e) => {
                                        tprintln!("{} {} REST API cancel failed: {}",
                                            "[FILL_DETECTION]".magenta().bold(),
                                            "⚠".yellow().bold(),
                                            e
                                        );
                                    }
                                }

                                // Second: WebSocket cancel (ultra-fast, no rate limits)
                                let ws_result = pac_ws_trading_clone
                                    .cancel_all_orders_ws(false, Some(&symbol_clone), false)
                                    .await;

                                match ws_result {
                                    Ok(count) => {
                                        tprintln!("{} {} WebSocket cancelled {} order(s)",
                                            "[FILL_DETECTION]".magenta().bold(),
                                            "✓".green().bold(),
                                            count
                                        );
                                    }
                                    Err(e) => {
                                        tprintln!("{} {} WebSocket cancel failed: {}",
                                            "[FILL_DETECTION]".magenta().bold(),
                                            "⚠".yellow().bold(),
                                            e
                                        );
                                    }
                                }

                                tprintln!("{} {} Dual cancellation complete",
                                    "[FILL_DETECTION]".magenta().bold(),
                                    "✓✓".green().bold()
                                );

                                tprintln!("{} {}, triggering hedge", format!("[{}]", symbol).bright_white().bold(), "Order filled".green().bold());

                                // Trigger hedge
                                let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                hedge_tx.send((order_side, filled_size, avg_px)).await.ok();
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
                        let bot_state_clone = bot_state_fill.clone();
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
                    FillEvent::PartialFill { filled_amount, original_amount, .. } => {
                        debug!("[FILL_DETECTION] Partial fill: {} / {}", filled_amount, original_amount);
                    }
                }
            })
            .await
            .ok();
    });

    // ═══════════════════════════════════════════════════
    // Task 4: Pacifica REST API Polling (Complement WebSocket)
    // ═══════════════════════════════════════════════════

    let pac_prices_rest_clone = pacifica_prices.clone();
    let pacifica_trading_rest_clone = pacifica_trading.clone();
    let rest_symbol = config.symbol.clone();
    let rest_agg_level = config.agg_level;
    let rest_poll_interval = config.pacifica_rest_poll_interval_secs;

    tokio::spawn(async move {
        let mut interval_timer = interval(Duration::from_secs(rest_poll_interval));
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;

            let trading = pacifica_trading_rest_clone.lock().await;
            match trading.get_best_bid_ask_rest(&rest_symbol, rest_agg_level).await {
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

    // Wait for initial orderbook data
    tprintln!("{} Waiting for orderbook data...", "[INIT]".cyan().bold());
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ═══════════════════════════════════════════════════
    // Task 5: Order Monitoring (Profit Check & Refresh)
    // ═══════════════════════════════════════════════════

    let bot_state_monitor = bot_state.clone();
    let pac_prices_monitor = pacifica_prices.clone();
    let hl_prices_monitor = hyperliquid_prices.clone();
    let config_monitor = config.clone();
    let evaluator_monitor = evaluator.clone();
    let pacifica_trading_monitor = Arc::clone(&pacifica_trading);
    let pacifica_ws_trading_monitor = Arc::clone(&pacifica_ws_trading);
    let hyperliquid_trading_monitor = Arc::clone(&hyperliquid_trading);

    tokio::spawn(async move {
        let mut monitor_interval = interval(Duration::from_millis(25)); // Check every 25ms (40 Hz)
        let mut log_interval = interval(Duration::from_secs(2)); // Log profit every 2 seconds

        loop {
            tokio::select! {
                _ = monitor_interval.tick() => {
                    let state = bot_state_monitor.read().await;

                    // Only monitor orders that are actively placed (not filled/hedging/complete)
                    if !matches!(state.status, BotStatus::OrderPlaced) {
                        continue;
                    }

                    let active_order = match &state.active_order {
                        Some(order) => order.clone(),
                        None => continue,
                    };
                    drop(state);

            // Skip monitoring if this is a temporary/pending order (still being placed)
            if active_order.client_order_id.starts_with("PENDING-") {
                continue;
            }

            // Check 1: Order age (refresh if > order_refresh_interval_secs)
            if active_order.placed_at.elapsed() > Duration::from_secs(config_monitor.order_refresh_interval_secs) {
                let (hl_bid, hl_ask) = *hl_prices_monitor.lock().unwrap();
                let age_ms = active_order.placed_at.elapsed().as_millis();
                tprintln!(
                    "{} Order age {}s (max {}s) | PAC: {} | HL: {}/{} → {}",
                    "[MONITOR]".yellow().bold(),
                    format!("{:.3}", age_ms as f64 / 1000.0).bright_white(),
                    config_monitor.order_refresh_interval_secs,
                    format!("${:.4}", active_order.price).cyan(),
                    format!("${:.4}", hl_bid).cyan(),
                    format!("${:.4}", hl_ask).cyan(),
                    "Refreshing".yellow()
                );

                // *** DUAL CANCELLATION (REST + WebSocket) ***
                // First: REST API cancel
                let rest_result = pacifica_trading_monitor
                    .lock()
                    .await
                    .cancel_all_orders(false, Some(&active_order.symbol), false)
                    .await;

                let rest_success = match rest_result {
                    Ok(count) => {
                        tprintln!("{} {} REST API cancelled {} order(s)",
                            "[MONITOR]".yellow().bold(),
                            "✓".green().bold(),
                            count
                        );
                        true
                    }
                    Err(e) => {
                        tprintln!("{} {} REST API cancel failed: {}",
                            "[MONITOR]".yellow().bold(),
                            "⚠".yellow().bold(),
                            e
                        );

                        // Check if order is still active (might have been filled/cancelled)
                        let state = bot_state_monitor.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        false
                    }
                };

                // Second: WebSocket cancel (ultra-fast, no rate limits)
                let ws_result = pacifica_ws_trading_monitor
                    .cancel_all_orders_ws(false, Some(&active_order.symbol), false)
                    .await;

                let ws_success = match ws_result {
                    Ok(count) => {
                        tprintln!("{} {} WebSocket cancelled {} order(s)",
                            "[MONITOR]".yellow().bold(),
                            "✓".green().bold(),
                            count
                        );
                        true
                    }
                    Err(e) => {
                        tprintln!("{} {} WebSocket cancel failed: {}",
                            "[MONITOR]".yellow().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                        false
                    }
                };

                // CRITICAL: Only clear state if at least ONE cancellation succeeded
                // If BOTH failed, keep the order in state to prevent placing new orders
                let at_least_one_succeeded = rest_success || ws_success;

                if !at_least_one_succeeded {
                    tprintln!("{} {} BOTH cancellations FAILED - keeping order in state to prevent race condition",
                        "[MONITOR]".yellow().bold(),
                        "⚠".red().bold()
                    );
                    // Don't clear state - order is still active on exchange!
                    continue;
                }

                // Clear active order ONLY if not filled/hedging AND at least one cancellation succeeded
                let mut state = bot_state_monitor.write().await;
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
                    let trading = pacifica_trading_monitor.lock().await;
                    match trading.get_best_bid_ask_rest(&config_monitor.symbol, config_monitor.agg_level).await {
                        Ok(Some((bid, ask))) => {
                            *pac_prices_monitor.lock().unwrap() = (bid, ask);
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
                    match hyperliquid_trading_monitor.get_l2_snapshot(&config_monitor.symbol).await {
                        Ok(Some((bid, ask))) => {
                            *hl_prices_monitor.lock().unwrap() = (bid, ask);
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
            let (hl_bid, hl_ask) = *hl_prices_monitor.lock().unwrap();

            if hl_bid == 0.0 || hl_ask == 0.0 {
                continue;
            }

            // Skip profit check if this is a temporary/pending order (redundant check for safety)
            if active_order.client_order_id.starts_with("PENDING-") {
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

            let current_profit = evaluator_monitor.recalculate_profit(&temp_opp, hl_bid, hl_ask);
            let profit_change = active_order.initial_profit_bps - current_profit;
            let profit_deviation = profit_change.abs();

            if profit_deviation > config_monitor.profit_cancel_threshold_bps {
                let hedge_price = match active_order.side {
                    OrderSide::Buy => hl_bid,
                    OrderSide::Sell => hl_ask,
                };

                let age_ms = active_order.placed_at.elapsed().as_millis();
                let change_direction = if profit_change > 0.0 { "dropped" } else { "increased" };
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

                // *** DUAL CANCELLATION (REST + WebSocket) ***
                // First: REST API cancel
                let rest_result = pacifica_trading_monitor
                    .lock()
                    .await
                    .cancel_all_orders(false, Some(&active_order.symbol), false)
                    .await;

                let rest_success = match rest_result {
                    Ok(count) => {
                        tprintln!("{} {} REST API cancelled {} order(s)",
                            "[MONITOR]".yellow().bold(),
                            "✓".green().bold(),
                            count
                        );
                        true
                    }
                    Err(e) => {
                        tprintln!("{} {} REST API cancel failed: {}",
                            "[MONITOR]".yellow().bold(),
                            "⚠".yellow().bold(),
                            e
                        );

                        // Check if order is still active (might have been filled/cancelled)
                        let state = bot_state_monitor.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        false
                    }
                };

                // Second: WebSocket cancel (ultra-fast, no rate limits)
                let ws_result = pacifica_ws_trading_monitor
                    .cancel_all_orders_ws(false, Some(&active_order.symbol), false)
                    .await;

                let ws_success = match ws_result {
                    Ok(count) => {
                        tprintln!("{} {} WebSocket cancelled {} order(s)",
                            "[MONITOR]".yellow().bold(),
                            "✓".green().bold(),
                            count
                        );
                        true
                    }
                    Err(e) => {
                        tprintln!("{} {} WebSocket cancel failed: {}",
                            "[MONITOR]".yellow().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                        false
                    }
                };

                // CRITICAL: Only clear state if at least ONE cancellation succeeded
                // If BOTH failed, keep the order in state to prevent placing new orders
                let at_least_one_succeeded = rest_success || ws_success;

                if !at_least_one_succeeded {
                    tprintln!("{} {} BOTH cancellations FAILED - keeping order in state to prevent race condition",
                        "[MONITOR]".yellow().bold(),
                        "⚠".red().bold()
                    );
                    // Don't clear state - order is still active on exchange!
                    continue;
                }

                // Clear active order ONLY if not filled/hedging AND at least one cancellation succeeded
                let mut state = bot_state_monitor.write().await;
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
                    let trading = pacifica_trading_monitor.lock().await;
                    match trading.get_best_bid_ask_rest(&config_monitor.symbol, config_monitor.agg_level).await {
                        Ok(Some((bid, ask))) => {
                            *pac_prices_monitor.lock().unwrap() = (bid, ask);
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
                    match hyperliquid_trading_monitor.get_l2_snapshot(&config_monitor.symbol).await {
                        Ok(Some((bid, ask))) => {
                            *hl_prices_monitor.lock().unwrap() = (bid, ask);
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
                    let state = bot_state_monitor.read().await;

                    // Only log profit for actively placed orders
                    if !matches!(state.status, BotStatus::OrderPlaced) {
                        continue;
                    }

                    let active_order = match &state.active_order {
                        Some(order) => order.clone(),
                        None => continue,
                    };
                    drop(state);

                    // Skip logging if this is a temporary/pending order
                    if active_order.client_order_id.starts_with("PENDING-") {
                        continue;
                    }

                    let (hl_bid, hl_ask) = *hl_prices_monitor.lock().unwrap();
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

                    let current_profit = evaluator_monitor.recalculate_profit(&temp_opp, hl_bid, hl_ask);
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
    });

    // ═══════════════════════════════════════════════════
    // Task 6: Hedge Execution
    // ═══════════════════════════════════════════════════

    let bot_state_hedge = bot_state.clone();
    let hl_prices_hedge = hyperliquid_prices.clone();
    let config_hedge = config.clone();
    let hl_trading_hedge = Arc::clone(&hyperliquid_trading);
    let pacifica_trading_hedge = pacifica_trading.clone();
    let pacifica_ws_trading_hedge = pacifica_ws_trading.clone();
    let shutdown_tx_hedge = shutdown_tx.clone();

    tokio::spawn(async move {
        while let Some((side, size, avg_price)) = hedge_rx.recv().await {
            tprintln!("{} Received trigger: {} {} @ {}",
                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                side.as_str().bright_yellow(),
                size,
                format!("${:.4}", avg_price).cyan()
            );

            // *** CRITICAL: ASYNC PRE-HEDGE DUAL CANCELLATION ***
            // Run cancellation in parallel with hedge execution to avoid adding latency
            // This provides extra safety layer in case fill detection cancellation missed anything
            let pac_trading_async = pacifica_trading_hedge.clone();
            let pac_ws_trading_async = pacifica_ws_trading_hedge.clone();
            let symbol_async = config_hedge.symbol.clone();

            tokio::spawn(async move {
                tprintln!("{} {} Pre-hedge safety: Async dual cancellation (REST + WebSocket)...",
                    format!("[{} HEDGE]", symbol_async).bright_magenta().bold(),
                    "⚡".yellow().bold()
                );

                // First: REST API cancel
                let rest_result = pac_trading_async
                    .lock()
                    .await
                    .cancel_all_orders(false, Some(&symbol_async), false)
                    .await;

                match rest_result {
                    Ok(count) => {
                        if count > 0 {
                            tprintln!("{} {} Async REST API cancelled {} order(s)",
                                format!("[{} HEDGE]", symbol_async).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                    }
                    Err(e) => {
                        tprintln!("{} {} Async REST API cancel failed: {}",
                            format!("[{} HEDGE]", symbol_async).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                    }
                }

                // Second: WebSocket cancel (ultra-fast, no rate limits)
                let ws_result = pac_ws_trading_async
                    .cancel_all_orders_ws(false, Some(&symbol_async), false)
                    .await;

                match ws_result {
                    Ok(count) => {
                        if count > 0 {
                            tprintln!("{} {} Async WebSocket cancelled {} order(s)",
                                format!("[{} HEDGE]", symbol_async).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                    }
                    Err(e) => {
                        tprintln!("{} {} Async WebSocket cancel failed: {}",
                            format!("[{} HEDGE]", symbol_async).bright_magenta().bold(),
                            "⚠".yellow().bold(),
                            e
                        );
                    }
                }
            });

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

                    // ═══════════════════════════════════════════════════
                    // Log trade to CSV
                    // ═══════════════════════════════════════════════════
                    if let (Some(pac_price), Some(hl_price)) = (pacifica_actual_price, hl_actual_price) {
                        if let (Some(pac_notional_val), Some(hl_notional_val)) = (pacifica_notional, hl_notional) {
                            let trade_record = csv_logger::TradeRecord::new(
                                chrono::Utc::now(),
                                config_hedge.symbol.clone(),
                                side,
                                pac_price,
                                size,
                                pac_notional_val,
                                pac_fee_usd,
                                hl_price,
                                size,
                                hl_notional_val,
                                hl_fee_usd,
                                expected_profit_bps.unwrap_or(0.0),
                                actual_profit_bps,
                                actual_profit_usd,
                            );

                            match csv_logger::log_trade("trades_history.csv", &trade_record) {
                                Ok(_) => {
                                    tprintln!("{} {} Trade logged to trades_history.csv",
                                        format!("[{} LOG]", config_hedge.symbol).bright_blue().bold(),
                                        "✓".green().bold()
                                    );
                                }
                                Err(e) => {
                                    tprintln!("{} {} Failed to log trade to CSV: {}",
                                        format!("[{} LOG]", config_hedge.symbol).bright_blue().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                }
                            }
                        }
                    }

                    // *** CRITICAL: FINAL DUAL CANCELLATION ***
                    // Cancel all orders one last time before marking complete
                    // This ensures no stray orders remain active
                    tprintln!("");
                    tprintln!("{} {} Post-hedge safety: Final dual cancellation (REST + WebSocket)...",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    // First: REST API cancel
                    let rest_result = pacifica_trading_hedge
                        .lock()
                        .await
                        .cancel_all_orders(false, Some(&config_hedge.symbol), false)
                        .await;

                    match rest_result {
                        Ok(count) => {
                            tprintln!("{} {} REST API cancelled {} order(s)",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                        Err(e) => {
                            tprintln!("{} {} REST API cancel failed: {}",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        }
                    }

                    // Second: WebSocket cancel (ultra-fast, no rate limits)
                    let ws_result = pacifica_ws_trading_hedge
                        .cancel_all_orders_ws(false, Some(&config_hedge.symbol), false)
                        .await;

                    match ws_result {
                        Ok(count) => {
                            tprintln!("{} {} WebSocket cancelled {} order(s)",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                        Err(e) => {
                            tprintln!("{} {} WebSocket cancel failed: {}",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        }
                    }

                    tprintln!("{} {} Final dual cancellation complete",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "✓✓".green().bold()
                    );

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

                    // *** CRITICAL: DUAL CANCELLATION ON ERROR ***
                    // Even if hedge fails, cancel all orders to prevent stray positions
                    tprintln!("{} {} Error recovery: Dual cancellation (REST + WebSocket)...",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "⚡".yellow().bold()
                    );

                    // First: REST API cancel
                    let rest_result = pacifica_trading_hedge
                        .lock()
                        .await
                        .cancel_all_orders(false, Some(&config_hedge.symbol), false)
                        .await;

                    match rest_result {
                        Ok(count) => {
                            tprintln!("{} {} REST API cancelled {} order(s)",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                        Err(e) => {
                            tprintln!("{} {} REST API cancel failed: {}",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        }
                    }

                    // Second: WebSocket cancel (ultra-fast, no rate limits)
                    let ws_result = pacifica_ws_trading_hedge
                        .cancel_all_orders_ws(false, Some(&config_hedge.symbol), false)
                        .await;

                    match ws_result {
                        Ok(count) => {
                            tprintln!("{} {} WebSocket cancelled {} order(s)",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "✓".green().bold(),
                                count
                            );
                        }
                        Err(e) => {
                            tprintln!("{} {} WebSocket cancel failed: {}",
                                format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                                "⚠".yellow().bold(),
                                e
                            );
                        }
                    }

                    tprintln!("{} {} Error recovery dual cancellation complete",
                        format!("[{} HEDGE]", config_hedge.symbol).bright_magenta().bold(),
                        "✓✓".green().bold()
                    );

                    let mut state = bot_state_hedge.write().await;
                    state.set_error(format!("Hedge failed: {}", e));

                    // Signal shutdown with error
                    shutdown_tx_hedge.send(()).await.ok();
                }
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // Task 7: Main Opportunity Evaluation Loop
    // ═══════════════════════════════════════════════════

    tprintln!("{} Starting opportunity evaluation loop",
        format!("[{} MAIN]", config.symbol).bright_white().bold()
    );
    tprintln!("");

    let mut eval_interval = interval(Duration::from_millis(100)); // Evaluate every 100ms

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

                // Check if grace period has elapsed (1 second after cancellation)
                if !state.grace_period_elapsed(1) {
                    continue;
                }
                drop(state);

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
                    // CRITICAL: Acquire order placement lock FIRST
                    // This ensures only ONE order can be placed at a time across all evaluation loops
                    let placement_guard = order_placement_lock.lock().await;

                    // Now verify bot is still idle (double-check for race conditions)
                    let mut state = bot_state.write().await;
                    if !state.is_idle() {
                        drop(state);
                        drop(placement_guard);
                        continue;
                    }

                    // CRITICAL: Create temporary active order IMMEDIATELY to block other loops
                    // This prevents race conditions where multiple evaluation loops try to place orders
                    // The temporary order will be updated with real details after API call succeeds
                    let temp_client_order_id = format!("PENDING-{}", chrono::Utc::now().timestamp_millis());
                    let temp_active_order = ActiveOrder {
                        client_order_id: temp_client_order_id.clone(),
                        symbol: config.symbol.clone(),
                        side: opp.direction,
                        price: opp.pacifica_price,
                        size: opp.size,
                        initial_profit_bps: opp.initial_profit_bps,
                        placed_at: Instant::now(),
                    };
                    state.set_active_order(temp_active_order);
                    drop(state); // Release write lock before API call

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

                    match pacifica_trading
                        .lock()
                        .await
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

                                // Update state with real order details
                                let mut state = bot_state.write().await;
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
                                drop(state);
                            } else {
                                tprintln!("{} {} Order placed but no client_order_id returned",
                                    format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                    "✗".red().bold()
                                );
                                // Clear temporary order
                                let mut state = bot_state.write().await;
                                state.clear_active_order();
                                drop(state);
                            }
                        }
                        Err(e) => {
                            tprintln!("{} {} Failed to place order: {}",
                                format!("[{} ORDER]", config.symbol).bright_yellow().bold(),
                                "✗".red().bold(),
                                e.to_string().red()
                            );
                            // Clear temporary order and return to Idle state
                            let mut state = bot_state.write().await;
                            state.clear_active_order();
                            drop(state);
                        }
                    }

                    drop(placement_guard);
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

    // Cancel any remaining orders on Pacifica at shutdown with dual cancellation
    tprintln!("");
    tprintln!("{} Dual cancellation of any remaining orders (REST + WebSocket)...",
        format!("[{} SHUTDOWN]", config.symbol).yellow().bold()
    );

    // First: REST API cancel
    {
        let trading = pacifica_trading.lock().await;
        match trading.cancel_all_orders(false, Some(&config.symbol), false).await {
            Ok(count) => tprintln!("{} {} REST API cancelled {} order(s)",
                format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => tprintln!("{} {} REST API cancel failed: {}",
                format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }
    }

    // Second: WebSocket cancel
    match pacifica_ws_trading.cancel_all_orders_ws(false, Some(&config.symbol), false).await {
        Ok(count) => tprintln!("{} {} WebSocket cancelled {} order(s)",
            format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
            "✓".green().bold(),
            count
        ),
        Err(e) => tprintln!("{} {} WebSocket cancel failed: {}",
            format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
            "⚠".yellow().bold(),
            e
        ),
    }

    tprintln!("{} {} Shutdown dual cancellation complete",
        format!("[{} SHUTDOWN]", config.symbol).yellow().bold(),
        "✓✓".green().bold()
    );

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
