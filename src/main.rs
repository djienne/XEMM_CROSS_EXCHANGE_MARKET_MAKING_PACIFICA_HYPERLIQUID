use anyhow::{Context, Result};
use colored::Colorize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, info, warn};
use tokio::signal;

use xemm_rust::bot::{ActiveOrder, BotState, BotStatus};
use xemm_rust::config::Config;
use xemm_rust::trade_fetcher;

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
    FillDetectionClient, FillDetectionConfig, FillEvent, PositionBaselineUpdater,
    OrderbookClient as PacificaOrderbookClient, OrderbookConfig as PacificaOrderbookConfig, OrderSide as PacificaOrderSide,
    PacificaCredentials, PacificaTrading, PacificaWsTrading,
};
use xemm_rust::strategy::{Opportunity, OpportunityEvaluator, OrderSide};

/// Rate limit tracker for exponential backoff
#[derive(Debug)]
struct RateLimitTracker {
    last_error_time: Option<Instant>,
    consecutive_errors: u32,
}

impl RateLimitTracker {
    fn new() -> Self {
        Self {
            last_error_time: None,
            consecutive_errors: 0,
        }
    }

    /// Record a rate limit error
    fn record_error(&mut self) {
        self.last_error_time = Some(Instant::now());
        self.consecutive_errors += 1;
    }

    /// Record a successful API call
    fn record_success(&mut self) {
        self.last_error_time = None;
        self.consecutive_errors = 0;
    }

    /// Get current backoff duration in seconds (exponential: 1, 2, 4, 8, 16, 32 max)
    fn get_backoff_secs(&self) -> u64 {
        if self.consecutive_errors == 0 {
            return 0;
        }
        std::cmp::min(2u64.pow(self.consecutive_errors - 1), 32)
    }

    /// Check if we should skip this operation due to active backoff
    fn should_skip(&self) -> bool {
        if let Some(last_error) = self.last_error_time {
            let backoff_duration = Duration::from_secs(self.get_backoff_secs());
            last_error.elapsed() < backoff_duration
        } else {
            false
        }
    }

    /// Get remaining backoff time in seconds
    fn remaining_backoff_secs(&self) -> f64 {
        if let Some(last_error) = self.last_error_time {
            let backoff_duration = Duration::from_secs(self.get_backoff_secs());
            let elapsed = last_error.elapsed();
            if elapsed < backoff_duration {
                (backoff_duration - elapsed).as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

/// Check if an error is a rate limit error
fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    let error_string = error.to_string().to_lowercase();
    error_string.contains("rate limit") || error_string.contains("too many requests") || error_string.contains("429")
}

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
    // Each task gets its own PacificaTrading instance to eliminate lock contention
    // Wrapped in Arc for sharing across closures, but NO MUTEX = no blocking!
    // This prevents tasks from blocking each other during HTTP calls
    let pacifica_trading_main = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create main Pacifica trading client")?);
    let pacifica_trading_fill = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create fill detection Pacifica trading client")?);
    let pacifica_trading_rest_fill = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create REST fill detection Pacifica trading client")?);
    let pacifica_trading_monitor = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create monitor Pacifica trading client")?);
    let pacifica_trading_hedge = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create hedge Pacifica trading client")?);
    let pacifica_trading_rest_poll = Arc::new(PacificaTrading::new(pacifica_credentials.clone())
        .context("Failed to create REST polling Pacifica trading client")?);


    // Initialize WebSocket trading client for ultra-fast cancellations (no rate limits)
    let pacifica_ws_trading = Arc::new(PacificaWsTrading::new(pacifica_credentials.clone(), false)); // false = mainnet

    let hyperliquid_trading = Arc::new(HyperliquidTrading::new(hyperliquid_credentials, false)
        .context("Failed to create Hyperliquid trading client")?);

    tprintln!("{} {}", "[INIT]".cyan().bold(), "Trading clients initialized (6 REST instances + WebSocket)".green());

    // Pre-fetch Hyperliquid metadata (szDecimals, etc.) to reduce hedge latency
    tprintln!("{} Pre-fetching Hyperliquid metadata for {}...", "[INIT]".cyan().bold(), config.symbol.bright_white());
    hyperliquid_trading.get_meta().await
        .context("Failed to pre-fetch Hyperliquid metadata")?;
    tprintln!("{} {} Hyperliquid metadata cached", "[INIT]".cyan().bold(), "✓".green().bold());

    // Cancel any existing orders on Pacifica at startup
    tprintln!("{} Cancelling any existing orders on Pacifica...", "[INIT]".cyan().bold());
    match pacifica_trading_main.cancel_all_orders(false, Some(&config.symbol), false).await {
        Ok(count) => tprintln!("{} {} Cancelled {} existing order(s)", "[INIT]".cyan().bold(), "✓".green().bold(), count),
        Err(e) => tprintln!("{} {} Failed to cancel existing orders: {}", "[INIT]".cyan().bold(), "⚠".yellow().bold(), e),
    }

    // Get market info to determine tick size
    let pacifica_tick_size: f64 = {
        let market_info = pacifica_trading_main.get_market_info().await.context("Failed to fetch Pacifica market info")?;
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

    // Shared state for tracking processed fills (to prevent duplicate hedges from WS + REST + Position monitor)
    let processed_fills = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::<String>::new()));

    // Track last known position for delta detection (4th fill detection method)
    #[derive(Debug, Clone)]
    struct PositionSnapshot {
        amount: f64,
        side: String,  // "bid" or "ask"
        last_check: std::time::Instant,
    }

    let last_position_snapshot = Arc::new(tokio::sync::Mutex::new(
        Option::<PositionSnapshot>::None
    ));

    // ═══════════════════════════════════════════════════
    // Task 3: Fill Detection
    // ═══════════════════════════════════════════════════

    let bot_state_fill = bot_state.clone();
    let hedge_tx_clone = hedge_tx.clone();
    // pacifica_trading_fill already defined at top level (no Arc/Mutex needed)
    let pacifica_ws_trading_fill = pacifica_ws_trading.clone();
    let symbol_fill = config.symbol.clone();
    let processed_fills_ws = processed_fills.clone();
    let fill_config = FillDetectionConfig {
        account: pacifica_credentials.account.clone(),
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
        enable_position_fill_detection: true,  // Enable position-based fill detection redundancy
    };

    let mut fill_client = FillDetectionClient::new(fill_config, false)
        .context("Failed to create fill detection client")?;

    // Get baseline updater BEFORE starting (to prevent duplicate hedge triggers)
    let baseline_updater = fill_client.get_baseline_updater();

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
                        let processed_fills_clone = processed_fills_ws.clone();
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

                                // *** CRITICAL: UPDATE POSITION BASELINE ***
                                // This prevents position-based detection from triggering duplicate hedge
                                let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                baseline_updater_clone.update_baseline(
                                    &symbol_clone,
                                    &side_str,
                                    filled_size,
                                    avg_px
                                );

                                tprintln!("{} {}, triggering hedge", format!("[{}]", symbol).bright_white().bold(), "Order filled".green().bold());

                                // Trigger hedge
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
                    FillEvent::PartialFill {
                        symbol,
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
                            symbol.bright_white().bold(),
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
                            let bot_state_clone = bot_state_fill.clone();
                            let hedge_tx = hedge_tx_clone.clone();
                            let side_str = side.clone();
                            let filled_amount_str = filled_amount.clone();
                            let avg_price_str = avg_price.clone();
                            let cloid = client_order_id.clone();

                            let pac_trading_clone = pacifica_trading_fill.clone();
                            let pac_ws_trading_clone = pacifica_ws_trading_fill.clone();
                            let symbol_clone = symbol_fill.clone();
                            let processed_fills_clone = processed_fills_ws.clone();
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
                                    // Mark as filled IMMEDIATELY to prevent main loop from placing new orders
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

                                    // *** CRITICAL: DUAL CANCELLATION (REST + WebSocket) ***
                                    tprintln!("{} {} Dual cancellation (REST + WebSocket)...",
                                        "[FILL_DETECTION]".magenta().bold(),
                                        "⚡".yellow().bold()
                                    );

                                    // First: REST API cancel
                                    let rest_result = pac_trading_clone
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

                                    // Second: WebSocket cancel
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

                                    // *** CRITICAL: UPDATE POSITION BASELINE ***
                                    // This prevents position-based detection from triggering duplicate hedge
                                    let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                    baseline_updater_clone.update_baseline(
                                        &symbol_clone,
                                        &side_str,
                                        filled_size,
                                        avg_px
                                    );

                                    tprintln!("{} {}, triggering hedge for partial fill",
                                        format!("[{}]", symbol).bright_white().bold(),
                                        "Partial fill hedging".green().bold()
                                    );

                                    // Trigger hedge with partial fill amount
                                    hedge_tx.send((order_side, filled_size, avg_px)).await.ok();
                                }
                            });
                        } else {
                            tprintln!(
                                "{} Partial fill notional ${:.2} below $10.00 threshold, skipping hedge (waiting for more fills)",
                                "[FILL_DETECTION]".magenta().bold(),
                                notional_value
                            );
                        }
                    }
                    FillEvent::PositionFill {
                        symbol,
                        side,
                        filled_amount,
                        avg_price,
                        cross_validated,
                        position_delta,
                        prev_position,
                        new_position,
                        ..
                    } => {
                        // Log position-based fill detection
                        if cross_validated {
                            info!(
                                "[POSITION FILL ✓] {} {} {} @ {} (pos delta: {} → {}, cross-validated)",
                                side, filled_amount, symbol, avg_price, prev_position, new_position
                            );
                        } else {
                            warn!(
                                "[POSITION FILL ⚠] {} {} {} @ {} (pos delta: {} → {}, MISSED BY PRIMARY!)",
                                side, filled_amount, symbol, avg_price, prev_position, new_position
                            );
                        }

                        // For cross-validated fills, this is just informational
                        // The order-based fill detection already triggered the hedge
                        // For non-cross-validated fills, this serves as a critical safety net
                        if !cross_validated {
                            // This fill was NOT detected by order updates - critical case!
                            tprintln!(
                                "{} {} POSITION-BASED FILL (SAFETY NET): {} {} {} @ {}",
                                "[FILL_DETECTION]".magenta().bold(),
                                "⚠".yellow().bold(),
                                side.bright_yellow(),
                                filled_amount.bright_white(),
                                symbol.bright_white().bold(),
                                avg_price.cyan()
                            );

                            // Spawn async task to handle like a regular fill
                            // This is the redundancy layer catching a missed fill
                            let bot_state_clone = bot_state_fill.clone();
                            let hedge_tx = hedge_tx_clone.clone();
                            let side_str = side.clone();
                            let filled_amount_str = filled_amount.clone();
                            let avg_price_str = avg_price.clone();

                            let pac_trading_clone = pacifica_trading_fill.clone();
                            let pac_ws_trading_clone = pacifica_ws_trading_fill.clone();
                            let symbol_clone = symbol_fill.clone();
                            let processed_fills_clone = processed_fills_ws.clone();

                            tokio::spawn(async move {
                                // Check deduplication (unlikely for position-based, but be safe)
                                let fill_id = format!("pos_{}_{}", position_delta, new_position);
                                {
                                    let mut processed = processed_fills_clone.lock().await;
                                    if processed.contains(&fill_id) {
                                        debug!("[POSITION FILL] Already processed, skipping");
                                        return;
                                    }
                                    processed.insert(fill_id);
                                }

                                // Update state
                                let order_side = match side_str.as_str() {
                                    "buy" => OrderSide::Buy,
                                    "sell" => OrderSide::Sell,
                                    _ => {
                                        warn!("[POSITION FILL] Unknown side: {}", side_str);
                                        return;
                                    }
                                };

                                let filled_size: f64 = filled_amount_str.parse().unwrap_or(0.0);

                                {
                                    let mut state = bot_state_clone.write().await;
                                    state.mark_filled(filled_size, order_side);
                                }

                                tprintln!(
                                    "{} {} Position-based fill detected - State updated",
                                    "[POSITION FILL]".magenta().bold(),
                                    "✓".green().bold()
                                );

                                // Cancel all orders (dual method)
                                pac_trading_clone
                                    .cancel_all_orders(false, Some(&symbol_clone), false)
                                    .await
                                    .ok();
                                pac_ws_trading_clone
                                    .cancel_all_orders_ws(false, Some(&symbol_clone), false)
                                    .await
                                    .ok();

                                // Trigger hedge
                                let avg_px: f64 = avg_price_str.parse().unwrap_or(0.0);
                                hedge_tx.send((order_side, filled_size, avg_px)).await.ok();
                            });
                        }
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

    let bot_state_rest_fill = bot_state.clone();
    let hedge_tx_rest = hedge_tx.clone();
    // pacifica_trading_rest_fill already defined at top level (no clone needed)
    let pacifica_ws_trading_rest_fill = pacifica_ws_trading.clone();
    let symbol_rest_fill = config.symbol.clone();
    let processed_fills_rest = processed_fills.clone();
    let min_hedge_notional = 10.0; // Same as partial fill threshold

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_millis(500)); // Poll every 500ms (0.5s)
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut consecutive_errors = 0u32;
        let mut last_known_filled_amount: f64 = 0.0;

        loop {
            poll_interval.tick().await;

            // Get active order info, with recovery logic for recent cancellations
            let active_order_info = {
                let state = bot_state_rest_fill.read().await;

                // Skip only for terminal states
                if matches!(state.status, xemm_rust::bot::BotStatus::Complete | xemm_rust::bot::BotStatus::Error(_)) {
                    last_known_filled_amount = 0.0;
                    continue;
                }

                // Try to get active order info
                if let Some(order) = &state.active_order {
                    Some((order.client_order_id.clone(), order.side))
                } else {
                    // No active order - check if we recently cancelled (possible race condition)
                    // Monitor might have cancelled a filled order before we detected the fill
                    if let Some(last_cancel_time) = state.last_cancellation_time {
                        if last_cancel_time.elapsed().as_secs() < 5 {
                            // Within 5 seconds of cancellation - check all open orders for orphaned fills
                            debug!("[REST_FILL_DETECTION] No active order but recent cancellation ({:.1}s ago) - checking for orphaned fills",
                                last_cancel_time.elapsed().as_secs_f64());
                            None // Will query all open orders below
                        } else {
                            // Old cancellation, truly idle
                            last_known_filled_amount = 0.0;
                            continue;
                        }
                    } else {
                        // Never cancelled, truly idle
                        last_known_filled_amount = 0.0;
                        continue;
                    }
                }
            };

            let client_order_id_opt = active_order_info.as_ref().map(|(id, _)| id.clone());
            let order_side_opt = active_order_info.as_ref().map(|(_, side)| *side);

            // Fetch open orders via REST API
            let open_orders_result = pacifica_trading_rest_fill.get_open_orders().await;

            match open_orders_result {
                Ok(orders) => {
                    consecutive_errors = 0; // Reset error counter on success

                    // Find our order - either by client_order_id or any filled order in recovery mode
                    let our_order = if let Some(ref cloid) = client_order_id_opt {
                        // Normal mode: Find by client_order_id
                        orders.iter().find(|o| &o.client_order_id == cloid)
                    } else {
                        // Recovery mode: Find any order with fills for our symbol
                        debug!("[REST_FILL_DETECTION] Recovery mode: searching {} orders for filled orders", orders.len());
                        orders.iter()
                            .filter(|o| o.symbol == symbol_rest_fill)
                            .find(|o| {
                                let filled: f64 = o.filled_amount.parse().unwrap_or(0.0);
                                filled > 0.0
                            })
                    };

                    if let Some(order) = our_order {
                        let filled_amount: f64 = order.filled_amount.parse().unwrap_or(0.0);
                        let initial_amount: f64 = order.initial_amount.parse().unwrap_or(0.0);
                        let price: f64 = order.price.parse().unwrap_or(0.0);

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

                            if is_full_fill || notional_value > min_hedge_notional {
                                let fill_type = if is_full_fill { "full" } else { "partial" };
                                let cloid = &order.client_order_id;
                                let fill_id = format!("{}_{}_rest", fill_type, cloid);

                                // Check if already processed (prevent duplicate hedges from WebSocket)
                                let mut processed = processed_fills_rest.lock().await;
                                if processed.contains(&fill_id) || processed.contains(&format!("full_{}", cloid)) || processed.contains(&format!("partial_{}", cloid)) {
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
                                            debug!("[REST_FILL_DETECTION] Unknown order side: {}, skipping", order.side);
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
                                    symbol_rest_fill.bright_white().bold(),
                                    format!("${:.6}", price).cyan(),
                                    filled_amount,
                                    initial_amount,
                                    format!("${:.2}", notional_value).cyan().bold(),
                                    "(REST API)".bright_black()
                                );

                                // Trigger hedge (same flow as WebSocket)
                                let bot_state_clone = bot_state_rest_fill.clone();
                                let hedge_tx_clone = hedge_tx_rest.clone();
                                let pac_trading_clone = pacifica_trading_rest_fill.clone();
                                let pac_ws_trading_clone = pacifica_ws_trading_rest_fill.clone();
                                let symbol_clone = symbol_rest_fill.clone();
                                let side_clone = order_side;

                                tokio::spawn(async move {
                                    // Update state
                                    {
                                        let mut state = bot_state_clone.write().await;
                                        state.mark_filled(filled_amount, side_clone);
                                    }

                                    tprintln!("{} {} State updated to Filled (REST)",
                                        "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                        "✓".green().bold()
                                    );

                                    // Dual cancellation
                                    tprintln!("{} {} Dual cancellation (REST + WebSocket)...",
                                        "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                        "⚡".yellow().bold()
                                    );

                                    // REST API cancel
                                    match pac_trading_clone.cancel_all_orders(false, Some(&symbol_clone), false).await {
                                        Ok(count) => {
                                            tprintln!("{} {} REST API cancelled {} order(s)",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "✓".green().bold(),
                                                count
                                            );
                                        }
                                        Err(e) => {
                                            tprintln!("{} {} REST API cancel failed: {}",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "⚠".yellow().bold(),
                                                e
                                            );
                                        }
                                    }

                                    // WebSocket cancel
                                    match pac_ws_trading_clone.cancel_all_orders_ws(false, Some(&symbol_clone), false).await {
                                        Ok(count) => {
                                            tprintln!("{} {} WebSocket cancelled {} order(s)",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "✓".green().bold(),
                                                count
                                            );
                                        }
                                        Err(e) => {
                                            tprintln!("{} {} WebSocket cancel failed: {}",
                                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                                "⚠".yellow().bold(),
                                                e
                                            );
                                        }
                                    }

                                    tprintln!("{} {} Dual cancellation complete, triggering hedge",
                                        "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                        "✓✓".green().bold()
                                    );

                                    // Trigger hedge
                                    hedge_tx_clone.send((side_clone, filled_amount, price)).await.ok();
                                });
                            } else {
                                debug!(
                                    "[REST_FILL_DETECTION] Partial fill notional ${:.2} below ${:.2} threshold, skipping",
                                    notional_value, min_hedge_notional
                                );
                            }
                        }
                    } else {
                        // Order no longer in open orders (might be fully filled or cancelled)
                        if last_known_filled_amount > 0.0 {
                            if let Some(ref cloid) = client_order_id_opt {
                                debug!("[REST_FILL_DETECTION] Order {} no longer in open orders (filled or cancelled)", cloid);
                            } else {
                                debug!("[REST_FILL_DETECTION] No filled orders found in recovery mode");
                            }
                            last_known_filled_amount = 0.0;
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;

                    // Check if it's a rate limit error
                    let is_rate_limit = e.to_string().to_lowercase().contains("rate limit");

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
                        debug!("[REST_FILL_DETECTION] Error fetching open orders (attempt {}): {}", consecutive_errors, e);

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
    });

    // ═══════════════════════════════════════════════════
    // Task 5.5: Position-Based Fill Detection (4th Layer - Ground Truth)
    // ═══════════════════════════════════════════════════

    let bot_state_position = bot_state.clone();
    let hedge_tx_position = hedge_tx.clone();
    let pacifica_trading_position = Arc::new(
        PacificaTrading::new(pacifica_credentials.clone())
            .context("Failed to create position monitor trading client")?
    );
    let pacifica_ws_trading_position = pacifica_ws_trading.clone();
    let symbol_position = config.symbol.clone();
    let processed_fills_position = processed_fills.clone();
    let last_position_snapshot_clone = last_position_snapshot.clone();

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_millis(500)); // 500ms polling
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            poll_interval.tick().await;

            // Only check if we have an active order
            let active_order_info = {
                let state = bot_state_position.read().await;
                if matches!(
                    state.status,
                    xemm_rust::bot::BotStatus::Complete | xemm_rust::bot::BotStatus::Error(_)
                ) {
                    continue;
                }

                state.active_order.as_ref().map(|o| (
                    o.client_order_id.clone(),
                    o.side,
                    o.size
                ))
            };

            if active_order_info.is_none() {
                // No active order - update snapshot for next order
                match pacifica_trading_position.get_positions().await {
                    Ok(positions) => {
                        let position = positions.iter().find(|p| p.symbol == symbol_position);
                        let mut snapshot = last_position_snapshot_clone.lock().await;

                        if let Some(pos) = position {
                            let amount: f64 = pos.amount.parse().unwrap_or(0.0);
                            *snapshot = Some(PositionSnapshot {
                                amount,
                                side: pos.side.clone(),
                                last_check: std::time::Instant::now(),
                            });
                            debug!("[POSITION_MONITOR] Updated baseline: {} {} {}",
                                symbol_position, pos.side, amount);
                        } else {
                            *snapshot = None;
                            debug!("[POSITION_MONITOR] No position for {}", symbol_position);
                        }
                    }
                    Err(e) => {
                        debug!("[POSITION_MONITOR] Failed to fetch baseline position: {}", e);
                    }
                }
                continue;
            }

            let (client_order_id, order_side, order_size) = active_order_info.unwrap();

            // Fetch current positions
            let positions_result = pacifica_trading_position.get_positions().await;

            match positions_result {
                Ok(positions) => {
                    let current_position = positions.iter().find(|p| p.symbol == symbol_position);
                    let last_snapshot = last_position_snapshot_clone.lock().await.clone();

                    // Calculate position delta
                    let (last_amount, last_side) = if let Some(ref snap) = last_snapshot {
                        (snap.amount, snap.side.clone())
                    } else {
                        (0.0, "none".to_string())
                    };

                    let (current_amount, current_side) = if let Some(pos) = current_position {
                        (pos.amount.parse::<f64>().unwrap_or(0.0), pos.side.clone())
                    } else {
                        (0.0, "none".to_string())
                    };

                    // Convert to signed position for delta calculation
                    let last_signed = match last_side.as_str() {
                        "bid" => last_amount,
                        "ask" => -last_amount,
                        _ => 0.0,
                    };

                    let current_signed = match current_side.as_str() {
                        "bid" => current_amount,
                        "ask" => -current_amount,
                        _ => 0.0,
                    };

                    let delta = current_signed - last_signed;

                    // Check if delta matches our order direction
                    let delta_matches_order = (delta > 0.0 && matches!(order_side, OrderSide::Buy))
                        || (delta < 0.0 && matches!(order_side, OrderSide::Sell));

                    // If delta is significant and matches order direction
                    if delta.abs() > 0.0001 && delta_matches_order {
                        // Position changed in expected direction - fill detected!
                        let fill_size = delta.abs();

                        tprintln!(
                            "{} {} Position delta detected: {} {} → {} {} (Δ {:.4})",
                            "[POSITION_MONITOR]".bright_cyan().bold(),
                            "⚡".yellow().bold(),
                            format!("{:.4}", last_signed).bright_white(),
                            last_side.yellow(),
                            format!("{:.4}", current_signed).bright_white(),
                            current_side.yellow(),
                            format!("{:.4}", delta.abs()).green().bold()
                        );

                        // Check bot state - don't trigger duplicate hedges
                        let current_state = {
                            let state = bot_state_position.read().await;
                            state.status.clone()
                        };

                        // Skip if already filled, hedging, or complete
                        if matches!(
                            current_state,
                            xemm_rust::bot::BotStatus::Filled |
                            xemm_rust::bot::BotStatus::Hedging |
                            xemm_rust::bot::BotStatus::Complete
                        ) {
                            tprintln!(
                                "{} {} Fill already handled by primary detection (state: {:?}), skipping duplicate hedge",
                                "[POSITION_MONITOR]".bright_cyan().bold(),
                                "ℹ".blue().bold(),
                                current_state
                            );

                            // Update snapshot to prevent continuous detection
                            let mut snapshot = last_position_snapshot_clone.lock().await;
                            *snapshot = Some(PositionSnapshot {
                                amount: current_amount,
                                side: current_side,
                                last_check: std::time::Instant::now(),
                            });
                            continue;
                        }

                        // Check if already processed - use consistent fill_id format with WebSocket detection
                        let fill_id = format!("full_{}", client_order_id);
                        let mut processed = processed_fills_position.lock().await;

                        if !processed.contains(&fill_id) {
                            processed.insert(fill_id.clone());
                            drop(processed);

                            tprintln!(
                                "{} {} FILL DETECTED via position change!",
                                "[POSITION_MONITOR]".bright_cyan().bold(),
                                "✓".green().bold()
                            );

                            // Update state to Filled
                            {
                                let mut state = bot_state_position.write().await;
                                state.mark_filled(fill_size, order_side);
                            }

                            // Dual cancellation
                            tprintln!("{} {} Dual cancellation (REST + WebSocket)...",
                                "[POSITION_MONITOR]".bright_cyan().bold(),
                                "⚡".yellow().bold()
                            );

                            let rest_result = pacifica_trading_position
                                .cancel_all_orders(false, Some(&symbol_position), false)
                                .await;

                            match rest_result {
                                Ok(count) => {
                                    tprintln!("{} {} REST API cancelled {} order(s)",
                                        "[POSITION_MONITOR]".bright_cyan().bold(),
                                        "✓".green().bold(),
                                        count
                                    );
                                }
                                Err(e) => {
                                    tprintln!("{} {} REST API cancel failed: {}",
                                        "[POSITION_MONITOR]".bright_cyan().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                }
                            }

                            let ws_result = pacifica_ws_trading_position
                                .cancel_all_orders_ws(false, Some(&symbol_position), false)
                                .await;

                            match ws_result {
                                Ok(count) => {
                                    tprintln!("{} {} WebSocket cancelled {} order(s)",
                                        "[POSITION_MONITOR]".bright_cyan().bold(),
                                        "✓".green().bold(),
                                        count
                                    );
                                }
                                Err(e) => {
                                    tprintln!("{} {} WebSocket cancel failed: {}",
                                        "[POSITION_MONITOR]".bright_cyan().bold(),
                                        "⚠".yellow().bold(),
                                        e
                                    );
                                }
                            }

                            // Estimate fill price (use current entry price or mid)
                            let estimated_price = current_position
                                .and_then(|p| p.entry_price.parse::<f64>().ok())
                                .unwrap_or(0.0);

                            tprintln!("{} Triggering hedge for position-detected fill",
                                "[POSITION_MONITOR]".bright_cyan().bold()
                            );

                            // Trigger hedge
                            hedge_tx_position.send((order_side, fill_size, estimated_price)).await.ok();
                        } else {
                            debug!("[POSITION_MONITOR] Fill already processed by another detection method");
                        }

                        // Update snapshot
                        let mut snapshot = last_position_snapshot_clone.lock().await;
                        *snapshot = Some(PositionSnapshot {
                            amount: current_amount,
                            side: current_side,
                            last_check: std::time::Instant::now(),
                        });
                    }
                }
                Err(e) => {
                    debug!("[POSITION_MONITOR] Failed to fetch positions: {}", e);
                }
            }
        }
    });

    // ═══════════════════════════════════════════════════
    // Task 6: Order Monitoring (Profit Check & Refresh)
    // ═══════════════════════════════════════════════════

    let bot_state_monitor = bot_state.clone();
    let pac_prices_monitor = pacifica_prices.clone();
    let hl_prices_monitor = hyperliquid_prices.clone();
    let config_monitor = config.clone();
    let evaluator_monitor = evaluator.clone();
    // pacifica_trading_monitor already defined at top level (no clone needed)
    let hyperliquid_trading_monitor = Arc::clone(&hyperliquid_trading);

    tokio::spawn(async move {
        let mut monitor_interval = interval(Duration::from_millis(25)); // Check every 25ms (40 Hz)
        let mut log_interval = interval(Duration::from_secs(2)); // Log profit every 2 seconds

        // Rate limit tracking for cancellations
        let mut cancel_rate_limit = RateLimitTracker::new();

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

            // Check 1: Order age (refresh if > order_refresh_interval_secs)
            if active_order.placed_at.elapsed() > Duration::from_secs(config_monitor.order_refresh_interval_secs) {
                // Check if we're in rate limit backoff period
                if cancel_rate_limit.should_skip() {
                    let remaining = cancel_rate_limit.remaining_backoff_secs();
                    debug!("[MONITOR] Skipping age cancellation (rate limit backoff, {:.1}s remaining)", remaining);
                    continue;
                }

                let (hl_bid, hl_ask) = *hl_prices_monitor.lock().unwrap();
                let age_ms = active_order.placed_at.elapsed().as_millis();

                // *** CRITICAL FIX: Check if order has fills BEFORE cancelling ***
                // If order is filled but WebSocket missed it, don't cancel - let REST fill detection handle it
                let filled_check_result = pacifica_trading_monitor.get_open_orders().await;

                match filled_check_result {
                    Ok(orders) => {
                        if let Some(order) = orders.iter().find(|o| o.client_order_id == active_order.client_order_id) {
                            let filled_amount: f64 = order.filled_amount.parse().unwrap_or(0.0);

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
                    config_monitor.order_refresh_interval_secs,
                    format!("${:.4}", active_order.price).cyan(),
                    format!("${:.4}", hl_bid).cyan(),
                    format!("${:.4}", hl_ask).cyan(),
                    "Refreshing".yellow()
                );

                // Cancel all orders for this symbol (only if no fills detected above)
                let cancel_result = pacifica_trading_monitor
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
                                cancel_rate_limit.consecutive_errors
                            );
                        } else {
                            // Other error - log but don't backoff
                            tprintln!("{} {} Failed to cancel order: {}", "[MONITOR]".yellow().bold(), "✗".red().bold(), e);
                        }

                        // Check if order is still active (might have been filled/cancelled)
                        let state = bot_state_monitor.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        continue;
                    }
                }

                // Clear active order ONLY if not filled/hedging
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
                    match pacifica_trading_monitor.get_best_bid_ask_rest(&config_monitor.symbol, config_monitor.agg_level).await {
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
                let filled_check_result = pacifica_trading_monitor.get_open_orders().await;

                match filled_check_result {
                    Ok(orders) => {
                        if let Some(order) = orders.iter().find(|o| o.client_order_id == active_order.client_order_id) {
                            let filled_amount: f64 = order.filled_amount.parse().unwrap_or(0.0);

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
                let cancel_result = pacifica_trading_monitor
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
                                cancel_rate_limit.consecutive_errors
                            );
                        } else {
                            // Other error - log but don't backoff
                            tprintln!("{} {} Failed to cancel order: {}", "[MONITOR]".yellow().bold(), "✗".red().bold(), e);
                        }

                        // Check if order is still active (might have been filled/cancelled)
                        let state = bot_state_monitor.read().await;
                        if state.active_order.is_none() {
                            // Order already cleared by fill detection or another task
                            continue;
                        }
                        drop(state);
                        continue;
                    }
                }

                // Clear active order ONLY if not filled/hedging
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
                    match pacifica_trading_monitor.get_best_bid_ask_rest(&config_monitor.symbol, config_monitor.agg_level).await {
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
                                    order_placement_rate_limit.consecutive_errors
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
