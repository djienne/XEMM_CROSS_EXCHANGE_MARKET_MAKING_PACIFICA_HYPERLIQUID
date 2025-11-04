use anyhow::{Context, Result};
use colored::Colorize;
use tracing::{error, info};
use xemm_rust::config::Config;
use xemm_rust::connector::pacifica::{OrderSide, PacificaCredentials, PacificaTrading, PacificaWsTrading};

/// Test the WebSocket cancel_all_orders functionality
///
/// This test will:
/// 1. Place 2-3 limit orders on Pacifica (using REST API)
/// 2. Use WebSocket cancel_all_orders to cancel them (WEBSOCKET!)
/// 3. Verify that all orders are cancelled
/// 4. Compare with REST API cancel_all_orders
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", "  WebSocket Cancel All Orders Test".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("");

    // Load configuration
    let config = Config::load_default().context("Failed to load config.json")?;

    // Load credentials
    dotenv::dotenv().ok();
    let credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials from environment")?;

    // Create trading clients
    let mut rest_trading = PacificaTrading::new(credentials.clone());
    let ws_trading = PacificaWsTrading::new(credentials.clone(), false); // false = mainnet

    info!("{} Trading clients initialized", "✓".green().bold());

    // Get market info
    let market_info = rest_trading
        .get_market_info()
        .await
        .context("Failed to fetch market info")?;

    let symbol_info = market_info
        .get(&config.symbol)
        .with_context(|| format!("Symbol {} not found", config.symbol))?;

    let tick_size: f64 = symbol_info
        .tick_size
        .parse()
        .context("Failed to parse tick size")?;

    info!(
        "{} Using symbol: {} (tick size: {})",
        "✓".green().bold(),
        config.symbol.bright_white().bold(),
        tick_size
    );

    // Get current market price via REST API
    info!("{} Fetching current market price...", "✓".green().bold());
    let (current_bid, current_ask) = rest_trading
        .get_best_bid_ask_rest(&config.symbol, config.agg_level)
        .await
        .context("Failed to get orderbook")?
        .context("No bid/ask available")?;

    let mid_price = (current_bid + current_ask) / 2.0;
    info!(
        "{} Current market: bid=${:.6}, ask=${:.6}, mid=${:.6}",
        "✓".green().bold(),
        current_bid,
        current_ask,
        mid_price
    );

    println!("");

    // ═══════════════════════════════════════════════════
    // PART 1: Test WebSocket cancel_all_orders
    // ═══════════════════════════════════════════════════

    println!("{}", "═══ PART 1: WebSocket Cancel All Orders ═══".bright_yellow().bold());
    println!("");

    // Step 1: Clear existing orders with REST API
    info!("{} Clearing existing orders (REST API)...", "[SETUP]".cyan().bold());
    match rest_trading
        .cancel_all_orders(false, Some(&config.symbol), false)
        .await
    {
        Ok(count) => info!("{} {} Cleared {} existing order(s)", "[SETUP]".cyan().bold(), "✓".green().bold(), count),
        Err(e) => error!("{} {} Failed: {}", "[SETUP]".cyan().bold(), "✗".red().bold(), e),
    }
    println!("");

    // Step 2: Place test orders via REST API
    info!("{} Placing 3 test orders (REST API)...", "[STEP 1]".blue().bold());

    // Use prices 5% away from market to avoid fills but stay within exchange limits
    let safe_buy_price = current_bid * 0.95; // 5% below current bid
    let safe_sell_price = current_ask * 1.05; // 5% above current ask

    // Calculate size to ensure $11+ notional (safety margin above $10 minimum)
    let min_notional = 11.0;
    let buy_size = (min_notional / safe_buy_price).ceil();
    let sell_size = (min_notional / safe_sell_price).ceil();

    info!(
        "{} Using safe prices: buy=${:.6}, sell=${:.6}",
        "[STEP 1]".blue().bold(),
        safe_buy_price,
        safe_sell_price
    );

    // Place 2 BUY orders
    for i in 1..=2 {
        let price = safe_buy_price - (i as f64 * tick_size * 10.0); // Slightly different prices
        let size = buy_size;

        match rest_trading
            .place_limit_order(
                &config.symbol,
                OrderSide::Buy,
                size,
                Some(price),
                0.0,
                None,
                None,
            )
            .await
        {
            Ok(order_data) => {
                info!(
                    "{} {} BUY order {}: ID={:?}",
                    "[STEP 1]".blue().bold(),
                    "✓".green().bold(),
                    i,
                    order_data.order_id
                );
            }
            Err(e) => {
                error!("{} {} BUY order {} failed: {}", "[STEP 1]".blue().bold(), "✗".red().bold(), i, e);
            }
        }
    }

    // Place 1 SELL order
    match rest_trading
        .place_limit_order(
            &config.symbol,
            OrderSide::Sell,
            sell_size,
            Some(safe_sell_price),
            0.0,
            None,
            None,
        )
        .await
    {
        Ok(order_data) => {
            info!(
                "{} {} SELL order: ID={:?}",
                "[STEP 1]".blue().bold(),
                "✓".green().bold(),
                order_data.order_id
            );
        }
        Err(e) => {
            error!("{} {} SELL order failed: {}", "[STEP 1]".blue().bold(), "✗".red().bold(), e);
        }
    }

    info!("{} {} All test orders placed", "[STEP 1]".blue().bold(), "✓".green().bold());
    println!("");

    // Wait for orders to be registered
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Step 3: Cancel all orders via WEBSOCKET
    info!("{} {} Cancelling via WebSocket...", "[STEP 2]".bright_magenta().bold(), "⚡".yellow().bold());
    match ws_trading
        .cancel_all_orders_ws(false, Some(&config.symbol), false)
        .await
    {
        Ok(count) => {
            println!(
                "{} {} {} Successfully cancelled {} order(s)",
                "[STEP 2]".bright_magenta().bold(),
                "⚡".yellow().bold(),
                "✓".green().bold(),
                count.to_string().bright_white().bold()
            );
            if count >= 3 {
                println!("{} {} WebSocket test PASSED!", "[RESULT]".green().bold(), "✓✓✓".green().bold());
            } else {
                error!(
                    "{} {} Expected 3 orders, cancelled {}",
                    "[RESULT]".red().bold(),
                    "✗✗✗".red().bold(),
                    count
                );
            }
        }
        Err(e) => {
            error!("{} {} WebSocket cancel failed: {}", "[STEP 2]".bright_magenta().bold(), "✗".red().bold(), e);
            error!("{} {} WebSocket test FAILED", "[RESULT]".red().bold(), "✗✗✗".red().bold());
        }
    }

    println!("");
    println!("");

    // ═══════════════════════════════════════════════════
    // PART 2: Compare with REST API (for verification)
    // ═══════════════════════════════════════════════════

    println!("{}", "═══ PART 2: REST API Comparison ═══".bright_yellow().bold());
    println!("");

    // Place another set of orders
    info!("{} Placing 3 more test orders (REST API)...", "[STEP 3]".blue().bold());

    for i in 1..=2 {
        let price = safe_buy_price - (i as f64 * tick_size * 10.0);

        rest_trading
            .place_limit_order(
                &config.symbol,
                OrderSide::Buy,
                buy_size,
                Some(price),
                0.0,
                None,
                None,
            )
            .await
            .ok();
    }

    rest_trading
        .place_limit_order(
            &config.symbol,
            OrderSide::Sell,
            sell_size,
            Some(safe_sell_price),
            0.0,
            None,
            None,
        )
        .await
        .ok();

    info!("{} {} Orders placed", "[STEP 3]".blue().bold(), "✓".green().bold());
    println!("");

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Cancel via REST API for comparison
    info!("{} Cancelling via REST API...", "[STEP 4]".cyan().bold());
    match rest_trading
        .cancel_all_orders(false, Some(&config.symbol), false)
        .await
    {
        Ok(count) => {
            println!(
                "{} {} Successfully cancelled {} order(s)",
                "[STEP 4]".cyan().bold(),
                "✓".green().bold(),
                count.to_string().bright_white().bold()
            );
            if count >= 3 {
                println!("{} {} REST API test PASSED!", "[RESULT]".green().bold(), "✓✓✓".green().bold());
            }
        }
        Err(e) => {
            error!("{} {} REST API cancel failed: {}", "[STEP 4]".cyan().bold(), "✗".red().bold(), e);
        }
    }

    println!("");
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());
    println!("{}", "  Test Complete".bright_cyan().bold());
    println!("{}", "  Both WebSocket and REST API methods tested".bright_cyan().bold());
    println!("{}", "═══════════════════════════════════════════════════".bright_cyan().bold());

    Ok(())
}
