use anyhow::{Context, Result};
use tracing::{error, info};
use xemm_rust::config::Config;
use xemm_rust::connector::pacifica::{OrderSide, PacificaCredentials, PacificaTrading};

/// Test the cancel_all_orders functionality
///
/// This test will:
/// 1. Place 2-3 limit orders on Pacifica
/// 2. Use cancel_all_orders to cancel them
/// 3. Verify that all orders are cancelled
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("═══════════════════════════════════════════════════");
    info!("  Cancel All Orders Test");
    info!("═══════════════════════════════════════════════════");
    info!("");

    // Load configuration
    let config = Config::load_default().context("Failed to load config.json")?;

    // Load credentials
    dotenv::dotenv().ok();
    let credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials from environment")?;

    // Create trading client
    let mut trading = PacificaTrading::new(credentials.clone());
    info!("[TEST] Trading client initialized");

    // Get market info
    let market_info = trading
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
        "[TEST] Using symbol: {} (tick size: {})",
        config.symbol, tick_size
    );
    info!("");

    // Step 1: Cancel any existing orders first
    info!("[STEP 1] Cancelling any existing orders...");
    match trading
        .cancel_all_orders(false, Some(&config.symbol), false)
        .await
    {
        Ok(count) => info!("[STEP 1] ✓ Cancelled {} existing order(s)", count),
        Err(e) => error!("[STEP 1] Failed to cancel existing orders: {}", e),
    }
    info!("");

    // Step 2: Place a few test orders
    info!("[STEP 2] Placing 3 test limit orders...");

    // Get a reference price (we'll use a price far from market to avoid fills)
    // Orders must be at least $10 notional
    let far_price_buy = 100.0; // Very low price for buy order
    let far_price_sell = 300.0; // Very high price for sell order

    // Place 2 BUY orders with $10+ notional (0.1 SOL @ $100 = $10)
    for i in 1..=2 {
        let price = far_price_buy + (i as f64 * tick_size);
        let size = 0.11; // 0.11 * $100 = $11 (above $10 minimum)

        match trading
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
                    "[STEP 2] ✓ Placed BUY order {}: ID={:?}, cloid={:?}",
                    i, order_data.order_id, order_data.client_order_id
                );
            }
            Err(e) => {
                error!("[STEP 2] Failed to place BUY order {}: {}", i, e);
            }
        }
    }

    // Place 1 SELL order with $10+ notional (0.04 SOL @ $300 = $12)
    let size = 0.04; // 0.04 * $300 = $12 (above $10 minimum)
    match trading
        .place_limit_order(
            &config.symbol,
            OrderSide::Sell,
            size,
            Some(far_price_sell),
            0.0,
            None,
            None,
        )
        .await
    {
        Ok(order_data) => {
            info!(
                "[STEP 2] ✓ Placed SELL order: ID={:?}, cloid={:?}",
                order_data.order_id, order_data.client_order_id
            );
        }
        Err(e) => {
            error!("[STEP 2] Failed to place SELL order: {}", e);
        }
    }

    info!("[STEP 2] All test orders placed");
    info!("");

    // Wait a bit for orders to be registered
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Step 3: Cancel all orders for the symbol
    info!("[STEP 3] Cancelling all orders for {}...", config.symbol);
    match trading
        .cancel_all_orders(false, Some(&config.symbol), false)
        .await
    {
        Ok(count) => {
            info!(
                "[STEP 3] ✓ Successfully cancelled {} order(s)",
                count
            );
            if count >= 3 {
                info!("[TEST] ✓✓✓ TEST PASSED - All orders were cancelled!");
            } else {
                error!(
                    "[TEST] ✗✗✗ TEST FAILED - Expected to cancel 3 orders, but cancelled {}",
                    count
                );
            }
        }
        Err(e) => {
            error!("[STEP 3] ✗ Failed to cancel all orders: {}", e);
            error!("[TEST] ✗✗✗ TEST FAILED - Could not cancel orders");
        }
    }

    info!("");
    info!("═══════════════════════════════════════════════════");
    info!("  Test Complete");
    info!("═══════════════════════════════════════════════════");

    Ok(())
}
