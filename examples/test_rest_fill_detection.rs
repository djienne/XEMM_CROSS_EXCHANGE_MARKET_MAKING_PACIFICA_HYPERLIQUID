/// Test REST API Fill Detection (Backup System)
///
/// This test simulates WebSocket fill detection failure and verifies that
/// REST API polling correctly detects fills and triggers hedges.
///
/// Test Scenario:
/// 1. Place a limit order on Pacifica
/// 2. Simulate WebSocket failure (don't start WebSocket task)
/// 3. Wait for order to fill
/// 4. Verify REST API detects fill within 500ms-1s
/// 5. Verify hedge executes correctly
/// 6. Verify no duplicate hedges
///
/// Usage:
/// ```
/// cargo run --example test_rest_fill_detection --release
/// ```

use anyhow::{Context, Result};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{info, debug};
use colored::Colorize;

use xemm_rust::config::Config;
use xemm_rust::connector::pacifica::{
    OpenOrderItem, PacificaCredentials, PacificaTrading, PacificaWsTrading,
};
use xemm_rust::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use xemm_rust::strategy::OrderSide;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("\n{}", "═".repeat(80).bright_cyan());
    println!("{}", "REST API Fill Detection Test".bright_white().bold());
    println!("{}", "═".repeat(80).bright_cyan());
    println!();

    // Load configuration
    let config = Config::load_default().unwrap();
    println!("{} Configuration loaded: symbol={}, notional=${}",
        "[TEST]".bright_yellow().bold(),
        config.symbol.bright_white().bold(),
        config.order_notional_usd
    );

    // Load credentials
    let pacifica_credentials = PacificaCredentials::from_env()
        .context("Failed to load Pacifica credentials")?;
    let hyperliquid_credentials = HyperliquidCredentials::from_env()
        .context("Failed to load Hyperliquid credentials")?;

    println!("{} Credentials loaded", "[TEST]".bright_yellow().bold());

    // Initialize trading clients
    let pacifica_trading = Arc::new(tokio::sync::Mutex::new(
        PacificaTrading::new(pacifica_credentials.clone()).unwrap()
    ));
    let pacifica_ws_trading = Arc::new(
        PacificaWsTrading::new(pacifica_credentials.clone(), false)
    );
    let hyperliquid_trading = Arc::new(
        HyperliquidTrading::new(hyperliquid_credentials, false)?
    );

    println!("{} Trading clients initialized", "[TEST]".bright_yellow().bold());
    println!();

    // Cancel all existing orders first
    println!("{} Cancelling all existing orders...", "[TEST]".bright_yellow().bold());
    match pacifica_trading.lock().await.cancel_all_orders(false, Some(&config.symbol), false).await {
        Ok(count) => println!("{} {} Cancelled {} order(s)", "[TEST]".bright_yellow().bold(), "✓".green().bold(), count),
        Err(e) => println!("{} {} Failed to cancel: {}", "[TEST]".bright_yellow().bold(), "⚠".yellow().bold(), e),
    }
    println!();

    // ═════════════════════════════════════════════════════
    // CRITICAL: Simulating WebSocket Failure
    // We deliberately DO NOT start WebSocket fill detection
    // ═════════════════════════════════════════════════════
    println!("{} {} WebSocket fill detection DISABLED (simulating failure)",
        "[TEST]".bright_yellow().bold(),
        "⚠".red().bold()
    );
    println!("{} REST API is the ONLY fill detection mechanism",
        "[TEST]".bright_yellow().bold()
    );
    println!();

    // ═════════════════════════════════════════════════════
    // REST API Fill Detection (Task 5)
    // ═════════════════════════════════════════════════════

    let processed_fills = Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::<String>::new()));
    let fill_detected = Arc::new(Mutex::new(false));
    let fill_detected_clone = fill_detected.clone();

    let pacifica_trading_rest = pacifica_trading.clone();
    let pacifica_ws_trading_rest = pacifica_ws_trading.clone();
    let symbol_rest = config.symbol.clone();
    let processed_fills_rest = processed_fills.clone();
    let min_hedge_notional = 10.0;

    // Track the order we place
    let active_order_cloid = Arc::new(Mutex::new(Option::<String>::None));
    let active_order_cloid_rest = active_order_cloid.clone();
    let active_order_side = Arc::new(Mutex::new(Option::<OrderSide>::None));
    let active_order_side_rest = active_order_side.clone();

    // Hedge channel
    let (hedge_tx, mut hedge_rx) = mpsc::channel::<(OrderSide, f64, f64)>(1);
    let hedge_tx_rest = hedge_tx.clone();

    tokio::spawn(async move {
        let mut poll_interval = interval(Duration::from_millis(500)); // Poll every 500ms
        poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut consecutive_errors = 0u32;
        let mut last_known_filled_amount: f64 = 0.0;

        loop {
            poll_interval.tick().await;

            // Get active order info
            let cloid_opt = active_order_cloid_rest.lock().unwrap().clone();
            let side_opt = active_order_side_rest.lock().unwrap().clone();

            if cloid_opt.is_none() {
                // No active order yet
                continue;
            }

            let client_order_id = cloid_opt.unwrap();
            let order_side = side_opt.unwrap();

            // Fetch open orders via REST API
            let open_orders_result = pacifica_trading_rest.lock().await.get_open_orders().await;

            match open_orders_result {
                Ok(orders) => {
                    consecutive_errors = 0;

                    // Find our order by client_order_id
                    let our_order = orders.iter().find(|o| o.client_order_id == client_order_id);

                    if let Some(order) = our_order {
                        let filled_amount: f64 = order.filled_amount.parse().unwrap_or(0.0);
                        let initial_amount: f64 = order.initial_amount.parse().unwrap_or(0.0);
                        let price: f64 = order.price.parse().unwrap_or(0.0);

                        // Check if there's a NEW fill
                        if filled_amount > last_known_filled_amount && filled_amount > 0.0 {
                            let new_fill_amount = filled_amount - last_known_filled_amount;
                            let notional_value = new_fill_amount * price;

                            println!("{} {} FILL DETECTED via REST API: {} -> {} (new: {}) | Notional: ${:.2}",
                                "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                "✓".green().bold(),
                                last_known_filled_amount,
                                filled_amount,
                                new_fill_amount,
                                notional_value
                            );

                            last_known_filled_amount = filled_amount;

                            let is_full_fill = (filled_amount - initial_amount).abs() < 0.0001;

                            if is_full_fill || notional_value > min_hedge_notional {
                                let fill_type = if is_full_fill { "full" } else { "partial" };
                                let fill_id = format!("{}_{}_rest", fill_type, client_order_id);

                                // Check duplicate
                                let mut processed = processed_fills_rest.lock().await;
                                if processed.contains(&fill_id) {
                                    debug!("[REST_FILL_DETECTION] Fill already processed, skipping");
                                    continue;
                                }
                                processed.insert(fill_id);
                                drop(processed);

                                *fill_detected_clone.lock().unwrap() = true;

                                println!("{} {} {} FILL: {} {} | Notional: ${}",
                                    "[REST_FILL_DETECTION]".bright_cyan().bold(),
                                    "✓".green().bold(),
                                    if is_full_fill { "FULL" } else { "PARTIAL" },
                                    order.side.bright_yellow(),
                                    filled_amount,
                                    format!("{:.2}", notional_value).cyan().bold()
                                );

                                // Trigger hedge
                                println!("{} Triggering hedge...", "[REST_FILL_DETECTION]".bright_cyan().bold());
                                hedge_tx_rest.send((order_side, filled_amount, price)).await.ok();
                            }
                        }
                    } else {
                        // Order no longer in open orders
                        if last_known_filled_amount > 0.0 {
                            debug!("[REST_FILL_DETECTION] Order no longer in open orders");
                            last_known_filled_amount = 0.0;
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    debug!("[REST_FILL_DETECTION] Error: {}", e);
                }
            }
        }
    });

    println!("{} REST API fill detection started (polling every 500ms)",
        "[TEST]".bright_yellow().bold()
    );
    println!();

    // ═════════════════════════════════════════════════════
    // Step 1: Place a limit order
    // ═════════════════════════════════════════════════════

    println!("{}", "═".repeat(80).bright_cyan());
    println!("{} {} Place a limit order on Pacifica", "[TEST]".bright_yellow().bold(), "STEP 1:".bright_white().bold());
    println!("{}", "═".repeat(80).bright_cyan());
    println!();

    // Get market info
    let mut trading = pacifica_trading.lock().await;
    let market_info = trading.get_market_info().await?;
    drop(trading);

    // For testing, we'll use a buy order at current bid (likely to fill quickly)
    println!("{} This test requires MANUAL interaction:", "[TEST]".bright_yellow().bold());
    println!("{} 1. The bot will place a LIMIT order", "[TEST]".bright_yellow().bold());
    println!("{} 2. You need to FILL the order manually (use another account)", "[TEST]".bright_yellow().bold());
    println!("{} 3. Or wait for the market to fill it", "[TEST]".bright_yellow().bold());
    println!();
    println!("{} Press ENTER when ready to place order...", "[TEST]".bright_yellow().bold());

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();

    // Simplified: Just place a small order and let user fill it
    println!("{} Order placement not implemented in this test stub", "[TEST]".bright_yellow().bold());
    println!("{} To complete this test:", "[TEST]".bright_yellow().bold());
    println!("  1. Run the main bot with a modified version that has a WebSocket kill switch");
    println!("  2. Place an order normally");
    println!("  3. Kill WebSocket fill detection");
    println!("  4. Manually fill the order");
    println!("  5. Observe REST API detecting the fill within 500ms-1s");
    println!();

    println!("{}", "═".repeat(80).bright_cyan());
    println!("{} Test stub complete", "[TEST]".bright_yellow().bold());
    println!("{} Implement full test by adding kill switch to main bot", "[TEST]".bright_yellow().bold());
    println!("{}", "═".repeat(80).bright_cyan());

    Ok(())
}
