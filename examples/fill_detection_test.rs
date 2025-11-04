use xemm_rust::connector::pacifica::{
    OrderbookClient, OrderbookConfig, PacificaTrading, PacificaCredentials, OrderSide,
    FillDetectionClient, FillDetectionConfig, FillEvent,
};
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::info;

/// Test script for fill detection functionality
///
/// This example:
/// 1. Connects to Pacifica orderbook to get current SOL prices
/// 2. Places a buy order 0.05% below mid price (likely to fill quickly)
/// 3. Monitors for fills using the fill detection WebSocket client
/// 4. Reports partial fills, full fills, and cancellations
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    info!("════════════════════════════════════════════════");
    info!("  Fill Detection Test");
    info!("════════════════════════════════════════════════");
    info!("");

    // Load credentials from .env
    dotenv::dotenv().ok();
    let credentials = PacificaCredentials::from_env()?;
    info!("✓ Loaded credentials for account: {}", credentials.account);

    // Shared state for current prices
    let prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let prices_clone = prices.clone();

    // Start orderbook client to get real-time prices
    info!("Starting orderbook client for SOL...");
    let orderbook_config = OrderbookConfig {
        symbol: "SOL".to_string(),
        agg_level: 1,
        reconnect_attempts: 5,
        ping_interval_secs: 30,
    };

    let mut orderbook_client = OrderbookClient::new(orderbook_config)?;

    // Spawn orderbook client in background
    tokio::spawn(async move {
        orderbook_client.start(move |bid, ask, _symbol, _timestamp| {
            let bid_price: f64 = bid.parse().unwrap_or(0.0);
            let ask_price: f64 = ask.parse().unwrap_or(0.0);
            *prices_clone.lock().unwrap() = (bid_price, ask_price);
        }).await.ok();
    });

    // Wait for initial price data
    info!("Waiting for price data...");
    sleep(Duration::from_secs(3)).await;

    // Get current prices
    let (bid, ask) = *prices.lock().unwrap();
    if bid == 0.0 || ask == 0.0 {
        anyhow::bail!("Failed to get valid prices from orderbook");
    }

    let mid = (bid + ask) / 2.0;
    info!("Current prices - Bid: ${:.2}, Ask: ${:.2}, Mid: ${:.2}", bid, ask, mid);

    // Calculate order price (0.05% below mid)
    let offset_percent = 0.05;
    let order_price = mid * (1.0 - offset_percent / 100.0);

    info!("");
    info!("════════════════════════════════════════════════");
    info!("  Placing Order");
    info!("════════════════════════════════════════════════");
    info!("Symbol: SOL");
    info!("Side: BUY");
    info!("Size: 0.1 SOL");
    info!("Price: ${:.2} ({}% below mid)", order_price, offset_percent);
    info!("════════════════════════════════════════════════");
    info!("");

    // Start fill detection client in background
    let fill_detection_config = FillDetectionConfig {
        account: credentials.account.clone(),
        reconnect_attempts: 5,
        ping_interval_secs: 30,
    };

    let mut fill_detection_client = FillDetectionClient::new(fill_detection_config, false)?;

    // Track fill events
    let fill_received = Arc::new(Mutex::new(false));
    let fill_received_clone = fill_received.clone();

    info!("Starting fill detection monitor...");
    tokio::spawn(async move {
        fill_detection_client.start(move |fill_event| {
            match fill_event {
                FillEvent::PartialFill {
                    order_id,
                    client_order_id,
                    symbol,
                    side,
                    filled_amount,
                    original_amount,
                    avg_price,
                    timestamp,
                } => {
                    info!("");
                    info!("🟡 ════════════════════════════════════════════════");
                    info!("🟡 PARTIAL FILL DETECTED!");
                    info!("🟡 ════════════════════════════════════════════════");
                    info!("Order ID: {}", order_id);
                    if let Some(cloid) = client_order_id {
                        info!("Client Order ID: {}", cloid);
                    }
                    info!("Symbol: {}", symbol);
                    info!("Side: {}", side);
                    info!("Filled: {} / {} SOL", filled_amount, original_amount);
                    info!("Average Price: ${}", avg_price);
                    info!("Timestamp: {}", timestamp);
                    info!("🟡 ════════════════════════════════════════════════");
                    info!("");
                    *fill_received_clone.lock().unwrap() = true;
                }
                FillEvent::FullFill {
                    order_id,
                    client_order_id,
                    symbol,
                    side,
                    filled_amount,
                    avg_price,
                    timestamp,
                } => {
                    info!("");
                    info!("🟢 ════════════════════════════════════════════════");
                    info!("🟢 FULL FILL DETECTED!");
                    info!("🟢 ════════════════════════════════════════════════");
                    info!("Order ID: {}", order_id);
                    if let Some(cloid) = client_order_id {
                        info!("Client Order ID: {}", cloid);
                    }
                    info!("Symbol: {}", symbol);
                    info!("Side: {}", side);
                    info!("Filled: {} SOL", filled_amount);
                    info!("Average Price: ${}", avg_price);
                    info!("Timestamp: {}", timestamp);
                    info!("🟢 ════════════════════════════════════════════════");
                    info!("");
                    *fill_received_clone.lock().unwrap() = true;
                }
                FillEvent::Cancelled {
                    order_id,
                    client_order_id,
                    symbol,
                    side,
                    filled_amount,
                    original_amount,
                    reason,
                    timestamp,
                } => {
                    info!("");
                    info!("🔴 ════════════════════════════════════════════════");
                    info!("🔴 ORDER CANCELLED!");
                    info!("🔴 ════════════════════════════════════════════════");
                    info!("Order ID: {}", order_id);
                    if let Some(cloid) = client_order_id {
                        info!("Client Order ID: {}", cloid);
                    }
                    info!("Symbol: {}", symbol);
                    info!("Side: {}", side);
                    info!("Filled: {} / {} SOL", filled_amount, original_amount);
                    info!("Reason: {}", reason);
                    info!("Timestamp: {}", timestamp);
                    info!("🔴 ════════════════════════════════════════════════");
                    info!("");
                    *fill_received_clone.lock().unwrap() = true;
                }
            }
        }).await.ok();
    });

    // Give fill detection time to connect and subscribe
    sleep(Duration::from_secs(2)).await;

    // Place the order
    let mut trading_client = PacificaTrading::new(credentials);

    info!("Placing order...");
    let order = trading_client.place_limit_order(
        "SOL",
        OrderSide::Buy,
        0.1,  // Small size for testing
        Some(order_price),  // Exact price
        0.05,  // Offset (ignored when exact price is provided)
        Some(bid),
        Some(ask),
    ).await?;

    let client_order_id = order.client_order_id.clone().unwrap();
    info!("✓ Order placed successfully!");
    info!("  Order ID: {}", order.order_id.unwrap());
    info!("  Client Order ID: {}", client_order_id);
    info!("");

    // Monitor for fills or timeout
    info!("Monitoring for fills (will wait up to 60 seconds)...");
    info!("The order is placed very close to mid price, so it should fill quickly.");
    info!("");

    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    loop {
        sleep(Duration::from_secs(1)).await;

        // Check if fill was received
        if *fill_received.lock().unwrap() {
            info!("Fill detection test completed successfully!");
            break;
        }

        // Check for timeout
        if start_time.elapsed() > timeout {
            info!("");
            info!("⏱️  Timeout reached (60 seconds)");
            info!("No fill detected - order may still be open on the exchange");
            info!("Attempting to cancel order...");

            match trading_client.cancel_order("SOL", &client_order_id).await {
                Ok(_) => {
                    info!("✓ Order cancelled successfully");
                    info!("The cancellation should be detected by the fill detection monitor");

                    // Wait a bit more to see if we get the cancellation event
                    sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    info!("✗ Failed to cancel order: {}", e);
                }
            }
            break;
        }

        // Show periodic status
        if start_time.elapsed().as_secs() % 10 == 0 {
            info!("Still monitoring... ({} seconds elapsed)", start_time.elapsed().as_secs());
        }
    }

    info!("");
    info!("════════════════════════════════════════════════");
    info!("Test completed. Exiting in 3 seconds...");
    info!("════════════════════════════════════════════════");

    sleep(Duration::from_secs(3)).await;

    Ok(())
}
