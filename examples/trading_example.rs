use xemm_rust::connector::pacifica::{
    OrderbookClient, OrderbookConfig, PacificaTrading, PacificaCredentials, OrderSide
};
use tracing::info;
use tokio::time::{sleep, Duration};

/// Example: Place a limit order and cancel it after 10 seconds
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    info!("═══════════════════════════════════════════════════");
    info!("  PACIFICA Trading Example");
    info!("═══════════════════════════════════════════════════");
    info!("This example demonstrates placing and cancelling orders");
    info!("═══════════════════════════════════════════════════");
    info!("");

    // Load credentials from .env
    let credentials = PacificaCredentials::from_env()?;
    info!("[PACIFICA] Loaded credentials for account: {}", credentials.account);

    // Create trading client (mainnet)
    let mut trading_client = PacificaTrading::new(credentials);

    // Create orderbook client to get current prices
    let orderbook_config = OrderbookConfig {
        symbol: "SOL".to_string(),
        agg_level: 1,
        reconnect_attempts: 3,
        ping_interval_secs: 30,
    };

    let mut orderbook_client = OrderbookClient::new(orderbook_config)?;

    info!("[PACIFICA] Connecting to orderbook WebSocket...");

    // Connect to orderbook
    tokio::spawn(async move {
        orderbook_client.start(|_bid, _ask, _symbol, _timestamp| {
            // Just consume updates, don't log
        }).await.ok();
    });

    // Wait for initial orderbook data
    sleep(Duration::from_secs(3)).await;

    // Get current market data
    info!("[PACIFICA] Fetching current SOL prices...");

    // For this example, we'll need to get bid/ask from another source
    // In production, you'd pass the orderbook data from the client above
    // For now, let's use a simple approach with exact price

    let symbol = "SOL";
    let order_size = 0.1; // 0.1 SOL

    // Example 1: Place buy order at 1% below current mid price
    info!("");
    info!("═══════════════════════════════════════════════════");
    info!("  Example 1: Place BUY order with mid price offset");
    info!("═══════════════════════════════════════════════════");

    // You would get these from your orderbook client in production
    let current_bid = 150.0; // Example bid price
    let current_ask = 150.2; // Example ask price

    info!("[PACIFICA] Current market: Bid=${} Ask=${}", current_bid, current_ask);

    let buy_order = trading_client.place_limit_order(
        symbol,
        OrderSide::Buy,
        order_size,
        None,  // Use mid price offset
        1.0,   // 1% below mid price
        Some(current_bid),
        Some(current_ask),
    ).await?;

    let buy_client_order_id = buy_order.client_order_id
        .as_ref()
        .expect("Client order ID should be present")
        .clone();

    info!("[PACIFICA] ✅ Buy order placed successfully!");
    info!("[PACIFICA]    Order ID: {}", buy_order.order_id.unwrap_or(0));
    info!("[PACIFICA]    Client Order ID: {}", buy_client_order_id);
    info!("");

    // Wait 10 seconds
    info!("[PACIFICA] ⏳ Waiting 10 seconds before cancelling...");
    sleep(Duration::from_secs(10)).await;

    // Cancel the buy order
    info!("[PACIFICA] 🗑️  Cancelling buy order...");
    trading_client.cancel_order(symbol, &buy_client_order_id).await?;
    info!("[PACIFICA] ✅ Buy order cancelled successfully!");
    info!("");

    // Example 2: Place sell order at exact price
    info!("═══════════════════════════════════════════════════");
    info!("  Example 2: Place SELL order at exact price");
    info!("═══════════════════════════════════════════════════");

    let exact_sell_price = 151.0; // Specific price
    info!("[PACIFICA] Placing sell order at exact price: ${}", exact_sell_price);

    let sell_order = trading_client.place_limit_order(
        symbol,
        OrderSide::Sell,
        order_size,
        Some(exact_sell_price),  // Exact price
        1.0,   // Not used when exact price is provided
        None,  // Not needed
        None,  // Not needed
    ).await?;

    let sell_client_order_id = sell_order.client_order_id
        .as_ref()
        .expect("Client order ID should be present")
        .clone();

    info!("[PACIFICA] ✅ Sell order placed successfully!");
    info!("[PACIFICA]    Order ID: {}", sell_order.order_id.unwrap_or(0));
    info!("[PACIFICA]    Client Order ID: {}", sell_client_order_id);
    info!("");

    // Wait 10 seconds
    info!("[PACIFICA] ⏳ Waiting 10 seconds before cancelling...");
    sleep(Duration::from_secs(10)).await;

    // Cancel the sell order
    info!("[PACIFICA] 🗑️  Cancelling sell order...");
    trading_client.cancel_order(symbol, &sell_client_order_id).await?;
    info!("[PACIFICA] ✅ Sell order cancelled successfully!");
    info!("");

    info!("═══════════════════════════════════════════════════");
    info!("  Trading Example Complete!");
    info!("═══════════════════════════════════════════════════");

    Ok(())
}
