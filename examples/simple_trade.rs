use xemm_rust::connector::pacifica::{
    OrderbookClient, OrderbookConfig, PacificaTrading, PacificaCredentials, OrderSide
};
use std::sync::{Arc, Mutex};
use tracing::info;
use tokio::time::{sleep, Duration};

/// Simple example: Get market price, place order, cancel after 10 seconds
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
    info!("  PACIFICA Simple Trading Example");
    info!("  Place order → Wait 10s → Cancel");
    info!("═══════════════════════════════════════════════════\n");

    // Load credentials from .env
    let credentials = PacificaCredentials::from_env()?;
    info!("[PACIFICA] Account: {}", credentials.account);

    // Shared state for orderbook prices
    let prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
    let prices_clone = prices.clone();

    // Start orderbook client
    let orderbook_config = OrderbookConfig {
        symbol: "SOL".to_string(),
        agg_level: 1,
        reconnect_attempts: 3,
        ping_interval_secs: 30,
    };

    let mut orderbook_client = OrderbookClient::new(orderbook_config)?;

    info!("[PACIFICA] Connecting to orderbook...");

    // Spawn orderbook client in background
    tokio::spawn(async move {
        orderbook_client.start(move |bid, ask, _symbol, _timestamp| {
            let bid_price: f64 = bid.parse().unwrap_or(0.0);
            let ask_price: f64 = ask.parse().unwrap_or(0.0);

            let mut p = prices_clone.lock().unwrap();
            *p = (bid_price, ask_price);
        }).await.ok();
    });

    // Wait for initial orderbook data
    info!("[PACIFICA] Waiting for market data...");
    sleep(Duration::from_secs(3)).await;

    // Get current prices
    let (bid, ask) = *prices.lock().unwrap();

    if bid == 0.0 || ask == 0.0 {
        anyhow::bail!("Failed to receive orderbook data");
    }

    let mid = (bid + ask) / 2.0;
    info!("[PACIFICA] Market: Bid=${:.2} Ask=${:.2} Mid=${:.2}\n", bid, ask, mid);

    // Create trading client (mainnet)
    let mut trading_client = PacificaTrading::new(credentials);

    // Place a buy order 1% below mid
    let symbol = "SOL";
    let size = 0.1; // 0.1 SOL

    info!("[PACIFICA] Placing BUY order:");
    info!("             Size: {} {}", size, symbol);
    info!("             Offset: 1% below mid");

    let order = trading_client.place_limit_order(
        symbol,
        OrderSide::Buy,
        size,
        None,         // Use mid price offset
        1.0,          // 1% below mid
        Some(bid),
        Some(ask),
    ).await?;

    let client_order_id = order.client_order_id
        .expect("Client order ID missing")
        .clone();

    info!("\n[PACIFICA] ✅ Order placed!");
    info!("             Order ID: {}", order.order_id.unwrap_or(0));
    info!("             Client ID: {}", client_order_id);

    // Countdown 10 seconds
    info!("\n[PACIFICA] ⏳ Waiting 10 seconds before cancelling...");
    for i in (1..=10).rev() {
        info!("             {} seconds remaining...", i);
        sleep(Duration::from_secs(1)).await;
    }

    // Cancel order
    info!("\n[PACIFICA] 🗑️  Cancelling order...");
    trading_client.cancel_order(symbol, &client_order_id).await?;

    info!("\n[PACIFICA] ✅ Order cancelled successfully!");
    info!("\n═══════════════════════════════════════════════════");
    info!("  Example Complete!");
    info!("═══════════════════════════════════════════════════\n");

    Ok(())
}
