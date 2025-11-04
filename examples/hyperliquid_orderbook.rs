use xemm_rust::connector::hyperliquid::{OrderbookClient, OrderbookConfig};
use xemm_rust::Config;
use tracing::info;

/// Example: Subscribe to Hyperliquid orderbook and print top of book
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    // Load configuration from config.json
    let app_config = Config::load_default()?;

    info!("═══════════════════════════════════════════════════");
    info!("  HYPERLIQUID Orderbook Example");
    info!("═══════════════════════════════════════════════════");
    info!("Subscribing to {} orderbook on mainnet", app_config.symbol);
    info!("Press Ctrl+C to stop");
    info!("═══════════════════════════════════════════════════");
    info!("");

    // Configure the client using settings from config.json
    let config = OrderbookConfig {
        coin: app_config.symbol,
        reconnect_attempts: app_config.reconnect_attempts,
        ping_interval_secs: app_config.ping_interval_secs,
        request_interval_ms: 500,  // Request every 500ms (2 updates per second)
    };

    // Create client
    let mut client = OrderbookClient::new(config)?;

    // Start client with callback for top of book updates
    client.start(|best_bid, best_ask, coin, timestamp| {
        info!(
            "[HYPERLIQUID] {} | Bid: {} | Ask: {} | Spread: {:.4} | TS: {}",
            coin,
            best_bid,
            best_ask,
            best_ask.parse::<f64>().unwrap_or(0.0) - best_bid.parse::<f64>().unwrap_or(0.0),
            timestamp
        );
    }).await?;

    Ok(())
}
