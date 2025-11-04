mod connector;
mod config;

use config::Config;
use connector::pacifica::{OrderbookClient, OrderbookConfig};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging with environment variable control
    // Set RUST_LOG=debug for verbose logging, or RUST_LOG=info for normal logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    // Load configuration from config.json
    let config = Config::load_default()
        .unwrap_or_else(|e| {
            info!("Failed to load config.json: {}. Using default config.", e);
            Config::default()
        });

    // Validate configuration
    if let Err(e) = config.validate() {
        anyhow::bail!("Invalid configuration: {}", e);
    }

    // Create orderbook client configuration from loaded config
    let orderbook_config = OrderbookConfig {
        symbol: config.symbol.clone(),
        agg_level: config.agg_level,
        reconnect_attempts: config.reconnect_attempts,
        ping_interval_secs: config.ping_interval_secs,
    };

    // Create and start the orderbook client
    let mut client = OrderbookClient::new(orderbook_config)?;

    info!("════════════════════════════════════════════");
    info!("  PACIFICA Exchange Connector");
    info!("════════════════════════════════════════════");
    info!("Configuration:");
    info!("  Exchange: PACIFICA");
    info!("  Symbol: {}", config.symbol);
    info!("  Aggregation Level: {}", config.agg_level);
    info!("  Network: Mainnet");
    info!("  Ping Interval: {}s", config.ping_interval_secs);
    info!("  Max Reconnect Attempts: {}", config.reconnect_attempts);
    info!("  Low Latency Mode: {}", if config.low_latency_mode { "ENABLED" } else { "DISABLED" });
    info!("════════════════════════════════════════════");
    info!("Starting PACIFICA orderbook stream...");
    info!("Press Ctrl+C to stop");
    info!("");

    // Start the client and handle top of book updates
    let low_latency = config.low_latency_mode;

    if low_latency {
        // Low-latency mode: minimal processing, log every 100th update
        let mut update_count = 0u64;
        client.start(move |best_bid, best_ask, symbol, timestamp| {
            update_count += 1;

            // Only log every 100th update to reduce I/O overhead
            if update_count % 100 == 0 {
                info!(
                    "[PACIFICA] {} | Bid: ${} | Ask: ${} | TS: {} | Updates: {}",
                    symbol, best_bid, best_ask, timestamp, update_count
                );
            }

            // In production, you would send data to a channel or store for processing
            // instead of logging every update
        }).await?;
    } else {
        // Normal mode: full logging with spread calculations
        client.start(|best_bid, best_ask, symbol, timestamp| {
            // Calculate spread
            let bid_price: f64 = best_bid.parse().unwrap_or(0.0);
            let ask_price: f64 = best_ask.parse().unwrap_or(0.0);
            let spread = ask_price - bid_price;
            let spread_bps = if bid_price > 0.0 {
                (spread / bid_price) * 10000.0
            } else {
                0.0
            };

            info!(
                "[PACIFICA] {} | Bid: ${} | Ask: ${} | Spread: ${:.2} ({:.2} bps) | TS: {}",
                symbol, best_bid, best_ask, spread, spread_bps, timestamp
            );
        }).await?;
    }

    Ok(())
}
