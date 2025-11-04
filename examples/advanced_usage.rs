use xemm_rust::connector::pacifica::{OrderbookClient, OrderbookConfig};
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use tracing::{info, warn};
use tokio::time::Duration;

/// Example showing advanced usage with price tracking and statistics
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
        )
        .init();

    info!("Starting Advanced Pacifica Orderbook Client Example");

    // Shared state for tracking prices
    let price_history = Arc::new(Mutex::new(PriceTracker::new(100)));
    let history_clone = price_history.clone();

    // Configure client
    let config = OrderbookConfig {
        symbol: "BTC".to_string(),
        agg_level: 1,
        reconnect_attempts: 5,
        ping_interval_secs: 30,
    };

    let mut client = OrderbookClient::new(config)?;

    // Spawn a task to print statistics every 10 seconds
    let stats_history = price_history.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            let tracker = stats_history.lock().unwrap();
            tracker.print_statistics();
        }
    });

    // Start client with callback
    client.start(move |best_bid, best_ask, symbol, timestamp| {
        let bid_price: f64 = best_bid.parse().unwrap_or(0.0);
        let ask_price: f64 = best_ask.parse().unwrap_or(0.0);
        let mid_price = (bid_price + ask_price) / 2.0;
        let spread = ask_price - bid_price;
        let spread_bps = if bid_price > 0.0 {
            (spread / bid_price) * 10000.0
        } else {
            0.0
        };

        // Update price history
        {
            let mut tracker = history_clone.lock().unwrap();
            tracker.add_update(bid_price, ask_price, mid_price, spread_bps, timestamp);
        }

        info!(
            "{} | Bid: ${:.2} | Ask: ${:.2} | Mid: ${:.2} | Spread: {:.2} bps",
            symbol, bid_price, ask_price, mid_price, spread_bps
        );
    }).await?;

    Ok(())
}

/// Structure to track price updates and calculate statistics
struct PriceTracker {
    max_size: usize,
    updates: VecDeque<PriceUpdate>,
}

struct PriceUpdate {
    bid: f64,
    ask: f64,
    mid: f64,
    spread_bps: f64,
    timestamp: u64,
}

impl PriceTracker {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            updates: VecDeque::with_capacity(max_size),
        }
    }

    fn add_update(&mut self, bid: f64, ask: f64, mid: f64, spread_bps: f64, timestamp: u64) {
        if self.updates.len() >= self.max_size {
            self.updates.pop_front();
        }

        self.updates.push_back(PriceUpdate {
            bid,
            ask,
            mid,
            spread_bps,
            timestamp,
        });
    }

    fn print_statistics(&self) {
        if self.updates.is_empty() {
            warn!("No price updates yet");
            return;
        }

        let count = self.updates.len();
        let avg_mid: f64 = self.updates.iter().map(|u| u.mid).sum::<f64>() / count as f64;
        let avg_spread: f64 = self.updates.iter().map(|u| u.spread_bps).sum::<f64>() / count as f64;

        let min_mid = self.updates.iter().map(|u| u.mid).fold(f64::INFINITY, f64::min);
        let max_mid = self.updates.iter().map(|u| u.mid).fold(f64::NEG_INFINITY, f64::max);

        let min_spread = self.updates.iter().map(|u| u.spread_bps).fold(f64::INFINITY, f64::min);
        let max_spread = self.updates.iter().map(|u| u.spread_bps).fold(f64::NEG_INFINITY, f64::max);

        info!("=== Statistics (last {} updates) ===", count);
        info!("Mid Price  -> Avg: ${:.2} | Min: ${:.2} | Max: ${:.2}", avg_mid, min_mid, max_mid);
        info!("Spread     -> Avg: {:.2} bps | Min: {:.2} bps | Max: {:.2} bps", avg_spread, min_spread, max_spread);
        info!("========================================");
    }
}
