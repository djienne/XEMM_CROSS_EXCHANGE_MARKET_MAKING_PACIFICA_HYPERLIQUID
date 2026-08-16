use std::sync::Arc;
use anyhow::{Context, Result};
use colored::Colorize;
use fast_float::parse;
use tracing::{info, warn};

use crate::connector::pacifica::{OrderbookClient as PacificaOrderbookClient, OrderbookConfig as PacificaOrderbookConfig};
use crate::connector::hyperliquid::{OrderbookClient as HyperliquidOrderbookClient, OrderbookConfig as HyperliquidOrderbookConfig};
use crate::util::atomic_price::AtomicPricePair;

/// Pacifica orderbook service
///
/// Subscribes to Pacifica orderbook WebSocket and updates shared price state.
/// Provides real-time bid/ask prices for opportunity evaluation.
pub struct PacificaOrderbookService {
    pub prices: Arc<AtomicPricePair>,
    pub symbol: String,
    pub agg_level: u32,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl PacificaOrderbookService {
    pub async fn run(self) -> Result<()> {
        loop {
            let pac_prices_clone = self.prices.clone();
            let pacifica_ob_config = PacificaOrderbookConfig {
                symbol: self.symbol.clone(),
                agg_level: self.agg_level,
                reconnect_attempts: self.reconnect_attempts,
                ping_interval_secs: self.ping_interval_secs,
            };

            let mut pacifica_ob_client = PacificaOrderbookClient::new(pacifica_ob_config)
                .context("Failed to create Pacifica orderbook client")?;

            info!("{} Starting orderbook client", "[PACIFICA_OB]".magenta().bold());
            match pacifica_ob_client
                .start(move |bid, ask, _symbol, _ts| {
                    let bid_price: f64 = parse(&bid).unwrap_or(0.0);
                    let ask_price: f64 = parse(&ask).unwrap_or(0.0);
                    pac_prices_clone.store(bid_price, ask_price);
                })
                .await
            {
                Ok(_) => break, // Graceful close
                Err(e) => {
                    warn!("{} {} All reconnect attempts exhausted: {}. Restarting service in 60s...",
                        "[PACIFICA_OB]".magenta().bold(),
                        "⚠".yellow().bold(),
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                }
            }
        }

        Ok(())
    }
}

/// Hyperliquid orderbook service
///
/// Subscribes to Hyperliquid orderbook WebSocket and updates shared price state.
/// Provides real-time bid/ask prices for hedge execution.
pub struct HyperliquidOrderbookService {
    pub prices: Arc<AtomicPricePair>,
    pub symbol: String,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl HyperliquidOrderbookService {
    pub async fn run(self) -> Result<()> {
        loop {
            let hl_prices_clone = self.prices.clone();
            let hyperliquid_ob_config = HyperliquidOrderbookConfig {
                coin: self.symbol.clone(),
                reconnect_attempts: self.reconnect_attempts,
                ping_interval_secs: self.ping_interval_secs,
            };

            let mut hyperliquid_ob_client = HyperliquidOrderbookClient::new(hyperliquid_ob_config)
                .context("Failed to create Hyperliquid orderbook client")?;

            info!("{} Starting orderbook client", "[HYPERLIQUID_OB]".magenta().bold());
            match hyperliquid_ob_client
                .start(move |bid, ask, _coin, _ts| {
                    let bid_price: f64 = parse(&bid).unwrap_or(0.0);
                    let ask_price: f64 = parse(&ask).unwrap_or(0.0);
                    hl_prices_clone.store(bid_price, ask_price);
                })
                .await
            {
                Ok(_) => break, // Graceful close
                Err(e) => {
                    warn!("{} {} All reconnect attempts exhausted: {}. Restarting service in 60s...",
                        "[HYPERLIQUID_OB]".magenta().bold(),
                        "⚠".yellow().bold(),
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                }
            }
        }

        Ok(())
    }
}
