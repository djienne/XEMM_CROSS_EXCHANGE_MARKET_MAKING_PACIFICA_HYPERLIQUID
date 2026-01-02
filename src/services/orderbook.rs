use std::sync::Arc;
use anyhow::{Context, Result};
use colored::Colorize;
use fast_float::parse;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::services::market_event::{MarketEvent, MarketEventHub, MarketSource};
use crate::services::ws_health::WsHealth;
use crate::connector::pacifica::{OrderbookClient as PacificaOrderbookClient, OrderbookConfig as PacificaOrderbookConfig};
use crate::connector::hyperliquid::{OrderbookClient as HyperliquidOrderbookClient, OrderbookConfig as HyperliquidOrderbookConfig};
use crate::util::atomic_price::AtomicPrice;

/// Pacifica orderbook service
///
/// Subscribes to Pacifica orderbook WebSocket and updates shared price state.
/// Provides real-time bid/ask prices for opportunity evaluation.
pub struct PacificaOrderbookService {
    pub prices: Arc<AtomicPrice>,
    pub market_events: Arc<MarketEventHub>,
    pub ws_health: Arc<WsHealth>,
    pub reconnect_rx: watch::Receiver<u64>,
    pub symbol: String,
    pub agg_level: u32,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl PacificaOrderbookService {
    pub async fn run(self) -> Result<()> {
        let pacifica_ob_config = PacificaOrderbookConfig {
            symbol: self.symbol.clone(),
            agg_level: self.agg_level,
            reconnect_attempts: self.reconnect_attempts,
            ping_interval_secs: self.ping_interval_secs,
        };

        let mut reconnect_rx = self.reconnect_rx;

        info!("{} Starting orderbook client", "[PACIFICA_OB]".magenta().bold());

        loop {
            let mut pacifica_ob_client = PacificaOrderbookClient::new(pacifica_ob_config.clone())
                .context("Failed to create Pacifica orderbook client")?;

            let pac_prices_clone = self.prices.clone();
            let market_events = self.market_events.clone();
            let ws_health = self.ws_health.clone();

            let start_fut = pacifica_ob_client.start(move |bid, ask, _symbol, _ts| {
                let bid_price: f64 = parse(&bid).unwrap_or(0.0);
                let ask_price: f64 = parse(&ask).unwrap_or(0.0);
                if bid_price > 0.0 && ask_price > 0.0 {
                    pac_prices_clone.store(bid_price, ask_price, _ts);
                    ws_health.record_update(MarketSource::Pacifica, WsHealth::now_ms());
                    market_events.push(MarketEvent { source: MarketSource::Pacifica, ts: _ts });
                }
            });

            tokio::select! {
                result = start_fut => {
                    if let Err(e) = result {
                        warn!("[PACIFICA_OB] Orderbook client exited with error: {}", e);
                    }
                }
                changed = reconnect_rx.changed() => {
                    if changed.is_ok() {
                        warn!("[PACIFICA_OB] Reconnect requested by health monitor");
                    } else {
                        break;
                    }
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
    pub prices: Arc<AtomicPrice>,
    pub market_events: Arc<MarketEventHub>,
    pub ws_health: Arc<WsHealth>,
    pub reconnect_rx: watch::Receiver<u64>,
    pub symbol: String,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl HyperliquidOrderbookService {
    pub async fn run(self) -> Result<()> {
        let hyperliquid_ob_config = HyperliquidOrderbookConfig {
            coin: self.symbol.clone(),
            reconnect_attempts: self.reconnect_attempts,
            ping_interval_secs: self.ping_interval_secs,
        };

        let mut reconnect_rx = self.reconnect_rx;

        info!("{} Starting orderbook client", "[HYPERLIQUID_OB]".magenta().bold());

        loop {
            let mut hyperliquid_ob_client = HyperliquidOrderbookClient::new(hyperliquid_ob_config.clone())
                .context("Failed to create Hyperliquid orderbook client")?;

            let hl_prices_clone = self.prices.clone();
            let market_events = self.market_events.clone();
            let ws_health = self.ws_health.clone();

            let start_fut = hyperliquid_ob_client.start(move |bid, ask, _coin, _ts| {
                let bid_price: f64 = parse(&bid).unwrap_or(0.0);
                let ask_price: f64 = parse(&ask).unwrap_or(0.0);
                if bid_price > 0.0 && ask_price > 0.0 {
                    hl_prices_clone.store(bid_price, ask_price, _ts);
                    ws_health.record_update(MarketSource::Hyperliquid, WsHealth::now_ms());
                    market_events.push(MarketEvent { source: MarketSource::Hyperliquid, ts: _ts });
                }
            });

            tokio::select! {
                result = start_fut => {
                    if let Err(e) = result {
                        warn!("[HYPERLIQUID_OB] Orderbook client exited with error: {}", e);
                    }
                }
                changed = reconnect_rx.changed() => {
                    if changed.is_ok() {
                        warn!("[HYPERLIQUID_OB] Reconnect requested by health monitor");
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
