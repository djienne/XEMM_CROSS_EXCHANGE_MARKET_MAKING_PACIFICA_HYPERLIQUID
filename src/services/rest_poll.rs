use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::interval;
use tracing::debug;

use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;

/// Pacifica REST API polling service
///
/// Complements WebSocket orderbook by polling REST API periodically.
/// Provides redundancy if WebSocket connection is lost or delayed.
/// Updates shared price state at configured interval.
pub struct PacificaRestPollService {
    pub prices: Arc<Mutex<(f64, f64)>>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub symbol: String,
    pub agg_level: u32,
    pub poll_interval_secs: u64,
}

impl PacificaRestPollService {
    pub async fn run(self) {
        let mut interval_timer = interval(Duration::from_secs(self.poll_interval_secs));
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;

            match self.pacifica_trading.get_best_bid_ask_rest(&self.symbol, self.agg_level).await {
                Ok(Some((bid, ask))) => {
                    // Update shared orderbook prices
                    *self.prices.lock().unwrap() = (bid, ask);
                    debug!(
                        "[PACIFICA_REST] Updated prices via REST: bid=${:.4}, ask=${:.4}",
                        bid, ask
                    );
                }
                Ok(None) => {
                    debug!("[PACIFICA_REST] No bid/ask available from REST API");
                }
                Err(e) => {
                    debug!("[PACIFICA_REST] Failed to fetch prices: {}", e);
                }
            }
        }
    }
}

/// Hyperliquid REST API polling service
///
/// Complements WebSocket orderbook by polling REST API periodically.
/// Provides redundancy if WebSocket connection is lost or delayed.
/// Updates shared price state at configured interval (typically 2s).
pub struct HyperliquidRestPollService {
    pub prices: Arc<Mutex<(f64, f64)>>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub symbol: String,
    pub poll_interval_secs: u64,
}

impl HyperliquidRestPollService {
    pub async fn run(self) {
        let mut interval_timer = interval(Duration::from_secs(self.poll_interval_secs));
        interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval_timer.tick().await;

            match self.hyperliquid_trading.get_l2_snapshot(&self.symbol).await {
                Ok(Some((bid, ask))) => {
                    // Update shared orderbook prices
                    *self.prices.lock().unwrap() = (bid, ask);
                    debug!(
                        "[HYPERLIQUID_REST] Updated prices via REST: bid=${:.4}, ask=${:.4}",
                        bid, ask
                    );
                }
                Ok(None) => {
                    debug!("[HYPERLIQUID_REST] No bid/ask available from REST API");
                }
                Err(e) => {
                    debug!("[HYPERLIQUID_REST] Failed to fetch prices: {}", e);
                }
            }
        }
    }
}
