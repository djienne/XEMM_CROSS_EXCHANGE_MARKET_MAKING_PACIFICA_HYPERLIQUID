use anyhow::{Context, Result};
use fast_float::parse;
use futures_util::{SinkExt, StreamExt};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use super::types::{HlWsMessage, SubscriptionMessage, SubscriptionParams};

const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Configuration for the orderbook client
#[derive(Debug, Clone)]
pub struct OrderbookConfig {
    pub coin: String,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl Default for OrderbookConfig {
    fn default() -> Self {
        Self {
            coin: "BTC".to_string(),
            reconnect_attempts: 5,
            ping_interval_secs: 30,
        }
    }
}

/// Hyperliquid orderbook WebSocket client
pub struct OrderbookClient {
    config: OrderbookConfig,
    ws_url: String,
}

impl OrderbookClient {
    /// Create a new orderbook client
    pub fn new(config: OrderbookConfig) -> Result<Self> {
        info!(
            "[HYPERLIQUID] Initializing orderbook client for {} on mainnet",
            config.coin
        );

        Ok(Self {
            config,
            ws_url: MAINNET_WS_URL.to_string(),
        })
    }

    /// Start the client and call the callback for each top-of-book update
    ///
    /// # Arguments
    /// * `callback` - Function called on each update with (best_bid, best_ask, coin, timestamp)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64) + Send + 'static,
    {
        let mut reconnect_count = 0;

        loop {
            match self.connect_and_run(&mut callback).await {
                Ok(_) => {
                    info!("[HYPERLIQUID] Connection closed gracefully");
                    break;
                }
                Err(e) => {
                    reconnect_count += 1;
                    error!(
                        "[HYPERLIQUID] Connection error (attempt {}/{}): {}",
                        reconnect_count, self.config.reconnect_attempts, e
                    );

                    if reconnect_count >= self.config.reconnect_attempts {
                        error!("[HYPERLIQUID] Max reconnection attempts reached");
                        return Err(e);
                    }

                    // Fast first reconnect (1s), then exponential backoff, capped at 30s
                    let backoff_secs = if reconnect_count == 1 {
                        1
                    } else {
                        std::cmp::min(2_u64.pow(reconnect_count - 1), 30)
                    };
                    warn!(
                        "[HYPERLIQUID] Reconnecting in {} seconds...",
                        backoff_secs
                    );
                    sleep(Duration::from_secs(backoff_secs)).await;
                }
            }
        }

        Ok(())
    }

    /// Connect to WebSocket and run the main loop
    async fn connect_and_run<F>(&mut self, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64) + Send + 'static,
    {
        info!("[HYPERLIQUID] Connecting to {}", self.ws_url);

        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to WebSocket")?;

        info!("[HYPERLIQUID] WebSocket connected successfully");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to L2 book
        let subscribe_msg = SubscriptionMessage {
            method: "subscribe".to_string(),
            subscription: SubscriptionParams {
                type_: "l2Book".to_string(),
                coin: self.config.coin.clone(),
            },
        };
        let subscribe_json = serde_json::to_string(&subscribe_msg)?;
        debug!("[HYPERLIQUID] Sending subscription: {}", subscribe_json);
        write.send(Message::Text(subscribe_json)).await?;
        info!("[HYPERLIQUID] Subscribed to l2Book for {}", self.config.coin);

        // Create interval for ping
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        ping_interval.tick().await; // Skip first tick

        loop {
            tokio::select! {
                // Handle incoming messages
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = self.handle_message(&text, callback) {
                                debug!("[HYPERLIQUID] Message handling error: {}", e);
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            debug!("[HYPERLIQUID] Received ping, sending pong");
                            write.send(Message::Pong(data)).await?;
                        }
                        Ok(Message::Pong(_)) => {
                            debug!("[HYPERLIQUID] Received pong");
                        }
                        Ok(Message::Close(_)) => {
                            info!("[HYPERLIQUID] Received close message");
                            break;
                        }
                        Err(e) => {
                            error!("[HYPERLIQUID] WebSocket error: {}", e);
                            return Err(e.into());
                        }
                        _ => {}
                    }
                }

                // Send ping periodically
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![])).await?;
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message (single-pass parsing for low latency)
    #[inline]
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64) + Send + 'static,
    {
        // Single-pass parsing using unified HlWsMessage enum
        let msg = match HlWsMessage::parse_fast(text) {
            Ok(m) => m,
            Err(_) => return Ok(()), // Silently ignore unparseable messages in hot path
        };

        match msg {
            HlWsMessage::L2Book { channel, data } if channel == "l2Book" => {
                // Extract top of book directly without extra allocation
                if data.levels.len() >= 2 {
                    let bids = &data.levels[0];
                    let asks = &data.levels[1];
                    if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
                        // Parse prices in hot path for lowest latency
                        let bid_price: f64 = parse(&best_bid.px).unwrap_or(0.0);
                        let ask_price: f64 = parse(&best_ask.px).unwrap_or(0.0);
                        if bid_price > 0.0 && ask_price > 0.0 {
                            callback(
                                bid_price,
                                ask_price,
                                &data.coin,
                                data.time,
                            );
                        }
                    }
                }
            }
            HlWsMessage::SubscriptionResponse { channel, .. } if channel == "subscriptionResponse" => {
                // Subscription confirmed - no action needed in hot path
            }
            _ => {
                // Unknown/other message types - ignore silently in hot path
            }
        }

        Ok(())
    }
}

impl Drop for OrderbookClient {
    fn drop(&mut self) {
        info!("[HYPERLIQUID] OrderbookClient dropped for coin: {}", self.config.coin);
    }
}
