use crate::connector::pacifica::types::{PingMessage, SubscribeMessage, WsMessage};
use anyhow::{anyhow, Result};
use fast_float::parse;
use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, Duration, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const MAINNET_WS_URL: &str = "wss://ws.pacifica.fi/ws";

/// Configuration for the orderbook client
#[derive(Debug, Clone)]
pub struct OrderbookConfig {
    pub symbol: String,
    pub agg_level: u32,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
}

impl Default for OrderbookConfig {
    fn default() -> Self {
        Self {
            symbol: "BTC".to_string(),
            agg_level: 1,
            reconnect_attempts: 5,
            ping_interval_secs: 30,
        }
    }
}

/// Orderbook WebSocket client with health check and auto-reconnect
pub struct OrderbookClient {
    config: OrderbookConfig,
    ws_url: String,
}

impl OrderbookClient {
    /// Create a new orderbook client
    ///
    /// # Arguments
    /// * `config` - Client configuration
    pub fn new(config: OrderbookConfig) -> Result<Self> {
        let ws_url = MAINNET_WS_URL.to_string();

        info!(
            "[PACIFICA] Initializing orderbook client for {} on mainnet",
            config.symbol
        );

        Ok(Self { config, ws_url })
    }

    /// Start the client with a callback for top of book updates
    ///
    /// # Arguments
    /// * `callback` - Function called with (best_bid_price, best_ask_price, symbol, timestamp)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64) + Send + 'static,
    {
        let mut reconnect_count = 0;

        loop {
            match self.connect_and_run(&mut callback).await {
                Ok(_) => {
                    info!("[PACIFICA] WebSocket connection closed gracefully");
                    break;
                }
                Err(e) => {
                    reconnect_count += 1;
                    error!(
                        "[PACIFICA] WebSocket error (attempt {}/{}): {}",
                        reconnect_count, self.config.reconnect_attempts, e
                    );

                    if reconnect_count >= self.config.reconnect_attempts {
                        return Err(anyhow!(
                            "[PACIFICA] Failed to connect after {} attempts",
                            self.config.reconnect_attempts
                        ));
                    }

                    // Fast first reconnect (1s), then exponential backoff, capped at 30s
                    let backoff_secs = if reconnect_count == 1 {
                        1
                    } else {
                        std::cmp::min(2_u64.pow(reconnect_count - 1), 30)
                    };
                    warn!("[PACIFICA] Reconnecting in {} seconds...", backoff_secs);
                    sleep(Duration::from_secs(backoff_secs)).await;
                }
            }
        }

        Ok(())
    }

    /// Internal method to connect and run the WebSocket client
    async fn connect_and_run<F>(&self, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64),
    {
        info!("[PACIFICA] Connecting to {}", self.ws_url);

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        info!("[PACIFICA] WebSocket connected successfully");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to orderbook
        let subscribe_msg = SubscribeMessage::new(
            self.config.symbol.clone(),
            self.config.agg_level,
        );
        let subscribe_json = serde_json::to_string(&subscribe_msg)?;

        debug!("[PACIFICA] Sending subscription: {}", subscribe_json);
        write.send(Message::Text(subscribe_json)).await?;
        info!("[PACIFICA] Subscribed to orderbook for {}", self.config.symbol);

        // Setup ping interval
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        ping_interval.tick().await; // Skip first immediate tick

        // Main event loop
        loop {
            tokio::select! {
                // Handle incoming messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            debug!("[PACIFICA] Received message: {}", text);
                            self.handle_message(&text, callback)?;
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("[PACIFICA] Received close message from server");
                            break;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            debug!("[PACIFICA] Received ping from server");
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Pong(_))) => {
                            debug!("[PACIFICA] Received pong from server");
                        }
                        Some(Err(e)) => {
                            return Err(anyhow!("[PACIFICA] WebSocket error: {}", e));
                        }
                        None => {
                            warn!("[PACIFICA] WebSocket stream ended");
                            break;
                        }
                        _ => {}
                    }
                }

                // Send periodic pings
                _ = ping_interval.tick() => {
                    let ping_msg = PingMessage::new();
                    let ping_json = serde_json::to_string(&ping_msg)?;
                    debug!("[PACIFICA] Sending ping: {}", ping_json);

                    if let Err(e) = write.send(Message::Text(ping_json)).await {
                        error!("[PACIFICA] Failed to send ping: {}", e);
                        return Err(anyhow!("[PACIFICA] Failed to send ping: {}", e));
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket messages (single-pass parsing for low latency)
    #[inline]
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, &str, u64),
    {
        // Single-pass parsing using unified WsMessage enum
        let msg = match WsMessage::parse_fast(text) {
            Ok(m) => m,
            Err(_) => return Ok(()), // Silently ignore unknown/unparseable messages
        };

        match msg {
            WsMessage::Book { channel, data } if channel == "book" => {
                // Extract top of book directly without extra allocation
                if let (Some(bids), Some(asks)) = (data.levels.get(0), data.levels.get(1)) {
                    if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
                        // Parse prices in hot path for lowest latency
                        let bid_price: f64 = parse(&best_bid.price).unwrap_or(0.0);
                        let ask_price: f64 = parse(&best_ask.price).unwrap_or(0.0);
                        if bid_price > 0.0 && ask_price > 0.0 {
                            callback(
                                bid_price,
                                ask_price,
                                &data.symbol,
                                data.timestamp,
                            );
                        }
                    }
                }
            }
            WsMessage::Pong { channel } if channel == "pong" => {
                // Pong received - no action needed
            }
            _ => {
                // Unknown message type - ignore silently in hot path
            }
        }

        Ok(())
    }
}

impl Drop for OrderbookClient {
    fn drop(&mut self) {
        info!("[PACIFICA] OrderbookClient dropped for symbol: {}", self.config.symbol);
    }
}
