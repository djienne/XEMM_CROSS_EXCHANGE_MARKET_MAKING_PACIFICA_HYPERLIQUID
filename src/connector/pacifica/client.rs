use crate::connector::pacifica::types::*;
use anyhow::{anyhow, Result};
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
        F: FnMut(String, String, String, u64) + Send + 'static,
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
        F: FnMut(String, String, String, u64),
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

    /// Handle incoming WebSocket messages
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(String, String, String, u64),
    {
        // First try to parse as generic response to check channel
        let response: WebSocketResponse = serde_json::from_str(text)?;

        match response.channel.as_str() {
            "pong" => {
                debug!("[PACIFICA] Received pong response");
                Ok(())
            }
            "book" => {
                // Parse as orderbook response
                let orderbook_response: OrderbookResponse = serde_json::from_str(text)?;
                let orderbook_data = orderbook_response.data;

                // Extract top of book
                let tob = orderbook_data.get_top_of_book();

                // Call the callback with top of book data
                if let (Some(bid), Some(ask)) = (tob.best_bid, tob.best_ask) {
                    callback(
                        bid.price,
                        ask.price,
                        tob.symbol,
                        tob.timestamp,
                    );
                } else {
                    warn!("[PACIFICA] Incomplete top of book data received");
                }

                Ok(())
            }
            other => {
                debug!("[PACIFICA] Received unknown channel: {}", other);
                Ok(())
            }
        }
    }
}

impl Drop for OrderbookClient {
    fn drop(&mut self) {
        info!("[PACIFICA] OrderbookClient dropped for symbol: {}", self.config.symbol);
    }
}
