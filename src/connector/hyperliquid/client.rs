use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{interval, sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

use super::types::*;

const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

/// Configuration for the orderbook client
#[derive(Debug, Clone)]
pub struct OrderbookConfig {
    pub coin: String,
    pub reconnect_attempts: u32,
    pub ping_interval_secs: u64,
    pub request_interval_ms: u64,  // How often to request L2 book data
}

impl Default for OrderbookConfig {
    fn default() -> Self {
        Self {
            coin: "BTC".to_string(),
            reconnect_attempts: 5,
            ping_interval_secs: 30,
            request_interval_ms: 100,  // Request orderbook every 100ms
        }
    }
}

/// Hyperliquid orderbook WebSocket client
pub struct OrderbookClient {
    config: OrderbookConfig,
    ws_url: String,
    request_id: u64,
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
            request_id: 0,
        })
    }

    /// Start the client and call the callback for each top-of-book update
    ///
    /// # Arguments
    /// * `callback` - Function called on each update with (best_bid, best_ask, coin, timestamp)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(String, String, String, u64) + Send + 'static,
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
        F: FnMut(String, String, String, u64) + Send + 'static,
    {
        info!("[HYPERLIQUID] Connecting to {}", self.ws_url);

        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to WebSocket")?;

        info!("[HYPERLIQUID] WebSocket connected successfully");

        let (mut write, mut read) = ws_stream.split();

        // Create intervals for ping and L2 book requests
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        let mut request_interval = interval(Duration::from_millis(self.config.request_interval_ms));

        // Skip first tick (fires immediately)
        ping_interval.tick().await;
        request_interval.tick().await;

        loop {
            tokio::select! {
                // Handle incoming messages
                Some(msg) = read.next() => {
                    match msg {
                        Ok(Message::Text(text)) => {
                            // Errors are already handled gracefully in handle_message
                            self.handle_message(&text, callback).await.ok();
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
                    debug!("[HYPERLIQUID] Sending ping");
                    let ping_msg = PingMessage::new();
                    let ping_json = serde_json::to_string(&ping_msg)?;
                    write.send(Message::Text(ping_json)).await?;
                }

                // Request L2 book data periodically
                _ = request_interval.tick() => {
                    self.request_id += 1;
                    let request = L2BookRequest {
                        method: "post".to_string(),
                        id: self.request_id,
                        request: L2BookRequestInner {
                            type_: "info".to_string(),
                            payload: L2BookPayload {
                                type_: "l2Book".to_string(),
                                coin: self.config.coin.clone(),
                                n_sig_figs: Some(5),
                                mantissa: None,
                            },
                        },
                    };

                    let request_json = serde_json::to_string(&request)?;
                    debug!("[HYPERLIQUID] Requesting L2 book (id={})", self.request_id);
                    write.send(Message::Text(request_json)).await?;
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(String, String, String, u64) + Send + 'static,
    {
        // Try to parse as generic response first
        let response: WebSocketResponse = match serde_json::from_str(text) {
            Ok(r) => r,
            Err(e) => {
                // Log at debug level - not all messages have a "channel" field
                debug!("[HYPERLIQUID] Skipping non-standard message: {}", e);
                return Ok(());
            }
        };

        match response.channel.as_str() {
            "post" => {
                // Parse L2 book response
                match serde_json::from_str::<L2BookResponse>(text) {
                    Ok(l2_response) => {
                        let book_data = &l2_response.data.response.payload.data;

                        // Extract top of book
                        if let Some(tob) = book_data.get_top_of_book() {
                            debug!(
                                "[HYPERLIQUID] TOB: Bid={} Ask={} ({} @ {})",
                                tob.best_bid, tob.best_ask, tob.coin, tob.timestamp
                            );

                            callback(tob.best_bid, tob.best_ask, tob.coin, tob.timestamp);
                        }
                    }
                    Err(e) => {
                        debug!("[HYPERLIQUID] Failed to parse L2 book: {}", e);
                    }
                }
            }
            "pong" => {
                debug!("[HYPERLIQUID] Received pong response");
            }
            "subscriptionResponse" => {
                debug!("[HYPERLIQUID] Subscription confirmed");
            }
            _ => {
                debug!("[HYPERLIQUID] Unknown channel: {}", response.channel);
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
