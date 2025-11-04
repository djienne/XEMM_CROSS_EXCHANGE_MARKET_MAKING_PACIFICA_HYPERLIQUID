use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::types::{
    AccountOrderUpdatesResponse, AccountOrderUpdatesSubscribe, FillEvent, PingMessage,
};

/// Configuration for fill detection client
#[derive(Debug, Clone)]
pub struct FillDetectionConfig {
    /// Account address to monitor
    pub account: String,
    /// Maximum number of reconnection attempts
    pub reconnect_attempts: u32,
    /// Ping interval in seconds
    pub ping_interval_secs: u64,
}

/// WebSocket client for monitoring order fills and updates
pub struct FillDetectionClient {
    config: FillDetectionConfig,
    ws_url: String,
}

impl FillDetectionClient {
    /// Create a new fill detection client
    ///
    /// # Arguments
    /// * `config` - Fill detection configuration
    /// * `is_testnet` - Whether to use testnet (false = mainnet)
    pub fn new(config: FillDetectionConfig, is_testnet: bool) -> Result<Self> {
        let ws_url = if is_testnet {
            "wss://test-ws.pacifica.fi/ws".to_string()
        } else {
            "wss://ws.pacifica.fi/ws".to_string()
        };

        Ok(Self { config, ws_url })
    }

    /// Start the fill detection client with a callback for fill events
    ///
    /// # Arguments
    /// * `callback` - Function called for each fill event (partial fill, full fill, cancellation)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(FillEvent) + Send + 'static,
    {
        let mut attempt = 0;

        loop {
            attempt += 1;
            info!(
                "Fill detection attempt {}/{}",
                attempt, self.config.reconnect_attempts
            );

            match self.connect_and_run(&mut callback).await {
                Ok(_) => {
                    info!("Fill detection client disconnected normally");
                    break;
                }
                Err(e) => {
                    error!("Fill detection error: {}", e);

                    if attempt >= self.config.reconnect_attempts {
                        error!("Max reconnection attempts reached");
                        return Err(e);
                    }

                    // Fast first reconnect (1s), then exponential backoff, capped at 30s
                    let backoff = if attempt == 1 {
                        1
                    } else {
                        std::cmp::min(2u64.pow(attempt - 1), 30)
                    };
                    warn!("Reconnecting in {} seconds...", backoff);
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                }
            }
        }

        Ok(())
    }

    /// Connect to WebSocket and run the monitoring loop
    async fn connect_and_run<F>(&self, callback: &mut F) -> Result<()>
    where
        F: FnMut(FillEvent) + Send + 'static,
    {
        info!("Connecting to Pacifica WebSocket: {}", self.ws_url);

        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to WebSocket")?;

        info!("WebSocket connected successfully");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to account order updates
        let subscribe_msg = AccountOrderUpdatesSubscribe::new(self.config.account.clone());
        let subscribe_json = serde_json::to_string(&subscribe_msg)?;
        write.send(Message::Text(subscribe_json)).await?;
        info!(
            "Subscribed to account_order_updates for account: {}",
            self.config.account
        );

        // Set up ping interval
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));

        loop {
            tokio::select! {
                // Handle incoming messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = self.handle_message(&text, callback) {
                                error!("Error handling message: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("WebSocket closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {}", e);
                            return Err(e.into());
                        }
                        None => {
                            info!("WebSocket stream ended");
                            break;
                        }
                        _ => {}
                    }
                }

                // Send periodic pings
                _ = ping_interval.tick() => {
                    let ping_msg = PingMessage::new();
                    let ping_json = serde_json::to_string(&ping_msg)?;
                    write.send(Message::Text(ping_json)).await?;
                    debug!("Sent ping");
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(FillEvent) + Send + 'static,
    {
        // Try to parse as a generic response first to check channel
        let response: serde_json::Value = serde_json::from_str(text)?;

        if let Some(channel) = response.get("channel").and_then(|v| v.as_str()) {
            match channel {
                "pong" => {
                    debug!("Received pong");
                }
                "account_order_updates" => {
                    // Parse as account order updates response
                    let updates: AccountOrderUpdatesResponse = serde_json::from_str(text)?;
                    debug!(
                        "Received {} order update(s)",
                        updates.data.len()
                    );

                    // Process each order update
                    for update in updates.data {
                        debug!(
                            "Order update - ID: {}, Status: {:?}, Event: {:?}, Filled: {}/{}",
                            update.order_id,
                            update.order_status,
                            update.order_event,
                            update.filled_amount,
                            update.original_amount
                        );

                        // Convert to fill event and call callback if applicable
                        if let Some(fill_event) = update.to_fill_event() {
                            callback(fill_event);
                        }
                    }
                }
                _ => {
                    debug!("Received message on channel: {}", channel);
                }
            }
        }

        Ok(())
    }
}
