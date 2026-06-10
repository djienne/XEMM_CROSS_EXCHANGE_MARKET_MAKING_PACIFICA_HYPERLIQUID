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

    /// Legacy String-typed callback wrapper (examples/tools). The bot's hot
    /// path uses `start_top_of_book` directly.
    ///
    /// # Arguments
    /// * `callback` - Function called on each update with (best_bid, best_ask, coin, timestamp)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(String, String, String, u64) + Send + 'static,
    {
        let coin = self.config.coin.clone();
        self.start_top_of_book(move |bid, ask, ts| {
            callback(bid.to_string(), ask.to_string(), coin.clone(), ts)
        })
        .await
    }

    /// Start the client with an f64 top-of-book callback (hot path).
    ///
    /// Each l2Book frame is parsed in a single pass that extracts only the
    /// best level per side (deeper levels are skipped without allocation).
    ///
    /// # Arguments
    /// * `callback` - Function called with (best_bid, best_ask, exchange_timestamp_ms)
    pub async fn start_top_of_book<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(f64, f64, u64) + Send + 'static,
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
                    warn!("[HYPERLIQUID] Reconnecting in {} seconds...", backoff_secs);
                    sleep(Duration::from_secs(backoff_secs)).await;
                }
            }
        }

        Ok(())
    }

    /// Connect to WebSocket and run the main loop
    async fn connect_and_run<F>(&mut self, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, u64) + Send + 'static,
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
        info!(
            "[HYPERLIQUID] Subscribed to l2Book for {}",
            self.config.coin
        );

        // Create interval for ping
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        ping_interval.tick().await; // Skip first tick

        // Staleness watchdog (mirrors the Pacifica clients): a half-open socket
        // can stop delivering frames while pings still write successfully for
        // minutes (TCP send buffer). Without this, the quote goes stale, the
        // QuoteStale gate blocks quoting, and nothing triggers a reconnect
        // until the OS finally fails a write.
        let stale_after =
            Duration::from_secs((self.config.ping_interval_secs.max(1)).saturating_mul(3));
        let mut stale_check = interval(Duration::from_secs(self.config.ping_interval_secs.max(1)));
        stale_check.tick().await;
        let mut last_inbound = tokio::time::Instant::now();

        loop {
            tokio::select! {
                // Handle incoming messages
                msg = read.next() => {
                    last_inbound = tokio::time::Instant::now();
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            // Errors are already handled gracefully in handle_message
                            self.handle_message(&text, callback).ok();
                        }
                        Some(Ok(Message::Ping(data))) => {
                            debug!("[HYPERLIQUID] Received ping, sending pong");
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Pong(_))) => {
                            debug!("[HYPERLIQUID] Received pong");
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("[HYPERLIQUID] Received close message");
                            break;
                        }
                        Some(Err(e)) => {
                            error!("[HYPERLIQUID] WebSocket error: {}", e);
                            return Err(e.into());
                        }
                        None => {
                            warn!("[HYPERLIQUID] WebSocket stream ended");
                            break;
                        }
                        _ => {}
                    }
                }

                // Send ping periodically
                _ = ping_interval.tick() => {
                    debug!("[HYPERLIQUID] Sending ping");
                    write.send(Message::Ping(vec![])).await?;
                }

                // Staleness watchdog. Return Err (not break) so `start` treats it
                // as a connection failure and reconnects + resubscribes, rather
                // than exiting as a graceful close.
                _ = stale_check.tick() => {
                    if last_inbound.elapsed() > stale_after {
                        return Err(anyhow::anyhow!(
                            "[HYPERLIQUID] No inbound frame for {:?}; socket stale",
                            last_inbound.elapsed()
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, u64) + Send + 'static,
    {
        // Hot path: nearly every frame on this socket is an l2Book frame.
        // Parse it directly in ONE pass that extracts only the top level per
        // side straight to f64 (deeper levels are skipped without allocation).
        // The envelope parse below runs only for the rare pong/ack frames.
        if let Ok(frame) = serde_json::from_str::<L2BookTopFrame>(text) {
            if frame.channel == "l2Book" {
                if let (Some(bid), Some(ask)) =
                    (frame.data.levels.best_bid, frame.data.levels.best_ask)
                {
                    callback(bid, ask, frame.data.time);
                }
                return Ok(());
            }
        }

        // Rare path: pong / subscription acks / unknown frames.
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
                // Keep for backward compatibility or other post requests
                debug!("[HYPERLIQUID] Received post response (unexpected for subscription model)");
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
        info!(
            "[HYPERLIQUID] OrderbookClient dropped for coin: {}",
            self.config.coin
        );
    }
}

#[async_trait::async_trait]
impl crate::services::price_source::PriceStream for OrderbookClient {
    fn label(&self) -> &'static str {
        "HYPERLIQUID_OB"
    }
    async fn run_with(
        &mut self,
        mut cb: crate::services::price_source::BookCallback,
    ) -> anyhow::Result<()> {
        self.start_top_of_book(move |bid, ask, ts| cb(bid, ask, ts))
            .await
    }
}
