use crate::connector::pacifica::types::*;
use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{interval, sleep, Duration};
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

    /// Legacy String-typed callback wrapper (examples/tools). The bot's hot
    /// path uses `start_top_of_book` directly.
    ///
    /// # Arguments
    /// * `callback` - Function called with (best_bid_price, best_ask_price, symbol, timestamp)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(String, String, String, u64) + Send + 'static,
    {
        let symbol = self.config.symbol.clone();
        self.start_top_of_book(move |bid, ask, ts| {
            callback(bid.to_string(), ask.to_string(), symbol.clone(), ts)
        })
        .await
    }

    /// Start the client with an f64 top-of-book callback (hot path).
    ///
    /// Each book frame is parsed in a single pass that extracts only the best
    /// level per side (deeper levels are skipped without allocation).
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
        F: FnMut(f64, f64, u64),
    {
        info!("[PACIFICA] Connecting to {}", self.ws_url);

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.ws_url).await?;
        info!("[PACIFICA] WebSocket connected successfully");

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to orderbook
        let subscribe_msg =
            SubscribeMessage::new(self.config.symbol.clone(), self.config.agg_level);
        let subscribe_json = serde_json::to_string(&subscribe_msg)?;

        debug!("[PACIFICA] Sending subscription: {}", subscribe_json);
        write.send(Message::Text(subscribe_json)).await?;
        info!(
            "[PACIFICA] Subscribed to orderbook for {}",
            self.config.symbol
        );

        // Setup ping interval
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));
        ping_interval.tick().await; // Skip first immediate tick

        // Staleness watchdog: if no inbound frame (incl. pong) arrives for several
        // ping cycles the socket is half-open; reconnect rather than appearing
        // healthy while no book updates flow.
        let stale_after =
            Duration::from_secs((self.config.ping_interval_secs.max(1)).saturating_mul(3));
        let mut stale_check = interval(Duration::from_secs(self.config.ping_interval_secs.max(1)));
        stale_check.tick().await;
        let mut last_inbound = tokio::time::Instant::now();

        // Main event loop
        loop {
            tokio::select! {
                // Handle incoming messages
                msg = read.next() => {
                    last_inbound = tokio::time::Instant::now();
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            debug!("[PACIFICA] Received message: {}", text);
                            // A single malformed frame must not tear down the whole
                            // connection (forcing a resubscribe); log and continue.
                            if let Err(e) = self.handle_message(&text, callback) {
                                warn!("[PACIFICA] Skipping unparseable book frame: {}", e);
                            }
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

                // Staleness watchdog. Return Err (not break) so `start` treats it as
                // a connection failure and reconnects + resubscribes, rather than
                // exiting as a graceful close.
                _ = stale_check.tick() => {
                    if last_inbound.elapsed() > stale_after {
                        return Err(anyhow!(
                            "[PACIFICA] No inbound frame for {:?}; socket stale",
                            last_inbound.elapsed()
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket messages
    fn handle_message<F>(&self, text: &str, callback: &mut F) -> Result<()>
    where
        F: FnMut(f64, f64, u64),
    {
        // Hot path: nearly every frame on this socket is a book frame. Parse it
        // directly in ONE pass that extracts only the top level per side
        // straight to f64 (deeper levels are skipped without allocation). The
        // envelope parse below runs only for the rare pong/ack frames.
        if let Ok(frame) = serde_json::from_str::<BookTopFrame>(text) {
            if frame.channel == "book" {
                if let (Some(bid), Some(ask)) =
                    (frame.data.levels.best_bid, frame.data.levels.best_ask)
                {
                    callback(bid, ask, frame.data.timestamp);
                } else {
                    warn!("[PACIFICA] Incomplete top of book data received");
                }
                return Ok(());
            }
        }

        // Rare path: pong / subscription acks / unknown frames.
        let response: WebSocketResponse = serde_json::from_str(text)?;
        match response.channel.as_deref() {
            Some("pong") => {
                debug!("[PACIFICA] Received pong response");
            }
            Some(other) => {
                debug!("[PACIFICA] Received unknown channel: {}", other);
            }
            None => {
                debug!(
                    "[PACIFICA] Received message without channel field, ignoring: {}",
                    text.chars().take(100).collect::<String>()
                );
            }
        }
        Ok(())
    }
}

impl Drop for OrderbookClient {
    fn drop(&mut self) {
        info!(
            "[PACIFICA] OrderbookClient dropped for symbol: {}",
            self.config.symbol
        );
    }
}

#[async_trait::async_trait]
impl crate::services::price_source::PriceStream for OrderbookClient {
    fn label(&self) -> &'static str {
        "PACIFICA_OB"
    }
    async fn run_with(
        &mut self,
        mut cb: crate::services::price_source::BookCallback,
    ) -> anyhow::Result<()> {
        self.start_top_of_book(move |bid, ask, ts| cb(bid, ask, ts))
            .await
    }
}
