use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::types::{AccountOrderUpdatesSubscribe, FillEvent, OrderUpdate, PingMessage};

/// Configuration for fill detection client
#[derive(Debug, Clone)]
pub struct FillDetectionConfig {
    /// Account address to monitor
    pub account: String,
    /// Maximum number of CONSECUTIVE failed reconnection attempts before
    /// giving up; a connection that stays up for `HEALTHY_CONNECTION_UPTIME`
    /// resets the budget. `None` = retry forever (the production setting: the
    /// fill stream is fail-closed and supervised, so exhausting reconnects
    /// would permanently halt quoting until a manual restart).
    pub max_attempts: Option<u32>,
    /// Ping interval in seconds
    pub ping_interval_secs: u64,
}

/// A connection that survives this long is "healthy": it resets the reconnect
/// budget so only consecutive rapid failures count toward `max_attempts`.
const HEALTHY_CONNECTION_UPTIME: Duration = Duration::from_secs(10);

/// Async hook invoked on every successful (re)connect so the caller can
/// perform REST reconciliation (e.g. scan open_orders for fills that
/// occurred during the outage). Returns `()`; errors should be logged.
pub type ReconcileHook = Arc<
    dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>
        + Send
        + Sync,
>;

/// WebSocket client for monitoring order fills and updates
pub struct FillDetectionClient {
    config: FillDetectionConfig,
    ws_url: String,
    /// Optional hook called after each (re)connect. Populate to run REST
    /// reconciliation and catch up on fills that happened during the outage.
    reconcile_hook: Arc<Mutex<Option<ReconcileHook>>>,
    ready: Arc<AtomicBool>,
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

        Ok(Self {
            config,
            ws_url,
            reconcile_hook: Arc::new(Mutex::new(None)),
            ready: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn ready_flag(&self) -> Arc<AtomicBool> {
        self.ready.clone()
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Register a reconciliation hook that runs after every (re)connect.
    /// Use this to fetch open orders / recent trades via REST so any fills
    /// that occurred during the WS outage are replayed into the fill pipeline.
    pub fn set_reconcile_hook(&self, hook: ReconcileHook) {
        *self.reconcile_hook.lock() = Some(hook);
    }

    /// Start the fill detection client with a callback for fill events
    ///
    /// There is no legitimate "graceful permanent close" for an account-stream
    /// subscriber: a server-initiated close or stream end is treated as a
    /// reconnect trigger, exactly like an error. A connection that lives at
    /// least `HEALTHY_CONNECTION_UPTIME` resets the attempt budget so only
    /// consecutive rapid failures can exhaust `max_attempts`.
    ///
    /// # Arguments
    /// * `callback` - Function called for each fill event (partial fill, full fill, cancellation)
    pub async fn start<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(FillEvent) + Send + 'static,
    {
        let mut attempt: u32 = 0;

        loop {
            attempt = attempt.saturating_add(1);
            match self.config.max_attempts {
                Some(max) => info!("Fill detection attempt {}/{}", attempt, max),
                None => info!("Fill detection attempt {} (unbounded)", attempt),
            }

            let connected_at = Instant::now();
            match self.connect_and_run(&mut callback).await {
                Ok(_) => {
                    warn!("Fill detection connection closed by server; reconnecting");
                }
                Err(e) => {
                    error!("Fill detection error: {}", e);
                }
            }
            self.ready.store(false, Ordering::Release);

            if connected_at.elapsed() >= HEALTHY_CONNECTION_UPTIME {
                attempt = 0;
            }

            if let Some(max) = self.config.max_attempts {
                if attempt >= max {
                    error!("Max consecutive fill-detection reconnection attempts reached");
                    anyhow::bail!(
                        "fill detection exhausted {} consecutive reconnect attempts",
                        max
                    );
                }
            }

            // Fast first reconnect (1s), then exponential backoff, capped at 30s
            let backoff = if attempt <= 1 {
                1
            } else {
                std::cmp::min(2u64.saturating_pow(attempt - 1), 30)
            };
            warn!("Reconnecting in {} seconds...", backoff);
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
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

        // Reconcile any fills that landed during the outage via a caller-supplied
        // REST catch-up hook. Runs once per (re)connect before we start serving
        // live messages. Errors inside the hook are logged, not propagated.
        let hook_opt = self.reconcile_hook.lock().clone();
        if let Some(hook) = hook_opt {
            info!("[PACIFICA_FILL] Running reconcile hook after connect");
            hook().await;
        }

        let (mut write, mut read) = ws_stream.split();

        // Subscribe to account order updates
        let subscribe_msg = AccountOrderUpdatesSubscribe::new(self.config.account.clone());
        let subscribe_json = serde_json::to_string(&subscribe_msg)?;
        write.send(Message::Text(subscribe_json)).await?;
        info!(
            "Subscribed to account_order_updates for account: {}",
            self.config.account
        );

        self.ready.store(true, Ordering::Release);

        // Set up ping interval
        let mut ping_interval = interval(Duration::from_secs(self.config.ping_interval_secs));

        // Staleness watchdog: a half-open fill socket can report ready while no
        // fills arrive. Reconnect if no inbound frame (incl. pong) for several
        // ping cycles so fills are not silently missed.
        let stale_after =
            Duration::from_secs(self.config.ping_interval_secs.max(1).saturating_mul(3));
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
                            if let Err(e) = self.handle_message(&text, callback) {
                                error!("Error handling message: {}", e);
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            info!("WebSocket closed by server");
                            self.ready.store(false, Ordering::Release);
                            break;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {}", e);
                            self.ready.store(false, Ordering::Release);
                            return Err(e.into());
                        }
                        None => {
                            info!("WebSocket stream ended");
                            self.ready.store(false, Ordering::Release);
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

                // Staleness watchdog. Return Err so `start` reconnects + re-runs the
                // reconcile hook + re-subscribes (a graceful break would not).
                _ = stale_check.tick() => {
                    if last_inbound.elapsed() > stale_after {
                        self.ready.store(false, Ordering::Release);
                        return Err(anyhow::anyhow!(
                            "[PACIFICA_FILL] No inbound frame for {:?}; socket stale",
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
                    // Parse each update independently from the already-decoded
                    // envelope. A single malformed/unknown sibling update must NOT
                    // drop the whole frame, which could discard a `Filled` event
                    // that shares the batch (=> missed hedge / naked exposure).
                    let items = response
                        .get("data")
                        .and_then(|d| d.as_array())
                        .cloned()
                        .unwrap_or_default();
                    debug!("Received {} order update(s)", items.len());

                    for item in items {
                        // Deserialize from &Value (no clone): the raw item is
                        // still available for the error log below.
                        match OrderUpdate::deserialize(&item) {
                            Ok(update) => {
                                debug!(
                                    "Order update - ID: {}, Status: {:?}, Event: {:?}, Filled: {}/{}",
                                    update.order_id,
                                    update.order_status,
                                    update.order_event,
                                    update.filled_amount,
                                    update.original_amount
                                );
                                if let Some(fill_event) = update.to_fill_event() {
                                    callback(fill_event);
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "[PACIFICA_FILL] Skipping unparseable order update \
                                     (sibling fills preserved): {} | raw={}",
                                    e, item
                                );
                            }
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
