use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::time::{interval, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::types::{
    AccountOrderUpdatesResponse, AccountOrderUpdatesSubscribe, AccountPositionsResponse,
    AccountPositionsSubscribe, FillEvent, PingMessage,
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
    /// Enable position-based fill detection (redundancy layer)
    pub enable_position_fill_detection: bool,
}

/// Position snapshot for tracking changes
#[derive(Debug, Clone)]
struct PositionSnapshot {
    quantity: f64,      // Signed quantity (+ for long, - for short)
    entry_price: f64,
    timestamp: u64,
}

/// WebSocket client for monitoring order fills and updates
pub struct FillDetectionClient {
    config: FillDetectionConfig,
    ws_url: String,
    /// Position snapshots per symbol for delta detection
    position_snapshots: Arc<Mutex<HashMap<String, PositionSnapshot>>>,
    /// Track last order fill time for cross-validation (symbol -> timestamp)
    last_order_fill_time: Arc<Mutex<Instant>>,
    /// Track which symbols have received their first position update (for baseline initialization)
    position_initialized: Arc<Mutex<HashSet<String>>>,
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
            position_snapshots: Arc::new(Mutex::new(HashMap::new())),
            last_order_fill_time: Arc::new(Mutex::new(Instant::now())),
            position_initialized: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Initialize position snapshots from external source (e.g., REST API at startup)
    ///
    /// This method allows pre-populating position baselines before starting the WebSocket,
    /// preventing false fill detection from pre-existing positions.
    ///
    /// # Arguments
    /// * `positions` - Vector of (symbol, quantity, entry_price, timestamp) tuples
    ///   where quantity is signed (+ for long, - for short)
    pub fn initialize_positions(&self, positions: Vec<(String, f64, f64, u64)>) {
        if let Ok(mut snapshots) = self.position_snapshots.lock() {
            if let Ok(mut initialized) = self.position_initialized.lock() {
                for (symbol, quantity, entry_price, timestamp) in positions {
                    snapshots.insert(
                        symbol.clone(),
                        PositionSnapshot {
                            quantity,
                            entry_price,
                            timestamp,
                        },
                    );
                    initialized.insert(symbol.clone());
                    info!(
                        "[POSITION INIT] Pre-initialized {} position: {:.4} @ ${:.4}",
                        symbol, quantity, entry_price
                    );
                }
            }
        }
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

        // Subscribe to account positions (for position-based fill detection)
        if self.config.enable_position_fill_detection {
            let positions_subscribe = AccountPositionsSubscribe::new(self.config.account.clone());
            let positions_json = serde_json::to_string(&positions_subscribe)?;
            write.send(Message::Text(positions_json)).await?;
            info!(
                "Subscribed to account_positions for account: {}",
                self.config.account
            );
        }

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

                    // Update last order fill time for cross-validation
                    if let Ok(mut last_time) = self.last_order_fill_time.lock() {
                        *last_time = Instant::now();
                    }

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
                "account_positions" => {
                    if self.config.enable_position_fill_detection {
                        // Parse as account positions response
                        let positions: AccountPositionsResponse = serde_json::from_str(text)?;
                        debug!(
                            "Received {} position update(s)",
                            positions.data.len()
                        );

                        // Process each position update
                        for position in positions.data {
                            if let Some(fill_event) = self.detect_fill_from_position(&position) {
                                callback(fill_event);
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

    /// Detect fill from position change (redundancy layer)
    fn detect_fill_from_position(&self, position: &super::types::PositionData) -> Option<FillEvent> {
        // Parse position data with validation
        let amount: f64 = match position.amount.parse::<f64>() {
            Ok(val) if val >= 0.0 && val.is_finite() => val,
            Ok(val) => {
                warn!(
                    "[POSITION VALIDATION] Invalid amount value: {} (must be non-negative and finite)",
                    val
                );
                return None;
            }
            Err(e) => {
                warn!(
                    "[POSITION VALIDATION] Failed to parse amount '{}': {}",
                    position.amount, e
                );
                return None;
            }
        };

        let entry_price: f64 = match position.entry_price.parse::<f64>() {
            Ok(val) if val > 0.0 && val.is_finite() => val,
            Ok(val) => {
                warn!(
                    "[POSITION VALIDATION] Invalid entry_price value: {} (must be positive and finite)",
                    val
                );
                return None;
            }
            Err(e) => {
                warn!(
                    "[POSITION VALIDATION] Failed to parse entry_price '{}': {}",
                    position.entry_price, e
                );
                return None;
            }
        };

        // Validate side field
        if position.side != "ask" && position.side != "bid" {
            warn!(
                "[POSITION VALIDATION] Invalid side value: '{}' (must be 'ask' or 'bid')",
                position.side
            );
            return None;
        }

        // Convert to signed quantity (+ for long, - for short)
        let quantity = if position.side == "ask" {
            -amount  // Short position
        } else {
            amount   // Long position
        };

        debug!(
            "[POSITION UPDATE] {} | amount: {}, side: {}, entry: ${}, quantity: {}",
            position.symbol, amount, position.side, entry_price, quantity
        );

        // Check if this is the first position update for this symbol
        let mut initialized = self.position_initialized.lock().ok()?;
        let is_first_update = !initialized.contains(&position.symbol);

        // Get previous position snapshot
        let mut snapshots = self.position_snapshots.lock().ok()?;
        let prev_snapshot = snapshots.get(&position.symbol);

        let prev_qty = prev_snapshot.map(|s| s.quantity).unwrap_or(0.0);
        let delta = quantity - prev_qty;

        // CRITICAL FIX: If this is the first position update for this symbol,
        // treat it as baseline initialization ONLY (don't trigger fills)
        // This prevents false positives from detecting pre-existing positions as new fills
        if is_first_update {
            info!(
                "[POSITION BASELINE] Initializing position snapshot for {}: {:.4} (side: {}, entry: ${:.4})",
                position.symbol,
                quantity,
                position.side,
                entry_price
            );

            // Mark this symbol as initialized
            initialized.insert(position.symbol.clone());
            drop(initialized);  // Release lock early

            // Save the baseline snapshot
            snapshots.insert(
                position.symbol.clone(),
                PositionSnapshot {
                    quantity,
                    entry_price,
                    timestamp: position.timestamp,
                },
            );

            return None;  // Don't trigger fill detection on first update
        }

        drop(initialized);  // Release lock early

        // Validate timestamp - reject stale updates (older than previous snapshot)
        if let Some(prev_snapshot) = prev_snapshot {
            if position.timestamp <= prev_snapshot.timestamp {
                warn!(
                    "[POSITION VALIDATION] Stale position update detected for {} (current: {}, previous: {}). Ignoring.",
                    position.symbol,
                    position.timestamp,
                    prev_snapshot.timestamp
                );
                return None;  // Ignore stale updates
            }
        }

        // Only detect fills if there's a significant change (> 0.0001 for floating point tolerance)
        if delta.abs() < 0.0001 {
            // Update snapshot even if no change
            snapshots.insert(
                position.symbol.clone(),
                PositionSnapshot {
                    quantity,
                    entry_price,
                    timestamp: position.timestamp,
                },
            );
            return None;
        }

        // Validate delta is reasonable (max 10 units for safety)
        // This prevents triggering on obviously corrupted data
        if delta.abs() > 10.0 {
            warn!(
                "[POSITION VALIDATION] Unreasonably large position delta detected: {:.4} (prev: {:.4}, new: {:.4}). Possible data corruption. NOT triggering hedge.",
                delta, prev_qty, quantity
            );

            // Update snapshot but don't trigger fill
            snapshots.insert(
                position.symbol.clone(),
                PositionSnapshot {
                    quantity,
                    entry_price,
                    timestamp: position.timestamp,
                },
            );
            return None;
        }

        // Determine fill side from delta
        let side = if delta > 0.0 { "buy" } else { "sell" };

        // Cross-validate: check if we received order fills recently (for logging purposes only)
        let (cross_validated, seconds_since_last_fill) = if let Ok(last_time) = self.last_order_fill_time.lock() {
            let elapsed = last_time.elapsed().as_secs();
            (elapsed < 60, elapsed)
        } else {
            (false, u64::MAX)
        };

        // Log fill detection with cross-validation status (informational only)
        if cross_validated {
            info!(
                "[POSITION FILL ✓] {} {:.4} {} @ {:.4} (pos: {:.4} → {:.4}, cross-validated with order updates)",
                side.to_uppercase(),
                delta.abs(),
                position.symbol,
                entry_price,
                prev_qty,
                quantity
            );
        } else {
            info!(
                "[POSITION FILL ⚠] {} {:.4} {} @ {:.4} (pos: {:.4} → {:.4}, {} sec since last order activity)",
                side.to_uppercase(),
                delta.abs(),
                position.symbol,
                entry_price,
                prev_qty,
                quantity,
                seconds_since_last_fill
            );
        }

        // Update snapshot before returning
        snapshots.insert(
            position.symbol.clone(),
            PositionSnapshot {
                quantity,
                entry_price,
                timestamp: position.timestamp,
            },
        );

        // Create position-based fill event
        Some(FillEvent::PositionFill {
            symbol: position.symbol.clone(),
            side: side.to_string(),
            filled_amount: delta.abs().to_string(),
            avg_price: entry_price.to_string(),
            timestamp: position.timestamp,
            position_delta: delta.to_string(),
            prev_position: prev_qty.to_string(),
            new_position: quantity.to_string(),
            cross_validated,
        })
    }
}
