use anyhow::{Context, Result};
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::trading::canonicalize_json;
use crate::connector::pacifica::trading::{OrderData, OrderSide};
use crate::connector::pacifica::PacificaCredentials;
use crate::market_rules::{pacifica_maker_price_for_is_buy, pacifica_size_floor};

/// Maximum time to wait for a request/response round trip before giving up.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
/// Keepalive ping cadence. Pacifica closes idle connections after 60 s.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// Max backoff between reconnect attempts.
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>;

/// Persistent WebSocket-based trading client for Pacifica.
///
/// One connection is maintained for the lifetime of the process. A background
/// task owns the socket, demultiplexes incoming frames by request id, and
/// reconnects on disconnect with exponential backoff. Callers push outbound
/// JSON through a channel and await a matching response via `oneshot`.
pub struct PacificaWsTrading {
    credentials: PacificaCredentials,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<String>,
    pending: PendingMap,
    connected: Arc<AtomicBool>,
}

impl PacificaWsTrading {
    /// Create a new WebSocket trading client and start the persistent
    /// connection task. Returns immediately; the connection is established
    /// asynchronously. Callers can observe `is_connected()` to probe status.
    pub fn new(credentials: PacificaCredentials, is_testnet: bool) -> Self {
        let ws_url = if is_testnet {
            "wss://test-ws.pacifica.fi/ws".to_string()
        } else {
            "wss://ws.pacifica.fi/ws".to_string()
        };

        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let connected = Arc::new(AtomicBool::new(false));
        let (outbound_tx, outbound_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

        // Spawn the persistent connection runner.
        let pending_bg = pending.clone();
        let connected_bg = connected.clone();
        tokio::spawn(async move {
            Self::run_connection_loop(ws_url, outbound_rx, pending_bg, connected_bg).await;
        });

        Self {
            credentials,
            outbound_tx,
            pending,
            connected,
        }
    }

    /// Returns true if the underlying WebSocket is currently connected.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub async fn place_limit_order_ws(
        &self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: f64,
        tick_size: &str,
        lot_size: &str,
        client_order_id: String,
    ) -> Result<OrderData> {
        if !self.is_connected() {
            anyhow::bail!("Pacifica WS not connected");
        }

        let rounded_price =
            pacifica_maker_price_for_is_buy(matches!(side, OrderSide::Buy), price, tick_size)?;
        let rounded_size = pacifica_size_floor(size, lot_size)?;
        if rounded_size <= 0.0 {
            anyhow::bail!("rounded Pacifica order size is zero");
        }

        let timestamp = chrono::Utc::now().timestamp_millis();
        let expiry_window: i64 = 5000;
        let header = json!({
            "type": "create_order",
            "timestamp": timestamp,
            "expiry_window": expiry_window,
        });
        let payload = json!({
            "symbol": symbol,
            "price": rounded_price.to_string(),
            "amount": rounded_size.to_string(),
            "side": side.as_str(),
            "tif": "ALO",
            "reduce_only": false,
            "client_order_id": client_order_id.clone(),
        });
        let signature = self.sign_message(header, payload.clone())?;

        let request_json = json!({
            "id": Uuid::new_v4().to_string(),
            "params": {
                "create_order": {
                    "account": self.credentials.account,
                    "agent_wallet": self.credentials.agent_wallet,
                    "signature": signature,
                    "timestamp": timestamp,
                    "expiry_window": expiry_window,
                    "symbol": symbol,
                    "price": rounded_price.to_string(),
                    "amount": rounded_size.to_string(),
                    "side": side.as_str(),
                    "tif": "ALO",
                    "reduce_only": false,
                    "client_order_id": client_order_id.clone(),
                }
            }
        });

        let response = self.send_request_and_wait(request_json).await?;
        let code = response.get("code").and_then(|v| v.as_u64()).unwrap_or(200);
        if code != 200 {
            let err = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown Pacifica WS placement error");
            anyhow::bail!("Pacifica WS place_limit_order failed: {}", err);
        }

        let data = response
            .get("data")
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
        let order_id = data
            .get("order_id")
            .or_else(|| data.get("i"))
            .and_then(|v| v.as_u64());
        let returned_cloid = data
            .get("client_order_id")
            .or_else(|| data.get("I"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or(client_order_id);

        Ok(OrderData {
            order_id,
            i: order_id,
            client_order_id: Some(returned_cloid),
            symbol: Some(symbol.to_string()),
            requested_size: Some(size),
            submitted_size: Some(rounded_size),
            submitted_price: Some(rounded_price),
        })
    }

    /// Cancel all orders via WebSocket. Returns the number of orders cancelled.
    ///
    /// Uses the long-lived connection established by `new()`. If the socket is
    /// currently down, returns an error so the caller (dual_cancel) can fall
    /// back to REST.
    pub async fn cancel_all_orders_ws(
        &self,
        all_symbols: bool,
        symbol: Option<&str>,
        exclude_reduce_only: bool,
    ) -> Result<u32> {
        if !all_symbols && symbol.is_none() {
            anyhow::bail!("symbol is required when all_symbols is false");
        }
        if !self.is_connected() {
            anyhow::bail!("Pacifica WS not connected");
        }

        let timestamp = chrono::Utc::now().timestamp_millis();
        let expiry_window: i64 = 5000;

        let header = json!({
            "type": "cancel_all_orders",
            "timestamp": timestamp,
            "expiry_window": expiry_window,
        });
        let mut payload = json!({
            "all_symbols": all_symbols,
            "exclude_reduce_only": exclude_reduce_only,
        });
        if let Some(sym) = symbol {
            payload["symbol"] = json!(sym);
        }

        let signature = self.sign_message(header, payload.clone())?;

        // Params object matching FULL_websocket.txt's cancel_all_orders format.
        let mut params_inner = serde_json::Map::new();
        params_inner.insert("account".into(), json!(self.credentials.account));
        params_inner.insert("agent_wallet".into(), json!(self.credentials.agent_wallet));
        params_inner.insert("signature".into(), json!(signature));
        params_inner.insert("timestamp".into(), json!(timestamp));
        params_inner.insert("expiry_window".into(), json!(expiry_window));
        params_inner.insert("all_symbols".into(), json!(all_symbols));
        params_inner.insert("exclude_reduce_only".into(), json!(exclude_reduce_only));
        if let Some(sym) = symbol {
            params_inner.insert("symbol".into(), json!(sym));
        }

        let request_json = json!({
            "id": Uuid::new_v4().to_string(),
            "params": {
                "cancel_all_orders": Value::Object(params_inner),
            },
        });

        let value = self.send_request_and_wait(request_json).await?;
        let code = value.get("code").and_then(|v| v.as_u64()).unwrap_or(0);
        if code == 200 {
            let cancelled = value
                .get("data")
                .and_then(|d| d.get("cancelled_count"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;
            info!("[PACIFICA_WS] Cancelled {} order(s)", cancelled);
            Ok(cancelled)
        } else {
            let err = value
                .get("error")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("code={}", code));
            anyhow::bail!("Pacifica WS cancel_all failed: {}", err);
        }
    }

    async fn send_request_and_wait(&self, mut request_json: Value) -> Result<Value> {
        if !self.is_connected() {
            anyhow::bail!("Pacifica WS not connected");
        }
        let request_id = request_json
            .get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        request_json["id"] = json!(request_id);
        let request_text = serde_json::to_string(&request_json)?;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(request_id.clone(), tx);

        if self.outbound_tx.send(request_text).is_err() {
            self.pending.lock().remove(&request_id);
            anyhow::bail!("Pacifica WS outbound channel closed");
        }

        match timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(_)) => {
                self.pending.lock().remove(&request_id);
                anyhow::bail!("Pacifica WS response channel dropped")
            }
            Err(_) => {
                self.pending.lock().remove(&request_id);
                anyhow::bail!("Pacifica WS request timed out after {:?}", REQUEST_TIMEOUT)
            }
        }
    }

    /// Background task: own the socket, multiplex writes, demultiplex reads,
    /// reconnect on failure. Exits only when `outbound_rx` is dropped (process
    /// shutdown).
    async fn run_connection_loop(
        ws_url: String,
        mut outbound_rx: tokio::sync::mpsc::UnboundedReceiver<String>,
        pending: PendingMap,
        connected: Arc<AtomicBool>,
    ) {
        let mut backoff = Duration::from_secs(1);
        loop {
            info!("[PACIFICA_WS] Connecting to {}", ws_url);
            let stream = match connect_async(&ws_url).await {
                Ok((s, _)) => {
                    info!("[PACIFICA_WS] Connected");
                    backoff = Duration::from_secs(1);
                    s
                }
                Err(e) => {
                    warn!(
                        "[PACIFICA_WS] Connect failed: {} - retrying in {:?}",
                        e, backoff
                    );
                    Self::fail_all_pending(&pending, "ws connect failed");
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, MAX_RECONNECT_BACKOFF);
                    continue;
                }
            };
            connected.store(true, Ordering::Release);

            let (mut write, mut read) = stream.split();
            let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
            keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            // Pump until one side fails.
            let dc_reason: String = loop {
                tokio::select! {
                    msg = outbound_rx.recv() => {
                        match msg {
                            Some(text) => {
                                if let Err(e) = write.send(Message::Text(text)).await {
                                    break format!("write error: {}", e);
                                }
                            }
                            None => {
                                // Sender dropped - process is shutting down.
                                let _ = write.close().await;
                                connected.store(false, Ordering::Release);
                                return;
                            }
                        }
                    }
                    _ = keepalive.tick() => {
                        let ping = json!({"method": "ping"}).to_string();
                        if let Err(e) = write.send(Message::Text(ping)).await {
                            break format!("keepalive write error: {}", e);
                        }
                    }
                    frame = read.next() => {
                        match frame {
                            Some(Ok(Message::Text(text))) => {
                                Self::dispatch_incoming(&text, &pending);
                            }
                            Some(Ok(Message::Ping(data))) => {
                                let _ = write.send(Message::Pong(data)).await;
                            }
                            Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(Message::Close(_))) => break "server closed".to_string(),
                            Some(Err(e)) => break format!("read error: {}", e),
                            None => break "stream ended".to_string(),
                            _ => {}
                        }
                    }
                }
            };

            connected.store(false, Ordering::Release);
            warn!(
                "[PACIFICA_WS] Disconnected: {} - reconnecting in {:?}",
                dc_reason, backoff
            );
            Self::fail_all_pending(&pending, &dc_reason);
            sleep(backoff).await;
            backoff = std::cmp::min(backoff * 2, MAX_RECONNECT_BACKOFF);
        }
    }

    /// Route an incoming text frame to its registered `oneshot::Sender`.
    fn dispatch_incoming(text: &str, pending: &PendingMap) {
        let value: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                debug!("[PACIFICA_WS] Skipping unparseable frame: {}", e);
                return;
            }
        };
        // The `id` field correlates responses to pending oneshots.
        let id = match value.get("id").and_then(|v| v.as_str()) {
            Some(s) => s.to_string(),
            None => {
                // Subscription push (no id) - ignore in the trading client.
                return;
            }
        };
        if let Some(tx) = pending.lock().remove(&id) {
            let _ = tx.send(value);
        } else {
            debug!("[PACIFICA_WS] Unmatched response id={}", id);
        }
    }

    /// On disconnect, drop every pending oneshot so callers unblock with an
    /// error rather than timing out at 2 s.
    fn fail_all_pending(pending: &PendingMap, reason: &str) {
        let mut g = pending.lock();
        if !g.is_empty() {
            error!(
                "[PACIFICA_WS] Failing {} pending request(s): {}",
                g.len(),
                reason
            );
        }
        g.clear();
    }

    /// Sign a message using Ed25519 (same scheme as REST).
    fn sign_message(&self, header: Value, payload: Value) -> Result<String> {
        let mut message = serde_json::json!({});
        if let Value::Object(ref mut map) = message {
            if let Value::Object(header_map) = header {
                for (k, v) in header_map {
                    map.insert(k, v);
                }
            }
            map.insert("data".to_string(), payload);
        }
        let canonical = canonicalize_json(&message);

        let private_key_bytes = bs58::decode(&self.credentials.private_key)
            .into_vec()
            .context("Failed to decode private key")?;
        if private_key_bytes.len() != 64 {
            anyhow::bail!(
                "Invalid private key length: expected 64 bytes, got {}",
                private_key_bytes.len()
            );
        }
        let seed_bytes: [u8; 32] = private_key_bytes[0..32]
            .try_into()
            .context("Failed to extract 32-byte seed")?;

        let signing_key = SigningKey::from_bytes(&seed_bytes);
        let signature = signing_key.sign(canonical.as_bytes());
        Ok(bs58::encode(signature.to_bytes()).into_string())
    }
}
