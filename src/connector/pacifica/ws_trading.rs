use anyhow::{Context, Result};
use ed25519_dalek::{Signer, SigningKey};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info};
use uuid::Uuid;

use super::types::{
    WsCancelAllOrdersData, WsCancelAllOrdersParams, WsCancelAllOrdersRequest,
    WsCancelAllOrdersResponse, WsErrorResponse,
};
use super::trading::canonicalize_json;
use crate::connector::pacifica::PacificaCredentials;

/// WebSocket-based trading client for Pacifica
///
/// This is an alternative to REST API trading operations with lower latency
/// and no rate limits. Uses the same WebSocket connection for multiple operations.
pub struct PacificaWsTrading {
    credentials: PacificaCredentials,
    ws_url: String,
}

impl PacificaWsTrading {
    /// Create a new WebSocket trading client
    ///
    /// # Arguments
    /// * `credentials` - Pacifica credentials
    /// * `is_testnet` - Whether to use testnet (false = mainnet)
    pub fn new(credentials: PacificaCredentials, is_testnet: bool) -> Self {
        let ws_url = if is_testnet {
            "wss://test-ws.pacifica.fi/ws".to_string()
        } else {
            "wss://ws.pacifica.fi/ws".to_string()
        };

        Self {
            credentials,
            ws_url,
        }
    }

    /// Cancel all orders via WebSocket
    ///
    /// # Arguments
    /// * `all_symbols` - Whether to cancel orders for all symbols
    /// * `symbol` - Symbol to cancel orders for (required if all_symbols is false)
    /// * `exclude_reduce_only` - Whether to exclude reduce-only orders
    ///
    /// # Returns
    /// Number of orders cancelled
    pub async fn cancel_all_orders_ws(
        &self,
        all_symbols: bool,
        symbol: Option<&str>,
        exclude_reduce_only: bool,
    ) -> Result<u32> {
        if !all_symbols && symbol.is_none() {
            anyhow::bail!("symbol is required when all_symbols is false");
        }

        info!(
            "[PACIFICA_WS] Cancelling all orders via WebSocket (all_symbols: {}, symbol: {:?}, exclude_reduce_only: {})",
            all_symbols,
            symbol,
            exclude_reduce_only
        );

        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.ws_url)
            .await
            .context("Failed to connect to Pacifica WebSocket")?;

        debug!("[PACIFICA_WS] Connected to {}", self.ws_url);

        let (mut write, mut read) = ws_stream.split();

        // Generate request ID
        let request_id = Uuid::new_v4().to_string();

        // Build signature
        let timestamp = chrono::Utc::now().timestamp_millis();
        let expiry_window = 5000;

        let header = json!({
            "type": "cancel_all_orders",
            "timestamp": timestamp,
            "expiry_window": expiry_window
        });

        let mut payload = json!({
            "all_symbols": all_symbols,
            "exclude_reduce_only": exclude_reduce_only
        });

        // Add symbol if provided
        if let Some(sym) = symbol {
            payload["symbol"] = json!(sym);
        }

        let signature = self.sign_message(header, payload.clone())?;

        // Build cancel all orders request
        let cancel_request = WsCancelAllOrdersRequest {
            id: request_id.clone(),
            params: WsCancelAllOrdersParams {
                cancel_all_orders: WsCancelAllOrdersData {
                    account: self.credentials.account.clone(),
                    agent_wallet: Some(self.credentials.agent_wallet.clone()),
                    signature,
                    timestamp,
                    expiry_window,
                    all_symbols,
                    exclude_reduce_only,
                    symbol: symbol.map(|s| s.to_string()),
                },
            },
        };

        // Serialize and send request
        let request_json = serde_json::to_string(&cancel_request)?;
        debug!("[PACIFICA_WS] Sending request: {}", request_json);
        write.send(Message::Text(request_json)).await?;

        // Wait for response with matching ID
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    debug!("[PACIFICA_WS] Received: {}", text);

                    // Try to parse as success response
                    if let Ok(response) = serde_json::from_str::<WsCancelAllOrdersResponse>(&text)
                    {
                        if response.id == request_id {
                            if response.code == 200 {
                                info!(
                                    "[PACIFICA_WS] Successfully cancelled {} order(s)",
                                    response.data.cancelled_count
                                );
                                return Ok(response.data.cancelled_count);
                            } else {
                                anyhow::bail!(
                                    "Cancel all orders failed with code: {}",
                                    response.code
                                );
                            }
                        }
                    }

                    // Try to parse as error response
                    if let Ok(error_response) = serde_json::from_str::<WsErrorResponse>(&text) {
                        if error_response.id == request_id {
                            let error_msg = error_response
                                .error
                                .unwrap_or_else(|| format!("Unknown error (code: {})", error_response.code));
                            anyhow::bail!("WebSocket error: {}", error_msg);
                        }
                    }

                    // Ignore messages with different IDs (might be from other subscriptions)
                }
                Ok(Message::Close(_)) => {
                    anyhow::bail!("WebSocket closed before receiving response");
                }
                Err(e) => {
                    anyhow::bail!("WebSocket error: {}", e);
                }
                _ => {}
            }
        }

        anyhow::bail!("WebSocket stream ended before receiving response")
    }

    /// Sign a message using Ed25519
    ///
    /// This is identical to the REST API signature method
    fn sign_message(
        &self,
        header: serde_json::Value,
        payload: serde_json::Value,
    ) -> Result<String> {
        // Construct message: {... header, data: payload}
        let mut message = serde_json::json!({});
        if let serde_json::Value::Object(ref mut map) = message {
            if let serde_json::Value::Object(header_map) = header {
                for (k, v) in header_map {
                    map.insert(k, v);
                }
            }
            map.insert("data".to_string(), payload);
        }

        // Canonicalize JSON (sort keys alphabetically)
        let canonical = canonicalize_json(&message);

        // Decode private key from base58
        let private_key_bytes = bs58::decode(&self.credentials.private_key)
            .into_vec()
            .context("Failed to decode private key")?;

        // Solana/Pacifica private keys are 64 bytes (32 bytes seed + 32 bytes public key)
        // Ed25519 SigningKey needs only the first 32 bytes (the seed)
        if private_key_bytes.len() != 64 {
            anyhow::bail!(
                "Invalid private key length: expected 64 bytes, got {}",
                private_key_bytes.len()
            );
        }

        let seed_bytes: [u8; 32] = private_key_bytes[0..32]
            .try_into()
            .context("Failed to extract 32-byte seed")?;

        // Create signing key and sign
        let signing_key = SigningKey::from_bytes(&seed_bytes);
        let signature = signing_key.sign(canonical.as_bytes());

        // Encode signature as base58
        Ok(bs58::encode(signature.to_bytes()).into_string())
    }
}
