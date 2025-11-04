use anyhow::{Context, Result};
use ed25519_dalek::{Signer, SigningKey};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use tracing::{debug, info};
use uuid::Uuid;

const MAINNET_REST_URL: &str = "https://api.pacifica.fi";

/// Credentials for Pacifica trading
#[derive(Debug, Clone)]
pub struct PacificaCredentials {
    pub account: String,           // Main wallet address (SOL_WALLET)
    pub agent_wallet: String,      // Agent wallet public key (API_PUBLIC)
    pub private_key: String,       // Agent wallet private key (API_PRIVATE)
}

impl PacificaCredentials {
    /// Load credentials from environment variables
    pub fn from_env() -> Result<Self> {
        dotenv::dotenv().ok(); // Load .env file

        let account = std::env::var("SOL_WALLET")
            .context("SOL_WALLET not found in environment")?;
        let agent_wallet = std::env::var("API_PUBLIC")
            .context("API_PUBLIC not found in environment")?;
        let private_key = std::env::var("API_PRIVATE")
            .context("API_PRIVATE not found in environment")?;

        Ok(Self {
            account,
            agent_wallet,
            private_key,
        })
    }
}

/// Market information for a symbol
#[derive(Debug, Clone, Deserialize)]
pub struct MarketInfo {
    pub symbol: String,
    pub tick_size: String,  // Minimum price increment (e.g., "0.1")
    pub lot_size: String,   // Minimum size increment (e.g., "0.001")
}

/// Order side
#[derive(Debug, Clone, Copy)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "bid",
            OrderSide::Sell => "ask",
        }
    }
}

/// Order response from API
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    pub success: Option<bool>,
    pub data: Option<OrderData>,
}

#[derive(Debug, Deserialize)]
pub struct OrderData {
    pub order_id: Option<u64>,
    #[serde(rename = "i")]
    pub i: Option<u64>,  // Alternative field name
    #[serde(rename = "I")]
    pub client_order_id: Option<String>,
    #[serde(rename = "s")]
    pub symbol: Option<String>,
}

/// Orderbook level from REST API
#[derive(Debug, Clone, Deserialize)]
pub struct RestBookLevel {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "a")]
    pub amount: String,
    #[serde(rename = "n")]
    pub num_orders: u32,
}

/// Orderbook snapshot from REST API (raw format)
#[derive(Debug, Clone, Deserialize)]
pub struct RestBookData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "l")]
    pub levels: (Vec<RestBookLevel>, Vec<RestBookLevel>), // (bids, asks)
}

/// Orderbook REST API response
#[derive(Debug, Deserialize)]
pub struct RestBookResponse {
    pub success: bool,
    pub data: Option<RestBookData>,
}

/// Simplified orderbook level [price, size]
#[derive(Debug, Clone)]
pub struct OrderbookLevel {
    pub price: String,
    pub size: String,
}

/// Simplified orderbook snapshot
#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub bids: Vec<OrderbookLevel>,
    pub asks: Vec<OrderbookLevel>,
}

/// Position information
#[derive(Debug, Clone, Deserialize)]
pub struct PositionInfo {
    pub symbol: String,
    pub amount: String,           // Signed size (positive = long, negative = short)
    pub entry_price: Option<String>,
    pub mark_price: Option<String>,
    pub unrealized_pnl: Option<String>,
    pub liquidation_price: Option<String>,
}

/// Positions response wrapper
#[derive(Debug, Clone, Deserialize)]
pub struct PositionsResponse {
    pub success: bool,
    pub data: Option<Vec<PositionInfo>>,
    pub error: Option<String>,
    pub code: Option<String>,
}

/// Trade history item from positions/history endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct TradeHistoryItem {
    pub history_id: u64,
    pub order_id: u64,
    pub client_order_id: String,
    pub symbol: String,
    pub amount: String,           // Size of trade in token denomination
    pub price: String,            // Current market price
    pub entry_price: String,      // Actual fill price
    pub fee: String,              // Fee paid
    pub pnl: String,              // PnL from this trade
    pub event_type: String,       // "fulfill_maker" or "fulfill_taker"
    pub side: String,             // "open_long", "open_short", "close_long", "close_short"
    pub created_at: u64,          // Timestamp in milliseconds
    pub cause: String,            // "normal", "market_liquidation", etc.
    #[serde(default)]
    pub value: Option<String>,    // Actual USD notional value of the trade (if provided by API)
}

/// Trade history response from API
#[derive(Debug, Deserialize)]
pub struct TradeHistoryResponse {
    pub success: bool,
    pub data: Option<Vec<TradeHistoryItem>>,
    pub error: Option<String>,
    pub code: Option<String>,
}

/// Pacifica trading client
pub struct PacificaTrading {
    credentials: PacificaCredentials,
    rest_url: String,
    client: reqwest::Client,
    market_info_cache: Option<HashMap<String, MarketInfo>>,
}

impl PacificaTrading {
    /// Create a new trading client (mainnet only)
    pub fn new(credentials: PacificaCredentials) -> Self {
        Self {
            credentials,
            rest_url: MAINNET_REST_URL.to_string(),
            client: reqwest::Client::new(),
            market_info_cache: None,
        }
    }

    /// Fetch market info for all symbols
    pub async fn get_market_info(&mut self) -> Result<&HashMap<String, MarketInfo>> {
        if self.market_info_cache.is_some() {
            return Ok(self.market_info_cache.as_ref().unwrap());
        }

        let url = format!("{}/api/v1/info", self.rest_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to fetch market info: {}", response.status());
        }

        #[derive(Deserialize)]
        struct ApiResponse {
            data: Vec<MarketInfo>,
        }

        let api_response: ApiResponse = response.json().await?;

        let mut cache = HashMap::new();
        for info in api_response.data {
            cache.insert(info.symbol.clone(), info);
        }

        info!("[PACIFICA] Cached market info for {} symbols", cache.len());
        self.market_info_cache = Some(cache);

        Ok(self.market_info_cache.as_ref().unwrap())
    }

    /// Fetch orderbook snapshot via REST API
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "SOL", "BTC")
    /// * `agg_level` - Aggregation level (1, 2, 5, 10, 100, 1000)
    ///
    /// # Returns
    /// Orderbook snapshot with bids and asks
    pub async fn get_orderbook_rest(
        &self,
        symbol: &str,
        agg_level: u32,
    ) -> Result<OrderbookSnapshot> {
        let url = format!(
            "{}/api/v1/book?symbol={}&agg_level={}",
            self.rest_url, symbol, agg_level
        );

        debug!("[PACIFICA] Fetching orderbook via REST: {}", url);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await?;
            anyhow::bail!("Failed to fetch orderbook: {} - {}", status, error_text);
        }

        let book_response: RestBookResponse = response.json().await?;

        if !book_response.success {
            anyhow::bail!("Orderbook request failed");
        }

        let book_data = book_response
            .data
            .context("No orderbook data in response")?;

        // Convert from RestBookLevel to OrderbookLevel
        let bids: Vec<OrderbookLevel> = book_data.levels.0
            .into_iter()
            .map(|level| OrderbookLevel {
                price: level.price,
                size: level.amount,
            })
            .collect();

        let asks: Vec<OrderbookLevel> = book_data.levels.1
            .into_iter()
            .map(|level| OrderbookLevel {
                price: level.price,
                size: level.amount,
            })
            .collect();

        debug!(
            "[PACIFICA] Orderbook for {}: {} bids, {} asks",
            symbol,
            bids.len(),
            asks.len()
        );

        Ok(OrderbookSnapshot { bids, asks })
    }

    /// Get best bid and ask prices from REST API
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "SOL", "BTC")
    /// * `agg_level` - Aggregation level (1, 2, 5, 10, 100, 1000)
    ///
    /// # Returns
    /// Tuple of (best_bid, best_ask) or None if orderbook is empty
    pub async fn get_best_bid_ask_rest(
        &self,
        symbol: &str,
        agg_level: u32,
    ) -> Result<Option<(f64, f64)>> {
        let snapshot = self.get_orderbook_rest(symbol, agg_level).await?;

        let best_bid = snapshot
            .bids
            .first()
            .and_then(|level| level.price.parse::<f64>().ok());

        let best_ask = snapshot
            .asks
            .first()
            .and_then(|level| level.price.parse::<f64>().ok());

        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => Ok(Some((bid, ask))),
            _ => Ok(None),
        }
    }

    /// Round price to tick size
    fn round_to_tick_size(&self, price: f64, tick_size: String) -> Result<f64> {
        let tick: f64 = tick_size.parse()?;
        let rounded = (price / tick).round() * tick;

        let decimal_places = if tick_size.contains('.') {
            tick_size.split('.').nth(1).unwrap().len()
        } else {
            0
        };

        let factor = 10_f64.powi(decimal_places as i32);
        Ok((rounded * factor).round() / factor)
    }

    /// Round size to lot size
    fn round_to_lot_size(&self, size: f64, lot_size: String) -> Result<f64> {
        let lot: f64 = lot_size.parse()?;
        let rounded = (size / lot).round() * lot;

        let decimal_places = if lot_size.contains('.') {
            lot_size.split('.').nth(1).unwrap().len()
        } else {
            0
        };

        let factor = 10_f64.powi(decimal_places as i32);
        Ok((rounded * factor).round() / factor)
    }

    /// Sign a message using Ed25519
    fn sign_message(&self, header: serde_json::Value, payload: serde_json::Value) -> Result<String> {
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
            .context("Failed to extract seed from private key")?;

        // Create signing key from seed
        let signing_key = SigningKey::from_bytes(&seed_bytes);

        // Sign the message
        let signature = signing_key.sign(canonical.as_bytes());

        // Encode signature as base58
        Ok(bs58::encode(signature.to_bytes()).into_string())
    }

    /// Place a limit order
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "SOL", "BTC")
    /// * `side` - Order side (Buy or Sell)
    /// * `size` - Order size
    /// * `price` - Optional exact price. If None, uses mid_price_offset
    /// * `mid_price_offset_pct` - Offset from mid price in percentage (default 1.0%)
    /// * `current_bid` - Current best bid price (required if price is None)
    /// * `current_ask` - Current best ask price (required if price is None)
    pub async fn place_limit_order(
        &mut self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: Option<f64>,
        mid_price_offset_pct: f64,
        current_bid: Option<f64>,
        current_ask: Option<f64>,
    ) -> Result<OrderData> {
        // Get market info and clone the strings we need
        let market_info = self.get_market_info().await?;
        let symbol_info = market_info
            .get(symbol)
            .context(format!("Market info not found for {}", symbol))?;

        let tick_size = symbol_info.tick_size.clone();
        let lot_size = symbol_info.lot_size.clone();

        // Calculate price
        let order_price = if let Some(p) = price {
            p
        } else {
            // Calculate from mid price with offset
            let bid = current_bid.context("current_bid required when price is None")?;
            let ask = current_ask.context("current_ask required when price is None")?;
            let mid = (bid + ask) / 2.0;

            match side {
                OrderSide::Buy => mid * (1.0 - mid_price_offset_pct / 100.0),
                OrderSide::Sell => mid * (1.0 + mid_price_offset_pct / 100.0),
            }
        };

        // Round to tick and lot size
        let rounded_price = self.round_to_tick_size(order_price, tick_size.clone())?;
        let rounded_size = self.round_to_lot_size(size, lot_size.clone())?;

        info!(
            "[PACIFICA] Placing {} order: {} {} @ ${} (tick: {}, lot: {})",
            match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
            rounded_size,
            symbol,
            rounded_price,
            tick_size,
            lot_size
        );

        // Generate client order ID
        let client_order_id = Uuid::new_v4().to_string();

        // Build signature
        let timestamp = chrono::Utc::now().timestamp_millis();
        let expiry_window = 5000; // 5 seconds

        let header = json!({
            "type": "create_order",
            "timestamp": timestamp,
            "expiry_window": expiry_window
        });

        let payload = json!({
            "symbol": symbol,
            "price": rounded_price.to_string(),
            "amount": rounded_size.to_string(),
            "side": side.as_str(),
            "tif": "ALO",  // Add Liquidity Only (post-only)
            "reduce_only": false,
            "client_order_id": client_order_id
        });

        let signature = self.sign_message(header, payload.clone())?;

        // Build request
        let request_body = serde_json::json!({
            "account": self.credentials.account,
            "signature": signature,
            "timestamp": timestamp,
            "expiry_window": expiry_window,
            "symbol": symbol,
            "price": rounded_price.to_string(),
            "amount": rounded_size.to_string(),
            "side": side.as_str(),
            "tif": "ALO",
            "reduce_only": false,
            "client_order_id": client_order_id,
            "agent_wallet": self.credentials.agent_wallet
        });

        debug!("[PACIFICA] Order request: {}", serde_json::to_string_pretty(&request_body)?);

        // Send request
        let url = format!("{}/api/v1/orders/create", self.rest_url);
        let response = self.client
            .post(&url)
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            anyhow::bail!("Order placement failed: {}", error_text);
        }

        let order_response: OrderResponse = response.json().await?;

        let order_data = order_response.data
            .context("No order data in response")?;

        let order_id = order_data.order_id
            .or(order_data.i)
            .context("No order ID in response")?;

        info!(
            "[PACIFICA] Order placed successfully: ID={}, ClientID={}",
            order_id,
            client_order_id
        );

        Ok(OrderData {
            order_id: Some(order_id),
            i: Some(order_id),
            client_order_id: Some(client_order_id),
            symbol: Some(symbol.to_string()),
        })
    }

    /// Cancel an order by client order ID
    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
    ) -> Result<()> {
        info!("[PACIFICA] Cancelling order: {} (ClientID: {})", symbol, client_order_id);

        // Build signature
        let timestamp = chrono::Utc::now().timestamp_millis();
        let expiry_window = 5000;

        let header = json!({
            "type": "cancel_order",
            "timestamp": timestamp,
            "expiry_window": expiry_window
        });

        let payload = json!({
            "symbol": symbol,
            "client_order_id": client_order_id
        });

        let signature = self.sign_message(header, payload.clone())?;

        // Build request
        let request_body = json!({
            "account": self.credentials.account,
            "signature": signature,
            "timestamp": timestamp,
            "expiry_window": expiry_window,
            "symbol": symbol,
            "client_order_id": client_order_id,
            "agent_wallet": self.credentials.agent_wallet
        });

        // Send request
        let url = format!("{}/api/v1/orders/cancel", self.rest_url);
        let response = self.client
            .post(&url)
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            anyhow::bail!("Order cancellation failed: {}", error_text);
        }

        info!("[PACIFICA] Order cancelled successfully: {}", client_order_id);

        Ok(())
    }

    /// Cancel all orders for a specific symbol or all symbols
    ///
    /// # Arguments
    /// * `all_symbols` - If true, cancels orders for all symbols. If false, cancels only for the specified symbol
    /// * `symbol` - Trading symbol (required if all_symbols is false)
    /// * `exclude_reduce_only` - If true, excludes reduce-only orders from cancellation
    pub async fn cancel_all_orders(
        &self,
        all_symbols: bool,
        symbol: Option<&str>,
        exclude_reduce_only: bool,
    ) -> Result<u32> {
        if !all_symbols && symbol.is_none() {
            anyhow::bail!("symbol is required when all_symbols is false");
        }

        info!(
            "[PACIFICA] Cancelling all orders (all_symbols: {}, symbol: {:?}, exclude_reduce_only: {})",
            all_symbols,
            symbol,
            exclude_reduce_only
        );

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

        // Build request
        let mut request_body = json!({
            "account": self.credentials.account,
            "signature": signature,
            "timestamp": timestamp,
            "expiry_window": expiry_window,
            "all_symbols": all_symbols,
            "exclude_reduce_only": exclude_reduce_only,
            "agent_wallet": self.credentials.agent_wallet
        });

        // Add symbol to request body if provided
        if let Some(sym) = symbol {
            request_body["symbol"] = json!(sym);
        }

        // Send request
        let url = format!("{}/api/v1/orders/cancel_all", self.rest_url);
        let response = self.client
            .post(&url)
            .json(&request_body)
            .send()
            .await?;

        // Get response text for debugging
        let response_text = response.text().await?;

        // Try to parse the response
        #[derive(serde::Deserialize, Debug)]
        struct CancelAllData {
            #[serde(default)]
            cancelled_count: Option<u32>,
        }

        #[derive(serde::Deserialize, Debug)]
        struct CancelAllResponse {
            success: bool,
            data: Option<CancelAllData>,
            error: Option<String>,
        }

        let cancel_response: CancelAllResponse = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse response: {}", response_text))?;

        if !cancel_response.success {
            let error_msg = cancel_response.error.unwrap_or_else(|| "Unknown error".to_string());
            anyhow::bail!("Cancel all orders failed: {}", error_msg);
        }

        let cancelled_count = cancel_response
            .data
            .and_then(|d| d.cancelled_count)
            .unwrap_or(0);

        info!(
            "[PACIFICA] Successfully cancelled {} order(s)",
            cancelled_count
        );

        Ok(cancelled_count)
    }

    /// Get trade history for the account
    ///
    /// # Arguments
    /// * `symbol` - Optional symbol to filter by (e.g., "ENA")
    /// * `limit` - Optional maximum number of records to return
    /// * `start_time` - Optional start time in milliseconds
    /// * `end_time` - Optional end time in milliseconds
    ///
    /// # Returns
    /// Vector of trade history items
    /// Get current open positions
    ///
    /// # Arguments
    /// * `symbol` - Optional symbol filter (e.g., "SOL")
    ///
    /// # Returns
    /// Vector of current open positions
    pub async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<PositionInfo>> {
        let mut url = format!("{}/api/v1/positions?account={}",
            self.rest_url,
            self.credentials.account
        );

        if let Some(sym) = symbol {
            url.push_str(&format!("&symbol={}", sym));
        }

        info!("[PACIFICA] Fetching positions from {}", url);

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch positions")?;

        let response_text = response.text().await?;
        debug!("[PACIFICA] Positions response: {}", response_text);

        let positions_response: PositionsResponse = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse positions response: {}", response_text))?;

        if !positions_response.success {
            let error_msg = positions_response.error.unwrap_or_else(|| "Unknown error".to_string());
            anyhow::bail!("Get positions failed: {}", error_msg);
        }

        Ok(positions_response.data.unwrap_or_default())
    }

    pub async fn get_trade_history(
        &self,
        symbol: Option<&str>,
        limit: Option<u32>,
        start_time: Option<u64>,
        end_time: Option<u64>,
    ) -> Result<Vec<TradeHistoryItem>> {
        let mut url = format!("{}/api/v1/positions/history?account={}",
            self.rest_url,
            self.credentials.account
        );

        if let Some(sym) = symbol {
            url.push_str(&format!("&symbol={}", sym));
        }

        if let Some(lim) = limit {
            url.push_str(&format!("&limit={}", lim));
        }

        if let Some(start) = start_time {
            url.push_str(&format!("&start_time={}", start));
        }

        if let Some(end) = end_time {
            url.push_str(&format!("&end_time={}", end));
        }

        info!("[PACIFICA] Fetching trade history from {}", url);

        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch trade history")?;

        let response_text = response.text().await?;

        // Log the raw JSON to see all available fields
        if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&response_text) {
            if let Some(data) = json_value.get("data").and_then(|d| d.as_array()) {
                if !data.is_empty() {
                    info!("[PACIFICA] Sample trade history item (raw JSON): {}", serde_json::to_string_pretty(&data[0]).unwrap_or_default());
                }
            }
        }

        let history_response: TradeHistoryResponse = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse trade history response: {}", response_text))?;

        if !history_response.success {
            let error_msg = history_response.error.unwrap_or_else(|| "Unknown error".to_string());
            anyhow::bail!("Get trade history failed: {}", error_msg);
        }

        Ok(history_response.data.unwrap_or_default())
    }
}

/// Canonicalize JSON by sorting keys alphabetically
/// This matches Python's json.dumps(obj, separators=(",", ":"))
pub fn canonicalize_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            // Use serde_json to properly escape the string
            // This matches Python's json.dumps() string escaping
            serde_json::to_string(s).unwrap()
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonicalize_json).collect();
            format!("[{}]", items.join(","))
        }
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();

            let pairs: Vec<String> = keys
                .iter()
                .map(|k| {
                    // Use serde_json to properly escape the key
                    let escaped_key = serde_json::to_string(k).unwrap();
                    format!("{}:{}", escaped_key, canonicalize_json(&map[*k]))
                })
                .collect();

            format!("{{{}}}", pairs.join(","))
        }
    }
}
