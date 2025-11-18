use anyhow::{Context, Result};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::transaction::eip712::TypedData;
use ethers::types::H256;
use ethers::utils::keccak256;
use reqwest::Client;
use serde_json::json;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::types::*;

const MAINNET_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const MAINNET_EXCHANGE_URL: &str = "https://api.hyperliquid.xyz/exchange";
const TESTNET_INFO_URL: &str = "https://api.hyperliquid-testnet.xyz/info";
const TESTNET_EXCHANGE_URL: &str = "https://api.hyperliquid-testnet.xyz/exchange";

/// Credentials for Hyperliquid trading
#[derive(Clone)]
pub struct HyperliquidCredentials {
    pub private_key: String,
}

impl HyperliquidCredentials {
    /// Load credentials from environment variables
    /// Expects HL_PRIVATE_KEY
    pub fn from_env() -> Result<Self> {
        let private_key = std::env::var("HL_PRIVATE_KEY")
            .context("HL_PRIVATE_KEY environment variable not set")?;

        Ok(Self {
            private_key,
        })
    }
}

    /// Hyperliquid trading client
    pub struct HyperliquidTrading {
        #[allow(dead_code)]
        credentials: HyperliquidCredentials,
        info_url: String,
        exchange_url: String,
    client: Client,
    wallet: LocalWallet,
    meta_cache: Arc<RwLock<Option<MetaResponse>>>,
    is_testnet: bool,
}

impl HyperliquidTrading {
    /// Create a new trading client
    ///
    /// # Arguments
    /// * `credentials` - Hyperliquid credentials (wallet address and private key)
    /// * `is_testnet` - Whether to use testnet (false = mainnet)
    pub fn new(credentials: HyperliquidCredentials, is_testnet: bool) -> Result<Self> {
        let (info_url, exchange_url) = if is_testnet {
            (TESTNET_INFO_URL.to_string(), TESTNET_EXCHANGE_URL.to_string())
        } else {
            (MAINNET_INFO_URL.to_string(), MAINNET_EXCHANGE_URL.to_string())
        };

        // Create wallet from private key
        let wallet = LocalWallet::from_str(&credentials.private_key)
            .context("Failed to create wallet from private key")?;

        Ok(Self {
            credentials,
            info_url,
            exchange_url,
            client: Client::new(),
            wallet,
            meta_cache: Arc::new(RwLock::new(None)),
            is_testnet,
        })
    }

    /// Returns true when this client is configured for testnet.
    pub fn is_testnet(&self) -> bool {
        self.is_testnet
    }

    /// Fetch asset metadata (for asset IDs and szDecimals)
    pub async fn get_meta(&self) -> Result<MetaResponse> {
        // Check cache first
        {
            let cache = self.meta_cache.read().await;
            if let Some(cached) = cache.as_ref() {
                return Ok(cached.clone());
            }
        }

        info!("[HYPERLIQUID] Fetching asset metadata");

        let response = self
            .client
            .post(&self.info_url)
            .json(&json!({
                "type": "meta"
            }))
            .send()
            .await
            .context("Failed to fetch meta")?;

        // Get response text for debugging
        let response_text = response
            .text()
            .await
            .context("Failed to read response text")?;

        debug!("[HYPERLIQUID] Meta response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>());

        let meta: MetaResponse = serde_json::from_str(&response_text)
            .context(format!("Failed to parse meta response. First 200 chars: {}",
                &response_text.chars().take(200).collect::<String>()))?;

        // Cache the result
        {
            let mut cache = self.meta_cache.write().await;
            *cache = Some(meta.clone());
        }

        debug!("[HYPERLIQUID] Loaded {} assets", meta.universe.len());
        Ok(meta)
    }

    /// Get asset ID from coin name
    pub async fn get_asset_id(&self, coin: &str) -> Result<u32> {
        let meta = self.get_meta().await?;

        let asset_index = meta
            .universe
            .iter()
            .position(|asset| asset.name == coin)
            .with_context(|| format!("Asset {} not found in meta", coin))?;

        Ok(asset_index as u32)
    }

    /// Get asset metadata (szDecimals, etc.)
    pub async fn get_asset_info(&self, coin: &str) -> Result<AssetMeta> {
        let meta = self.get_meta().await?;

        meta.universe
            .iter()
            .find(|asset| asset.name == coin)
            .cloned()
            .with_context(|| format!("Asset {} not found in meta", coin))
    }

    /// Get L2 orderbook snapshot via info endpoint
    pub async fn get_l2_snapshot(&self, coin: &str) -> Result<Option<(f64, f64)>> {
        debug!("[HYPERLIQUID] Fetching L2 snapshot for {}", coin);

        let request_body = serde_json::json!({
            "type": "l2Book",
            "coin": coin
        });

        let response = self.client
            .post(&self.info_url)
            .json(&request_body)
            .send()
            .await
            .context("Failed to fetch L2 snapshot")?;

        if !response.status().is_success() {
            anyhow::bail!("L2 snapshot request failed: {}", response.status());
        }

        let response_text = response.text().await.context("Failed to read L2 response")?;

        // Parse response to extract levels
        let data: serde_json::Value = serde_json::from_str(&response_text)
            .context("Failed to parse L2 response")?;

        // Extract best bid and ask from levels
        // levels[0] = array of bid levels [{px, sz, n}, ...]
        // levels[1] = array of ask levels [{px, sz, n}, ...]
        let levels = data.get("levels")
            .and_then(|v| v.as_array())
            .context("Missing levels array in L2 response")?;

        if levels.len() < 2 {
            return Ok(None);
        }

        // Get best bid (first element of bids array)
        let best_bid = levels.get(0)
            .and_then(|bids| bids.as_array())
            .and_then(|bids| bids.first())
            .and_then(|bid| bid.get("px"))
            .and_then(|px| px.as_str())
            .and_then(|s| s.parse::<f64>().ok());

        // Get best ask (first element of asks array)
        let best_ask = levels.get(1)
            .and_then(|asks| asks.as_array())
            .and_then(|asks| asks.first())
            .and_then(|ask| ask.get("px"))
            .and_then(|px| px.as_str())
            .and_then(|s| s.parse::<f64>().ok());

        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => {
                debug!("[HYPERLIQUID] L2 snapshot: bid=${:.6}, ask=${:.6}", bid, ask);
                Ok(Some((bid, ask)))
            }
            _ => Ok(None),
        }
    }

    /// Round price to proper tick size
    /// Prices can have up to 5 significant figures
    /// Max decimals = MAX_DECIMALS - szDecimals (6 for perps, 8 for spot)
    ///
    /// Reference: hyperliquid.js roundPrice() - EXACT MATCH
    fn round_price(price: f64, sz_decimals: i32, is_spot: bool, _is_buy: bool, _aggressive: bool) -> String {
        let max_decimals = (if is_spot { 8 } else { 6 }) - sz_decimals;

        // Step 1: Round to 5 significant figures
        // Equivalent to JavaScript's toPrecision(5) then parseFloat
        let rounded = if price > 0.0 {
            // Calculate the magnitude (power of 10)
            let magnitude = price.log10().floor();
            let scale = 10_f64.powf(magnitude - 4.0); // 5 sig figs = magnitude - 4

            // Round to 5 significant figures
            (price / scale).round() * scale
        } else {
            price
        };

        // Step 2: Limit to max decimal places
        // Equivalent to JavaScript's toFixed(maxDecimals) then parseFloat
        let rounded = if max_decimals >= 0 {
            let decimal_multiplier = 10_f64.powi(max_decimals);
            (rounded * decimal_multiplier).round() / decimal_multiplier
        } else {
            rounded
        };

        // Step 3: Format with max_decimals precision
        let max_decimals_clamped = max_decimals.max(0) as usize;
        let result = format!("{:.prec$}", rounded, prec = max_decimals_clamped);

        // Step 4: Remove trailing zeros (like JavaScript's toString())
        if result.contains('.') {
            result.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            result
        }
    }

    /// Round size to proper lot size (szDecimals)
    fn round_size(size: f64, sz_decimals: i32) -> String {
        let rounded = format!("{:.prec$}", size, prec = sz_decimals.max(0) as usize)
            .parse::<f64>()
            .unwrap();

        rounded.to_string()
    }

    /// Construct connection ID for EIP-712 signing
    /// This is a keccak256 hash of msgpack-encoded action + nonce + vault indicator
    fn construct_connection_id(
        action: &Action,
        nonce: u64,
        vault_address: Option<&str>,
    ) -> Result<H256> {
        // Encode action with msgpack (using named encoding for maps, not arrays)
        let action_bytes = rmp_serde::encode::to_vec_named(action)
            .context("Failed to encode action with msgpack")?;

        let mut data_to_hash = Vec::new();

        // Add action bytes
        data_to_hash.extend_from_slice(&action_bytes);

        // Add nonce as 8-byte big-endian
        data_to_hash.extend_from_slice(&nonce.to_be_bytes());

        // Add vault address indicator (1 if vault, 0 if not)
        data_to_hash.push(if vault_address.is_some() { 1 } else { 0 });

        // Hash the combined data
        let hash = keccak256(&data_to_hash);

        Ok(H256::from(hash))
    }

    /// Sign an action using EIP-712
    async fn sign_action(
        &self,
        action: &Action,
        nonce: u64,
        vault_address: Option<&str>,
    ) -> Result<Signature> {
        // Construct connection ID
        let connection_id = Self::construct_connection_id(action, nonce, vault_address)?;

        // Construct EIP-712 domain
        let domain = json!({
            "chainId": 1337,
            "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1"
        });

        // Construct EIP-712 types
        let types = json!({
            "Agent": [
                { "name": "source", "type": "string" },
                { "name": "connectionId", "type": "bytes32" }
            ]
        });

        // Construct phantom agent
        // source: "a" for mainnet, "b" for testnet
        let source = if self.is_testnet { "b" } else { "a" };

        let message = json!({
            "source": source,
            "connectionId": format!("0x{}", hex::encode(connection_id.as_bytes()))
        });

        // Create EIP-712 typed data
        let typed_data = TypedData {
            domain: serde_json::from_value(domain)?,
            types: serde_json::from_value(types)?,
            primary_type: "Agent".to_string(),
            message: serde_json::from_value(message)?,
        };

        // Sign the typed data
        let sig = self.wallet.sign_typed_data(&typed_data).await?;

        // Convert r and s from U256 to 32-byte arrays
        let mut r_bytes = [0u8; 32];
        let mut s_bytes = [0u8; 32];
        sig.r.to_big_endian(&mut r_bytes);
        sig.s.to_big_endian(&mut s_bytes);

        Ok(Signature {
            r: format!("0x{}", hex::encode(r_bytes)),
            s: format!("0x{}", hex::encode(s_bytes)),
            v: sig.v as u32,
        })
    }

    /// Build a signed market order request (IOC limit order with slippage).
    ///
    /// This constructs and signs the order payload that can be sent either via
    /// REST (`/exchange`) or via WebSocket `post` (type: "action").
    pub async fn build_market_order_request(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
    ) -> Result<OrderRequest> {
        // Get asset ID and metadata
        let asset_id = self.get_asset_id(coin).await?;
        let asset_info = self.get_asset_info(coin).await?;

        // Check if we have bid/ask prices
        if bid.is_none() || ask.is_none() {
            anyhow::bail!("Bid and ask prices are required. Please provide them from the orderbook client.");
        }

        let bid_price = bid.unwrap();
        let ask_price = ask.unwrap();
        let mid_price = (bid_price + ask_price) / 2.0;

        // Calculate limit price with slippage
        // For buy: midPrice * (1 + slippage)
        // For sell: midPrice * (1 - slippage)
        let limit_price = if is_buy {
            mid_price * (1.0 + slippage)
        } else {
            mid_price * (1.0 - slippage)
        };

        // Round price and size
        let is_spot = asset_id >= 10000;
        let limit_price_str = Self::round_price(limit_price, asset_info.sz_decimals, is_spot, is_buy, true); // aggressive=true for market orders
        let size_str = Self::round_size(size, asset_info.sz_decimals);

        info!(
            "[HYPERLIQUID] Market order {} {} {} at limit {} (mid: {:.2}, slippage: {}%, szDecimals: {})",
            if is_buy { "BUY" } else { "SELL" },
            size_str,
            coin,
            limit_price_str,
            mid_price,
            slippage * 100.0,
            asset_info.sz_decimals
        );

        // Construct order
        let order = Order {
            a: asset_id,
            b: is_buy,
            p: limit_price_str,
            s: size_str,
            r: reduce_only,
            t: OrderType {
                limit: LimitOrderType {
                    tif: TimeInForce::Ioc,
                },
            },
            c: None,
        };

        // Construct action
        let action = Action {
            type_: "order".to_string(),
            orders: vec![order],
            grouping: "na".to_string(),
        };

        // Get nonce (current timestamp in milliseconds)
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64;

        // Sign the action
        let signature = self.sign_action(&action, nonce, None).await?;

        // Construct request payload
        Ok(OrderRequest {
            action,
            nonce,
            signature,
            vault_address: None,
        })
    }

    /// Place a market order (IOC limit order with slippage)
    ///
    /// # Arguments
    /// * `coin` - Coin symbol (e.g., "SOL", "BTC")
    /// * `is_buy` - True for buy, false for sell
    /// * `size` - Order size
    /// * `slippage` - Slippage tolerance (default 0.05 = 5%)
    /// * `reduce_only` - Whether this is a reduce-only order
    /// * `bid` - Current bid price (if None, will fetch from orderbook client)
    /// * `ask` - Current ask price (if None, will fetch from orderbook client)
    ///
    /// # Returns
    /// Order response with status and order ID
    pub async fn place_market_order(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
    ) -> Result<OrderResponse> {
        // Build signed order payload (shared with WebSocket execution path)
        let payload = self
            .build_market_order_request(coin, is_buy, size, slippage, reduce_only, bid, ask)
            .await?;

        // Send order via REST API
        debug!("[HYPERLIQUID] Sending order to exchange");
        let response = self
            .client
            .post(&self.exchange_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to send order")?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            anyhow::bail!("Order failed: {}", error_text);
        }

        // Get response text for debugging
        let response_text = response
            .text()
            .await
            .context("Failed to read response text")?;

        debug!("[HYPERLIQUID] Order response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>());

        let order_response: OrderResponse = serde_json::from_str(&response_text)
            .context(format!("Failed to parse order response. Response text: {}",
                &response_text.chars().take(300).collect::<String>()))?;

        // Check if response indicates error
        match &order_response.response {
            crate::connector::hyperliquid::OrderResponseContent::Error(error_msg) => {
                anyhow::bail!("Order rejected by exchange: {}", error_msg);
            }
            crate::connector::hyperliquid::OrderResponseContent::Success(_) => {
                info!("[HYPERLIQUID] Order response: {:?}", order_response);
            }
        }

        Ok(order_response)
    }

    /// Get user fills (trade history)
    ///
    /// # Arguments
    /// * `user` - User wallet address in 42-character hexadecimal format
    /// * `aggregate_by_time` - When true, partial fills are combined when a crossing order
    ///                         gets filled by multiple different resting orders
    ///
    /// # Returns
    /// Vector of user fills (up to 2000 most recent fills)
    pub async fn get_user_fills(&self, user: &str, aggregate_by_time: bool) -> Result<Vec<UserFill>> {
        info!("[HYPERLIQUID] Fetching user fills for {} (aggregate: {})", user, aggregate_by_time);

        let payload = json!({
            "type": "userFills",
            "user": user,
            "aggregateByTime": aggregate_by_time
        });

        let response = self
            .client
            .post(&self.info_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to fetch user fills")?;

        let response_text = response.text().await?;
        debug!("[HYPERLIQUID] User fills response: {}", response_text);

        let fills: Vec<UserFill> = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse user fills response: {}", response_text))?;

        debug!("[HYPERLIQUID] Retrieved {} fill(s)", fills.len());

        Ok(fills)
    }

    /// Get user state (positions and margin summary)
    ///
    /// # Arguments
    /// * `user` - User wallet address in 42-character hexadecimal format
    ///
    /// # Returns
    /// User state with positions, margin summary, and account info
    pub async fn get_user_state(&self, user: &str) -> Result<UserState> {
        debug!("[HYPERLIQUID] Fetching user state for {}", user);

        let payload = json!({
            "type": "clearinghouseState",
            "user": user
        });

        let response = self
            .client
            .post(&self.info_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to fetch user state")?;

        let response_text = response.text().await?;
        debug!("[HYPERLIQUID] User state response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>());

        let user_state: UserState = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse user state response: {}", response_text))?;

        debug!("[HYPERLIQUID] Retrieved {} position(s)", user_state.asset_positions.len());

        Ok(user_state)
    }

    /// Get wallet address from the internal wallet
    pub fn get_wallet_address(&self) -> String {
        format!("{:?}", self.wallet.address())
    }
}
