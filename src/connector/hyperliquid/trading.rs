use anyhow::{Context, Result};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::H256;
use ethers::utils::keccak256;
use reqwest::Client;
use serde_json::json;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::types::*;
use crate::market_rules::hyperliquid_size_floor;

/// Current Unix time in milliseconds (0 if the clock is before the epoch).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Refresh the cached asset metadata at most this often. The HL universe is
/// append-only (existing asset indices are stable), so this is a low-frequency
/// safety refresh against any future reindexing, not a hot-path concern.
const META_TTL_MS: u64 = 3_600_000; // 1 hour

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

        Ok(Self { private_key })
    }
}

/// Hyperliquid trading client
pub struct HyperliquidTrading {
    credentials: HyperliquidCredentials,
    info_url: String,
    exchange_url: String,
    client: Client,
    wallet: LocalWallet,
    meta_cache: Arc<RwLock<Option<MetaResponse>>>,
    /// Wall-clock ms when `meta_cache` was last populated (0 = never).
    meta_fetched_at: AtomicU64,
    /// Hot-path cache of `(coin, asset_id, AssetMeta)` for the single traded
    /// symbol. Avoids cloning the entire asset universe out of `meta_cache`
    /// on every order build (3x per hedge before this existed).
    asset_cache: parking_lot::RwLock<Option<(String, u32, AssetMeta)>>,
    /// `account_address()` result, computed once (env var reads take a
    /// process-global lock and this is called on every position/status check).
    account_address_cache: OnceLock<String>,
    is_testnet: bool,
    /// Monotonic nonce source: strictly increasing, fast-forwarded to wall-clock
    /// ms but never going backward, so two signed actions in the same millisecond
    /// (or across a clock step-back) cannot collide.
    nonce: AtomicU64,
    /// Precomputed EIP-712 constants: the domain, Agent type hash, and source
    /// hash never change for the life of the client, so per-signature work
    /// reduces to two keccaks over fixed-size buffers + the ECDSA sign.
    eip712_domain_separator: [u8; 32],
    eip712_agent_type_hash: [u8; 32],
    eip712_source_hash: [u8; 32],
}

impl HyperliquidTrading {
    /// Create a new trading client
    ///
    /// # Arguments
    /// * `credentials` - Hyperliquid credentials (wallet address and private key)
    /// * `is_testnet` - Whether to use testnet (false = mainnet)
    pub fn new(credentials: HyperliquidCredentials, is_testnet: bool) -> Result<Self> {
        let (info_url, exchange_url) = if is_testnet {
            (
                TESTNET_INFO_URL.to_string(),
                TESTNET_EXCHANGE_URL.to_string(),
            )
        } else {
            (
                MAINNET_INFO_URL.to_string(),
                MAINNET_EXCHANGE_URL.to_string(),
            )
        };

        // Create wallet from private key
        let wallet = LocalWallet::from_str(&credentials.private_key)
            .context("Failed to create wallet from private key")?;

        // Bounded HTTP: a hung REST hedge submit / orderStatus query must fail
        // in seconds, not stall the serial hedge executor for the OS TCP
        // timeout (minutes). The error path treats a timeout as an uncertain
        // submit and retries with the SAME cloid, so this is idempotency-safe.
        // pool_idle_timeout(None) keeps the warm TLS connection alive between
        // calls (the reconciler polls every 1s, so it never actually idles).
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .connect_timeout(std::time::Duration::from_secs(2))
            .tcp_nodelay(true)
            .pool_idle_timeout(None)
            .build()
            .context("Failed to build Hyperliquid HTTP client")?;

        let source = if is_testnet { "b" } else { "a" };

        Ok(Self {
            credentials,
            info_url,
            exchange_url,
            client,
            wallet,
            meta_cache: Arc::new(RwLock::new(None)),
            meta_fetched_at: AtomicU64::new(0),
            asset_cache: parking_lot::RwLock::new(None),
            account_address_cache: OnceLock::new(),
            is_testnet,
            nonce: AtomicU64::new(now_ms()),
            eip712_domain_separator: Self::compute_domain_separator(),
            eip712_agent_type_hash: keccak256(b"Agent(string source,bytes32 connectionId)"),
            eip712_source_hash: keccak256(source.as_bytes()),
        })
    }

    /// EIP-712 domain separator for Hyperliquid's phantom-agent domain:
    /// `{name: "Exchange", version: "1", chainId: 1337, verifyingContract: 0x0}`.
    /// Constant for the life of the process, so computed once.
    fn compute_domain_separator() -> [u8; 32] {
        let type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );
        let name_hash = keccak256(b"Exchange");
        let version_hash = keccak256(b"1");
        let mut chain_id = [0u8; 32];
        chain_id[24..].copy_from_slice(&1337u64.to_be_bytes());
        let verifying_contract = [0u8; 32]; // address(0), left-padded

        let mut encoded = Vec::with_capacity(160);
        encoded.extend_from_slice(&type_hash);
        encoded.extend_from_slice(&name_hash);
        encoded.extend_from_slice(&version_hash);
        encoded.extend_from_slice(&chain_id);
        encoded.extend_from_slice(&verifying_contract);
        keccak256(&encoded)
    }

    /// EIP-712 digest for `Agent { source, connectionId }` using the
    /// precomputed domain separator / type hash / source hash.
    fn eip712_agent_digest(&self, connection_id: H256) -> [u8; 32] {
        let mut struct_encoded = [0u8; 96];
        struct_encoded[0..32].copy_from_slice(&self.eip712_agent_type_hash);
        struct_encoded[32..64].copy_from_slice(&self.eip712_source_hash);
        struct_encoded[64..96].copy_from_slice(connection_id.as_bytes());
        let struct_hash = keccak256(struct_encoded);

        let mut digest_input = [0u8; 66];
        digest_input[0] = 0x19;
        digest_input[1] = 0x01;
        digest_input[2..34].copy_from_slice(&self.eip712_domain_separator);
        digest_input[34..66].copy_from_slice(&struct_hash);
        keccak256(digest_input)
    }

    /// Returns true when this client is configured for testnet.
    pub fn is_testnet(&self) -> bool {
        self.is_testnet
    }

    /// Lock-free strictly-increasing nonce, fast-forwarded to wall-clock ms.
    fn next_nonce(&self) -> u64 {
        let now = now_ms();
        let mut prev = self.nonce.load(Ordering::Relaxed);
        loop {
            let next = now.max(prev + 1);
            match self.nonce.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return next,
                Err(observed) => prev = observed,
            }
        }
    }

    /// Fetch asset metadata (for asset IDs and szDecimals)
    pub async fn get_meta(&self) -> Result<MetaResponse> {
        // Check cache first (honoring the TTL so a stale asset map is refreshed).
        {
            let cache = self.meta_cache.read().await;
            if let Some(cached) = cache.as_ref() {
                let age = now_ms().saturating_sub(self.meta_fetched_at.load(Ordering::Relaxed));
                if age < META_TTL_MS {
                    return Ok(cached.clone());
                }
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

        debug!(
            "[HYPERLIQUID] Meta response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>()
        );

        let meta: MetaResponse = serde_json::from_str(&response_text).context(format!(
            "Failed to parse meta response. First 200 chars: {}",
            &response_text.chars().take(200).collect::<String>()
        ))?;

        // Cache the result
        {
            let mut cache = self.meta_cache.write().await;
            *cache = Some(meta.clone());
            self.meta_fetched_at.store(now_ms(), Ordering::Relaxed);
        }

        debug!("[HYPERLIQUID] Loaded {} assets", meta.universe.len());
        Ok(meta)
    }

    /// True when the cached meta (and therefore the per-symbol asset cache)
    /// has outlived `META_TTL_MS` and should be refreshed.
    fn meta_stale(&self) -> bool {
        now_ms().saturating_sub(self.meta_fetched_at.load(Ordering::Relaxed)) >= META_TTL_MS
    }

    /// Refresh the per-symbol asset cache from (possibly re-fetched) meta.
    async fn refresh_asset_cache(&self, coin: &str) -> Result<(u32, AssetMeta)> {
        let meta = self.get_meta().await?;
        let asset_index = meta
            .universe
            .iter()
            .position(|asset| asset.name == coin)
            .with_context(|| format!("Asset {} not found in meta", coin))?;
        let asset = meta.universe[asset_index].clone();
        *self.asset_cache.write() = Some((coin.to_string(), asset_index as u32, asset.clone()));
        Ok((asset_index as u32, asset))
    }

    /// Get asset ID from coin name (per-symbol cached; no universe clone).
    pub async fn get_asset_id(&self, coin: &str) -> Result<u32> {
        if !self.meta_stale() {
            if let Some((cached_coin, asset_id, _)) = self.asset_cache.read().as_ref() {
                if cached_coin == coin {
                    return Ok(*asset_id);
                }
            }
        }
        self.refresh_asset_cache(coin).await.map(|(id, _)| id)
    }

    /// Get asset metadata (szDecimals, etc.) (per-symbol cached).
    pub async fn get_asset_info(&self, coin: &str) -> Result<AssetMeta> {
        if !self.meta_stale() {
            if let Some((cached_coin, _, asset)) = self.asset_cache.read().as_ref() {
                if cached_coin == coin {
                    return Ok(asset.clone());
                }
            }
        }
        self.refresh_asset_cache(coin).await.map(|(_, asset)| asset)
    }

    /// Get L2 orderbook snapshot via info endpoint
    pub async fn get_l2_snapshot(&self, coin: &str) -> Result<Option<(f64, f64)>> {
        debug!("[HYPERLIQUID] Fetching L2 snapshot for {}", coin);

        let request_body = serde_json::json!({
            "type": "l2Book",
            "coin": coin
        });

        let response = self
            .client
            .post(&self.info_url)
            .json(&request_body)
            .send()
            .await
            .context("Failed to fetch L2 snapshot")?;

        if !response.status().is_success() {
            anyhow::bail!("L2 snapshot request failed: {}", response.status());
        }

        let response_text = response
            .text()
            .await
            .context("Failed to read L2 response")?;

        // Parse response to extract levels
        let data: serde_json::Value =
            serde_json::from_str(&response_text).context("Failed to parse L2 response")?;

        // Extract best bid and ask from levels
        // levels[0] = array of bid levels [{px, sz, n}, ...]
        // levels[1] = array of ask levels [{px, sz, n}, ...]
        let levels = data
            .get("levels")
            .and_then(|v| v.as_array())
            .context("Missing levels array in L2 response")?;

        if levels.len() < 2 {
            return Ok(None);
        }

        // Get best bid (first element of bids array). Reject non-finite /
        // non-positive levels at parse time so a corrupted L2 response can never
        // feed garbage into hedge price math.
        let best_bid = levels
            .get(0)
            .and_then(|bids| bids.as_array())
            .and_then(|bids| bids.first())
            .and_then(|bid| bid.get("px"))
            .and_then(|px| px.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0);

        // Get best ask (first element of asks array)
        let best_ask = levels
            .get(1)
            .and_then(|asks| asks.as_array())
            .and_then(|asks| asks.first())
            .and_then(|ask| ask.get("px"))
            .and_then(|px| px.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0);

        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) => {
                debug!(
                    "[HYPERLIQUID] L2 snapshot: bid=${:.6}, ask=${:.6}",
                    bid, ask
                );
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
    fn round_price(
        price: f64,
        sz_decimals: i32,
        is_spot: bool,
        _is_buy: bool,
        _aggressive: bool,
    ) -> String {
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
            result
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            result
        }
    }

    /// Round size to proper lot size (szDecimals)
    fn round_size(size: f64, sz_decimals: i32) -> String {
        let rounded = hyperliquid_size_floor(size, sz_decimals);

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

    /// Sign an action using EIP-712.
    ///
    /// The digest is computed manually from precomputed constants (see
    /// `eip712_agent_digest`) instead of rebuilding a `TypedData` JSON
    /// structure per call; digest parity with the `TypedData` path is pinned
    /// by `eip712_manual_digest_matches_typed_data` below.
    async fn sign_action(
        &self,
        action: &Action,
        nonce: u64,
        vault_address: Option<&str>,
    ) -> Result<Signature> {
        // Construct connection ID
        let connection_id = Self::construct_connection_id(action, nonce, vault_address)?;

        let digest = self.eip712_agent_digest(connection_id);
        let sig = self.wallet.sign_hash(H256::from(digest))?;

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
        self.build_market_order_request_with_cloid(
            coin,
            is_buy,
            size,
            slippage,
            reduce_only,
            bid,
            ask,
            None,
        )
        .await
    }

    pub async fn build_market_order_request_with_cloid(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
        cloid: Option<String>,
    ) -> Result<OrderRequest> {
        // Get asset ID and metadata
        let asset_id = self.get_asset_id(coin).await?;
        let asset_info = self.get_asset_info(coin).await?;

        // Check if we have bid/ask prices
        if bid.is_none() || ask.is_none() {
            anyhow::bail!(
                "Bid and ask prices are required. Please provide them from the orderbook client."
            );
        }

        let bid_price = bid.unwrap();
        let ask_price = ask.unwrap();
        // Hard-validate the inputs before any price math: a non-finite or absurd
        // bid/ask (corrupted REST L2 / data glitch) must never produce a hedge
        // order priced from inf/garbage.
        anyhow::ensure!(
            bid_price.is_finite()
                && ask_price.is_finite()
                && bid_price > 0.0
                && ask_price > 0.0
                && ask_price >= bid_price,
            "Invalid bid/ask for HL order: bid={bid_price} ask={ask_price}"
        );
        // Never sign a degenerate zero-size order: a fill below one HL size step
        // would otherwise floor to "0". Bail so the caller routes the residual to
        // the reconciler instead of sending an order the venue rejects.
        anyhow::ensure!(
            hyperliquid_size_floor(size, asset_info.sz_decimals) > 0.0,
            "HL hedge size {} floors to 0 at szDecimals={}",
            size,
            asset_info.sz_decimals
        );
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
        let mut limit_price_str =
            Self::round_price(limit_price, asset_info.sz_decimals, is_spot, is_buy, true); // aggressive=true for market orders

        // round_price rounds to NEAREST, so a small configured slippage can land
        // the IOC limit on the non-marketable side of the book (=> missed hedge).
        // Bump one grid step toward marketability if that happened.
        {
            let max_decimals = ((if is_spot { 8 } else { 6 }) - asset_info.sz_decimals).max(0);
            let grid_step = 10_f64.powi(-max_decimals);
            if let Ok(mut p) = limit_price_str.parse::<f64>() {
                if is_buy && p < ask_price {
                    p += grid_step;
                    limit_price_str =
                        Self::round_price(p, asset_info.sz_decimals, is_spot, is_buy, true);
                } else if !is_buy && p > bid_price {
                    p -= grid_step;
                    limit_price_str =
                        Self::round_price(p, asset_info.sz_decimals, is_spot, is_buy, true);
                }
            }
        }

        // Final price sanity: never sign a non-finite / non-positive limit.
        anyhow::ensure!(
            limit_price_str
                .parse::<f64>()
                .map_or(false, |p| p.is_finite() && p > 0.0),
            "Computed HL limit price not finite/positive: {limit_price_str}"
        );

        let size_str = Self::round_size(size, asset_info.sz_decimals);

        // debug (not info): this runs on every hedge submit attempt - the
        // latency-critical fill->hedge path must not pay for log formatting.
        debug!(
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
            c: cloid,
        };

        // Construct action
        let action = Action {
            type_: "order".to_string(),
            orders: vec![order],
            grouping: "na".to_string(),
        };

        // Monotonic nonce (never collides within a millisecond / across a
        // clock step-back).
        let nonce = self.next_nonce();

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
        self.place_market_order_with_cloid(
            coin,
            is_buy,
            size,
            slippage,
            reduce_only,
            bid,
            ask,
            None,
        )
        .await
    }

    pub async fn place_market_order_with_cloid(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
        cloid: Option<String>,
    ) -> Result<OrderResponse> {
        // Build signed order payload (shared with WebSocket execution path)
        let payload = self
            .build_market_order_request_with_cloid(
                coin,
                is_buy,
                size,
                slippage,
                reduce_only,
                bid,
                ask,
                cloid,
            )
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

        debug!(
            "[HYPERLIQUID] Order response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>()
        );

        let order_response: OrderResponse =
            serde_json::from_str(&response_text).context(format!(
                "Failed to parse order response. Response text: {}",
                &response_text.chars().take(300).collect::<String>()
            ))?;

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
    pub async fn get_user_fills(
        &self,
        user: &str,
        aggregate_by_time: bool,
    ) -> Result<Vec<UserFill>> {
        info!(
            "[HYPERLIQUID] Fetching user fills for {} (aggregate: {})",
            user, aggregate_by_time
        );

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

    /// Query authoritative status of a single order by cloid (0x + 32 hex) or oid.
    ///
    /// Hyperliquid's `orderStatus` info endpoint accepts either a u64 oid or a
    /// 16-byte hex client order id; our hedge cloids are 16-byte hex so they are
    /// valid inputs. `orderStatus` does not carry an average fill price, so when
    /// the order shows a fill we resolve the size-weighted avg price from
    /// `userFills` by the now-known oid (one extra round-trip only on the fill
    /// path; resting/rejected/unknown stay single round-trip).
    pub async fn query_order_status(&self, cloid_or_oid: &str) -> Result<OrderStatusQuery> {
        let user = self.account_address();
        // A &str serializes to a JSON string, which HL interprets as the cloid form.
        let payload = json!({
            "type": "orderStatus",
            "user": user,
            "oid": cloid_or_oid,
        });

        let response = self
            .client
            .post(&self.info_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to fetch order status")?;

        let response_text = response.text().await?;
        let parsed: OrderStatusResponse = serde_json::from_str(&response_text).with_context(|| {
            format!(
                "Failed to parse orderStatus response: {}",
                response_text.chars().take(300).collect::<String>()
            )
        })?;

        if parsed.status == "unknownOid" {
            return Ok(OrderStatusQuery::Unknown);
        }
        let Some(wrapper) = parsed.order else {
            return Ok(OrderStatusQuery::Unknown);
        };

        let orig_sz = wrapper.order.orig_sz.parse::<f64>().unwrap_or(0.0);
        let remaining = wrapper.order.sz.parse::<f64>().unwrap_or(0.0);
        let filled_sz = (orig_sz - remaining).max(0.0);
        let oid = wrapper.order.oid;
        let status = wrapper.status;

        // Only pay for the avg-price lookup when something actually filled.
        // `userFills` is eventually consistent and can lag `orderStatus`, so if it
        // hasn't surfaced the fill yet, fall back to the order's limit price rather
        // than recording a $0 fill price into the PnL/audit trail. For a marketable
        // IOC the limit price is a close, conservative proxy.
        let avg_px = if filled_sz > 0.0 {
            let from_fills = self.avg_fill_px_for_oid(oid).await.unwrap_or(0.0);
            if from_fills > 0.0 {
                from_fills
            } else {
                wrapper.order.limit_px.parse::<f64>().unwrap_or(0.0)
            }
        } else {
            0.0
        };

        let s = status.as_str();
        Ok(if s == "filled" {
            OrderStatusQuery::Filled {
                filled_sz,
                avg_px,
                oid,
            }
        } else if s == "open" || s == "triggered" {
            if filled_sz > 0.0 {
                OrderStatusQuery::Filled {
                    filled_sz,
                    avg_px,
                    oid,
                }
            } else {
                OrderStatusQuery::Resting {
                    oid,
                    remaining_sz: remaining,
                }
            }
        } else if s == "rejected" || s.ends_with("Rejected") {
            OrderStatusQuery::Rejected { oid, status }
        } else {
            // canceled / marginCanceled / <variant>Canceled: may carry a partial fill.
            OrderStatusQuery::Canceled {
                oid,
                status,
                filled_sz,
                avg_px,
            }
        })
    }

    /// Size-weighted average fill price for an order id, from recent user fills.
    async fn avg_fill_px_for_oid(&self, oid: u64) -> Result<f64> {
        let fills = self.get_user_fills(&self.account_address(), true).await?;
        let mut size_sum = 0.0;
        let mut notional = 0.0;
        for fill in fills.iter().filter(|fill| fill.oid == oid) {
            let px = fill.px.parse::<f64>().unwrap_or(0.0);
            let sz = fill.sz.parse::<f64>().unwrap_or(0.0);
            if px > 0.0 && sz > 0.0 {
                size_sum += sz;
                notional += px * sz;
            }
        }
        if size_sum > 0.0 {
            Ok(notional / size_sum)
        } else {
            Ok(0.0)
        }
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
        debug!(
            "[HYPERLIQUID] User state response (first 500 chars): {}",
            &response_text.chars().take(500).collect::<String>()
        );

        let user_state: UserState = serde_json::from_str(&response_text)
            .with_context(|| format!("Failed to parse user state response: {}", response_text))?;

        debug!(
            "[HYPERLIQUID] Retrieved {} position(s)",
            user_state.asset_positions.len()
        );

        Ok(user_state)
    }

    /// Get wallet address from the internal wallet
    pub fn get_wallet_address(&self) -> String {
        format!("{:?}", self.wallet.address())
    }

    /// Canonical Hyperliquid account address to query for positions/fills.
    ///
    /// Honors the `HL_WALLET` env var (the *account* being traded, which may
    /// differ from the key-derived *signing* wallet for agent/API-wallet
    /// setups) and falls back to the derived address. All services must use
    /// this so position/exposure checks agree on a single account. Resolved
    /// once and cached: env reads take a process-global lock and this is on
    /// every position/orderStatus call.
    pub fn account_address(&self) -> String {
        self.account_address_cache
            .get_or_init(|| std::env::var("HL_WALLET").unwrap_or_else(|_| self.get_wallet_address()))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::transaction::eip712::{Eip712, TypedData};

    fn test_client(is_testnet: bool) -> HyperliquidTrading {
        HyperliquidTrading::new(
            HyperliquidCredentials {
                // Well-known throwaway dev key (anvil account 0); never funded.
                private_key:
                    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
                        .to_string(),
            },
            is_testnet,
        )
        .unwrap()
    }

    /// Golden parity: the manual precomputed-constant digest must equal the
    /// digest the previous `TypedData`-based implementation produced. Pins the
    /// L4 optimization against the reference encoder for both networks.
    #[test]
    fn eip712_manual_digest_matches_typed_data() {
        for is_testnet in [false, true] {
            let client = test_client(is_testnet);

            let action = Action {
                type_: "order".to_string(),
                orders: vec![Order {
                    a: 5,
                    b: true,
                    p: "123.45".to_string(),
                    s: "0.5".to_string(),
                    r: false,
                    t: OrderType {
                        limit: LimitOrderType {
                            tif: TimeInForce::Ioc,
                        },
                    },
                    c: Some("0x000102030405060708090a0b0c0d0e0f".to_string()),
                }],
                grouping: "na".to_string(),
            };
            let connection_id =
                HyperliquidTrading::construct_connection_id(&action, 1_234_567, None).unwrap();

            // Reference digest via the exact TypedData construction the
            // pre-optimization sign_action used.
            let source = if is_testnet { "b" } else { "a" };
            let typed_data = TypedData {
                domain: serde_json::from_value(json!({
                    "chainId": 1337,
                    "name": "Exchange",
                    "verifyingContract": "0x0000000000000000000000000000000000000000",
                    "version": "1"
                }))
                .unwrap(),
                types: serde_json::from_value(json!({
                    "Agent": [
                        { "name": "source", "type": "string" },
                        { "name": "connectionId", "type": "bytes32" }
                    ]
                }))
                .unwrap(),
                primary_type: "Agent".to_string(),
                message: serde_json::from_value(json!({
                    "source": source,
                    "connectionId": format!("0x{}", hex::encode(connection_id.as_bytes()))
                }))
                .unwrap(),
            };
            let reference = typed_data.encode_eip712().unwrap();

            let manual = client.eip712_agent_digest(connection_id);
            assert_eq!(
                reference, manual,
                "EIP-712 digest mismatch (testnet={})",
                is_testnet
            );
        }
    }
}

/// REST top-of-book poller for the generic `PricePollService`.
pub struct HyperliquidPoller {
    pub trading: std::sync::Arc<HyperliquidTrading>,
    pub symbol: String,
}

#[async_trait::async_trait]
impl crate::services::price_source::PricePoll for HyperliquidPoller {
    fn label(&self) -> &'static str {
        "HYPERLIQUID_REST"
    }
    async fn fetch(&self) -> Result<Option<(f64, f64)>> {
        self.trading.get_l2_snapshot(&self.symbol).await
    }
}
