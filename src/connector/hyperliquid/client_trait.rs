//! Hyperliquid trading client trait for dependency injection and testing.
//!
//! This trait abstracts the Hyperliquid trading API, enabling:
//! - Unit testing with mock implementations
//! - Swappable implementations (live vs mock)

use anyhow::Result;
use async_trait::async_trait;

use super::types::{AssetMeta, OrderRequest, OrderResponse, UserFill, UserState};

/// Trait for Hyperliquid trading operations.
///
/// This trait is implemented by `HyperliquidTrading` for live trading
/// and by mock implementations for unit testing.
#[async_trait]
pub trait HyperliquidTradingClient: Send + Sync {
    /// Get L2 orderbook snapshot via info endpoint.
    ///
    /// Returns the best bid and ask prices, or None if unavailable.
    async fn get_l2_snapshot(&self, coin: &str) -> Result<Option<(f64, f64)>>;

    /// Place a market order (IOC limit order with slippage).
    ///
    /// # Arguments
    /// * `coin` - Coin symbol (e.g., "SOL", "BTC")
    /// * `is_buy` - True for buy, false for sell
    /// * `size` - Order size
    /// * `slippage` - Slippage tolerance (e.g., 0.05 = 5%)
    /// * `reduce_only` - Whether this is a reduce-only order
    /// * `bid` - Current bid price
    /// * `ask` - Current ask price
    async fn place_market_order(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
    ) -> Result<OrderResponse>;

    /// Build a signed market order request for WebSocket execution.
    ///
    /// This constructs and signs the order payload that can be sent
    /// via WebSocket `post` (type: "action").
    async fn build_market_order_request(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
    ) -> Result<OrderRequest>;

    /// Build a signed market order request with pre-cached asset metadata.
    ///
    /// This is the low-latency version that skips metadata lookups.
    /// Use this when you have pre-fetched asset_id and sz_decimals.
    async fn build_market_order_request_with_meta(
        &self,
        coin: &str,
        is_buy: bool,
        size: f64,
        slippage: f64,
        reduce_only: bool,
        bid: Option<f64>,
        ask: Option<f64>,
        asset_id: u32,
        sz_decimals: i32,
    ) -> Result<OrderRequest>;

    /// Get user fills (trade history).
    ///
    /// # Arguments
    /// * `user` - User wallet address in 42-character hexadecimal format
    /// * `aggregate_by_time` - When true, partial fills are combined
    ///
    /// # Returns
    /// Vector of user fills (up to 2000 most recent fills)
    async fn get_user_fills(&self, user: &str, aggregate_by_time: bool) -> Result<Vec<UserFill>>;

    /// Get user state (positions and margin summary).
    ///
    /// # Arguments
    /// * `user` - User wallet address in 42-character hexadecimal format
    async fn get_user_state(&self, user: &str) -> Result<UserState>;

    /// Get asset metadata (szDecimals, maxLeverage, etc.).
    async fn get_asset_info(&self, coin: &str) -> Result<AssetMeta>;

    /// Get asset ID from coin name.
    async fn get_asset_id(&self, coin: &str) -> Result<u32>;

    /// Returns true when this client is configured for testnet.
    fn is_testnet(&self) -> bool;

    /// Get wallet address from the internal wallet.
    fn get_wallet_address(&self) -> String;
}
