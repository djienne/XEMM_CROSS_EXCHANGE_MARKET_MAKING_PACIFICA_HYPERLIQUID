//! Pacifica trading client trait for dependency injection and testing.
//!
//! This trait abstracts the Pacifica trading API, enabling:
//! - Unit testing with mock implementations
//! - Swappable implementations (live vs mock)

use async_trait::async_trait;
use anyhow::Result;
use crate::connector::pacifica::trading::{OrderData, OpenOrderItem, PositionItem};
use crate::strategy::OrderSide;

/// Trait for Pacifica trading operations.
///
/// This trait is implemented by `PacificaTrading` for live trading
/// and by mock implementations for unit testing.
#[async_trait]
pub trait PacificaTradingClient: Send + Sync {
    /// Place a limit order on Pacifica.
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "SOL", "BTC")
    /// * `side` - Order side (Buy or Sell)
    /// * `size` - Order size in token units
    /// * `price` - Explicit limit price, or None to calculate from mid price
    /// * `mid_price_offset_pct` - If price is None, offset from mid price (%)
    /// * `current_bid` - Current best bid (required if price is None)
    /// * `current_ask` - Current best ask (required if price is None)
    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: Option<f64>,
        mid_price_offset_pct: f64,
        current_bid: Option<f64>,
        current_ask: Option<f64>,
    ) -> Result<OrderData>;

    /// Cancel all orders for a specific symbol or all symbols.
    ///
    /// # Arguments
    /// * `all_symbols` - If true, cancels orders for all symbols
    /// * `symbol` - Trading symbol (required if all_symbols is false)
    /// * `exclude_reduce_only` - If true, excludes reduce-only orders
    ///
    /// # Returns
    /// Number of orders cancelled
    async fn cancel_all_orders(
        &self,
        all_symbols: bool,
        symbol: Option<&str>,
        exclude_reduce_only: bool,
    ) -> Result<u32>;

    /// Get open orders for the account.
    ///
    /// # Returns
    /// Vector of open orders
    async fn get_open_orders(&self) -> Result<Vec<OpenOrderItem>>;

    /// Get current positions for the account.
    ///
    /// # Returns
    /// Vector of position items
    async fn get_positions(&self) -> Result<Vec<PositionItem>>;

    /// Cancel a single order by client order ID.
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol
    /// * `client_order_id` - Client order ID to cancel
    async fn cancel_order(&self, symbol: &str, client_order_id: &str) -> Result<()>;
}
