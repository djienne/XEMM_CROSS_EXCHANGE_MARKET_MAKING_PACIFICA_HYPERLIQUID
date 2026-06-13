//! Maker-venue abstraction.
//!
//! The bot quotes passive maker orders on one venue (today Pacifica) and hedges
//! on Hyperliquid (the permanent taker, intentionally NOT behind this trait).
//! These traits decouple the ~control plane~ — order placement, cancel, account
//! and market queries, and the fill stream — from any one venue so a future
//! maker can be dropped in by implementing them in a new connector module.
//!
//! Dispatched as `Arc<dyn MakerExchange>` / `Box<dyn MakerFillStream>`. The only
//! hot-path call is [`MakerExchange::place_limit_order`], which is a network
//! round-trip, so dynamic dispatch is immeasurable against the I/O. The
//! latency-critical book → quote → opportunity path never touches these traits.

pub mod types;

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::strategy::OrderSide;

pub use types::{
    MakerFillEvent, MakerFillSummary, MakerOpenOrder, MakerOrderAck, MakerPosition,
    MakerSymbolRules, MakerTrade,
};

/// Async hook invoked on every successful (re)connect of the fill stream so the
/// caller can run REST reconciliation (e.g. scan open orders for fills missed
/// during the outage). Structurally identical to the Pacifica connector's
/// `ReconcileHook` so an adapter can pass it straight through.
pub type MakerReconcileHook =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync>;

/// Callback the fill stream pushes normalized events through.
pub type MakerFillCallback = Box<dyn FnMut(MakerFillEvent) + Send + 'static>;

/// Updates the fill stream's internal position baseline when an order-based
/// detector observes a fill, so the position-redundancy layer does not emit a
/// duplicate hedge for the same fill.
pub trait MakerBaselineUpdater: Send + Sync {
    fn update_baseline(&self, symbol: &str, side: OrderSide, filled: f64, avg_price: f64);
}

/// The maker venue control plane: order placement, cancellation, and account /
/// market queries. Implemented once per maker venue in its connector module.
#[async_trait]
pub trait MakerExchange: Send + Sync + 'static {
    /// Short label for logs (e.g. `"PACIFICA"`).
    fn label(&self) -> &'static str;

    /// Tick / lot rules for `symbol` (the venue's market-info endpoint).
    async fn symbol_rules(&self, symbol: &str) -> Result<MakerSymbolRules>;

    /// Place a post-only limit order. The implementation owns transport choice
    /// (e.g. WS-preferred with REST fallback) and venue side encoding. `rules`
    /// is the pre-fetched tick/lot for `symbol`; `current_bid`/`current_ask`
    /// are passed through for venues that need a marketability reference.
    #[allow(clippy::too_many_arguments)]
    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        size: f64,
        price: f64,
        rules: &MakerSymbolRules,
        client_order_id: String,
        current_bid: f64,
        current_ask: f64,
    ) -> Result<MakerOrderAck>;

    /// Cancel all orders for `symbol`. Returns per-transport counts
    /// `(primary, secondary)` (e.g. `(rest, ws)`) to preserve split logging;
    /// single-transport venues return the count in `.0` and `0` in `.1`.
    async fn cancel_all(&self, symbol: &str) -> Result<(u32, u32)>;

    /// All currently open orders for the account.
    async fn open_orders(&self) -> Result<Vec<MakerOpenOrder>>;

    /// Signed position for `symbol` (+long / -short); zeroed when flat.
    async fn position(&self, symbol: &str) -> Result<MakerPosition>;

    /// Like [`MakerExchange::position`] but distinguishes "no position row"
    /// (`None`) from a flat/zero position. The position monitor needs that to
    /// keep a `None` baseline distinct from a real zero.
    async fn position_opt(&self, symbol: &str) -> Result<Option<MakerPosition>>;

    /// Up to `limit` recent trade-history rows for `symbol`.
    async fn recent_trades(&self, symbol: &str, limit: u32) -> Result<Vec<MakerTrade>>;

    /// Weighted fill summary for one client order id, with internal retry.
    async fn maker_fill_summary(
        &self,
        symbol: &str,
        client_order_id: &str,
        max_attempts: u32,
    ) -> MakerFillSummary;
}

/// Long-lived fill / order-lifecycle stream for the maker venue.
///
/// Mirrors [`crate::services::price_source::PriceStream`]: `run_with` loops
/// internally with its own reconnect backoff and returns only when the stream
/// is permanently closed.
#[async_trait]
pub trait MakerFillStream: Send + 'static {
    fn label(&self) -> &'static str;
    /// The readiness flag the stream flips on (re)connect. MUST be the same
    /// `Arc` the underlying client mutates, so startup gating observes it.
    fn ready_flag(&self) -> Arc<AtomicBool>;
    fn set_reconcile_hook(&self, hook: MakerReconcileHook);
    fn baseline_updater(&self) -> Arc<dyn MakerBaselineUpdater>;
    async fn run_with(&mut self, cb: MakerFillCallback) -> Result<()>;
}
