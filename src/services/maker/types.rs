//! Normalized boundary types for the maker-venue abstraction.
//!
//! These are exchange-agnostic: all prices/sizes are `f64` and all sides are
//! the strategy-level [`OrderSide`], never venue strings like `"bid"`/`"ask"`.
//! A connector's adapter (e.g. `PacificaMaker`) converts its wire types into
//! these so services never see venue-specific shapes.

use crate::strategy::OrderSide;

/// Tick / lot rules for one maker symbol.
///
/// Kept as `String` (not `f64`) so the venue's exact decimal precision is
/// preserved when a tick-aligned price is serialized to a string at signing
/// time — round-tripping through `f64` could perturb the least-significant
/// digit and get the order rejected.
#[derive(Debug, Clone)]
pub struct MakerSymbolRules {
    pub tick_size: String,
    pub lot_size: String,
}

/// Normalized acknowledgement returned by a maker order placement.
#[derive(Debug, Clone, Default)]
pub struct MakerOrderAck {
    /// Venue order id. Pacifica returns it under either `order_id` or `i`; the
    /// adapter folds them so callers see one field.
    pub order_id: Option<u64>,
    /// The **server-echoed** client order id, used for the same-order check
    /// after placement. This is intentionally the server's echo, not the
    /// locally generated UUID (which the caller already holds and displays).
    pub client_order_id: Option<String>,
    pub submitted_price: Option<f64>,
    pub submitted_size: Option<f64>,
}

/// Normalized open order from the maker venue.
#[derive(Debug, Clone)]
pub struct MakerOpenOrder {
    pub order_id: u64,
    pub client_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub price: f64,
    pub initial_amount: f64,
    pub filled_amount: f64,
    pub cancelled_amount: f64,
    pub reduce_only: bool,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Normalized signed position for one symbol: `+` long, `-` short, `0` flat.
#[derive(Debug, Clone, Copy, Default)]
pub struct MakerPosition {
    pub signed_base: f64,
    pub entry_price: f64,
}

/// Normalized trade-history row.
///
/// Carries only the fields the cumulative-from-history and audit paths read.
/// `side` is intentionally omitted: the venue's `open_long`/`close_short` style
/// encoding is not consumed by any history loop, and forcing a mapping would
/// risk silently dropping rows on an unrecognized value.
#[derive(Debug, Clone)]
pub struct MakerTrade {
    pub order_id: u64,
    pub client_order_id: Option<String>,
    pub amount: f64,
    pub entry_price: f64,
    pub fee: f64,
    /// True when this row is the **maker** leg of a fill (Pacifica
    /// `event_type == "fulfill_maker"`); taker fills are excluded by callers.
    pub is_maker_fill: bool,
    pub created_at: u64,
}

/// Weighted fill summary for one client order id (mirrors the connector-level
/// `TradeFetchResult`): encapsulates any venue-specific maker/taker filtering
/// and multi-fill weighting so callers get a single rolled-up result.
#[derive(Debug, Clone, Default)]
pub struct MakerFillSummary {
    pub fill_price: Option<f64>,
    pub actual_fee: Option<f64>,
    pub total_size: Option<f64>,
    pub total_notional: Option<f64>,
}

/// Normalized fill / order-lifecycle event emitted by a [`super::MakerFillStream`].
///
/// Sides are [`OrderSide`]; amounts and prices are `f64`. This is the
/// venue-agnostic equivalent of the connector's `FillEvent`.
#[derive(Debug, Clone)]
pub enum MakerFillEvent {
    Partial {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: OrderSide,
        filled: f64,
        original: f64,
        avg_price: f64,
        ts: u64,
    },
    Full {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: OrderSide,
        filled: f64,
        avg_price: f64,
        ts: u64,
    },
    Cancelled {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: OrderSide,
        filled: f64,
        original: f64,
        reason: String,
        ts: u64,
    },
    /// Fill inferred from a position delta (redundancy layer).
    Position {
        symbol: String,
        side: OrderSide,
        filled: f64,
        avg_price: f64,
        ts: u64,
        position_delta: f64,
        prev_position: f64,
        new_position: f64,
        cross_validated: bool,
    },
}
