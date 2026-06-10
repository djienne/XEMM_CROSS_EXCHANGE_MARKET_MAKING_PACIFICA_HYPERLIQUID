use serde::{Deserialize, Serialize};

/// Websocket subscription message
#[derive(Debug, Serialize)]
pub struct SubscribeMessage {
    pub method: String,
    pub params: SubscribeParams,
}

/// Subscription parameters for orderbook
#[derive(Debug, Serialize)]
pub struct SubscribeParams {
    pub source: String,
    pub symbol: String,
    pub agg_level: u32,
}

/// Unsubscription message
#[derive(Debug, Serialize)]
pub struct UnsubscribeMessage {
    pub method: String,
    pub params: SubscribeParams,
}

/// Ping message for keepalive
#[derive(Debug, Serialize)]
pub struct PingMessage {
    pub method: String,
}

/// Generic websocket response
#[derive(Debug, Deserialize)]
pub struct WebSocketResponse {
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Orderbook stream response
#[derive(Debug, Deserialize)]
pub struct OrderbookResponse {
    pub channel: String,
    pub data: OrderbookData,
}

/// Orderbook data structure
#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookData {
    #[serde(rename = "l")]
    pub levels: Vec<Vec<BookLevel>>, // [bids, asks]
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub timestamp: u64,
}

/// Book level with aggregated orders
#[derive(Debug, Clone, Deserialize)]
pub struct BookLevel {
    #[serde(rename = "a")]
    pub amount: String, // Total amount in aggregation level
    #[serde(rename = "n")]
    pub num_orders: u32, // Number of orders in aggregation level
    #[serde(rename = "p")]
    pub price: String, // Price (highest for bids, lowest for asks)
}

/// Top of book data (best bid and ask)
#[derive(Debug, Clone)]
pub struct TopOfBook {
    pub symbol: String,
    pub best_bid: Option<BookLevel>,
    pub best_ask: Option<BookLevel>,
    pub timestamp: u64,
}

impl OrderbookData {
    /// Extract the top of book (best bid and ask)
    pub fn get_top_of_book(&self) -> TopOfBook {
        let best_bid = self.levels.get(0).and_then(|bids| bids.first()).cloned();

        let best_ask = self.levels.get(1).and_then(|asks| asks.first()).cloned();

        TopOfBook {
            symbol: self.symbol.clone(),
            best_bid,
            best_ask,
            timestamp: self.timestamp,
        }
    }
}

// ============================================================================
// Hot-path top-of-book frame (single-pass, minimal allocation)
// ============================================================================

/// Hot-path book frame: parses ONLY the best level per side, straight to f64,
/// skipping deeper levels without allocating them. This is the
/// highest-frequency message in the system; the full `OrderbookResponse`
/// (every level as `String`s) remains for depth-aware tooling.
#[derive(Debug, Deserialize)]
pub struct BookTopFrame {
    pub channel: String,
    pub data: BookTopData,
}

#[derive(Debug, Deserialize)]
pub struct BookTopData {
    #[serde(rename = "l")]
    pub levels: TopOfBookLevels,
    #[serde(rename = "t")]
    pub timestamp: u64,
}

/// `[bids, asks]` where only each side's first (best) level price is kept.
#[derive(Debug)]
pub struct TopOfBookLevels {
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
}

impl<'de> Deserialize<'de> for TopOfBookLevels {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LevelsVisitor;
        impl<'de> serde::de::Visitor<'de> for LevelsVisitor {
            type Value = TopOfBookLevels;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("[[bid levels], [ask levels]]")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let best_bid = seq.next_element::<SideTopPrice>()?.and_then(|side| side.0);
                let best_ask = seq.next_element::<SideTopPrice>()?.and_then(|side| side.0);
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(TopOfBookLevels { best_bid, best_ask })
            }
        }
        deserializer.deserialize_seq(LevelsVisitor)
    }
}

/// One side's level array; keeps only the first level's price and skips the
/// remaining levels without allocating them.
struct SideTopPrice(Option<f64>);

impl<'de> Deserialize<'de> for SideTopPrice {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SideVisitor;
        impl<'de> serde::de::Visitor<'de> for SideVisitor {
            type Value = SideTopPrice;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an array of book levels")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let first = seq.next_element::<TopLevelPrice>()?;
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(SideTopPrice(first.map(|level| level.p)))
            }
        }
        deserializer.deserialize_seq(SideVisitor)
    }
}

/// A single book level, extracting only `p` (price) as f64. Other fields
/// (`a`, `n`, ...) are skipped by serde's default unknown-field handling.
#[derive(Deserialize)]
struct TopLevelPrice {
    #[serde(rename = "p", deserialize_with = "f64_from_str_or_number")]
    p: f64,
}

/// Accepts a price encoded as a JSON string (the exchange wire format) or a
/// bare number, parsing straight to f64 without an intermediate String.
pub(crate) fn f64_from_str_or_number<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct PriceVisitor;
    impl serde::de::Visitor<'_> for PriceVisitor {
        type Value = f64;

        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a price as string or number")
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<f64, E> {
            fast_float::parse(v).map_err(|_| E::custom("invalid price string"))
        }

        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<f64, E> {
            Ok(v)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<f64, E> {
            Ok(v as f64)
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<f64, E> {
            Ok(v as f64)
        }
    }
    deserializer.deserialize_any(PriceVisitor)
}

#[cfg(test)]
mod book_top_frame_tests {
    use super::*;

    #[test]
    fn book_top_frame_extracts_best_levels_only() {
        let frame: BookTopFrame = serde_json::from_str(
            r#"{"channel":"book","data":{"l":[[{"a":"10","n":3,"p":"100.5"},{"a":"5","n":1,"p":"100.4"}],[{"a":"7","n":2,"p":"100.6"},{"a":"9","n":4,"p":"100.7"}]],"s":"SOL","t":1234}}"#,
        )
        .unwrap();
        assert_eq!(frame.channel, "book");
        assert_eq!(frame.data.timestamp, 1234);
        assert_eq!(frame.data.levels.best_bid, Some(100.5));
        assert_eq!(frame.data.levels.best_ask, Some(100.6));
    }

    #[test]
    fn book_top_frame_handles_empty_sides() {
        let frame: BookTopFrame = serde_json::from_str(
            r#"{"channel":"book","data":{"l":[[],[{"a":"7","n":2,"p":"100.6"}]],"s":"SOL","t":1}}"#,
        )
        .unwrap();
        assert_eq!(frame.data.levels.best_bid, None);
        assert_eq!(frame.data.levels.best_ask, Some(100.6));
    }

    #[test]
    fn non_book_frames_fail_book_parse_but_envelope_succeeds() {
        let pong = r#"{"channel":"pong"}"#;
        assert!(serde_json::from_str::<BookTopFrame>(pong).is_err());
        let envelope: WebSocketResponse = serde_json::from_str(pong).unwrap();
        assert_eq!(envelope.channel.as_deref(), Some("pong"));
    }
}

impl SubscribeMessage {
    pub fn new(symbol: String, agg_level: u32) -> Self {
        Self {
            method: "subscribe".to_string(),
            params: SubscribeParams {
                source: "book".to_string(),
                symbol,
                agg_level,
            },
        }
    }
}

impl UnsubscribeMessage {
    pub fn new(symbol: String, agg_level: u32) -> Self {
        Self {
            method: "unsubscribe".to_string(),
            params: SubscribeParams {
                source: "book".to_string(),
                symbol,
                agg_level,
            },
        }
    }
}

impl PingMessage {
    pub fn new() -> Self {
        Self {
            method: "ping".to_string(),
        }
    }
}

impl Default for PingMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// Account order updates subscription parameters
#[derive(Debug, Serialize)]
pub struct AccountOrderUpdatesParams {
    pub source: String,
    pub account: String,
}

/// Account order updates subscription message
#[derive(Debug, Serialize)]
pub struct AccountOrderUpdatesSubscribe {
    pub method: String,
    pub params: AccountOrderUpdatesParams,
}

impl AccountOrderUpdatesSubscribe {
    pub fn new(account: String) -> Self {
        Self {
            method: "subscribe".to_string(),
            params: AccountOrderUpdatesParams {
                source: "account_order_updates".to_string(),
                account,
            },
        }
    }
}

/// Order event type
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderEvent {
    Make,
    StopCreated,
    FulfillMarket,
    FulfillLimit,
    Adjust,
    StopParentOrderFilled,
    StopTriggered,
    StopUpgrade,
    Cancel,
    ForceCancel,
    Expired,
    PostOnlyRejected,
    SelfTradePrevented,
    /// Any event the server adds in the future: parse-tolerant so a new value
    /// never fails the whole frame (handled as "unknown" by `to_fill_event`).
    #[serde(other)]
    Unknown,
}

/// Order status
#[derive(Debug, Clone, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
    /// Any status the server adds in the future: parse-tolerant. `to_fill_event`
    /// maps it to `None` (no fill emitted), so it can never be mis-hedged.
    #[serde(other)]
    Unknown,
}

/// Order update data
#[derive(Debug, Clone, Deserialize)]
pub struct OrderUpdate {
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "I")]
    pub client_order_id: Option<String>,
    #[serde(rename = "u")]
    pub account: String,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "d")]
    pub side: String, // "bid" or "ask"
    #[serde(rename = "p")]
    pub avg_filled_price: String,
    // Non-fill-critical: tolerate absence so a fill in the same frame is not lost.
    #[serde(rename = "ip", default)]
    pub initial_price: String,
    #[serde(rename = "a")]
    pub original_amount: String,
    #[serde(rename = "f")]
    pub filled_amount: String,
    #[serde(rename = "oe")]
    pub order_event: OrderEvent,
    #[serde(rename = "os")]
    pub order_status: OrderStatus,
    #[serde(rename = "ot")]
    pub order_type: String, // "limit" or "market"
    #[serde(rename = "sp")]
    pub stop_price: Option<String>,
    #[serde(rename = "si")]
    pub stop_parent_order_id: Option<String>,
    #[serde(rename = "r")]
    pub reduce_only: bool,
    #[serde(rename = "ut")]
    pub updated_at: u64, // milliseconds
    // Non-fill-critical: tolerate absence so a fill in the same frame is not lost.
    #[serde(rename = "ct", default)]
    pub created_at: u64, // milliseconds
}

// NOTE: `account_order_updates` frames are now decoded per-item from the generic
// envelope (see fill_detection::handle_message) so one malformed sibling cannot
// drop a whole batch; the previous strict `AccountOrderUpdatesResponse` wrapper
// is therefore no longer used.

/// Fill information extracted from order update
#[derive(Debug, Clone)]
pub enum FillEvent {
    PartialFill {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: String,
        filled_amount: String,
        original_amount: String,
        avg_price: String,
        timestamp: u64,
    },
    FullFill {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: String,
        filled_amount: String,
        avg_price: String,
        timestamp: u64,
    },
    Cancelled {
        order_id: u64,
        client_order_id: Option<String>,
        symbol: String,
        side: String,
        filled_amount: String,
        original_amount: String,
        reason: String,
        timestamp: u64,
    },
    /// Fill detected from position change (redundancy layer)
    PositionFill {
        symbol: String,
        side: String,          // "buy" or "sell" (derived from position delta)
        filled_amount: String, // Absolute value of position delta
        avg_price: String,     // Entry price from position
        timestamp: u64,
        position_delta: String, // Signed position change for diagnostics
        prev_position: String,  // Previous position size
        new_position: String,   // New position size
        cross_validated: bool,  // Whether fill was also detected by order updates
    },
}

impl OrderUpdate {
    /// Convert order update to fill event if applicable
    pub fn to_fill_event(&self) -> Option<FillEvent> {
        match self.order_status {
            OrderStatus::PartiallyFilled => Some(FillEvent::PartialFill {
                order_id: self.order_id,
                client_order_id: self.client_order_id.clone(),
                symbol: self.symbol.clone(),
                side: self.side.clone(),
                filled_amount: self.filled_amount.clone(),
                original_amount: self.original_amount.clone(),
                avg_price: self.avg_filled_price.clone(),
                timestamp: self.updated_at,
            }),
            OrderStatus::Filled => Some(FillEvent::FullFill {
                order_id: self.order_id,
                client_order_id: self.client_order_id.clone(),
                symbol: self.symbol.clone(),
                side: self.side.clone(),
                filled_amount: self.filled_amount.clone(),
                avg_price: self.avg_filled_price.clone(),
                timestamp: self.updated_at,
            }),
            OrderStatus::Cancelled => {
                let reason = match self.order_event {
                    OrderEvent::Cancel => "user_cancelled",
                    OrderEvent::ForceCancel => "force_cancelled",
                    OrderEvent::Expired => "expired",
                    OrderEvent::PostOnlyRejected => "post_only_rejected",
                    OrderEvent::SelfTradePrevented => "self_trade_prevented",
                    _ => "unknown",
                };
                Some(FillEvent::Cancelled {
                    order_id: self.order_id,
                    client_order_id: self.client_order_id.clone(),
                    symbol: self.symbol.clone(),
                    side: self.side.clone(),
                    filled_amount: self.filled_amount.clone(),
                    original_amount: self.original_amount.clone(),
                    reason: reason.to_string(),
                    timestamp: self.updated_at,
                })
            }
            _ => None,
        }
    }
}

// ===================================================
// WebSocket Trading Operations
// ===================================================

// NOTE: the old strongly-typed WS trading request/response structs were
// removed when `ws_trading.rs` was refactored to build payloads inline
// against a persistent connection.

// ===================================================
// Account Positions WebSocket
// ===================================================

/// Account positions subscription parameters
#[derive(Debug, Serialize)]
pub struct AccountPositionsParams {
    pub source: String,
    pub account: String,
}

/// Account positions subscription message
#[derive(Debug, Serialize)]
pub struct AccountPositionsSubscribe {
    pub method: String,
    pub params: AccountPositionsParams,
}

impl AccountPositionsSubscribe {
    pub fn new(account: String) -> Self {
        Self {
            method: "subscribe".to_string(),
            params: AccountPositionsParams {
                source: "account_positions".to_string(),
                account,
            },
        }
    }
}

/// Position data from WebSocket stream
#[derive(Debug, Clone, Deserialize)]
pub struct PositionData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub amount: String, // Position size (always positive)
    #[serde(rename = "p")]
    pub entry_price: String, // Average entry price
    #[serde(rename = "t")]
    pub timestamp: u64, // Timestamp in milliseconds
    #[serde(rename = "d")]
    pub side: String, // "bid" (long) or "ask" (short)
    #[serde(rename = "m")]
    pub margin: String, // Position margin
    #[serde(rename = "f")]
    pub funding: String, // Funding fee
    #[serde(rename = "i")]
    pub isolated: bool, // Is isolated position
}

/// Account positions response
#[derive(Debug, Deserialize)]
pub struct AccountPositionsResponse {
    pub channel: String,
    pub data: Vec<PositionData>,
}
