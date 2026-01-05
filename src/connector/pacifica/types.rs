//! Pacifica API types for serialization/deserialization.
//! Many fields are included for API completeness but may not be used directly.
#![allow(dead_code)]

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

/// Generic websocket response (kept for backwards compatibility)
#[derive(Debug, Deserialize)]
pub struct WebSocketResponse {
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Orderbook stream response (kept for backwards compatibility)
#[derive(Debug, Deserialize)]
pub struct OrderbookResponse {
    pub channel: String,
    pub data: OrderbookData,
}

/// Unified WebSocket message for single-pass parsing (hot path optimization)
#[derive(Debug, Deserialize)]
pub enum WsMessage {
    /// Orderbook update message
    Book {
        channel: String,
        data: OrderbookData,
    },
    /// Pong response (channel only, no data)
    Pong {
        channel: String,
    },
}

impl WsMessage {
    /// Fast parsing method that checks channel first to avoid trying multiple variants
    pub fn parse_fast(text: &str) -> Result<Self, serde_json::Error> {
        // Helper to check channel
        #[derive(Deserialize)]
        struct ChannelHelper<'a> {
            channel: &'a str,
        }

        // Helper for Book message
        #[derive(Deserialize)]
        struct BookMsg {
            channel: String,
            data: OrderbookData,
        }

        // Helper for Pong message
        #[derive(Deserialize)]
        struct PongMsg {
            channel: String,
        }

        // Fast check of channel
        match serde_json::from_str::<ChannelHelper>(text) {
            Ok(helper) => match helper.channel {
                "book" => {
                    let msg: BookMsg = serde_json::from_str(text)?;
                    Ok(WsMessage::Book {
                        channel: msg.channel,
                        data: msg.data,
                    })
                }
                "pong" => {
                    let msg: PongMsg = serde_json::from_str(text)?;
                    Ok(WsMessage::Pong {
                        channel: msg.channel,
                    })
                }
                _ => Err(serde::de::Error::custom("Unknown channel")),
            },
            Err(e) => Err(e),
        }
    }
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
        let best_bid = self.levels.get(0)
            .and_then(|bids| bids.first())
            .cloned();

        let best_ask = self.levels.get(1)
            .and_then(|asks| asks.first())
            .cloned();

        TopOfBook {
            symbol: self.symbol.clone(),
            best_bid,
            best_ask,
            timestamp: self.timestamp,
        }
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
    #[serde(rename = "ip")]
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
    #[serde(rename = "ct")]
    pub created_at: u64, // milliseconds
}

/// Account order updates response
#[derive(Debug, Deserialize)]
pub struct AccountOrderUpdatesResponse {
    pub channel: String,
    pub data: Vec<OrderUpdate>,
}

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
        side: String,              // "buy" or "sell" (derived from position delta)
        filled_amount: String,     // Absolute value of position delta
        avg_price: String,         // Entry price from position
        timestamp: u64,
        position_delta: String,    // Signed position change for diagnostics
        prev_position: String,     // Previous position size
        new_position: String,      // New position size
        cross_validated: bool,     // Whether fill was also detected by order updates
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
            },
            _ => None,
        }
    }
}

// ═══════════════════════════════════════════════════
// WebSocket Trading Operations
// ═══════════════════════════════════════════════════

/// WebSocket cancel all orders request
#[derive(Debug, Serialize)]
pub struct WsCancelAllOrdersRequest {
    /// Request ID (UUID)
    pub id: String,
    /// Request parameters
    pub params: WsCancelAllOrdersParams,
}

/// Parameters for cancel all orders
#[derive(Debug, Serialize)]
pub struct WsCancelAllOrdersParams {
    pub cancel_all_orders: WsCancelAllOrdersData,
}

/// Cancel all orders data payload
#[derive(Debug, Serialize)]
pub struct WsCancelAllOrdersData {
    /// User's wallet address
    pub account: String,
    /// Agent wallet address (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_wallet: Option<String>,
    /// Cryptographic signature
    pub signature: String,
    /// Current timestamp in milliseconds
    pub timestamp: i64,
    /// Signature expiry in milliseconds
    pub expiry_window: i64,
    /// Whether to cancel orders for all symbols
    pub all_symbols: bool,
    /// Whether to exclude reduce-only orders
    pub exclude_reduce_only: bool,
    /// Trading pair symbol (required if all_symbols is false)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
}

/// WebSocket cancel all orders response
#[derive(Debug, Deserialize)]
pub struct WsCancelAllOrdersResponse {
    /// Status code
    pub code: u16,
    /// Response data
    pub data: WsCancelAllOrdersResponseData,
    /// Request ID (matches request)
    pub id: String,
    /// Response timestamp
    pub t: i64,
    /// Response type
    #[serde(rename = "type")]
    pub response_type: String,
}

/// Cancel all orders response data
#[derive(Debug, Deserialize)]
pub struct WsCancelAllOrdersResponseData {
    /// Number of orders successfully cancelled
    pub cancelled_count: u32,
}

/// Generic WebSocket trading error response
#[derive(Debug, Deserialize)]
pub struct WsErrorResponse {
    pub code: u16,
    pub error: Option<String>,
    pub id: String,
    pub t: i64,
    #[serde(rename = "type")]
    pub response_type: String,
}

// ═══════════════════════════════════════════════════
// Account Positions WebSocket
// ═══════════════════════════════════════════════════

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
    pub amount: String,         // Position size (always positive)
    #[serde(rename = "p")]
    pub entry_price: String,    // Average entry price
    #[serde(rename = "t")]
    pub timestamp: u64,         // Timestamp in milliseconds
    #[serde(rename = "d")]
    pub side: String,           // "bid" (long) or "ask" (short)
    #[serde(rename = "m")]
    pub margin: String,         // Position margin
    #[serde(rename = "f")]
    pub funding: String,        // Funding fee
    #[serde(rename = "i")]
    pub isolated: bool,         // Is isolated position
}

/// Account positions response
#[derive(Debug, Deserialize)]
pub struct AccountPositionsResponse {
    pub channel: String,
    pub data: Vec<PositionData>,
}

/// Unified WebSocket message for fill detection (single-pass parsing optimization)
#[derive(Debug, Deserialize)]
pub enum FillDetectionWsMessage {
    /// Account order updates (fills, cancellations)
    OrderUpdates {
        channel: String,
        data: Vec<OrderUpdate>,
    },
    /// Account positions (for position-based fill detection)
    Positions {
        channel: String,
        data: Vec<PositionData>,
    },
    /// Pong response
    Pong {
        channel: String,
    },
}

impl FillDetectionWsMessage {
    /// Fast parsing method that checks channel first to avoid trying multiple variants
    pub fn parse_fast(text: &str) -> Result<Self, serde_json::Error> {
        // Helper to check channel
        #[derive(Deserialize)]
        struct ChannelHelper<'a> {
            channel: &'a str,
        }

        // Helper for OrderUpdates message
        #[derive(Deserialize)]
        struct OrderUpdatesMsg {
            channel: String,
            data: Vec<OrderUpdate>,
        }

        // Helper for Positions message
        #[derive(Deserialize)]
        struct PositionsMsg {
            channel: String,
            data: Vec<PositionData>,
        }

        // Helper for Pong message
        #[derive(Deserialize)]
        struct PongMsg {
            channel: String,
        }

        // Fast check of channel
        match serde_json::from_str::<ChannelHelper>(text) {
            Ok(helper) => match helper.channel {
                "account_order_updates" => {
                    let msg: OrderUpdatesMsg = serde_json::from_str(text)?;
                    Ok(FillDetectionWsMessage::OrderUpdates {
                        channel: msg.channel,
                        data: msg.data,
                    })
                }
                "account_positions" => {
                    let msg: PositionsMsg = serde_json::from_str(text)?;
                    Ok(FillDetectionWsMessage::Positions {
                        channel: msg.channel,
                        data: msg.data,
                    })
                }
                "pong" => {
                    let msg: PongMsg = serde_json::from_str(text)?;
                    Ok(FillDetectionWsMessage::Pong {
                        channel: msg.channel,
                    })
                }
                _ => Err(serde::de::Error::custom("Unknown channel")),
            },
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDERBOOK_JSON: &str = r#"{
        "channel": "book",
        "data": {
            "l": [
                [{"p": "100.50", "a": "10.5", "n": 3}],
                [{"p": "100.55", "a": "8.2", "n": 2}]
            ],
            "s": "SOL",
            "t": 1704067200000
        }
    }"#;

    const PONG_JSON: &str = r#"{"channel": "pong"}"#;

    const ORDER_UPDATE_FILLED_JSON: &str = r#"{
        "channel": "account_order_updates",
        "data": [{
            "i": 12345,
            "I": "test-order-123",
            "u": "account123",
            "s": "SOL",
            "d": "bid",
            "p": "100.50",
            "ip": "100.50",
            "a": "1.0",
            "f": "1.0",
            "oe": "fulfill_limit",
            "os": "filled",
            "ot": "limit",
            "r": false,
            "ut": 1704067200000,
            "ct": 1704067190000
        }]
    }"#;

    const ORDER_UPDATE_PARTIAL_JSON: &str = r#"{
        "channel": "account_order_updates",
        "data": [{
            "i": 12346,
            "I": "test-order-456",
            "u": "account123",
            "s": "SOL",
            "d": "ask",
            "p": "100.55",
            "ip": "100.55",
            "a": "2.0",
            "f": "0.5",
            "oe": "fulfill_limit",
            "os": "partially_filled",
            "ot": "limit",
            "r": false,
            "ut": 1704067200000,
            "ct": 1704067190000
        }]
    }"#;

    const ORDER_UPDATE_CANCELLED_JSON: &str = r#"{
        "channel": "account_order_updates",
        "data": [{
            "i": 12347,
            "I": "test-order-789",
            "u": "account123",
            "s": "SOL",
            "d": "bid",
            "p": "100.45",
            "ip": "100.45",
            "a": "1.0",
            "f": "0.0",
            "oe": "cancel",
            "os": "cancelled",
            "ot": "limit",
            "r": false,
            "ut": 1704067200000,
            "ct": 1704067190000
        }]
    }"#;

    #[test]
    fn test_orderbook_parsing() {
        let response: OrderbookResponse = serde_json::from_str(ORDERBOOK_JSON).unwrap();

        assert_eq!(response.channel, "book");
        assert_eq!(response.data.symbol, "SOL");
        assert_eq!(response.data.timestamp, 1704067200000);
        assert_eq!(response.data.levels.len(), 2);

        // Check bids (first level array)
        let bids = &response.data.levels[0];
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].price, "100.50");
        assert_eq!(bids[0].amount, "10.5");
        assert_eq!(bids[0].num_orders, 3);

        // Check asks (second level array)
        let asks = &response.data.levels[1];
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].price, "100.55");
        assert_eq!(asks[0].amount, "8.2");
        assert_eq!(asks[0].num_orders, 2);
    }

    #[test]
    fn test_top_of_book_extraction() {
        let response: OrderbookResponse = serde_json::from_str(ORDERBOOK_JSON).unwrap();
        let top = response.data.get_top_of_book();

        assert_eq!(top.symbol, "SOL");
        assert_eq!(top.timestamp, 1704067200000);

        let best_bid = top.best_bid.unwrap();
        assert_eq!(best_bid.price, "100.50");
        assert_eq!(best_bid.amount, "10.5");

        let best_ask = top.best_ask.unwrap();
        assert_eq!(best_ask.price, "100.55");
        assert_eq!(best_ask.amount, "8.2");
    }

    #[test]
    fn test_top_of_book_empty_levels() {
        let data = OrderbookData {
            levels: vec![vec![], vec![]],
            symbol: "SOL".to_string(),
            timestamp: 1704067200000,
        };
        let top = data.get_top_of_book();

        assert!(top.best_bid.is_none());
        assert!(top.best_ask.is_none());
    }

    #[test]
    fn test_ws_message_parse_fast_book() {
        let msg = WsMessage::parse_fast(ORDERBOOK_JSON).unwrap();

        match msg {
            WsMessage::Book { channel, data } => {
                assert_eq!(channel, "book");
                assert_eq!(data.symbol, "SOL");
                assert_eq!(data.timestamp, 1704067200000);
            }
            _ => panic!("Expected Book message"),
        }
    }

    #[test]
    fn test_ws_message_parse_fast_pong() {
        let msg = WsMessage::parse_fast(PONG_JSON).unwrap();

        match msg {
            WsMessage::Pong { channel } => {
                assert_eq!(channel, "pong");
            }
            _ => panic!("Expected Pong message"),
        }
    }

    #[test]
    fn test_ws_message_parse_fast_unknown_channel() {
        let unknown_json = r#"{"channel": "unknown_channel"}"#;
        let result = WsMessage::parse_fast(unknown_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_order_update_to_fill_event_full() {
        let response: AccountOrderUpdatesResponse =
            serde_json::from_str(ORDER_UPDATE_FILLED_JSON).unwrap();

        assert_eq!(response.data.len(), 1);
        let order_update = &response.data[0];

        assert_eq!(order_update.order_id, 12345);
        assert_eq!(order_update.client_order_id, Some("test-order-123".to_string()));
        assert_eq!(order_update.symbol, "SOL");
        assert_eq!(order_update.side, "bid");
        assert_eq!(order_update.order_status, OrderStatus::Filled);

        let fill_event = order_update.to_fill_event().unwrap();
        match fill_event {
            FillEvent::FullFill {
                order_id,
                client_order_id,
                symbol,
                side,
                filled_amount,
                avg_price,
                ..
            } => {
                assert_eq!(order_id, 12345);
                assert_eq!(client_order_id, Some("test-order-123".to_string()));
                assert_eq!(symbol, "SOL");
                assert_eq!(side, "bid");
                assert_eq!(filled_amount, "1.0");
                assert_eq!(avg_price, "100.50");
            }
            _ => panic!("Expected FullFill event"),
        }
    }

    #[test]
    fn test_order_update_to_fill_event_partial() {
        let response: AccountOrderUpdatesResponse =
            serde_json::from_str(ORDER_UPDATE_PARTIAL_JSON).unwrap();

        let order_update = &response.data[0];
        assert_eq!(order_update.order_status, OrderStatus::PartiallyFilled);

        let fill_event = order_update.to_fill_event().unwrap();
        match fill_event {
            FillEvent::PartialFill {
                order_id,
                filled_amount,
                original_amount,
                ..
            } => {
                assert_eq!(order_id, 12346);
                assert_eq!(filled_amount, "0.5");
                assert_eq!(original_amount, "2.0");
            }
            _ => panic!("Expected PartialFill event"),
        }
    }

    #[test]
    fn test_order_update_to_fill_event_cancelled() {
        let response: AccountOrderUpdatesResponse =
            serde_json::from_str(ORDER_UPDATE_CANCELLED_JSON).unwrap();

        let order_update = &response.data[0];
        assert_eq!(order_update.order_status, OrderStatus::Cancelled);
        assert_eq!(order_update.order_event, OrderEvent::Cancel);

        let fill_event = order_update.to_fill_event().unwrap();
        match fill_event {
            FillEvent::Cancelled { reason, .. } => {
                assert_eq!(reason, "user_cancelled");
            }
            _ => panic!("Expected Cancelled event"),
        }
    }

    #[test]
    fn test_order_update_cancelled_reasons() {
        // Test various cancel reasons
        let reasons = [
            (OrderEvent::Cancel, "user_cancelled"),
            (OrderEvent::ForceCancel, "force_cancelled"),
            (OrderEvent::Expired, "expired"),
            (OrderEvent::PostOnlyRejected, "post_only_rejected"),
            (OrderEvent::SelfTradePrevented, "self_trade_prevented"),
        ];

        for (event, expected_reason) in reasons {
            let order_update = OrderUpdate {
                order_id: 1,
                client_order_id: None,
                account: "acc".to_string(),
                symbol: "SOL".to_string(),
                side: "bid".to_string(),
                avg_filled_price: "0".to_string(),
                initial_price: "100".to_string(),
                original_amount: "1".to_string(),
                filled_amount: "0".to_string(),
                order_event: event,
                order_status: OrderStatus::Cancelled,
                order_type: "limit".to_string(),
                stop_price: None,
                stop_parent_order_id: None,
                reduce_only: false,
                updated_at: 0,
                created_at: 0,
            };

            if let Some(FillEvent::Cancelled { reason, .. }) = order_update.to_fill_event() {
                assert_eq!(reason, expected_reason);
            } else {
                panic!("Expected Cancelled event");
            }
        }
    }

    #[test]
    fn test_fill_detection_ws_message_parse_order_updates() {
        let msg = FillDetectionWsMessage::parse_fast(ORDER_UPDATE_FILLED_JSON).unwrap();

        match msg {
            FillDetectionWsMessage::OrderUpdates { channel, data } => {
                assert_eq!(channel, "account_order_updates");
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].order_id, 12345);
            }
            _ => panic!("Expected OrderUpdates message"),
        }
    }

    #[test]
    fn test_fill_detection_ws_message_parse_pong() {
        let msg = FillDetectionWsMessage::parse_fast(PONG_JSON).unwrap();

        match msg {
            FillDetectionWsMessage::Pong { channel } => {
                assert_eq!(channel, "pong");
            }
            _ => panic!("Expected Pong message"),
        }
    }

    #[test]
    fn test_subscribe_message_creation() {
        let msg = SubscribeMessage::new("SOL".to_string(), 1);

        assert_eq!(msg.method, "subscribe");
        assert_eq!(msg.params.source, "book");
        assert_eq!(msg.params.symbol, "SOL");
        assert_eq!(msg.params.agg_level, 1);
    }

    #[test]
    fn test_unsubscribe_message_creation() {
        let msg = UnsubscribeMessage::new("SOL".to_string(), 1);

        assert_eq!(msg.method, "unsubscribe");
        assert_eq!(msg.params.source, "book");
        assert_eq!(msg.params.symbol, "SOL");
    }

    #[test]
    fn test_ping_message_creation() {
        let msg = PingMessage::new();
        assert_eq!(msg.method, "ping");

        let default_msg = PingMessage::default();
        assert_eq!(default_msg.method, "ping");
    }

    #[test]
    fn test_account_order_updates_subscribe() {
        let msg = AccountOrderUpdatesSubscribe::new("account123".to_string());

        assert_eq!(msg.method, "subscribe");
        assert_eq!(msg.params.source, "account_order_updates");
        assert_eq!(msg.params.account, "account123");
    }

    #[test]
    fn test_account_positions_subscribe() {
        let msg = AccountPositionsSubscribe::new("account123".to_string());

        assert_eq!(msg.method, "subscribe");
        assert_eq!(msg.params.source, "account_positions");
        assert_eq!(msg.params.account, "account123");
    }

    #[test]
    fn test_order_event_enum_values() {
        // Verify all order events deserialize correctly
        let events = [
            ("make", OrderEvent::Make),
            ("fulfill_market", OrderEvent::FulfillMarket),
            ("fulfill_limit", OrderEvent::FulfillLimit),
            ("cancel", OrderEvent::Cancel),
            ("force_cancel", OrderEvent::ForceCancel),
            ("expired", OrderEvent::Expired),
            ("post_only_rejected", OrderEvent::PostOnlyRejected),
            ("self_trade_prevented", OrderEvent::SelfTradePrevented),
        ];

        for (json_val, expected) in events {
            let json = format!("\"{}\"", json_val);
            let parsed: OrderEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, expected);
        }
    }

    #[test]
    fn test_order_status_enum_values() {
        let statuses = [
            ("open", OrderStatus::Open),
            ("partially_filled", OrderStatus::PartiallyFilled),
            ("filled", OrderStatus::Filled),
            ("cancelled", OrderStatus::Cancelled),
            ("rejected", OrderStatus::Rejected),
        ];

        for (json_val, expected) in statuses {
            let json = format!("\"{}\"", json_val);
            let parsed: OrderStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
