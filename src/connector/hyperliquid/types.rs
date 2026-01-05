//! Hyperliquid API types for serialization/deserialization.
//! Field names match API exactly (non-snake_case).
#![allow(dead_code, non_snake_case)]

use serde::{Deserialize, Serialize};

/// WebSocket subscription message
#[derive(Debug, Serialize)]
pub struct SubscriptionMessage {
    pub method: String,
    pub subscription: SubscriptionParams,
}

#[derive(Debug, Serialize)]
pub struct SubscriptionParams {
    #[serde(rename = "type")]
    pub type_: String,
    pub coin: String,
}

/// L2 Book request (POST over WebSocket)
#[derive(Debug, Serialize)]
pub struct L2BookRequest {
    pub method: String,
    pub id: u64,
    pub request: L2BookRequestInner,
}

#[derive(Debug, Serialize)]
pub struct L2BookRequestInner {
    #[serde(rename = "type")]
    pub type_: String,
    pub payload: L2BookPayload,
}

#[derive(Debug, Serialize)]
pub struct L2BookPayload {
    #[serde(rename = "type")]
    pub type_: String,
    pub coin: String,
    #[serde(rename = "nSigFigs")]
    pub n_sig_figs: Option<u32>,
    pub mantissa: Option<u32>,
}

/// WebSocket response wrapper (kept for backwards compatibility)
#[derive(Debug, Deserialize)]
pub struct WebSocketResponse {
    pub channel: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Unified WebSocket message for single-pass parsing (hot path optimization)
#[derive(Debug, Deserialize)]
pub enum HlWsMessage {
    /// L2 Book subscription update
    L2Book {
        channel: String,
        data: L2BookData,
    },
    /// Subscription confirmation
    SubscriptionResponse {
        channel: String,
        data: serde_json::Value,
    },
}

impl HlWsMessage {
    /// Fast parsing method that checks channel first
    pub fn parse_fast(text: &str) -> Result<Self, serde_json::Error> {
        // Helper to check channel
        #[derive(Deserialize)]
        struct ChannelHelper<'a> {
            channel: &'a str,
        }

        // Helper for L2Book message
        #[derive(Deserialize)]
        struct L2BookMsg {
            channel: String,
            data: L2BookData,
        }

        // Helper for SubscriptionResponse message
        #[derive(Deserialize)]
        struct SubResponseMsg {
            channel: String,
            data: serde_json::Value,
        }

        // Fast check of channel
        match serde_json::from_str::<ChannelHelper>(text) {
            Ok(helper) => match helper.channel {
                "l2Book" => {
                    let msg: L2BookMsg = serde_json::from_str(text)?;
                    Ok(HlWsMessage::L2Book {
                        channel: msg.channel,
                        data: msg.data,
                    })
                }
                "subscriptionResponse" => {
                    let msg: SubResponseMsg = serde_json::from_str(text)?;
                    Ok(HlWsMessage::SubscriptionResponse {
                        channel: msg.channel,
                        data: msg.data,
                    })
                }
                _ => Err(serde::de::Error::custom("Unknown channel")),
            },
            Err(e) => Err(e),
        }
    }
}

/// Generic WebSocket POST request wrapper (info or action)
#[derive(Debug, Serialize)]
pub struct WsPostRequest<T> {
    pub method: String,
    pub id: u64,
    pub request: WsPostRequestInner<T>,
}

#[derive(Debug, Serialize)]
pub struct WsPostRequestInner<T> {
    #[serde(rename = "type")]
    pub type_: String,
    pub payload: T,
}

/// Generic WebSocket POST response (for info/action/error)
#[derive(Debug, Deserialize)]
pub struct WsPostResponse {
    pub channel: String,
    pub data: WsPostResponseData,
}

#[derive(Debug, Deserialize)]
pub struct WsPostResponseData {
    pub id: u64,
    pub response: WsPostResponseInner,
}

#[derive(Debug, Deserialize)]
pub struct WsPostResponseInner {
    #[serde(rename = "type")]
    pub type_: String,
    pub payload: serde_json::Value,
}

/// L2 Book response
#[derive(Debug, Deserialize)]
pub struct L2BookResponse {
    pub channel: String,
    pub data: L2BookResponseData,
}

#[derive(Debug, Deserialize)]
pub struct L2BookResponseData {
    pub id: u64,
    pub response: L2BookResponseInner,
}

#[derive(Debug, Deserialize)]
pub struct L2BookResponseInner {
    #[serde(rename = "type")]
    pub type_: String,
    pub payload: L2BookResponsePayload,
}

#[derive(Debug, Deserialize)]
pub struct L2BookResponsePayload {
    #[serde(rename = "type")]
    pub type_: String,
    pub data: L2BookData,
}

/// L2 orderbook data
#[derive(Debug, Clone, Deserialize)]
pub struct L2BookData {
    pub coin: String,
    pub time: u64,
    pub levels: Vec<Vec<BookLevel>>, // [bids, asks]
}

/// Subscription response for l2Book
#[derive(Debug, Deserialize)]
pub struct L2BookSubscriptionResponse {
    pub channel: String,
    pub data: L2BookData,
}

/// Book level with price, size, and number of orders
#[derive(Debug, Clone, Deserialize)]
pub struct BookLevel {
    pub px: String,  // Price
    pub sz: String,  // Size
    pub n: u32,      // Number of orders
}

/// Top of book (best bid and ask)
#[derive(Debug, Clone)]
pub struct TopOfBook {
    pub best_bid: String,
    pub best_ask: String,
    pub coin: String,
    pub timestamp: u64,
}

impl L2BookData {
    /// Extract top of book (best bid and best ask)
    pub fn get_top_of_book(&self) -> Option<TopOfBook> {
        if self.levels.len() < 2 {
            return None;
        }

        let bids = &self.levels[0];
        let asks = &self.levels[1];

        if bids.is_empty() || asks.is_empty() {
            return None;
        }

        // First level is the best price
        let best_bid = &bids[0];
        let best_ask = &asks[0];

        Some(TopOfBook {
            best_bid: best_bid.px.clone(),
            best_ask: best_ask.px.clone(),
            coin: self.coin.clone(),
            timestamp: self.time,
        })
    }
}



/// Time in force for orders
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum TimeInForce {
    Gtc,  // Good till cancel
    Ioc,  // Immediate or cancel (for market orders)
    Alo,  // Add liquidity only (post-only)
}

/// Order type configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderType {
    pub limit: LimitOrderType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitOrderType {
    pub tif: TimeInForce,
}

/// Order for placement
#[derive(Debug, Clone, Serialize)]
pub struct Order {
    pub a: u32,           // Asset ID
    pub b: bool,          // Is buy
    pub p: String,        // Limit price
    pub s: String,        // Size
    pub r: bool,          // Reduce only
    pub t: OrderType,     // Order type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>, // Client order ID
}

/// Action to be signed
#[derive(Debug, Clone, Serialize)]
pub struct Action {
    #[serde(rename = "type")]
    pub type_: String,
    pub orders: Vec<Order>,
    pub grouping: String,
}

/// EIP-712 signature
#[derive(Debug, Clone, Serialize)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u32,
}

/// Signed order request payload
#[derive(Debug, Clone, Serialize)]
pub struct OrderRequest {
    pub action: Action,
    pub nonce: u64,
    pub signature: Signature,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vaultAddress: Option<String>,
}

/// Asset metadata from meta endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct AssetMeta {
    pub name: String,
    #[serde(rename = "szDecimals")]
    pub sz_decimals: i32,
    #[serde(rename = "maxLeverage")]
    pub max_leverage: Option<u32>,
    #[serde(rename = "marginTableId")]
    pub margin_table_id: Option<u32>,
    #[serde(rename = "isDelisted")]
    pub is_delisted: Option<bool>,
}

/// Meta response
#[derive(Debug, Clone, Deserialize)]
pub struct MetaResponse {
    pub universe: Vec<AssetMeta>,
}

/// Order placement response
#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponse {
    pub status: String,
    pub response: OrderResponseContent,
}

/// Response content can be either success (object) or error (string)
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OrderResponseContent {
    Success(OrderResponseData),
    Error(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderResponseData {
    #[serde(rename = "type", default)]
    pub type_: Option<String>,
    pub data: OrderStatusData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderStatusData {
    pub statuses: Vec<OrderStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OrderStatus {
    Resting { resting: RestingOrder },
    Filled { filled: FilledOrder },
    Error { error: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct RestingOrder {
    pub oid: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FilledOrder {
    pub totalSz: String,
    pub avgPx: String,
    pub oid: u64,
}

/// User fill item from userFills endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct UserFill {
    pub coin: String,             // Symbol (e.g., "AVAX" or "@107" for spot)
    pub px: String,               // Fill price
    pub sz: String,               // Fill size
    pub side: String,             // "B" for buy, "A" for ask/sell
    pub time: u64,                // Timestamp in milliseconds
    pub dir: String,              // Direction: "Open Long", "Sell", "Buy", etc.
    pub fee: String,              // Fee amount
    #[serde(rename = "feeToken")]
    pub fee_token: String,        // Fee token (e.g., "USDC")
    pub oid: u64,                 // Order ID
    pub tid: u64,                 // Trade ID
    pub hash: String,             // Transaction hash
    pub crossed: bool,            // Whether order crossed the spread
    #[serde(rename = "closedPnl")]
    pub closed_pnl: String,       // Closed PnL
    #[serde(rename = "startPosition")]
    pub start_position: String,   // Position size before fill
    #[serde(rename = "builderFee")]
    pub builder_fee: Option<String>, // Optional builder fee
}

/// User state from clearinghouseState endpoint
#[derive(Debug, Clone, Deserialize)]
pub struct UserState {
    #[serde(rename = "assetPositions")]
    pub asset_positions: Vec<AssetPosition>,
    #[serde(rename = "crossMarginSummary")]
    pub cross_margin_summary: CrossMarginSummary,
    #[serde(rename = "marginSummary")]
    pub margin_summary: MarginSummary,
    #[serde(rename = "withdrawable")]
    pub withdrawable: String,
    pub time: u64,
}

/// Asset position wrapper
#[derive(Debug, Clone, Deserialize)]
pub struct AssetPosition {
    #[serde(rename = "type")]
    pub type_: String,  // "oneWay" for one-way mode
    pub position: Position,
}

/// Position data for a specific asset
#[derive(Debug, Clone, Deserialize)]
pub struct Position {
    pub coin: String,                 // Symbol (e.g., "SOL", "BTC")
    pub szi: String,                  // Signed size (positive = long, negative = short)
    #[serde(rename = "entryPx")]
    pub entry_px: Option<String>,     // Entry price
    #[serde(rename = "positionValue")]
    pub position_value: String,       // Position value in USD
    #[serde(rename = "unrealizedPnl")]
    pub unrealized_pnl: String,       // Unrealized PnL
    #[serde(rename = "returnOnEquity")]
    pub return_on_equity: String,     // ROE
    #[serde(rename = "liquidationPx")]
    pub liquidation_px: Option<String>, // Liquidation price (if applicable)
    pub leverage: Leverage,           // Leverage configuration
    #[serde(rename = "marginUsed")]
    pub margin_used: String,          // Margin used for this position
    #[serde(rename = "maxLeverage")]
    pub max_leverage: u32,            // Max leverage allowed for this asset
    #[serde(rename = "cumFunding")]
    pub cum_funding: CumFunding,      // Cumulative funding
}

/// Cumulative funding data
#[derive(Debug, Clone, Deserialize)]
pub struct CumFunding {
    #[serde(rename = "allTime")]
    pub all_time: String,
    #[serde(rename = "sinceOpen")]
    pub since_open: String,
    #[serde(rename = "sinceChange")]
    pub since_change: String,
}

/// Leverage configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Leverage {
    #[serde(rename = "type")]
    pub type_: String,  // "cross" or "isolated"
    pub value: u32,     // Leverage value (e.g., 1, 5, 20)
    #[serde(rename = "rawUsd")]
    pub raw_usd: Option<String>,  // Raw USD value (for isolated positions)
}

/// Margin summary
#[derive(Debug, Clone, Deserialize)]
pub struct MarginSummary {
    #[serde(rename = "accountValue")]
    pub account_value: String,        // Total account value
    #[serde(rename = "totalNtlPos")]
    pub total_ntl_pos: String,        // Total notional position
    #[serde(rename = "totalRawUsd")]
    pub total_raw_usd: String,        // Total raw USD
    #[serde(rename = "totalMarginUsed")]
    pub total_margin_used: String,    // Total margin used
}

/// Cross margin summary
#[derive(Debug, Clone, Deserialize)]
pub struct CrossMarginSummary {
    #[serde(rename = "accountValue")]
    pub account_value: String,        // Account value
    #[serde(rename = "totalNtlPos")]
    pub total_ntl_pos: String,        // Total notional position
    #[serde(rename = "totalRawUsd")]
    pub total_raw_usd: String,        // Total raw USD
    #[serde(rename = "totalMarginUsed")]
    pub total_margin_used: String,    // Total margin used
}

#[cfg(test)]
mod tests {
    use super::*;

    const L2_BOOK_JSON: &str = r#"{
        "channel": "l2Book",
        "data": {
            "coin": "SOL",
            "time": 1704067200000,
            "levels": [
                [{"px": "100.50", "sz": "10.5", "n": 3}],
                [{"px": "100.55", "sz": "8.2", "n": 2}]
            ]
        }
    }"#;

    const SUBSCRIPTION_RESPONSE_JSON: &str = r#"{
        "channel": "subscriptionResponse",
        "data": {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "SOL"}}
    }"#;

    const ORDER_RESPONSE_SUCCESS_JSON: &str = r#"{
        "status": "ok",
        "response": {
            "type": "order",
            "data": {
                "statuses": [
                    {"filled": {"totalSz": "1.0", "avgPx": "100.50", "oid": 12345}}
                ]
            }
        }
    }"#;

    const ORDER_RESPONSE_RESTING_JSON: &str = r#"{
        "status": "ok",
        "response": {
            "type": "order",
            "data": {
                "statuses": [
                    {"resting": {"oid": 12346}}
                ]
            }
        }
    }"#;

    const ORDER_RESPONSE_ERROR_JSON: &str = r#"{
        "status": "err",
        "response": "Insufficient margin"
    }"#;

    const USER_FILL_JSON: &str = r#"{
        "coin": "SOL",
        "px": "100.50",
        "sz": "1.0",
        "side": "B",
        "time": 1704067200000,
        "dir": "Open Long",
        "fee": "0.025",
        "feeToken": "USDC",
        "oid": 12345,
        "tid": 67890,
        "hash": "0xabc123",
        "crossed": true,
        "closedPnl": "0",
        "startPosition": "0"
    }"#;

    const USER_STATE_JSON: &str = r#"{
        "assetPositions": [{
            "type": "oneWay",
            "position": {
                "coin": "SOL",
                "szi": "1.5",
                "entryPx": "100.50",
                "positionValue": "150.75",
                "unrealizedPnl": "2.25",
                "returnOnEquity": "0.015",
                "leverage": {"type": "cross", "value": 5},
                "marginUsed": "30.15",
                "maxLeverage": 20,
                "cumFunding": {"allTime": "0.5", "sinceOpen": "0.1", "sinceChange": "0.05"}
            }
        }],
        "crossMarginSummary": {
            "accountValue": "1000.00",
            "totalNtlPos": "150.75",
            "totalRawUsd": "1000.00",
            "totalMarginUsed": "30.15"
        },
        "marginSummary": {
            "accountValue": "1000.00",
            "totalNtlPos": "150.75",
            "totalRawUsd": "1000.00",
            "totalMarginUsed": "30.15"
        },
        "withdrawable": "969.85",
        "time": 1704067200000
    }"#;

    #[test]
    fn test_l2_book_parsing() {
        let response: L2BookSubscriptionResponse = serde_json::from_str(L2_BOOK_JSON).unwrap();

        assert_eq!(response.channel, "l2Book");
        assert_eq!(response.data.coin, "SOL");
        assert_eq!(response.data.time, 1704067200000);
        assert_eq!(response.data.levels.len(), 2);

        // Check bids (first level array)
        let bids = &response.data.levels[0];
        assert_eq!(bids.len(), 1);
        assert_eq!(bids[0].px, "100.50");
        assert_eq!(bids[0].sz, "10.5");
        assert_eq!(bids[0].n, 3);

        // Check asks (second level array)
        let asks = &response.data.levels[1];
        assert_eq!(asks.len(), 1);
        assert_eq!(asks[0].px, "100.55");
        assert_eq!(asks[0].sz, "8.2");
        assert_eq!(asks[0].n, 2);
    }

    #[test]
    fn test_top_of_book_extraction() {
        let response: L2BookSubscriptionResponse = serde_json::from_str(L2_BOOK_JSON).unwrap();
        let top = response.data.get_top_of_book().unwrap();

        assert_eq!(top.coin, "SOL");
        assert_eq!(top.timestamp, 1704067200000);
        assert_eq!(top.best_bid, "100.50");
        assert_eq!(top.best_ask, "100.55");
    }

    #[test]
    fn test_top_of_book_empty_levels() {
        let data = L2BookData {
            coin: "SOL".to_string(),
            time: 1704067200000,
            levels: vec![vec![], vec![]],
        };
        let top = data.get_top_of_book();
        assert!(top.is_none());
    }

    #[test]
    fn test_top_of_book_missing_levels() {
        let data = L2BookData {
            coin: "SOL".to_string(),
            time: 1704067200000,
            levels: vec![],
        };
        let top = data.get_top_of_book();
        assert!(top.is_none());
    }

    #[test]
    fn test_hl_ws_message_parse_fast_l2book() {
        let msg = HlWsMessage::parse_fast(L2_BOOK_JSON).unwrap();

        match msg {
            HlWsMessage::L2Book { channel, data } => {
                assert_eq!(channel, "l2Book");
                assert_eq!(data.coin, "SOL");
                assert_eq!(data.time, 1704067200000);
            }
            _ => panic!("Expected L2Book message"),
        }
    }

    #[test]
    fn test_hl_ws_message_parse_fast_subscription() {
        let msg = HlWsMessage::parse_fast(SUBSCRIPTION_RESPONSE_JSON).unwrap();

        match msg {
            HlWsMessage::SubscriptionResponse { channel, .. } => {
                assert_eq!(channel, "subscriptionResponse");
            }
            _ => panic!("Expected SubscriptionResponse message"),
        }
    }

    #[test]
    fn test_hl_ws_message_parse_fast_unknown_channel() {
        let unknown_json = r#"{"channel": "unknown_channel", "data": {}}"#;
        let result = HlWsMessage::parse_fast(unknown_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_order_response_success_filled() {
        let response: OrderResponse = serde_json::from_str(ORDER_RESPONSE_SUCCESS_JSON).unwrap();

        assert_eq!(response.status, "ok");
        match response.response {
            OrderResponseContent::Success(data) => {
                assert_eq!(data.data.statuses.len(), 1);
                match &data.data.statuses[0] {
                    OrderStatus::Filled { filled } => {
                        assert_eq!(filled.totalSz, "1.0");
                        assert_eq!(filled.avgPx, "100.50");
                        assert_eq!(filled.oid, 12345);
                    }
                    _ => panic!("Expected Filled status"),
                }
            }
            _ => panic!("Expected Success response"),
        }
    }

    #[test]
    fn test_order_response_success_resting() {
        let response: OrderResponse = serde_json::from_str(ORDER_RESPONSE_RESTING_JSON).unwrap();

        assert_eq!(response.status, "ok");
        match response.response {
            OrderResponseContent::Success(data) => {
                match &data.data.statuses[0] {
                    OrderStatus::Resting { resting } => {
                        assert_eq!(resting.oid, 12346);
                    }
                    _ => panic!("Expected Resting status"),
                }
            }
            _ => panic!("Expected Success response"),
        }
    }

    #[test]
    fn test_order_response_error() {
        let response: OrderResponse = serde_json::from_str(ORDER_RESPONSE_ERROR_JSON).unwrap();

        assert_eq!(response.status, "err");
        match response.response {
            OrderResponseContent::Error(msg) => {
                assert_eq!(msg, "Insufficient margin");
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_user_fill_parsing() {
        let fill: UserFill = serde_json::from_str(USER_FILL_JSON).unwrap();

        assert_eq!(fill.coin, "SOL");
        assert_eq!(fill.px, "100.50");
        assert_eq!(fill.sz, "1.0");
        assert_eq!(fill.side, "B");
        assert_eq!(fill.time, 1704067200000);
        assert_eq!(fill.dir, "Open Long");
        assert_eq!(fill.fee, "0.025");
        assert_eq!(fill.fee_token, "USDC");
        assert_eq!(fill.oid, 12345);
        assert_eq!(fill.tid, 67890);
        assert_eq!(fill.hash, "0xabc123");
        assert!(fill.crossed);
        assert_eq!(fill.closed_pnl, "0");
        assert_eq!(fill.start_position, "0");
    }

    #[test]
    fn test_user_state_parsing() {
        let state: UserState = serde_json::from_str(USER_STATE_JSON).unwrap();

        assert_eq!(state.withdrawable, "969.85");
        assert_eq!(state.time, 1704067200000);

        // Check position
        assert_eq!(state.asset_positions.len(), 1);
        let pos = &state.asset_positions[0];
        assert_eq!(pos.type_, "oneWay");
        assert_eq!(pos.position.coin, "SOL");
        assert_eq!(pos.position.szi, "1.5");
        assert_eq!(pos.position.entry_px, Some("100.50".to_string()));
        assert_eq!(pos.position.position_value, "150.75");
        assert_eq!(pos.position.unrealized_pnl, "2.25");
        assert_eq!(pos.position.leverage.type_, "cross");
        assert_eq!(pos.position.leverage.value, 5);
        assert_eq!(pos.position.max_leverage, 20);

        // Check margin summary
        assert_eq!(state.margin_summary.account_value, "1000.00");
        assert_eq!(state.margin_summary.total_ntl_pos, "150.75");
        assert_eq!(state.margin_summary.total_margin_used, "30.15");

        // Check cross margin summary
        assert_eq!(state.cross_margin_summary.account_value, "1000.00");
    }

    #[test]
    fn test_subscription_message_creation() {
        let msg = SubscriptionMessage {
            method: "subscribe".to_string(),
            subscription: SubscriptionParams {
                type_: "l2Book".to_string(),
                coin: "SOL".to_string(),
            },
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"method\":\"subscribe\""));
        assert!(json.contains("\"type\":\"l2Book\""));
        assert!(json.contains("\"coin\":\"SOL\""));
    }

    #[test]
    fn test_time_in_force_serialization() {
        let tifs = [
            (TimeInForce::Gtc, "Gtc"),
            (TimeInForce::Ioc, "Ioc"),
            (TimeInForce::Alo, "Alo"),
        ];

        for (tif, expected) in tifs {
            let json = serde_json::to_string(&tif).unwrap();
            assert_eq!(json, format!("\"{}\"", expected));
        }
    }

    #[test]
    fn test_order_creation() {
        let order = Order {
            a: 5,  // Asset ID
            b: true,  // Is buy
            p: "100.50".to_string(),
            s: "1.0".to_string(),
            r: false,  // Not reduce only
            t: OrderType {
                limit: LimitOrderType {
                    tif: TimeInForce::Ioc,
                },
            },
            c: Some("client-123".to_string()),
        };

        let json = serde_json::to_string(&order).unwrap();
        assert!(json.contains("\"a\":5"));
        assert!(json.contains("\"b\":true"));
        assert!(json.contains("\"p\":\"100.50\""));
        assert!(json.contains("\"s\":\"1.0\""));
        assert!(json.contains("\"r\":false"));
        assert!(json.contains("\"c\":\"client-123\""));
    }

    #[test]
    fn test_action_creation() {
        let action = Action {
            type_: "order".to_string(),
            orders: vec![Order {
                a: 5,
                b: true,
                p: "100.50".to_string(),
                s: "1.0".to_string(),
                r: false,
                t: OrderType {
                    limit: LimitOrderType {
                        tif: TimeInForce::Ioc,
                    },
                },
                c: None,
            }],
            grouping: "na".to_string(),
        };

        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("\"type\":\"order\""));
        assert!(json.contains("\"grouping\":\"na\""));
    }

    #[test]
    fn test_meta_response_parsing() {
        let meta_json = r#"{
            "universe": [
                {"name": "SOL", "szDecimals": 2, "maxLeverage": 20},
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 50}
            ]
        }"#;

        let meta: MetaResponse = serde_json::from_str(meta_json).unwrap();

        assert_eq!(meta.universe.len(), 2);
        assert_eq!(meta.universe[0].name, "SOL");
        assert_eq!(meta.universe[0].sz_decimals, 2);
        assert_eq!(meta.universe[0].max_leverage, Some(20));
        assert_eq!(meta.universe[1].name, "BTC");
        assert_eq!(meta.universe[1].sz_decimals, 5);
    }

    #[test]
    fn test_book_level_parsing() {
        let level_json = r#"{"px": "100.50", "sz": "10.5", "n": 3}"#;
        let level: BookLevel = serde_json::from_str(level_json).unwrap();

        assert_eq!(level.px, "100.50");
        assert_eq!(level.sz, "10.5");
        assert_eq!(level.n, 3);
    }

    #[test]
    fn test_cumulative_funding_parsing() {
        let funding_json = r#"{"allTime": "1.5", "sinceOpen": "0.5", "sinceChange": "0.1"}"#;
        let funding: CumFunding = serde_json::from_str(funding_json).unwrap();

        assert_eq!(funding.all_time, "1.5");
        assert_eq!(funding.since_open, "0.5");
        assert_eq!(funding.since_change, "0.1");
    }

    #[test]
    fn test_leverage_parsing() {
        // Cross leverage
        let cross_json = r#"{"type": "cross", "value": 10}"#;
        let cross: Leverage = serde_json::from_str(cross_json).unwrap();
        assert_eq!(cross.type_, "cross");
        assert_eq!(cross.value, 10);
        assert!(cross.raw_usd.is_none());

        // Isolated leverage
        let isolated_json = r#"{"type": "isolated", "value": 5, "rawUsd": "100.00"}"#;
        let isolated: Leverage = serde_json::from_str(isolated_json).unwrap();
        assert_eq!(isolated.type_, "isolated");
        assert_eq!(isolated.value, 5);
        assert_eq!(isolated.raw_usd, Some("100.00".to_string()));
    }
}
