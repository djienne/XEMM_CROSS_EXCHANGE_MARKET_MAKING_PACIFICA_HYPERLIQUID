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

/// WebSocket response wrapper
#[derive(Debug, Deserialize)]
pub struct WebSocketResponse {
    pub channel: String,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
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
