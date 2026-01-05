//! Mock implementations for trading clients used in unit tests.
//!
//! These mocks allow testing business logic without making real API calls.
//! Only compiled in test mode (`#[cfg(test)]`).

use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;

use crate::connector::hyperliquid::client_trait::HyperliquidTradingClient;
use crate::connector::hyperliquid::types::{
    AssetMeta, OrderRequest, OrderResponse, OrderResponseContent, OrderResponseData,
    OrderStatus, OrderStatusData, FilledOrder, UserFill, UserState, AssetPosition, Position,
    Leverage, CumFunding, MarginSummary, CrossMarginSummary,
};
use crate::connector::pacifica::client_trait::PacificaTradingClient;
use crate::connector::pacifica::trading::{OrderData, OpenOrderItem, PositionItem};
use crate::strategy::OrderSide;

/// Mock Hyperliquid trading client for unit tests.
///
/// Configure return values via the `*_result` fields before calling methods.
/// Track call counts via the `*_calls` fields.
pub struct MockHyperliquidTradingClient {
    /// Result to return from get_l2_snapshot
    pub get_l2_snapshot_result: Mutex<Result<Option<(f64, f64)>>>,
    /// Result to return from place_market_order
    pub place_market_order_result: Mutex<Result<OrderResponse>>,
    /// Result to return from get_user_fills
    pub get_user_fills_result: Mutex<Result<Vec<UserFill>>>,
    /// Result to return from get_user_state
    pub get_user_state_result: Mutex<Result<UserState>>,
    /// Result to return from get_asset_info
    pub get_asset_info_result: Mutex<Result<AssetMeta>>,
    /// Wallet address to return
    pub wallet_address: String,
    /// Whether this is testnet
    pub is_testnet_value: bool,

    // Call tracking
    pub get_l2_snapshot_calls: AtomicU32,
    pub place_market_order_calls: AtomicU32,
    pub get_user_fills_calls: AtomicU32,
    pub get_user_state_calls: AtomicU32,

    // Capture last call arguments
    pub last_market_order_is_buy: Mutex<Option<bool>>,
    pub last_market_order_size: Mutex<Option<f64>>,
}

impl MockHyperliquidTradingClient {
    /// Create a new mock with default success responses.
    pub fn new() -> Self {
        Self {
            get_l2_snapshot_result: Mutex::new(Ok(Some((100.0, 100.1)))),
            place_market_order_result: Mutex::new(Ok(Self::default_order_response())),
            get_user_fills_result: Mutex::new(Ok(vec![])),
            get_user_state_result: Mutex::new(Ok(Self::default_user_state())),
            get_asset_info_result: Mutex::new(Ok(Self::default_asset_meta())),
            wallet_address: "0xmock_wallet".to_string(),
            is_testnet_value: false,
            get_l2_snapshot_calls: AtomicU32::new(0),
            place_market_order_calls: AtomicU32::new(0),
            get_user_fills_calls: AtomicU32::new(0),
            get_user_state_calls: AtomicU32::new(0),
            last_market_order_is_buy: Mutex::new(None),
            last_market_order_size: Mutex::new(None),
        }
    }

    /// Create a default successful OrderResponse.
    pub fn default_order_response() -> OrderResponse {
        OrderResponse {
            status: "ok".to_string(),
            response: OrderResponseContent::Success(OrderResponseData {
                type_: Some("order".to_string()),
                data: OrderStatusData {
                    statuses: vec![OrderStatus::Filled {
                        filled: FilledOrder {
                            totalSz: "1.0".to_string(),
                            avgPx: "100.05".to_string(),
                            oid: 12345,
                        },
                    }],
                },
            }),
        }
    }

    /// Create a default UserState.
    pub fn default_user_state() -> UserState {
        UserState {
            asset_positions: vec![],
            cross_margin_summary: CrossMarginSummary {
                account_value: "1000.0".to_string(),
                total_ntl_pos: "0".to_string(),
                total_raw_usd: "1000.0".to_string(),
                total_margin_used: "0".to_string(),
            },
            margin_summary: MarginSummary {
                account_value: "1000.0".to_string(),
                total_ntl_pos: "0".to_string(),
                total_raw_usd: "1000.0".to_string(),
                total_margin_used: "0".to_string(),
            },
            withdrawable: "1000.0".to_string(),
            time: 0,
        }
    }

    /// Create a default AssetMeta.
    pub fn default_asset_meta() -> AssetMeta {
        AssetMeta {
            name: "SOL".to_string(),
            sz_decimals: 2,
            max_leverage: Some(20),
            margin_table_id: None,
            is_delisted: None,
        }
    }

    /// Create a UserState with a specific position.
    pub fn user_state_with_position(coin: &str, size: f64) -> UserState {
        let mut state = Self::default_user_state();
        state.asset_positions.push(AssetPosition {
            type_: "oneWay".to_string(),
            position: Position {
                coin: coin.to_string(),
                szi: size.to_string(),
                entry_px: Some("100.0".to_string()),
                position_value: (size.abs() * 100.0).to_string(),
                unrealized_pnl: "0".to_string(),
                return_on_equity: "0".to_string(),
                liquidation_px: None,
                leverage: Leverage {
                    type_: "cross".to_string(),
                    value: 5,
                    raw_usd: None,
                },
                margin_used: "20.0".to_string(),
                max_leverage: 20,
                cum_funding: CumFunding {
                    all_time: "0".to_string(),
                    since_open: "0".to_string(),
                    since_change: "0".to_string(),
                },
            },
        });
        state
    }
}

impl Default for MockHyperliquidTradingClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HyperliquidTradingClient for MockHyperliquidTradingClient {
    async fn get_l2_snapshot(&self, _coin: &str) -> Result<Option<(f64, f64)>> {
        self.get_l2_snapshot_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.get_l2_snapshot_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(*v),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn place_market_order(
        &self,
        _coin: &str,
        is_buy: bool,
        size: f64,
        _slippage: f64,
        _reduce_only: bool,
        _bid: Option<f64>,
        _ask: Option<f64>,
    ) -> Result<OrderResponse> {
        self.place_market_order_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_market_order_is_buy.lock().unwrap() = Some(is_buy);
        *self.last_market_order_size.lock().unwrap() = Some(size);

        let guard = self.place_market_order_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn build_market_order_request(
        &self,
        _coin: &str,
        _is_buy: bool,
        _size: f64,
        _slippage: f64,
        _reduce_only: bool,
        _bid: Option<f64>,
        _ask: Option<f64>,
    ) -> Result<OrderRequest> {
        // Return a mock OrderRequest - not typically used in tests
        Err(anyhow::anyhow!("build_market_order_request not implemented in mock"))
    }

    async fn build_market_order_request_with_meta(
        &self,
        _coin: &str,
        _is_buy: bool,
        _size: f64,
        _slippage: f64,
        _reduce_only: bool,
        _bid: Option<f64>,
        _ask: Option<f64>,
        _asset_id: u32,
        _sz_decimals: i32,
    ) -> Result<OrderRequest> {
        // Return a mock OrderRequest - not typically used in tests
        Err(anyhow::anyhow!("build_market_order_request_with_meta not implemented in mock"))
    }

    async fn get_user_fills(&self, _user: &str, _aggregate_by_time: bool) -> Result<Vec<UserFill>> {
        self.get_user_fills_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.get_user_fills_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn get_user_state(&self, _user: &str) -> Result<UserState> {
        self.get_user_state_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.get_user_state_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn get_asset_info(&self, _coin: &str) -> Result<AssetMeta> {
        let guard = self.get_asset_info_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn get_asset_id(&self, _coin: &str) -> Result<u32> {
        Ok(5) // Default asset ID for SOL
    }

    fn is_testnet(&self) -> bool {
        self.is_testnet_value
    }

    fn get_wallet_address(&self) -> String {
        self.wallet_address.clone()
    }
}

/// Mock Pacifica trading client for unit tests.
///
/// Configure return values via the `*_result` fields before calling methods.
/// Track call counts via the `*_calls` fields.
pub struct MockPacificaTradingClient {
    /// Result to return from place_limit_order
    pub place_limit_order_result: Mutex<Result<OrderData>>,
    /// Result to return from cancel_all_orders
    pub cancel_all_orders_result: Mutex<Result<u32>>,
    /// Result to return from get_open_orders
    pub get_open_orders_result: Mutex<Result<Vec<OpenOrderItem>>>,
    /// Result to return from get_positions
    pub get_positions_result: Mutex<Result<Vec<PositionItem>>>,
    /// Result to return from cancel_order
    pub cancel_order_result: Mutex<Result<()>>,

    // Call tracking
    pub place_limit_order_calls: AtomicU32,
    pub cancel_all_orders_calls: AtomicU32,
    pub get_open_orders_calls: AtomicU32,
    pub get_positions_calls: AtomicU32,
    pub cancel_order_calls: AtomicU32,

    // Capture last call arguments
    pub last_order_side: Mutex<Option<OrderSide>>,
    pub last_order_price: Mutex<Option<f64>>,
    pub last_order_size: Mutex<Option<f64>>,
}

impl MockPacificaTradingClient {
    /// Create a new mock with default success responses.
    pub fn new() -> Self {
        Self {
            place_limit_order_result: Mutex::new(Ok(Self::default_order_data())),
            cancel_all_orders_result: Mutex::new(Ok(0)),
            get_open_orders_result: Mutex::new(Ok(vec![])),
            get_positions_result: Mutex::new(Ok(vec![])),
            cancel_order_result: Mutex::new(Ok(())),
            place_limit_order_calls: AtomicU32::new(0),
            cancel_all_orders_calls: AtomicU32::new(0),
            get_open_orders_calls: AtomicU32::new(0),
            get_positions_calls: AtomicU32::new(0),
            cancel_order_calls: AtomicU32::new(0),
            last_order_side: Mutex::new(None),
            last_order_price: Mutex::new(None),
            last_order_size: Mutex::new(None),
        }
    }

    /// Create a default successful OrderData.
    pub fn default_order_data() -> OrderData {
        OrderData {
            order_id: Some(12345),
            i: Some(12345),
            client_order_id: Some("mock-client-order-id".to_string()),
            symbol: Some("SOL".to_string()),
        }
    }

    /// Create an OrderData with a specific client_order_id.
    pub fn order_data_with_cloid(cloid: &str) -> OrderData {
        OrderData {
            order_id: Some(12345),
            i: Some(12345),
            client_order_id: Some(cloid.to_string()),
            symbol: Some("SOL".to_string()),
        }
    }

    /// Create a PositionItem.
    pub fn position_item(symbol: &str, side: &str, amount: f64) -> PositionItem {
        PositionItem {
            symbol: symbol.to_string(),
            side: side.to_string(),
            amount: amount.to_string(),
            entry_price: "100.0".to_string(),
            margin: None,
            funding: "0".to_string(),
            isolated: false,
            created_at: 0,
            updated_at: 0,
        }
    }
}

impl Default for MockPacificaTradingClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PacificaTradingClient for MockPacificaTradingClient {
    async fn place_limit_order(
        &self,
        _symbol: &str,
        side: OrderSide,
        size: f64,
        price: Option<f64>,
        _mid_price_offset_pct: f64,
        _current_bid: Option<f64>,
        _current_ask: Option<f64>,
    ) -> Result<OrderData> {
        self.place_limit_order_calls.fetch_add(1, Ordering::SeqCst);
        *self.last_order_side.lock().unwrap() = Some(side);
        *self.last_order_price.lock().unwrap() = price;
        *self.last_order_size.lock().unwrap() = Some(size);

        let guard = self.place_limit_order_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn cancel_all_orders(
        &self,
        _all_symbols: bool,
        _symbol: Option<&str>,
        _exclude_reduce_only: bool,
    ) -> Result<u32> {
        self.cancel_all_orders_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.cancel_all_orders_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(*v),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn get_open_orders(&self) -> Result<Vec<OpenOrderItem>> {
        self.get_open_orders_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.get_open_orders_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn get_positions(&self) -> Result<Vec<PositionItem>> {
        self.get_positions_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.get_positions_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(v.clone()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }

    async fn cancel_order(&self, _symbol: &str, _client_order_id: &str) -> Result<()> {
        self.cancel_order_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.cancel_order_result.lock().unwrap();
        match &*guard {
            Ok(()) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }
}

/// Mock Pacifica WebSocket trading client for unit tests.
pub struct MockPacificaWsTrading {
    /// Result to return from cancel_all_orders_ws
    pub cancel_all_orders_ws_result: Mutex<Result<u32>>,
    /// Call tracking
    pub cancel_all_orders_ws_calls: AtomicU32,
}

impl MockPacificaWsTrading {
    pub fn new() -> Self {
        Self {
            cancel_all_orders_ws_result: Mutex::new(Ok(0)),
            cancel_all_orders_ws_calls: AtomicU32::new(0),
        }
    }

    /// Simulate cancel_all_orders_ws call
    pub async fn cancel_all_orders_ws(
        &self,
        _all_symbols: bool,
        _symbol: Option<&str>,
        _exclude_reduce_only: bool,
    ) -> Result<u32> {
        self.cancel_all_orders_ws_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.cancel_all_orders_ws_result.lock().unwrap();
        match &*guard {
            Ok(v) => Ok(*v),
            Err(e) => Err(anyhow::anyhow!("{}", e)),
        }
    }
}

impl Default for MockPacificaWsTrading {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_hyperliquid_client_default() {
        let mock = MockHyperliquidTradingClient::new();

        // Test get_l2_snapshot
        let result = mock.get_l2_snapshot("SOL").await.unwrap();
        assert_eq!(result, Some((100.0, 100.1)));
        assert_eq!(mock.get_l2_snapshot_calls.load(Ordering::SeqCst), 1);

        // Test place_market_order
        let result = mock
            .place_market_order("SOL", true, 1.0, 0.05, false, Some(100.0), Some(100.1))
            .await
            .unwrap();
        assert_eq!(result.status, "ok");
        assert_eq!(mock.place_market_order_calls.load(Ordering::SeqCst), 1);
        assert_eq!(*mock.last_market_order_is_buy.lock().unwrap(), Some(true));
        assert_eq!(*mock.last_market_order_size.lock().unwrap(), Some(1.0));
    }

    #[tokio::test]
    async fn test_mock_hyperliquid_client_custom_response() {
        let mock = MockHyperliquidTradingClient::new();

        // Set custom error response
        *mock.get_l2_snapshot_result.lock().unwrap() = Err(anyhow::anyhow!("Network error"));

        let result = mock.get_l2_snapshot("SOL").await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Network error"));
    }

    #[tokio::test]
    async fn test_mock_pacifica_client_default() {
        let mock = MockPacificaTradingClient::new();

        // Test place_limit_order
        let result = mock
            .place_limit_order("SOL", OrderSide::Buy, 1.0, Some(100.0), 0.0, None, None)
            .await
            .unwrap();
        assert_eq!(result.order_id, Some(12345));
        assert_eq!(mock.place_limit_order_calls.load(Ordering::SeqCst), 1);

        // Test cancel_all_orders
        let result = mock.cancel_all_orders(false, Some("SOL"), false).await.unwrap();
        assert_eq!(result, 0);
        assert_eq!(mock.cancel_all_orders_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_mock_pacifica_client_tracks_arguments() {
        let mock = MockPacificaTradingClient::new();

        mock.place_limit_order("SOL", OrderSide::Sell, 2.5, Some(150.0), 0.0, None, None)
            .await
            .unwrap();

        assert_eq!(*mock.last_order_side.lock().unwrap(), Some(OrderSide::Sell));
        assert_eq!(*mock.last_order_price.lock().unwrap(), Some(150.0));
        assert_eq!(*mock.last_order_size.lock().unwrap(), Some(2.5));
    }

    #[tokio::test]
    async fn test_mock_pacifica_ws_trading() {
        let mock = MockPacificaWsTrading::new();

        let result = mock.cancel_all_orders_ws(false, Some("SOL"), false).await.unwrap();
        assert_eq!(result, 0);
        assert_eq!(mock.cancel_all_orders_ws_calls.load(Ordering::SeqCst), 1);
    }
}
