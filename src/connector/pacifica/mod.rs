mod client;
pub mod fill_detection;
pub mod maker_exchange;
pub mod trading;
mod types;
pub mod ws_trading;

pub use client::{OrderbookClient, OrderbookConfig};
pub use fill_detection::{
    FillDetectionClient, FillDetectionConfig, PositionBaselineUpdater, ReconcileHook,
};
pub use trading::{
    OpenOrderItem, OrderSide, PacificaCredentials, PacificaTrading, PositionItem, PositionResponse,
    TradeHistoryItem,
};
pub use maker_exchange::{PacificaFillStream, PacificaMaker};
pub(crate) use types::f64_from_str_or_number;
pub use types::{FillEvent, OrderEvent, OrderStatus};
pub use ws_trading::PacificaWsTrading;
