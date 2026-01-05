mod types;
mod client;
pub mod trading;
pub mod client_trait;
pub mod fill_detection;
pub mod ws_trading;

pub use client::{OrderbookClient, OrderbookConfig};
pub use trading::{OpenOrderItem, PositionItem, PositionResponse, PacificaTrading, PacificaCredentials, TradeHistoryItem};
pub use crate::strategy::OrderSide;
pub use fill_detection::{FillDetectionClient, FillDetectionConfig, PositionBaselineUpdater};
pub use ws_trading::PacificaWsTrading;
pub use client_trait::PacificaTradingClient;
pub use types::{FillEvent, OrderStatus, OrderEvent};
