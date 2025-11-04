mod types;
mod client;
pub mod trading;
pub mod fill_detection;
pub mod ws_trading;

pub use client::{OrderbookClient, OrderbookConfig};
pub use trading::{PacificaTrading, PacificaCredentials, OrderSide, TradeHistoryItem};
pub use fill_detection::{FillDetectionClient, FillDetectionConfig};
pub use ws_trading::PacificaWsTrading;
pub use types::{FillEvent, OrderStatus, OrderEvent};
