/// Service modules - each task runs in its own service

pub mod fill_detection;
pub mod fill_handler;
pub mod market_event;
pub mod rest_fill_detection;
pub mod position_monitor;
pub mod order_monitor;
pub mod hedge;
pub mod orderbook;
pub mod rest_poll;
pub mod ws_health;
pub mod opportunity_loop;

pub use fill_handler::{FillHandler, FillSource, FillType, hash_cloid};

use crate::strategy::OrderSide;

/// HedgeEvent represents a single hedge trigger coming from any
/// fill detection layer. It is carried through a low-latency queue
/// between the fill detection "thread(s)" and the hedge executor.
///
/// Struct layout for explicit field naming (replaces tuple for clarity).
#[derive(Debug, Clone)]
pub struct HedgeEvent {
    pub side: OrderSide,
    pub size: f64,
    pub avg_price: f64,
    pub fill_timestamp: std::time::Instant,
    /// Client order ID for deduplication in hedge service
    pub client_order_id: String,
}
