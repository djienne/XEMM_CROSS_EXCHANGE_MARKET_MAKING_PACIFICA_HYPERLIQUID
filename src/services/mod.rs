/// Service modules - each task runs in its own service

pub mod fill_detection;
pub mod rest_fill_detection;
pub mod position_monitor;
pub mod order_monitor;
pub mod hedge;
pub mod orderbook;
pub mod rest_poll;

use crate::strategy::OrderSide;

/// HedgeEvent represents a single hedge trigger coming from any
/// fill detection layer. It is carried through a low-latency queue
/// between the fill detection “thread(s)” and the hedge executor.
///
/// Tuple layout:
/// (side, size, avg_price, fill_detect_timestamp)
pub type HedgeEvent = (OrderSide, f64, f64, std::time::Instant);
