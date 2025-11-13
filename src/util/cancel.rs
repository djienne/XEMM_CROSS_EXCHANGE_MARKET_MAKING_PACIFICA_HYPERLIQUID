use anyhow::Result;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};

/// Performs dual cancellation (REST + WebSocket) for redundancy
///
/// This function attempts to cancel orders via both REST API and WebSocket
/// to maximize the chance of successful cancellation. Both methods are attempted
/// regardless of individual failures.
///
/// # Arguments
/// * `rest` - REST API trading client
/// * `ws` - WebSocket trading client
/// * `symbol` - Symbol to cancel orders for
///
/// # Returns
/// * `Ok((rest_count, ws_count))` - Number of orders cancelled by each method
pub async fn dual_cancel(
    rest: &PacificaTrading,
    ws: &PacificaWsTrading,
    symbol: &str,
) -> Result<(u32, u32)> {
    // REST API cancel (fast, reliable)
    let rest_count = match rest.cancel_all_orders(false, Some(symbol), false).await {
        Ok(count) => count,
        Err(e) => {
            tracing::warn!("REST cancel_all_orders failed: {}", e);
            0
        }
    };

    // WebSocket cancel (ultra-fast, no rate limits)
    let ws_count = match ws.cancel_all_orders_ws(false, Some(symbol), false).await {
        Ok(count) => count,
        Err(e) => {
            tracing::warn!("WS cancel_all_orders_ws failed: {}", e);
            0
        }
    };

    Ok((rest_count, ws_count))
}
