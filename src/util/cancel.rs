use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use anyhow::Result;

/// Cancel all `symbol` orders via REST and WebSocket concurrently (redundancy).
///
/// Returns `(rest_count, ws_count)`; a failed leg logs and counts 0. Errors only
/// when BOTH legs fail, so callers can tell "both transports failed" apart from
/// "nothing to cancel".
pub async fn dual_cancel(
    rest: &PacificaTrading,
    ws: &PacificaWsTrading,
    symbol: &str,
) -> Result<(u32, u32)> {
    let (rest_result, ws_result) = tokio::join!(
        rest.cancel_all_orders(false, Some(symbol), false),
        ws.cancel_all_orders_ws(false, Some(symbol), false)
    );
    if let (Err(r), Err(w)) = (&rest_result, &ws_result) {
        anyhow::bail!("REST and WS cancel_all both failed: {}; {}", r, w);
    }
    let count = |res: Result<u32>, leg: &str| {
        res.unwrap_or_else(|e| {
            tracing::warn!("{} cancel_all failed: {}", leg, e);
            0
        })
    };
    Ok((count(rest_result, "REST"), count(ws_result, "WS")))
}
