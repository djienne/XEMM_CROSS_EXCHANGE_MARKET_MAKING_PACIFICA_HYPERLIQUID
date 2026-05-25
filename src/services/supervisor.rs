use std::future::Future;
use std::sync::Arc;

use tracing::{error, warn};

use crate::services::trade_gate::{GateReason, TradeGate};

pub fn spawn_supervised<F>(name: &'static str, trade_gate: Arc<TradeGate>, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let handle = tokio::spawn(future);
        match handle.await {
            Ok(()) => warn!("[SUPERVISOR] task '{}' exited", name),
            Err(e) => error!("[SUPERVISOR] task '{}' failed: {}", name, e),
        }
        trade_gate.block(GateReason::ServiceDown);
    });
}
