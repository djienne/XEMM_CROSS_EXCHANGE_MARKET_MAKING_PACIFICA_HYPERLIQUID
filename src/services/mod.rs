pub mod fill_aggregator;
pub mod fill_dedup;
/// Service modules - each task runs in its own service.
pub mod fill_detection;
pub mod hedge;
pub mod hedge_store;
pub mod order_monitor;
pub mod position_monitor;
pub mod position_reconciler;
pub mod post_trade_auditor;
pub mod price_source;
pub mod rest_fill_detection;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::bot::BotState;
use crate::strategy::OrderSide;

/// One residual hedge intent from a fill detector or reconciler.
#[derive(Debug, Clone)]
pub struct HedgeEvent {
    pub source_order_id: u64,
    pub hedge_seq: u64,
    pub side: OrderSide,
    pub size: f64,
    pub avg_price: f64,
    pub detected_at: std::time::Instant,
    pub terminal: bool,
}

impl From<fill_aggregator::HedgeDecision> for HedgeEvent {
    fn from(decision: fill_aggregator::HedgeDecision) -> Self {
        Self {
            source_order_id: decision.source_order_id,
            hedge_seq: decision.hedge_seq,
            side: decision.side,
            size: decision.size,
            avg_price: decision.avg_price,
            detected_at: decision.detected_at,
            terminal: decision.terminal,
        }
    }
}

/// Enqueue a hedge intent without dropping exposure on backpressure.
///
/// If the low-latency queue is full or closed, the intent is appended to a
/// local JSONL file and maker placement is halted via Reconciling/Error state.
pub async fn enqueue_hedge_intent(
    hedge_tx: &mpsc::Sender<HedgeEvent>,
    bot_state: &Arc<RwLock<BotState>>,
    intent: HedgeEvent,
) -> anyhow::Result<()> {
    if let Err(e) = hedge_store::append_lifecycle_update(hedge_store::HedgeLifecycleUpdate::new(
        &intent,
        hedge_store::HedgeIntentStatus::Created,
    ))
    .await
    {
        let mut state = bot_state.write();
        state.set_error(format!(
            "failed to persist hedge intent before enqueue: {}",
            e
        ));
    }

    match hedge_tx.try_send(intent.clone()) {
        Ok(()) => {
            hedge_store::append_lifecycle_update(hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::Queued,
            ))
            .await?;
            Ok(())
        }
        Err(mpsc::error::TrySendError::Full(intent)) => {
            let mut update = hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::QueueFull,
            );
            update.reason = Some("hedge queue full".to_string());
            hedge_store::append_lifecycle_update(update).await?;
            let mut state = bot_state.write();
            state.mark_reconciling();
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(intent)) => {
            let mut update = hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::QueueClosed,
            );
            update.reason = Some("hedge queue closed".to_string());
            hedge_store::append_lifecycle_update(update).await?;
            let mut state = bot_state.write();
            state.set_error("hedge queue closed; intent persisted".to_string());
            Ok(())
        }
    }
}
