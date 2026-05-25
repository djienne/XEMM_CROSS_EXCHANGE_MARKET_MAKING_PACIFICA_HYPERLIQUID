pub mod cancel_manager;
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
pub mod safety_monitor;
pub mod supervisor;
pub mod trade_gate;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::bot::BotState;
use crate::strategy::OrderSide;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeSource {
    MakerFill,
    PositionMonitor,
    Reconciler,
    PlacementRecovery,
}

impl HedgeSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MakerFill => "maker_fill",
            Self::PositionMonitor => "position_monitor",
            Self::Reconciler => "reconciler",
            Self::PlacementRecovery => "placement_recovery",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeVenueSide {
    Buy,
    Sell,
}

impl HedgeVenueSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }

    pub const fn is_buy(self) -> bool {
        matches!(self, Self::Buy)
    }

    pub const fn from_maker_side(side: OrderSide) -> Self {
        match side {
            OrderSide::Buy => Self::Sell,
            OrderSide::Sell => Self::Buy,
        }
    }

    pub const fn synthetic_maker_side(self) -> OrderSide {
        match self {
            Self::Buy => OrderSide::Sell,
            Self::Sell => OrderSide::Buy,
        }
    }
}

/// One residual hedge intent from a fill detector or reconciler.
#[derive(Debug, Clone)]
pub struct HedgeIntent {
    pub source_order_id: u64,
    pub hedge_seq: u64,
    pub source: HedgeSource,
    pub maker_side: Option<OrderSide>,
    pub hedge_side: HedgeVenueSide,
    pub size: f64,
    pub avg_price: f64,
    pub detected_at: std::time::Instant,
    pub terminal: bool,
}

impl HedgeIntent {
    pub fn from_maker_fill(
        source_order_id: u64,
        hedge_seq: u64,
        side: OrderSide,
        size: f64,
        avg_price: f64,
        detected_at: std::time::Instant,
        terminal: bool,
    ) -> Self {
        Self {
            source_order_id,
            hedge_seq,
            source: HedgeSource::MakerFill,
            maker_side: Some(side),
            hedge_side: HedgeVenueSide::from_maker_side(side),
            size,
            avg_price,
            detected_at,
            terminal,
        }
    }

    pub fn from_venue_side(
        source_order_id: u64,
        hedge_seq: u64,
        source: HedgeSource,
        hedge_side: HedgeVenueSide,
        size: f64,
        avg_price: f64,
        terminal: bool,
    ) -> Self {
        Self {
            source_order_id,
            hedge_seq,
            source,
            maker_side: None,
            hedge_side,
            size,
            avg_price,
            detected_at: std::time::Instant::now(),
            terminal,
        }
    }

    #[inline]
    pub fn audit_maker_side(&self) -> OrderSide {
        self.maker_side
            .unwrap_or_else(|| self.hedge_side.synthetic_maker_side())
    }
}

impl From<fill_aggregator::HedgeDecision> for HedgeIntent {
    fn from(decision: fill_aggregator::HedgeDecision) -> Self {
        Self::from_maker_fill(
            decision.source_order_id,
            decision.hedge_seq,
            decision.side,
            decision.size,
            decision.avg_price,
            decision.detected_at,
            decision.terminal,
        )
    }
}

/// Enqueue a hedge intent without dropping exposure on backpressure.
///
/// If the low-latency queue is full or closed, the intent is appended to a
/// local JSONL file and maker placement is halted via Reconciling/Error state.
pub async fn enqueue_hedge_intent(
    hedge_tx: &mpsc::Sender<HedgeIntent>,
    bot_state: &Arc<RwLock<BotState>>,
    intent: HedgeIntent,
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
