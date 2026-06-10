pub mod cancel_manager;
pub mod fill_aggregator;
pub mod fill_dedup;
/// Service modules - each task runs in its own service.
pub mod fill_detection;
pub mod hedge;
pub mod hedge_store;
pub mod metrics;
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
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

use crate::bot::BotState;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::strategy::OrderSide;

/// Signed per-venue positions for `symbol`: `(pacifica, hyperliquid)`.
///
/// Pacifica reports unsigned amounts with a side ("bid" = long, "ask" = short);
/// Hyperliquid's `szi` is already signed. Shared by the reconciler, safety
/// monitor, hedge executor, and shutdown path so every exposure decision uses
/// the same sign convention.
pub async fn signed_positions(
    pacifica: &PacificaTrading,
    hyperliquid: &HyperliquidTrading,
    symbol: &str,
) -> anyhow::Result<(f64, f64)> {
    let pacifica_positions = pacifica.get_positions().await?;
    let pacifica_position = pacifica_positions
        .iter()
        .find(|position| position.symbol == symbol)
        .map(|position| {
            let amount: f64 = fast_float::parse(&position.amount).unwrap_or(0.0);
            match position.side.as_str() {
                "bid" => amount,
                "ask" => -amount,
                _ => 0.0,
            }
        })
        .unwrap_or(0.0);

    let user_state = hyperliquid
        .get_user_state(&hyperliquid.account_address())
        .await?;
    let hyperliquid_position = user_state
        .asset_positions
        .iter()
        .find(|ap| ap.position.coin == symbol)
        .map(|ap| fast_float::parse(&ap.position.szi).unwrap_or(0.0))
        .unwrap_or(0.0);

    Ok((pacifica_position, hyperliquid_position))
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HedgeEnqueueResult {
    Queued,
    PersistedButNotQueued { reason: String },
}

impl HedgeEnqueueResult {
    #[inline]
    pub fn queued(&self) -> bool {
        matches!(self, Self::Queued)
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
) -> anyhow::Result<HedgeEnqueueResult> {
    // Journaling is best-effort: a saturated/stalled lifecycle log must NOT abort
    // the hedge. The authoritative exposure decision is the hedge channel state
    // below, not whether the Created record was persisted.
    hedge_store::try_append_lifecycle_update(hedge_store::HedgeLifecycleUpdate::new(
        &intent,
        hedge_store::HedgeIntentStatus::Created,
    ))
    .await;

    match hedge_tx.try_send(intent.clone()) {
        Ok(()) => {
            hedge_store::try_append_lifecycle_update(hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::Queued,
            ))
            .await;
            Ok(HedgeEnqueueResult::Queued)
        }
        Err(mpsc::error::TrySendError::Full(intent)) => {
            metrics::risk_metrics()
                .queue_full_count
                .fetch_add(1, Ordering::AcqRel);
            let mut update = hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::QueueFull,
            );
            update.reason = Some("hedge queue full".to_string());
            hedge_store::try_append_lifecycle_update(update).await;
            let mut state = bot_state.write();
            state.mark_reconciling();
            Ok(HedgeEnqueueResult::PersistedButNotQueued {
                reason: "hedge queue full".to_string(),
            })
        }
        Err(mpsc::error::TrySendError::Closed(intent)) => {
            let mut update = hedge_store::HedgeLifecycleUpdate::new(
                &intent,
                hedge_store::HedgeIntentStatus::QueueClosed,
            );
            update.reason = Some("hedge queue closed".to_string());
            hedge_store::try_append_lifecycle_update(update).await;
            let mut state = bot_state.write();
            state.set_error("hedge queue closed; intent persisted".to_string());
            Ok(HedgeEnqueueResult::PersistedButNotQueued {
                reason: "hedge queue closed".to_string(),
            })
        }
    }
}
