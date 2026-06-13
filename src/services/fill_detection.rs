use colored::Colorize;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::services::cancel_manager::{request_cancel, CancelDemand, CancelIntent, CancelReason};
use crate::services::fill_aggregator::{FillAggregator, HedgeReservation};
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::maker::{
    MakerBaselineUpdater, MakerExchange, MakerFillEvent, MakerFillStream, MakerReconcileHook,
};
use crate::services::metrics;
use crate::services::supervisor::spawn_supervised_fail_closed;
use crate::services::trade_gate::TradeGate;
use crate::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use crate::util::log::{tag_static, Color};

/// WebSocket-based fill detection service (primary fill detection method).
pub struct FillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub cancel_tx: mpsc::Sender<CancelIntent>,
    pub cancel_demand: Arc<CancelDemand>,
    /// Maker handle used by the reconnect reconcile hook (REST open-order scan).
    pub maker: Arc<dyn MakerExchange>,
    pub fill_stream: Box<dyn MakerFillStream>,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub baseline_updater: Arc<dyn MakerBaselineUpdater>,
    pub atomic_status: Arc<std::sync::atomic::AtomicU8>,
    pub order_snapshot: Arc<crate::services::order_monitor::SharedOrderSnapshot>,
    pub trade_gate: Arc<TradeGate>,
    pub low_latency_mode: bool,
}

/// Single ordered consumer for fill events.
///
/// The WS read callback does nothing but push the event into an unbounded
/// channel (unbounded deliberately: events are rate-limited by real order
/// activity and dropping one risks a missed hedge); this processor handles
/// them strictly in arrival order. That replaces the previous
/// tokio::spawn-per-event design, which paid a scheduling hop per fill AND
/// could reorder a Cancelled event's state cleanup ahead of its own
/// residual-fill check (the cleanup clearing `active_order` made the residual
/// path's `is_our_order` test fail).
struct FillEventProcessor {
    bot_state: Arc<RwLock<BotState>>,
    hedge_tx: mpsc::Sender<HedgeIntent>,
    cancel_tx: mpsc::Sender<CancelIntent>,
    cancel_demand: Arc<CancelDemand>,
    symbol: String,
    processed_fills: Arc<FillDedup>,
    fill_aggregator: Arc<FillAggregator>,
    baseline_updater: Arc<dyn MakerBaselineUpdater>,
    atomic_status: Arc<std::sync::atomic::AtomicU8>,
    order_snapshot: Arc<crate::services::order_monitor::SharedOrderSnapshot>,
    low_latency_mode: bool,
}

impl FillEventProcessor {
    /// True when `cloid` matches the currently tracked active order.
    fn is_our_order(&self, client_order_id: &Option<String>) -> bool {
        self.bot_state
            .read()
            .active_order
            .as_ref()
            .and_then(|order| {
                client_order_id
                    .as_ref()
                    .map(|cloid| order.client_order_id == *cloid)
            })
            .unwrap_or(false)
    }

    async fn process(&self, fill_event: MakerFillEvent) {
        match fill_event {
            MakerFillEvent::Full {
                order_id,
                symbol: fill_symbol,
                side,
                filled,
                avg_price,
                client_order_id,
                ..
            } => {
                let fill_detect_start = std::time::Instant::now();
                if !self.low_latency_mode {
                    info!(
                        "{} {} FULL FILL: {} {} {} @ {} (cloid: {})",
                        tag_static("FILL_DETECTION", Color::Magenta),
                        "OK".green().bold(),
                        side.as_str().bright_yellow(),
                        filled.to_string().bright_white(),
                        fill_symbol.bright_white().bold(),
                        avg_price.to_string().cyan(),
                        client_order_id.as_deref().unwrap_or("None")
                    );
                }

                if !self.is_our_order(&client_order_id) {
                    return;
                }

                let order_side = side;
                let filled_size = filled;
                let avg_px = avg_price;

                if !self
                    .fill_aggregator
                    .observe_fill(order_id, order_side, filled_size, avg_px, true)
                {
                    debug!(
                        "[FILL_DETECTION] Full fill already accounted for (order_id={})",
                        order_id
                    );
                    return;
                }

                let dedup_cloid = client_order_id
                    .clone()
                    .unwrap_or_else(|| order_id.to_string());
                if !self
                    .processed_fills
                    .insert_if_new(FillKey::from_cloid_cumulative(dedup_cloid, filled_size))
                {
                    debug!(
                        "[FILL_DETECTION] Full fill already processed (order_id={}, cloid={:?})",
                        order_id, client_order_id
                    );
                    return;
                }

                let Some(reservation) = self.fill_aggregator.try_reserve_hedge(order_id) else {
                    debug!(
                        "[FILL_DETECTION] Full fill residual no longer reservable (order_id={})",
                        order_id
                    );
                    return;
                };

                {
                    let mut state = self.bot_state.write();
                    state.mark_filled(reservation.size, order_side);
                }

                request_cancel(
                    &self.cancel_tx,
                    &self.cancel_demand,
                    self.symbol.clone(),
                    CancelReason::PartialFill,
                );
                self.baseline_updater
                    .update_baseline(&self.symbol, order_side, filled_size, avg_px);

                if !self.low_latency_mode {
                    info!(
                        "{} {} Hedge triggered in {:.1}ms",
                        format!("[{}]", self.symbol).bright_white().bold(),
                        "Order filled".green().bold(),
                        fill_detect_start.elapsed().as_secs_f64() * 1000.0
                    );
                }

                enqueue_reserved_hedge(
                    &self.fill_aggregator,
                    &self.hedge_tx,
                    &self.bot_state,
                    reservation,
                    "full fill",
                )
                .await;
            }
            MakerFillEvent::Partial {
                order_id,
                symbol: fill_symbol,
                side,
                filled,
                original,
                avg_price,
                client_order_id,
                ..
            } => {
                let filled_size = filled;
                let fill_price = avg_price;
                let notional_value = filled_size * fill_price;
                if !self.low_latency_mode {
                    info!(
                        "{} {} PARTIAL FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {}",
                        tag_static("FILL_DETECTION", Color::Magenta),
                        "FAST".yellow().bold(),
                        side.as_str().bright_yellow(),
                        filled.to_string().bright_white(),
                        fill_symbol.bright_white().bold(),
                        avg_price.to_string().cyan(),
                        filled.to_string().bright_white(),
                        original,
                        format!("${:.2}", notional_value).cyan().bold()
                    );
                }

                if !self.is_our_order(&client_order_id) {
                    return;
                }

                let order_side = side;
                let target_size = original;
                if !self.fill_aggregator.observe_fill_with_target(
                    order_id,
                    order_side,
                    filled_size,
                    fill_price,
                    false,
                    Some(target_size),
                ) {
                    debug!(
                        "[FILL_DETECTION] Partial fill accumulated (order_id={}, cumulative=${:.2})",
                        order_id, notional_value
                    );
                    return;
                }

                let dedup_cloid = client_order_id
                    .clone()
                    .unwrap_or_else(|| order_id.to_string());
                if !self
                    .processed_fills
                    .insert_if_new(FillKey::from_cloid_cumulative(dedup_cloid, filled_size))
                {
                    debug!(
                        "[FILL_DETECTION] Partial fill already processed (order_id={}, cloid={:?})",
                        order_id, client_order_id
                    );
                    return;
                }

                let Some(reservation) = self.fill_aggregator.try_reserve_hedge(order_id) else {
                    return;
                };
                warn!(
                    "{} {} Partial-fill threshold reached (order_id={}): hedging {} @ ${:.4}",
                    tag_static("FILL_DETECTION", Color::Magenta),
                    "WARN".yellow().bold(),
                    order_id,
                    reservation.size,
                    reservation.avg_price
                );

                {
                    let mut state = self.bot_state.write();
                    state.mark_filled(reservation.size, reservation.side);
                }

                request_cancel(
                    &self.cancel_tx,
                    &self.cancel_demand,
                    self.symbol.clone(),
                    CancelReason::PartialFill,
                );
                self.baseline_updater.update_baseline(
                    &self.symbol,
                    order_side,
                    reservation.size,
                    reservation.avg_price,
                );

                enqueue_reserved_hedge(
                    &self.fill_aggregator,
                    &self.hedge_tx,
                    &self.bot_state,
                    reservation,
                    "partial fill",
                )
                .await;
            }
            MakerFillEvent::Cancelled {
                order_id,
                client_order_id,
                side,
                filled,
                reason,
                ..
            } => {
                debug!(
                    "[FILL_DETECTION] Order cancelled: {} (reason: {}, filled_amount: {})",
                    client_order_id.as_deref().unwrap_or("None"),
                    reason,
                    filled
                );

                // Residual-fill check FIRST, state cleanup second: the cleanup
                // clears `active_order`, which would make the residual path's
                // is_our_order test fail if it ran first (the old per-event
                // spawns raced exactly that way).
                let filled_so_far = filled;
                if filled_so_far > 0.0 && self.is_our_order(&client_order_id) {
                    if self
                        .fill_aggregator
                        .observe_fill(order_id, side, filled_so_far, 0.0, true)
                    {
                        let dedup_key = client_order_id
                            .clone()
                            .map(|cloid| FillKey::from_cloid_cumulative(cloid, filled_so_far))
                            .unwrap_or_else(|| {
                                FillKey::from_order_cumulative(order_id, filled_so_far)
                            });
                        if self.processed_fills.insert_if_new(dedup_key) {
                            if let Some(reservation) =
                                self.fill_aggregator.try_reserve_hedge(order_id)
                            {
                                warn!(
                                    "[FILL_DETECTION] Cancellation left {} filled on order_id={}, emitting hedge",
                                    reservation.size, order_id
                                );
                                enqueue_reserved_hedge(
                                    &self.fill_aggregator,
                                    &self.hedge_tx,
                                    &self.bot_state,
                                    reservation,
                                    "cancel residual",
                                )
                                .await;
                            }
                        }
                    }
                }

                // State cleanup (previously its own unordered spawn).
                {
                    let mut state = self.bot_state.write();
                    let is_our_order = state
                        .active_order
                        .as_ref()
                        .and_then(|order| {
                            client_order_id
                                .as_ref()
                                .map(|id| order.client_order_id == *id)
                        })
                        .unwrap_or(false);

                    if !is_our_order {
                        return;
                    }

                    match &state.status {
                        BotStatus::OrderPlaced | BotStatus::Cancelling => {
                            state.clear_active_order();
                            crate::services::order_monitor::sync_atomic_status(
                                &self.atomic_status,
                                &state.status,
                            );
                            self.order_snapshot.set(None);
                            debug!("[BOT] Active order cancelled, returning to Idle");
                        }
                        BotStatus::Filled | BotStatus::Hedging | BotStatus::Complete => {
                            debug!(
                                "[BOT] Cancellation confirmed for {:?}; hedge path owns state",
                                state.status
                            );
                        }
                        BotStatus::Idle
                        | BotStatus::Error(_)
                        | BotStatus::Placing
                        | BotStatus::Reconciling
                        | BotStatus::PlacementUnknown
                        | BotStatus::CancelPending
                        | BotStatus::HedgeUnknown
                        | BotStatus::ShuttingDown => {
                            debug!(
                                "[BOT] Cancellation received in {:?} state (ignoring)",
                                state.status
                            );
                        }
                    }
                }
            }
            MakerFillEvent::Position { .. } => {
                debug!("[FILL_DETECTION] Position fill event handled by position monitor");
            }
        }
    }
}

impl FillDetectionService {
    pub async fn run(mut self) {
        info!(
            "{} Starting fill detection client",
            tag_static("FILL_DETECTION", Color::Magenta)
        );

        let fill_stream = &mut self.fill_stream;

        {
            let maker_for_hook = self.maker.clone();
            let symbol_for_hook = self.symbol.clone();
            let aggregator_for_hook = self.fill_aggregator.clone();
            let processed_for_hook = self.processed_fills.clone();
            let hedge_tx_for_hook = self.hedge_tx.clone();
            let bot_state_for_hook = self.bot_state.clone();

            let hook: MakerReconcileHook = Arc::new(move || {
                let maker = maker_for_hook.clone();
                let symbol = symbol_for_hook.clone();
                let aggregator = aggregator_for_hook.clone();
                let processed = processed_for_hook.clone();
                let hedge_tx = hedge_tx_for_hook.clone();
                let bot_state = bot_state_for_hook.clone();
                Box::pin(async move {
                    let active_cloid = bot_state
                        .read()
                        .active_order
                        .as_ref()
                        .map(|order| order.client_order_id.clone());
                    match maker.open_orders().await {
                        Ok(orders) => {
                            for order in orders.into_iter().filter(|o| o.symbol == symbol) {
                                if active_cloid
                                    .as_ref()
                                    .map(|cloid| &order.client_order_id != cloid)
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let filled = order.filled_amount;
                                if filled <= 0.0 {
                                    continue;
                                }
                                let initial = order.initial_amount;
                                let price = order.price;
                                let is_terminal = (filled - initial).abs() < 1e-9;
                                let side = order.side;

                                if !aggregator.observe_fill_with_target(
                                    order.order_id,
                                    side,
                                    filled,
                                    price,
                                    is_terminal,
                                    Some(initial),
                                ) {
                                    continue;
                                }

                                if !processed.insert_if_new(FillKey::from_cloid_cumulative(
                                    order.client_order_id.clone(),
                                    filled,
                                )) {
                                    continue;
                                }

                                let Some(reservation) =
                                    aggregator.try_reserve_hedge(order.order_id)
                                else {
                                    continue;
                                };
                                warn!(
                                    "[FILL_DETECTION] Reconnect reconcile emitted missed fill (order_id={}, size={}, price=${:.4})",
                                    order.order_id, reservation.size, reservation.avg_price
                                );
                                enqueue_reserved_hedge(
                                    &aggregator,
                                    &hedge_tx,
                                    &bot_state,
                                    reservation,
                                    "reconnected fill",
                                )
                                .await;
                            }
                        }
                        Err(e) => warn!("[FILL_DETECTION] Reconcile REST call failed: {}", e),
                    }
                })
            });
            fill_stream.set_reconcile_hook(hook);
        }

        // Single ordered consumer (see FillEventProcessor docs). Unbounded
        // channel: a dropped fill event risks a missed hedge, and real order
        // flow bounds the rate.
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<MakerFillEvent>();
        let processor = FillEventProcessor {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            cancel_tx: self.cancel_tx.clone(),
            cancel_demand: self.cancel_demand.clone(),
            symbol: self.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            baseline_updater: self.baseline_updater.clone(),
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
            low_latency_mode: self.low_latency_mode,
        };
        // Fail-closed: the processor owns the (non-clonable) event receiver; if
        // it dies, fills are no longer turned into hedges, so quoting must halt.
        spawn_supervised_fail_closed(
            "fill_event_processor",
            self.trade_gate.clone(),
            async move {
                while let Some(event) = event_rx.recv().await {
                    processor.process(event).await;
                }
            },
        );

        // The WS read callback does nothing but forward: no locks, no parsing,
        // no spawned task per event.
        fill_stream
            .run_with(Box::new(move |fill_event| {
                let _ = event_tx.send(fill_event);
            }))
            .await
            .ok();
    }
}

async fn enqueue_reserved_hedge(
    aggregator: &FillAggregator,
    hedge_tx: &mpsc::Sender<HedgeIntent>,
    bot_state: &Arc<RwLock<BotState>>,
    reservation: HedgeReservation,
    context: &str,
) -> bool {
    let intent: HedgeIntent = reservation.into();
    match enqueue_hedge_intent(hedge_tx, bot_state, intent).await {
        Ok(HedgeEnqueueResult::Queued) => {
            metrics::risk_metrics()
                .fill_detect_to_hedge_enqueue_us
                .store(
                    reservation
                        .detected_at
                        .elapsed()
                        .as_micros()
                        .min(u64::MAX as u128) as u64,
                    std::sync::atomic::Ordering::Release,
                );
            aggregator.commit_queued(reservation);
            true
        }
        Ok(HedgeEnqueueResult::PersistedButNotQueued { reason }) => {
            aggregator.release_reservation(reservation);
            warn!(
                "[FILL_DETECTION] Hedge not queued for {}; released reservation: {}",
                context, reason
            );
            false
        }
        Err(e) => {
            aggregator.release_reservation(reservation);
            warn!(
                "[FILL_DETECTION] Hedge enqueue failed for {}; released reservation: {}",
                context, e
            );
            false
        }
    }
}
