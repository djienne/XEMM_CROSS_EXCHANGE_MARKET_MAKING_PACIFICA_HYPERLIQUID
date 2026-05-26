use colored::Colorize;
use fast_float::parse;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::{
    FillDetectionClient, FillEvent, PacificaTrading, PositionBaselineUpdater,
};
use crate::services::cancel_manager::{request_cancel, CancelDemand, CancelIntent, CancelReason};
use crate::services::fill_aggregator::{FillAggregator, HedgeReservation};
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::metrics;
use crate::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use crate::strategy::OrderSide;
use crate::util::log::{tag_static, Color};

/// WebSocket-based fill detection service (primary fill detection method).
pub struct FillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub cancel_tx: mpsc::Sender<CancelIntent>,
    pub cancel_demand: Arc<CancelDemand>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub fill_client: FillDetectionClient,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub baseline_updater: PositionBaselineUpdater,
    pub atomic_status: Arc<std::sync::atomic::AtomicU8>,
    pub order_snapshot: Arc<crate::services::order_monitor::SharedOrderSnapshot>,
    pub low_latency_mode: bool,
}

impl FillDetectionService {
    pub async fn run(mut self) {
        info!(
            "{} Starting fill detection client",
            tag_static("FILL_DETECTION", Color::Magenta)
        );

        let fill_client = &mut self.fill_client;

        {
            let pac_for_hook = self.pacifica_trading.clone();
            let symbol_for_hook = self.symbol.clone();
            let aggregator_for_hook = self.fill_aggregator.clone();
            let processed_for_hook = self.processed_fills.clone();
            let hedge_tx_for_hook = self.hedge_tx.clone();
            let bot_state_for_hook = self.bot_state.clone();

            let hook: crate::connector::pacifica::ReconcileHook = Arc::new(move || {
                let pac = pac_for_hook.clone();
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
                    match pac.get_open_orders().await {
                        Ok(orders) => {
                            for order in orders.into_iter().filter(|o| o.symbol == symbol) {
                                if active_cloid
                                    .as_ref()
                                    .map(|cloid| &order.client_order_id != cloid)
                                    .unwrap_or(false)
                                {
                                    continue;
                                }

                                let filled: f64 = parse(&order.filled_amount).unwrap_or(0.0);
                                if filled <= 0.0 {
                                    continue;
                                }
                                let initial: f64 = parse(&order.initial_amount).unwrap_or(0.0);
                                let price: f64 = parse(&order.price).unwrap_or(0.0);
                                let is_terminal = (filled - initial).abs() < 1e-9;
                                let Some(side) = order_side_from_str(&order.side) else {
                                    continue;
                                };

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
            fill_client.set_reconcile_hook(hook);
        }

        let bot_state = self.bot_state.clone();
        let hedge_tx = self.hedge_tx.clone();
        let cancel_tx = self.cancel_tx.clone();
        let cancel_demand = self.cancel_demand.clone();
        let symbol = self.symbol.clone();
        let processed_fills = self.processed_fills.clone();
        let fill_aggregator = self.fill_aggregator.clone();
        let baseline_updater = self.baseline_updater.clone();
        let atomic_status = self.atomic_status.clone();
        let order_snapshot = self.order_snapshot.clone();
        let low_latency_mode = self.low_latency_mode;

        fill_client
            .start(move |fill_event| match fill_event {
                FillEvent::FullFill {
                    order_id,
                    symbol: fill_symbol,
                    side,
                    filled_amount,
                    avg_price,
                    client_order_id,
                    ..
                } => {
                    if !low_latency_mode {
                        info!(
                            "{} {} FULL FILL: {} {} {} @ {} (cloid: {})",
                            tag_static("FILL_DETECTION", Color::Magenta),
                            "OK".green().bold(),
                            side.bright_yellow(),
                            filled_amount.bright_white(),
                            fill_symbol.bright_white().bold(),
                            avg_price.cyan(),
                            client_order_id.as_deref().unwrap_or("None")
                        );
                    }

                    let bot_state = bot_state.clone();
                    let hedge_tx = hedge_tx.clone();
                    let cancel_tx = cancel_tx.clone();
                    let cancel_demand = cancel_demand.clone();
                    let processed_fills = processed_fills.clone();
                    let fill_aggregator = fill_aggregator.clone();
                    let baseline_updater = baseline_updater.clone();
                    let symbol = symbol.clone();

                    tokio::spawn(async move {
                        let fill_detect_start = std::time::Instant::now();
                        let is_our_order = bot_state
                            .read()
                            .active_order
                            .as_ref()
                            .and_then(|order| {
                                client_order_id
                                    .as_ref()
                                    .map(|cloid| order.client_order_id == *cloid)
                            })
                            .unwrap_or(false);
                        if !is_our_order {
                            return;
                        }

                        let Some(order_side) = order_side_from_str(&side) else {
                            error!(
                                "{} {} Unknown side: {}",
                                tag_static("FILL_DETECTION", Color::Magenta),
                                "FAIL".red().bold(),
                                side
                            );
                            return;
                        };
                        let filled_size: f64 = parse(&filled_amount).unwrap_or(0.0);
                        let avg_px: f64 = parse(&avg_price).unwrap_or(0.0);

                        if !fill_aggregator.observe_fill(
                            order_id,
                            order_side,
                            filled_size,
                            avg_px,
                            true,
                        ) {
                            debug!(
                                "[FILL_DETECTION] Full fill already accounted for (order_id={})",
                                order_id
                            );
                            return;
                        }

                        let dedup_cloid = client_order_id
                            .clone()
                            .unwrap_or_else(|| order_id.to_string());
                        if !processed_fills
                            .insert_if_new(FillKey::from_cloid_cumulative(dedup_cloid, filled_size))
                        {
                            debug!(
                                "[FILL_DETECTION] Full fill already processed (order_id={}, cloid={:?})",
                                order_id, client_order_id
                            );
                            return;
                        }

                        let Some(reservation) = fill_aggregator.try_reserve_hedge(order_id) else {
                            debug!(
                                "[FILL_DETECTION] Full fill residual no longer reservable (order_id={})",
                                order_id
                            );
                            return;
                        };

                        {
                            let mut state = bot_state.write();
                            state.mark_filled(reservation.size, order_side);
                        }

                        request_cancel(
                            &cancel_tx,
                            &cancel_demand,
                            symbol.clone(),
                            CancelReason::PartialFill,
                        );
                        baseline_updater.update_baseline(&symbol, &side, filled_size, avg_px);

                        if !low_latency_mode {
                            info!(
                                "{} {} Hedge triggered in {:.1}ms",
                                format!("[{}]", symbol).bright_white().bold(),
                                "Order filled".green().bold(),
                                fill_detect_start.elapsed().as_secs_f64() * 1000.0
                            );
                        }

                        enqueue_reserved_hedge(
                            &fill_aggregator,
                            &hedge_tx,
                            &bot_state,
                            reservation,
                            "full fill",
                        )
                        .await;
                    });
                }
                FillEvent::PartialFill {
                    order_id,
                    symbol: fill_symbol,
                    side,
                    filled_amount,
                    original_amount,
                    avg_price,
                    client_order_id,
                    ..
                } => {
                    let filled_size: f64 = parse(&filled_amount).unwrap_or(0.0);
                    let fill_price: f64 = parse(&avg_price).unwrap_or(0.0);
                    let notional_value = filled_size * fill_price;
                    if !low_latency_mode {
                        info!(
                            "{} {} PARTIAL FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {}",
                            tag_static("FILL_DETECTION", Color::Magenta),
                            "FAST".yellow().bold(),
                            side.bright_yellow(),
                            filled_amount.bright_white(),
                            fill_symbol.bright_white().bold(),
                            avg_price.cyan(),
                            filled_amount.bright_white(),
                            original_amount,
                            format!("${:.2}", notional_value).cyan().bold()
                        );
                    }

                    let bot_state = bot_state.clone();
                    let hedge_tx = hedge_tx.clone();
                    let cancel_tx = cancel_tx.clone();
                    let cancel_demand = cancel_demand.clone();
                    let processed_fills = processed_fills.clone();
                    let fill_aggregator = fill_aggregator.clone();
                    let baseline_updater = baseline_updater.clone();
                    let symbol = symbol.clone();

                    tokio::spawn(async move {
                        let is_our_order = bot_state
                            .read()
                            .active_order
                            .as_ref()
                            .and_then(|order| {
                                client_order_id
                                    .as_ref()
                                    .map(|cloid| order.client_order_id == *cloid)
                            })
                            .unwrap_or(false);
                        if !is_our_order {
                            return;
                        }

                        let Some(order_side) = order_side_from_str(&side) else {
                            error!(
                                "{} {} Unknown side: {}",
                                tag_static("FILL_DETECTION", Color::Magenta),
                                "FAIL".red().bold(),
                                side
                            );
                            return;
                        };
                        let target_size: f64 = parse(&original_amount).unwrap_or(0.0);
                        if !fill_aggregator.observe_fill_with_target(
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
                        if !processed_fills
                            .insert_if_new(FillKey::from_cloid_cumulative(dedup_cloid, filled_size))
                        {
                            debug!(
                                "[FILL_DETECTION] Partial fill already processed (order_id={}, cloid={:?})",
                                order_id, client_order_id
                            );
                            return;
                        }

                        let Some(reservation) = fill_aggregator.try_reserve_hedge(order_id) else {
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
                            let mut state = bot_state.write();
                            state.mark_filled(reservation.size, reservation.side);
                        }

                        request_cancel(
                            &cancel_tx,
                            &cancel_demand,
                            symbol.clone(),
                            CancelReason::PartialFill,
                        );
                        baseline_updater.update_baseline(
                            &symbol,
                            &side,
                            reservation.size,
                            reservation.avg_price,
                        );

                        enqueue_reserved_hedge(
                            &fill_aggregator,
                            &hedge_tx,
                            &bot_state,
                            reservation,
                            "partial fill",
                        )
                        .await;
                    });
                }
                FillEvent::Cancelled {
                    order_id,
                    client_order_id,
                    side,
                    filled_amount,
                    reason,
                    ..
                } => {
                    debug!(
                        "[FILL_DETECTION] Order cancelled: {} (reason: {}, filled_amount: {})",
                        client_order_id.as_deref().unwrap_or("None"),
                        reason,
                        filled_amount
                    );

                    let bot_state_for_residual = bot_state.clone();
                    let hedge_tx_for_residual = hedge_tx.clone();
                    let processed_for_residual = processed_fills.clone();
                    let aggregator_for_residual = fill_aggregator.clone();
                    let residual_client_order_id = client_order_id.clone();
                    tokio::spawn(async move {
                        let filled_so_far: f64 = parse(&filled_amount).unwrap_or(0.0);
                        if filled_so_far <= 0.0 {
                            return;
                        }

                        let is_our_order = bot_state_for_residual
                            .read()
                            .active_order
                            .as_ref()
                            .and_then(|order| {
                                residual_client_order_id
                                    .as_ref()
                                    .map(|cloid| order.client_order_id == *cloid)
                            })
                            .unwrap_or(false);
                        if !is_our_order {
                            return;
                        }

                        let Some(order_side) = order_side_from_str(&side) else {
                            return;
                        };
                        if !aggregator_for_residual.observe_fill(
                            order_id,
                            order_side,
                            filled_so_far,
                            0.0,
                            true,
                        ) {
                            return;
                        }

                        let dedup_key = residual_client_order_id
                            .clone()
                            .map(|cloid| FillKey::from_cloid_cumulative(cloid, filled_so_far))
                            .unwrap_or_else(|| {
                                FillKey::from_order_cumulative(order_id, filled_so_far)
                            });
                        if !processed_for_residual.insert_if_new(dedup_key) {
                            return;
                        }

                        let Some(reservation) =
                            aggregator_for_residual.try_reserve_hedge(order_id)
                        else {
                            return;
                        };
                        warn!(
                            "[FILL_DETECTION] Cancellation left {} filled on order_id={}, emitting hedge",
                            reservation.size, order_id
                        );
                        enqueue_reserved_hedge(
                            &aggregator_for_residual,
                            &hedge_tx_for_residual,
                            &bot_state_for_residual,
                            reservation,
                            "cancel residual",
                        )
                        .await;
                    });

                    let bot_state = bot_state.clone();
                    let cloid = client_order_id.clone();
                    let atomic_status = atomic_status.clone();
                    let order_snapshot = order_snapshot.clone();
                    tokio::spawn(async move {
                        let mut state = bot_state.write();
                        let is_our_order = state
                            .active_order
                            .as_ref()
                            .and_then(|order| cloid.as_ref().map(|id| order.client_order_id == *id))
                            .unwrap_or(false);

                        if !is_our_order {
                            return;
                        }

                        match &state.status {
                            BotStatus::OrderPlaced | BotStatus::Cancelling => {
                                state.clear_active_order();
                                crate::services::order_monitor::sync_atomic_status(
                                    &atomic_status,
                                    &state.status,
                                );
                                order_snapshot.set(None);
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
                    });
                }
                FillEvent::PositionFill { .. } => {
                    debug!("[FILL_DETECTION] Position fill event handled by position monitor");
                }
            })
            .await
            .ok();
    }
}

fn order_side_from_str(side: &str) -> Option<OrderSide> {
    match side {
        "buy" | "bid" => Some(OrderSide::Buy),
        "sell" | "ask" => Some(OrderSide::Sell),
        _ => None,
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
