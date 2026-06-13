use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use colored::Colorize;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::services::cancel_manager::{request_cancel, CancelDemand, CancelIntent, CancelReason};
use crate::services::fill_aggregator::{FillAggregator, HedgeReservation};
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::maker::{MakerExchange, MakerOpenOrder};
use crate::services::metrics;
use crate::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use crate::strategy::OrderSide;
use crate::util::log::{tag_static, Color};
use crate::util::rate_limit::is_rate_limit_error;

#[derive(Debug, Clone, Copy)]
struct RestFillState {
    last_cumulative: f64,
    initial_amount: f64,
    order_id: u64,
    side: OrderSide,
    last_seen_at: Instant,
}

/// REST API fill detection service (backup/fallback method).
pub struct RestFillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub cancel_tx: mpsc::Sender<CancelIntent>,
    pub cancel_demand: Arc<CancelDemand>,
    pub maker: Arc<dyn MakerExchange>,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub min_hedge_notional: f64,
    pub poll_interval_ms: u64,
    pub low_latency_mode: bool,
}

impl RestFillDetectionService {
    pub async fn run(self) {
        let mut consecutive_errors = 0u32;
        let mut per_order: HashMap<String, RestFillState> = HashMap::new();

        loop {
            let active_order = {
                let state = self.bot_state.read();
                if matches!(state.status, BotStatus::Complete | BotStatus::Error(_)) {
                    None
                } else {
                    state.active_order.as_ref().map(|order| {
                        (
                            order.client_order_id.clone(),
                            order.order_id,
                            order.side,
                            order.size,
                        )
                    })
                }
            };

            let poll_ms = if active_order.is_some() {
                self.poll_interval_ms
            } else {
                1000
            };
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;

            let Some((client_order_id, order_id_opt, order_side, order_size)) = active_order else {
                per_order.retain(|_, state| state.last_seen_at.elapsed() < Duration::from_secs(60));
                continue;
            };

            match self.maker.open_orders().await {
                Ok(orders) => {
                    consecutive_errors = 0;
                    if let Some(order) = orders
                        .iter()
                        .find(|order| order.client_order_id == client_order_id)
                    {
                        self.process_open_order(order, &mut per_order).await;
                        continue;
                    }

                    debug!(
                        "[REST_FILL_DETECTION] Active order {} missing from open_orders; checking trade history",
                        client_order_id
                    );
                    match self
                        .recover_disappeared_order(
                            &client_order_id,
                            order_id_opt,
                            order_side,
                            order_size,
                            &mut per_order,
                        )
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            warn!(
                                "[REST_FILL_DETECTION] Active order {} disappeared without open-order or trade-history confirmation; entering reconciliation",
                                client_order_id
                            );
                            let mut state = self.bot_state.write();
                            if matches!(
                                state.status,
                                BotStatus::OrderPlaced
                                    | BotStatus::Placing
                                    | BotStatus::CancelPending
                            ) {
                                state.mark_reconciling();
                            }
                        }
                        Err(e) => {
                            warn!(
                                "[REST_FILL_DETECTION] Trade-history recovery failed for {}: {}",
                                client_order_id, e
                            );
                        }
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if is_rate_limit_error(&e) {
                        let backoff_secs = std::cmp::min(2u64.pow(consecutive_errors - 1), 32);
                        warn!(
                            "{} Rate limit hit, backing off for {} seconds",
                            tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                            backoff_secs
                        );
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    } else {
                        debug!(
                            "[REST_FILL_DETECTION] Error fetching open orders (attempt {}): {}",
                            consecutive_errors, e
                        );
                        if consecutive_errors >= 5 {
                            warn!(
                                "{} {} consecutive errors fetching open orders",
                                tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                                consecutive_errors
                            );
                        }
                    }
                }
            }
        }
    }

    async fn process_open_order(
        &self,
        order: &MakerOpenOrder,
        per_order: &mut HashMap<String, RestFillState>,
    ) {
        // MakerOpenOrder is pre-typed by the adapter (f64 amounts/price, OrderSide
        // side); an unrecognized venue side was already dropped at the boundary.
        let filled_amount = order.filled_amount;
        let initial_amount = order.initial_amount;
        let price = order.price;
        let order_side = order.side;

        let state = per_order
            .entry(order.client_order_id.clone())
            .or_insert(RestFillState {
                last_cumulative: 0.0,
                initial_amount,
                order_id: order.order_id,
                side: order_side,
                last_seen_at: Instant::now(),
            });
        state.initial_amount = state.initial_amount.max(initial_amount);
        state.order_id = order.order_id;
        state.side = order_side;
        state.last_seen_at = Instant::now();

        if filled_amount <= state.last_cumulative || filled_amount <= 0.0 {
            return;
        }

        let delta = filled_amount - state.last_cumulative;
        state.last_cumulative = filled_amount;
        let is_full_fill = (filled_amount - initial_amount).abs() < 0.0001;
        self.process_cumulative_fill(
            order.order_id,
            order.client_order_id.clone(),
            order_side,
            filled_amount,
            initial_amount,
            price,
            is_full_fill,
            delta,
        )
        .await;
    }

    async fn recover_disappeared_order(
        &self,
        client_order_id: &str,
        order_id_opt: Option<u64>,
        order_side: OrderSide,
        order_size: f64,
        per_order: &mut HashMap<String, RestFillState>,
    ) -> anyhow::Result<bool> {
        let trades = self.maker.recent_trades(&self.symbol, 100).await?;

        let mut cumulative = 0.0;
        let mut notional = 0.0;
        let mut order_id = order_id_opt.unwrap_or(0);
        for trade in trades
            .iter()
            .filter(|trade| trade.client_order_id.as_deref() == Some(client_order_id))
        {
            let qty = trade.amount;
            let price = trade.entry_price;
            if qty <= 0.0 {
                continue;
            }
            cumulative += qty;
            notional += qty * price.max(0.0);
            order_id = trade.order_id;
        }

        if cumulative <= 0.0 {
            return Ok(false);
        }

        let avg_price = if notional > 0.0 {
            notional / cumulative
        } else {
            0.0
        };
        let entry = per_order
            .entry(client_order_id.to_string())
            .or_insert(RestFillState {
                last_cumulative: 0.0,
                initial_amount: order_size,
                order_id,
                side: order_side,
                last_seen_at: Instant::now(),
            });
        if cumulative <= entry.last_cumulative {
            return Ok(true);
        }
        let delta = cumulative - entry.last_cumulative;
        entry.last_cumulative = cumulative;
        entry.initial_amount = entry.initial_amount.max(order_size);
        entry.order_id = order_id;
        entry.last_seen_at = Instant::now();
        let target = entry.initial_amount;

        // Only claim terminal when the summed fills actually cover the order size.
        // The trade-history fetch is capped (no pagination here), so a high-volume
        // account could undercount; marking a partial as terminal would lock the
        // aggregator at the low cumulative. Leaving it non-terminal lets a later
        // poll add fills, and the position reconciler nets any genuine remainder.
        let is_terminal = cumulative + 1e-9 >= target;

        self.process_cumulative_fill(
            order_id,
            client_order_id.to_string(),
            order_side,
            cumulative,
            target,
            avg_price,
            is_terminal,
            delta,
        )
        .await;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    async fn process_cumulative_fill(
        &self,
        order_id: u64,
        client_order_id: String,
        order_side: OrderSide,
        filled_amount: f64,
        initial_amount: f64,
        price: f64,
        is_terminal: bool,
        delta: f64,
    ) {
        let notional_value = delta * price;
        if !is_terminal && notional_value <= self.min_hedge_notional {
            debug!(
                "[REST_FILL_DETECTION] Fill notional ${:.2} < ${:.2} threshold",
                notional_value, self.min_hedge_notional
            );
            return;
        }

        if !self.fill_aggregator.observe_fill_with_target(
            order_id,
            order_side,
            filled_amount,
            price,
            is_terminal,
            Some(initial_amount),
        ) {
            debug!(
                "[REST_FILL_DETECTION] Accumulated/restated fill (order_id={}, cumulative={})",
                order_id, filled_amount
            );
            return;
        }

        if !self
            .processed_fills
            .insert_if_new(FillKey::from_cloid_cumulative(
                client_order_id.clone(),
                filled_amount,
            ))
        {
            debug!(
                "[REST_FILL_DETECTION] Fill already processed (order_id={}, cloid={}, cumulative={})",
                order_id, client_order_id, filled_amount
            );
            return;
        }

        let Some(reservation) = self.fill_aggregator.try_reserve_hedge(order_id) else {
            return;
        };

        if !self.low_latency_mode {
            info!(
                "{} {} {} FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {} {}",
                tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                "*".green().bold(),
                if is_terminal { "FULL" } else { "PARTIAL" },
                order_side.as_str().bright_yellow(),
                reservation.size,
                self.symbol.bright_white().bold(),
                format!("${:.6}", price).cyan(),
                filled_amount,
                initial_amount,
                format!("${:.2}", notional_value).cyan().bold(),
                "(REST API)".bright_black()
            );
        }

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

        if !enqueue_reserved_hedge(
            &self.fill_aggregator,
            &self.hedge_tx,
            &self.bot_state,
            reservation,
            "REST fill",
        )
        .await
        {
            error!(
                "{} {} Hedge intent was not queued for REST fill",
                tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                "FAIL".red().bold()
            );
        }
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
                "[REST_FILL_DETECTION] Hedge not queued for {}; released reservation: {}",
                context, reason
            );
            false
        }
        Err(e) => {
            aggregator.release_reservation(reservation);
            warn!(
                "[REST_FILL_DETECTION] Hedge enqueue failed for {}; released reservation: {}",
                context, e
            );
            false
        }
    }
}
