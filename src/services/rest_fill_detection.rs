use std::sync::Arc;
use std::time::Duration;

use colored::Colorize;
use fast_float::parse;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::connector::pacifica::PacificaTrading;
use crate::services::cancel_manager::{CancelIntent, CancelReason};
use crate::services::fill_aggregator::FillAggregator;
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::{enqueue_hedge_intent, HedgeIntent};
use crate::strategy::OrderSide;
use crate::util::log::{tag_static, Color};
use crate::util::rate_limit::is_rate_limit_error;

/// REST API fill detection service (backup/fallback method).
pub struct RestFillDetectionService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub cancel_tx: mpsc::Sender<CancelIntent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub min_hedge_notional: f64,
    pub poll_interval_ms: u64,
}

impl RestFillDetectionService {
    pub async fn run(self) {
        let mut consecutive_errors = 0u32;
        let mut last_known_filled_amount: f64 = 0.0;

        loop {
            let has_active_order = {
                let state = self.bot_state.read();
                state.has_active_order_fast()
                    || matches!(
                        state.status,
                        BotStatus::Filled | BotStatus::Hedging | BotStatus::Reconciling
                    )
            };

            let poll_ms = if has_active_order {
                self.poll_interval_ms
            } else {
                1000
            };
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;

            let is_terminal = {
                let state = self.bot_state.read();
                matches!(state.status, BotStatus::Complete | BotStatus::Error(_))
            };
            if is_terminal {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let active_order_info = {
                let state = self.bot_state.read();
                if let Some(ref order) = state.active_order {
                    Some((order.client_order_id.clone(), order.side))
                } else if matches!(state.status, BotStatus::Filled | BotStatus::Hedging) {
                    None
                } else {
                    last_known_filled_amount = 0.0;
                    continue;
                }
            };

            let client_order_id_opt = active_order_info.as_ref().map(|(id, _)| id.clone());
            let order_side_opt = active_order_info.as_ref().map(|(_, side)| *side);

            match self.pacifica_trading.get_open_orders().await {
                Ok(orders) => {
                    consecutive_errors = 0;

                    let our_order = if let Some(ref cloid) = client_order_id_opt {
                        orders.iter().find(|o| &o.client_order_id == cloid)
                    } else {
                        debug!(
                            "[REST_FILL_DETECTION] Recovery mode: searching {} orders for filled orders",
                            orders.len()
                        );
                        orders.iter().filter(|o| o.symbol == self.symbol).find(|o| {
                            let filled: f64 = parse(&o.filled_amount).unwrap_or(0.0);
                            filled > 0.0
                        })
                    };

                    let Some(order) = our_order else {
                        if client_order_id_opt.is_some() {
                            debug!("[REST_FILL_DETECTION] Active order not found in open_orders");
                        }
                        continue;
                    };

                    let filled_amount: f64 = parse(&order.filled_amount).unwrap_or(0.0);
                    let initial_amount: f64 = parse(&order.initial_amount).unwrap_or(0.0);
                    let price: f64 = parse(&order.price).unwrap_or(0.0);

                    if filled_amount <= last_known_filled_amount || filled_amount <= 0.0 {
                        continue;
                    }

                    let new_fill_amount = filled_amount - last_known_filled_amount;
                    let notional_value = new_fill_amount * price;
                    last_known_filled_amount = filled_amount;

                    let is_full_fill = (filled_amount - initial_amount).abs() < 0.0001;
                    let order_side = if let Some(side) = order_side_opt {
                        side
                    } else {
                        match order.side.as_str() {
                            "bid" | "buy" => OrderSide::Buy,
                            "ask" | "sell" => OrderSide::Sell,
                            _ => continue,
                        }
                    };

                    let decision = self.fill_aggregator.on_fill_with_target(
                        order.order_id,
                        order_side,
                        filled_amount,
                        price,
                        is_full_fill,
                        Some(initial_amount),
                    );

                    let Some(decision) = decision else {
                        debug!(
                            "[REST_FILL_DETECTION] Accumulated partial (order_id={}, cumulative ${:.2})",
                            order.order_id, notional_value
                        );
                        continue;
                    };

                    if !is_full_fill && notional_value <= self.min_hedge_notional {
                        debug!(
                            "[REST_FILL_DETECTION] Fill notional ${:.2} < ${:.2} threshold",
                            notional_value, self.min_hedge_notional
                        );
                        continue;
                    }

                    if !self
                        .processed_fills
                        .insert_if_new(FillKey::from_cloid_cumulative(
                            order.client_order_id.clone(),
                            filled_amount,
                        ))
                    {
                        debug!(
                            "[REST_FILL_DETECTION] Fill already processed (order_id={}, cloid={}, cumulative={})",
                            order.order_id, order.client_order_id, filled_amount
                        );
                        continue;
                    }

                    info!(
                        "{} {} {} FILL: {} {} {} @ {} | Filled: {} / {} | Notional: {} {}",
                        tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                        "*".green().bold(),
                        if is_full_fill { "FULL" } else { "PARTIAL" },
                        order.side.bright_yellow(),
                        decision.size,
                        self.symbol.bright_white().bold(),
                        format!("${:.6}", price).cyan(),
                        filled_amount,
                        initial_amount,
                        format!("${:.2}", notional_value).cyan().bold(),
                        "(REST API)".bright_black()
                    );

                    let bot_state = self.bot_state.clone();
                    let hedge_tx = self.hedge_tx.clone();
                    let cancel_tx = self.cancel_tx.clone();
                    let symbol = self.symbol.clone();

                    tokio::spawn(async move {
                        {
                            let mut state = bot_state.write();
                            state.mark_filled(decision.size, decision.side);
                        }

                        info!(
                            "{} {} State updated to Filled (REST)",
                            tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                            "*".green().bold()
                        );

                        let _ = cancel_tx
                            .try_send(CancelIntent::new(symbol.clone(), CancelReason::PartialFill));

                        info!(
                            "{} {}, triggering hedge (REST)",
                            format!("[{}]", symbol).bright_white().bold(),
                            "Order filled".green().bold()
                        );

                        if let Err(e) =
                            enqueue_hedge_intent(&hedge_tx, &bot_state, decision.into()).await
                        {
                            error!(
                                "{} {} Hedge intent enqueue failed: {}",
                                tag_static("REST_FILL_DETECTION", Color::BrightCyan),
                                "x".red().bold(),
                                e
                            );
                        }
                    });
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
}
