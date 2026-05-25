use colored::Colorize;
use fast_float::parse;
use parking_lot::Mutex;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::app::PositionSnapshot;
use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading};
use crate::services::fill_aggregator::FillAggregator;
use crate::services::fill_dedup::{FillDedup, FillKey};
use crate::services::{enqueue_hedge_intent, HedgeEvent};
use crate::strategy::OrderSide;
use crate::util::log::{tag_static, Color};

/// Position-based fill detection service (4th layer - ground truth)
///
/// Polls Pacifica positions via REST API every 500ms to detect position changes.
/// This is the ultimate fallback - if position changed in the expected direction,
/// a fill definitely occurred regardless of WebSocket/REST/order status detection.
pub struct PositionMonitorService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeEvent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub symbol: String,
    pub processed_fills: Arc<FillDedup>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub last_position_snapshot: Arc<Mutex<Option<PositionSnapshot>>>,
}

impl PositionMonitorService {
    pub async fn run(self) {
        loop {
            // Adaptive polling: fast when order active, slow when idle
            let has_active_order = {
                let state = self.bot_state.read();
                state.has_active_order_fast()
            };

            let poll_ms = if has_active_order { 250 } else { 2000 };
            tokio::time::sleep(Duration::from_millis(poll_ms)).await;

            // Only check if we have an active order
            let active_order_info = {
                let state = self.bot_state.read();
                if matches!(
                    state.status,
                    crate::bot::BotStatus::Complete | crate::bot::BotStatus::Error(_)
                ) {
                    continue;
                }

                state
                    .active_order
                    .as_ref()
                    .map(|o| (o.order_id, o.client_order_id.clone(), o.side, o.size))
            };

            if active_order_info.is_none() {
                // No active order - update snapshot for next order
                match self.pacifica_trading.get_positions().await {
                    Ok(positions) => {
                        let position = positions.iter().find(|p| p.symbol == self.symbol);
                        let mut snapshot = self.last_position_snapshot.lock();

                        if let Some(pos) = position {
                            let amount: f64 = parse(&pos.amount).unwrap_or(0.0);
                            *snapshot = Some(PositionSnapshot {
                                amount,
                                side: pos.side.clone(),
                                last_check: std::time::Instant::now(),
                            });
                            debug!(
                                "[POSITION_MONITOR] Updated baseline: {} {} {}",
                                self.symbol, pos.side, amount
                            );
                        } else {
                            *snapshot = None;
                            debug!("[POSITION_MONITOR] No position for {}", self.symbol);
                        }
                    }
                    Err(e) => {
                        debug!(
                            "[POSITION_MONITOR] Failed to fetch baseline position: {}",
                            e
                        );
                    }
                }
                continue;
            }

            let (order_id_opt, client_order_id, order_side, _order_size) =
                active_order_info.unwrap();

            // Fetch current positions
            let positions_result = self.pacifica_trading.get_positions().await;

            match positions_result {
                Ok(positions) => {
                    let current_position = positions.iter().find(|p| p.symbol == self.symbol);
                    let last_snapshot = self.last_position_snapshot.lock().clone();

                    // Calculate position delta
                    let (last_amount, last_side) = if let Some(ref snap) = last_snapshot {
                        (snap.amount, snap.side.clone())
                    } else {
                        (0.0, "none".to_string())
                    };

                    let (current_amount, current_side) = if let Some(pos) = current_position {
                        (pos.amount.parse::<f64>().unwrap_or(0.0), pos.side.clone())
                    } else {
                        (0.0, "none".to_string())
                    };

                    // Convert to signed position for delta calculation
                    let last_signed = match last_side.as_str() {
                        "bid" => last_amount,
                        "ask" => -last_amount,
                        _ => 0.0,
                    };

                    let current_signed = match current_side.as_str() {
                        "bid" => current_amount,
                        "ask" => -current_amount,
                        _ => 0.0,
                    };

                    let delta = current_signed - last_signed;

                    // Check if delta matches our order direction
                    let delta_matches_order = (delta > 0.0 && matches!(order_side, OrderSide::Buy))
                        || (delta < 0.0 && matches!(order_side, OrderSide::Sell));

                    // If delta is significant and matches order direction
                    if delta.abs() > 0.0001 && delta_matches_order {
                        // Position changed in expected direction - fill detected!
                        let fill_size = delta.abs();

                        info!(
                            "{} {} Position delta detected: {} {} -> {} {} (delta {:.4})",
                            tag_static("POSITION_MONITOR", Color::BrightCyan),
                            "FAST".yellow().bold(),
                            format!("{:.4}", last_signed).bright_white(),
                            last_side.yellow(),
                            format!("{:.4}", current_signed).bright_white(),
                            current_side.yellow(),
                            format!("{:.4}", delta.abs()).green().bold()
                        );

                        // CRITICAL FIX: Skip if this is first position change from None baseline (startup)
                        // This prevents hedging the bot's first order fill which establishes initial position
                        if last_snapshot.is_none() && last_signed.abs() < 0.0001 {
                            info!(
                                "{} {} Skipping hedge for first position change from baseline (startup initialization)",
                                tag_static("POSITION_MONITOR", Color::BrightCyan),
                                "INFO".blue().bold()
                            );

                            // Update snapshot to prevent continuous detection
                            let mut snapshot = self.last_position_snapshot.lock();
                            *snapshot = Some(PositionSnapshot {
                                amount: current_amount,
                                side: current_side,
                                last_check: std::time::Instant::now(),
                            });
                            continue;
                        }

                        // Check bot state - don't trigger duplicate hedges
                        let current_state = {
                            let state = self.bot_state.read();
                            state.status.clone()
                        };

                        // Skip if already filled, hedging, or complete
                        if matches!(
                            current_state,
                            crate::bot::BotStatus::Filled
                                | crate::bot::BotStatus::Hedging
                                | crate::bot::BotStatus::Complete
                        ) {
                            info!(
                                "{} {} Fill already handled by primary detection (state: {:?}), skipping duplicate hedge",
                                tag_static("POSITION_MONITOR", Color::BrightCyan),
                                "INFO".blue().bold(),
                                current_state
                            );

                            // Update snapshot to prevent continuous detection
                            let mut snapshot = self.last_position_snapshot.lock();
                            *snapshot = Some(PositionSnapshot {
                                amount: current_amount,
                                side: current_side,
                                last_check: std::time::Instant::now(),
                            });
                            continue;
                        }

                        let estimated_price = current_position
                            .and_then(|p| p.entry_price.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        let hedge_event = if let Some(id) = order_id_opt {
                            match self.fill_aggregator.on_fill(
                                id,
                                order_side,
                                fill_size,
                                estimated_price,
                                true,
                            ) {
                                Some(decision) => decision.into(),
                                None => {
                                    debug!(
                                        "[POSITION_MONITOR] Position fill already accounted for (order_id={})",
                                        id
                                    );
                                    continue;
                                }
                            }
                        } else {
                            HedgeEvent {
                                source_order_id: 0,
                                hedge_seq: 0,
                                side: order_side,
                                size: fill_size,
                                avg_price: estimated_price,
                                detected_at: std::time::Instant::now(),
                                terminal: true,
                            }
                        };

                        let dedup_key =
                            FillKey::from_cloid_cumulative(client_order_id.clone(), fill_size);
                        let should_process = self.processed_fills.insert_if_new(dedup_key);

                        if should_process {
                            info!(
                                "{} {} FILL DETECTED via position change!",
                                tag_static("POSITION_MONITOR", Color::BrightCyan),
                                "OK".green().bold()
                            );

                            // Update state to Filled
                            {
                                let mut state = self.bot_state.write();
                                state.mark_filled(hedge_event.size, hedge_event.side);
                            }

                            info!(
                                "{} Triggering hedge for position-detected fill",
                                tag_static("POSITION_MONITOR", Color::BrightCyan)
                            );

                            if let Err(e) = enqueue_hedge_intent(
                                &self.hedge_tx,
                                &self.bot_state,
                                hedge_event.clone(),
                            )
                            .await
                            {
                                error!(
                                    "{} {} Hedge intent enqueue failed: {}",
                                    tag_static("POSITION_MONITOR", Color::BrightCyan),
                                    "x".red().bold(),
                                    e
                                );
                            }

                            // Dual cancellation
                            info!(
                                "{} {} Dual cancellation (REST + WebSocket)...",
                                tag_static("POSITION_MONITOR", Color::BrightCyan),
                                "FAST".yellow().bold()
                            );

                            let rest_result = self
                                .pacifica_trading
                                .cancel_all_orders(false, Some(&self.symbol), false)
                                .await;

                            match rest_result {
                                Ok(count) => {
                                    info!(
                                        "{} {} REST API cancelled {} order(s)",
                                        tag_static("POSITION_MONITOR", Color::BrightCyan),
                                        "OK".green().bold(),
                                        count
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "{} {} REST API cancel failed: {}",
                                        tag_static("POSITION_MONITOR", Color::BrightCyan),
                                        "WARN".yellow().bold(),
                                        e
                                    );
                                }
                            }

                            let ws_result = self
                                .pacifica_ws_trading
                                .cancel_all_orders_ws(false, Some(&self.symbol), false)
                                .await;

                            match ws_result {
                                Ok(count) => {
                                    info!(
                                        "{} {} WebSocket cancelled {} order(s)",
                                        tag_static("POSITION_MONITOR", Color::BrightCyan),
                                        "OK".green().bold(),
                                        count
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "{} {} WebSocket cancel failed: {}",
                                        tag_static("POSITION_MONITOR", Color::BrightCyan),
                                        "WARN".yellow().bold(),
                                        e
                                    );
                                }
                            }
                        } else {
                            debug!(
                                "[POSITION_MONITOR] Fill already processed by another detection method"
                            );
                        }

                        // Update snapshot
                        let mut snapshot = self.last_position_snapshot.lock();
                        *snapshot = Some(PositionSnapshot {
                            amount: current_amount,
                            side: current_side,
                            last_check: std::time::Instant::now(),
                        });
                    }
                }
                Err(e) => {
                    debug!("[POSITION_MONITOR] Failed to fetch positions: {}", e);
                }
            }
        }
    }
}
