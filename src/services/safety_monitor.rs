use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fast_float::parse;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus, RunState};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::market_rules::fallback_rules;
use crate::services::fill_aggregator::FillAggregator;
use crate::services::order_monitor::{update_order_snapshot, SharedOrderSnapshot};
use crate::services::trade_gate::{GateReason, TradeGate};
use crate::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use crate::util::price::SharedQuote;

pub struct SafetyMonitorService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub atomic_status: Arc<AtomicU8>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    pub trade_gate: Arc<TradeGate>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub pacifica_prices: Arc<SharedQuote>,
    pub hyperliquid_prices: Arc<SharedQuote>,
    pub fill_ws_ready: Arc<AtomicBool>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub config: Config,
}

impl SafetyMonitorService {
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.update_fast_health();
            self.update_open_orders().await;
            self.update_positions().await;
            self.recover_unknown_placement().await;
        }
    }

    fn update_fast_health(&self) {
        let run_state = RunState::load(&self.atomic_status);
        self.trade_gate.update_from_run_state(run_state);
        self.trade_gate.set(
            GateReason::FillWsDown,
            !self.fill_ws_ready.load(Ordering::Acquire),
        );
        self.trade_gate.update_quote_health(
            self.pacifica_prices.snapshot(),
            self.hyperliquid_prices.snapshot(),
            Duration::from_millis(self.config.max_quote_age_ms),
        );
    }

    async fn update_open_orders(&self) {
        match self.pacifica_trading.get_open_orders().await {
            Ok(orders) => {
                self.trade_gate.allow(GateReason::OpenOrderUnknown);
                let has_symbol_order = orders
                    .iter()
                    .any(|order| order.symbol == self.config.symbol);
                let run_state = RunState::load(&self.atomic_status);
                let own_cloid = self
                    .bot_state
                    .read()
                    .active_order
                    .as_ref()
                    .map(|order| order.client_order_id.clone());
                let only_own_order = own_cloid.as_ref().map_or(false, |cloid| {
                    orders
                        .iter()
                        .filter(|order| order.symbol == self.config.symbol)
                        .all(|order| &order.client_order_id == cloid)
                });
                self.trade_gate.set(
                    GateReason::OpenOrderExists,
                    has_symbol_order
                        && !(matches!(run_state, RunState::OrderPlaced) && only_own_order),
                );
            }
            Err(e) => {
                debug!("[SAFETY] Open-order check failed: {}", e);
                self.trade_gate.block(GateReason::OpenOrderUnknown);
            }
        }
    }

    async fn update_positions(&self) {
        match self.signed_positions().await {
            Ok((pacifica, hyperliquid)) => {
                self.trade_gate.allow(GateReason::PositionUnknown);
                let net = pacifica + hyperliquid;
                // L1: residuals below the exchange minimum order size cannot be
                // hedged on-venue, so treat them as neutral here too (consistent
                // with the position reconciler) instead of permanently gating.
                let effective_dust = self
                    .config
                    .neutral_dust_base
                    .max(fallback_rules(&self.config.symbol).min_size);
                self.trade_gate
                    .set(GateReason::NetExposure, net.abs() > effective_dust);
                if net.abs() > effective_dust
                    && RunState::load(&self.atomic_status) == RunState::Idle
                {
                    let mut state = self.bot_state.write();
                    if matches!(state.status, BotStatus::Idle) {
                        state.mark_reconciling();
                    }
                }
            }
            Err(e) => {
                debug!("[SAFETY] Position check failed: {}", e);
                self.trade_gate.block(GateReason::PositionUnknown);
            }
        }
    }

    async fn recover_unknown_placement(&self) {
        if RunState::load(&self.atomic_status) != RunState::PlacementUnknown {
            return;
        }

        let active = {
            let state = self.bot_state.read();
            match state.active_order.clone() {
                Some(order) => order,
                None => return,
            }
        };

        match self.pacifica_trading.get_open_orders().await {
            Ok(orders) => {
                if let Some(order) = orders
                    .iter()
                    .find(|order| order.client_order_id == active.client_order_id)
                {
                    info!(
                        "[SAFETY] Recovered unknown placement as open order (cloid={})",
                        active.client_order_id
                    );
                    let mut state = self.bot_state.write();
                    if let Some(active_order) = state.active_order.as_mut() {
                        active_order.order_id = Some(order.order_id);
                        active_order.price = parse(&order.price).unwrap_or(active.price);
                        active_order.size = parse(&order.initial_amount).unwrap_or(active.size);
                    }
                    state.status = BotStatus::OrderPlaced;
                    state.store_status();
                    update_order_snapshot(
                        &self.order_snapshot,
                        active.side,
                        parse(&order.price).unwrap_or(active.price),
                        parse(&order.initial_amount).unwrap_or(active.size),
                        active.initial_profit_bps,
                    );
                    self.trade_gate.allow(GateReason::PlacementUnknown);
                    return;
                }
            }
            Err(e) => {
                warn!(
                    "[SAFETY] Unknown-placement open-order recovery failed: {}",
                    e
                );
                self.trade_gate.block(GateReason::OpenOrderUnknown);
                return;
            }
        }

        if active.placed_at.elapsed() < Duration::from_secs(5) {
            return;
        }

        match self
            .pacifica_trading
            .get_trade_history(Some(&self.config.symbol), Some(50), None, None)
            .await
        {
            Ok(trades) => {
                if let Some(fill) = trades
                    .iter()
                    .find(|trade| trade.client_order_id.as_deref() == Some(&active.client_order_id))
                {
                    let filled = parse(&fill.amount).unwrap_or(0.0);
                    let price = parse(&fill.entry_price).unwrap_or(active.price);
                    if self.fill_aggregator.observe_fill(
                        fill.order_id,
                        active.side,
                        filled,
                        price,
                        true,
                    ) {
                        let Some(reservation) =
                            self.fill_aggregator.try_reserve_hedge(fill.order_id)
                        else {
                            self.trade_gate.allow(GateReason::PlacementUnknown);
                            return;
                        };
                        let intent: HedgeIntent = reservation.into();
                        {
                            let mut state = self.bot_state.write();
                            state.mark_filled(intent.size, active.side);
                        }
                        match enqueue_hedge_intent(&self.hedge_tx, &self.bot_state, intent).await {
                            Ok(HedgeEnqueueResult::Queued) => {
                                self.fill_aggregator.commit_queued(reservation);
                            }
                            Ok(HedgeEnqueueResult::PersistedButNotQueued { reason }) => {
                                self.fill_aggregator.release_reservation(reservation);
                                warn!(
                                    "[SAFETY] Recovered placement fill persisted but not queued: {}",
                                    reason
                                );
                            }
                            Err(e) => {
                                self.fill_aggregator.release_reservation(reservation);
                                warn!("[SAFETY] Failed to enqueue recovered placement fill: {}", e);
                            }
                        }
                    }
                    self.trade_gate.allow(GateReason::PlacementUnknown);
                    return;
                }
            }
            Err(e) => {
                warn!(
                    "[SAFETY] Unknown-placement trade-history recovery failed: {}",
                    e
                );
            }
        }

        if active.placed_at.elapsed() >= Duration::from_secs(10) {
            warn!(
                "[SAFETY] Placement remains unknown after bounded recovery window; staying in Reconciling"
            );
            let mut state = self.bot_state.write();
            if matches!(state.status, BotStatus::PlacementUnknown) {
                state.mark_reconciling();
            }
        }
    }

    async fn signed_positions(&self) -> anyhow::Result<(f64, f64)> {
        let pacifica_positions = self.pacifica_trading.get_positions().await?;
        let pacifica_position = pacifica_positions
            .iter()
            .find(|position| position.symbol == self.config.symbol)
            .map(|position| {
                let amount = parse(&position.amount).unwrap_or(0.0);
                match position.side.as_str() {
                    "bid" => amount,
                    "ask" => -amount,
                    _ => 0.0,
                }
            })
            .unwrap_or(0.0);

        let hyperliquid_state = self
            .hyperliquid_trading
            .get_user_state(&self.hyperliquid_trading.account_address())
            .await?;
        let hyperliquid_position = hyperliquid_state
            .asset_positions
            .iter()
            .find(|position| position.position.coin == self.config.symbol)
            .map(|position| parse(&position.position.szi).unwrap_or(0.0))
            .unwrap_or(0.0);

        Ok((pacifica_position, hyperliquid_position))
    }
}
