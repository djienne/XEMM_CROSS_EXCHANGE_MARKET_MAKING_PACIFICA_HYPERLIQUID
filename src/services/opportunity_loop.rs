use anyhow::Result;
use colored::Colorize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::Instant;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

use crate::bot::{ActiveOrder, BotState};
use crate::config::Config;
use crate::connector::pacifica::{PacificaTradingClient, OrderSide as PacificaOrderSide};
use crate::services::{
    hash_cloid,
    market_event::MarketEventHub,
    order_monitor::{AtomicBotStatus, OrderMonitorService, SharedOrderSnapshot, sync_atomic_status, update_order_snapshot},
};
use crate::strategy::OpportunityEvaluator;
use crate::util::atomic_price::AtomicPrice;
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};


pub struct OpportunityLoopService {
    config: Config,
    bot_state: Arc<RwLock<BotState>>,
    pacifica_trading: Arc<dyn PacificaTradingClient + Send + Sync>,
    pacifica_prices: Arc<AtomicPrice>,
    hyperliquid_prices: Arc<AtomicPrice>,
    evaluator: OpportunityEvaluator,
    atomic_status: Arc<AtomicU8>,
    last_cancel_ms: Arc<AtomicU64>,
    order_snapshot: Arc<SharedOrderSnapshot>,
    order_monitor: Arc<OrderMonitorService>,
    /// Expected client_order_id hash for lock-free fill ownership check
    expected_cloid_hash: Arc<AtomicU64>,
}

impl OpportunityLoopService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        bot_state: Arc<RwLock<BotState>>,
        pacifica_trading: Arc<dyn PacificaTradingClient + Send + Sync>,
        pacifica_prices: Arc<AtomicPrice>,
        hyperliquid_prices: Arc<AtomicPrice>,
        evaluator: OpportunityEvaluator,
        atomic_status: Arc<AtomicU8>,
        last_cancel_ms: Arc<AtomicU64>,
        order_snapshot: Arc<SharedOrderSnapshot>,
        order_monitor: Arc<OrderMonitorService>,
        expected_cloid_hash: Arc<AtomicU64>,
    ) -> Self {
        Self {
            config,
            bot_state,
            pacifica_trading,
            pacifica_prices,
            hyperliquid_prices,
            evaluator,
            atomic_status,
            last_cancel_ms,
            order_snapshot,
            order_monitor,
            expected_cloid_hash,
        }
    }

    pub async fn run(
        &self,
        market_events: &MarketEventHub,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) -> Result<()> {
        info!("{} Starting opportunity evaluation loop...", "[MAIN]".bright_magenta().bold());
        
        // Rate limiter for order placement errors
        // Exponential backoff tracker (no args needed)
        let mut order_placement_rate_limit = RateLimitTracker::new();

        // Signal handlers
        let sigint = tokio::signal::ctrl_c();
        tokio::pin!(sigint);

        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

        loop {
            // Process available events (limit to 50 per tick to prevent starvation)
            let mut event_count = 0;
            // ... (rest of loop logic same as before until select) ...
            let mut last_event = None;

            while let Some(event) = market_events.pop() {
                last_event = Some(event);
                event_count += 1;
                if event_count >= 50 {
                    break;
                }
            }

            if let Some(event) = last_event {
                // CRITICAL: Check active order for profit deviation / age on every price update
                // This enables event-driven order monitoring (cancel if profit drops)
                self.order_monitor.check_on_market_event(event.source);

                let status = self.atomic_status.load(Ordering::Acquire);

                // Only evaluate if Idle
                if status == AtomicBotStatus::Idle as u8 {
                    let local_now_ms = now_epoch_ms();
                    let opp_timestamp = if event.ts > 0 { event.ts } else { local_now_ms };
                    let last_cancel_ms = self.last_cancel_ms.load(Ordering::Acquire);
                    
                    // Grace period check
                    if last_cancel_ms == 0 || local_now_ms.saturating_sub(last_cancel_ms) >= self.config.cancel_grace_period_ms {
                        
                        // Rate limit check
                        if !order_placement_rate_limit.should_skip() {
                            let pac_snapshot = self.pacifica_prices.load();
                            let hl_snapshot = self.hyperliquid_prices.load();
                            let (pac_bid, pac_ask) = (pac_snapshot.bid, pac_snapshot.ask);
                            let (hl_bid, hl_ask) = (hl_snapshot.bid, hl_snapshot.ask);

                            if pac_bid > 0.0 && pac_ask > 0.0 && hl_bid > 0.0 && hl_ask > 0.0 {
                                self.evaluate_and_place_orders(
                                    pac_bid, pac_ask, hl_bid, hl_ask, 
                                    opp_timestamp, 
                                    &mut order_placement_rate_limit
                                ).await;
                            }
                        } else {
                            // Log rate limit backoff occasionally
                            let remaining = order_placement_rate_limit.remaining_backoff_secs();
                            if remaining as u64 % 5 == 0 || remaining < 1.0 {
                                debug!("[MAIN] Skipping order placement (rate limit backoff, {:.1}s remaining)", remaining);
                            }
                        }
                    }
                }
            }

            tokio::select! {
                _ = market_events.notifier().notified() => {}

                _ = &mut sigint => {
                    info!("{} {} Received SIGINT (Ctrl+C), initiating graceful shutdown...",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold(),
                        "⚠".yellow().bold()
                    );
                    break;
                }

                _ = async {
                    #[cfg(unix)]
                    {
                        sigterm.recv().await
                    }
                    #[cfg(not(unix))]
                    {
                        std::future::pending::<Option<()>>().await
                    }
                } => {
                    info!("{} {} Received SIGTERM (Docker shutdown), initiating graceful shutdown...",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold(),
                        "⚠".yellow().bold()
                    );
                    break;
                }

                _ = shutdown_rx.recv() => {
                    info!("{} Shutdown signal received",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold()
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    async fn evaluate_and_place_orders(
        &self,
        pac_bid: f64,
        pac_ask: f64,
        hl_bid: f64,
        hl_ask: f64,
        opp_timestamp: u64,
        order_placement_rate_limit: &mut RateLimitTracker,
    ) {
        let buy_opp = self.evaluator.evaluate_buy_opportunity(
            hl_bid,
            self.config.order_notional_usd,
            opp_timestamp,
        );
        let sell_opp = self.evaluator.evaluate_sell_opportunity(
            hl_ask,
            self.config.order_notional_usd,
            opp_timestamp,
        );

        let pac_mid = (pac_bid + pac_ask) / 2.0;
        let best_opp = OpportunityEvaluator::pick_best_opportunity(buy_opp, sell_opp, pac_mid);

        if let Some(opp) = best_opp {
            // Double check status before lock
            if self.atomic_status.load(Ordering::Acquire) != AtomicBotStatus::Idle as u8 {
                return;
            }

            let mut state = self.bot_state.write().await;
            if !state.is_idle() {
                return;
            }

            info!(
                "{} {} @ {} → HL {} | Size: {} | Profit: {} | PAC: {}/{} | HL: {}/{}",
                format!("[{} OPPORTUNITY]", self.config.symbol).bright_green().bold(),
                opp.direction.as_str().bright_yellow().bold(),
                format!("${:.6}", opp.pacifica_price).cyan().bold(),
                format!("${:.6}", opp.hyperliquid_price).cyan(),
                format!("{:.4}", opp.size).bright_white(),
                format!("{:.2} bps", opp.initial_profit_bps).green().bold(),
                format!("${:.6}", pac_bid).cyan(),
                format!("${:.6}", pac_ask).cyan(),
                format!("${:.6}", hl_bid).cyan(),
                format!("${:.6}", hl_ask).cyan()
            );

            info!("{} Placing {} on Pacifica...",
                format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                opp.direction.as_str().bright_yellow().bold()
            );

            let pacifica_side: PacificaOrderSide = opp.direction.into();

            match self.pacifica_trading
                .place_limit_order(
                    &self.config.symbol,
                    pacifica_side,
                    opp.size,
                    Some(opp.pacifica_price),
                    0.0,
                    Some(pac_bid),
                    Some(pac_ask),
                )
                .await
            {
                Ok(order_data) => {
                    order_placement_rate_limit.record_success();

                    if let Some(client_order_id) = order_data.client_order_id {
                        let order_id = order_data.order_id.unwrap_or(0);
                        info!(
                            "{} {} Placed {} #{} @ {} | cloid: {}...{}",
                            format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                            "✓".green().bold(),
                            opp.direction.as_str().bright_yellow(),
                            order_id,
                            format!("${:.4}", opp.pacifica_price).cyan().bold(),
                            &client_order_id[..8],
                            &client_order_id[client_order_id.len()-4..]
                        );

                        // Update expected cloid hash for lock-free fill ownership check
                        self.expected_cloid_hash.store(hash_cloid(&client_order_id), Ordering::Release);

                        let active_order = ActiveOrder {
                            client_order_id,
                            symbol: self.config.symbol.clone(),
                            side: opp.direction,
                            price: opp.pacifica_price,
                            size: opp.size,
                            initial_profit_bps: opp.initial_profit_bps,
                            placed_at: Instant::now(),
                        };

                        state.set_active_order(active_order);

                        sync_atomic_status(&self.atomic_status, &state.status);
                        update_order_snapshot(
                            &self.order_snapshot,
                            opp.direction,
                            opp.pacifica_price,
                            opp.size,
                            opp.initial_profit_bps,
                        );
                    } else {
                        info!("{} {} Order placed but no client_order_id returned",
                            format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                            "✗".red().bold()
                        );
                    }
                }
                Err(e) => {
                    if is_rate_limit_error(&e) {
                        order_placement_rate_limit.record_error();
                        let backoff_secs = order_placement_rate_limit.get_backoff_secs();
                        info!(
                            "{} {} Failed to place order: Rate limit exceeded. Backing off for {}s (attempt #{})",
                            format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                            "⚠".yellow().bold(),
                            backoff_secs,
                            order_placement_rate_limit.consecutive_errors()
                        );
                    } else {
                        info!("{} {} Failed to place order: {}",
                            format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                            "✗".red().bold(),
                            e.to_string().red()
                        );
                    }
                }
            }
        }
    }
}


fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
