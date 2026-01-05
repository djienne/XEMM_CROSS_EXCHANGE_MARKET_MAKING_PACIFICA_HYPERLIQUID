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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bot::BotState;
    use crate::config::Config;
    use crate::connector::mocks::MockPacificaTradingClient;
    use crate::services::order_monitor::SharedOrderSnapshot;
    use crate::strategy::OpportunityEvaluator;
    use crate::util::atomic_price::AtomicPrice;
    use crate::util::rate_limit::RateLimitTracker;
    use std::sync::atomic::Ordering;

    /// Create a test config with reasonable defaults
    fn test_config() -> Config {
        let mut config = Config::default();
        config.symbol = "SOL".to_string();
        config.pacifica_maker_fee_bps = 2.0;
        config.hyperliquid_taker_fee_bps = 3.0;
        config.profit_rate_bps = 5.0;
        config.order_notional_usd = 100.0;
        config.profit_cancel_threshold_bps = 2.0;
        config.cancel_grace_period_ms = 1000;
        config.hyperliquid_slippage = 0.05;
        config.hyperliquid_use_ws_for_hedge = false;
        config.min_hedge_notional_usd = 10.0;
        config
    }

    /// Create test evaluator
    fn test_evaluator(config: &Config) -> OpportunityEvaluator {
        OpportunityEvaluator::new(
            config.pacifica_maker_fee_bps,
            config.hyperliquid_taker_fee_bps,
            config.profit_rate_bps,
            0.01, // pacifica_tick_size
        )
    }

    /// Create a minimal OrderMonitorService for testing
    /// This creates a stub that won't panic but doesn't need real implementations
    fn create_stub_order_monitor(
        bot_state: Arc<RwLock<BotState>>,
        atomic_status: Arc<AtomicU8>,
        order_snapshot: Arc<SharedOrderSnapshot>,
        pacifica_prices: Arc<AtomicPrice>,
        hyperliquid_prices: Arc<AtomicPrice>,
        config: Config,
        evaluator: OpportunityEvaluator,
    ) -> Arc<OrderMonitorService> {
        use crate::connector::pacifica::{PacificaTrading, PacificaCredentials};
        use crate::connector::hyperliquid::{HyperliquidTrading, HyperliquidCredentials};

        // Create dummy trading clients (won't be called in tests)
        let pac_creds = PacificaCredentials {
            account: "dummy".to_string(),
            agent_wallet: "dummy".to_string(),
            private_key: "dummy".to_string(),
        };
        let pac_trading = Arc::new(PacificaTrading::new(pac_creds).expect("Dummy pac client"));

        let hl_creds = HyperliquidCredentials {
            private_key: "0x0000000000000000000000000000000000000000000000000000000000000001".to_string(),
        };
        let hl_trading = Arc::new(HyperliquidTrading::new(hl_creds, false).expect("Dummy hl client"));

        let (service, _cancel_rx) = OrderMonitorService::new(
            bot_state,
            atomic_status,
            order_snapshot,
            pacifica_prices,
            hyperliquid_prices,
            config,
            evaluator,
            pac_trading,
            hl_trading,
        );

        Arc::new(service)
    }

    /// Create the OpportunityLoopService for testing
    fn create_test_service(
        mock_pacifica: Arc<MockPacificaTradingClient>,
    ) -> (OpportunityLoopService, Arc<AtomicU8>, Arc<AtomicU64>, Arc<RwLock<BotState>>, Arc<AtomicPrice>, Arc<AtomicPrice>) {
        let config = test_config();
        let bot_state = Arc::new(RwLock::new(BotState::new()));
        let pacifica_prices = Arc::new(AtomicPrice::new());
        let hyperliquid_prices = Arc::new(AtomicPrice::new());
        let evaluator = test_evaluator(&config);
        let atomic_status = Arc::new(AtomicU8::new(0)); // Idle
        let last_cancel_ms = Arc::new(AtomicU64::new(0));
        let order_snapshot = Arc::new(SharedOrderSnapshot::new());
        let expected_cloid_hash = Arc::new(AtomicU64::new(0));

        // Create order monitor (with stub implementations)
        let order_monitor = create_stub_order_monitor(
            bot_state.clone(),
            atomic_status.clone(),
            order_snapshot.clone(),
            pacifica_prices.clone(),
            hyperliquid_prices.clone(),
            config.clone(),
            evaluator.clone(),
        );

        let service = OpportunityLoopService::new(
            config,
            bot_state.clone(),
            mock_pacifica,
            pacifica_prices.clone(),
            hyperliquid_prices.clone(),
            evaluator,
            atomic_status.clone(),
            last_cancel_ms.clone(),
            order_snapshot,
            order_monitor,
            expected_cloid_hash,
        );

        (service, atomic_status, last_cancel_ms, bot_state, pacifica_prices, hyperliquid_prices)
    }

    // ============== evaluate_and_place_orders tests ==============

    #[tokio::test]
    async fn test_places_buy_order_when_profitable() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, _atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();

        // Create a BUY opportunity: HL bid is high relative to Pacifica
        // HL bid at $101, we can buy on Pacifica and sell on HL for profit
        service.evaluate_and_place_orders(
            100.0,   // pac_bid
            100.1,   // pac_ask
            101.0,   // hl_bid - HIGH enough for buy opportunity
            101.1,   // hl_ask
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Order should be placed
        assert_eq!(
            mock_pacifica.place_limit_order_calls.load(Ordering::SeqCst),
            1,
            "One order should be placed for buy opportunity"
        );

        // Check it was a BUY order
        let side = mock_pacifica.last_order_side.lock().unwrap();
        assert!(
            matches!(*side, Some(crate::strategy::OrderSide::Buy)),
            "Should be a BUY order"
        );
    }

    #[tokio::test]
    async fn test_places_sell_order_when_profitable() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, _atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();

        // Create a SELL opportunity: HL ask is low relative to Pacifica
        // HL ask at $99, we can sell on Pacifica and buy on HL for profit
        service.evaluate_and_place_orders(
            100.0,   // pac_bid
            100.1,   // pac_ask
            98.9,    // hl_bid
            99.0,    // hl_ask - LOW enough for sell opportunity
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Order should be placed
        assert_eq!(
            mock_pacifica.place_limit_order_calls.load(Ordering::SeqCst),
            1,
            "One order should be placed for sell opportunity"
        );

        // Check it was a SELL order
        let side = mock_pacifica.last_order_side.lock().unwrap();
        assert!(
            matches!(*side, Some(crate::strategy::OrderSide::Sell)),
            "Should be a SELL order"
        );
    }

    #[tokio::test]
    async fn test_no_order_when_status_not_idle() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        // Set status to OrderPlaced (1)
        atomic_status.store(1, Ordering::Release);

        let mut rate_limiter = RateLimitTracker::new();

        // Even with profitable opportunity, should not place order
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,  // High HL bid (buy opportunity)
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        assert_eq!(
            mock_pacifica.place_limit_order_calls.load(Ordering::SeqCst),
            0,
            "No order when status is not Idle"
        );
    }

    #[tokio::test]
    async fn test_no_order_when_bot_state_not_idle() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, _atomic_status, _last_cancel_ms, bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        // Set bot state to non-idle by setting an active order
        {
            let mut state = bot_state.write().await;
            state.set_active_order(ActiveOrder {
                client_order_id: "existing-order".to_string(),
                symbol: "SOL".to_string(),
                side: crate::strategy::OrderSide::Buy,
                price: 100.0,
                size: 1.0,
                initial_profit_bps: 10.0,
                placed_at: Instant::now(),
            });
        }

        let mut rate_limiter = RateLimitTracker::new();

        // Even with profitable opportunity, should not place order
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,  // High HL bid (buy opportunity)
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        assert_eq!(
            mock_pacifica.place_limit_order_calls.load(Ordering::SeqCst),
            0,
            "No order when bot state is not Idle"
        );
    }

    #[tokio::test]
    async fn test_successful_order_updates_state() {
        use crate::bot::BotStatus;

        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, atomic_status, _last_cancel_ms, bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();

        // Place a profitable order
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,  // High HL bid (buy opportunity)
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Check atomic status was updated to OrderPlaced (1)
        assert_eq!(
            atomic_status.load(Ordering::Acquire),
            1,
            "Atomic status should be OrderPlaced after successful order"
        );

        // Check bot state was updated
        let state = bot_state.read().await;
        assert!(
            matches!(state.status, BotStatus::OrderPlaced),
            "Bot status should be OrderPlaced"
        );
        assert!(state.active_order.is_some(), "Active order should be set");
    }

    #[tokio::test]
    async fn test_successful_order_sets_expected_cloid() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, _atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();

        // Place a profitable order
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Expected cloid hash should be set (non-zero)
        assert_ne!(
            service.expected_cloid_hash.load(Ordering::Acquire),
            0,
            "Expected cloid hash should be set after order placement"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_error_triggers_backoff() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());

        // Configure mock to return rate limit error
        *mock_pacifica.place_limit_order_result.lock().unwrap() =
            Err(anyhow::anyhow!("Rate limit exceeded"));

        let (service, _atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();
        assert_eq!(rate_limiter.consecutive_errors(), 0);

        // Try to place order (will fail with rate limit)
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Rate limiter should record the error
        assert_eq!(
            rate_limiter.consecutive_errors(),
            1,
            "Rate limiter should track the error"
        );
    }

    #[tokio::test]
    async fn test_successful_order_resets_rate_limit() {
        let mock_pacifica = Arc::new(MockPacificaTradingClient::new());
        let (service, _atomic_status, _last_cancel_ms, _bot_state, _pac_prices, _hl_prices) =
            create_test_service(mock_pacifica.clone());

        let mut rate_limiter = RateLimitTracker::new();

        // Simulate previous errors
        rate_limiter.record_error();
        rate_limiter.record_error();
        assert_eq!(rate_limiter.consecutive_errors(), 2);

        // Place a successful order
        service.evaluate_and_place_orders(
            100.0,
            100.1,
            101.0,
            101.1,
            now_epoch_ms(),
            &mut rate_limiter,
        ).await;

        // Rate limiter should be reset
        assert_eq!(
            rate_limiter.consecutive_errors(),
            0,
            "Rate limiter should reset after successful order"
        );
    }

    // ============== OpportunityEvaluator tests (testing real evaluation logic) ==============

    #[test]
    fn test_evaluator_buy_opportunity_calculation() {
        let config = test_config();
        let evaluator = test_evaluator(&config);

        // High HL bid should create buy opportunity
        let opp = evaluator.evaluate_buy_opportunity(101.0, 100.0, now_epoch_ms());

        assert!(opp.is_some(), "Should have buy opportunity with high HL bid");
        let opp = opp.unwrap();
        assert!(opp.initial_profit_bps > 0.0, "Profit should be positive");
    }

    #[test]
    fn test_evaluator_sell_opportunity_calculation() {
        let config = test_config();
        let evaluator = test_evaluator(&config);

        // Low HL ask should create sell opportunity
        let opp = evaluator.evaluate_sell_opportunity(99.0, 100.0, now_epoch_ms());

        assert!(opp.is_some(), "Should have sell opportunity with low HL ask");
        let opp = opp.unwrap();
        assert!(opp.initial_profit_bps > 0.0, "Profit should be positive");
    }

}
