use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::debug;

use crate::bot::{ActiveOrder, BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, PacificaCredentials, PacificaTrading,
    PacificaWsTrading, OrderSide as PacificaOrderSide,
};
use crate::services::{
    fill_detection::FillDetectionService, hedge::HedgeService, order_monitor::OrderMonitorService,
    orderbook::{HyperliquidOrderbookService, PacificaOrderbookService},
    position_monitor::PositionMonitorService, rest_fill_detection::RestFillDetectionService,
    rest_poll::{HyperliquidRestPollService, PacificaRestPollService}, HedgeEvent,
};
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};

// Macro for timestamped colored output
macro_rules! tprintln {
    ($($arg:tt)*) => {{
        println!("{} {}",
            chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string().bright_black(),
            format!($($arg)*)
        );
    }};
}

/// Position snapshot for tracking position deltas
#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    pub amount: f64,
    pub side: String, // "bid" or "ask"
    pub last_check: Instant,
}

/// XemmBot - Main application structure that encapsulates all bot components
pub struct XemmBot {
    pub config: Config,
    pub bot_state: Arc<RwLock<BotState>>,

    // Trading clients (each task gets its own instance to avoid lock contention)
    pub pacifica_trading_main: Arc<PacificaTrading>,
    pub pacifica_trading_fill: Arc<PacificaTrading>,
    pub pacifica_trading_rest_fill: Arc<PacificaTrading>,
    pub pacifica_trading_monitor: Arc<PacificaTrading>,
    pub pacifica_trading_hedge: Arc<PacificaTrading>,
    pub pacifica_trading_rest_poll: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,

    // Shared state (prices)
    pub pacifica_prices: Arc<Mutex<(f64, f64)>>, // (bid, ask)
    pub hyperliquid_prices: Arc<Mutex<(f64, f64)>>, // (bid, ask)

    // Opportunity evaluator
    pub evaluator: OpportunityEvaluator,

    // Fill tracking state
    pub processed_fills: Arc<parking_lot::Mutex<HashSet<String>>>,
    pub last_position_snapshot: Arc<parking_lot::Mutex<Option<PositionSnapshot>>>,

    // Channels
    pub hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pub hedge_rx: Option<mpsc::UnboundedReceiver<HedgeEvent>>,
    pub shutdown_tx: mpsc::Sender<()>,
    pub shutdown_rx: Option<mpsc::Receiver<()>>,

    // Credentials (needed for spawning services)
    pub pacifica_credentials: PacificaCredentials,
}

impl XemmBot {
    /// Create and initialize a new XemmBot instance
    ///
    /// This performs all the wiring:
    /// - Loads config and validates it
    /// - Loads credentials from environment
    /// - Creates all trading clients
    /// - Pre-fetches Hyperliquid metadata
    /// - Cancels existing orders
    /// - Fetches Pacifica tick size
    /// - Creates OpportunityEvaluator
    /// - Initializes shared state and channels
    pub async fn new() -> Result<Self> {
        use colored::Colorize;

        println!(
            "{} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "═══════════════════════════════════════════════════"
                .bright_cyan()
                .bold()
        );
        println!(
            "{} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "  XEMM Bot - Cross-Exchange Market Making"
                .bright_cyan()
                .bold()
        );
        println!(
            "{} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "═══════════════════════════════════════════════════"
                .bright_cyan()
                .bold()
        );
        println!();

        // Load configuration
        let config = Config::load_default().context("Failed to load config.json")?;
        config.validate().context("Invalid configuration")?;

        println!(
            "{} {} Symbol: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            config.symbol.bright_white().bold()
        );
        println!(
            "{} {} Order Notional: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("${:.2}", config.order_notional_usd).bright_white()
        );
        println!(
            "{} {} Pacifica Maker Fee: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.pacifica_maker_fee_bps).bright_white()
        );
        println!(
            "{} {} Hyperliquid Taker Fee: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.hyperliquid_taker_fee_bps).bright_white()
        );
        println!(
            "{} {} Target Profit: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.profit_rate_bps).green().bold()
        );
        println!(
            "{} {} Profit Cancel Threshold: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.profit_cancel_threshold_bps).yellow()
        );
        println!(
            "{} {} Order Refresh Interval: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} secs", config.order_refresh_interval_secs).bright_white()
        );
        println!(
            "{} {} Pacifica REST Poll Interval: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{} secs", config.pacifica_rest_poll_interval_secs).bright_white()
        );
        println!(
            "{} {} Hyperliquid Market Order maximum allowed Slippage: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[CONFIG]".blue().bold(),
            format!("{}%", config.hyperliquid_slippage * 100.0).bright_white()
        );
        println!();

        // Load credentials
        dotenv::dotenv().ok();
        let pacifica_credentials =
            PacificaCredentials::from_env().context("Failed to load Pacifica credentials from environment")?;
        let hyperliquid_credentials =
            HyperliquidCredentials::from_env().context("Failed to load Hyperliquid credentials from environment")?;

        println!(
            "{} {} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            "Credentials loaded successfully".green()
        );

        // Initialize trading clients
        let pacifica_trading_main = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create main Pacifica trading client")?,
        );
        let pacifica_trading_fill = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create fill detection Pacifica trading client")?,
        );
        let pacifica_trading_rest_fill = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create REST fill detection Pacifica trading client")?,
        );
        let pacifica_trading_monitor = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create monitor Pacifica trading client")?,
        );
        let pacifica_trading_hedge = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create hedge Pacifica trading client")?,
        );
        let pacifica_trading_rest_poll = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create REST polling Pacifica trading client")?,
        );

        // Initialize WebSocket trading client for ultra-fast cancellations
        let pacifica_ws_trading = Arc::new(PacificaWsTrading::new(pacifica_credentials.clone(), false)); // false = mainnet

        let hyperliquid_trading = Arc::new(
            HyperliquidTrading::new(hyperliquid_credentials, false)
                .context("Failed to create Hyperliquid trading client")?,
        );

        println!(
            "{} {} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            "Trading clients initialized (6 REST instances + WebSocket)".green()
        );

        // Pre-fetch Hyperliquid metadata (szDecimals, etc.) to reduce hedge latency
        println!(
            "{} {} Pre-fetching Hyperliquid metadata for {}...",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            config.symbol.bright_white()
        );
        hyperliquid_trading
            .get_meta()
            .await
            .context("Failed to pre-fetch Hyperliquid metadata")?;
        println!(
            "{} {} {} Hyperliquid metadata cached",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            "✓".green().bold()
        );

        // Cancel any existing orders on Pacifica at startup
        println!(
            "{} {} Cancelling any existing orders on Pacifica...",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold()
        );
        match pacifica_trading_main
            .cancel_all_orders(false, Some(&config.symbol), false)
            .await
        {
            Ok(count) => println!(
                "{} {} {} Cancelled {} existing order(s)",
                chrono::Utc::now()
                    .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                    .to_string()
                    .bright_black(),
                "[INIT]".cyan().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => println!(
                "{} {} {} Failed to cancel existing orders: {}",
                chrono::Utc::now()
                    .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                    .to_string()
                    .bright_black(),
                "[INIT]".cyan().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }

        // Get market info to determine tick size
        let pacifica_tick_size: f64 = {
            let market_info = pacifica_trading_main
                .get_market_info()
                .await
                .context("Failed to fetch Pacifica market info")?;
            let symbol_info = market_info
                .get(&config.symbol)
                .with_context(|| format!("Symbol {} not found in market info", config.symbol))?;
            symbol_info.tick_size.parse().context("Failed to parse tick size")?
        };

        println!(
            "{} {} Pacifica tick size for {}: {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            config.symbol.bright_white(),
            format!("{}", pacifica_tick_size).bright_white()
        );

        // Create opportunity evaluator
        let evaluator = OpportunityEvaluator::new(
            config.pacifica_maker_fee_bps,
            config.hyperliquid_taker_fee_bps,
            config.profit_rate_bps,
            pacifica_tick_size,
        );

        println!(
            "{} {} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            "Opportunity evaluator created".green()
        );

        // Shared state for orderbook prices
        let pacifica_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)
        let hyperliquid_prices = Arc::new(Mutex::new((0.0, 0.0))); // (bid, ask)

        // Shared bot state
        let bot_state = Arc::new(RwLock::new(BotState::new()));

        // Channels for communication
        // Unbounded hedge event queue: producers never block when enqueueing,
        // hedge executor processes events sequentially.
        let (hedge_tx, hedge_rx) = mpsc::unbounded_channel::<HedgeEvent>(); // (side, size, avg_price, fill_timestamp)
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

        // Fill tracking state
        let processed_fills = Arc::new(parking_lot::Mutex::new(HashSet::<String>::new()));
        let last_position_snapshot = Arc::new(parking_lot::Mutex::new(Option::<PositionSnapshot>::None));

        println!(
            "{} {} {}",
            chrono::Utc::now()
                .format("%Y-%m-%dT%H:%M:%S%.6fZ")
                .to_string()
                .bright_black(),
            "[INIT]".cyan().bold(),
            "State and channels initialized".green()
        );
        println!();

        Ok(XemmBot {
            config,
            bot_state,
            pacifica_trading_main,
            pacifica_trading_fill,
            pacifica_trading_rest_fill,
            pacifica_trading_monitor,
            pacifica_trading_hedge,
            pacifica_trading_rest_poll,
            pacifica_ws_trading,
            hyperliquid_trading,
            pacifica_prices,
            hyperliquid_prices,
            evaluator,
            processed_fills,
            last_position_snapshot,
            hedge_tx,
            hedge_rx: Some(hedge_rx),
            shutdown_tx,
            shutdown_rx: Some(shutdown_rx),
            pacifica_credentials,
        })
    }

    /// Run the bot - spawn all services and execute main loop
    pub async fn run(mut self) -> Result<()> {
        // ═══════════════════════════════════════════════════
        // SPAWN ALL SERVICES
        // ═══════════════════════════════════════════════════

        // Service 1: Pacifica Orderbook (WebSocket)
        let pacifica_ob_service = PacificaOrderbookService {
            prices: self.pacifica_prices.clone(),
            symbol: self.config.symbol.clone(),
            agg_level: self.config.agg_level,
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        };
        tokio::spawn(async move {
            pacifica_ob_service.run().await.ok();
        });

        // Service 2: Hyperliquid Orderbook (WebSocket)
        let hyperliquid_ob_service = HyperliquidOrderbookService {
            prices: self.hyperliquid_prices.clone(),
            symbol: self.config.symbol.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        };
        tokio::spawn(async move {
            hyperliquid_ob_service.run().await.ok();
        });
        let fill_config = FillDetectionConfig {
            account: self.pacifica_credentials.account.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
            enable_position_fill_detection: true,
        };
        let fill_client = FillDetectionClient::new(fill_config.clone(), false)
            .context("Failed to create fill detection client")?;
        let baseline_updater = fill_client.get_baseline_updater();

        let fill_service = FillDetectionService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            pacifica_trading: self.pacifica_trading_fill.clone(),
            pacifica_ws_trading: self.pacifica_ws_trading.clone(),
            fill_config,
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            baseline_updater,
        };
        tokio::spawn(async move {
            fill_service.run().await;
        });

        // Service 4: Pacifica REST Poll (price redundancy)
        let pacifica_rest_poll_service = PacificaRestPollService {
            prices: self.pacifica_prices.clone(),
            pacifica_trading: self.pacifica_trading_rest_poll.clone(),
            symbol: self.config.symbol.clone(),
            agg_level: self.config.agg_level,
            poll_interval_secs: self.config.pacifica_rest_poll_interval_secs,
        };
        tokio::spawn(async move {
            pacifica_rest_poll_service.run().await;
        });

        // Service 4.5: Hyperliquid REST Poll (price redundancy)
        let hyperliquid_rest_poll_service = HyperliquidRestPollService {
            prices: self.hyperliquid_prices.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            symbol: self.config.symbol.clone(),
            poll_interval_secs: 2,
        };
        tokio::spawn(async move {
            hyperliquid_rest_poll_service.run().await;
        });

        // Wait for initial orderbook data
        tprintln!("{} Waiting for orderbook data...", "[INIT]".cyan().bold());
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Service 5: REST Fill Detection (backup)
        let rest_fill_service = RestFillDetectionService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            pacifica_trading: self.pacifica_trading_rest_fill.clone(),
            pacifica_ws_trading: self.pacifica_ws_trading.clone(),
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            min_hedge_notional: 10.0,
        };
        tokio::spawn(async move {
            rest_fill_service.run().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Service 5.5: Position Monitor (ground truth)
        let pacifica_trading_position = Arc::new(
            PacificaTrading::new(self.pacifica_credentials.clone())
                .context("Failed to create position monitor trading client")?
        );
        let position_monitor_service = PositionMonitorService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            pacifica_trading: pacifica_trading_position,
            pacifica_ws_trading: self.pacifica_ws_trading.clone(),
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            last_position_snapshot: self.last_position_snapshot.clone(),
        };
        tokio::spawn(async move {
            position_monitor_service.run().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Service 6: Order Monitor (age/profit monitoring)
        let order_monitor_service = OrderMonitorService {
            bot_state: self.bot_state.clone(),
            pacifica_prices: self.pacifica_prices.clone(),
            hyperliquid_prices: self.hyperliquid_prices.clone(),
            config: self.config.clone(),
            evaluator: self.evaluator.clone(),
            pacifica_trading: self.pacifica_trading_monitor.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
        };
        tokio::spawn(async move {
            order_monitor_service.run().await;
        });

        // Service 7: Hedge Execution
        let hedge_service = HedgeService {
            bot_state: self.bot_state.clone(),
            hedge_rx: self.hedge_rx.take().unwrap(),
            hyperliquid_prices: self.hyperliquid_prices.clone(),
            config: self.config.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            pacifica_trading: self.pacifica_trading_hedge.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        };
        tokio::spawn(async move {
            hedge_service.run().await;
        });

        // ═══════════════════════════════════════════════════
        // MAIN OPPORTUNITY EVALUATION LOOP
        // ═══════════════════════════════════════════════════

        tprintln!("{} Starting opportunity evaluation loop",
            format!("[{} MAIN]", self.config.symbol).bright_white().bold()
        );
        tprintln!("");

        let mut eval_interval = interval(Duration::from_millis(1));
        let mut order_placement_rate_limit = RateLimitTracker::new();

        // Helper async function to wait for SIGTERM
        async fn wait_for_sigterm() {
            #[cfg(unix)]
            {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to setup SIGTERM handler");
                sigterm.recv().await;
            }

            #[cfg(not(unix))]
            {
                std::future::pending::<()>().await;
            }
        }

        let mut shutdown_rx = self.shutdown_rx.take().unwrap();

        loop {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    tprintln!("{} {} Received SIGINT (Ctrl+C), initiating graceful shutdown...",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold(),
                        "⚠".yellow().bold()
                    );
                    break;
                }

                _ = wait_for_sigterm() => {
                    tprintln!("{} {} Received SIGTERM (Docker shutdown), initiating graceful shutdown...",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold(),
                        "⚠".yellow().bold()
                    );
                    break;
                }

                _ = eval_interval.tick() => {
                    // Check if we should exit
                    let state = self.bot_state.read().await;
                    if state.is_terminal() {
                        break;
                    }

                    // Only evaluate if idle
                    if !state.is_idle() {
                        continue;
                    }

                    // Check grace period
                    if !state.grace_period_elapsed(3) {
                        continue;
                    }
                    drop(state);

                    // Check rate limit backoff
                    if order_placement_rate_limit.should_skip() {
                        let remaining = order_placement_rate_limit.remaining_backoff_secs();
                        if remaining as u64 % 5 == 0 || remaining < 1.0 {
                            debug!("[MAIN] Skipping order placement (rate limit backoff, {:.1}s remaining)", remaining);
                        }
                        continue;
                    }

                    // Get current prices
                    let (pac_bid, pac_ask) = *self.pacifica_prices.lock().unwrap();
                    let (hl_bid, hl_ask) = *self.hyperliquid_prices.lock().unwrap();

                    // Validate prices
                    if pac_bid == 0.0 || pac_ask == 0.0 || hl_bid == 0.0 || hl_ask == 0.0 {
                        continue;
                    }

                    // Evaluate opportunities
                    let buy_opp = self.evaluator.evaluate_buy_opportunity(hl_bid, self.config.order_notional_usd);
                    let sell_opp = self.evaluator.evaluate_sell_opportunity(hl_ask, self.config.order_notional_usd);

                    let pac_mid = (pac_bid + pac_ask) / 2.0;
                    let best_opp = OpportunityEvaluator::pick_best_opportunity(buy_opp, sell_opp, pac_mid);

                    if let Some(opp) = best_opp {
                        // Double-check bot is still idle
                        let mut state = self.bot_state.write().await;
                        if !state.is_idle() {
                            continue;
                        }

                        tprintln!(
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

                        // Place order
                        tprintln!("{} Placing {} on Pacifica...",
                            format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                            opp.direction.as_str().bright_yellow().bold()
                        );

                        let pacifica_side = match opp.direction {
                            OrderSide::Buy => PacificaOrderSide::Buy,
                            OrderSide::Sell => PacificaOrderSide::Sell,
                        };

                        match self.pacifica_trading_main
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
                                    tprintln!(
                                        "{} {} Placed {} #{} @ {} | cloid: {}...{}",
                                        format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                                        "✓".green().bold(),
                                        opp.direction.as_str().bright_yellow(),
                                        order_id,
                                        format!("${:.4}", opp.pacifica_price).cyan().bold(),
                                        &client_order_id[..8],
                                        &client_order_id[client_order_id.len()-4..]
                                    );

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
                                } else {
                                    tprintln!("{} {} Order placed but no client_order_id returned",
                                        format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                                        "✗".red().bold()
                                    );
                                }
                            }
                            Err(e) => {
                                if is_rate_limit_error(&e) {
                                    order_placement_rate_limit.record_error();
                                    let backoff_secs = order_placement_rate_limit.get_backoff_secs();
                                    tprintln!(
                                        "{} {} Failed to place order: Rate limit exceeded. Backing off for {}s (attempt #{})",
                                        format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                                        "⚠".yellow().bold(),
                                        backoff_secs,
                                        order_placement_rate_limit.consecutive_errors()
                                    );
                                } else {
                                    tprintln!("{} {} Failed to place order: {}",
                                        format!("[{} ORDER]", self.config.symbol).bright_yellow().bold(),
                                        "✗".red().bold(),
                                        e.to_string().red()
                                    );
                                }
                            }
                        }
                    }
                }

                _ = shutdown_rx.recv() => {
                    tprintln!("{} Shutdown signal received",
                        format!("[{} MAIN]", self.config.symbol).bright_white().bold()
                    );
                    break;
                }
            }
        }

        // ═══════════════════════════════════════════════════
        // SHUTDOWN CLEANUP
        // ═══════════════════════════════════════════════════

        tprintln!("");
        tprintln!("{} Cancelling any remaining orders...",
            format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold()
        );

        match self.pacifica_trading_main.cancel_all_orders(false, Some(&self.config.symbol), false).await {
            Ok(count) => tprintln!("{} {} Cancelled {} order(s)",
                format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => tprintln!("{} {} Failed to cancel orders: {}",
                format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }

        // Final state check
        let final_state = self.bot_state.read().await;
        match &final_state.status {
            BotStatus::Complete => {
                tprintln!("");
                tprintln!("{} {}", "✓".green().bold(), "Bot completed successfully!".green().bold());
                tprintln!("Final position: {}", final_state.position);
                Ok(())
            }
            BotStatus::Error(e) => {
                tprintln!("");
                tprintln!("{} {}: {}", "✗".red().bold(), "Bot terminated with error".red().bold(), e.to_string().red());
                anyhow::bail!("Bot failed: {}", e)
            }
            _ => {
                tprintln!("");
                tprintln!("{} Bot terminated in unexpected state: {:?}", "⚠".yellow().bold(), final_state.status);
                Ok(())
            }
        }
    }
}
