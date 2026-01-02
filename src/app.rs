use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info};

use crate::bot::{ActiveOrder, BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, PacificaCredentials, PacificaTrading,
    PacificaWsTrading, OrderSide as PacificaOrderSide,
};
use crate::services::{
    fill_detection::FillDetectionService, hedge::HedgeService,
    order_monitor::{AtomicBotStatus, OrderMonitorService, SharedOrderSnapshot, spawn_monitor_tasks, sync_atomic_status, update_order_snapshot},
    orderbook::{HyperliquidOrderbookService, PacificaOrderbookService},
    market_event::{MarketEventHub, MarketSource},
    position_monitor::PositionMonitorService, rest_fill_detection::RestFillDetectionService,
    rest_poll::{HyperliquidRestPollService, PacificaRestPollService}, HedgeEvent,
};
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};
use crate::util::atomic_price::AtomicPrice;



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
    pub pacifica_prices: Arc<AtomicPrice>, // (bid, ask)
    pub hyperliquid_prices: Arc<AtomicPrice>, // (bid, ask)

    // Opportunity evaluator
    pub evaluator: OpportunityEvaluator,

    // Fill tracking state
    pub processed_fills: Arc<parking_lot::Mutex<HashSet<String>>>,
    pub last_position_snapshot: Arc<parking_lot::Mutex<Option<PositionSnapshot>>>,

    // Order monitor state (lock-free)
    pub atomic_status: Arc<AtomicU8>,
    pub last_cancel_ms: Arc<AtomicU64>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,

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

        info!("{}", "═══════════════════════════════════════════════════"
                .bright_cyan()
                .bold()
        );
        info!("{}", "  XEMM Bot - Cross-Exchange Market Making"
                .bright_cyan()
                .bold()
        );
        info!("{}", "═══════════════════════════════════════════════════"
                .bright_cyan()
                .bold()
        );
        info!("");

        // Load configuration
        let config = Config::load_default().context("Failed to load config.json")?;
        config.validate().context("Invalid configuration")?;

        info!("{} Symbol: {}",
            "[CONFIG]".blue().bold(),
            config.symbol.bright_white().bold()
        );
        info!("{} Order Notional: {}",
            "[CONFIG]".blue().bold(),
            format!("${:.2}", config.order_notional_usd).bright_white()
        );
        info!("{} Pacifica Maker Fee: {}",
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.pacifica_maker_fee_bps).bright_white()
        );
        info!("{} Hyperliquid Taker Fee: {}",
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.hyperliquid_taker_fee_bps).bright_white()
        );
        info!("{} Target Profit: {}",
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.profit_rate_bps).green().bold()
        );
        info!("{} Profit Cancel Threshold: {}",
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.profit_cancel_threshold_bps).yellow()
        );
        info!("{} Order Refresh Interval: {}",
            "[CONFIG]".blue().bold(),
            format!("{} secs", config.order_refresh_interval_secs).bright_white()
        );
        info!("{} Pacifica REST Poll Interval: {}",
            "[CONFIG]".blue().bold(),
            format!("{} secs", config.pacifica_rest_poll_interval_secs).bright_white()
        );
        info!("{} Active Order REST Poll Interval: {}",
            "[CONFIG]".blue().bold(),
            format!("{} ms", config.pacifica_active_order_rest_poll_interval_ms).bright_white()
        );
        info!("{} Hyperliquid Market Order maximum allowed Slippage: {}",
            "[CONFIG]".blue().bold(),
            format!("{}%", config.hyperliquid_slippage * 100.0).bright_white()
        );
        info!("");

        // Load credentials
        dotenv::dotenv().ok();
        let pacifica_credentials =
            PacificaCredentials::from_env().context("Failed to load Pacifica credentials from environment")?;
        let hyperliquid_credentials =
            HyperliquidCredentials::from_env().context("Failed to load Hyperliquid credentials from environment")?;

        info!("{} {}",
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

        info!("{} {}",
            "[INIT]".cyan().bold(),
            "Trading clients initialized (6 REST instances + WebSocket)".green()
        );

        // Pre-fetch Hyperliquid metadata (szDecimals, etc.) to reduce hedge latency
        info!("{} Pre-fetching Hyperliquid metadata for {}...",
            "[INIT]".cyan().bold(),
            config.symbol.bright_white()
        );
        hyperliquid_trading
            .get_meta()
            .await
            .context("Failed to pre-fetch Hyperliquid metadata")?;
        info!("{} {} Hyperliquid metadata cached",
            "[INIT]".cyan().bold(),
            "✓".green().bold()
        );

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

        info!("{} Pacifica tick size for {}: {}",
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

        info!("{} {}",
            "[INIT]".cyan().bold(),
            "Opportunity evaluator created".green()
        );

        // Shared state for orderbook prices
        let pacifica_prices = Arc::new(AtomicPrice::new()); // (bid, ask)
        let hyperliquid_prices = Arc::new(AtomicPrice::new()); // (bid, ask)

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

        info!("{} {}",
            "[INIT]".cyan().bold(),
            "State and channels initialized".green()
        );
        info!("");

        // Initialize order monitor state (shared atomics)
        let (atomic_status, last_cancel_ms) = {
            let state = bot_state.read().await;
            (state.status_atomic.clone(), state.last_cancel_ms.clone())
        };
        let order_snapshot = Arc::new(SharedOrderSnapshot::new());

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
            atomic_status,
            last_cancel_ms,
            order_snapshot,
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
        // PRE-FLIGHT: ORDERBOOK WS + TRADING API SANITY
        // ═══════════════════════════════════════════════════
        let market_events = Arc::new(MarketEventHub::new(1024));

        // Service 1: Pacifica Orderbook (WebSocket)
        let pacifica_ob_service = PacificaOrderbookService {
            prices: self.pacifica_prices.clone(),
            market_events: market_events.clone(),
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
            market_events: market_events.clone(),
            symbol: self.config.symbol.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        };
        tokio::spawn(async move {
            hyperliquid_ob_service.run().await.ok();
        });

        self.wait_for_ws_ready(&market_events).await?;
        self.verify_trading_ready().await?;

        // Cancel any existing orders on Pacifica only after sanity checks
        self.cancel_existing_orders().await?;

        // ═══════════════════════════════════════════════════
        // SPAWN ALL SERVICES
        // ═══════════════════════════════════════════════════

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
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
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

        // Service 5: REST Fill Detection (backup)
        let rest_fill_service = RestFillDetectionService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            pacifica_trading: self.pacifica_trading_rest_fill.clone(),
            pacifica_ws_trading: self.pacifica_ws_trading.clone(),
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            min_hedge_notional: 10.0,
            poll_interval_ms: self.config.pacifica_active_order_rest_poll_interval_ms,
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
        let (order_monitor_service, cancel_rx) = OrderMonitorService::new(
            self.bot_state.clone(),
            self.atomic_status.clone(),
            self.order_snapshot.clone(),
            self.pacifica_prices.clone(),
            self.hyperliquid_prices.clone(),
            self.config.clone(),
            self.evaluator.clone(),
            self.pacifica_trading_monitor.clone(),
            self.hyperliquid_trading.clone(),
        );
        let order_monitor_service = Arc::new(order_monitor_service);
        spawn_monitor_tasks(order_monitor_service.clone(), cancel_rx);

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

        info!("{} Starting opportunity evaluation loop",
            format!("[{} MAIN]", self.config.symbol).bright_white().bold()
        );
        info!("");

        let mut order_placement_rate_limit = RateLimitTracker::new();

        let sigint = signal::ctrl_c();
        tokio::pin!(sigint);
        
        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to setup SIGTERM handler");

        let mut shutdown_rx = self.shutdown_rx.take().unwrap();

        let now_epoch_ms = || -> u64 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        };

        'main: loop {
            while let Some(event) = market_events.pop() {
                let status = self.atomic_status.load(Ordering::Acquire);
                if status == AtomicBotStatus::Complete as u8 || status == AtomicBotStatus::Error as u8 {
                    break 'main;
                }

                if status == AtomicBotStatus::OrderPlaced as u8 {
                    order_monitor_service.check_on_market_event(event.source);
                    continue;
                }

                if status != AtomicBotStatus::Idle as u8 {
                    continue;
                }

                let local_now_ms = now_epoch_ms();
                let opp_timestamp = if event.ts > 0 { event.ts } else { local_now_ms };
                let last_cancel_ms = self.last_cancel_ms.load(Ordering::Acquire);
                if last_cancel_ms != 0 && local_now_ms.saturating_sub(last_cancel_ms) < 3_000 {
                    continue;
                }

                if order_placement_rate_limit.should_skip() {
                    let remaining = order_placement_rate_limit.remaining_backoff_secs();
                    if remaining as u64 % 5 == 0 || remaining < 1.0 {
                        debug!(
                            "[MAIN] Skipping order placement (rate limit backoff, {:.1}s remaining)",
                            remaining
                        );
                    }
                    continue;
                }

                let pac_snapshot = self.pacifica_prices.load();
                let hl_snapshot = self.hyperliquid_prices.load();
                let (pac_bid, pac_ask) = (pac_snapshot.bid, pac_snapshot.ask);
                let (hl_bid, hl_ask) = (hl_snapshot.bid, hl_snapshot.ask);

                if pac_bid == 0.0 || pac_ask == 0.0 || hl_bid == 0.0 || hl_ask == 0.0 {
                    continue;
                }

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
                    if self.atomic_status.load(Ordering::Acquire)
                        != AtomicBotStatus::Idle as u8
                    {
                        continue;
                    }

                    let mut state = self.bot_state.write().await;
                    if !state.is_idle() {
                        continue;
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

        // ═══════════════════════════════════════════════════
        // SHUTDOWN CLEANUP
        // ═══════════════════════════════════════════════════

        info!("");
        info!("{} Cancelling any remaining orders...",
            format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold()
        );

        match self.pacifica_trading_main.cancel_all_orders(false, Some(&self.config.symbol), false).await {
            Ok(count) => info!("{} {} Cancelled {} order(s)",
                format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => info!("{} {} Failed to cancel orders: {}",
                format!("[{} SHUTDOWN]", self.config.symbol).yellow().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }

        // Final state check
        let final_state = self.bot_state.read().await;
        match &final_state.status {
            BotStatus::Complete => {
                info!("");
                info!("{} {}", "✓".green().bold(), "Bot completed successfully!".green().bold());
                info!("Final position: {}", final_state.position);
                Ok(())
            }
            BotStatus::Error(e) => {
                info!("");
                info!("{} {}: {}", "✗".red().bold(), "Bot terminated with error".red().bold(), e.to_string().red());
                anyhow::bail!("Bot failed: {}", e)
            }
            _ => {
                info!("");
                info!("{} Bot terminated in unexpected state: {:?}", "⚠".yellow().bold(), final_state.status);
                Ok(())
            }
        }
    }

    async fn wait_for_ws_ready(&self, market_events: &MarketEventHub) -> Result<()> {
        const WS_READY_TIMEOUT_SECS: u64 = 10;
        let deadline = Instant::now() + Duration::from_secs(WS_READY_TIMEOUT_SECS);
        let mut pacifica_ready = false;
        let mut hyperliquid_ready = false;

        info!(
            "{} Waiting for orderbook WebSocket readiness (timeout {}s)...",
            "[INIT]".cyan().bold(),
            WS_READY_TIMEOUT_SECS
        );

        while Instant::now() < deadline {
            while let Some(event) = market_events.pop() {
                match event.source {
                    MarketSource::Pacifica => pacifica_ready = true,
                    MarketSource::Hyperliquid => hyperliquid_ready = true,
                }
                if pacifica_ready && hyperliquid_ready {
                    info!("{} {} Orderbook WebSockets ready",
                        "[INIT]".cyan().bold(),
                        "✓".green().bold()
                    );
                    return Ok(());
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let _ = tokio::time::timeout(remaining, market_events.notifier().notified()).await;
        }

        anyhow::bail!("Orderbook WebSockets did not become ready before timeout");
    }

    async fn verify_trading_ready(&self) -> Result<()> {
        self.verify_pacifica_trading().await?;
        self.verify_hyperliquid_trading().await?;
        Ok(())
    }

    async fn verify_pacifica_trading(&self) -> Result<()> {
        info!("{} Verifying Pacifica trading API access...",
            "[INIT]".cyan().bold()
        );

        let open_orders = self
            .pacifica_trading_main
            .get_open_orders()
            .await
            .context("Pacifica trading API check failed (open orders)")?;

        info!("{} {} Pacifica trading API ready ({} open orders)",
            "[INIT]".cyan().bold(),
            "✓".green().bold(),
            open_orders.len()
        );
        Ok(())
    }

    async fn verify_hyperliquid_trading(&self) -> Result<()> {
        info!("{} Verifying Hyperliquid trading API access...",
            "[INIT]".cyan().bold()
        );

        let derived_wallet = self.hyperliquid_trading.get_wallet_address();
        if let Ok(expected_wallet) = std::env::var("HL_WALLET") {
            let expected = normalize_address(&expected_wallet);
            let derived = normalize_address(&derived_wallet);
            if expected != derived {
                anyhow::bail!(
                    "HL_WALLET mismatch: expected {}, derived {}",
                    expected_wallet,
                    derived_wallet
                );
            }
        }

        self.hyperliquid_trading
            .get_user_state(&derived_wallet)
            .await
            .context("Hyperliquid info endpoint unavailable (user state)")?;

        self.hyperliquid_trading
            .sanity_check_signing()
            .await
            .context("Hyperliquid signing sanity check failed")?;

        info!("{} {} Hyperliquid trading API ready",
            "[INIT]".cyan().bold(),
            "✓".green().bold()
        );
        Ok(())
    }

    async fn cancel_existing_orders(&self) -> Result<()> {
        info!("{} Cancelling any existing orders on Pacifica...",
            "[INIT]".cyan().bold()
        );

        match self.pacifica_trading_main
            .cancel_all_orders(false, Some(&self.config.symbol), false)
            .await
        {
            Ok(count) => info!("{} {} Cancelled {} existing order(s)",
                "[INIT]".cyan().bold(),
                "✓".green().bold(),
                count
            ),
            Err(e) => info!("{} {} Failed to cancel existing orders: {}",
                "[INIT]".cyan().bold(),
                "⚠".yellow().bold(),
                e
            ),
        }

        Ok(())
    }
}

fn normalize_address(address: &str) -> String {
    address.trim().to_lowercase()
}
