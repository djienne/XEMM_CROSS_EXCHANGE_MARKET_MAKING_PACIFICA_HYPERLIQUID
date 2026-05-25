use anyhow::{Context, Result};
use colored::Colorize;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::bot::{ActiveOrder, BotState, BotStatus, RunState};
use crate::config::Config;
use crate::connector::hyperliquid::trading::HyperliquidPoller;
use crate::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use crate::connector::hyperliquid::{
    OrderbookClient as HlOrderbookClient, OrderbookConfig as HlOrderbookConfig,
};
use crate::connector::pacifica::trading::PacificaPoller;
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, OrderSide as PacificaOrderSide, PacificaCredentials,
    PacificaTrading, PacificaWsTrading,
};
use crate::connector::pacifica::{
    OrderbookClient as PacOrderbookClient, OrderbookConfig as PacOrderbookConfig,
};
use crate::services::{
    cancel_manager::CancelManagerService,
    enqueue_hedge_intent,
    fill_aggregator::FillAggregator,
    fill_dedup::FillDedup,
    fill_detection::FillDetectionService,
    hedge::HedgeService,
    hedge_store,
    order_monitor::{
        spawn_monitor_tasks, update_order_snapshot, OrderMonitorService, SharedOrderSnapshot,
    },
    position_monitor::PositionMonitorService,
    position_reconciler::PositionReconcilerService,
    post_trade_auditor::PostTradeAuditorService,
    price_source::{PricePollService, PriceStreamService},
    rest_fill_detection::RestFillDetectionService,
    safety_monitor::SafetyMonitorService,
    supervisor::spawn_supervised,
    trade_gate::{GateReason, TradeGate},
    HedgeIntent,
};
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::cancel::dual_cancel;
use crate::util::log::{tag, tag_static, Color};
use crate::util::price::{QuoteSource, SharedQuote};
use crate::util::rate_limit::{
    is_rate_limit_error, EndpointGroup, RateLimitBook, RateLimitPriority,
};

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

    // Trading clients. `PacificaTrading` wraps a cloneable, thread-safe
    // `reqwest::Client` and an `Arc<Mutex<_>>` metadata cache, so all services
    // share a single instance - the previous 6 instances each kept their own
    // connection pool and cache with no concurrency benefit.
    pub pacifica_trading: Arc<PacificaTrading>,
    pub pacifica_ws_trading: Arc<PacificaWsTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,

    // Shared state (prices)
    pub pacifica_prices: Arc<SharedQuote>,    // (bid, ask)
    pub hyperliquid_prices: Arc<SharedQuote>, // (bid, ask)

    // Opportunity evaluator
    pub evaluator: OpportunityEvaluator,

    // Fill tracking state (bounded dedup keyed on client_order_id + cumulative size)
    pub processed_fills: Arc<FillDedup>,
    // Partial-fill aggregator (residual hedge deltas per order cycle)
    pub fill_aggregator: Arc<FillAggregator>,
    pub last_position_snapshot: Arc<parking_lot::Mutex<Option<PositionSnapshot>>>,

    // Order monitor state (lock-free)
    pub atomic_status: Arc<AtomicU8>,
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    pub trade_gate: Arc<TradeGate>,
    pub pacifica_tick_size: String,
    pub pacifica_lot_size: String,

    // Channels
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub hedge_rx: Option<mpsc::Receiver<HedgeIntent>>,
    pub shutdown_tx: mpsc::Sender<()>,
    pub shutdown_rx: Option<mpsc::Receiver<()>>,

    // Hedge-service drain signal. Fired on SIGINT/SIGTERM so any queued hedge
    // event completes before the process exits.
    pub hedge_shutdown_signal: Arc<tokio::sync::Notify>,

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

        info!(
            "{}",
            "==================================================="
                .bright_cyan()
                .bold()
        );
        info!(
            "{}",
            "  XEMM Bot - Cross-Exchange Market Making"
                .bright_cyan()
                .bold()
        );
        info!(
            "{}",
            "==================================================="
                .bright_cyan()
                .bold()
        );
        info!("");

        // Load configuration
        let config = Config::load_default().context("Failed to load config.json")?;
        config.validate().context("Invalid configuration")?;

        info!(
            "{} Symbol: {}",
            tag_static("CONFIG", Color::Blue),
            config.symbol.bright_white().bold()
        );
        info!(
            "{} Order Notional: {}",
            tag_static("CONFIG", Color::Blue),
            format!("${:.2}", config.order_notional_usd).bright_white()
        );
        info!(
            "{} Pacifica Maker Fee: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} bps", config.pacifica_maker_fee_bps).bright_white()
        );
        info!(
            "{} Hyperliquid Taker Fee: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} bps", config.hyperliquid_taker_fee_bps).bright_white()
        );
        info!(
            "{} Target Profit: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} bps", config.profit_rate_bps).green().bold()
        );
        info!(
            "{} Profit Cancel Threshold: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} bps", config.profit_cancel_threshold_bps).yellow()
        );
        info!(
            "{} Order Refresh Interval: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} secs", config.order_refresh_interval_secs).bright_white()
        );
        info!(
            "{} Pacifica REST Poll Interval: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} secs", config.pacifica_rest_poll_interval_secs).bright_white()
        );
        info!(
            "{} Active Order REST Poll Interval: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{} ms", config.pacifica_active_order_rest_poll_interval_ms).bright_white()
        );
        info!(
            "{} Hyperliquid Market Order maximum allowed Slippage: {}",
            tag_static("CONFIG", Color::Blue),
            format!("{}%", config.hyperliquid_slippage * 100.0).bright_white()
        );
        info!("");

        // Load credentials
        dotenv::dotenv().ok();
        let pacifica_credentials = PacificaCredentials::from_env()
            .context("Failed to load Pacifica credentials from environment")?;
        let hyperliquid_credentials = HyperliquidCredentials::from_env()
            .context("Failed to load Hyperliquid credentials from environment")?;

        info!(
            "{} {}",
            tag_static("INIT", Color::Cyan),
            "Credentials loaded successfully".green()
        );

        // Initialize trading client (single shared instance across all services)
        let pacifica_trading = Arc::new(
            PacificaTrading::new(pacifica_credentials.clone())
                .context("Failed to create Pacifica trading client")?,
        );

        // Initialize WebSocket trading client for ultra-fast cancellations
        let pacifica_ws_trading =
            Arc::new(PacificaWsTrading::new(pacifica_credentials.clone(), false)); // false = mainnet

        let hyperliquid_trading = Arc::new(
            HyperliquidTrading::new(hyperliquid_credentials, false)
                .context("Failed to create Hyperliquid trading client")?,
        );

        info!(
            "{} {}",
            tag_static("INIT", Color::Cyan),
            "Trading clients initialized (shared REST + WebSocket)".green()
        );

        // Pre-fetch Hyperliquid metadata (szDecimals, etc.) to reduce hedge latency
        info!(
            "{} Pre-fetching Hyperliquid metadata for {}...",
            tag_static("INIT", Color::Cyan),
            config.symbol.bright_white()
        );
        hyperliquid_trading
            .get_meta()
            .await
            .context("Failed to pre-fetch Hyperliquid metadata")?;
        info!(
            "{} {} Hyperliquid metadata cached",
            tag_static("INIT", Color::Cyan),
            "OK".green().bold()
        );

        // Cancel any existing orders on Pacifica at startup
        info!(
            "{} Cancelling any existing orders on Pacifica...",
            tag_static("INIT", Color::Cyan)
        );
        match pacifica_trading
            .cancel_all_orders(false, Some(&config.symbol), false)
            .await
        {
            Ok(count) => info!(
                "{} {} Cancelled {} existing order(s)",
                tag_static("INIT", Color::Cyan),
                "OK".green().bold(),
                count
            ),
            Err(e) => info!(
                "{} {} Failed to cancel existing orders: {}",
                tag_static("INIT", Color::Cyan),
                "WARN".yellow().bold(),
                e
            ),
        }

        if !config.allow_startup_with_cancel_failure {
            match pacifica_trading.get_open_orders().await {
                Ok(orders) => {
                    let stale_for_symbol = orders.iter().any(|o| o.symbol == config.symbol);
                    anyhow::ensure!(
                        !stale_for_symbol,
                        "Startup safety gate failed: Pacifica still has open {} orders after cancel_all",
                        config.symbol
                    );
                }
                Err(e) => {
                    anyhow::bail!(
                        "Startup safety gate failed: could not verify Pacifica open orders after cancel_all: {}",
                        e
                    );
                }
            }
        }

        // Get market info to determine tick size
        let (pacifica_tick_size, pacifica_lot_size): (String, String) = {
            let market_info = pacifica_trading
                .get_market_info()
                .await
                .context("Failed to fetch Pacifica market info")?;
            let symbol_info = market_info
                .get(&config.symbol)
                .with_context(|| format!("Symbol {} not found in market info", config.symbol))?;
            (symbol_info.tick_size.clone(), symbol_info.lot_size.clone())
        };
        let pacifica_tick_size_f64: f64 = pacifica_tick_size
            .parse()
            .context("Failed to parse tick size")?;

        info!(
            "{} Pacifica tick size for {}: {}",
            tag_static("INIT", Color::Cyan),
            config.symbol.bright_white(),
            pacifica_tick_size.as_str().bright_white()
        );

        // Create opportunity evaluator
        let evaluator = OpportunityEvaluator::new(
            config.pacifica_maker_fee_bps,
            config.hyperliquid_taker_fee_bps,
            config.profit_rate_bps,
            pacifica_tick_size_f64,
        );

        info!(
            "{} {}",
            tag_static("INIT", Color::Cyan),
            "Opportunity evaluator created".green()
        );

        // Shared state for orderbook prices
        let pacifica_prices = SharedQuote::empty();
        let hyperliquid_prices = SharedQuote::empty();

        // Shared canonical run state and cold bot snapshot.
        let atomic_status = RunState::new_atomic(RunState::Idle);
        let bot_state = Arc::new(RwLock::new(BotState::with_run_state(atomic_status.clone())));

        // Bounded hedge-event queue so backpressure is visible. A capacity of 256
        // is generous: one fill cycle per evaluation, well above the sustained
        // rate. If it ever fills, `try_send` on the producer side will surface
        // the backlog instead of hiding it in an unbounded queue.
        let (hedge_tx, hedge_rx) = mpsc::channel::<HedgeIntent>(256);
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

        // Fill tracking state
        let processed_fills = FillDedup::new_default();
        // Emergency ceiling = 2x the configured order notional. Anything above this on
        // partial fills alone is treated as a terminal signal to avoid runaway exposure.
        let fill_aggregator = FillAggregator::with_thresholds(
            config.order_notional_usd * 2.0,
            config.partial_hedge_min_notional_usd,
            config.partial_hedge_min_fraction,
            Duration::from_millis(config.partial_hedge_idle_timeout_ms),
            config.partial_hedge_min_base,
            config.fill_aggregator_max_entries,
        );
        let last_position_snapshot =
            Arc::new(parking_lot::Mutex::new(Option::<PositionSnapshot>::None));

        info!(
            "{} {}",
            tag_static("INIT", Color::Cyan),
            "State and channels initialized".green()
        );
        info!("");

        // Initialize order monitor state
        let order_snapshot = Arc::new(SharedOrderSnapshot::new());
        let trade_gate = TradeGate::new();

        Ok(XemmBot {
            config,
            bot_state,
            pacifica_trading,
            pacifica_ws_trading,
            hyperliquid_trading,
            pacifica_prices,
            hyperliquid_prices,
            evaluator,
            processed_fills,
            fill_aggregator,
            last_position_snapshot,
            atomic_status,
            order_snapshot,
            trade_gate,
            pacifica_tick_size,
            pacifica_lot_size,
            hedge_tx,
            hedge_rx: Some(hedge_rx),
            shutdown_tx,
            shutdown_rx: Some(shutdown_rx),
            hedge_shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            pacifica_credentials,
        })
    }

    /// Run the bot - spawn all services and execute main loop
    pub async fn run(mut self) -> Result<()> {
        hedge_store::start_lifecycle_logger(16_384);

        // ===================================================
        // SPAWN ALL SERVICES
        // ===================================================

        // Service 1: Pacifica Orderbook (WebSocket, via generic PriceStreamService)
        let pacifica_ob_client = PacOrderbookClient::new(PacOrderbookConfig {
            symbol: self.config.symbol.clone(),
            agg_level: self.config.agg_level,
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        })
        .context("Failed to create Pacifica orderbook client")?;
        spawn_supervised(
            "pacifica_price_stream",
            self.trade_gate.clone(),
            PriceStreamService {
                client: pacifica_ob_client,
                prices: self.pacifica_prices.clone(),
            }
            .run(),
        );

        // Service 2: Hyperliquid Orderbook (WebSocket, via generic PriceStreamService)
        let hyperliquid_ob_client = HlOrderbookClient::new(HlOrderbookConfig {
            coin: self.config.symbol.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        })
        .context("Failed to create Hyperliquid orderbook client")?;
        spawn_supervised(
            "hyperliquid_price_stream",
            self.trade_gate.clone(),
            PriceStreamService {
                client: hyperliquid_ob_client,
                prices: self.hyperliquid_prices.clone(),
            }
            .run(),
        );
        let fill_config = FillDetectionConfig {
            account: self.pacifica_credentials.account.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
            enable_position_fill_detection: true,
        };
        let fill_client = FillDetectionClient::new(fill_config.clone(), false)
            .context("Failed to create fill detection client")?;
        let fill_ws_ready = fill_client.ready_flag();
        let baseline_updater = fill_client.get_baseline_updater();

        let (cancel_tx, cancel_rx) = CancelManagerService::channel();
        let cancel_manager = Arc::new(CancelManagerService {
            bot_state: self.bot_state.clone(),
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
            rest: self.pacifica_trading.clone(),
            ws: self.pacifica_ws_trading.clone(),
            trade_gate: self.trade_gate.clone(),
            config: self.config.clone(),
        });
        {
            let cancel_manager = cancel_manager.clone();
            spawn_supervised("cancel_manager", self.trade_gate.clone(), async move {
                cancel_manager.run(cancel_rx).await;
            });
        }

        let fill_service = FillDetectionService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            cancel_tx: cancel_tx.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            fill_client,
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            baseline_updater,
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
        };
        spawn_supervised("fill_detection", self.trade_gate.clone(), async move {
            fill_service.run().await;
        });

        // Service 4: Pacifica REST Poll (price redundancy, via generic PricePollService)
        spawn_supervised(
            "pacifica_price_poll",
            self.trade_gate.clone(),
            PricePollService {
                source: Arc::new(PacificaPoller {
                    trading: self.pacifica_trading.clone(),
                    symbol: self.config.symbol.clone(),
                    agg_level: self.config.agg_level,
                }),
                prices: self.pacifica_prices.clone(),
                interval_secs: self.config.pacifica_rest_poll_interval_secs,
            }
            .run(),
        );

        // Service 4.5: Hyperliquid REST Poll (price redundancy, via generic PricePollService)
        spawn_supervised(
            "hyperliquid_price_poll",
            self.trade_gate.clone(),
            PricePollService {
                source: Arc::new(HyperliquidPoller {
                    trading: self.hyperliquid_trading.clone(),
                    symbol: self.config.symbol.clone(),
                }),
                prices: self.hyperliquid_prices.clone(),
                interval_secs: 2,
            }
            .run(),
        );

        // Wait for initial orderbook and fill-stream readiness.
        info!(
            "{} Waiting for streaming readiness...",
            tag_static("INIT", Color::Cyan)
        );
        self.wait_for_startup_readiness(fill_ws_ready.clone())
            .await
            .context("Startup readiness gate failed")?;

        // Service 5: REST Fill Detection (backup)
        let rest_fill_service = RestFillDetectionService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            cancel_tx: cancel_tx.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            min_hedge_notional: self.config.partial_hedge_min_notional_usd,
            poll_interval_ms: self.config.pacifica_active_order_rest_poll_interval_ms,
        };
        spawn_supervised("rest_fill_detection", self.trade_gate.clone(), async move {
            rest_fill_service.run().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Service 5.5: Position Monitor (ground truth) - shares the single Pacifica client.
        let position_monitor_service = PositionMonitorService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            cancel_tx: cancel_tx.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            symbol: self.config.symbol.clone(),
            processed_fills: self.processed_fills.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            last_position_snapshot: self.last_position_snapshot.clone(),
        };
        spawn_supervised("position_monitor", self.trade_gate.clone(), async move {
            position_monitor_service.run().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let position_reconciler_service = PositionReconcilerService {
            bot_state: self.bot_state.clone(),
            hedge_tx: self.hedge_tx.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            config: self.config.clone(),
        };
        spawn_supervised("position_reconciler", self.trade_gate.clone(), async move {
            position_reconciler_service.run().await;
        });

        // Service 6: Order Monitor (age/profit monitoring)
        let order_monitor_service = OrderMonitorService::new(
            self.bot_state.clone(),
            self.atomic_status.clone(),
            self.order_snapshot.clone(),
            self.pacifica_prices.clone(),
            self.hyperliquid_prices.clone(),
            self.config.clone(),
            self.evaluator.clone(),
            cancel_tx.clone(),
        );
        let order_monitor_service = Arc::new(order_monitor_service);
        spawn_monitor_tasks(order_monitor_service);

        let safety_monitor = SafetyMonitorService {
            bot_state: self.bot_state.clone(),
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
            trade_gate: self.trade_gate.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            pacifica_prices: self.pacifica_prices.clone(),
            hyperliquid_prices: self.hyperliquid_prices.clone(),
            fill_ws_ready: fill_ws_ready.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            hedge_tx: self.hedge_tx.clone(),
            config: self.config.clone(),
        };
        spawn_supervised("safety_monitor", self.trade_gate.clone(), async move {
            safety_monitor.run().await;
        });

        // Background: aggregator idle-flush + GC (1 Hz). Catches partials that
        // never received a terminal event and evicts emitted entries older than
        // 5 minutes so the map cannot grow unbounded over a long run.
        {
            let aggregator = self.fill_aggregator.clone();
            let hedge_tx = self.hedge_tx.clone();
            let bot_state = self.bot_state.clone();
            spawn_supervised(
                "fill_aggregator_flush",
                self.trade_gate.clone(),
                async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(1));
                    loop {
                        ticker.tick().await;
                        // Flush stale partials whose terminal event never arrived.
                        for (order_id, d) in aggregator.flush_idle() {
                            warn!(
                            "[AGGREGATOR] Idle-timeout flush (order_id={}): emitting residual hedge for {} @ ${:.4}",
                            order_id, d.size, d.avg_price
                        );
                            if let Err(e) =
                                enqueue_hedge_intent(&hedge_tx, &bot_state, d.into()).await
                            {
                                warn!("[AGGREGATOR] Failed to enqueue idle residual hedge: {}", e);
                            }
                        }
                        aggregator.gc(Duration::from_secs(300));
                    }
                },
            );
        }

        let (audit_tx, audit_rx) = mpsc::channel(64);
        let post_trade_auditor = PostTradeAuditorService {
            bot_state: self.bot_state.clone(),
            audit_rx,
            config: self.config.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            pacifica_trading: self.pacifica_trading.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
        };
        spawn_supervised("post_trade_auditor", self.trade_gate.clone(), async move {
            post_trade_auditor.run().await;
        });

        // Service 7: Hedge Execution
        let hedge_service = HedgeService {
            bot_state: self.bot_state.clone(),
            hedge_rx: self.hedge_rx.take().unwrap(),
            hyperliquid_prices: self.hyperliquid_prices.clone(),
            config: self.config.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            fill_aggregator: self.fill_aggregator.clone(),
            audit_tx,
            cancel_tx: cancel_tx.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_signal: self.hedge_shutdown_signal.clone(),
        };
        tokio::spawn(async move {
            hedge_service.run().await;
        });

        // ===================================================
        // MAIN OPPORTUNITY EVALUATION LOOP
        // ===================================================

        info!(
            "{} Starting opportunity evaluation loop",
            tag(&self.config.symbol, "MAIN", Color::BrightWhite)
        );
        info!("");

        let mut eval_interval = interval(Duration::from_millis(1));
        let rate_limits = RateLimitBook::new();

        let sigint = signal::ctrl_c();
        tokio::pin!(sigint);

        #[cfg(unix)]
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to setup SIGTERM handler");

        let mut shutdown_rx = self.shutdown_rx.take().unwrap();

        loop {
            tokio::select! {
                _ = &mut sigint => {
                    info!("{} {} Received SIGINT (Ctrl+C), initiating graceful shutdown...",
                        tag(&self.config.symbol, "MAIN", Color::BrightWhite),
                        "WARN".yellow().bold()
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
                        tag(&self.config.symbol, "MAIN", Color::BrightWhite),
                        "WARN".yellow().bold()
                    );
                    break;
                }

                _ = eval_interval.tick() => {
                    let run_state = RunState::load(&self.atomic_status);
                    if run_state.is_terminal() {
                        break;
                    }

                    let max_quote_age = Duration::from_millis(self.config.max_quote_age_ms);
                    if !self.trade_gate.allow_quote(
                        run_state,
                        self.pacifica_prices.snapshot(),
                        self.hyperliquid_prices.snapshot(),
                        max_quote_age,
                        Duration::from_secs(3),
                    ) {
                        continue;
                    }

                    // Check rate limit backoff
                    if rate_limits.should_skip_with_priority(
                        EndpointGroup::PacificaPlace,
                        RateLimitPriority::Alpha,
                    ) {
                        let remaining = rate_limits
                            .remaining_backoff_secs(EndpointGroup::PacificaPlace);
                        if remaining as u64 % 5 == 0 || remaining < 1.0 {
                            debug!("[MAIN] Skipping order placement (rate limit backoff, {:.1}s remaining)", remaining);
                        }
                        continue;
                    }

                    // Get fresh coherent price snapshots.
                    let Some(pac_quote) = self.pacifica_prices.usable_snapshot(max_quote_age) else {
                        continue;
                    };
                    let Some(hl_quote) = self.hyperliquid_prices.usable_snapshot(max_quote_age) else {
                        continue;
                    };
                    let (pac_bid, pac_ask) = (pac_quote.bid, pac_quote.ask);
                    let (hl_bid, hl_ask) = (hl_quote.bid, hl_quote.ask);

                    // Get timestamp once for this evaluation cycle
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    // Evaluate opportunities
                    let buy_opp = self.evaluator.evaluate_buy_opportunity_local(
                        hl_bid,
                        pac_bid,
                        pac_ask,
                        self.config.order_notional_usd,
                        now_ms,
                        self.config.max_quote_distance_bps,
                    );
                    let sell_opp = self.evaluator.evaluate_sell_opportunity_local(
                        hl_ask,
                        pac_bid,
                        pac_ask,
                        self.config.order_notional_usd,
                        now_ms,
                        self.config.max_quote_distance_bps,
                    );

                    let best_opp = OpportunityEvaluator::pick_best_local_book_opportunity(
                        buy_opp, sell_opp, pac_bid, pac_ask,
                    );

                    if let Some(opp) = best_opp {
                        // Reserve placement atomically, then release the state lock before
                        // Pacifica REST I/O.
                        if !RunState::try_transition(
                            &self.atomic_status,
                            RunState::Idle,
                            RunState::Placing,
                        ) {
                            continue;
                        }

                        let client_order_id = Uuid::new_v4().to_string();
                        let prospective_order = ActiveOrder {
                            order_id: None,
                            client_order_id: client_order_id.clone(),
                            symbol: self.config.symbol.clone(),
                            side: opp.direction,
                            price: opp.pacifica_price,
                            size: opp.size,
                            initial_profit_bps: opp.initial_profit_bps,
                            placed_at: Instant::now(),
                        };
                        {
                            let mut state = self.bot_state.write();
                            state.set_prospective_order(prospective_order);
                        }
                        update_order_snapshot(
                            &self.order_snapshot,
                            opp.direction,
                            opp.pacifica_price,
                            opp.size,
                            opp.initial_profit_bps,
                        );

                        debug!(
                            symbol = %self.config.symbol,
                            side = opp.direction.as_str(),
                            pacifica_price = opp.pacifica_price,
                            hyperliquid_price = opp.hyperliquid_price,
                            size = opp.size,
                            profit_bps = opp.initial_profit_bps,
                            pacifica_bid = pac_bid,
                            pacifica_ask = pac_ask,
                            hyperliquid_bid = hl_bid,
                            hyperliquid_ask = hl_ask,
                            "placing Pacifica maker order"
                        );

                        let pacifica_side = match opp.direction {
                            OrderSide::Buy => PacificaOrderSide::Buy,
                            OrderSide::Sell => PacificaOrderSide::Sell,
                        };

                        let placement_result = if self.pacifica_ws_trading.is_connected() {
                            self.pacifica_ws_trading
                                .place_limit_order_ws(
                                    &self.config.symbol,
                                    pacifica_side,
                                    opp.size,
                                    opp.pacifica_price,
                                    &self.pacifica_tick_size,
                                    &self.pacifica_lot_size,
                                    client_order_id.clone(),
                                )
                                .await
                        } else {
                            self.pacifica_trading
                                .place_limit_order_with_client_order_id(
                                    &self.config.symbol,
                                    pacifica_side,
                                    opp.size,
                                    Some(opp.pacifica_price),
                                    0.0,
                                    Some(pac_bid),
                                    Some(pac_ask),
                                    Some(client_order_id.clone()),
                                )
                                .await
                        };

                        match placement_result {
                            Ok(order_data) => {
                                rate_limits.record_success(EndpointGroup::PacificaPlace);

                                if let Some(returned_cloid) = order_data.client_order_id {
                                    let order_id_opt = order_data.order_id.or(order_data.i);
                                    let order_id_display = order_id_opt.unwrap_or(0);
                                    let submitted_price = order_data.submitted_price.unwrap_or(opp.pacifica_price);
                                    let submitted_size = order_data.submitted_size.unwrap_or(opp.size);
                                    info!(
                                        "{} {} Placed {} #{} @ {} | cloid: {}...{}",
                                        tag(&self.config.symbol, "ORDER", Color::BrightYellow),
                                        "OK".green().bold(),
                                        opp.direction.as_str().bright_yellow(),
                                        order_id_display,
                                        format!("${:.4}", submitted_price).cyan().bold(),
                                        &returned_cloid[..8],
                                        &returned_cloid[returned_cloid.len()-4..]
                                    );

                                    let mut state = self.bot_state.write();
                                    if let Some(active_order) = state.active_order.as_mut() {
                                        if active_order.client_order_id == returned_cloid {
                                            active_order.order_id = order_id_opt;
                                            active_order.price = submitted_price;
                                            active_order.size = submitted_size;
                                        }
                                    }
                                    if matches!(state.status, BotStatus::Placing) {
                                        state.status = BotStatus::OrderPlaced;
                                        state.store_status();
                                    }

                                    // Update order snapshot for order monitor
                                    update_order_snapshot(
                                        &self.order_snapshot,
                                        opp.direction,
                                        submitted_price,
                                        submitted_size,
                                        opp.initial_profit_bps,
                                    );
                                } else {
                                    warn!("{} {} Order placement response omitted client_order_id; entering placement recovery",
                                        tag(&self.config.symbol, "ORDER", Color::BrightYellow),
                                        "FAIL".red().bold()
                                    );
                                    let mut state = self.bot_state.write();
                                    let same_order = state
                                        .active_order
                                        .as_ref()
                                        .map(|order| order.client_order_id == client_order_id)
                                        .unwrap_or(false);
                                    if same_order && matches!(state.status, BotStatus::Placing | BotStatus::OrderPlaced) {
                                        state.mark_placement_unknown();
                                        self.trade_gate.block(GateReason::PlacementUnknown);
                                    }
                                }
                            }
                            Err(e) => {
                                if is_rate_limit_error(&e) {
                                    rate_limits.record_error(EndpointGroup::PacificaPlace);
                                    let backoff_secs = rate_limits.get_backoff_secs(EndpointGroup::PacificaPlace);
                                    info!(
                                        "{} {} Failed to place order: Rate limit exceeded. Backing off for {}s (attempt #{})",
                                        tag(&self.config.symbol, "ORDER", Color::BrightYellow),
                                        "WARN".yellow().bold(),
                                        backoff_secs,
                                        rate_limits.consecutive_errors(EndpointGroup::PacificaPlace)
                                    );
                                } else {
                                    info!("{} {} Failed to place order: {}",
                                        tag(&self.config.symbol, "ORDER", Color::BrightYellow),
                                        "FAIL".red().bold(),
                                        e.to_string().red()
                                    );
                                }
                                let mut state = self.bot_state.write();
                                let same_order = state
                                    .active_order
                                    .as_ref()
                                    .map(|order| order.client_order_id == client_order_id)
                                    .unwrap_or(false);
                                if same_order && matches!(state.status, BotStatus::Placing | BotStatus::OrderPlaced) {
                                    state.mark_placement_unknown();
                                    self.trade_gate.block(GateReason::PlacementUnknown);
                                }
                            }
                        }
                    }
                }

                _ = shutdown_rx.recv() => {
                    info!("{} Shutdown signal received",
                        tag(&self.config.symbol, "MAIN", Color::BrightWhite)
                    );
                    break;
                }
            }
        }

        // ===================================================
        // SHUTDOWN CLEANUP
        // ===================================================
        self.trade_gate.block(GateReason::ShuttingDown);
        {
            let mut state = self.bot_state.write();
            if !matches!(state.status, BotStatus::Error(_)) {
                state.mark_shutting_down();
            }
        }

        // Tell the hedge service to drain any in-flight events. A fill can arrive
        // during the shutdown window; dropping it would leave a Pacifica position
        // un-hedged.
        info!(
            "{} Notifying hedge service to drain pending events...",
            tag(&self.config.symbol, "SHUTDOWN", Color::Yellow)
        );
        self.hedge_shutdown_signal.notify_one();

        // Wait up to 5 s for the hedge service to either signal `shutdown_tx`
        // (normal exit path) or timeout. We tolerate the timeout to avoid hanging
        // the process indefinitely if something is stuck on the Hyperliquid side.
        match tokio::time::timeout(Duration::from_secs(5), shutdown_rx.recv()).await {
            Ok(Some(())) => info!(
                "{} {} Hedge service drained cleanly",
                tag(&self.config.symbol, "SHUTDOWN", Color::Yellow),
                "OK".green().bold()
            ),
            Ok(None) => info!(
                "{} {} Hedge service already exited",
                tag(&self.config.symbol, "SHUTDOWN", Color::Yellow),
                "OK".green().bold()
            ),
            Err(_) => warn!(
                "{} {} Hedge drain timed out after 5s - check data/unhedged_positions.jsonl",
                tag(&self.config.symbol, "SHUTDOWN", Color::Yellow),
                "WARN".yellow().bold()
            ),
        }

        info!("");
        info!(
            "{} Cancelling any remaining orders...",
            tag(&self.config.symbol, "SHUTDOWN", Color::Yellow)
        );

        match dual_cancel(
            &self.pacifica_trading,
            &self.pacifica_ws_trading,
            &self.config.symbol,
        )
        .await
        {
            Ok((rest_count, ws_count)) => info!(
                "{} {} Cancelled remaining orders (REST: {}, WS: {})",
                tag(&self.config.symbol, "SHUTDOWN", Color::Yellow),
                "OK".green().bold(),
                rest_count,
                ws_count
            ),
            Err(e) => info!(
                "{} {} Failed to cancel orders: {}",
                tag(&self.config.symbol, "SHUTDOWN", Color::Yellow),
                "WARN".yellow().bold(),
                e
            ),
        }

        if let Ok((pacifica_position, hyperliquid_position)) = self.signed_positions().await {
            let net = pacifica_position + hyperliquid_position;
            if net.abs() > self.config.neutral_dust_base {
                self.persist_unresolved_exposure(net).await;
                anyhow::bail!(
                    "Shutdown left unresolved net exposure {:.8} {}",
                    net,
                    self.config.symbol
                );
            }
        }

        // Final state check
        let final_state = self.bot_state.read();
        match &final_state.status {
            BotStatus::Complete => {
                info!("");
                info!(
                    "{} {}",
                    "OK".green().bold(),
                    "Bot completed successfully!".green().bold()
                );
                info!("Final position: {}", final_state.position);
                Ok(())
            }
            BotStatus::Idle | BotStatus::ShuttingDown => {
                info!("");
                info!(
                    "{} Bot stopped cleanly after {} completed cycle(s)",
                    "OK".green().bold(),
                    final_state.cycle_count
                );
                Ok(())
            }
            BotStatus::Error(e) => {
                info!("");
                info!(
                    "{} {}: {}",
                    "FAIL".red().bold(),
                    "Bot terminated with error".red().bold(),
                    e.to_string().red()
                );
                anyhow::bail!("Bot failed: {}", e)
            }
            _ => {
                info!("");
                info!(
                    "{} Bot terminated in unexpected state: {:?}",
                    "WARN".yellow().bold(),
                    final_state.status
                );
                Ok(())
            }
        }
    }

    async fn wait_for_startup_readiness(&self, fill_ws_ready: Arc<AtomicBool>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let max_quote_age = Duration::from_millis(self.config.max_quote_age_ms);
        loop {
            let pacifica = self.pacifica_prices.snapshot();
            let hyperliquid = self.hyperliquid_prices.snapshot();
            self.trade_gate
                .update_quote_health(pacifica, hyperliquid, max_quote_age);
            self.trade_gate.set(
                GateReason::FillWsDown,
                !fill_ws_ready.load(Ordering::Acquire),
            );

            let pac_stream_ready = pacifica
                .filter(|q| q.source == QuoteSource::Stream)
                .and_then(|_| self.pacifica_prices.usable_snapshot(max_quote_age))
                .is_some();
            let hl_stream_ready = hyperliquid
                .filter(|q| q.source == QuoteSource::Stream)
                .and_then(|_| self.hyperliquid_prices.usable_snapshot(max_quote_age))
                .is_some();
            let fill_ready = fill_ws_ready.load(Ordering::Acquire);

            if pac_stream_ready && hl_stream_ready && fill_ready {
                self.trade_gate.allow(GateReason::QuoteStale);
                self.trade_gate.allow(GateReason::PriceWsDown);
                self.trade_gate.allow(GateReason::FillWsDown);
                info!(
                    "{} {} Streaming readiness satisfied",
                    tag_static("INIT", Color::Cyan),
                    "OK".green().bold()
                );
                return Ok(());
            }

            if Instant::now() >= deadline {
                anyhow::bail!(
                    "startup readiness timed out; gate reasons: {:?}",
                    self.trade_gate.describe()
                );
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn persist_unresolved_exposure(&self, net: f64) {
        let path = std::path::Path::new("data/unresolved_exposure.jsonl");
        if let Some(parent) = path.parent() {
            let _ = tokio::fs::create_dir_all(parent).await;
        }
        let record = serde_json::json!({
            "ts_ms": chrono::Utc::now().timestamp_millis(),
            "symbol": &self.config.symbol,
            "net_base": net,
        });
        if let Ok(mut file) = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            use tokio::io::AsyncWriteExt;
            let _ = file.write_all(record.to_string().as_bytes()).await;
            let _ = file.write_all(b"\n").await;
        }
    }

    #[allow(dead_code)]
    async fn pre_place_safety_check(&self) -> bool {
        match self.pacifica_trading.get_open_orders().await {
            Ok(orders)
                if orders
                    .iter()
                    .any(|order| order.symbol == self.config.symbol) =>
            {
                warn!(
                    "{} {} Placement safety gate found existing Pacifica open orders; cancelling and entering reconciliation",
                    tag(&self.config.symbol, "SAFETY", Color::Yellow),
                    "WARN".yellow().bold()
                );
                let _ = self
                    .pacifica_trading
                    .cancel_all_orders(false, Some(&self.config.symbol), false)
                    .await;
                let mut state = self.bot_state.write();
                state.mark_reconciling();
                return false;
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    "{} {} Placement safety gate could not verify Pacifica open orders: {}",
                    tag(&self.config.symbol, "SAFETY", Color::Yellow),
                    "WARN".yellow().bold(),
                    e
                );
                return false;
            }
        }

        let (pacifica_position, hyperliquid_position) = match self.signed_positions().await {
            Ok(positions) => positions,
            Err(e) => {
                warn!(
                    "{} {} Placement safety gate could not verify positions: {}",
                    tag(&self.config.symbol, "SAFETY", Color::Yellow),
                    "WARN".yellow().bold(),
                    e
                );
                return false;
            }
        };

        let net_position = pacifica_position + hyperliquid_position;
        if net_position.abs() > self.config.neutral_dust_base {
            warn!(
                "{} {} Placement blocked: net exposure {:.8} exceeds dust {}",
                tag(&self.config.symbol, "SAFETY", Color::Yellow),
                "WARN".yellow().bold(),
                net_position,
                self.config.neutral_dust_base
            );
            let mut state = self.bot_state.write();
            state.mark_reconciling();
            return false;
        }

        true
    }

    async fn signed_positions(&self) -> Result<(f64, f64)> {
        let pacifica_positions = self.pacifica_trading.get_positions().await?;
        let pacifica_position = pacifica_positions
            .iter()
            .find(|position| position.symbol == self.config.symbol)
            .map(|position| {
                let amount = position.amount.parse::<f64>().unwrap_or(0.0);
                match position.side.as_str() {
                    "bid" => amount,
                    "ask" => -amount,
                    _ => 0.0,
                }
            })
            .unwrap_or(0.0);

        let hyperliquid_state = self
            .hyperliquid_trading
            .get_user_state(&self.hyperliquid_trading.get_wallet_address())
            .await?;
        let hyperliquid_position = hyperliquid_state
            .asset_positions
            .iter()
            .find(|position| position.position.coin == self.config.symbol)
            .map(|position| position.position.szi.parse::<f64>().unwrap_or(0.0))
            .unwrap_or(0.0);

        Ok((pacifica_position, hyperliquid_position))
    }
}
