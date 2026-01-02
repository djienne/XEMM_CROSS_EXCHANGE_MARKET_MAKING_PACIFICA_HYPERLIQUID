use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, RwLock};
use tracing::{info, warn, error};

use crate::bot::{BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::{
    FillDetectionClient, FillDetectionConfig, PacificaCredentials, PacificaTrading,
    PacificaWsTrading,
};
use crate::services::{
    fill_detection::FillDetectionService, hedge::HedgeService,
    order_monitor::{OrderMonitorService, SharedOrderSnapshot, spawn_monitor_tasks},
    orderbook::{HyperliquidOrderbookService, PacificaOrderbookService},
    market_event::{MarketEventHub, MarketSource},
    position_monitor::PositionMonitorService, rest_fill_detection::RestFillDetectionService,
    rest_poll::{HyperliquidRestPollService, PacificaRestPollService}, ws_health::{WsHealth, WsHealthMonitor}, HedgeEvent,
};
use crate::strategy::OpportunityEvaluator;

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
    /// Expected client_order_id for fast fill ownership check
    pub expected_cloid: Arc<parking_lot::Mutex<Option<String>>>,

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
    pub async fn new() -> Result<Self> {
        crate::bot::BotFactory::create().await
    }

    /// Run the bot - spawn all services and execute main loop
    pub async fn run(mut self) -> Result<()> {
        // ═══════════════════════════════════════════════════
        // PRE-FLIGHT: ORDERBOOK WS + TRADING API SANITY
        // ═══════════════════════════════════════════════════
        let market_events = Arc::new(MarketEventHub::new(1024));
        let ws_health = Arc::new(WsHealth::new());
        let (pacifica_reconnect_tx, pacifica_reconnect_rx) = watch::channel(0u64);
        let (hyperliquid_reconnect_tx, hyperliquid_reconnect_rx) = watch::channel(0u64);

        // Service 1: Pacifica Orderbook (WebSocket)
        let pacifica_ob_service = PacificaOrderbookService {
            prices: self.pacifica_prices.clone(),
            market_events: market_events.clone(),
            ws_health: ws_health.clone(),
            reconnect_rx: pacifica_reconnect_rx,
            symbol: self.config.symbol.clone(),
            agg_level: self.config.agg_level,
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        };
        tokio::spawn(async move {
            if let Err(e) = pacifica_ob_service.run().await {
                error!("{} {} Pacifica orderbook service exited: {}",
                    "[ORDERBOOK]".cyan().bold(), "✗".red().bold(), e);
            }
        });

        // Service 2: Hyperliquid Orderbook (WebSocket)
        let hyperliquid_ob_service = HyperliquidOrderbookService {
            prices: self.hyperliquid_prices.clone(),
            market_events: market_events.clone(),
            ws_health: ws_health.clone(),
            reconnect_rx: hyperliquid_reconnect_rx,
            symbol: self.config.symbol.clone(),
            reconnect_attempts: self.config.reconnect_attempts,
            ping_interval_secs: self.config.ping_interval_secs,
        };
        tokio::spawn(async move {
            if let Err(e) = hyperliquid_ob_service.run().await {
                error!("{} {} Hyperliquid orderbook service exited: {}",
                    "[ORDERBOOK]".cyan().bold(), "✗".red().bold(), e);
            }
        });

        let ws_health_monitor = WsHealthMonitor {
            symbol: self.config.symbol.clone(),
            health: ws_health.clone(),
            bot_state: self.bot_state.clone(),
            pacifica_trading: self.pacifica_trading_main.clone(),
            pacifica_ws_trading: self.pacifica_ws_trading.clone(),
            atomic_status: self.atomic_status.clone(),
            order_snapshot: self.order_snapshot.clone(),
            pacifica_reconnect_tx,
            hyperliquid_reconnect_tx,
            stale_threshold_ms: self.config.ws_stale_threshold_ms,
        };
        tokio::spawn(async move {
            ws_health_monitor.run().await;
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
            min_hedge_notional: self.config.min_hedge_notional_usd,
            expected_cloid: self.expected_cloid.clone(),
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
            poll_interval_secs: self.config.hyperliquid_rest_poll_interval_secs,
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
            min_hedge_notional: self.config.min_hedge_notional_usd,
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
        // Pre-fetch asset metadata for low-latency hedge execution
        let cached_asset_meta = match self.hyperliquid_trading.get_asset_info(&self.config.symbol).await {
            Ok(info) => {
                let asset_id = self.hyperliquid_trading.get_asset_id(&self.config.symbol).await.unwrap_or(0);
                info!("{} {} Pre-cached asset meta: id={}, sz_decimals={}",
                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                    "✓".green().bold(),
                    asset_id,
                    info.sz_decimals
                );
                Some(crate::services::hedge::CachedAssetMeta {
                    asset_id,
                    sz_decimals: info.sz_decimals,
                })
            }
            Err(e) => {
                warn!("{} {} Failed to pre-cache asset meta (will fetch on demand): {}",
                    format!("[{} HEDGE]", self.config.symbol).bright_magenta().bold(),
                    "⚠".yellow().bold(),
                    e
                );
                None
            }
        };

        let hedge_service = HedgeService {
            bot_state: self.bot_state.clone(),
            hedge_rx: self.hedge_rx.take().unwrap(),
            hyperliquid_prices: self.hyperliquid_prices.clone(),
            config: self.config.clone(),
            hyperliquid_trading: self.hyperliquid_trading.clone(),
            pacifica_trading: self.pacifica_trading_hedge.clone(),
            shutdown_tx: self.shutdown_tx.clone(),
            cached_asset_meta,
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



            let opportunity_loop = crate::services::opportunity_loop::OpportunityLoopService::new(
                self.config.clone(),
                self.bot_state.clone(),
                self.pacifica_trading_main.clone(),
                self.pacifica_prices.clone(),
                self.hyperliquid_prices.clone(),
                self.evaluator.clone(),
                self.atomic_status.clone(),
                self.last_cancel_ms.clone(),
                self.order_snapshot.clone(),
                order_monitor_service.clone(),
                self.expected_cloid.clone(),
            );

            // Run the opportunity loop (blocking until shutdown)
            let shutdown_rx = self.shutdown_rx.take().unwrap();
            if let Err(e) = opportunity_loop.run(&market_events, shutdown_rx).await {
                match e.downcast_ref::<std::io::Error>() {
                    Some(io_err) if io_err.kind() == std::io::ErrorKind::Interrupted => {
                         // Ignore IO interruption (shutdown)
                    }
                    _ => {
                        error!("Opportunity loop error: {}", e);
                    }
                }
            }
            // Re-assign shutdown_rx to satisfy the compiler/struct if needed,
            // or just break since we are shutting down.
            // The `break` here is not strictly necessary as the function will return
            // after this block, but it matches the original loop's exit logic.
        // The original `main: loop` is replaced by the `opportunity_loop.run` call.
        // The `break; }` from the instruction closes the implicit loop structure
        // that the `opportunity_loop.run` call effectively replaces.
        // Since `opportunity_loop.run` is blocking until shutdown,
        // the code will proceed to cleanup after it returns.

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
