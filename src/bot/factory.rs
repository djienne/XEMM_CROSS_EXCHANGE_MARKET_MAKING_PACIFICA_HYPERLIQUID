use anyhow::{Context, Result};
use colored::Colorize;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::info;

use crate::app::{PositionSnapshot, XemmBot};
use crate::bot::BotState;
use crate::config::Config;
use crate::connector::hyperliquid::{HyperliquidCredentials, HyperliquidTrading};
use crate::connector::pacifica::{
    PacificaCredentials, PacificaTrading, PacificaWsTrading,
};
use crate::services::{
    order_monitor::SharedOrderSnapshot, HedgeEvent,
};
use crate::strategy::OpportunityEvaluator;
use crate::util::atomic_price::AtomicPrice;

pub struct BotFactory;

impl BotFactory {
    /// Create and initialize a new XemmBot instance
    pub async fn create() -> Result<XemmBot> {
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
        // ... (truncated logs for brevity, kept essential ones) ...
        info!("{} Target Profit: {}",
            "[CONFIG]".blue().bold(),
            format!("{} bps", config.profit_rate_bps).green().bold()
        );

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
        let pacifica_ws_trading = Arc::new(PacificaWsTrading::new(pacifica_credentials.clone(), false));

        let hyperliquid_trading = Arc::new(
            HyperliquidTrading::new(hyperliquid_credentials, false)
                .context("Failed to create Hyperliquid trading client")?,
        );

        info!("{} {}",
            "[INIT]".cyan().bold(),
            "Trading clients initialized (6 REST + 1 WS)".green()
        );

        // Pre-fetch Hyperliquid metadata
        info!("{} Pre-fetching Hyperliquid metadata...", "[INIT]".cyan().bold());
        hyperliquid_trading
            .get_meta()
            .await
            .context("Failed to pre-fetch Hyperliquid metadata")?;

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

        // Create opportunity evaluator
        let evaluator = OpportunityEvaluator::new(
            config.pacifica_maker_fee_bps,
            config.hyperliquid_taker_fee_bps,
            config.profit_rate_bps,
            pacifica_tick_size,
        );

        // Shared state
        let pacifica_prices = Arc::new(AtomicPrice::new());
        let hyperliquid_prices = Arc::new(AtomicPrice::new());
        let bot_state = Arc::new(RwLock::new(BotState::new()));
        let (hedge_tx, hedge_rx) = mpsc::unbounded_channel::<HedgeEvent>();
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        let processed_fills = Arc::new(parking_lot::Mutex::new(HashSet::<String>::new()));
        let last_position_snapshot = Arc::new(parking_lot::Mutex::new(Option::<PositionSnapshot>::None));

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
}
