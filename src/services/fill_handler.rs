//! Unified fill handling logic for all detection methods.
//!
//! This module provides a single `FillHandler` that encapsulates the common
//! fill processing logic used by WebSocket, REST, and position-based detection.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use colored::Colorize;
use parking_lot::Mutex;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};

use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading, PositionBaselineUpdater};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;

/// Identifies the source of fill detection for logging.
#[derive(Debug, Clone, Copy)]
pub enum FillSource {
    WebSocket,
    RestApi,
    PositionDelta,
}

impl FillSource {
    fn log_prefix(&self) -> String {
        match self {
            FillSource::WebSocket => "[FILL_DETECTION]".magenta().bold().to_string(),
            FillSource::RestApi => "[REST_FILL_DETECTION]".bright_cyan().bold().to_string(),
            FillSource::PositionDelta => "[POSITION_MONITOR]".bright_cyan().bold().to_string(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            FillSource::WebSocket => "WebSocket",
            FillSource::RestApi => "REST",
            FillSource::PositionDelta => "Position",
        }
    }
}

/// Fill type for deduplication and logging.
#[derive(Debug, Clone, Copy)]
pub enum FillType {
    Full,
    Partial,
}

impl FillType {
    fn prefix(&self) -> &'static str {
        match self {
            FillType::Full => "full",
            FillType::Partial => "partial",
        }
    }
}

/// Shared fill handler that provides unified fill processing logic.
///
/// All fill detection services should use this handler to:
/// 1. Deduplicate fills (prevent double hedging)
/// 2. Update bot state
/// 3. Trigger background cancellation
/// 4. Send hedge event
#[derive(Clone)]
pub struct FillHandler {
    bot_state: Arc<RwLock<BotState>>,
    hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pacifica_trading: Arc<PacificaTrading>,
    pacifica_ws_trading: Arc<PacificaWsTrading>,
    symbol: String,
    processed_fills: Arc<Mutex<HashSet<String>>>,
    baseline_updater: Option<PositionBaselineUpdater>,
}

impl FillHandler {
    pub fn new(
        bot_state: Arc<RwLock<BotState>>,
        hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
        pacifica_trading: Arc<PacificaTrading>,
        pacifica_ws_trading: Arc<PacificaWsTrading>,
        symbol: String,
        processed_fills: Arc<Mutex<HashSet<String>>>,
        baseline_updater: Option<PositionBaselineUpdater>,
    ) -> Self {
        Self {
            bot_state,
            hedge_tx,
            pacifica_trading,
            pacifica_ws_trading,
            symbol,
            processed_fills,
            baseline_updater,
        }
    }

    /// Parse order side from string (handles both "buy"/"sell" and "bid"/"ask").
    pub fn parse_side(side_str: &str) -> Option<OrderSide> {
        match side_str {
            "buy" | "bid" => Some(OrderSide::Buy),
            "sell" | "ask" => Some(OrderSide::Sell),
            _ => None,
        }
    }

    /// Check if a fill has already been processed (thread-safe).
    ///
    /// Returns `true` if the fill should be processed, `false` if already handled.
    pub fn try_mark_processed(&self, fill_type: FillType, client_order_id: &str) -> bool {
        let fill_id = format!("{}_{}", fill_type.prefix(), client_order_id);
        let mut processed = self.processed_fills.lock();

        // Also check for alternative fill IDs (WebSocket vs REST may use different prefixes)
        if processed.contains(&fill_id)
            || processed.contains(&format!("full_{}", client_order_id))
            || processed.contains(&format!("partial_{}", client_order_id))
            || processed.contains(&format!("full_{}_rest", client_order_id))
            || processed.contains(&format!("partial_{}_rest", client_order_id))
        {
            return false;
        }

        processed.insert(fill_id);
        true
    }

    /// Process a fill event - the core unified logic.
    ///
    /// This method:
    /// 1. Updates bot state to Filled
    /// 2. Spawns background dual cancellation
    /// 3. Updates position baseline (if available)
    /// 4. Triggers hedge immediately
    ///
    /// # Arguments
    /// * `source` - Where the fill was detected (for logging)
    /// * `fill_type` - Full or partial fill
    /// * `side` - Order side (Buy/Sell)
    /// * `filled_size` - Amount filled
    /// * `fill_price` - Average fill price
    /// * `side_str` - Original side string (for baseline updater)
    ///
    /// # Returns
    /// The `Instant` when processing started (for latency tracking)
    pub async fn process_fill(
        &self,
        source: FillSource,
        fill_type: FillType,
        side: OrderSide,
        filled_size: f64,
        fill_price: f64,
        side_str: Option<&str>,
    ) -> Instant {
        let fill_start = Instant::now();
        let prefix = source.log_prefix();

        // 1. Update state to Filled IMMEDIATELY
        {
            let mut state = self.bot_state.write().await;
            state.mark_filled(filled_size, side);
        }

        info!(
            "{} {} {:?} FILL DETECTED - State updated to Filled",
            prefix,
            "✓".green().bold(),
            fill_type
        );

        // 2. Spawn background dual cancellation (non-blocking)
        info!(
            "{} {} Spawning async dual cancellation (REST + WebSocket)...",
            prefix,
            "⚡".yellow().bold()
        );

        let pac_trading = self.pacifica_trading.clone();
        let pac_ws_trading = self.pacifica_ws_trading.clone();
        let symbol = self.symbol.clone();
        let source_name = source.name();

        tokio::spawn(async move {
            match dual_cancel(&pac_trading, &pac_ws_trading, &symbol).await {
                Ok((rest_count, ws_count)) => {
                    info!(
                        "{} {} Background dual cancellation complete (REST: {}, WS: {}) [{}]",
                        "[FILL_HANDLER]".bright_blue().bold(),
                        "✓✓".green().bold(),
                        rest_count,
                        ws_count,
                        source_name
                    );
                }
                Err(e) => {
                    error!(
                        "{} {} Background dual cancellation failed: {} [{}]",
                        "[FILL_HANDLER]".bright_blue().bold(),
                        "✗".red().bold(),
                        e,
                        source_name
                    );
                }
            }
        });

        // 3. Update position baseline (prevents position monitor from re-detecting)
        if let (Some(updater), Some(side_s)) = (&self.baseline_updater, side_str) {
            updater.update_baseline(&self.symbol, side_s, filled_size, fill_price);
        }

        // 4. Trigger hedge immediately
        let latency_ms = fill_start.elapsed().as_secs_f64() * 1000.0;
        info!(
            "{} {} Hedge triggered in {:.1}ms (cancellation running async)",
            format!("[{}]", self.symbol).bright_white().bold(),
            format!("{:?} fill", fill_type).green().bold(),
            latency_ms
        );

        if let Err(e) = self.hedge_tx.send((side, filled_size, fill_price, fill_start)) {
            error!(
                "{} {} Failed to send hedge event: {}",
                prefix,
                "✗".red().bold(),
                e
            );
        }

        fill_start
    }

    /// Check if an order is ours by comparing client_order_id.
    pub async fn is_our_order(&self, client_order_id: Option<&str>) -> bool {
        let state = self.bot_state.read().await;
        state
            .active_order
            .as_ref()
            .and_then(|o| client_order_id.map(|id| o.client_order_id == id))
            .unwrap_or(false)
    }

    /// Convenience method for the full fill handling flow (async).
    ///
    /// Combines ownership check, deduplication, and processing.
    pub async fn handle_fill(
        &self,
        source: FillSource,
        fill_type: FillType,
        client_order_id: Option<&str>,
        side_str: &str,
        filled_size: f64,
        fill_price: f64,
    ) -> bool {
        let prefix = source.log_prefix();

        // Check if this is our order
        if !self.is_our_order(client_order_id).await {
            debug!("{} Fill is not for our order, ignoring", prefix);
            return false;
        }

        // Check for duplicate
        let cloid = match client_order_id {
            Some(id) => id,
            None => {
                debug!("{} No client_order_id, cannot deduplicate", prefix);
                return false;
            }
        };

        if !self.try_mark_processed(fill_type, cloid) {
            debug!(
                "{} {:?} fill already processed (duplicate), skipping",
                prefix, fill_type
            );
            return false;
        }

        // Parse side
        let side = match Self::parse_side(side_str) {
            Some(s) => s,
            None => {
                error!("{} {} Unknown side: {}", prefix, "✗".red().bold(), side_str);
                return false;
            }
        };

        // Process the fill
        self.process_fill(source, fill_type, side, filled_size, fill_price, Some(side_str))
            .await;

        true
    }
}
