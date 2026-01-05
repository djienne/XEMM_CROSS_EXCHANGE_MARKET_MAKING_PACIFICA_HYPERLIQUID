//! Unified fill handling logic for all detection methods.
//!
//! This module provides a single `FillHandler` that encapsulates the common
//! fill processing logic used by WebSocket, REST, and position-based detection.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use colored::Colorize;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};

use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading, PositionBaselineUpdater};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;

// Pre-computed colored log prefixes (avoid allocation in hot path)
static PREFIX_WEBSOCKET: Lazy<String> = Lazy::new(|| "[FILL_DETECTION]".magenta().bold().to_string());
static PREFIX_REST_API: Lazy<String> = Lazy::new(|| "[REST_FILL_DETECTION]".bright_cyan().bold().to_string());
static PREFIX_POSITION_DELTA: Lazy<String> = Lazy::new(|| "[POSITION_MONITOR]".bright_cyan().bold().to_string());
static PREFIX_FILL_HANDLER: Lazy<String> = Lazy::new(|| "[FILL_HANDLER]".bright_blue().bold().to_string());

/// Status values for atomic fast-path checks (must match BotStatus encoding)
#[allow(dead_code)]
const STATUS_IDLE: u8 = 0;
const STATUS_ORDER_PLACED: u8 = 1;
const STATUS_FILLED: u8 = 2;
#[allow(dead_code)] // Reserved for future state tracking
const STATUS_HEDGING: u8 = 3;

/// Identifies the source of fill detection for logging.
#[derive(Debug, Clone, Copy)]
pub enum FillSource {
    WebSocket,
    RestApi,
    PositionDelta,
}

impl FillSource {
    fn log_prefix(&self) -> &'static str {
        match self {
            FillSource::WebSocket => &PREFIX_WEBSOCKET,
            FillSource::RestApi => &PREFIX_REST_API,
            FillSource::PositionDelta => &PREFIX_POSITION_DELTA,
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
    /// Atomic status for lock-free fast-path rejection
    atomic_status: Arc<AtomicU8>,
    /// Expected client_order_id for fast ownership check (avoids RwLock)
    expected_cloid: Arc<Mutex<Option<String>>>,
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
            atomic_status: Arc::new(AtomicU8::new(STATUS_IDLE)),
            expected_cloid: Arc::new(Mutex::new(None)),
        }
    }

    /// Create with external atomic status (for sharing with other services)
    pub fn with_atomic_status(
        bot_state: Arc<RwLock<BotState>>,
        hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
        pacifica_trading: Arc<PacificaTrading>,
        pacifica_ws_trading: Arc<PacificaWsTrading>,
        symbol: String,
        processed_fills: Arc<Mutex<HashSet<String>>>,
        baseline_updater: Option<PositionBaselineUpdater>,
        atomic_status: Arc<AtomicU8>,
        expected_cloid: Arc<Mutex<Option<String>>>,
    ) -> Self {
        Self {
            bot_state,
            hedge_tx,
            pacifica_trading,
            pacifica_ws_trading,
            symbol,
            processed_fills,
            baseline_updater,
            atomic_status,
            expected_cloid,
        }
    }

    /// Update expected client_order_id when placing an order
    pub fn set_expected_cloid(&self, cloid: Option<String>) {
        *self.expected_cloid.lock() = cloid;
    }

    /// Get the atomic status handle for external updates
    pub fn atomic_status(&self) -> &Arc<AtomicU8> {
        &self.atomic_status
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
    /// Uses client_order_id directly as the dedup key (avoids allocations).
    pub fn try_mark_processed(&self, _fill_type: FillType, client_order_id: &str) -> bool {
        let mut processed = self.processed_fills.lock();

        // Use client_order_id directly - it's already unique per order
        if processed.contains(client_order_id) {
            return false;
        }

        processed.insert(client_order_id.to_string());
        true
    }

    /// Clear the processed fills set (call at end of cycle to prevent memory growth).
    pub fn clear_processed_fills(&self) {
        self.processed_fills.lock().clear();
    }

    /// Fast-path check using atomics - no locks needed.
    /// Returns true if fill processing should continue, false to reject early.
    #[inline]
    fn atomic_fast_path_check(&self) -> bool {
        let status = self.atomic_status.load(Ordering::Acquire);
        // Only process fills when we have an active order (OrderPlaced state)
        status == STATUS_ORDER_PLACED
    }

    /// Fast ownership check using cached cloid - uses parking_lot Mutex (no async)
    #[inline]
    fn fast_is_our_order(&self, client_order_id: Option<&str>) -> bool {
        match client_order_id {
            Some(cloid) => {
                let expected = self.expected_cloid.lock();
                expected.as_ref().map(|e| e == cloid).unwrap_or(false)
            }
            None => false,
        }
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

        // 1. Update state to Filled IMMEDIATELY (RwLock first, then atomic)
        {
            let mut state = self.bot_state.write().await;
            state.mark_filled(filled_size, side);
        }
        // Update atomic status AFTER RwLock state is updated (prevents race condition)
        self.atomic_status.store(STATUS_FILLED, Ordering::Release);

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
                        &*PREFIX_FILL_HANDLER,
                        "✓✓".green().bold(),
                        rest_count,
                        ws_count,
                        source_name
                    );
                }
                Err(e) => {
                    error!(
                        "{} {} Background dual cancellation failed: {} [{}]",
                        &*PREFIX_FILL_HANDLER,
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
            // Channel closed - likely during shutdown. Log but don't panic.
            error!(
                "{} {} Hedge channel closed (expected during shutdown): {}",
                prefix,
                "✗".red().bold(),
                e
            );
        }

        // Clear expected cloid after fill processing to prevent stale state
        self.set_expected_cloid(None);

        fill_start
    }

    /// Check if an order is ours by comparing client_order_id (async fallback).
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
    /// Optimized with atomic fast-path rejection and batched lock acquisitions.
    pub async fn handle_fill(
        &self,
        source: FillSource,
        fill_type: FillType,
        client_order_id: Option<&str>,
        side_str: &str,
        filled_size: f64,
        fill_price: f64,
    ) -> bool {
        // FAST PATH 1: Atomic status check (no locks)
        if !self.atomic_fast_path_check() {
            return false;
        }

        // FAST PATH 2: Ownership check using cached cloid (parking_lot Mutex - fast)
        if !self.fast_is_our_order(client_order_id) {
            debug!("{} Fill is not for our order, ignoring", source.log_prefix());
            return false;
        }

        // FAST PATH 3: Client order ID required for deduplication
        let cloid = match client_order_id {
            Some(id) => id,
            None => {
                debug!("{} No client_order_id, cannot deduplicate", source.log_prefix());
                return false;
            }
        };

        // FAST PATH 4: Deduplication check (parking_lot Mutex - fast)
        if !self.try_mark_processed(fill_type, cloid) {
            debug!(
                "{} {:?} fill already processed (duplicate), skipping",
                source.log_prefix(), fill_type
            );
            return false;
        }

        // Parse side (no locks)
        let side = match Self::parse_side(side_str) {
            Some(s) => s,
            None => {
                error!("{} Unknown side: {}", source.log_prefix(), side_str);
                return false;
            }
        };

        // Process the fill (updates RwLock state, then atomic status, then triggers hedge)
        self.process_fill(source, fill_type, side, filled_size, fill_price, Some(side_str))
            .await;

        true
    }
}
