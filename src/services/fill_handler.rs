//! Unified fill handling logic for all detection methods.
//!
//! This module provides a single `FillHandler` that encapsulates the common
//! fill processing logic used by WebSocket, REST, and position-based detection.
//!
//! Deduplication is handled by the hedge service (single consumer), not here.
//! This keeps the fill handler lock-free for maximum hot-path performance.

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use colored::Colorize;
use once_cell::sync::Lazy;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};

use crate::bot::BotState;
use crate::connector::pacifica::{PacificaTrading, PacificaWsTrading, PositionBaselineUpdater};
use crate::services::HedgeEvent;
use crate::strategy::OrderSide;
use crate::util::cancel::dual_cancel;

/// Fast FNV-1a hash for client_order_id comparison (lock-free).
/// Uses 64-bit FNV-1a which has good distribution for short strings like UUIDs.
#[inline]
pub fn hash_cloid(s: &str) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET;
    for byte in s.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

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


/// Fill handler that provides unified fill processing logic.
///
/// All fill detection services should use this handler to:
/// 1. Check ownership (is this fill for our order?)
/// 2. Update bot state
/// 3. Trigger background cancellation
/// 4. Send hedge event (dedup handled by hedge service)
#[derive(Clone)]
pub struct FillHandler {
    bot_state: Arc<RwLock<BotState>>,
    hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
    pacifica_trading: Arc<PacificaTrading>,
    pacifica_ws_trading: Arc<PacificaWsTrading>,
    symbol: String,
    baseline_updater: Option<PositionBaselineUpdater>,
    /// Atomic status for lock-free fast-path rejection
    atomic_status: Arc<AtomicU8>,
    /// Hash of expected client_order_id for lock-free ownership check
    /// 0 means no expected order
    expected_cloid_hash: Arc<AtomicU64>,
}

impl FillHandler {
    pub fn new(
        bot_state: Arc<RwLock<BotState>>,
        hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
        pacifica_trading: Arc<PacificaTrading>,
        pacifica_ws_trading: Arc<PacificaWsTrading>,
        symbol: String,
        baseline_updater: Option<PositionBaselineUpdater>,
    ) -> Self {
        Self {
            bot_state,
            hedge_tx,
            pacifica_trading,
            pacifica_ws_trading,
            symbol,
            baseline_updater,
            atomic_status: Arc::new(AtomicU8::new(STATUS_IDLE)),
            expected_cloid_hash: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create with external atomic status (for sharing with other services)
    pub fn with_atomic_status(
        bot_state: Arc<RwLock<BotState>>,
        hedge_tx: mpsc::UnboundedSender<HedgeEvent>,
        pacifica_trading: Arc<PacificaTrading>,
        pacifica_ws_trading: Arc<PacificaWsTrading>,
        symbol: String,
        baseline_updater: Option<PositionBaselineUpdater>,
        atomic_status: Arc<AtomicU8>,
        expected_cloid_hash: Arc<AtomicU64>,
    ) -> Self {
        Self {
            bot_state,
            hedge_tx,
            pacifica_trading,
            pacifica_ws_trading,
            symbol,
            baseline_updater,
            atomic_status,
            expected_cloid_hash,
        }
    }

    /// Update expected client_order_id when placing an order (lock-free)
    pub fn set_expected_cloid(&self, cloid: Option<&str>) {
        let hash = cloid.map(hash_cloid).unwrap_or(0);
        self.expected_cloid_hash.store(hash, Ordering::Release);
    }

    /// Get the atomic status handle for external updates
    pub fn atomic_status(&self) -> &Arc<AtomicU8> {
        &self.atomic_status
    }

    /// Get the expected cloid hash handle for external updates
    pub fn expected_cloid_hash(&self) -> &Arc<AtomicU64> {
        &self.expected_cloid_hash
    }

    /// Parse order side from string (handles both "buy"/"sell" and "bid"/"ask").
    pub fn parse_side(side_str: &str) -> Option<OrderSide> {
        match side_str {
            "buy" | "bid" => Some(OrderSide::Buy),
            "sell" | "ask" => Some(OrderSide::Sell),
            _ => None,
        }
    }

    /// Fast-path check using atomics - no locks needed.
    /// Returns true if fill processing should continue, false to reject early.
    #[inline]
    fn atomic_fast_path_check(&self) -> bool {
        let status = self.atomic_status.load(Ordering::Acquire);
        // Only process fills when we have an active order (OrderPlaced state)
        status == STATUS_ORDER_PLACED
    }

    /// Fast ownership check using atomic hash comparison (completely lock-free)
    #[inline]
    fn fast_is_our_order(&self, client_order_id: Option<&str>) -> bool {
        match client_order_id {
            Some(cloid) => {
                let expected_hash = self.expected_cloid_hash.load(Ordering::Acquire);
                // 0 means no expected order
                expected_hash != 0 && hash_cloid(cloid) == expected_hash
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
    /// 4. Triggers hedge immediately (dedup handled by hedge service)
    ///
    /// # Arguments
    /// * `source` - Where the fill was detected (for logging)
    /// * `fill_type` - Full or partial fill
    /// * `side` - Order side (Buy/Sell)
    /// * `filled_size` - Amount filled
    /// * `fill_price` - Average fill price
    /// * `client_order_id` - Order ID for hedge service deduplication
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
        client_order_id: &str,
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

        // 4. Trigger hedge immediately (hedge service handles dedup)
        let latency_ms = fill_start.elapsed().as_secs_f64() * 1000.0;
        info!(
            "{} {} Hedge triggered in {:.1}ms (cancellation running async)",
            format!("[{}]", self.symbol).bright_white().bold(),
            format!("{:?} fill", fill_type).green().bold(),
            latency_ms
        );

        let hedge_event = HedgeEvent {
            side,
            size: filled_size,
            avg_price: fill_price,
            fill_timestamp: fill_start,
            client_order_id: client_order_id.to_string(),
        };

        if let Err(e) = self.hedge_tx.send(hedge_event) {
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
    /// Optimized with atomic fast-path rejection. Dedup is handled by hedge service.
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

        // FAST PATH 3: Client order ID required for hedge service dedup
        let cloid = match client_order_id {
            Some(id) => id,
            None => {
                debug!("{} No client_order_id, cannot process", source.log_prefix());
                return false;
            }
        };

        // Parse side (no locks)
        let side = match Self::parse_side(side_str) {
            Some(s) => s,
            None => {
                error!("{} Unknown side: {}", source.log_prefix(), side_str);
                return false;
            }
        };

        // Process the fill (updates state, triggers hedge - dedup handled by hedge service)
        self.process_fill(source, fill_type, side, filled_size, fill_price, cloid, Some(side_str))
            .await;

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============== hash_cloid tests ==============

    #[test]
    fn test_hash_cloid_deterministic() {
        let cloid = "550e8400-e29b-41d4-a716-446655440000";
        let hash1 = hash_cloid(cloid);
        let hash2 = hash_cloid(cloid);
        assert_eq!(hash1, hash2, "Same input should produce same hash");
    }

    #[test]
    fn test_hash_cloid_different_inputs() {
        let hash1 = hash_cloid("cloid-1");
        let hash2 = hash_cloid("cloid-2");
        assert_ne!(hash1, hash2, "Different inputs should produce different hashes");
    }

    #[test]
    fn test_hash_cloid_empty_string() {
        let hash = hash_cloid("");
        // FNV-1a hash of empty string should be the offset basis
        assert_eq!(hash, 0xcbf29ce484222325);
    }

    // ============== parse_side tests ==============

    #[test]
    fn test_parse_side_buy() {
        assert_eq!(FillHandler::parse_side("buy"), Some(OrderSide::Buy));
        assert_eq!(FillHandler::parse_side("bid"), Some(OrderSide::Buy));
    }

    #[test]
    fn test_parse_side_sell() {
        assert_eq!(FillHandler::parse_side("sell"), Some(OrderSide::Sell));
        assert_eq!(FillHandler::parse_side("ask"), Some(OrderSide::Sell));
    }

    #[test]
    fn test_parse_side_invalid() {
        assert_eq!(FillHandler::parse_side("long"), None);
        assert_eq!(FillHandler::parse_side("short"), None);
        assert_eq!(FillHandler::parse_side(""), None);
        assert_eq!(FillHandler::parse_side("BUY"), None); // Case sensitive
    }

    // ============== FillSource tests ==============

    #[test]
    fn test_fill_source_names() {
        assert_eq!(FillSource::WebSocket.name(), "WebSocket");
        assert_eq!(FillSource::RestApi.name(), "REST");
        assert_eq!(FillSource::PositionDelta.name(), "Position");
    }

    // ============== Atomic status tests ==============

    #[test]
    fn test_atomic_status_order_placed_allows_fill() {
        let status = AtomicU8::new(STATUS_ORDER_PLACED);
        assert_eq!(status.load(Ordering::Acquire), STATUS_ORDER_PLACED);
    }

    #[test]
    fn test_atomic_status_idle_rejects_fill() {
        let status = AtomicU8::new(STATUS_IDLE);
        assert_ne!(status.load(Ordering::Acquire), STATUS_ORDER_PLACED);
    }

    #[test]
    fn test_atomic_status_filled_rejects_fill() {
        let status = AtomicU8::new(STATUS_FILLED);
        assert_ne!(status.load(Ordering::Acquire), STATUS_ORDER_PLACED);
    }

    // ============== Expected cloid hash tests ==============

    #[test]
    fn test_expected_cloid_hash_matching() {
        let expected_cloid = Arc::new(AtomicU64::new(0));
        let cloid = "test-cloid-123";

        // Set expected cloid
        expected_cloid.store(hash_cloid(cloid), Ordering::Release);

        // Check matching
        let stored_hash = expected_cloid.load(Ordering::Acquire);
        assert_eq!(stored_hash, hash_cloid(cloid));
    }

    #[test]
    fn test_expected_cloid_hash_not_matching() {
        let expected_cloid = Arc::new(AtomicU64::new(0));
        let cloid1 = "test-cloid-123";
        let cloid2 = "different-cloid";

        // Set expected cloid
        expected_cloid.store(hash_cloid(cloid1), Ordering::Release);

        // Check not matching
        let stored_hash = expected_cloid.load(Ordering::Acquire);
        assert_ne!(stored_hash, hash_cloid(cloid2));
    }

    #[test]
    fn test_expected_cloid_hash_zero_means_no_order() {
        let expected_cloid = Arc::new(AtomicU64::new(0));

        // Zero means no expected order
        let stored_hash = expected_cloid.load(Ordering::Acquire);
        assert_eq!(stored_hash, 0);
    }

    // ============== Integration-like tests with FillHandler ==============

    /// Helper to create a minimal FillHandler for testing atomic operations
    fn create_test_fill_handler() -> (FillHandler, mpsc::UnboundedReceiver<HedgeEvent>) {
        use crate::bot::BotState;

        let bot_state = Arc::new(RwLock::new(BotState::new()));
        let (hedge_tx, hedge_rx) = mpsc::unbounded_channel();

        // For these tests we need real PacificaTrading instances
        // but we won't call any methods that use them
        // Instead, we'll test the atomic fast-path directly

        // Create a minimal fill handler
        let handler = FillHandler {
            bot_state,
            hedge_tx,
            // These will be unused in fast-path tests
            pacifica_trading: create_dummy_pacifica_trading(),
            pacifica_ws_trading: create_dummy_pacifica_ws_trading(),
            symbol: "SOL".to_string(),
            baseline_updater: None,
            atomic_status: Arc::new(AtomicU8::new(STATUS_IDLE)),
            expected_cloid_hash: Arc::new(AtomicU64::new(0)),
        };

        (handler, hedge_rx)
    }

    /// Create a dummy PacificaTrading - only used for struct creation, never called
    fn create_dummy_pacifica_trading() -> Arc<PacificaTrading> {
        use crate::connector::pacifica::PacificaCredentials;

        // This will fail if methods are called, but we won't call them in these tests
        let creds = PacificaCredentials {
            account: "dummy".to_string(),
            agent_wallet: "dummy".to_string(),
            private_key: "dummy".to_string(),
        };
        // Note: This will succeed as it's just constructing the client
        Arc::new(PacificaTrading::new(creds).expect("Dummy client creation"))
    }

    /// Create a dummy PacificaWsTrading
    fn create_dummy_pacifica_ws_trading() -> Arc<PacificaWsTrading> {
        use crate::connector::pacifica::PacificaCredentials;

        let creds = PacificaCredentials {
            account: "dummy".to_string(),
            agent_wallet: "dummy".to_string(),
            private_key: "dummy".to_string(),
        };
        Arc::new(PacificaWsTrading::new(creds, false))
    }

    #[test]
    fn test_fill_handler_set_expected_cloid() {
        let (handler, _rx) = create_test_fill_handler();

        // Initially no expected cloid
        assert_eq!(handler.expected_cloid_hash.load(Ordering::Acquire), 0);

        // Set expected cloid
        let cloid = "test-order-123";
        handler.set_expected_cloid(Some(cloid));
        assert_eq!(
            handler.expected_cloid_hash.load(Ordering::Acquire),
            hash_cloid(cloid)
        );

        // Clear expected cloid
        handler.set_expected_cloid(None);
        assert_eq!(handler.expected_cloid_hash.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_fill_handler_atomic_fast_path_check_idle() {
        let (handler, _rx) = create_test_fill_handler();

        // Status is IDLE by default
        assert!(!handler.atomic_fast_path_check(), "Should reject when IDLE");
    }

    #[test]
    fn test_fill_handler_atomic_fast_path_check_order_placed() {
        let (handler, _rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);
        assert!(
            handler.atomic_fast_path_check(),
            "Should accept when ORDER_PLACED"
        );
    }

    #[test]
    fn test_fill_handler_atomic_fast_path_check_filled() {
        let (handler, _rx) = create_test_fill_handler();

        // Set status to FILLED
        handler.atomic_status.store(STATUS_FILLED, Ordering::Release);
        assert!(
            !handler.atomic_fast_path_check(),
            "Should reject when FILLED"
        );
    }

    #[test]
    fn test_fill_handler_fast_is_our_order_matching() {
        let (handler, _rx) = create_test_fill_handler();

        let cloid = "my-order-id";
        handler.set_expected_cloid(Some(cloid));

        assert!(
            handler.fast_is_our_order(Some(cloid)),
            "Should match when cloid equals expected"
        );
    }

    #[test]
    fn test_fill_handler_fast_is_our_order_not_matching() {
        let (handler, _rx) = create_test_fill_handler();

        handler.set_expected_cloid(Some("my-order-id"));

        assert!(
            !handler.fast_is_our_order(Some("other-order-id")),
            "Should not match when cloid differs"
        );
    }

    #[test]
    fn test_fill_handler_fast_is_our_order_none_cloid() {
        let (handler, _rx) = create_test_fill_handler();

        handler.set_expected_cloid(Some("my-order-id"));

        assert!(
            !handler.fast_is_our_order(None),
            "Should reject when cloid is None"
        );
    }

    #[test]
    fn test_fill_handler_fast_is_our_order_no_expected() {
        let (handler, _rx) = create_test_fill_handler();

        // No expected cloid set (default is 0)
        assert!(
            !handler.fast_is_our_order(Some("any-order-id")),
            "Should reject when no expected cloid"
        );
    }

    // ============== Async tests ==============

    #[tokio::test]
    async fn test_handle_fill_rejects_when_idle() {
        let (handler, mut rx) = create_test_fill_handler();

        // Status is IDLE by default
        let cloid = "test-order-123";
        handler.set_expected_cloid(Some(cloid));

        let result = handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                Some(cloid),
                "buy",
                1.0,
                100.0,
            )
            .await;

        assert!(!result, "Should reject fill when status is IDLE");

        // No hedge event should be sent
        assert!(rx.try_recv().is_err(), "No hedge event should be sent");
    }

    #[tokio::test]
    async fn test_handle_fill_rejects_wrong_cloid() {
        let (handler, mut rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);
        handler.set_expected_cloid(Some("expected-order"));

        let result = handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                Some("wrong-order"), // Different cloid
                "buy",
                1.0,
                100.0,
            )
            .await;

        assert!(!result, "Should reject fill with wrong cloid");
        assert!(rx.try_recv().is_err(), "No hedge event should be sent");
    }

    #[tokio::test]
    async fn test_handle_fill_rejects_missing_cloid() {
        let (handler, mut rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);
        handler.set_expected_cloid(Some("expected-order"));

        let result = handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                None, // No cloid
                "buy",
                1.0,
                100.0,
            )
            .await;

        assert!(!result, "Should reject fill with no cloid");
        assert!(rx.try_recv().is_err(), "No hedge event should be sent");
    }

    #[tokio::test]
    async fn test_handle_fill_success_sends_hedge_event() {
        let (handler, mut rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);

        let cloid = "my-order-123";
        handler.set_expected_cloid(Some(cloid));

        let result = handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                Some(cloid),
                "buy",
                1.5,
                99.50,
            )
            .await;

        assert!(result, "Should accept valid fill");

        // Check hedge event was sent
        let event = rx.try_recv().expect("Hedge event should be sent");
        assert_eq!(event.client_order_id, cloid);
        assert_eq!(event.size, 1.5);
        assert_eq!(event.avg_price, 99.50);
        assert!(matches!(event.side, OrderSide::Buy));
    }

    #[tokio::test]
    async fn test_handle_fill_updates_status_to_filled() {
        let (handler, _rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);

        let cloid = "my-order-123";
        handler.set_expected_cloid(Some(cloid));

        handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                Some(cloid),
                "sell",
                2.0,
                101.0,
            )
            .await;

        // Status should be updated to FILLED
        assert_eq!(
            handler.atomic_status.load(Ordering::Acquire),
            STATUS_FILLED,
            "Status should be FILLED after processing"
        );
    }

    #[tokio::test]
    async fn test_handle_fill_clears_expected_cloid() {
        let (handler, _rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);

        let cloid = "my-order-123";
        handler.set_expected_cloid(Some(cloid));

        handler
            .handle_fill(
                FillSource::RestApi,
                FillType::Full,
                Some(cloid),
                "buy",
                1.0,
                100.0,
            )
            .await;

        // Expected cloid should be cleared after processing
        assert_eq!(
            handler.expected_cloid_hash.load(Ordering::Acquire),
            0,
            "Expected cloid should be cleared after fill"
        );
    }

    #[tokio::test]
    async fn test_handle_fill_updates_bot_state() {
        use crate::bot::BotStatus;

        let (handler, _rx) = create_test_fill_handler();

        // Set status to ORDER_PLACED
        handler.atomic_status.store(STATUS_ORDER_PLACED, Ordering::Release);

        let cloid = "my-order-123";
        handler.set_expected_cloid(Some(cloid));

        handler
            .handle_fill(
                FillSource::WebSocket,
                FillType::Full,
                Some(cloid),
                "buy",
                3.0,
                50.0,
            )
            .await;

        // Check bot state was updated to Filled
        let state = handler.bot_state.read().await;
        assert!(
            matches!(state.status, BotStatus::Filled),
            "Bot state should be Filled"
        );
    }
}
