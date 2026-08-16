use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::atomic_price::{AtomicInstant, AtomicPricePair};
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};

// ============================================================================
// ATOMIC STATUS FOR LOCK-FREE HOT PATH CHECKS
// ============================================================================

/// Atomic bot status for lock-free checks in hot path
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtomicBotStatus {
    Idle = 0,
    OrderPlaced = 1,
    Filled = 2,
    Hedging = 3,
    Complete = 4,
    Error = 5,
}

impl From<&BotStatus> for AtomicBotStatus {
    fn from(status: &BotStatus) -> Self {
        match status {
            BotStatus::Idle => AtomicBotStatus::Idle,
            BotStatus::OrderPlaced => AtomicBotStatus::OrderPlaced,
            BotStatus::Filled => AtomicBotStatus::Filled,
            BotStatus::Hedging => AtomicBotStatus::Hedging,
            BotStatus::Complete => AtomicBotStatus::Complete,
            BotStatus::Error(_) => AtomicBotStatus::Error,
        }
    }
}

// ============================================================================
// LIGHTWEIGHT ORDER SNAPSHOT (AVOID CLONING FULL ORDER)
// ============================================================================

/// Minimal order data needed for monitoring (avoids full clone)
#[derive(Debug, Clone, Copy)]
pub struct OrderSnapshot {
    pub side: OrderSide,
    pub price: f64,
    pub size: f64,
    pub initial_profit_bps: f64,
    pub placed_at: Instant,
}

/// Shared order snapshot updated atomically by order placer
/// Uses Option wrapped in Mutex for atomic swap semantics
pub struct SharedOrderSnapshot {
    inner: Mutex<Option<OrderSnapshot>>,
}

impl SharedOrderSnapshot {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    #[inline]
    pub fn get(&self) -> Option<OrderSnapshot> {
        *self.inner.lock()
    }

    #[inline]
    pub fn set(&self, snapshot: Option<OrderSnapshot>) {
        *self.inner.lock() = snapshot;
    }
}

// ============================================================================
// CANCELLATION REQUEST CHANNEL (DECOUPLE FROM HOT PATH)
// ============================================================================

/// Cancellation request sent from monitor to cancellation handler
#[derive(Debug)]
pub enum CancelRequest {
    /// Cancel due to age expiry
    AgeExpiry { symbol: String, reason: String },
    /// Cancel due to profit deviation
    ProfitDeviation { symbol: String, current_profit_bps: f64, deviation_bps: f64 },
}

// ============================================================================
// ORDER MONITOR SERVICE (OPTIMIZED)
// ============================================================================

/// Order monitoring service
///
/// Monitors active orders for:
/// 1. Age - refreshes order if age > order_refresh_interval_secs
/// 2. Profit deviation - cancels if profit drops > profit_cancel_threshold_bps
/// 3. Periodic profit logging every 2 seconds
///
/// Key optimizations:
/// - Lock-free status check via atomic
/// - No REST calls in hot loop (delegated to separate task)
/// - No cloning (uses lightweight snapshot)
/// - No allocations in hot path
pub struct OrderMonitorService {
    // Shared state (write lock only needed for mutations)
    pub bot_state: Arc<RwLock<BotState>>,
    
    // Lock-free status for hot path (updated by state manager)
    pub atomic_status: Arc<AtomicU8>,
    
    // Lightweight order snapshot (updated when order placed)
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    
    // Price feeds (lock-free reads via SeqLock atomics)
    pub pacifica_prices: Arc<AtomicPricePair>,
    pub hyperliquid_prices: Arc<AtomicPricePair>,
    
    // Configuration
    pub config: Config,
    pub evaluator: OpportunityEvaluator,
    
    // Trading connectors (only used by cancellation task)
    pub pacifica_trading: Arc<PacificaTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,

    // Lock-free grace period timestamp (shared with main loop)
    pub last_cancellation_time: Arc<AtomicInstant>,

    // Channel for cancel requests (decouples hot path from I/O)
    pub cancel_tx: mpsc::Sender<CancelRequest>,
}

impl OrderMonitorService {
    /// Create a new order monitor service with cancellation channel
    pub fn new(
        bot_state: Arc<RwLock<BotState>>,
        atomic_status: Arc<AtomicU8>,
        order_snapshot: Arc<SharedOrderSnapshot>,
        pacifica_prices: Arc<AtomicPricePair>,
        hyperliquid_prices: Arc<AtomicPricePair>,
        config: Config,
        evaluator: OpportunityEvaluator,
        pacifica_trading: Arc<PacificaTrading>,
        hyperliquid_trading: Arc<HyperliquidTrading>,
        last_cancellation_time: Arc<AtomicInstant>,
    ) -> (Self, mpsc::Receiver<CancelRequest>) {
        // Bounded channel to prevent unbounded growth, but large enough to not block
        let (cancel_tx, cancel_rx) = mpsc::channel(64);

        let service = Self {
            bot_state,
            atomic_status,
            order_snapshot,
            pacifica_prices,
            hyperliquid_prices,
            config,
            evaluator,
            pacifica_trading,
            hyperliquid_trading,
            last_cancellation_time,
            cancel_tx,
        };
        
        (service, cancel_rx)
    }

    /// Main monitoring loop - LATENCY CRITICAL
    /// 
    /// This loop runs at 1kHz and must complete each iteration in <1ms.
    /// All I/O operations are delegated to separate tasks via channels.
    pub async fn run_monitor_loop(&self) {
        let mut monitor_interval = interval(Duration::from_millis(1));
        
        // Timing thresholds
        let age_threshold = Duration::from_secs(self.config.order_refresh_interval_secs);
        let profit_threshold = self.config.profit_cancel_threshold_bps;

        loop {
            monitor_interval.tick().await;

            // FAST PATH: Lock-free status check
            let status = self.atomic_status.load(Ordering::Acquire);
            if status != AtomicBotStatus::OrderPlaced as u8 {
                continue;
            }

            // Get order snapshot (single lock, no clone of complex types)
            let snapshot = match self.order_snapshot.get() {
                Some(s) => s,
                None => continue,
            };

            // Get prices (lock-free SeqLock read)
            let (hl_bid, hl_ask) = self.hyperliquid_prices.load();
            if hl_bid == 0.0 || hl_ask == 0.0 {
                continue;
            }

            let age = snapshot.placed_at.elapsed();

            // Check 1: Age threshold
            if age > age_threshold {
                // Send cancel request (non-blocking)
                let _ = self.cancel_tx.try_send(CancelRequest::AgeExpiry {
                    symbol: self.config.symbol.clone(),
                    reason: format!("age {}ms > {}s threshold", age.as_millis(), self.config.order_refresh_interval_secs),
                });
                continue;
            }

            // Check 2: Profit deviation (using raw method - no allocation)
            let current_profit = self.evaluator.recalculate_profit_raw(
                snapshot.side,
                snapshot.price,
                hl_bid,
                hl_ask,
            );
            
            // Consistent calculation: positive = profit dropped (bad)
            let profit_change = snapshot.initial_profit_bps - current_profit;
            let profit_deviation = profit_change.abs();

            if profit_deviation > profit_threshold {
                // Send cancel request (non-blocking)
                let _ = self.cancel_tx.try_send(CancelRequest::ProfitDeviation {
                    symbol: self.config.symbol.clone(),
                    current_profit_bps: current_profit,
                    deviation_bps: profit_deviation,
                });
            }
        }
    }

    /// Cancellation handler task - runs separately from hot path
    /// 
    /// Handles all I/O operations: REST API calls, state updates, logging
    pub async fn run_cancellation_handler(
        &self,
        mut cancel_rx: mpsc::Receiver<CancelRequest>,
    ) {
        let mut rate_limit = RateLimitTracker::new();

        while let Some(request) = cancel_rx.recv().await {
            // Check rate limit backoff
            if rate_limit.should_skip() {
                debug!(
                    "[CANCEL] Skipping cancellation (rate limit backoff, {:.1}s remaining)",
                    rate_limit.remaining_backoff_secs()
                );
                continue;
            }

            // Double-check state hasn't changed (order might have filled)
            let status = self.atomic_status.load(Ordering::Acquire);
            if status != AtomicBotStatus::OrderPlaced as u8 {
                debug!("[CANCEL] Skipping - status changed to {}", status);
                continue;
            }

            // Get current snapshot for logging
            let _snapshot = self.order_snapshot.get();

            // Check for partial fills before cancelling
            match self.check_for_fills().await {
                FillCheckResult::HasFills(amount) => {
                    info!(
                        "[CANCEL] Order has fills ({}) - skipping cancellation, waiting for fill detection",
                        amount
                    );
                    continue;
                }
                FillCheckResult::NotFound => {
                    debug!("[CANCEL] Order not in open orders - might be filled/cancelled");
                    continue;
                }
                FillCheckResult::NoFills => {
                    // Safe to proceed with cancellation
                }
                FillCheckResult::CheckFailed(e) => {
                    debug!("[CANCEL] Fill check failed: {} - proceeding with cancellation", e);
                    // Continue with cancellation (safer than leaving hanging orders)
                }
            }

            // Log the cancellation reason
            match &request {
                CancelRequest::AgeExpiry { reason, .. } => {
                    info!("[CANCEL] Age expiry: {}", reason);
                }
                CancelRequest::ProfitDeviation { current_profit_bps, deviation_bps, .. } => {
                    info!(
                        "[CANCEL] Profit deviation: current={:.2} bps, deviation={:.2} bps",
                        current_profit_bps, deviation_bps
                    );
                }
            }

            // Execute cancellation
            let symbol = match &request {
                CancelRequest::AgeExpiry { symbol, .. } => symbol,
                CancelRequest::ProfitDeviation { symbol, .. } => symbol,
            };

            match self.pacifica_trading.cancel_all_orders(false, Some(symbol), false).await {
                Ok(_) => {
                    rate_limit.record_success();

                    // Clear state only if still in OrderPlaced
                    let mut state = self.bot_state.write().await;
                    if matches!(state.status, BotStatus::OrderPlaced) {
                        state.clear_active_order();
                        self.atomic_status.store(AtomicBotStatus::Idle as u8, Ordering::Release);
                        self.order_snapshot.set(None);
                        // Update lock-free grace period timestamp
                        self.last_cancellation_time.set_now();
                    }
                    drop(state);

                    // Refresh prices in parallel (not blocking the handler)
                    self.refresh_prices_parallel().await;
                }
                Err(e) => {
                    if is_rate_limit_error(&e) {
                        rate_limit.record_error();
                        warn!(
                            "[CANCEL] Rate limit exceeded. Backing off for {}s (attempt #{})",
                            rate_limit.get_backoff_secs(),
                            rate_limit.consecutive_errors()
                        );
                    } else {
                        warn!("[CANCEL] Failed to cancel: {}", e);
                    }
                }
            }
        }
    }

    /// Periodic profit logging task - runs at 0.5 Hz (every 2 seconds)
    pub async fn run_profit_logger(&self) {
        let mut log_interval = interval(Duration::from_secs(2));

        loop {
            log_interval.tick().await;

            // Only log for active orders
            let status = self.atomic_status.load(Ordering::Acquire);
            if status != AtomicBotStatus::OrderPlaced as u8 {
                continue;
            }

            let snapshot = match self.order_snapshot.get() {
                Some(s) => s,
                None => continue,
            };

            let (hl_bid, hl_ask) = self.hyperliquid_prices.load();
            if hl_bid == 0.0 || hl_ask == 0.0 {
                continue;
            }

            let current_profit = self.evaluator.recalculate_profit_raw(
                snapshot.side,
                snapshot.price,
                hl_bid,
                hl_ask,
            );
            let profit_change = current_profit - snapshot.initial_profit_bps;
            let age_ms = snapshot.placed_at.elapsed().as_millis();

            let hedge_price = match snapshot.side {
                OrderSide::Buy => hl_bid,
                OrderSide::Sell => hl_ask,
            };

            info!(
                "[PROFIT] Current: {:.2} bps (initial: {:.2}, change: {:+.2}) | PAC: ${:.4} | HL: ${:.4} | Age: {:.3}s",
                current_profit,
                snapshot.initial_profit_bps,
                profit_change,
                snapshot.price,
                hedge_price,
                age_ms as f64 / 1000.0
            );
        }
    }

    /// Check if order has fills (called from cancellation handler, not hot path)
    async fn check_for_fills(&self) -> FillCheckResult {
        // Get client_order_id from full state (only in cancellation handler)
        let state = self.bot_state.read().await;
        let client_order_id = match &state.active_order {
            Some(order) => order.client_order_id.clone(),
            None => return FillCheckResult::NotFound,
        };
        drop(state);

        match self.pacifica_trading.get_open_orders().await {
            Ok(orders) => {
                if let Some(order) = orders.iter().find(|o| o.client_order_id == client_order_id) {
                    let filled_amount: f64 = fast_float::parse(&order.filled_amount).unwrap_or(0.0);
                    if filled_amount > 0.0 {
                        FillCheckResult::HasFills(filled_amount)
                    } else {
                        FillCheckResult::NoFills
                    }
                } else {
                    FillCheckResult::NotFound
                }
            }
            Err(e) => FillCheckResult::CheckFailed(e.to_string()),
        }
    }

    /// Refresh prices from both exchanges in parallel
    async fn refresh_prices_parallel(&self) {
        let pac_future = self.pacifica_trading.get_best_bid_ask_rest(
            &self.config.symbol,
            self.config.agg_level,
        );
        let hl_future = self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol);

        let (pac_result, hl_result) = tokio::join!(pac_future, hl_future);

        if let Ok(Some((bid, ask))) = pac_result {
            self.pacifica_prices.store(bid, ask);
            debug!("[REFRESH] Pacifica: bid=${:.6}, ask=${:.6}", bid, ask);
        }

        if let Ok(Some((bid, ask))) = hl_result {
            self.hyperliquid_prices.store(bid, ask);
            debug!("[REFRESH] Hyperliquid: bid=${:.6}, ask=${:.6}", bid, ask);
        }
    }
}

/// Result of checking for partial fills
enum FillCheckResult {
    HasFills(f64),
    NoFills,
    NotFound,
    CheckFailed(String),
}

// ============================================================================
// HELPER: Update atomic status when BotState changes
// ============================================================================

/// Call this whenever BotState.status changes to keep atomic in sync
#[inline]
pub fn sync_atomic_status(atomic: &AtomicU8, status: &BotStatus) {
    let atomic_val = AtomicBotStatus::from(status) as u8;
    atomic.store(atomic_val, Ordering::Release);
}

/// Call this when placing a new order to update snapshot
#[inline]
pub fn update_order_snapshot(
    snapshot: &SharedOrderSnapshot,
    side: OrderSide,
    price: f64,
    size: f64,
    initial_profit_bps: f64,
) {
    snapshot.set(Some(OrderSnapshot {
        side,
        price,
        size,
        initial_profit_bps,
        placed_at: Instant::now(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_bot_status_from_all_variants() {
        assert_eq!(AtomicBotStatus::from(&BotStatus::Idle) as u8, 0);
        assert_eq!(AtomicBotStatus::from(&BotStatus::OrderPlaced) as u8, 1);
        assert_eq!(AtomicBotStatus::from(&BotStatus::Filled) as u8, 2);
        assert_eq!(AtomicBotStatus::from(&BotStatus::Hedging) as u8, 3);
        assert_eq!(AtomicBotStatus::from(&BotStatus::Complete) as u8, 4);
        assert_eq!(
            AtomicBotStatus::from(&BotStatus::Error("anything".to_string())) as u8,
            5
        );
    }

    #[test]
    fn test_sync_atomic_status() {
        let atomic = AtomicU8::new(0);

        let cases: Vec<(BotStatus, u8)> = vec![
            (BotStatus::Idle, 0),
            (BotStatus::OrderPlaced, 1),
            (BotStatus::Filled, 2),
            (BotStatus::Hedging, 3),
            (BotStatus::Complete, 4),
            (BotStatus::Error("err".to_string()), 5),
        ];

        for (status, expected) in cases {
            sync_atomic_status(&atomic, &status);
            assert_eq!(
                atomic.load(Ordering::Acquire),
                expected,
                "Failed for {:?}",
                status
            );
        }
    }

    #[test]
    fn test_shared_order_snapshot_none_on_new() {
        let snapshot = SharedOrderSnapshot::new();
        assert!(snapshot.get().is_none());
    }

    #[test]
    fn test_shared_order_snapshot_set_and_get() {
        let shared = SharedOrderSnapshot::new();

        // Set a snapshot
        let snap = OrderSnapshot {
            side: OrderSide::Buy,
            price: 100.5,
            size: 2.0,
            initial_profit_bps: 5.5,
            placed_at: Instant::now(),
        };
        shared.set(Some(snap));

        let got = shared.get().expect("should be Some");
        assert_eq!(got.side, OrderSide::Buy);
        assert_eq!(got.price, 100.5);
        assert_eq!(got.size, 2.0);
        assert_eq!(got.initial_profit_bps, 5.5);

        // Clear it
        shared.set(None);
        assert!(shared.get().is_none());
    }

    #[test]
    fn test_update_order_snapshot_helper() {
        let shared = SharedOrderSnapshot::new();

        update_order_snapshot(&shared, OrderSide::Sell, 200.0, 3.0, 7.5);

        let got = shared.get().expect("should be Some after update");
        assert_eq!(got.side, OrderSide::Sell);
        assert_eq!(got.price, 200.0);
        assert_eq!(got.size, 3.0);
        assert_eq!(got.initial_profit_bps, 7.5);
    }
}

// ============================================================================
// STARTUP HELPER
// ============================================================================

/// Spawn all monitor tasks
pub fn spawn_monitor_tasks(service: Arc<OrderMonitorService>, cancel_rx: mpsc::Receiver<CancelRequest>) {
    // Hot path monitor (1kHz)
    let service_clone = Arc::clone(&service);
    tokio::spawn(async move {
        service_clone.run_monitor_loop().await;
    });

    // Cancellation handler (processes cancel requests)
    let service_clone = Arc::clone(&service);
    tokio::spawn(async move {
        service_clone.run_cancellation_handler(cancel_rx).await;
    });

    // Profit logger (0.5 Hz)
    let service_clone = Arc::clone(&service);
    tokio::spawn(async move {
        service_clone.run_profit_logger().await;
    });
}
