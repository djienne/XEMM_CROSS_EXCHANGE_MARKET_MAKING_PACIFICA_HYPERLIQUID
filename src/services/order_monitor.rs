use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use arc_swap::ArcSwapOption;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::bot::{BotState, BotStatus};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::services::market_event::MarketSource;
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::rate_limit::{is_rate_limit_error, RateLimitTracker};
use crate::util::atomic_price::AtomicPrice;

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
/// Uses ArcSwap for lock-free snapshot reads
pub struct SharedOrderSnapshot {
    inner: ArcSwapOption<OrderSnapshot>,
}

impl SharedOrderSnapshot {
    pub fn new() -> Self {
        Self {
            inner: ArcSwapOption::from(None),
        }
    }

    #[inline]
    pub fn get(&self) -> Option<Arc<OrderSnapshot>> {
        self.inner.load_full()
    }

    #[inline]
    pub fn set(&self, snapshot: Option<OrderSnapshot>) {
        self.inner.store(snapshot.map(Arc::new));
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
/// - Event-driven checks (no polling interval)
/// - No REST calls in hot path (delegated to separate task)
/// - Snapshot reads are lock-free
pub struct OrderMonitorService {
    // Shared state (write lock only needed for mutations)
    pub bot_state: Arc<RwLock<BotState>>,
    
    // Lock-free status for hot path (updated by state manager)
    pub atomic_status: Arc<AtomicU8>,
    
    // Lightweight order snapshot (updated when order placed)
    pub order_snapshot: Arc<SharedOrderSnapshot>,
    
    // Price feeds (lock-free reads via parking_lot)
    pub pacifica_prices: Arc<AtomicPrice>,
    pub hyperliquid_prices: Arc<AtomicPrice>,
    
    // Configuration
    pub config: Config,
    pub evaluator: OpportunityEvaluator,
    pub age_threshold: Duration,
    pub profit_threshold: f64,
    
    // Trading connectors (only used by cancellation task)
    pub pacifica_trading: Arc<PacificaTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    
    // Channel for cancel requests (decouples hot path from I/O)
    pub cancel_tx: mpsc::Sender<CancelRequest>,
}

impl OrderMonitorService {
    /// Create a new order monitor service with cancellation channel
    pub fn new(
        bot_state: Arc<RwLock<BotState>>,
        atomic_status: Arc<AtomicU8>,
        order_snapshot: Arc<SharedOrderSnapshot>,
        pacifica_prices: Arc<AtomicPrice>,
        hyperliquid_prices: Arc<AtomicPrice>,
        config: Config,
        evaluator: OpportunityEvaluator,
        pacifica_trading: Arc<PacificaTrading>,
        hyperliquid_trading: Arc<HyperliquidTrading>,
    ) -> (Self, mpsc::Receiver<CancelRequest>) {
        // Bounded channel to prevent unbounded growth, but large enough to not block
        let (cancel_tx, cancel_rx) = mpsc::channel(64);
        
        let age_threshold = Duration::from_secs(config.order_refresh_interval_secs);
        let profit_threshold = config.profit_cancel_threshold_bps;

        let service = Self {
            bot_state,
            atomic_status,
            order_snapshot,
            pacifica_prices,
            hyperliquid_prices,
            config,
            evaluator,
            age_threshold,
            profit_threshold,
            pacifica_trading,
            hyperliquid_trading,
            cancel_tx,
        };
        
        (service, cancel_rx)
    }

    /// Market-driven monitoring - triggered by incoming price updates.
    pub fn check_on_market_event(&self, _source: MarketSource) {
        // FAST PATH: Lock-free status check
        let status = self.atomic_status.load(Ordering::Acquire);
        if status != AtomicBotStatus::OrderPlaced as u8 {
            return;
        }

        // Get order snapshot (single atomic load)
        let snapshot = match self.order_snapshot.get() {
            Some(s) => s,
            None => return,
        };

        let age = snapshot.placed_at.elapsed();

        // Check 1: Age threshold
        if age > self.age_threshold {
            let _ = self.cancel_tx.try_send(CancelRequest::AgeExpiry {
                symbol: self.config.symbol.clone(),
                reason: format!(
                    "age {}ms > {}s threshold",
                    age.as_millis(),
                    self.config.order_refresh_interval_secs
                ),
            });
            return;
        }

        let (hl_bid, hl_ask) = self.hyperliquid_prices.load_bid_ask();
        if hl_bid == 0.0 || hl_ask == 0.0 {
            return;
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

        if profit_deviation > self.profit_threshold {
            let _ = self.cancel_tx.try_send(CancelRequest::ProfitDeviation {
                symbol: self.config.symbol.clone(),
                current_profit_bps: current_profit,
                deviation_bps: profit_deviation,
            });
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

            let (hl_bid, hl_ask) = self.hyperliquid_prices.load_bid_ask();
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
            self.pacifica_prices.store(bid, ask, 0);
            debug!("[REFRESH] Pacifica: bid=${:.6}, ask=${:.6}", bid, ask);
        }

        if let Ok(Some((bid, ask))) = hl_result {
            self.hyperliquid_prices.store(bid, ask, 0);
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

// ============================================================================
// STARTUP HELPER
// ============================================================================

/// Spawn all monitor tasks
pub fn spawn_monitor_tasks(service: Arc<OrderMonitorService>, cancel_rx: mpsc::Receiver<CancelRequest>) {
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
