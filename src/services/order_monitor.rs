use parking_lot::RwLock;
use std::sync::atomic::{compiler_fence, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::info;

use crate::bot::{BotState, BotStatus, RunState};
use crate::config::Config;
use crate::services::cancel_manager::{CancelIntent, CancelReason};
use crate::strategy::{OpportunityEvaluator, OrderSide};
use crate::util::price::{now_ns, prices_valid, SharedQuote};

// ============================================================================
// ATOMIC STATUS FOR LOCK-FREE HOT PATH CHECKS
// ============================================================================

/// Atomic bot status for lock-free checks in hot path
pub type AtomicBotStatus = RunState;

impl From<&BotStatus> for AtomicBotStatus {
    fn from(status: &BotStatus) -> Self {
        status.run_state()
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
    pub placed_at_ns: u64,
    pub cancel_requested: bool,
}

/// Shared order snapshot updated with a seqlock so readers avoid a mutex.
pub struct AtomicOrderSnapshot {
    seq: AtomicU64,
    present: AtomicU8,
    side: AtomicU8,
    price_bits: AtomicU64,
    size_bits: AtomicU64,
    initial_profit_bits: AtomicU64,
    placed_at_ns: AtomicU64,
    cancel_requested: AtomicU8,
}

pub type SharedOrderSnapshot = AtomicOrderSnapshot;

impl AtomicOrderSnapshot {
    pub fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            present: AtomicU8::new(0),
            side: AtomicU8::new(0),
            price_bits: AtomicU64::new(0),
            size_bits: AtomicU64::new(0),
            initial_profit_bits: AtomicU64::new(0),
            placed_at_ns: AtomicU64::new(0),
            cancel_requested: AtomicU8::new(0),
        }
    }

    #[inline]
    pub fn get(&self) -> Option<OrderSnapshot> {
        for _ in 0..8 {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }
            compiler_fence(Ordering::Acquire);
            let present = self.present.load(Ordering::Relaxed);
            let side = self.side.load(Ordering::Relaxed);
            let price = f64::from_bits(self.price_bits.load(Ordering::Relaxed));
            let size = f64::from_bits(self.size_bits.load(Ordering::Relaxed));
            let initial_profit_bps =
                f64::from_bits(self.initial_profit_bits.load(Ordering::Relaxed));
            let placed_at_ns = self.placed_at_ns.load(Ordering::Relaxed);
            let cancel_requested = self.cancel_requested.load(Ordering::Relaxed) != 0;
            compiler_fence(Ordering::Acquire);
            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 && seq2 % 2 == 0 {
                if present == 0 {
                    return None;
                }
                return Some(OrderSnapshot {
                    side: if side == 0 {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    price,
                    size,
                    initial_profit_bps,
                    placed_at_ns,
                    cancel_requested,
                });
            }
        }
        None
    }

    #[inline]
    pub fn set(&self, snapshot: Option<OrderSnapshot>) {
        let seq = self.seq.fetch_add(1, Ordering::AcqRel);
        match snapshot {
            Some(snapshot) => {
                self.side.store(
                    match snapshot.side {
                        OrderSide::Buy => 0,
                        OrderSide::Sell => 1,
                    },
                    Ordering::Release,
                );
                self.price_bits
                    .store(snapshot.price.to_bits(), Ordering::Release);
                self.size_bits
                    .store(snapshot.size.to_bits(), Ordering::Release);
                self.initial_profit_bits
                    .store(snapshot.initial_profit_bps.to_bits(), Ordering::Release);
                self.placed_at_ns
                    .store(snapshot.placed_at_ns, Ordering::Release);
                self.cancel_requested
                    .store(u8::from(snapshot.cancel_requested), Ordering::Release);
                self.present.store(1, Ordering::Release);
            }
            None => {
                self.present.store(0, Ordering::Release);
                self.cancel_requested.store(0, Ordering::Release);
            }
        }
        self.seq.store(seq + 2, Ordering::Release);
    }

    #[inline]
    pub fn request_cancel(&self) -> bool {
        self.present.load(Ordering::Acquire) != 0
            && self
                .cancel_requested
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }

    #[inline]
    pub fn reset_cancel_request(&self) {
        self.cancel_requested.store(0, Ordering::Release);
    }
}

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

    // Price feeds (lock-free reads via parking_lot)
    pub pacifica_prices: Arc<SharedQuote>,
    pub hyperliquid_prices: Arc<SharedQuote>,

    // Configuration
    pub config: Config,
    pub evaluator: OpportunityEvaluator,

    pub cancel_tx: mpsc::Sender<CancelIntent>,
}

impl OrderMonitorService {
    /// Create a new order monitor service with cancellation channel
    pub fn new(
        bot_state: Arc<RwLock<BotState>>,
        atomic_status: Arc<AtomicU8>,
        order_snapshot: Arc<SharedOrderSnapshot>,
        pacifica_prices: Arc<SharedQuote>,
        hyperliquid_prices: Arc<SharedQuote>,
        config: Config,
        evaluator: OpportunityEvaluator,
        cancel_tx: mpsc::Sender<CancelIntent>,
    ) -> Self {
        Self {
            bot_state,
            atomic_status,
            order_snapshot,
            pacifica_prices,
            hyperliquid_prices,
            config,
            evaluator,
            cancel_tx,
        }
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

            // Get prices (parking_lot mutex is very fast for uncontended case)
            let (hl_bid, hl_ask) = self.hyperliquid_prices.load();
            if !prices_valid(hl_bid, hl_ask) {
                continue;
            }

            let age = Duration::from_nanos(now_ns().saturating_sub(snapshot.placed_at_ns));

            // Check 1: Age threshold
            if age > age_threshold {
                // Send allocation-free cancel request (non-blocking). The human
                // formatting happens in the cold-path handler.
                if self.order_snapshot.request_cancel() {
                    let _ = self.cancel_tx.try_send(CancelIntent::new(
                        self.config.symbol.clone(),
                        CancelReason::AgeExpiry {
                            age_ms: age.as_millis() as u64,
                        },
                    ));
                }
                continue;
            }

            // Check 2: Profit deviation (using raw method - no allocation)
            let current_profit = self.evaluator.recalculate_profit_raw(
                snapshot.side,
                snapshot.price,
                hl_bid,
                hl_ask,
            );

            // One-sided deterioration: improvements are not cancellation reasons.
            let profit_drop = snapshot.initial_profit_bps - current_profit;

            if should_cancel_for_profit_drop(
                snapshot.initial_profit_bps,
                current_profit,
                profit_threshold,
            ) && self.order_snapshot.request_cancel()
            {
                // Send allocation-free cancel request (non-blocking).
                let _ = self.cancel_tx.try_send(CancelIntent::new(
                    self.config.symbol.clone(),
                    CancelReason::ProfitDeviation {
                        current_profit_bps: current_profit,
                        deviation_bps: profit_drop,
                    },
                ));
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
            if !prices_valid(hl_bid, hl_ask) {
                continue;
            }

            let current_profit = self.evaluator.recalculate_profit_raw(
                snapshot.side,
                snapshot.price,
                hl_bid,
                hl_ask,
            );
            let profit_change = current_profit - snapshot.initial_profit_bps;
            let age_ms = now_ns().saturating_sub(snapshot.placed_at_ns) / 1_000_000;

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
        placed_at_ns: now_ns(),
        cancel_requested: false,
    }));
}

#[inline]
pub fn should_cancel_for_profit_drop(
    initial_profit_bps: f64,
    current_profit_bps: f64,
    threshold_bps: f64,
) -> bool {
    initial_profit_bps - current_profit_bps > threshold_bps
}

// ============================================================================
// STARTUP HELPER
// ============================================================================

/// Spawn all monitor tasks
pub fn spawn_monitor_tasks(service: Arc<OrderMonitorService>) {
    // Hot path monitor (1kHz)
    let service_clone = Arc::clone(&service);
    tokio::spawn(async move {
        service_clone.run_monitor_loop().await;
    });

    // Profit logger (0.5 Hz)
    let service_clone = Arc::clone(&service);
    tokio::spawn(async move {
        service_clone.run_profit_logger().await;
    });
}
