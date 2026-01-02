use crate::strategy::OrderSide;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Bot status enumeration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotStatus {
    /// Bot is idle, waiting for an opportunity
    Idle,
    /// Order has been placed on Pacifica
    OrderPlaced,
    /// Order has been filled on Pacifica
    Filled,
    /// Hedge is being executed on Hyperliquid
    Hedging,
    /// Full cycle complete (order filled + hedged)
    Complete,
    /// Error occurred
    Error(String),
}

/// Active order information
#[derive(Debug, Clone)]
pub struct ActiveOrder {
    /// Client order ID
    pub client_order_id: String,
    /// Trading symbol (e.g., "SOL")
    pub symbol: String,
    /// Order side (Buy or Sell)
    pub side: OrderSide,
    /// Limit price
    pub price: f64,
    /// Order size
    pub size: f64,
    /// Initial calculated profit in basis points
    pub initial_profit_bps: f64,
    /// When the order was placed
    pub placed_at: Instant,
}

/// Bot state (thread-safe via Arc<RwLock<BotState>>)
#[derive(Debug)]
pub struct BotState {
    /// Currently active order (if any)
    pub active_order: Option<ActiveOrder>,
    /// Current position size (+ for long, - for short, 0 for flat)
    pub position: f64,
    /// Current bot status
    pub status: BotStatus,
    /// Atomic status for fast lock-free checks (0=Idle, 1=OrderPlaced, 2=Filled, 3=Hedging, 4=Complete, 5=Error)
    pub status_atomic: Arc<AtomicU8>,
    /// Last cancellation time in epoch ms (0 = none)
    pub last_cancel_ms: Arc<AtomicU64>,
}

impl BotState {
    /// Create a new bot state in Idle status
    pub fn new() -> Self {
        Self {
            active_order: None,
            position: 0.0,
            status: BotStatus::Idle,
            status_atomic: Arc::new(AtomicU8::new(0)), // 0 = Idle
            last_cancel_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Set active order and update status
    pub fn set_active_order(&mut self, order: ActiveOrder) {
        self.active_order = Some(order);
        self.status = BotStatus::OrderPlaced;
        self.status_atomic.store(1, Ordering::Release); // 1 = OrderPlaced
    }

    /// Clear active order and return to Idle
    pub fn clear_active_order(&mut self) {
        self.active_order = None;
        self.status = BotStatus::Idle;
        self.status_atomic.store(0, Ordering::Release); // 0 = Idle
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_cancel_ms.store(now_ms, Ordering::Release);
    }

    /// Mark order as filled
    pub fn mark_filled(&mut self, filled_size: f64, side: OrderSide) {
        self.status = BotStatus::Filled;
        self.status_atomic.store(2, Ordering::Release); // 2 = Filled

        // Update position
        match side {
            OrderSide::Buy => self.position += filled_size,
            OrderSide::Sell => self.position -= filled_size,
        }
    }

    /// Mark as hedging
    pub fn mark_hedging(&mut self) {
        self.status = BotStatus::Hedging;
        self.status_atomic.store(3, Ordering::Release); // 3 = Hedging
    }

    /// Mark as complete
    pub fn mark_complete(&mut self) {
        self.status = BotStatus::Complete;
        self.status_atomic.store(4, Ordering::Release); // 4 = Complete
        self.active_order = None;
    }

    /// Set error status
    pub fn set_error(&mut self, error: String) {
        self.status = BotStatus::Error(error);
        self.status_atomic.store(5, Ordering::Release); // 5 = Error
    }

    /// Check if bot is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, BotStatus::Complete | BotStatus::Error(_))
    }

    /// Check if bot is idle
    pub fn is_idle(&self) -> bool {
        self.status == BotStatus::Idle
    }

    /// Fast lock-free check if bot is idle (using atomic status)
    pub fn is_idle_fast(&self) -> bool {
        self.status_atomic.load(Ordering::Acquire) == 0
    }

    /// Fast lock-free check if bot has an active order (OrderPlaced status)
    pub fn has_active_order_fast(&self) -> bool {
        self.status_atomic.load(Ordering::Acquire) == 1
    }

    /// Fast lock-free get current status as u8
    pub fn get_status_atomic(&self) -> u8 {
        self.status_atomic.load(Ordering::Acquire)
    }

    /// Check if the grace period has passed since last cancellation
    /// Returns true if no cancellation or if grace_period_secs has elapsed
    pub fn grace_period_elapsed(&self, now_ms: u64, grace_period_secs: u64) -> bool {
        let last_ms = self.last_cancel_ms.load(Ordering::Acquire);
        if last_ms == 0 {
            return true;
        }
        now_ms.saturating_sub(last_ms) >= grace_period_secs * 1000
    }
}

impl Default for BotState {
    fn default() -> Self {
        Self::new()
    }
}
