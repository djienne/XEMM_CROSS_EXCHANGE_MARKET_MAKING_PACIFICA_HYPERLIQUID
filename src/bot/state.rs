use crate::strategy::OrderSide;
use std::time::Instant;

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
    /// Last time an order was cancelled (for grace period enforcement)
    pub last_cancellation_time: Option<Instant>,
}

impl BotState {
    /// Create a new bot state in Idle status
    pub fn new() -> Self {
        Self {
            active_order: None,
            position: 0.0,
            status: BotStatus::Idle,
            last_cancellation_time: None,
        }
    }

    /// Set active order and update status
    pub fn set_active_order(&mut self, order: ActiveOrder) {
        self.active_order = Some(order);
        self.status = BotStatus::OrderPlaced;
    }

    /// Clear active order and return to Idle
    pub fn clear_active_order(&mut self) {
        self.active_order = None;
        self.status = BotStatus::Idle;
        self.last_cancellation_time = Some(Instant::now());
    }

    /// Mark order as filled
    pub fn mark_filled(&mut self, filled_size: f64, side: OrderSide) {
        self.status = BotStatus::Filled;

        // Update position
        match side {
            OrderSide::Buy => self.position += filled_size,
            OrderSide::Sell => self.position -= filled_size,
        }
    }

    /// Mark as hedging
    pub fn mark_hedging(&mut self) {
        self.status = BotStatus::Hedging;
    }

    /// Mark as complete
    pub fn mark_complete(&mut self) {
        self.status = BotStatus::Complete;
        self.active_order = None;
    }

    /// Set error status
    pub fn set_error(&mut self, error: String) {
        self.status = BotStatus::Error(error);
    }

    /// Check if bot is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self.status, BotStatus::Complete | BotStatus::Error(_))
    }

    /// Check if bot is idle
    pub fn is_idle(&self) -> bool {
        self.status == BotStatus::Idle
    }

    /// Check if the grace period has passed since last cancellation
    /// Returns true if no cancellation or if grace_period_secs has elapsed
    pub fn grace_period_elapsed(&self, grace_period_secs: u64) -> bool {
        match self.last_cancellation_time {
            None => true, // No previous cancellation
            Some(last_cancel) => last_cancel.elapsed().as_secs() >= grace_period_secs,
        }
    }
}

impl Default for BotState {
    fn default() -> Self {
        Self::new()
    }
}
