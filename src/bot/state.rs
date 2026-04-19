use crate::strategy::OrderSide;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
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
    /// Exchange-assigned order ID (canonical across fill-detection paths).
    /// `None` only if the exchange response omitted it.
    pub order_id: Option<u64>,
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
            status_atomic: Arc::new(AtomicU8::new(0)), // 0 = Idle
            last_cancellation_time: None,
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
        self.last_cancellation_time = Some(Instant::now());
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn sample_order() -> ActiveOrder {
        ActiveOrder {
            order_id: Some(42),
            client_order_id: "cloid-1234".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 150.0,
            size: 0.1,
            initial_profit_bps: 15.0,
            placed_at: Instant::now(),
        }
    }

    #[test]
    fn new_state_is_idle() {
        let s = BotState::new();
        assert!(s.is_idle());
        assert!(s.is_idle_fast());
        assert!(!s.is_terminal());
        assert_eq!(s.position, 0.0);
        assert_eq!(s.get_status_atomic(), 0);
        assert!(s.active_order.is_none());
        assert!(s.last_cancellation_time.is_none());
    }

    #[test]
    fn set_active_order_transitions_to_order_placed() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        assert_eq!(s.status, BotStatus::OrderPlaced);
        assert_eq!(s.get_status_atomic(), 1);
        assert!(s.has_active_order_fast());
        assert!(!s.is_idle());
        assert!(s.active_order.is_some());
    }

    #[test]
    fn clear_active_order_back_to_idle_and_records_cancel_time() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.clear_active_order();
        assert!(s.is_idle());
        assert_eq!(s.get_status_atomic(), 0);
        assert!(s.active_order.is_none());
        assert!(s.last_cancellation_time.is_some());
    }

    #[test]
    fn mark_filled_updates_position_buy() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.mark_filled(0.3, OrderSide::Buy);
        assert_eq!(s.status, BotStatus::Filled);
        assert_eq!(s.get_status_atomic(), 2);
        assert!((s.position - 0.3).abs() < 1e-9);
    }

    #[test]
    fn mark_filled_updates_position_sell() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.mark_filled(0.5, OrderSide::Sell);
        assert_eq!(s.status, BotStatus::Filled);
        assert!((s.position + 0.5).abs() < 1e-9);
    }

    #[test]
    fn mark_hedging_then_complete_clears_order() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.mark_hedging();
        assert_eq!(s.get_status_atomic(), 3);
        s.mark_complete();
        assert_eq!(s.get_status_atomic(), 4);
        assert!(s.is_terminal());
        assert!(s.active_order.is_none());
    }

    #[test]
    fn set_error_flips_to_terminal_error() {
        let mut s = BotState::new();
        s.set_error("boom".to_string());
        assert_eq!(s.get_status_atomic(), 5);
        assert!(s.is_terminal());
        match &s.status {
            BotStatus::Error(msg) => assert_eq!(msg, "boom"),
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn atomic_status_mirrors_status_field() {
        let mut s = BotState::new();
        let expected = [
            (BotStatus::OrderPlaced, 1u8),
            (BotStatus::Filled, 2),
            (BotStatus::Hedging, 3),
            (BotStatus::Complete, 4),
        ];
        s.set_active_order(sample_order());
        assert_eq!(s.status_atomic.load(Ordering::Acquire), expected[0].1);
        s.mark_filled(0.1, OrderSide::Buy);
        assert_eq!(s.status_atomic.load(Ordering::Acquire), expected[1].1);
        s.mark_hedging();
        assert_eq!(s.status_atomic.load(Ordering::Acquire), expected[2].1);
        s.mark_complete();
        assert_eq!(s.status_atomic.load(Ordering::Acquire), expected[3].1);
    }

    #[test]
    fn grace_period_elapsed_without_prior_cancel_is_true() {
        let s = BotState::new();
        assert!(s.grace_period_elapsed(3));
    }

    #[test]
    fn grace_period_not_elapsed_right_after_cancel() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.clear_active_order();
        assert!(!s.grace_period_elapsed(60));
    }
}

