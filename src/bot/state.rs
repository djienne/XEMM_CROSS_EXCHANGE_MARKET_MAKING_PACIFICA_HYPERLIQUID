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
///
/// The authoritative atomic status mirror lives in `XemmBot.atomic_status`
/// (a single `Arc<AtomicU8>` shared across all services). State transition
/// methods here update only the `BotState` fields; callers must sync the
/// external atomic via `sync_atomic_status()` when needed.
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

    /// Check if bot has an active order (OrderPlaced status)
    pub fn has_active_order_fast(&self) -> bool {
        matches!(self.status, BotStatus::OrderPlaced)
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
    use std::time::Instant;

    fn make_order() -> ActiveOrder {
        ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.5,
            initial_profit_bps: 5.0,
            placed_at: Instant::now(),
        }
    }

    #[test]
    fn test_new_state_is_idle() {
        let state = BotState::new();
        assert_eq!(state.status, BotStatus::Idle);
        assert!(state.active_order.is_none());
        assert_eq!(state.position, 0.0);
        assert!(state.last_cancellation_time.is_none());
    }

    #[test]
    fn test_full_lifecycle() {
        let mut state = BotState::new();

        // Set active order → OrderPlaced
        state.set_active_order(make_order());
        assert_eq!(state.status, BotStatus::OrderPlaced);
        assert!(state.active_order.is_some());

        // Mark filled → Filled, position updated
        state.mark_filled(1.5, OrderSide::Buy);
        assert_eq!(state.status, BotStatus::Filled);
        assert_eq!(state.position, 1.5);

        // Mark hedging → Hedging
        state.mark_hedging();
        assert_eq!(state.status, BotStatus::Hedging);

        // Mark complete → Complete, active_order cleared
        state.mark_complete();
        assert_eq!(state.status, BotStatus::Complete);
        assert!(state.active_order.is_none());
    }

    #[test]
    fn test_clear_active_order() {
        let mut state = BotState::new();
        state.set_active_order(make_order());
        assert_eq!(state.status, BotStatus::OrderPlaced);

        state.clear_active_order();
        assert_eq!(state.status, BotStatus::Idle);
        assert!(state.active_order.is_none());
        assert!(state.last_cancellation_time.is_some());
    }

    #[test]
    fn test_is_terminal() {
        let mut state = BotState::new();
        assert!(!state.is_terminal()); // Idle

        state.status = BotStatus::OrderPlaced;
        assert!(!state.is_terminal());

        state.status = BotStatus::Filled;
        assert!(!state.is_terminal());

        state.status = BotStatus::Hedging;
        assert!(!state.is_terminal());

        state.status = BotStatus::Complete;
        assert!(state.is_terminal());

        state.status = BotStatus::Error("test".to_string());
        assert!(state.is_terminal());
    }

    #[test]
    fn test_position_tracking() {
        let mut state = BotState::new();
        state.mark_filled(1.5, OrderSide::Buy);
        assert_eq!(state.position, 1.5);

        state.mark_filled(0.5, OrderSide::Sell);
        assert_eq!(state.position, 1.0);
    }

    #[test]
    fn test_grace_period_no_cancellation() {
        let state = BotState::new();
        // No cancellation ever → grace period is elapsed
        assert!(state.grace_period_elapsed(10));
    }

    #[test]
    fn test_grace_period_within_and_after() {
        let mut state = BotState::new();
        state.set_active_order(make_order());
        state.clear_active_order();

        // Just cancelled — 10 second grace period should NOT be elapsed
        assert!(!state.grace_period_elapsed(10));
        // 0 second grace period should be elapsed immediately
        assert!(state.grace_period_elapsed(0));
    }
}
