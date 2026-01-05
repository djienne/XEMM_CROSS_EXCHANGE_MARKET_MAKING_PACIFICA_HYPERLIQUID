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

    /// Clear active order without updating the cancellation grace timer
    pub fn clear_active_order_no_grace(&mut self) {
        self.active_order = None;
        self.status = BotStatus::Idle;
        self.status_atomic.store(0, Ordering::Release); // 0 = Idle
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_bot_state_new() {
        let state = BotState::new();

        assert!(state.active_order.is_none());
        assert_eq!(state.position, 0.0);
        assert_eq!(state.status, BotStatus::Idle);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 0);
        assert_eq!(state.last_cancel_ms.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_bot_state_default() {
        let state = BotState::default();
        assert_eq!(state.status, BotStatus::Idle);
    }

    #[test]
    fn test_set_active_order() {
        let mut state = BotState::new();

        let order = ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        };

        state.set_active_order(order);

        assert!(state.active_order.is_some());
        assert_eq!(state.status, BotStatus::OrderPlaced);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 1);

        let active = state.active_order.as_ref().unwrap();
        assert_eq!(active.client_order_id, "test-123");
        assert_eq!(active.symbol, "SOL");
        assert_eq!(active.side, OrderSide::Buy);
        assert_eq!(active.price, 100.0);
    }

    #[test]
    fn test_clear_active_order() {
        let mut state = BotState::new();

        // Set an order first
        state.set_active_order(ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });

        // Clear it
        state.clear_active_order();

        assert!(state.active_order.is_none());
        assert_eq!(state.status, BotStatus::Idle);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 0);

        // Last cancel should be set
        assert!(state.last_cancel_ms.load(Ordering::Acquire) > 0);
    }

    #[test]
    fn test_clear_active_order_no_grace() {
        let mut state = BotState::new();

        state.set_active_order(ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });

        // Clear without grace period update
        state.clear_active_order_no_grace();

        assert!(state.active_order.is_none());
        assert_eq!(state.status, BotStatus::Idle);
        // last_cancel_ms should NOT be updated
        assert_eq!(state.last_cancel_ms.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_mark_filled_buy() {
        let mut state = BotState::new();
        assert_eq!(state.position, 0.0);

        state.mark_filled(1.5, OrderSide::Buy);

        assert_eq!(state.status, BotStatus::Filled);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 2);
        assert_eq!(state.position, 1.5);  // Long position
    }

    #[test]
    fn test_mark_filled_sell() {
        let mut state = BotState::new();
        assert_eq!(state.position, 0.0);

        state.mark_filled(2.0, OrderSide::Sell);

        assert_eq!(state.status, BotStatus::Filled);
        assert_eq!(state.position, -2.0);  // Short position
    }

    #[test]
    fn test_mark_hedging() {
        let mut state = BotState::new();

        state.mark_hedging();

        assert_eq!(state.status, BotStatus::Hedging);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 3);
    }

    #[test]
    fn test_mark_complete() {
        let mut state = BotState::new();

        // Set an order first
        state.set_active_order(ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });

        state.mark_complete();

        assert_eq!(state.status, BotStatus::Complete);
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 4);
        assert!(state.active_order.is_none());  // Order cleared
    }

    #[test]
    fn test_set_error() {
        let mut state = BotState::new();

        state.set_error("Test error".to_string());

        assert!(matches!(state.status, BotStatus::Error(ref msg) if msg == "Test error"));
        assert_eq!(state.status_atomic.load(Ordering::Acquire), 5);
    }

    #[test]
    fn test_is_terminal() {
        let mut state = BotState::new();

        // Idle is not terminal
        assert!(!state.is_terminal());

        // Complete is terminal
        state.mark_complete();
        assert!(state.is_terminal());

        // Error is terminal
        let mut state2 = BotState::new();
        state2.set_error("Error".to_string());
        assert!(state2.is_terminal());
    }

    #[test]
    fn test_is_idle() {
        let mut state = BotState::new();
        assert!(state.is_idle());

        state.mark_hedging();
        assert!(!state.is_idle());
    }

    #[test]
    fn test_is_idle_fast() {
        let mut state = BotState::new();
        assert!(state.is_idle_fast());

        state.set_active_order(ActiveOrder {
            client_order_id: "test".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });
        assert!(!state.is_idle_fast());
    }

    #[test]
    fn test_has_active_order_fast() {
        let mut state = BotState::new();
        assert!(!state.has_active_order_fast());

        state.set_active_order(ActiveOrder {
            client_order_id: "test".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });
        assert!(state.has_active_order_fast());

        state.mark_filled(1.0, OrderSide::Buy);
        assert!(!state.has_active_order_fast());  // status is now Filled (2)
    }

    #[test]
    fn test_get_status_atomic() {
        let mut state = BotState::new();

        assert_eq!(state.get_status_atomic(), 0);  // Idle

        state.set_active_order(ActiveOrder {
            client_order_id: "test".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });
        assert_eq!(state.get_status_atomic(), 1);  // OrderPlaced

        state.mark_filled(1.0, OrderSide::Buy);
        assert_eq!(state.get_status_atomic(), 2);  // Filled

        state.mark_hedging();
        assert_eq!(state.get_status_atomic(), 3);  // Hedging

        state.mark_complete();
        assert_eq!(state.get_status_atomic(), 4);  // Complete
    }

    #[test]
    fn test_grace_period_elapsed_no_cancel() {
        let state = BotState::new();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // With no cancel (last_cancel_ms = 0), should always return true
        assert!(state.grace_period_elapsed(now_ms, 3));
    }

    #[test]
    fn test_grace_period_elapsed_after_cancel() {
        let mut state = BotState::new();

        // Set an order and clear it to set last_cancel_ms
        state.set_active_order(ActiveOrder {
            client_order_id: "test".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.0,
            size: 1.0,
            initial_profit_bps: 10.0,
            placed_at: Instant::now(),
        });
        state.clear_active_order();

        let cancel_time = state.last_cancel_ms.load(Ordering::Acquire);

        // Right after cancel, grace period not elapsed
        assert!(!state.grace_period_elapsed(cancel_time + 1000, 3));  // 1 sec < 3 sec

        // After grace period
        assert!(state.grace_period_elapsed(cancel_time + 4000, 3));  // 4 sec > 3 sec
    }

    #[test]
    fn test_position_accumulation() {
        let mut state = BotState::new();

        // Multiple buys should accumulate
        state.mark_filled(1.0, OrderSide::Buy);
        state.status = BotStatus::Idle;  // Reset for next fill

        state.mark_filled(0.5, OrderSide::Buy);
        assert_eq!(state.position, 1.5);

        // Sell reduces position
        state.status = BotStatus::Idle;
        state.mark_filled(2.0, OrderSide::Sell);
        assert_eq!(state.position, -0.5);  // 1.5 - 2.0 = -0.5
    }

    #[test]
    fn test_bot_status_equality() {
        assert_eq!(BotStatus::Idle, BotStatus::Idle);
        assert_eq!(BotStatus::OrderPlaced, BotStatus::OrderPlaced);
        assert_eq!(BotStatus::Filled, BotStatus::Filled);
        assert_eq!(BotStatus::Hedging, BotStatus::Hedging);
        assert_eq!(BotStatus::Complete, BotStatus::Complete);
        assert_eq!(BotStatus::Error("a".to_string()), BotStatus::Error("a".to_string()));

        assert_ne!(BotStatus::Idle, BotStatus::OrderPlaced);
        assert_ne!(BotStatus::Error("a".to_string()), BotStatus::Error("b".to_string()));
    }

    #[test]
    fn test_active_order_clone() {
        let order = ActiveOrder {
            client_order_id: "test-123".to_string(),
            symbol: "SOL".to_string(),
            side: OrderSide::Buy,
            price: 100.5,
            size: 1.5,
            initial_profit_bps: 10.5,
            placed_at: Instant::now(),
        };

        let cloned = order.clone();

        assert_eq!(cloned.client_order_id, order.client_order_id);
        assert_eq!(cloned.symbol, order.symbol);
        assert_eq!(cloned.side, order.side);
        assert_eq!(cloned.price, order.price);
        assert_eq!(cloned.size, order.size);
        assert_eq!(cloned.initial_profit_bps, order.initial_profit_bps);
    }
}
