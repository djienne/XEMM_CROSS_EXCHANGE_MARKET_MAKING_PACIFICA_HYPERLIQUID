use crate::strategy::OrderSide;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Canonical lock-free run state for hot-path checks.
///
/// The `Arc<AtomicU8>` carrying this value is shared by the main loop,
/// monitors, fill detectors, and hedge executor. `BotStatus` remains the
/// richer cold-path diagnostic snapshot, but all fast placement/cancel/hedge
/// decisions read this enum.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Idle = 0,
    Placing = 1,
    OrderPlaced = 2,
    Cancelling = 3,
    Filled = 4,
    Hedging = 5,
    Reconciling = 6,
    PlacementUnknown = 7,
    CancelPending = 8,
    HedgeUnknown = 9,
    ShuttingDown = 10,
    Complete = 11,
    Error = 12,
}

impl RunState {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Idle,
            1 => Self::Placing,
            2 => Self::OrderPlaced,
            3 => Self::Cancelling,
            4 => Self::Filled,
            5 => Self::Hedging,
            6 => Self::Reconciling,
            7 => Self::PlacementUnknown,
            8 => Self::CancelPending,
            9 => Self::HedgeUnknown,
            10 => Self::ShuttingDown,
            11 => Self::Complete,
            12 => Self::Error,
            _ => Self::Error,
        }
    }

    #[inline]
    pub fn new_atomic(initial: Self) -> Arc<AtomicU8> {
        Arc::new(AtomicU8::new(initial.as_u8()))
    }

    #[inline]
    pub fn load(atomic: &AtomicU8) -> Self {
        Self::from_u8(atomic.load(Ordering::Acquire))
    }

    #[inline]
    pub fn store(atomic: &AtomicU8, state: Self) {
        atomic.store(state.as_u8(), Ordering::Release);
    }

    #[inline]
    pub fn try_transition(atomic: &AtomicU8, from: Self, to: Self) -> bool {
        atomic
            .compare_exchange(
                from.as_u8(),
                to.as_u8(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    #[inline]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Error)
    }
}

/// Bot status enumeration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BotStatus {
    /// Bot is idle, waiting for an opportunity
    Idle,
    /// Main loop has reserved the right to place an order
    Placing,
    /// Order has been placed on Pacifica
    OrderPlaced,
    /// Existing maker order is being cancelled
    Cancelling,
    /// Order has been filled on Pacifica
    Filled,
    /// Hedge is being executed on Hyperliquid
    Hedging,
    /// Exposure/open-order state is being reconciled
    Reconciling,
    /// Placement may have reached the exchange but the response was lost.
    PlacementUnknown,
    /// Cancellation is pending verification.
    CancelPending,
    /// Hedge submit/fill state is uncertain and requires reconciliation.
    HedgeUnknown,
    /// Shutdown has stopped new placement and is draining risk.
    ShuttingDown,
    /// Full cycle complete (order filled + hedged)
    Complete,
    /// Error occurred
    Error(String),
}

impl BotStatus {
    #[inline]
    pub fn run_state(&self) -> RunState {
        match self {
            BotStatus::Idle => RunState::Idle,
            BotStatus::Placing => RunState::Placing,
            BotStatus::OrderPlaced => RunState::OrderPlaced,
            BotStatus::Cancelling => RunState::Cancelling,
            BotStatus::Filled => RunState::Filled,
            BotStatus::Hedging => RunState::Hedging,
            BotStatus::Reconciling => RunState::Reconciling,
            BotStatus::PlacementUnknown => RunState::PlacementUnknown,
            BotStatus::CancelPending => RunState::CancelPending,
            BotStatus::HedgeUnknown => RunState::HedgeUnknown,
            BotStatus::ShuttingDown => RunState::ShuttingDown,
            BotStatus::Complete => RunState::Complete,
            BotStatus::Error(_) => RunState::Error,
        }
    }
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
    /// Canonical atomic run state for fast lock-free checks.
    pub run_state: Arc<AtomicU8>,
    /// Last time an order was cancelled (for grace period enforcement)
    pub last_cancellation_time: Option<Instant>,
    /// Number of successful fill+hedge cycles completed by this process.
    pub cycle_count: u64,
}

impl BotState {
    /// Create a new bot state in Idle status
    pub fn new() -> Self {
        Self::with_run_state(RunState::new_atomic(RunState::Idle))
    }

    /// Create a bot state backed by an externally shared canonical run-state.
    pub fn with_run_state(run_state: Arc<AtomicU8>) -> Self {
        RunState::store(&run_state, RunState::Idle);
        Self {
            active_order: None,
            position: 0.0,
            status: BotStatus::Idle,
            run_state,
            last_cancellation_time: None,
            cycle_count: 0,
        }
    }

    #[inline]
    pub fn store_status(&self) {
        RunState::store(&self.run_state, self.status.run_state());
    }

    /// Mark the state snapshot as a placement reservation.
    pub fn mark_placing(&mut self) {
        self.status = BotStatus::Placing;
        self.store_status();
    }

    /// Register an order before the REST placement call returns.
    ///
    /// The client order ID is already known at this point, which lets racing
    /// WebSocket fill events match the order even during the placement window.
    pub fn set_prospective_order(&mut self, order: ActiveOrder) {
        self.active_order = Some(order);
        self.status = BotStatus::Placing;
        self.store_status();
    }

    /// Set active order and update status
    pub fn set_active_order(&mut self, order: ActiveOrder) {
        self.active_order = Some(order);
        self.status = BotStatus::OrderPlaced;
        self.store_status();
    }

    /// Clear active order and return to Idle
    pub fn clear_active_order(&mut self) {
        self.active_order = None;
        self.status = BotStatus::Idle;
        self.store_status();
        self.last_cancellation_time = Some(Instant::now());
    }

    /// Clear a locally reserved order that definitely never reached the exchange.
    pub fn clear_prospective_order(&mut self) {
        self.active_order = None;
        self.status = BotStatus::Idle;
        self.store_status();
    }

    /// Mark active order cancellation in progress.
    pub fn mark_cancelling(&mut self) {
        self.status = BotStatus::Cancelling;
        self.store_status();
    }

    /// Mark order as filled
    pub fn mark_filled(&mut self, filled_size: f64, side: OrderSide) {
        self.status = BotStatus::Filled;
        self.store_status();

        // Update position
        match side {
            OrderSide::Buy => self.position += filled_size,
            OrderSide::Sell => self.position -= filled_size,
        }
    }

    /// Mark as hedging
    pub fn mark_hedging(&mut self) {
        self.status = BotStatus::Hedging;
        self.store_status();
    }

    /// Mark as reconciling.
    pub fn mark_reconciling(&mut self) {
        self.status = BotStatus::Reconciling;
        self.store_status();
    }

    pub fn mark_placement_unknown(&mut self) {
        self.status = BotStatus::PlacementUnknown;
        self.store_status();
    }

    pub fn mark_cancel_pending(&mut self) {
        self.status = BotStatus::CancelPending;
        self.store_status();
    }

    pub fn mark_hedge_unknown(&mut self) {
        self.status = BotStatus::HedgeUnknown;
        self.store_status();
    }

    pub fn mark_shutting_down(&mut self) {
        self.status = BotStatus::ShuttingDown;
        self.store_status();
    }

    /// Mark as complete
    pub fn mark_complete(&mut self) {
        self.status = BotStatus::Complete;
        self.store_status();
        self.active_order = None;
    }

    /// Record a successful cycle and return to idle without terminating the
    /// process. This is the normal post-audit path for the persistent bot.
    pub fn mark_cycle_complete_and_idle(&mut self) {
        self.active_order = None;
        self.position = 0.0;
        self.cycle_count = self.cycle_count.saturating_add(1);
        self.status = BotStatus::Idle;
        self.store_status();
    }

    /// Set error status
    pub fn set_error(&mut self, error: String) {
        self.status = BotStatus::Error(error);
        self.store_status();
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
        RunState::load(&self.run_state) == RunState::Idle
    }

    /// Fast lock-free check if bot has an active order (OrderPlaced status)
    pub fn has_active_order_fast(&self) -> bool {
        RunState::load(&self.run_state) == RunState::OrderPlaced
    }

    /// Fast lock-free get current status as u8
    pub fn get_status_atomic(&self) -> u8 {
        self.run_state.load(Ordering::Acquire)
    }

    /// Fast lock-free get current run state.
    pub fn get_run_state(&self) -> RunState {
        RunState::load(&self.run_state)
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
    fn shared_run_state_is_canonical() {
        let atomic = RunState::new_atomic(RunState::Idle);
        let mut s = BotState::with_run_state(atomic.clone());
        assert!(RunState::try_transition(
            &atomic,
            RunState::Idle,
            RunState::Placing
        ));
        assert_eq!(RunState::load(&atomic), RunState::Placing);
        s.mark_placing();
        assert_eq!(s.status, BotStatus::Placing);
        assert_eq!(s.get_run_state(), RunState::Placing);
    }

    #[test]
    fn set_active_order_transitions_to_order_placed() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        assert_eq!(s.status, BotStatus::OrderPlaced);
        assert_eq!(s.get_run_state(), RunState::OrderPlaced);
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
        assert_eq!(s.get_run_state(), RunState::Idle);
        assert!(s.active_order.is_none());
        assert!(s.last_cancellation_time.is_some());
    }

    #[test]
    fn mark_filled_updates_position_buy() {
        let mut s = BotState::new();
        s.set_active_order(sample_order());
        s.mark_filled(0.3, OrderSide::Buy);
        assert_eq!(s.status, BotStatus::Filled);
        assert_eq!(s.get_run_state(), RunState::Filled);
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
        assert_eq!(s.get_run_state(), RunState::Hedging);
        s.mark_complete();
        assert_eq!(s.get_run_state(), RunState::Complete);
        assert!(s.is_terminal());
        assert!(s.active_order.is_none());
    }

    #[test]
    fn set_error_flips_to_terminal_error() {
        let mut s = BotState::new();
        s.set_error("boom".to_string());
        assert_eq!(s.get_run_state(), RunState::Error);
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
            (BotStatus::OrderPlaced, RunState::OrderPlaced.as_u8()),
            (BotStatus::Filled, RunState::Filled.as_u8()),
            (BotStatus::Hedging, RunState::Hedging.as_u8()),
            (BotStatus::Complete, RunState::Complete.as_u8()),
        ];
        s.set_active_order(sample_order());
        assert_eq!(s.run_state.load(Ordering::Acquire), expected[0].1);
        s.mark_filled(0.1, OrderSide::Buy);
        assert_eq!(s.run_state.load(Ordering::Acquire), expected[1].1);
        s.mark_hedging();
        assert_eq!(s.run_state.load(Ordering::Acquire), expected[2].1);
        s.mark_complete();
        assert_eq!(s.run_state.load(Ordering::Acquire), expected[3].1);
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
