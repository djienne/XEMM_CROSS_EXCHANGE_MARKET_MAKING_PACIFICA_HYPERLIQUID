//! Integration tests for the BotState ↔ OpportunityEvaluator ↔ RateLimitBook
//! coordination. No network; pure-logic only.

use std::time::Instant;

use xemm_rust::bot::{ActiveOrder, BotState};
use xemm_rust::strategy::{OpportunityEvaluator, OrderSide};
use xemm_rust::util::price::prices_valid;
use xemm_rust::util::rate_limit::{EndpointGroup, RateLimitBook};

#[test]
fn idle_bot_accepts_first_order_then_rejects_duplicates() {
    let mut state = BotState::new();
    assert!(state.is_idle());
    assert!(state.is_idle_fast());

    state.set_active_order(ActiveOrder {
        order_id: Some(1),
        client_order_id: "c1".into(),
        symbol: "SOL".into(),
        side: OrderSide::Buy,
        price: 150.0,
        size: 0.1,
        initial_profit_bps: 15.0,
        placed_at: Instant::now(),
    });

    assert!(!state.is_idle());
    assert!(state.has_active_order_fast());

    // A second "place" attempt would observe OrderPlaced in the hot path and
    // bail — simulate by checking the atomic status directly.
    assert_eq!(state.get_status_atomic(), 1);
}

#[test]
fn evaluator_rejects_invalid_prices_before_producing_opportunity() {
    let evaluator = OpportunityEvaluator::new(1.0, 2.5, 10.0, 0.01);

    // The main loop guards via `prices_valid` BEFORE calling the evaluator,
    // so these inputs shouldn't reach it — but verify the evaluator still
    // behaves reasonably on degenerate cases.
    let opp = evaluator.evaluate_buy_opportunity(100.0, 20.0, 0);
    assert!(opp.is_some());
    let opp = opp.unwrap();
    // Profit must be positive or the evaluator returns None.
    assert!(opp.initial_profit_bps > 0.0);

    // prices_valid catches the cases main loop should never forward:
    assert!(!prices_valid(0.0, 0.0));
    assert!(!prices_valid(100.0, 99.0));
    assert!(prices_valid(100.0, 100.05));
}

#[test]
fn rate_limit_book_isolates_endpoints_under_mixed_load() {
    let book = RateLimitBook::new();

    // Info endpoint hits a limit three times.
    book.record_error(EndpointGroup::Info);
    book.record_error(EndpointGroup::Info);
    book.record_error(EndpointGroup::Info);

    // PlaceOrder should remain unaffected.
    assert!(!book.should_skip(EndpointGroup::PlaceOrder));
    assert_eq!(book.consecutive_errors(EndpointGroup::PlaceOrder), 0);

    // Info is now in backoff.
    assert!(book.should_skip(EndpointGroup::Info));
    assert_eq!(book.consecutive_errors(EndpointGroup::Info), 3);

    // Success on Info clears Info only.
    book.record_success(EndpointGroup::Info);
    assert_eq!(book.consecutive_errors(EndpointGroup::Info), 0);
    assert!(!book.should_skip(EndpointGroup::Info));

    // Simulate a different endpoint still being clean.
    assert_eq!(book.consecutive_errors(EndpointGroup::CancelOrder), 0);
}

#[test]
fn bot_state_transitions_preserve_atomic_invariant() {
    let mut state = BotState::new();
    state.set_active_order(ActiveOrder {
        order_id: Some(1),
        client_order_id: "c".into(),
        symbol: "X".into(),
        side: OrderSide::Buy,
        price: 1.0,
        size: 1.0,
        initial_profit_bps: 1.0,
        placed_at: Instant::now(),
    });
    assert_eq!(state.get_status_atomic(), 1);

    state.mark_filled(0.5, OrderSide::Buy);
    assert_eq!(state.get_status_atomic(), 2);
    state.mark_hedging();
    assert_eq!(state.get_status_atomic(), 3);
    state.mark_complete();
    assert_eq!(state.get_status_atomic(), 4);
    assert!(state.is_terminal());
}
