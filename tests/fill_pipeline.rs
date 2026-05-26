//! Integration tests for the fill -> dedup -> aggregator -> hedge-decision pipeline.
//!
//! These run without any network: they exercise the coordination between the
//! pure-logic modules that were hardened in FR-6/FR-7/FR-19. The goal is to
//! pin down the combined behaviour so a future refactor can't silently break
//! dedup or partial-fill aggregation.

use std::time::Duration;

use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::mpsc;
use xemm_rust::bot::BotState;
use xemm_rust::services::fill_aggregator::FillAggregator;
use xemm_rust::services::fill_dedup::{FillDedup, FillKey};
use xemm_rust::services::{enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent};
use xemm_rust::strategy::OrderSide;

/// Partial fill followed by full fill must hedge exactly once, and the hedge
/// must reflect the total accumulated size - not just the terminal increment.
#[test]
fn partial_then_full_emits_single_hedge_with_total_size() {
    let dedup = FillDedup::new(64);
    let agg = FillAggregator::new(1_000.0);

    // Order 42: 0.3 filled @ $100 (partial) - no hedge yet.
    assert!(agg.on_fill(42, OrderSide::Buy, 0.3, 100.0, false).is_none());
    assert!(!dedup.contains(&FillKey::from_order_cumulative(42, 0.3)));

    // Then full fill, cumulative 1.0 @ $100.5 - must emit.
    let decision = agg
        .on_fill(42, OrderSide::Buy, 1.0, 100.5, true)
        .expect("terminal event should emit");
    assert_eq!(decision.side, OrderSide::Buy);
    assert!((decision.size - 1.0).abs() < 1e-9);
    assert!((decision.avg_price - 100.5).abs() < 1e-9);

    // Only now do we insert into dedup (simulating the service pattern).
    assert!(dedup.insert_if_new(FillKey::from_order_cumulative(42, 1.0)));

    // A late-arriving event for the same order must be a no-op.
    assert!(agg.on_fill(42, OrderSide::Buy, 1.0, 100.5, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::from_order_cumulative(42, 1.0)));
}

/// Three independent detectors racing on the same fill - only one hedge.
#[test]
fn multiple_detectors_on_same_order_yield_single_hedge() {
    let dedup = FillDedup::new(64);
    let agg = FillAggregator::new(1_000.0);

    // WS sees the full fill first and emits.
    let d_ws = agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).unwrap();
    assert!(dedup.insert_if_new(FillKey::from_order_cumulative(99, 0.5)));

    // REST sees the same fill a tick later - aggregator returns None.
    assert!(agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::from_order_cumulative(99, 0.5)));

    // Position monitor sees the delta - also aggregator None.
    assert!(agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::from_order_cumulative(99, 0.5)));

    // Exactly one hedge decision was produced.
    assert!((d_ws.size - 0.5).abs() < 1e-9);
}

/// Partial fill that blows past the emergency notional must emit even though
/// it's not terminal - insures against a runaway exchange.
#[test]
fn emergency_notional_breach_emits_without_terminal() {
    let agg = FillAggregator::new(100.0); // low ceiling for the test

    // 1.5 @ $100 = $150 notional > $100 ceiling.
    let d = agg
        .on_fill(7, OrderSide::Buy, 1.5, 100.0, false)
        .expect("emergency breach must emit");
    assert!((d.size - 1.5).abs() < 1e-9);
}

/// Dedup cap holds even as keys churn - no unbounded growth.
#[test]
fn dedup_respects_cap_under_churn() {
    let dedup = FillDedup::new(8);
    for i in 0..1000u64 {
        dedup.insert_if_new(FillKey::OrderId(i));
    }
    assert_eq!(dedup.len(), 8);
    // Most recent keys retained.
    assert!(dedup.contains(&FillKey::OrderId(999)));
    assert!(dedup.contains(&FillKey::OrderId(992)));
    assert!(!dedup.contains(&FillKey::OrderId(0)));
}

/// The idle flusher emits accumulators that never saw a terminal event. This
/// is the fallback path for exchange bugs where the final update is dropped.
#[test]
fn idle_flush_emits_stale_partials() {
    // Construct a short idle_timeout directly (crate-public fields via accessor
    // aren't needed because `flush_idle`/`on_fill` are the only API).
    let agg = FillAggregator::new(1_000_000.0);
    assert!(agg.on_fill(1, OrderSide::Buy, 0.3, 100.0, false).is_none());
    // Default idle_timeout is 2s; sleep a hair longer for determinism.
    std::thread::sleep(Duration::from_millis(2100));
    let flushed = agg.flush_idle();
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].0, 1);
    assert!((flushed[0].1.size - 0.3).abs() < 1e-9);

    // Emitting again does nothing.
    let again = agg.flush_idle();
    assert!(again.is_empty());
}

/// A stale partial can be hedged, and a later terminal event for the same
/// order must hedge only the newly-filled residual.
#[test]
fn idle_partial_then_later_terminal_emits_residual() {
    let agg = FillAggregator::new(1_000_000.0);
    assert!(agg.on_fill(77, OrderSide::Buy, 0.4, 100.0, false).is_none());
    std::thread::sleep(Duration::from_millis(2100));
    let first = agg.flush_idle();
    assert_eq!(first.len(), 1);
    assert!((first[0].1.size - 0.4).abs() < 1e-9);

    let second = agg
        .on_fill(77, OrderSide::Buy, 1.0, 101.0, true)
        .expect("later terminal fill should emit residual");
    assert!((second.size - 0.6).abs() < 1e-9);
    assert!(second.terminal);
}

#[tokio::test]
async fn queue_full_releases_reserved_pending() {
    let agg = FillAggregator::new(50.0);
    assert!(agg.observe_fill(88, OrderSide::Buy, 1.0, 100.0, false));
    let reservation = agg.try_reserve_hedge(88).unwrap();
    let intent: HedgeIntent = reservation.into();

    let (tx, mut rx) = mpsc::channel(1);
    tx.try_send(HedgeIntent::from_maker_fill(
        1,
        0,
        OrderSide::Buy,
        1.0,
        100.0,
        std::time::Instant::now(),
        true,
    ))
    .unwrap();
    let bot_state = Arc::new(RwLock::new(BotState::new()));

    let result = enqueue_hedge_intent(&tx, &bot_state, intent).await.unwrap();
    assert!(matches!(
        result,
        HedgeEnqueueResult::PersistedButNotQueued { .. }
    ));
    agg.release_reservation(reservation);

    let state = agg.snapshot(88).unwrap();
    assert!((state.residual() - 1.0).abs() < 1e-9);
    let again = agg.try_reserve_hedge(88).unwrap();
    assert!((again.size - 1.0).abs() < 1e-9);

    let _ = rx.try_recv();
}
