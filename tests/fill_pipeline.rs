//! Integration tests for the fill → dedup → aggregator → hedge-decision pipeline.
//!
//! These run without any network: they exercise the coordination between the
//! pure-logic modules that were hardened in FR-6/FR-7/FR-19. The goal is to
//! pin down the combined behaviour so a future refactor can't silently break
//! dedup or partial-fill aggregation.

use std::time::Duration;

use xemm_rust::services::fill_aggregator::FillAggregator;
use xemm_rust::services::fill_dedup::{FillDedup, FillKey};
use xemm_rust::strategy::OrderSide;

/// Partial fill followed by full fill must hedge exactly once, and the hedge
/// must reflect the total accumulated size — not just the terminal increment.
#[test]
fn partial_then_full_emits_single_hedge_with_total_size() {
    let dedup = FillDedup::new(64);
    let agg = FillAggregator::new(1_000.0);

    // Order 42: 0.3 filled @ $100 (partial) — no hedge yet.
    assert!(agg.on_fill(42, OrderSide::Buy, 0.3, 100.0, false).is_none());
    assert!(!dedup.contains(&FillKey::OrderId(42)));

    // Then full fill, cumulative 1.0 @ $100.5 — must emit.
    let decision = agg
        .on_fill(42, OrderSide::Buy, 1.0, 100.5, true)
        .expect("terminal event should emit");
    assert_eq!(decision.side, OrderSide::Buy);
    assert!((decision.size - 1.0).abs() < 1e-9);
    assert!((decision.avg_price - 100.5).abs() < 1e-9);

    // Only now do we insert into dedup (simulating the service pattern).
    assert!(dedup.insert_if_new(FillKey::OrderId(42)));

    // A late-arriving event for the same order must be a no-op.
    assert!(agg.on_fill(42, OrderSide::Buy, 1.0, 100.5, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::OrderId(42)));
}

/// Three independent detectors racing on the same fill — only one hedge.
#[test]
fn multiple_detectors_on_same_order_yield_single_hedge() {
    let dedup = FillDedup::new(64);
    let agg = FillAggregator::new(1_000.0);

    // WS sees the full fill first and emits.
    let d_ws = agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).unwrap();
    assert!(dedup.insert_if_new(FillKey::OrderId(99)));

    // REST sees the same fill a tick later — aggregator returns None.
    assert!(agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::OrderId(99)));

    // Position monitor sees the delta — also aggregator None.
    assert!(agg.on_fill(99, OrderSide::Sell, 0.5, 50.0, true).is_none());
    assert!(!dedup.insert_if_new(FillKey::OrderId(99)));

    // Exactly one hedge decision was produced.
    assert!((d_ws.size - 0.5).abs() < 1e-9);
}

/// Partial fill that blows past the emergency notional must emit even though
/// it's not terminal — insures against a runaway exchange.
#[test]
fn emergency_notional_breach_emits_without_terminal() {
    let agg = FillAggregator::new(100.0); // low ceiling for the test

    // 1.5 @ $100 = $150 notional > $100 ceiling.
    let d = agg
        .on_fill(7, OrderSide::Buy, 1.5, 100.0, false)
        .expect("emergency breach must emit");
    assert!((d.size - 1.5).abs() < 1e-9);
}

/// Dedup cap holds even as keys churn — no unbounded growth.
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
