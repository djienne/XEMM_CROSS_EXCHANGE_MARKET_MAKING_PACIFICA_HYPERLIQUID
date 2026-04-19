use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::strategy::OrderSide;

/// Accumulator for partial fills on a single order. The aggregator combines
/// events from all fill-detection paths (WS / REST / position-monitor) into
/// one authoritative view so only a single hedge is emitted per order cycle.
#[derive(Debug, Clone, Copy)]
pub struct PartialAccumulator {
    pub cumulative_size: f64,
    pub avg_price: f64,
    pub side: OrderSide,
    pub last_updated: Instant,
    pub created_at: Instant,
    /// Set once the aggregator has emitted a HedgeDecision for this order.
    pub emitted: bool,
}

/// Decision returned by `on_fill` when a hedge should fire.
#[derive(Debug, Clone, Copy)]
pub struct HedgeDecision {
    pub side: OrderSide,
    pub size: f64,
    pub avg_price: f64,
    pub detected_at: Instant,
}

/// Aggregates fill events from multiple detectors into at most one hedge
/// emission per `order_id`.
///
/// Emission rules (first one that fires wins, and the entry stays marked
/// `emitted=true` thereafter so later events for the same order are ignored):
///
/// 1. Terminal event — a full fill, or a cancellation that left some
///    fills behind. Emits whatever was accumulated.
/// 2. Notional breach — cumulative fill notional exceeds
///    `emergency_notional_usd`, a defensive upper bound (e.g. 2× the
///    configured order size) in case the exchange silently splits a fill
///    across many partials without a final terminal event.
/// 3. Idle timeout — `maybe_flush_idle` checks for accumulators with no
///    update in the last `idle_timeout`, emits them, and marks them
///    emitted. Called from a background task at ~1 Hz.
pub struct FillAggregator {
    inner: Mutex<HashMap<u64, PartialAccumulator>>,
    pub emergency_notional_usd: f64,
    pub idle_timeout: Duration,
}

impl FillAggregator {
    pub fn new(emergency_notional_usd: f64) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd,
            idle_timeout: Duration::from_secs(2),
        })
    }

    /// Record a fill for `order_id`. `absolute_filled_size` and `avg_price`
    /// are the authoritative totals as reported by the exchange (not the
    /// per-event increment) — WS `account_order_updates` already reports
    /// these fields as running totals.
    pub fn on_fill(
        &self,
        order_id: u64,
        side: OrderSide,
        absolute_filled_size: f64,
        avg_price: f64,
        is_terminal: bool,
    ) -> Option<HedgeDecision> {
        let now = Instant::now();
        let mut g = self.inner.lock();
        let entry = g.entry(order_id).or_insert_with(|| PartialAccumulator {
            cumulative_size: 0.0,
            avg_price: 0.0,
            side,
            last_updated: now,
            created_at: now,
            emitted: false,
        });

        if entry.emitted {
            return None;
        }

        // Update to authoritative absolute totals. Fill detectors can arrive
        // out of order (REST may race WS); keep the larger cumulative.
        if absolute_filled_size > entry.cumulative_size {
            entry.cumulative_size = absolute_filled_size;
            entry.avg_price = avg_price;
        }
        entry.last_updated = now;

        let should_emit = is_terminal
            || (entry.cumulative_size * entry.avg_price) >= self.emergency_notional_usd;

        if should_emit && entry.cumulative_size > 0.0 {
            entry.emitted = true;
            return Some(HedgeDecision {
                side: entry.side,
                size: entry.cumulative_size,
                avg_price: entry.avg_price,
                detected_at: now,
            });
        }
        None
    }

    /// Scan accumulators and emit any that have been idle past `idle_timeout`
    /// with a non-zero accumulated size. Caller is responsible for dispatching
    /// the returned decisions to the hedge queue.
    pub fn flush_idle(&self) -> Vec<(u64, HedgeDecision)> {
        let now = Instant::now();
        let mut out = Vec::new();
        let mut g = self.inner.lock();
        for (order_id, acc) in g.iter_mut() {
            if !acc.emitted
                && acc.cumulative_size > 0.0
                && now.duration_since(acc.last_updated) >= self.idle_timeout
            {
                acc.emitted = true;
                out.push((
                    *order_id,
                    HedgeDecision {
                        side: acc.side,
                        size: acc.cumulative_size,
                        avg_price: acc.avg_price,
                        detected_at: now,
                    },
                ));
            }
        }
        out
    }

    /// Remove accumulators that have been emitted and are older than
    /// `ttl`. Prevents unbounded growth across long bot runs.
    pub fn gc(&self, ttl: Duration) {
        let now = Instant::now();
        let mut g = self.inner.lock();
        g.retain(|_, acc| !acc.emitted || now.duration_since(acc.created_at) < ttl);
    }

    /// Current accumulator count (diagnostic / test use).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_terminal_partial_does_not_emit() {
        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 0.5, 100.0, false);
        assert!(d.is_none());
    }

    #[test]
    fn terminal_emits_with_accumulated_total() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 0.5, 100.0, false).is_none());
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 101.0, true).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
        assert!((d.avg_price - 101.0).abs() < 1e-9);
        assert_eq!(d.side, OrderSide::Buy);
    }

    #[test]
    fn second_event_after_emit_is_ignored() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_some());
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_none());
    }

    #[test]
    fn emergency_notional_emits_without_terminal() {
        let agg = FillAggregator::new(50.0);
        let d = agg.on_fill(1, OrderSide::Sell, 1.0, 100.0, false).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
    }

    #[test]
    fn out_of_order_cumulative_keeps_larger() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 0.8, 100.0, false).is_none());
        assert!(agg.on_fill(1, OrderSide::Buy, 0.5, 101.0, false).is_none());
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 102.0, true).unwrap();
        assert!((d.size - 1.0).abs() < 1e-9);
        assert!((d.avg_price - 102.0).abs() < 1e-9);
    }

    #[test]
    fn flush_idle_emits_stale_accumulators() {
        // Build an aggregator with short idle_timeout for test determinism.
        let agg = Arc::new(FillAggregator {
            inner: Mutex::new(HashMap::new()),
            emergency_notional_usd: 1_000_000.0,
            idle_timeout: Duration::from_millis(10),
        });
        let _ = agg.on_fill(1, OrderSide::Buy, 0.3, 100.0, false);
        std::thread::sleep(Duration::from_millis(20));
        let flushed = agg.flush_idle();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, 1);
        assert!((flushed[0].1.size - 0.3).abs() < 1e-9);
    }

    #[test]
    fn gc_removes_old_emitted_entries() {
        let agg = FillAggregator::new(1000.0);
        assert!(agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).is_some());
        assert_eq!(agg.len(), 1);
        std::thread::sleep(Duration::from_millis(5));
        agg.gc(Duration::from_millis(1));
        assert_eq!(agg.len(), 0);
    }
}
