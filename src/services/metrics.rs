use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct RiskMetrics {
    pub fill_detect_to_hedge_enqueue_us: AtomicU64,
    pub hedge_enqueue_to_submit_us: AtomicU64,
    pub hedge_submit_to_ack_us: AtomicU64,
    pub hedge_target_qty_bits: AtomicU64,
    pub hedge_confirmed_qty_bits: AtomicU64,
    pub hedge_residual_qty_bits: AtomicU64,
    pub aggregator_reserved_qty_bits: AtomicU64,
    pub aggregator_unreserved_residual_qty_bits: AtomicU64,
    pub cancel_demand_generation: AtomicU64,
    pub cancel_verify_latency_ms: AtomicU64,
    pub placement_unknown_count: AtomicU64,
    pub hedge_unknown_count: AtomicU64,
    pub queue_full_count: AtomicU64,
}

static RISK_METRICS: RiskMetrics = RiskMetrics {
    fill_detect_to_hedge_enqueue_us: AtomicU64::new(0),
    hedge_enqueue_to_submit_us: AtomicU64::new(0),
    hedge_submit_to_ack_us: AtomicU64::new(0),
    hedge_target_qty_bits: AtomicU64::new(0),
    hedge_confirmed_qty_bits: AtomicU64::new(0),
    hedge_residual_qty_bits: AtomicU64::new(0),
    aggregator_reserved_qty_bits: AtomicU64::new(0),
    aggregator_unreserved_residual_qty_bits: AtomicU64::new(0),
    cancel_demand_generation: AtomicU64::new(0),
    cancel_verify_latency_ms: AtomicU64::new(0),
    placement_unknown_count: AtomicU64::new(0),
    hedge_unknown_count: AtomicU64::new(0),
    queue_full_count: AtomicU64::new(0),
};

pub fn risk_metrics() -> &'static RiskMetrics {
    &RISK_METRICS
}

pub fn store_f64(slot: &AtomicU64, value: f64) {
    slot.store(value.to_bits(), Ordering::Release);
}

pub fn load_f64(slot: &AtomicU64) -> f64 {
    f64::from_bits(slot.load(Ordering::Acquire))
}
