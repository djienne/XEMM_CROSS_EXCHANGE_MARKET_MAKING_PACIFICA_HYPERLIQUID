use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub struct PriceSnapshot {
    pub bid: f64,
    pub ask: f64,
    pub ts: u64,
}

/// Lock-free top-of-book snapshot using a simple seqlock.
#[derive(Debug)]
pub struct AtomicPrice {
    seq: AtomicU64,
    bid_bits: AtomicU64,
    ask_bits: AtomicU64,
    ts: AtomicU64,
}

impl AtomicPrice {
    pub fn new() -> Self {
        Self {
            seq: AtomicU64::new(0),
            bid_bits: AtomicU64::new(0),
            ask_bits: AtomicU64::new(0),
            ts: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn store(&self, bid: f64, ask: f64, ts: u64) {
        // Single-threaded runtime guarantees no concurrent writers.
        self.seq.fetch_add(1, Ordering::AcqRel); // mark write in progress (odd)
        self.bid_bits.store(bid.to_bits(), Ordering::Relaxed);
        self.ask_bits.store(ask.to_bits(), Ordering::Relaxed);
        self.ts.store(ts, Ordering::Relaxed);
        self.seq.fetch_add(1, Ordering::Release); // publish (even)
    }

    #[inline]
    pub fn load(&self) -> PriceSnapshot {
        loop {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 & 1 == 1 {
                // Write in progress - spin with CPU hint
                std::hint::spin_loop();
                continue;
            }
            let bid = f64::from_bits(self.bid_bits.load(Ordering::Relaxed));
            let ask = f64::from_bits(self.ask_bits.load(Ordering::Relaxed));
            let ts = self.ts.load(Ordering::Relaxed);
            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                return PriceSnapshot { bid, ask, ts };
            }
            // Inconsistent read - spin with CPU hint before retry
            std::hint::spin_loop();
        }
    }

    #[inline]
    pub fn load_bid_ask(&self) -> (f64, f64) {
        let snapshot = self.load();
        (snapshot.bid, snapshot.ask)
    }
}

impl Default for AtomicPrice {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_default() {
        let price1 = AtomicPrice::new();
        let price2 = AtomicPrice::default();

        let snap1 = price1.load();
        let snap2 = price2.load();

        assert_eq!(snap1.bid, 0.0);
        assert_eq!(snap1.ask, 0.0);
        assert_eq!(snap1.ts, 0);
        assert_eq!(snap2.bid, 0.0);
        assert_eq!(snap2.ask, 0.0);
        assert_eq!(snap2.ts, 0);
    }

    #[test]
    fn test_store_and_load() {
        let price = AtomicPrice::new();

        price.store(100.5, 100.6, 1234567890);

        let snap = price.load();
        assert_eq!(snap.bid, 100.5);
        assert_eq!(snap.ask, 100.6);
        assert_eq!(snap.ts, 1234567890);
    }

    #[test]
    fn test_store_updates_all_fields() {
        let price = AtomicPrice::new();

        // First store
        price.store(50.0, 51.0, 1000);
        let snap1 = price.load();
        assert_eq!(snap1.bid, 50.0);
        assert_eq!(snap1.ask, 51.0);
        assert_eq!(snap1.ts, 1000);

        // Second store - all fields updated
        price.store(60.0, 61.0, 2000);
        let snap2 = price.load();
        assert_eq!(snap2.bid, 60.0);
        assert_eq!(snap2.ask, 61.0);
        assert_eq!(snap2.ts, 2000);
    }

    #[test]
    fn test_load_bid_ask() {
        let price = AtomicPrice::new();

        price.store(99.5, 100.5, 12345);

        let (bid, ask) = price.load_bid_ask();
        assert_eq!(bid, 99.5);
        assert_eq!(ask, 100.5);
    }

    #[test]
    fn test_price_snapshot_clone() {
        let snap = PriceSnapshot {
            bid: 100.0,
            ask: 101.0,
            ts: 12345,
        };

        let cloned = snap;  // Copy
        assert_eq!(cloned.bid, 100.0);
        assert_eq!(cloned.ask, 101.0);
        assert_eq!(cloned.ts, 12345);
    }

    #[test]
    fn test_extreme_values() {
        let price = AtomicPrice::new();

        // Very small values
        price.store(0.000001, 0.000002, 1);
        let snap = price.load();
        assert!((snap.bid - 0.000001).abs() < 1e-12);
        assert!((snap.ask - 0.000002).abs() < 1e-12);

        // Very large values
        price.store(1e15, 1e15 + 1.0, 2);
        let snap = price.load();
        assert_eq!(snap.bid, 1e15);
        assert_eq!(snap.ask, 1e15 + 1.0);

        // Max timestamp
        price.store(100.0, 101.0, u64::MAX);
        let snap = price.load();
        assert_eq!(snap.ts, u64::MAX);
    }

    #[test]
    fn test_negative_prices() {
        // Negative prices shouldn't happen in trading but test for robustness
        let price = AtomicPrice::new();

        price.store(-1.0, -0.5, 100);
        let snap = price.load();
        assert_eq!(snap.bid, -1.0);
        assert_eq!(snap.ask, -0.5);
    }

    #[test]
    fn test_nan_and_infinity() {
        let price = AtomicPrice::new();

        // NaN
        price.store(f64::NAN, f64::NAN, 1);
        let snap = price.load();
        assert!(snap.bid.is_nan());
        assert!(snap.ask.is_nan());

        // Infinity
        price.store(f64::INFINITY, f64::NEG_INFINITY, 2);
        let snap = price.load();
        assert!(snap.bid.is_infinite());
        assert!(snap.ask.is_infinite());
    }

    #[test]
    fn test_multiple_sequential_updates() {
        let price = AtomicPrice::new();

        for i in 0..100 {
            let bid = 100.0 + i as f64;
            let ask = 101.0 + i as f64;
            let ts = i as u64;

            price.store(bid, ask, ts);

            let snap = price.load();
            assert_eq!(snap.bid, bid);
            assert_eq!(snap.ask, ask);
            assert_eq!(snap.ts, ts);
        }
    }

    #[test]
    fn test_seqlock_consistency() {
        // Verify that reads always return consistent snapshots
        let price = AtomicPrice::new();

        for i in 0..1000 {
            let bid = (i as f64) * 0.01;
            let ask = (i as f64) * 0.01 + 0.001;
            let ts = i as u64;

            price.store(bid, ask, ts);
            let snap = price.load();

            // All values should be from the same store operation
            // The relationship bid + 0.001 = ask should hold
            assert!((snap.ask - snap.bid - 0.001).abs() < 1e-10,
                "Inconsistent read: bid={}, ask={}", snap.bid, snap.ask);
        }
    }
}
