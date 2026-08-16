use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lock-free price pair using a SeqLock pattern.
///
/// Guarantees that readers always see a consistent (bid, ask) pair from
/// the same update, without any mutex. Writers increment a sequence counter
/// to odd (writing) and back to even (done). Readers spin-retry if they
/// observe an odd sequence or a mismatch between pre/post reads.
pub struct AtomicPricePair {
    sequence: AtomicU64,
    bid_bits: AtomicU64,
    ask_bits: AtomicU64,
}

impl AtomicPricePair {
    pub const fn new() -> Self {
        Self {
            sequence: AtomicU64::new(0),
            bid_bits: AtomicU64::new(0),
            ask_bits: AtomicU64::new(0),
        }
    }

    /// Store a new (bid, ask) pair. Only one writer should call this at a time
    /// (enforced by the single-owner pattern in each orderbook service).
    pub fn store(&self, bid: f64, ask: f64) {
        // Odd sequence signals "write in progress"
        self.sequence.fetch_add(1, Ordering::Release);
        self.bid_bits.store(bid.to_bits(), Ordering::Release);
        self.ask_bits.store(ask.to_bits(), Ordering::Release);
        // Even sequence signals "write complete"
        self.sequence.fetch_add(1, Ordering::Release);
    }

    /// Load a consistent (bid, ask) pair. Lock-free, wait-free in practice
    /// (writers finish in nanoseconds so spin is ~0 iterations).
    #[inline]
    pub fn load(&self) -> (f64, f64) {
        loop {
            let seq1 = self.sequence.load(Ordering::Acquire);
            if seq1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let bid = f64::from_bits(self.bid_bits.load(Ordering::Acquire));
            let ask = f64::from_bits(self.ask_bits.load(Ordering::Acquire));
            let seq2 = self.sequence.load(Ordering::Acquire);
            if seq1 == seq2 {
                return (bid, ask);
            }
            std::hint::spin_loop();
        }
    }
}

// SAFETY: AtomicPricePair is composed entirely of atomics.
unsafe impl Send for AtomicPricePair {}
unsafe impl Sync for AtomicPricePair {}

/// Lock-free timestamp for grace period checks.
///
/// Stores a millisecond-resolution timestamp as an atomic u64.
/// Avoids needing an RwLock just to check elapsed time.
pub struct AtomicInstant {
    millis_since_epoch: AtomicU64,
}

impl AtomicInstant {
    pub fn new() -> Self {
        Self {
            millis_since_epoch: AtomicU64::new(0),
        }
    }

    /// Record the current time.
    pub fn set_now(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.millis_since_epoch.store(now, Ordering::Release);
    }

    /// Check if at least `secs` seconds have elapsed since the last `set_now()`.
    /// Returns `true` if never set (no grace period needed).
    pub fn has_elapsed(&self, secs: u64) -> bool {
        let stored = self.millis_since_epoch.load(Ordering::Acquire);
        if stored == 0 {
            return true;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now.saturating_sub(stored) >= secs * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_price_pair_basic() {
        let pair = AtomicPricePair::new();
        assert_eq!(pair.load(), (0.0, 0.0));

        pair.store(100.5, 101.0);
        assert_eq!(pair.load(), (100.5, 101.0));

        pair.store(200.25, 200.75);
        assert_eq!(pair.load(), (200.25, 200.75));
    }

    #[test]
    fn test_atomic_instant_never_set() {
        let instant = AtomicInstant::new();
        assert!(instant.has_elapsed(1));
        assert!(instant.has_elapsed(100));
    }

    #[test]
    fn test_atomic_instant_just_set() {
        let instant = AtomicInstant::new();
        instant.set_now();
        // Just set, so 10 seconds should NOT have elapsed
        assert!(!instant.has_elapsed(10));
        // But 0 seconds should have elapsed
        assert!(instant.has_elapsed(0));
    }

    #[test]
    fn test_atomic_price_pair_f64_edge_values() {
        let pair = AtomicPricePair::new();

        // NaN
        pair.store(f64::NAN, f64::NAN);
        let (bid, ask) = pair.load();
        assert!(bid.is_nan());
        assert!(ask.is_nan());

        // Infinity
        pair.store(f64::INFINITY, f64::NEG_INFINITY);
        assert_eq!(pair.load(), (f64::INFINITY, f64::NEG_INFINITY));

        // Negative zero
        pair.store(-0.0, 0.0);
        let (bid, ask) = pair.load();
        assert_eq!(bid.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(ask.to_bits(), 0.0_f64.to_bits());

        // Subnormal (smallest positive subnormal)
        let subnormal = f64::from_bits(1);
        pair.store(subnormal, subnormal);
        let (bid, ask) = pair.load();
        assert_eq!(bid.to_bits(), 1);
        assert_eq!(ask.to_bits(), 1);
    }

    #[test]
    fn test_atomic_price_pair_many_updates() {
        let pair = AtomicPricePair::new();
        for i in 0..1000u64 {
            let bid = i as f64;
            let ask = i as f64 + 0.5;
            pair.store(bid, ask);
        }
        // Last stored value should be readable
        assert_eq!(pair.load(), (999.0, 999.5));
    }

    #[test]
    fn test_atomic_price_pair_concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;

        let pair = Arc::new(AtomicPricePair::new());
        // Initial valid pair so readers don't see (0,0) as torn
        pair.store(0.0, 0.5);

        let writer_pair = Arc::clone(&pair);
        let writer = thread::spawn(move || {
            for i in 1..10_000u64 {
                writer_pair.store(i as f64, i as f64 + 0.5);
            }
        });

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let reader_pair = Arc::clone(&pair);
                thread::spawn(move || {
                    for _ in 0..10_000 {
                        let (bid, ask) = reader_pair.load();
                        // The critical invariant: ask == bid + 0.5 always
                        assert!(
                            (ask - bid - 0.5).abs() < f64::EPSILON,
                            "Torn read detected! bid={}, ask={}, diff={}",
                            bid,
                            ask,
                            ask - bid
                        );
                    }
                })
            })
            .collect();

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }

    #[test]
    fn test_atomic_instant_overwrite() {
        let instant = AtomicInstant::new();
        instant.set_now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        instant.set_now(); // Overwrite with fresh timestamp
        // Just reset, so 10 seconds should NOT have elapsed
        assert!(!instant.has_elapsed(10));
        // 0 seconds should have elapsed
        assert!(instant.has_elapsed(0));
    }

    #[test]
    fn test_atomic_instant_large_secs() {
        let instant = AtomicInstant::new();
        instant.set_now();
        // u64::MAX / 1000 is huge — should not panic due to saturating_sub
        assert!(!instant.has_elapsed(u64::MAX / 1000));
    }
}
