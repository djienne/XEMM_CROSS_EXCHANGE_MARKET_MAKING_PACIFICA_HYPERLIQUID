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
                continue;
            }
            let bid = f64::from_bits(self.bid_bits.load(Ordering::Relaxed));
            let ask = f64::from_bits(self.ask_bits.load(Ordering::Relaxed));
            let ts = self.ts.load(Ordering::Relaxed);
            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 {
                return PriceSnapshot { bid, ask, ts };
            }
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
