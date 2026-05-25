use std::sync::atomic::{compiler_fence, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Validate a `(bid, ask)` tuple before using it for quoting or hedging.
///
/// Returns true only if both sides are strictly positive, the book is not
/// crossed, and the spread is not absurdly wide.
#[inline]
pub fn prices_valid(bid: f64, ask: f64) -> bool {
    bid > 0.0 && ask > 0.0 && ask > bid && (ask - bid) / bid <= 0.05
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSource {
    Unknown = 0,
    Stream = 1,
    Rest = 2,
}

impl QuoteSource {
    #[inline]
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Stream,
            2 => Self::Rest,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct QuoteSnapshot {
    pub bid: f64,
    pub ask: f64,
    pub exchange_ts_ms: u64,
    pub recv_ns: u64,
    pub source: QuoteSource,
    pub seq: u64,
}

#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

#[inline]
pub fn quote_usable(snapshot: QuoteSnapshot, now_ns: u64, max_age_ns: u64) -> bool {
    prices_valid(snapshot.bid, snapshot.ask)
        && snapshot.recv_ns > 0
        && now_ns.saturating_sub(snapshot.recv_ns) <= max_age_ns
}

/// Seqlock-style shared top-of-book quote.
///
/// Readers obtain a coherent bid/ask/timestamp/source snapshot without a
/// mutex. Writers reserve the sequence by making it odd, publish all fields,
/// then make it even. REST fallback writes do not overwrite a fresh streaming
/// quote, preventing a slower poll from replacing a good WebSocket update.
pub struct AtomicQuote {
    seq: AtomicU64,
    bid_bits: AtomicU64,
    ask_bits: AtomicU64,
    exchange_ts_ms: AtomicU64,
    recv_ns: AtomicU64,
    source: AtomicU64,
}

/// Backward-compatible name used by existing services.
pub type SharedQuote = AtomicQuote;

impl AtomicQuote {
    const REST_OVERWRITE_STREAM_AFTER_NS: u64 = 1_000_000_000;

    /// Construct an empty quote (bid = ask = 0.0). `prices_valid` rejects
    /// this as "not yet populated".
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicU64::new(0),
            bid_bits: AtomicU64::new(0),
            ask_bits: AtomicU64::new(0),
            exchange_ts_ms: AtomicU64::new(0),
            recv_ns: AtomicU64::new(0),
            source: AtomicU64::new(QuoteSource::Unknown as u64),
        })
    }

    #[inline]
    fn reserve_write(&self) -> u64 {
        loop {
            let seq = self.seq.load(Ordering::Acquire);
            if seq % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }
            if self
                .seq
                .compare_exchange(seq, seq + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return seq;
            }
        }
    }

    /// Load the current pair for legacy call sites. Prefer `snapshot()` for
    /// freshness-sensitive paths.
    #[inline]
    pub fn load(&self) -> (f64, f64) {
        match self.snapshot() {
            Some(s) => (s.bid, s.ask),
            None => (0.0, 0.0),
        }
    }

    /// Load a coherent snapshot.
    #[inline]
    pub fn snapshot(&self) -> Option<QuoteSnapshot> {
        for _ in 0..8 {
            let seq1 = self.seq.load(Ordering::Acquire);
            if seq1 % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }

            compiler_fence(Ordering::Acquire);

            let bid = f64::from_bits(self.bid_bits.load(Ordering::Relaxed));
            let ask = f64::from_bits(self.ask_bits.load(Ordering::Relaxed));
            let exchange_ts_ms = self.exchange_ts_ms.load(Ordering::Relaxed);
            let recv_ns = self.recv_ns.load(Ordering::Relaxed);
            let source = QuoteSource::from_u8(self.source.load(Ordering::Relaxed) as u8);

            compiler_fence(Ordering::Acquire);

            let seq2 = self.seq.load(Ordering::Acquire);
            if seq1 == seq2 && seq2 % 2 == 0 {
                return Some(QuoteSnapshot {
                    bid,
                    ask,
                    exchange_ts_ms,
                    recv_ns,
                    source,
                    seq: seq2,
                });
            }
        }
        None
    }

    /// Publish a new pair without source metadata.
    #[inline]
    pub fn store(&self, bid: f64, ask: f64) {
        self.store_with_source(bid, ask, 0, QuoteSource::Unknown);
    }

    /// Publish a new pair with source and exchange timestamp metadata.
    #[inline]
    pub fn store_with_source(&self, bid: f64, ask: f64, exchange_ts_ms: u64, source: QuoteSource) {
        let now = now_ns();
        if source == QuoteSource::Rest {
            if let Some(existing) = self.snapshot() {
                if existing.source == QuoteSource::Stream
                    && now.saturating_sub(existing.recv_ns) < Self::REST_OVERWRITE_STREAM_AFTER_NS
                {
                    return;
                }
            }
        }

        let seq = self.reserve_write();
        self.ask_bits.store(ask.to_bits(), Ordering::Release);
        self.bid_bits.store(bid.to_bits(), Ordering::Release);
        self.exchange_ts_ms.store(exchange_ts_ms, Ordering::Release);
        self.recv_ns.store(now, Ordering::Release);
        self.source.store(source as u64, Ordering::Release);
        self.seq.store(seq + 2, Ordering::Release);
    }

    /// Return a coherent, fresh snapshot.
    #[inline]
    pub fn usable_snapshot(&self, max_age: Duration) -> Option<QuoteSnapshot> {
        let snap = self.snapshot()?;
        if quote_usable(
            snap,
            now_ns(),
            max_age.as_nanos().min(u64::MAX as u128) as u64,
        ) {
            Some(snap)
        } else {
            None
        }
    }

    /// True if a `store()` has been issued with both sides strictly positive.
    #[inline]
    pub fn is_populated(&self) -> bool {
        self.snapshot()
            .map(|s| s.bid > 0.0 && s.ask > 0.0)
            .unwrap_or(false)
    }
}

impl std::fmt::Debug for AtomicQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.snapshot() {
            Some(s) => write!(
                f,
                "AtomicQuote(bid={}, ask={}, recv_ns={}, source={:?}, seq={})",
                s.bid, s.ask, s.recv_ns, s.source, s.seq
            ),
            None => write!(f, "AtomicQuote(<writing>)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_sentinel_rejected() {
        assert!(!prices_valid(0.0, 0.0));
        assert!(!prices_valid(0.0, 100.0));
        assert!(!prices_valid(100.0, 0.0));
    }

    #[test]
    fn negative_rejected() {
        assert!(!prices_valid(-1.0, 1.0));
        assert!(!prices_valid(1.0, -1.0));
    }

    #[test]
    fn crossed_book_rejected() {
        assert!(!prices_valid(100.0, 99.5));
        assert!(!prices_valid(100.0, 100.0));
    }

    #[test]
    fn wide_spread_rejected() {
        assert!(!prices_valid(100.0, 110.0));
    }

    #[test]
    fn sane_quote_accepted() {
        assert!(prices_valid(100.0, 100.01));
        assert!(prices_valid(100.0, 104.99));
        assert!(prices_valid(0.01, 0.0101));
    }

    #[test]
    fn shared_quote_empty_loads_zero() {
        let q = SharedQuote::empty();
        assert_eq!(q.load(), (0.0, 0.0));
        assert!(!q.is_populated());
        assert!(!prices_valid(q.load().0, q.load().1));
    }

    #[test]
    fn shared_quote_store_load_roundtrip() {
        let q = SharedQuote::empty();
        q.store(100.0, 100.05);
        assert_eq!(q.load(), (100.0, 100.05));
        assert!(q.is_populated());
        let (b, a) = q.load();
        assert!(prices_valid(b, a));
    }

    #[test]
    fn quote_snapshot_tracks_freshness_and_source() {
        let q = SharedQuote::empty();
        q.store_with_source(100.0, 100.05, 123, QuoteSource::Stream);
        let snap = q.snapshot().unwrap();
        assert_eq!(snap.source, QuoteSource::Stream);
        assert_eq!(snap.exchange_ts_ms, 123);
        assert!(quote_usable(snap, now_ns(), 1_000_000_000));
    }

    #[test]
    fn fresh_stream_quote_is_not_overwritten_by_rest() {
        let q = SharedQuote::empty();
        q.store_with_source(100.0, 100.05, 1, QuoteSource::Stream);
        q.store_with_source(99.0, 99.05, 2, QuoteSource::Rest);
        let snap = q.snapshot().unwrap();
        assert_eq!(snap.source, QuoteSource::Stream);
        assert_eq!((snap.bid, snap.ask), (100.0, 100.05));
    }

    #[test]
    fn stale_quote_is_not_usable() {
        let q = SharedQuote::empty();
        q.store(100.0, 100.05);
        assert!(q.usable_snapshot(Duration::from_secs(1)).is_some());
        assert!(q.usable_snapshot(Duration::from_nanos(0)).is_none());
    }

    #[test]
    fn shared_quote_concurrent_readers_never_see_garbage() {
        use std::thread;

        let q = SharedQuote::empty();
        q.store(100.0, 100.05);

        let writer_q = Arc::clone(&q);
        let writer = thread::spawn(move || {
            for i in 0..50_000u64 {
                if i % 2 == 0 {
                    writer_q.store(100.0, 100.05);
                } else {
                    writer_q.store(200.0, 200.10);
                }
            }
        });

        let mut reader_handles = Vec::new();
        for _ in 0..4 {
            let rq = Arc::clone(&q);
            reader_handles.push(thread::spawn(move || {
                for _ in 0..50_000 {
                    let (b, a) = rq.load();
                    assert!(b.is_finite() && a.is_finite());
                    assert!(b >= 0.0 && a >= 0.0);
                    if prices_valid(b, a) {
                        assert!((b - 100.0).abs() < 0.01 || (b - 200.0).abs() < 0.01);
                    }
                }
            }));
        }
        writer.join().unwrap();
        for h in reader_handles {
            h.join().unwrap();
        }
    }
}
