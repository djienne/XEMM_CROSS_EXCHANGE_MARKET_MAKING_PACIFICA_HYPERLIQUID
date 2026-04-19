use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Validate a `(bid, ask)` tuple before using it for quoting or hedging.
///
/// Returns true only if:
/// - both sides are strictly positive (catches the `(0.0, 0.0)`
///   "never-written" sentinel and any negative garbage from a bad parse),
/// - the book is not crossed (`ask > bid`),
/// - the spread is not absurdly wide (≤ 5 %). A wider spread is almost
///   always stale / broken feed and using it for an opportunity evaluation
///   would place an order at a runaway price.
#[inline]
pub fn prices_valid(bid: f64, ask: f64) -> bool {
    bid > 0.0
        && ask > 0.0
        && ask > bid
        && (ask - bid) / bid <= 0.05
}

/// Wait-free shared top-of-book quote.
///
/// Two independent `AtomicU64`s hold the raw bit representations of the bid
/// and ask as `f64`. Readers call `load()` and writers call `store()`; no
/// locking, no allocation. Because we store the two halves separately, a
/// reader racing a writer can observe a torn `(bid, ask)` pair — but we
/// pipe every read through `prices_valid`, which rejects crossed/negative/
/// wide-spread pairs. A torn read that happens to pass `prices_valid`
/// implies the values are almost-current in both halves, which is exactly
/// what the hot path wants.
///
/// Writer order: ask then bid (Release).
/// Reader order: bid then ask (Acquire).
///
/// With that ordering, if a reader observes a newly-written bid it may see
/// an older ask; the resulting pair is very likely to cross or exceed the
/// 5 % spread filter, and the caller simply skips that tick.
pub struct SharedQuote {
    bid_bits: AtomicU64,
    ask_bits: AtomicU64,
}

impl SharedQuote {
    /// Construct an empty quote (bid = ask = 0.0). `prices_valid` rejects
    /// this as "not yet populated".
    pub fn empty() -> Arc<Self> {
        Arc::new(Self {
            bid_bits: AtomicU64::new(0),
            ask_bits: AtomicU64::new(0),
        })
    }

    /// Load the current pair. Wait-free. See the struct doc for tearing
    /// semantics; callers must validate with `prices_valid` before use.
    #[inline]
    pub fn load(&self) -> (f64, f64) {
        // Read bid first (the "newer" half) then ask.
        let bid = f64::from_bits(self.bid_bits.load(Ordering::Acquire));
        let ask = f64::from_bits(self.ask_bits.load(Ordering::Acquire));
        (bid, ask)
    }

    /// Publish a new pair.
    #[inline]
    pub fn store(&self, bid: f64, ask: f64) {
        // Write ask first so that any reader who observes the new bid will
        // already have (at worst) a fresh ask waiting.
        self.ask_bits.store(ask.to_bits(), Ordering::Release);
        self.bid_bits.store(bid.to_bits(), Ordering::Release);
    }

    /// True if a `store()` has been issued with both sides strictly positive.
    /// Used as a quick "is populated" probe; full validation still goes
    /// through `prices_valid`.
    #[inline]
    pub fn is_populated(&self) -> bool {
        let (bid, ask) = self.load();
        bid > 0.0 && ask > 0.0
    }
}

impl std::fmt::Debug for SharedQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (bid, ask) = self.load();
        write!(f, "SharedQuote(bid={}, ask={})", bid, ask)
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
        // 10% spread is rejected as stale
        assert!(!prices_valid(100.0, 110.0));
    }

    #[test]
    fn sane_quote_accepted() {
        assert!(prices_valid(100.0, 100.01));
        assert!(prices_valid(100.0, 104.99)); // just under 5%
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
    fn shared_quote_concurrent_readers_never_see_garbage() {
        use std::sync::Arc;
        use std::thread;

        let q = SharedQuote::empty();
        // Seed with a valid pair first so the single writer can mutate safely.
        q.store(100.0, 100.05);

        let writer_q = Arc::clone(&q);
        let writer = thread::spawn(move || {
            // Run many writes alternating between two sane pairs.
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
                    // Any observed pair must either validate (one of the two
                    // sane snapshots) OR fail validation (torn read). It must
                    // never be NaN, Inf, or wildly out of range.
                    assert!(b.is_finite() && a.is_finite());
                    assert!(b >= 0.0 && a >= 0.0);
                    if prices_valid(b, a) {
                        // If it validates, it must be close to one of the two
                        // pairs we alternate between.
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
