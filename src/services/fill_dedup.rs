use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// Canonical fill dedup key.
///
/// The pre-generated Pacifica client order ID is the canonical identifier
/// because it is known before REST placement returns and is shared across
/// WebSocket, REST, and position-inferred paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FillKey {
    OrderId(u64),
    OrderCumulative {
        order_id: u64,
        cumulative_quanta: u128,
    },
    Cloid(String),
    CloidCumulative {
        cloid: String,
        cumulative_quanta: u128,
    },
}

impl FillKey {
    pub fn from_order_id(id: u64) -> Self {
        FillKey::OrderId(id)
    }

    pub fn from_cloid(cloid: impl Into<String>) -> Self {
        FillKey::Cloid(cloid.into())
    }

    pub fn from_order_cumulative(id: u64, cumulative_size: f64) -> Self {
        FillKey::OrderCumulative {
            order_id: id,
            cumulative_quanta: quantize_size(cumulative_size),
        }
    }

    pub fn from_cloid_cumulative(cloid: impl Into<String>, cumulative_size: f64) -> Self {
        FillKey::CloidCumulative {
            cloid: cloid.into(),
            cumulative_quanta: quantize_size(cumulative_size),
        }
    }
}

#[inline]
fn quantize_size(size: f64) -> u128 {
    // u128 so even ultra-cheap, very-large-size symbols (where size * 1e9 would
    // overflow u64 and saturate to u64::MAX) cannot collapse distinct cumulative
    // sizes onto the same dedup key.
    (size.max(0.0) * 1_000_000_000.0).round() as u128
}

/// Bounded fill-dedup set.
///
/// Keeps the most recent `cap` keys. When full, the oldest key is evicted
/// on insert. Thread-safe via `parking_lot::Mutex`.
#[derive(Debug)]
pub struct FillDedup {
    inner: Mutex<FillDedupInner>,
}

#[derive(Debug)]
struct FillDedupInner {
    set: HashSet<FillKey>,
    order: VecDeque<FillKey>,
    cap: usize,
}

impl FillDedup {
    pub fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(FillDedupInner {
                set: HashSet::with_capacity(cap),
                order: VecDeque::with_capacity(cap),
                cap,
            }),
        })
    }

    /// Default capacity suitable for a long-running market-maker: 4096 keys
    /// covers ~1 order/second for ~68 minutes before any eviction kicks in.
    pub fn new_default() -> Arc<Self> {
        Self::new(4096)
    }

    /// Return true if the key was newly inserted, false if already present.
    ///
    /// This is the sole API used by fill detectors: the detector emits a hedge
    /// event only when this returns `true`.
    pub fn insert_if_new(&self, key: FillKey) -> bool {
        let mut g = self.inner.lock();
        if g.set.contains(&key) {
            return false;
        }
        if g.order.len() >= g.cap {
            if let Some(evicted) = g.order.pop_front() {
                g.set.remove(&evicted);
            }
        }
        g.order.push_back(key.clone());
        g.set.insert(key);
        true
    }

    /// Current number of tracked keys (diagnostic / test use).
    pub fn len(&self) -> usize {
        self.inner.lock().set.len()
    }

    /// Check membership without inserting (diagnostic / test use).
    pub fn contains(&self, key: &FillKey) -> bool {
        self.inner.lock().set.contains(key)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_new_key_returns_true() {
        let d = FillDedup::new(8);
        assert!(d.insert_if_new(FillKey::OrderId(1)));
        assert!(d.contains(&FillKey::OrderId(1)));
    }

    #[test]
    fn reinserting_same_key_returns_false() {
        let d = FillDedup::new(8);
        assert!(d.insert_if_new(FillKey::OrderId(42)));
        assert!(!d.insert_if_new(FillKey::OrderId(42)));
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn cap_is_enforced_with_fifo_eviction() {
        let d = FillDedup::new(3);
        for i in 0..5u64 {
            assert!(d.insert_if_new(FillKey::OrderId(i)));
        }
        assert_eq!(d.len(), 3);
        // Oldest evicted: 0, 1
        assert!(!d.contains(&FillKey::OrderId(0)));
        assert!(!d.contains(&FillKey::OrderId(1)));
        // Retained: 2, 3, 4
        assert!(d.contains(&FillKey::OrderId(2)));
        assert!(d.contains(&FillKey::OrderId(3)));
        assert!(d.contains(&FillKey::OrderId(4)));
    }

    #[test]
    fn cloid_and_order_id_do_not_collide() {
        let d = FillDedup::new(8);
        assert!(d.insert_if_new(FillKey::OrderId(100)));
        assert!(d.insert_if_new(FillKey::Cloid("100".to_string())));
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn cumulative_fill_keys_allow_later_residuals() {
        let d = FillDedup::new(8);
        assert!(d.insert_if_new(FillKey::from_cloid_cumulative("cloid-42", 0.4)));
        assert!(!d.insert_if_new(FillKey::from_cloid_cumulative("cloid-42", 0.4)));
        assert!(d.insert_if_new(FillKey::from_cloid_cumulative("cloid-42", 1.0)));
    }

    #[test]
    fn evicted_key_can_be_reinserted() {
        let d = FillDedup::new(2);
        d.insert_if_new(FillKey::OrderId(1));
        d.insert_if_new(FillKey::OrderId(2));
        d.insert_if_new(FillKey::OrderId(3)); // evicts 1
        assert!(!d.contains(&FillKey::OrderId(1)));
        assert!(d.insert_if_new(FillKey::OrderId(1)));
        assert!(d.contains(&FillKey::OrderId(1)));
    }
}
