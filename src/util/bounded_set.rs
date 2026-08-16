use std::collections::{HashSet, VecDeque};

/// A bounded set that evicts the oldest entries when capacity is reached.
/// Maintains insertion order via VecDeque for FIFO eviction.
pub struct BoundedSet {
    set: HashSet<String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl BoundedSet {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: HashSet::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn contains(&self, value: &str) -> bool {
        self.set.contains(value)
    }

    pub fn insert(&mut self, value: String) {
        if self.set.contains(&value) {
            return;
        }
        // Evict oldest entries if at capacity
        while self.set.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            } else {
                break;
            }
        }
        self.order.push_back(value.clone());
        self.set.insert(value);
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}
