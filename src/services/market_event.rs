use crossbeam_queue::ArrayQueue;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy)]
pub enum MarketSource {
    Pacifica,
    Hyperliquid,
}

#[derive(Debug, Clone, Copy)]
pub struct MarketEvent {
    pub source: MarketSource,
    pub ts: u64,
}

/// Lock-free market event hub with a coalescing ring buffer.
pub struct MarketEventHub {
    queue: ArrayQueue<MarketEvent>,
    notify: Notify,
}

impl MarketEventHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            notify: Notify::new(),
        }
    }

    #[inline]
    pub fn push(&self, event: MarketEvent) {
        if self.queue.push(event).is_err() {
            let _ = self.queue.pop();
            let _ = self.queue.push(event);
        }
        self.notify.notify_one();
    }

    #[inline]
    pub fn pop(&self) -> Option<MarketEvent> {
        self.queue.pop()
    }

    pub fn notifier(&self) -> &Notify {
        &self.notify
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_source_enum() {
        let pacifica = MarketSource::Pacifica;
        let hyperliquid = MarketSource::Hyperliquid;

        // Test Debug trait
        assert_eq!(format!("{:?}", pacifica), "Pacifica");
        assert_eq!(format!("{:?}", hyperliquid), "Hyperliquid");

        // Test Clone and Copy
        let pacifica_copy = pacifica;
        assert!(matches!(pacifica_copy, MarketSource::Pacifica));
    }

    #[test]
    fn test_market_event_creation() {
        let event = MarketEvent {
            source: MarketSource::Pacifica,
            ts: 1234567890,
        };

        assert!(matches!(event.source, MarketSource::Pacifica));
        assert_eq!(event.ts, 1234567890);
    }

    #[test]
    fn test_market_event_clone() {
        let event = MarketEvent {
            source: MarketSource::Hyperliquid,
            ts: 9876543210,
        };

        let cloned = event;  // Copy
        assert!(matches!(cloned.source, MarketSource::Hyperliquid));
        assert_eq!(cloned.ts, 9876543210);
    }

    #[test]
    fn test_hub_new() {
        let hub = MarketEventHub::new(10);

        // Initially empty
        assert!(hub.pop().is_none());
    }

    #[test]
    fn test_hub_push_pop_single() {
        let hub = MarketEventHub::new(10);

        let event = MarketEvent {
            source: MarketSource::Pacifica,
            ts: 1000,
        };

        hub.push(event);

        let popped = hub.pop();
        assert!(popped.is_some());
        let popped = popped.unwrap();
        assert!(matches!(popped.source, MarketSource::Pacifica));
        assert_eq!(popped.ts, 1000);

        // Now empty
        assert!(hub.pop().is_none());
    }

    #[test]
    fn test_hub_push_pop_fifo_order() {
        let hub = MarketEventHub::new(10);

        // Push multiple events
        hub.push(MarketEvent { source: MarketSource::Pacifica, ts: 1 });
        hub.push(MarketEvent { source: MarketSource::Hyperliquid, ts: 2 });
        hub.push(MarketEvent { source: MarketSource::Pacifica, ts: 3 });

        // Pop in FIFO order
        let e1 = hub.pop().unwrap();
        assert!(matches!(e1.source, MarketSource::Pacifica));
        assert_eq!(e1.ts, 1);

        let e2 = hub.pop().unwrap();
        assert!(matches!(e2.source, MarketSource::Hyperliquid));
        assert_eq!(e2.ts, 2);

        let e3 = hub.pop().unwrap();
        assert!(matches!(e3.source, MarketSource::Pacifica));
        assert_eq!(e3.ts, 3);

        assert!(hub.pop().is_none());
    }

    #[test]
    fn test_hub_capacity_drops_oldest() {
        let hub = MarketEventHub::new(3);

        // Fill to capacity
        hub.push(MarketEvent { source: MarketSource::Pacifica, ts: 1 });
        hub.push(MarketEvent { source: MarketSource::Pacifica, ts: 2 });
        hub.push(MarketEvent { source: MarketSource::Pacifica, ts: 3 });

        // Push one more - should drop oldest (ts=1)
        hub.push(MarketEvent { source: MarketSource::Hyperliquid, ts: 4 });

        // Should get ts=2, ts=3, ts=4 (oldest dropped)
        let e1 = hub.pop().unwrap();
        assert_eq!(e1.ts, 2);

        let e2 = hub.pop().unwrap();
        assert_eq!(e2.ts, 3);

        let e3 = hub.pop().unwrap();
        assert_eq!(e3.ts, 4);

        assert!(hub.pop().is_none());
    }

    #[test]
    fn test_hub_notifier() {
        let hub = MarketEventHub::new(10);

        // Should return a reference to Notify
        let notifier = hub.notifier();
        assert!(std::ptr::eq(notifier, &hub.notify));
    }

    #[test]
    fn test_hub_multiple_overflow() {
        let hub = MarketEventHub::new(2);

        // Overflow multiple times
        for i in 0..10 {
            hub.push(MarketEvent {
                source: if i % 2 == 0 { MarketSource::Pacifica } else { MarketSource::Hyperliquid },
                ts: i as u64,
            });
        }

        // Should have the last 2 events (ts=8, ts=9)
        let e1 = hub.pop().unwrap();
        let e2 = hub.pop().unwrap();

        // Due to the drop behavior, we should have events 8 and 9
        assert!(e1.ts >= 8);
        assert!(e2.ts >= 8);
        assert!(hub.pop().is_none());
    }

    #[test]
    fn test_hub_empty_pop() {
        let hub = MarketEventHub::new(10);

        // Pop from empty queue
        assert!(hub.pop().is_none());
        assert!(hub.pop().is_none());
        assert!(hub.pop().is_none());
    }
}
