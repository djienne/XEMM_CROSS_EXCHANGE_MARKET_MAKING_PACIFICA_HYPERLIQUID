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
