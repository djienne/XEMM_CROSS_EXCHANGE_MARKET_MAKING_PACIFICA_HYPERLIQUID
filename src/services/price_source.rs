//! Generic price-stream and price-poll services.
//!
//! Replaces the four near-identical services in `orderbook.rs` and
//! `rest_poll.rs` with two generic structs parameterised over a trait per
//! transport. Adding a third exchange now only requires one trait impl on
//! the connector side - no new service wrapper.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::time::interval;
use tracing::{debug, error, info};

use crate::util::price::{QuoteSource, SharedQuote};

/// Callback the connector pushes (bid, ask, exchange_ts_ms) through. Prices
/// arrive already parsed to f64 by the connector's single-pass frame parser.
/// Boxed so the trait below can stay object-safe across client types.
pub type BookCallback = Box<dyn FnMut(f64, f64, u64) + Send + 'static>;

/// WebSocket / streaming source of top-of-book updates.
///
/// Implementations should loop internally with their own reconnect backoff.
/// `run_with` returns when the stream is permanently closed or reconnect
/// retries are exhausted.
#[async_trait]
pub trait PriceStream: Send + 'static {
    fn label(&self) -> &'static str;
    async fn run_with(&mut self, cb: BookCallback) -> Result<()>;
}

/// REST poller. Called at a fixed interval; returns one snapshot per call.
#[async_trait]
pub trait PricePoll: Send + Sync + 'static {
    fn label(&self) -> &'static str;
    async fn fetch(&self) -> Result<Option<(f64, f64)>>;
}

/// Generic stream service: owns a `PriceStream`, pushes every update into a
/// shared `SharedQuote`. Spawn one `tokio::spawn(service.run())`.
pub struct PriceStreamService<C: PriceStream> {
    pub client: C,
    pub prices: Arc<SharedQuote>,
}

impl<C: PriceStream> PriceStreamService<C> {
    pub async fn run(mut self) {
        let label = self.client.label();
        info!("[{}] Starting price stream", label);
        let prices = self.prices.clone();
        let cb: BookCallback = Box::new(move |bid, ask, ts| {
            prices.store_with_source(bid, ask, ts, QuoteSource::Stream);
        });
        if let Err(e) = self.client.run_with(cb).await {
            error!("[{}] Price stream exited: {}", label, e);
        }
    }
}

/// Generic poll service: invokes `PricePoll::fetch` every `interval_secs`
/// and writes successful snapshots to `SharedQuote`.
pub struct PricePollService<P: PricePoll> {
    pub source: Arc<P>,
    pub prices: Arc<SharedQuote>,
    pub interval_secs: u64,
}

impl<P: PricePoll> PricePollService<P> {
    pub async fn run(self) {
        let mut ticker = interval(Duration::from_secs(self.interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let label = self.source.label();
        loop {
            ticker.tick().await;
            match self.source.fetch().await {
                Ok(Some((bid, ask))) => {
                    self.prices
                        .store_with_source(bid, ask, 0, QuoteSource::Rest);
                    debug!(
                        "[{}] Updated prices via REST: bid=${:.4}, ask=${:.4}",
                        label, bid, ask
                    );
                }
                Ok(None) => {
                    debug!("[{}] No bid/ask available", label);
                }
                Err(e) => {
                    debug!("[{}] Fetch failed: {}", label, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockPoll {
        queue: Mutex<Vec<Option<(f64, f64)>>>,
    }

    #[async_trait]
    impl PricePoll for MockPoll {
        fn label(&self) -> &'static str {
            "MOCK"
        }
        async fn fetch(&self) -> Result<Option<(f64, f64)>> {
            let mut q = self.queue.lock().unwrap();
            Ok(if q.is_empty() { None } else { q.remove(0) })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn poll_service_writes_to_shared_quote() {
        let prices = SharedQuote::empty();
        let source = Arc::new(MockPoll {
            queue: Mutex::new(vec![Some((100.0, 100.1)), Some((101.0, 101.2))]),
        });

        let service = PricePollService {
            source: source.clone(),
            prices: prices.clone(),
            interval_secs: 1,
        };

        // Service loops forever; spawn and give it a moment to drain two snapshots.
        let handle = tokio::spawn(async move { service.run().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // First tick fires immediately; wait enough for 1-2 intervals in a short bench
        // (we tolerate 1s interval here; alternative: shorten but that's brittle).
        // Flag the timing: we check `is_populated` rather than exact values since
        // the mock exhausts and returns None afterwards.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(prices.is_populated());
        handle.abort();
    }
}
