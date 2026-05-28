use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use crate::services::trade_gate::{GateReason, TradeGate};

/// Fail-CLOSED supervisor.
///
/// For tasks that own a non-clonable resource (e.g. an mpsc `Receiver`) and
/// therefore cannot be re-created from clones. On exit/panic it latches
/// `GateReason::ServiceDown` permanently, halting new maker placement until the
/// process is restarted (the supported recovery path is `run_bot_loop_cargo.sh`).
pub fn spawn_supervised_fail_closed<F>(name: &'static str, trade_gate: Arc<TradeGate>, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let handle = tokio::spawn(future);
        match handle.await {
            Ok(()) => warn!("[SUPERVISOR] task '{}' exited (fail-closed)", name),
            Err(e) => error!("[SUPERVISOR] task '{}' panicked (fail-closed): {}", name, e),
        }
        trade_gate.block(GateReason::ServiceDown);
    });
}

/// Tuning for the restarting supervisor.
#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_restarts_in_window: u32,
    pub window: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
            max_restarts_in_window: 8,
            window: Duration::from_secs(60),
        }
    }
}

impl RestartPolicy {
    /// Bounded exponential backoff: `base * 2^min(consecutive, 7)`, capped at
    /// `max_delay`. The shift is clamped so it cannot overflow.
    pub fn backoff(&self, consecutive: u32) -> Duration {
        let shift = consecutive.min(7);
        self.base_delay
            .saturating_mul(1u32 << shift)
            .min(self.max_delay)
    }
}

/// Fail-OPEN (self-healing) supervisor.
///
/// For tasks reconstructable from cheap clones (`Arc`/`Config`/clonable config).
/// `factory` produces a fresh future each incarnation. On exit or panic we log,
/// back off, and re-invoke the factory.
///
/// `GateReason::ServiceDown` is treated as *transient* here: it is blocked while
/// the task is not running (during the backoff gap) and cleared right before the
/// next incarnation starts. This is safe because price/fill health is independently
/// gated by `QuoteStale`/`PriceWsDown`/`FillWsDown`. If the task crash-loops past
/// `max_restarts_in_window`, `ServiceDown` is latched (while still retrying slowly)
/// until an incarnation runs longer than `window` (the "recovered" signal).
pub fn spawn_supervised_with_factory<Fut, Mk>(
    name: &'static str,
    trade_gate: Arc<TradeGate>,
    policy: RestartPolicy,
    mut factory: Mk,
) where
    Fut: Future<Output = ()> + Send + 'static,
    Mk: FnMut() -> Fut + Send + 'static,
{
    tokio::spawn(async move {
        let mut consecutive: u32 = 0;
        let mut window_start = Instant::now();
        let mut restarts_in_window: u32 = 0;
        let mut latched = false;

        loop {
            let started = Instant::now();
            let handle = tokio::spawn(factory());
            match handle.await {
                Ok(()) => warn!("[SUPERVISOR] task '{}' exited; restarting", name),
                Err(e) => error!("[SUPERVISOR] task '{}' panicked; restarting: {}", name, e),
            }

            // A long-lived incarnation is the "recovered" signal: reset counters
            // and clear any latch.
            if started.elapsed() >= policy.window {
                consecutive = 0;
                restarts_in_window = 0;
                window_start = Instant::now();
                if latched {
                    latched = false;
                    trade_gate.allow(GateReason::ServiceDown);
                }
            }

            // Rolling-window crash-loop accounting.
            if window_start.elapsed() > policy.window {
                window_start = Instant::now();
                restarts_in_window = 0;
            }
            restarts_in_window += 1;
            if restarts_in_window > policy.max_restarts_in_window && !latched {
                error!(
                    "[SUPERVISOR] task '{}' exceeded {} restarts in {:?}; latching ServiceDown",
                    name, policy.max_restarts_in_window, policy.window
                );
                latched = true;
            }

            // Bounded exponential backoff. Mark down while not running.
            let delay = policy.backoff(consecutive);
            consecutive = consecutive.saturating_add(1);
            trade_gate.block(GateReason::ServiceDown);
            tokio::time::sleep(delay).await;

            info!("[SUPERVISOR] restarting task '{}' after {:?}", name, delay);
            // Clear the transient down-flag before re-spawning unless we are in a
            // crash-loop latch (then it stays blocked until a stable run clears it).
            if !latched {
                trade_gate.allow(GateReason::ServiceDown);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let policy = RestartPolicy {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            max_restarts_in_window: 8,
            window: Duration::from_secs(60),
        };
        assert_eq!(policy.backoff(0), Duration::from_millis(100));
        assert_eq!(policy.backoff(1), Duration::from_millis(200));
        assert_eq!(policy.backoff(2), Duration::from_millis(400));
        // Capped at max_delay and never overflows for large counters.
        assert_eq!(policy.backoff(20), Duration::from_secs(5));
        assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(5));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn factory_restarts_after_panic_and_clears_service_down() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let gate = TradeGate::new();
        gate.block(GateReason::ServiceDown);
        let runs = Arc::new(AtomicU32::new(0));

        {
            let runs = runs.clone();
            spawn_supervised_with_factory(
                "test_task",
                gate.clone(),
                RestartPolicy {
                    base_delay: Duration::from_millis(10),
                    max_delay: Duration::from_millis(50),
                    max_restarts_in_window: 8,
                    window: Duration::from_millis(200),
                },
                move || {
                    let runs = runs.clone();
                    async move {
                        let n = runs.fetch_add(1, Ordering::AcqRel);
                        if n == 0 {
                            panic!("first incarnation fails");
                        }
                        // Second+ incarnation: run "forever".
                        futures_util::future::pending::<()>().await;
                    }
                },
            );
        }

        // Let the first incarnation panic and the supervisor back off + restart.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            runs.load(Ordering::Acquire) >= 2,
            "task should have restarted after panic"
        );
        // A healthy restarted task clears the transient ServiceDown gate.
        assert!(!gate.is_blocked(GateReason::ServiceDown));
    }
}
