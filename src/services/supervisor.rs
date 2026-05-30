use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{error, info, warn};

use crate::services::trade_gate::TradeGate;

/// An incarnation that runs at least this long is treated as "healthy", which
/// resets the restart backoff.
const HEALTHY_UPTIME: Duration = Duration::from_secs(10);

/// Fail-CLOSED supervisor.
///
/// For tasks that own a non-clonable resource (e.g. an mpsc `Receiver`) and
/// therefore cannot be re-created from clones. On exit/panic it latches
/// `ServiceDown` permanently, halting new maker placement until the process is
/// restarted (the supported recovery path is `run_bot_loop_cargo.sh`). A
/// restartable task coming back up will NOT clear this latch.
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
        trade_gate.latch_service_down();
    });
}

/// Tuning for the restarting supervisor.
#[derive(Clone, Copy, Debug)]
pub struct RestartPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(30),
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
/// mark the task down on the trade gate (which blocks new placement while it is
/// down), back off, mark it up, and re-invoke the factory.
///
/// `ServiceDown` is refcounted on the gate, so this task's recovery never clears
/// a fail-closed task's latch, and two restartable tasks do not clear each
/// other. Downtime is bounded by the backoff; there is no permanent latch for a
/// restartable task (a chronically broken one simply keeps retrying every
/// `max_delay` while quoting stays blocked during each gap). A run that lasts at
/// least `HEALTHY_UPTIME` resets the backoff.
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
        loop {
            let started = Instant::now();
            // Run one incarnation under an inner spawn so a panic is catchable
            // (handle.await returns Err on panic) and triggers a restart.
            let handle = tokio::spawn(factory());
            match handle.await {
                Ok(()) => warn!("[SUPERVISOR] task '{}' exited; restarting", name),
                Err(e) => error!("[SUPERVISOR] task '{}' panicked; restarting: {}", name, e),
            }

            // Assert the gate the instant the incarnation is dead, before any
            // backoff bookkeeping, so there is no window where a dead task does not
            // hold ServiceDown.
            trade_gate.mark_service_down();

            // A sufficiently long-lived incarnation is the "healthy" signal:
            // reset the backoff so a venue that reconnects after hours does not
            // start at max_delay.
            if started.elapsed() >= HEALTHY_UPTIME {
                consecutive = 0;
            }

            let delay = policy.backoff(consecutive);
            consecutive = consecutive.saturating_add(1);

            // Block quoting while the task is down (through the backoff gap).
            tokio::time::sleep(delay).await;
            trade_gate.mark_service_up();
            info!("[SUPERVISOR] restarting task '{}' after {:?}", name, delay);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::trade_gate::GateReason;

    #[test]
    fn backoff_is_exponential_and_capped() {
        let policy = RestartPolicy {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        };
        assert_eq!(policy.backoff(0), Duration::from_millis(100));
        assert_eq!(policy.backoff(1), Duration::from_millis(200));
        assert_eq!(policy.backoff(2), Duration::from_millis(400));
        // Capped at max_delay and never overflows for large counters.
        assert_eq!(policy.backoff(20), Duration::from_secs(5));
        assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(5));
    }

    #[test]
    fn restartable_recovery_does_not_clear_failclosed_latch() {
        // A fail-closed task latches ServiceDown permanently.
        let gate = TradeGate::new();
        gate.latch_service_down();
        assert!(gate.is_blocked(GateReason::ServiceDown));

        // A restartable task going down then up must NOT clear the latch.
        gate.mark_service_down();
        gate.mark_service_up();
        assert!(
            gate.is_blocked(GateReason::ServiceDown),
            "fail-closed latch must survive a restartable task's recovery"
        );
    }

    #[test]
    fn service_down_is_refcounted_across_restartable_tasks() {
        let gate = TradeGate::new();
        gate.allow(GateReason::ServiceDown); // start clear
        // Two restartable tasks down.
        gate.mark_service_down();
        gate.mark_service_down();
        assert!(gate.is_blocked(GateReason::ServiceDown));
        // One recovers: still blocked because the other is down.
        gate.mark_service_up();
        assert!(gate.is_blocked(GateReason::ServiceDown));
        // Both recovered: cleared.
        gate.mark_service_up();
        assert!(!gate.is_blocked(GateReason::ServiceDown));
        // Extra mark_service_up must not underflow / spuriously toggle.
        gate.mark_service_up();
        assert!(!gate.is_blocked(GateReason::ServiceDown));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn factory_restarts_after_panic_and_clears_service_down() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let gate = TradeGate::new();
        gate.allow(GateReason::ServiceDown);
        let runs = Arc::new(AtomicU32::new(0));

        {
            let runs = runs.clone();
            spawn_supervised_with_factory(
                "test_task",
                gate.clone(),
                RestartPolicy {
                    base_delay: Duration::from_millis(10),
                    max_delay: Duration::from_millis(50),
                },
                move || {
                    let runs = runs.clone();
                    async move {
                        let n = runs.fetch_add(1, Ordering::AcqRel);
                        if n == 0 {
                            panic!("first incarnation fails");
                        }
                        futures_util::future::pending::<()>().await;
                    }
                },
            );
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            runs.load(Ordering::Acquire) >= 2,
            "task should have restarted after panic"
        );
        // The restarted (healthy) task is up, so ServiceDown is cleared.
        assert!(!gate.is_blocked(GateReason::ServiceDown));
    }
}
