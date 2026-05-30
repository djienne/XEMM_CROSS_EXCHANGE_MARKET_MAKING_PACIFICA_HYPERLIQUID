use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::bot::RunState;
use crate::util::price::{now_ns, quote_usable, QuoteSnapshot, QuoteSource};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GateReason {
    OpenOrderUnknown = 0,
    OpenOrderExists = 1,
    PositionUnknown = 2,
    NetExposure = 3,
    QuoteStale = 4,
    FillWsDown = 5,
    PriceWsDown = 6,
    PlacementUnknown = 7,
    CancelPending = 8,
    HedgePending = 9,
    HedgeUnknown = 10,
    Reconciling = 11,
    ShuttingDown = 12,
    LoggerBackpressure = 13,
    ServiceDown = 14,
}

impl GateReason {
    #[inline]
    pub const fn bit(self) -> u64 {
        1u64 << (self as u8)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenOrderUnknown => "open_order_unknown",
            Self::OpenOrderExists => "open_order_exists",
            Self::PositionUnknown => "position_unknown",
            Self::NetExposure => "net_exposure",
            Self::QuoteStale => "quote_stale",
            Self::FillWsDown => "fill_ws_down",
            Self::PriceWsDown => "price_ws_down",
            Self::PlacementUnknown => "placement_unknown",
            Self::CancelPending => "cancel_pending",
            Self::HedgePending => "hedge_pending",
            Self::HedgeUnknown => "hedge_unknown",
            Self::Reconciling => "reconciling",
            Self::ShuttingDown => "shutting_down",
            Self::LoggerBackpressure => "logger_backpressure",
            Self::ServiceDown => "service_down",
        }
    }
}

const INITIAL_REASONS: u64 = GateReason::OpenOrderUnknown.bit()
    | GateReason::PositionUnknown.bit()
    | GateReason::QuoteStale.bit()
    | GateReason::FillWsDown.bit()
    | GateReason::PriceWsDown.bit();

#[derive(Debug)]
pub struct TradeGate {
    reasons: AtomicU64,
    pub last_cancel_ns: AtomicU64,
    /// Count of restartable supervised tasks currently down (in backoff). The
    /// `ServiceDown` reason bit is held while this is > 0. Refcounted so one
    /// task coming back up does not clear another's down state.
    service_down: AtomicU32,
    /// Set permanently when a fail-closed task (one owning a non-recreatable
    /// resource) dies. Keeps `ServiceDown` latched regardless of `service_down`,
    /// so a restartable task's recovery can never silently un-halt the bot.
    service_down_latched: AtomicBool,
}

impl TradeGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            reasons: AtomicU64::new(INITIAL_REASONS),
            last_cancel_ns: AtomicU64::new(0),
            service_down: AtomicU32::new(0),
            service_down_latched: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn block(&self, reason: GateReason) {
        self.reasons.fetch_or(reason.bit(), Ordering::AcqRel);
    }

    #[inline]
    pub fn allow(&self, reason: GateReason) {
        self.reasons.fetch_and(!reason.bit(), Ordering::AcqRel);
    }

    #[inline]
    pub fn set(&self, reason: GateReason, blocked: bool) {
        if blocked {
            self.block(reason);
        } else {
            self.allow(reason);
        }
    }

    #[inline]
    pub fn reasons_bits(&self) -> u64 {
        self.reasons.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_blocked(&self, reason: GateReason) -> bool {
        // The fail-closed latch is authoritative for ServiceDown: once set it can
        // never be cleared by `mark_service_up`'s bit manipulation, closing the
        // race where a restartable recovery clears the bit while the latch is set.
        if reason == GateReason::ServiceDown
            && self.service_down_latched.load(Ordering::Acquire)
        {
            return true;
        }
        self.reasons_bits() & reason.bit() != 0
    }

    #[inline]
    pub fn mark_cancel_now(&self) {
        self.last_cancel_ns.store(now_ns(), Ordering::Release);
    }

    /// A restartable supervised task has gone down (entering backoff). Holds the
    /// `ServiceDown` gate while at least one such task is down.
    pub fn mark_service_down(&self) {
        self.service_down.fetch_add(1, Ordering::AcqRel);
        self.block(GateReason::ServiceDown);
    }

    /// A restartable supervised task has come back up. Clears `ServiceDown` only
    /// when no restartable task remains down AND no fail-closed latch is set.
    pub fn mark_service_up(&self) {
        // Saturating so concurrent decrements can never underflow.
        let _ = self.service_down.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |c| Some(c.saturating_sub(1)),
        );
        if self.service_down.load(Ordering::Acquire) == 0
            && !self.service_down_latched.load(Ordering::Acquire)
        {
            self.allow(GateReason::ServiceDown);
            // Recheck: a concurrent `mark_service_down` may have bumped the count
            // and set the bit between our load and our `allow`, which would leave
            // the bit clear while a task is actually down. Re-assert it if so. The
            // count is the source of truth; this keeps the mirrored bit coherent.
            // Also re-assert from the latch in case a concurrent
            // `latch_service_down` set it between our checks (defense in depth; the
            // read path already treats the latch as authoritative).
            if self.service_down.load(Ordering::Acquire) != 0
                || self.service_down_latched.load(Ordering::Acquire)
            {
                self.block(GateReason::ServiceDown);
            }
        }
    }

    /// A fail-closed task (owns a non-recreatable resource such as an mpsc
    /// Receiver) has died. Latch `ServiceDown` permanently — only a process
    /// restart recovers. A restartable task's `mark_service_up` will not clear it.
    pub fn latch_service_down(&self) {
        self.service_down_latched.store(true, Ordering::Release);
        self.block(GateReason::ServiceDown);
    }

    #[inline]
    pub fn cancel_grace_elapsed(&self, grace: Duration) -> bool {
        let last = self.last_cancel_ns.load(Ordering::Acquire);
        if last == 0 {
            return true;
        }
        now_ns().saturating_sub(last) >= grace.as_nanos().min(u64::MAX as u128) as u64
    }

    pub fn update_quote_health(
        &self,
        pacifica: Option<QuoteSnapshot>,
        hyperliquid: Option<QuoteSnapshot>,
        max_quote_age: Duration,
    ) {
        let now = now_ns();
        let max_age_ns = max_quote_age.as_nanos().min(u64::MAX as u128) as u64;
        let pac_usable = pacifica
            .map(|q| quote_usable(q, now, max_age_ns))
            .unwrap_or(false);
        let hl_usable = hyperliquid
            .map(|q| quote_usable(q, now, max_age_ns))
            .unwrap_or(false);
        self.set(GateReason::QuoteStale, !(pac_usable && hl_usable));

        let pac_stream = pacifica
            .map(|q| q.source == QuoteSource::Stream)
            .unwrap_or(false);
        let hl_stream = hyperliquid
            .map(|q| q.source == QuoteSource::Stream)
            .unwrap_or(false);
        self.set(GateReason::PriceWsDown, !(pac_stream && hl_stream));
    }

    pub fn update_from_run_state(&self, state: RunState) {
        self.set(GateReason::Reconciling, state == RunState::Reconciling);
        self.set(
            GateReason::PlacementUnknown,
            state == RunState::PlacementUnknown,
        );
        self.set(
            GateReason::CancelPending,
            matches!(state, RunState::Cancelling | RunState::CancelPending),
        );
        self.set(GateReason::HedgePending, state == RunState::Hedging);
        self.set(GateReason::HedgeUnknown, state == RunState::HedgeUnknown);
        self.set(GateReason::ShuttingDown, state == RunState::ShuttingDown);
    }

    pub fn allow_quote(
        &self,
        state: RunState,
        pacifica: Option<QuoteSnapshot>,
        hyperliquid: Option<QuoteSnapshot>,
        max_quote_age: Duration,
        cancel_grace: Duration,
    ) -> bool {
        self.update_from_run_state(state);
        self.update_quote_health(pacifica, hyperliquid, max_quote_age);

        if state != RunState::Idle {
            return false;
        }
        if !self.cancel_grace_elapsed(cancel_grace) {
            return false;
        }
        // The set-once fail-closed latch is a hard gate independent of the
        // mirrored ServiceDown bit, so a concurrent `mark_service_up` clearing the
        // bit can never un-halt quoting after a fail-closed task has died.
        self.reasons_bits() == 0 && !self.service_down_latched.load(Ordering::Acquire)
    }

    pub fn describe(&self) -> Vec<&'static str> {
        let bits = self.reasons_bits();
        [
            GateReason::OpenOrderUnknown,
            GateReason::OpenOrderExists,
            GateReason::PositionUnknown,
            GateReason::NetExposure,
            GateReason::QuoteStale,
            GateReason::FillWsDown,
            GateReason::PriceWsDown,
            GateReason::PlacementUnknown,
            GateReason::CancelPending,
            GateReason::HedgePending,
            GateReason::HedgeUnknown,
            GateReason::Reconciling,
            GateReason::ShuttingDown,
            GateReason::LoggerBackpressure,
            GateReason::ServiceDown,
        ]
        .iter()
        .copied()
        .filter(|reason| bits & reason.bit() != 0)
        .map(GateReason::as_str)
        .collect()
    }
}

impl Default for TradeGate {
    fn default() -> Self {
        Self {
            reasons: AtomicU64::new(INITIAL_REASONS),
            last_cancel_ns: AtomicU64::new(0),
            service_down: AtomicU32::new(0),
            service_down_latched: AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::price::QuoteSource;

    fn quote(source: QuoteSource) -> QuoteSnapshot {
        QuoteSnapshot {
            bid: 100.0,
            ask: 100.1,
            exchange_ts_ms: 0,
            recv_ns: now_ns(),
            source,
            seq: 2,
        }
    }

    #[test]
    fn new_gate_is_fail_closed() {
        let gate = TradeGate::new();
        assert!(gate.is_blocked(GateReason::OpenOrderUnknown));
        assert!(!gate.allow_quote(
            RunState::Idle,
            Some(quote(QuoteSource::Stream)),
            Some(quote(QuoteSource::Stream)),
            Duration::from_millis(500),
            Duration::from_secs(0),
        ));
    }

    #[test]
    fn quote_requires_fresh_stream_quotes_and_no_reasons() {
        let gate = TradeGate::new();
        for reason in [
            GateReason::OpenOrderUnknown,
            GateReason::PositionUnknown,
            GateReason::QuoteStale,
            GateReason::FillWsDown,
            GateReason::PriceWsDown,
        ] {
            gate.allow(reason);
        }
        assert!(gate.allow_quote(
            RunState::Idle,
            Some(quote(QuoteSource::Stream)),
            Some(quote(QuoteSource::Stream)),
            Duration::from_millis(500),
            Duration::from_secs(0),
        ));
        assert!(!gate.allow_quote(
            RunState::Idle,
            Some(quote(QuoteSource::Rest)),
            Some(quote(QuoteSource::Stream)),
            Duration::from_millis(500),
            Duration::from_secs(0),
        ));
    }

    #[test]
    fn latch_is_authoritative_even_if_bit_cleared() {
        // Simulate the lost-update race outcome directly: latch set, but the
        // mirrored ServiceDown bit cleared and the count at zero (as a racing
        // mark_service_up could leave it). The read path must still halt.
        let gate = TradeGate::new();
        gate.latch_service_down();
        // Forcibly clear the bit and zero the count, mimicking the race result.
        gate.allow(GateReason::ServiceDown);
        assert_eq!(gate.service_down.load(Ordering::Acquire), 0);
        assert_eq!(gate.reasons_bits() & GateReason::ServiceDown.bit(), 0);

        assert!(
            gate.is_blocked(GateReason::ServiceDown),
            "latch must force ServiceDown blocked regardless of the bit"
        );
        // Clear the remaining initial reasons so only the latch could gate.
        for reason in [
            GateReason::OpenOrderUnknown,
            GateReason::PositionUnknown,
            GateReason::QuoteStale,
            GateReason::FillWsDown,
            GateReason::PriceWsDown,
        ] {
            gate.allow(reason);
        }
        assert!(
            !gate.allow_quote(
                RunState::Idle,
                Some(quote(QuoteSource::Stream)),
                Some(quote(QuoteSource::Stream)),
                Duration::from_millis(500),
                Duration::from_secs(0),
            ),
            "quoting must stay halted while the fail-closed latch is set"
        );
    }

    #[test]
    fn concurrent_latch_and_service_up_keeps_gate_blocked() {
        use std::sync::Arc;
        use std::thread;
        // Stress the actual race: a fail-closed task latching down concurrently
        // with a restartable task's recovery must never leave quoting allowed.
        for _ in 0..2_000 {
            let gate = TradeGate::new();
            for reason in [
                GateReason::OpenOrderUnknown,
                GateReason::PositionUnknown,
                GateReason::QuoteStale,
                GateReason::FillWsDown,
                GateReason::PriceWsDown,
            ] {
                gate.allow(reason);
            }
            // One restartable task is down (count=1, bit set).
            gate.mark_service_down();

            let g1 = Arc::clone(&gate);
            let g2 = Arc::clone(&gate);
            let t1 = thread::spawn(move || g1.mark_service_up());
            let t2 = thread::spawn(move || g2.latch_service_down());
            t1.join().unwrap();
            t2.join().unwrap();

            assert!(
                gate.is_blocked(GateReason::ServiceDown),
                "ServiceDown must remain blocked after a latch races a recovery"
            );
        }
    }
}
