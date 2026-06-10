use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use fast_float::parse;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::bot::{BotState, RunState};
use crate::config::Config;
use crate::connector::hyperliquid::HyperliquidTrading;
use crate::connector::pacifica::PacificaTrading;
use crate::market_rules::{fallback_rules, is_dust_or_below_min};
use crate::services::fill_aggregator::FillAggregator;
use crate::services::{
    enqueue_hedge_intent, HedgeEnqueueResult, HedgeIntent, HedgeSource, HedgeVenueSide,
};
use crate::strategy::OrderSide;

/// Slow safety loop that reconciles net exposure across exchanges.
pub struct PositionReconcilerService {
    pub bot_state: Arc<RwLock<BotState>>,
    pub hedge_tx: mpsc::Sender<HedgeIntent>,
    pub pacifica_trading: Arc<PacificaTrading>,
    pub hyperliquid_trading: Arc<HyperliquidTrading>,
    pub fill_aggregator: Arc<FillAggregator>,
    pub config: Config,
}

impl PositionReconcilerService {
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(Duration::from_millis(1000));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let seq = AtomicU64::new(0);
        // `unhedged_since` measures *continuous, non-decreasing* unhedged exposure
        // age; it is reset whenever exposure shrinks (progress) so a slow-but-
        // working hedge is not punished toward the hard-error ceiling.
        let mut unhedged_since: Option<Instant> = None;
        // Separate, non-resettable clock for the hard terminal stop: rearmed ONLY
        // when exposure genuinely returns to neutral/dust, never on transient
        // progress, so a sawtooth shrinking exposure cannot starve the safety stop.
        let mut hard_unhedged_since: Option<Instant> = None;
        let mut last_enqueued: Option<(f64, Instant)> = None;
        let mut prev_eff_net: Option<f64> = None;
        // L11: require consecutive neutral reads before completing the cycle, so a
        // single transiently-neutral REST sample cannot zero a live position.
        let mut neutral_confirmations: u32 = 0;

        loop {
            ticker.tick().await;
            let run_state = {
                let state = self.bot_state.read();
                state.get_run_state()
            };

            if run_state == RunState::Complete {
                continue;
            }
            if run_state == RunState::Error {
                debug!("[RECONCILER] State is Error; checking exposure for diagnostics");
            }

            let pac_pos = match self.pacifica_position().await {
                Ok(pos) => pos,
                Err(e) => {
                    debug!("[RECONCILER] Pacifica position fetch failed: {}", e);
                    continue;
                }
            };

            let hl_pos = match self.hyperliquid_position().await {
                Ok(pos) => pos,
                Err(e) => {
                    debug!("[RECONCILER] Hyperliquid position fetch failed: {}", e);
                    continue;
                }
            };

            let rules = fallback_rules(&self.config.symbol);
            // L1: a residual below the exchange minimum order size cannot be
            // neutralized on-venue, so treat anything within max(dust, min_size)
            // as effectively neutral for control flow.
            let effective_dust = self.config.neutral_dust_base.max(rules.min_size);

            let net = pac_pos + hl_pos;
            if net.abs() <= effective_dust {
                unhedged_since = None;
                hard_unhedged_since = None;
                last_enqueued = None;
                prev_eff_net = None;
                neutral_confirmations = neutral_confirmations.saturating_add(1);
                if neutral_confirmations >= 2 {
                    // Venue truth confirms neutrality across two reads: any
                    // unknown-settled hedge quantity is resolved (it either
                    // filled or this loop netted it), so retire the quarantine
                    // and let the aggregator GC those entries.
                    self.fill_aggregator.resolve_unknowns_on_neutral();
                }
                let can_complete = {
                    let state = self.bot_state.read();
                    state.active_order.is_none()
                        // Two successive neutral ticks guard against a transiently
                        // neutral sample zeroing a live position.
                        && neutral_confirmations >= 2
                        && matches!(
                            run_state,
                            RunState::Reconciling | RunState::Hedging | RunState::Filled
                        )
                };
                if can_complete {
                    match self.pacifica_trading.get_open_orders().await {
                        Ok(orders)
                            if !orders
                                .iter()
                                .any(|order| order.symbol == self.config.symbol) =>
                        {
                            let mut state = self.bot_state.write();
                            if state.active_order.is_none() {
                                info!(
                                    "[RECONCILER] Net exposure is neutral and no Pacifica orders remain; marking cycle complete"
                                );
                                state.mark_cycle_complete_and_idle();
                            }
                        }
                        Ok(_) => {
                            debug!(
                                "[RECONCILER] Net exposure is neutral but Pacifica still has open orders"
                            );
                        }
                        Err(e) => {
                            debug!(
                                "[RECONCILER] Net exposure is neutral but open-order check failed: {}",
                                e
                            );
                        }
                    }
                }
                continue;
            }
            // Net is not neutral this tick: any prior neutral streak is broken.
            neutral_confirmations = 0;

            // NOTE: we intentionally do NOT early-`continue` on `RunState::Hedging`.
            // Skipping the whole tick there would also skip the hard-error timer, so
            // a wedged Hedging state with real exposure would never escalate. The
            // duplicate-hedge race is instead prevented by the pending-aware check
            // below (`effective_net = net - total_pending_qty()`), since the primary
            // hedge reserves its quantity in the aggregator before/while executing.
            let mid_price = self.estimate_mid_price().await.unwrap_or(0.0);

            // M2: subtract hedge quantity already reserved/in-flight by the primary
            // hedge path so the reconciler reasons only about genuinely UNCOVERED
            // exposure. This both prevents racing an in-flight hedge AND prevents a
            // normal in-flight hedge (e.g. a $20 fill vs a $10 max_unhedged_usd) from
            // tripping the error caps before the hedge has had a chance to land.
            // Subtract only same-direction in-flight hedge coverage: a net-long
            // (net > 0) exposure is reduced by hedges reserved for Buy-side maker
            // fills, a net-short by Sell-side. Using the side-filtered pending (not
            // the side-agnostic total) avoids an opposite-side reservation ever
            // masking real exposure.
            let maker_side = if net > 0.0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let pending = self.fill_aggregator.pending_qty_for_side(maker_side);
            let abs_net = net.abs();
            let effective_net = (abs_net - pending).max(0.0);

            // Covered by an in-flight hedge: nothing to do, and not "unhedged" for
            // the hard-error timer.
            if effective_net <= effective_dust {
                unhedged_since = None;
                hard_unhedged_since = None;
                last_enqueued = None;
                prev_eff_net = None;
                debug!(
                    "[RECONCILER] Net {} is covered by in-flight hedge (pending {}); deferring",
                    net, pending
                );
                continue;
            }

            // L1: unhedgeable sub-min residual. Surface it but do NOT escalate to a
            // terminal error or enqueue a doomed order; reset the timer so it does
            // not accumulate toward the hard limit.
            if is_dust_or_below_min(effective_net, mid_price, rules, effective_dust) {
                warn!(
                    "[RECONCILER] Uncovered exposure {} (net {}) is below {} exchange min/min-notional; cannot auto-hedge (operator action may be required)",
                    effective_net, net, self.config.symbol
                );
                unhedged_since = None;
                hard_unhedged_since = None;
                last_enqueued = None;
                prev_eff_net = None;
                if !matches!(run_state, RunState::Error) {
                    self.bot_state.write().mark_reconciling();
                }
                continue;
            }

            // M3: reset the hard-error clock whenever UNCOVERED exposure shrinks
            // (progress), so a slow-but-working hedge is not punished for latency.
            if let Some(prev) = prev_eff_net {
                if effective_net + effective_dust < prev {
                    unhedged_since = Some(Instant::now());
                }
            }
            prev_eff_net = Some(effective_net);

            let first_seen = *unhedged_since.get_or_insert_with(Instant::now);
            let unhedged_for = first_seen.elapsed();
            // Hard clock: measures CONTINUOUS exposure age; only the neutral/dust
            // reset sites clear it, so transient progress cannot rearm it.
            let hard_first_seen = *hard_unhedged_since.get_or_insert_with(Instant::now);
            let hard_unhedged_for = hard_first_seen.elapsed();
            let usd_exposure = if mid_price > 0.0 {
                Some(effective_net * mid_price)
            } else {
                None
            };
            let limit_breach =
                self.exposure_limit_breach(effective_net, usd_exposure, hard_unhedged_for);
            {
                let mut state = self.bot_state.write();
                if limit_breach {
                    state.set_error(format!(
                        "Unhedged exposure limit breached: net_base={:.8}, uncovered_base={:.8}, net_usd={}, age_ms={}",
                        net,
                        effective_net,
                        usd_exposure
                            .map(|value| format!("{:.4}", value))
                            .unwrap_or_else(|| "unknown".to_string()),
                        unhedged_for.as_millis()
                    ));
                } else if !matches!(run_state, RunState::Error) {
                    state.mark_reconciling();
                }
            }

            // M2: hold off until the primary hedge path has had a grace window to land.
            if unhedged_for < Duration::from_millis(self.config.reconciler_grace_ms) {
                debug!(
                    "[RECONCILER] Uncovered exposure {} within reconciler grace ({}ms); deferring",
                    effective_net, self.config.reconciler_grace_ms
                );
                continue;
            }

            let retry_after = Duration::from_millis(self.config.max_unhedged_ms);
            if !Self::should_enqueue_residual(last_enqueued, effective_net, effective_dust, retry_after)
            {
                debug!(
                    "[RECONCILER] Uncovered exposure {} remains pending from a recent hedge intent",
                    effective_net
                );
                continue;
            }

            let hedge_side = if net > 0.0 {
                HedgeVenueSide::Sell
            } else {
                HedgeVenueSide::Buy
            };
            let hedge_seq = seq.fetch_add(1, Ordering::AcqRel);
            let intent = HedgeIntent::from_venue_side(
                chrono::Utc::now().timestamp_millis().max(0) as u64,
                hedge_seq,
                HedgeSource::Reconciler,
                hedge_side,
                effective_net,
                0.0,
                false,
            );

            warn!(
                "[RECONCILER] Net exposure {} exceeds dust {} (pending {}); queued residual hedge {}",
                net, effective_dust, pending, intent.size
            );
            match enqueue_hedge_intent(&self.hedge_tx, &self.bot_state, intent).await {
                Ok(HedgeEnqueueResult::Queued) => {
                    last_enqueued = Some((effective_net, Instant::now()));
                }
                Ok(HedgeEnqueueResult::PersistedButNotQueued { reason }) => {
                    warn!("[RECONCILER] Residual hedge was not queued: {}", reason);
                }
                Err(e) => {
                    warn!("[RECONCILER] Failed to enqueue residual hedge: {}", e);
                }
            }
        }
    }

    fn exposure_limit_breach(
        &self,
        abs_base: f64,
        usd_exposure: Option<f64>,
        hard_unhedged_for: Duration,
    ) -> bool {
        // base/USD ceilings escalate immediately; the time ceiling uses the
        // non-resettable hard clock so a sawtooth exposure cannot starve it.
        (self.config.max_unhedged_base > 0.0 && abs_base > self.config.max_unhedged_base)
            || usd_exposure
                .map(|usd| self.config.max_unhedged_usd > 0.0 && usd > self.config.max_unhedged_usd)
                .unwrap_or(false)
            || hard_unhedged_for >= Duration::from_millis(self.config.max_unhedged_hard_ms)
    }

    fn should_enqueue_residual(
        last_enqueued: Option<(f64, Instant)>,
        net: f64,
        dust: f64,
        retry_after: Duration,
    ) -> bool {
        let Some((last_net, last_at)) = last_enqueued else {
            return true;
        };
        (net - last_net).abs() > dust || last_at.elapsed() >= retry_after
    }

    async fn estimate_mid_price(&self) -> Option<f64> {
        let Ok(Some((bid, ask))) = self
            .hyperliquid_trading
            .get_l2_snapshot(&self.config.symbol)
            .await
        else {
            return None;
        };
        if bid <= 0.0 || ask <= 0.0 {
            return None;
        }
        Some((bid + ask) / 2.0)
    }

    async fn pacifica_position(&self) -> anyhow::Result<f64> {
        let positions = self.pacifica_trading.get_positions().await?;
        let Some(pos) = positions.iter().find(|p| p.symbol == self.config.symbol) else {
            return Ok(0.0);
        };
        let amount: f64 = parse(&pos.amount).unwrap_or(0.0);
        Ok(match pos.side.as_str() {
            "bid" => amount,
            "ask" => -amount,
            _ => 0.0,
        })
    }

    async fn hyperliquid_position(&self) -> anyhow::Result<f64> {
        let wallet = self.hyperliquid_trading.account_address();
        let user_state = self.hyperliquid_trading.get_user_state(&wallet).await?;
        let Some(pos) = user_state
            .asset_positions
            .iter()
            .find(|ap| ap.position.coin == self.config.symbol)
        else {
            return Ok(0.0);
        };
        Ok(parse(&pos.position.szi).unwrap_or(0.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_same_residual_is_not_reenqueued() {
        let now = Instant::now();
        assert!(!PositionReconcilerService::should_enqueue_residual(
            Some((0.5, now)),
            0.505,
            0.01,
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn changed_or_stale_residual_is_reenqueued() {
        let now = Instant::now();
        assert!(PositionReconcilerService::should_enqueue_residual(
            Some((0.5, now)),
            0.7,
            0.01,
            Duration::from_secs(5),
        ));
        assert!(PositionReconcilerService::should_enqueue_residual(
            Some((0.5, now - Duration::from_secs(10))),
            0.5,
            0.01,
            Duration::from_secs(5),
        ));
    }

    #[test]
    fn neutral_double_confirmation_resolves_unknown_quarantine() {
        use crate::services::fill_aggregator::HedgeSettlement;

        let agg = FillAggregator::new(1000.0);
        let d = agg.on_fill(1, OrderSide::Buy, 1.0, 100.0, true).unwrap();
        agg.settle_hedge(HedgeSettlement::unknown(1, d.size, 0.0));
        assert!(agg.snapshot(1).unwrap().unverified_unknown_qty > 0.0);

        // Mirrors the reconciler's neutral branch after two confirmations.
        agg.resolve_unknowns_on_neutral();
        let state = agg.snapshot(1).unwrap();
        assert!(state.unverified_unknown_qty.abs() < 1e-9);
        assert!((state.cumulative_hedged_confirmed - 1.0).abs() < 1e-9);
    }
}
