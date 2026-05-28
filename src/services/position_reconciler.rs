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
        let mut last_enqueued: Option<(f64, Instant)> = None;
        let mut prev_abs_net: Option<f64> = None;

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
                last_enqueued = None;
                prev_abs_net = None;
                let can_complete = {
                    let state = self.bot_state.read();
                    state.active_order.is_none()
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

            // M2: while the primary hedge path is actively executing, do not race
            // it with a duplicate corrective hedge. The exposure timer keeps
            // running so a genuinely stuck hedge still escalates.
            if run_state == RunState::Hedging {
                debug!("[RECONCILER] Hedge in progress; deferring corrective hedge this tick");
                continue;
            }

            let mid_price = self.estimate_mid_price().await.unwrap_or(0.0);

            // L1: unhedgeable sub-min residual. Surface it but do NOT escalate to a
            // terminal error or enqueue a doomed order; reset the timer so it does
            // not accumulate toward the hard limit.
            if is_dust_or_below_min(net, mid_price, rules, effective_dust) {
                warn!(
                    "[RECONCILER] Net exposure {} is below {} exchange min/min-notional; cannot auto-hedge (operator action may be required)",
                    net, self.config.symbol
                );
                unhedged_since = None;
                last_enqueued = None;
                prev_abs_net = None;
                if !matches!(run_state, RunState::Error) {
                    self.bot_state.write().mark_reconciling();
                }
                continue;
            }

            // M3: reset the hard-error clock whenever exposure shrinks (progress),
            // so a slow-but-working hedge is not punished for latency.
            let abs_net = net.abs();
            if let Some(prev) = prev_abs_net {
                if abs_net + effective_dust < prev {
                    unhedged_since = Some(Instant::now());
                }
            }
            prev_abs_net = Some(abs_net);

            let first_seen = *unhedged_since.get_or_insert_with(Instant::now);
            let unhedged_for = first_seen.elapsed();
            let usd_exposure = if mid_price > 0.0 {
                Some(abs_net * mid_price)
            } else {
                None
            };
            let limit_breach = self.exposure_limit_breach(abs_net, usd_exposure, unhedged_for);
            {
                let mut state = self.bot_state.write();
                if limit_breach {
                    state.set_error(format!(
                        "Unhedged exposure limit breached: net_base={:.8}, net_usd={}, age_ms={}",
                        net,
                        usd_exposure
                            .map(|value| format!("{:.4}", value))
                            .unwrap_or_else(|| "unknown".to_string()),
                        unhedged_for.as_millis()
                    ));
                } else if !matches!(run_state, RunState::Error) {
                    state.mark_reconciling();
                }
            }

            // M2: subtract hedge quantity already reserved/in-flight by the primary
            // hedge path so we only correct the genuinely uncovered exposure.
            let pending = self.fill_aggregator.total_pending_qty();
            let effective_net = (abs_net - pending).max(0.0);
            if effective_net <= effective_dust {
                debug!(
                    "[RECONCILER] Net {} is covered by in-flight hedge (pending {}); not enqueuing",
                    net, pending
                );
                continue;
            }

            // M2: hold off until the primary hedge path has had a grace window to land.
            if unhedged_for < Duration::from_millis(self.config.reconciler_grace_ms) {
                debug!(
                    "[RECONCILER] Net exposure {} within reconciler grace ({}ms); deferring",
                    net, self.config.reconciler_grace_ms
                );
                continue;
            }

            let retry_after = Duration::from_millis(self.config.max_unhedged_ms);
            if !Self::should_enqueue_residual(last_enqueued, net, effective_dust, retry_after) {
                debug!(
                    "[RECONCILER] Net exposure {} remains pending from a recent hedge intent",
                    net
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
                    last_enqueued = Some((net, Instant::now()));
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
        unhedged_for: Duration,
    ) -> bool {
        // M3: base/USD ceilings escalate immediately; the time ceiling uses the
        // larger hard limit so a slow-but-progressing hedge stays in Reconciling.
        (self.config.max_unhedged_base > 0.0 && abs_base > self.config.max_unhedged_base)
            || usd_exposure
                .map(|usd| self.config.max_unhedged_usd > 0.0 && usd > self.config.max_unhedged_usd)
                .unwrap_or(false)
            || unhedged_for >= Duration::from_millis(self.config.max_unhedged_hard_ms)
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
}
