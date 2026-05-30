# MEGA_PLAN_FIX_PLUS

Prioritized correctness / safety review of the XEMM Rust trading bot, with concrete fixes.

- **Date:** 2026-05-30
- **Scope:** `src/**` (the live trading bot, ~16K LOC) plus connectors, config, deploy scripts.
- **Method:** 10 dimension reviewers (concurrency, hedge pipeline, 5-layer fill detection, strategy/money-math, order lifecycle, Pacifica connector, Hyperliquid connector, app lifecycle, panics/robustness, config/secrets). Every candidate finding was then independently re-read by an adversarial verifier. **43 candidates → 39 confirmed, 4 refuted.**
- **Overall posture:** No deterministic, always-fires fund-loss bug. The codebase is already heavily hardened (`MEGA_FIX_PLAN_4/5/6`). What remains: (a) latent races / fail-open holes in the safety machinery, (b) brittle parsing that lets one malformed exchange frame crash the process or drop a fill, (c) edge-case fill/hedge accounting that can leave a position naked for seconds.

## Three recurring themes
1. **Fail-open safety gaps** — the `ServiceDown` latch, the unsupervised monitor loop, and the supervisor restart window can each let the bot keep quoting while a critical service is dead. (H1, H4, L8, L9)
2. **Brittle parsing** — a single malformed/unknown WS frame or response field can crash the process, tear down a connection, or drop a whole batch of fills. (H3, H5, M5, M7, L1, L2, L3)
3. **Naked-exposure windows** — fill/hedge accounting edge cases (cancel-vs-fill, synthetic-fill double-hedge, stale baseline, stale residual price, size-floor-to-zero) that leave directional exposure unhedged. (H2, M1, M2, M4, M8)

## Status summary

| Band | Count | IDs |
|------|-------|-----|
| 🔴 High | 6 | H1–H6 |
| 🟠 Medium | 13 | M1–M13 |
| 🟡 Low | 20 | L1–L20 |
| ✅ Refuted (no action) | 4 | R1–R4 |

## Suggested execution order
1. **H6 + rotate the HL key** — secret exposure (fastest, highest blast radius).
2. **H5, H1, H4** — a live crash + two fail-open safety holes; small, high-leverage diffs.
3. **H3, H2** — the two missed-hedge paths (parse robustness + cancel/fill race).
4. **M1–M5** — double-hedge, phantom fill, wrong price, stale residual price, rejection-wedging.
5. **M6–M13**, then **L1–L20** opportunistically (most are one-liners).

> Recommended workflow: implement in a worktree, run `cargo build` + `cargo test` after each item, and review the diff before anything touches `master`.

---

# 🔴 HIGH — fix before next live run

## H1. `ServiceDown` safety latch can be silently cleared by a concurrent recovery (fail-open)
**Location:** `src/services/trade_gate.rs:128-147` (`mark_service_up`) vs `:152-155` (`latch_service_down`); read path at `:223` (`allow_quote`), `:110-112` (`is_blocked`).
**Category:** race · **Severity:** High · **Confidence:** High

**Problem.** The fail-closed latch exists so a dead **hedge executor** (it owns the non-clonable `hedge_rx`) halts new maker placement. But the read path consults only the ServiceDown *bit*, never `service_down_latched`. The bit has exactly three writers: `mark_service_down` (block), `mark_service_up` (allow/block), `latch_service_down` (block). The count-only recheck added by commit `35caae9` keys off the count, never the latch. Interleaving a restartable task's recovery with a fail-closed task's death:

```
A(mark_service_up): fetch_update count 1->0; load count==0 -> true;
                    load service_down_latched -> false (B's store not yet visible); enter if;
B(latch_service_down): store(latched=true); block(ServiceDown)   // bit SET
A: allow(ServiceDown)                                            // bit CLEARED
A: recheck load count==0 -> does NOT re-block
```
Final: `latched==true`, `count==0`, **bit CLEAR**. Unlike the count-only race `35caae9` fixed, this is **not self-correcting** — it persists until process restart.

**Impact.** The bot resumes placing maker orders on Pacifica while the hedge executor (or cancel_manager / fill_detection) is dead → every subsequent fill is naked, accumulating with no auto-recovery. Defeats the core fail-closed invariant. **Real money loss.**

**Fix.** Make the latch authoritative on the read path (it is set-once, so the load is cheap and robust on any memory model):

```rust
// allow_quote (trade_gate.rs:223): require the latch to be clear
self.reasons_bits() == 0 && !self.service_down_latched.load(Ordering::Acquire)

// is_blocked (trade_gate.rs:110-112): latch forces ServiceDown blocked
pub fn is_blocked(&self, reason: GateReason) -> bool {
    if reason == GateReason::ServiceDown
        && self.service_down_latched.load(Ordering::Acquire)
    {
        return true;
    }
    self.reasons_bits() & reason.bit() != 0
}
```
Optional defense-in-depth: also OR the latch into the count recheck at `:143`. Add a stress/loom regression test that drives `latch_service_down()` concurrently with `mark_service_up()` and asserts `allow_quote`/`is_blocked(ServiceDown)` stays blocked.

---

## H2. Cancel-vs-fill race: a filled order is mistaken for a cancelled one, dropping the fast hedge path
**Location:** `src/services/cancel_manager.rs:205-217, 248-263`
**Category:** race · **Severity:** High · **Confidence:** High

**Problem.** When `verify_no_open_orders` finds the order absent, it assumes "cancelled" and calls `clear_active_order` (zeroes `active_order`, clears cloid). But the order may be absent **because it filled**. The millisecond hedge path is skipped.

**Impact.** A real fill is treated as a no-op cancel → naked exposure. Recovery falls solely to the 1s `position_reconciler` (after `reconciler_grace_ms`, default ~2.5s) or the 500ms safety monitor, so the position sits naked for seconds and is then hedged at a market-moved price — a repeatable loss on exactly the adverse-selection fills the cancel was avoiding. New-order placement is correctly blocked during this window by the cancel-grace + NetExposure gate (why this is High, not Critical).

**Fix.** Make the cancel-verify path fill-aware before zeroing `active_order`:
1. In `verify_no_open_orders`, capture the active cloid; on the "order absent" outcome, determine *why* it is absent — check the snapshot's `filled_amount` for that cloid and/or `get_trade_history(Some(symbol), Some(100), …)` filtered by the active cloid (as `rest_fill_detection.rs:203-272` does) to compute cumulative filled.
2. If cumulative filled > what the `FillAggregator` already hedged for that `order_id`, route the residual to the fill path (`observe_fill` → `try_reserve_hedge` → `enqueue_reserved_hedge`, deduped via `processed_fills`) and **do not** call `clear_active_order`.
3. Only on a genuine zero-fill cancel proceed with `clear_after_verified_cancel` as today.

This requires giving `CancelManagerService` access to `FillAggregator`, `FillDedup`, and `hedge_tx`. **Minimal interim guard:** in `clear_after_verified_cancel`, skip `clear_active_order()` (leave status `Cancelling`) whenever `fill_aggregator.snapshot(order_id)` shows a non-zero residual, so the WS/REST detectors can still claim the fill.

---

## H3. One malformed / unknown-enum order update drops the ENTIRE batch frame → missed hedge
**Location:** `src/connector/pacifica/fill_detection.rs:412-435`; enums `types.rs:160-187`, `OrderUpdate` `:190-226`, `AccountOrderUpdatesResponse` `:229-233`
**Category:** parsing · **Severity:** High · **Confidence:** High

**Problem.** `account_order_updates` arrives as a batch, but `handle_message` does a single strict `serde` parse of the whole frame. A single sibling update with an unknown enum value (new server status) or a missing non-critical field fails the parse → the **entire frame is dropped**, including any valid `Filled` event in it.

**Impact.** A fill that should be hedged is dropped → naked Pacifica exposure. The position-based redundancy layer may eventually catch it, but only with delay and only if `enable_position_fill_detection` + the position stream are healthy.

**Fix.** Tolerant enums + per-item parsing.
```rust
// types.rs
// OrderEvent: append  #[serde(other)] Unknown
// OrderStatus: append #[serde(other)] Unknown   // to_fill_event's `_ => None` already handles it
// Non-fill-critical fields tolerant:
#[serde(rename = "ip", default)] pub initial_price: String,
#[serde(rename = "ct", default)] pub created_at: u64,
// Keep p (avg price), a (orig amount), f (filled), d (side), os, oe, ut REQUIRED.
```
```rust
// fill_detection.rs handle_message — parse each item independently
let resp: serde_json::Value = serde_json::from_str(text)?;
let items = resp.get("data").and_then(|d| d.as_array()).cloned().unwrap_or_default();
*self.last_order_fill_time.lock() = Instant::now();
for item in items {
    match serde_json::from_value::<OrderUpdate>(item.clone()) {
        Ok(update) => if let Some(ev) = update.to_fill_event() { callback(ev); },
        Err(e) => warn!("[PACIFICA_FILL] Skipping unparseable order update (siblings preserved): {} | raw={}", e, item),
    }
}
```
Optional: count consecutive parse-skips and surface to the health/alert layer so a persistent schema break is noticed, not just logged.

---

## H4. Order-monitor cancel-trigger loop is spawned UNSUPERVISED (silent panic stops all maker cancellation)
**Location:** `src/services/order_monitor.rs:412-424` (`spawn_monitor_tasks`); consumers at `src/app.rs:766-767`
**Category:** lifecycle · **Severity:** High · **Confidence:** High

**Problem.** The loop that cancels maker orders on age / profit deterioration is a bare `tokio::spawn`, not behind the supervisor. If it panics, maker orders are no longer cancelled.

**Impact.** Orders sit on the book and fill at stale/adverse prices (negative selection), with no halt and no operator signal, while the bot keeps placing new ones. Exposure churns indefinitely.

**Fix.** Supervise the critical loop; keep the profit logger best-effort.
```rust
pub fn spawn_monitor_tasks(service: Arc<OrderMonitorService>, trade_gate: Arc<TradeGate>) {
    spawn_supervised_with_factory(
        "order_monitor",
        trade_gate,
        RestartPolicy::default(),
        {
            let service = Arc::clone(&service);
            move || { let service = Arc::clone(&service); async move { service.run_monitor_loop().await } }
        },
    );
    let logger = Arc::clone(&service);
    tokio::spawn(async move { logger.run_profit_logger().await; }); // logging only — best-effort
}
```
Update the call site at `app.rs:767` to pass `self.trade_gate.clone()`. Add a regression test asserting a panicking `run_monitor_loop` marks then clears `ServiceDown` (mirror `supervisor.rs::factory_restarts_after_panic_and_clears_service_down`).

---

## H5. Order-placement success path panics on a short/invalid server-returned cloid (live crash)
**Location:** `src/app.rs:1108-1117` (slices at `:1115-1116`); value from `src/connector/pacifica/ws_trading.rs:155-160`
**Category:** panic · **Severity:** High (reported Critical; verifier downgraded only because it needs a malformed echo) · **Confidence:** High

**Problem.** The success log does `&returned_cloid[..8]` / `&returned_cloid[len-4..]` on the **venue-echoed** cloid. A short string underflows; a non-ASCII byte boundary panics. Either way the **main loop panics** in the live order-placement path.

**Impact.** Process panic, possibly while a fill is in flight or arriving → bot wedged/dead while holding (or about to hold) naked exposure.

**Fix.** Log the **local** UUID `client_order_id` (in scope at `app.rs:1030`, always 36-char ASCII) — keep `returned_cloid` only for the equality check that follows.
```rust
info!(
    "{} {} Placed {} #{} @ {} | cloid: {}...{}",
    tag(&self.config.symbol, "ORDER", Color::BrightYellow), "OK".green().bold(),
    opp.direction.as_str().bright_yellow(), order_id_display,
    format!("${:.4}", submitted_price).cyan().bold(),
    &client_order_id[..8], &client_order_id[client_order_id.len()-4..]
);
```
If the server echo must be shown, use char-safe slicing: `returned_cloid.chars().take(8).collect::<String>()` etc. Leave the `same_order` check at `:1120-1139` unchanged.

---

## H6. Committed example binary prints the Hyperliquid private key to stdout
**Location:** `examples/verify_wallet.rs:13` (read at `:10`); also `standalone-utils/verify_wallet.js:9`
**Category:** secret-leak · **Severity:** High · **Confidence:** High

**Problem.** `println!`/`console.log` echo `HL_PRIVATE_KEY`.

**Impact.** If ever run on the VPS or its output captured (`nohup.out`, `docker logs`, CI), the wallet is fully compromised and can be drained.

**Fix.**
1. `examples/verify_wallet.rs`: delete the two debug `println!` (lines 12-13). The derivation check (`:17-33`) still validates key→address without printing. If a hint is needed, print only a mask: `len={} prefix={}` via `hl_private_key.get(..4)`.
2. `standalone-utils/verify_wallet.js`: delete line 9 `console.log('Private key:', privateKey);`.
3. `deploy.py`: add `examples/` (or at minimum `**/verify_wallet.*`) to `EXCLUDE_PATTERNS`. Note the Dockerfile build stage needs `examples/` in its build context, so the scrub (steps 1-2) is the primary fix and the deploy exclusion is defense-in-depth.
4. **Rotate the Hyperliquid signing key** — it may already be in repo history, CI logs, or scrollback.

---

# 🟠 MEDIUM — fix soon

## M1. Position-monitor synthetic-fill path bypasses the aggregator → reconciler can double-hedge one fill
**Location:** `src/services/position_monitor.rs:230-267` (else branch `:252-267`); cross-ref `position_reconciler.rs:146-160`
**Category:** double-hedge · **Severity:** Medium · **Confidence:** High

When the maker `order_id` is briefly unknown, the synthetic-fill branch builds a `HedgeIntent` directly and never registers it in the `FillAggregator`. So `pending_qty_for_side` doesn't reflect it, and the reconciler's `effective_net = abs_net - pending` can hedge the same fill again → a doubled taker position on Hyperliquid that must be unwound at market (realized loss + fees).

**Fix.** Make the synthetic branch register in the aggregator like the real branch, keyed by a namespaced id so it can't collide with real Pacifica order_ids:
```rust
let source_order_id = synthetic_source_id(...) | (1u64 << 63); // disjoint range
if !self.fill_aggregator.observe_fill(source_order_id, order_side, fill_size, estimated_price, true) { continue; }
let Some(r) = self.fill_aggregator.try_reserve_hedge(source_order_id) else { continue; };
reservation = Some(r);
let intent: HedgeIntent = r.into(); // source/side/size/price/seq from the reservation
// fall through to existing enqueue_hedge_intent + commit_queued/release_reservation (lines 290-324)
```
Verify `From<HedgeReservation> for HedgeIntent` preserves `HedgeSource::PositionMonitor`. Keep the existing `processed_fills` dedup.

## M2. Stale position-monitor baseline → phantom fill of the prior order's size on the next order
**Location:** `src/services/position_monitor.rs:73-102` (idle refresh) and `:114-158` (delta vs frozen baseline); baseline written only at `:77, :183, :213`
**Category:** position-drift · **Severity:** Medium · **Confidence:** High

The idle-refresh error arm swallows REST errors and leaves a stale `Some(..)` baseline. The active branch then computes a delta against it, detecting a phantom "fill" equal to a prior order's residual → over-hedge / opposite-side naked exposure, plus a spurious `mark_filled` that corrupts `bot.position`.

**Fix.**
- (a) In the idle-refresh error arm (`:95-101`) set `*self.last_position_snapshot.lock() = None;` so a failed refresh can't leave a stale baseline.
- (b) At the top of the active branch (`~:114`), if the baseline is `None` or its `last_check` is older than ~2× idle poll (i.e. not refreshed since the order went active), do a fresh authoritative `get_positions`, overwrite the baseline, and `continue` *without* running delta detection this tick. Only run delta detection once the baseline is known fresh (newer than the order's `placed_at`).

## M3. `decimals_from_step_text` returns 0 for scientific-notation / no-dot ticks → maker price snapped to integer (wrong price)
**Location:** `src/market_rules.rs:38-90`; consumed at `ws_trading.rs:92-93`
**Category:** money-math · **Severity:** Medium · **Confidence:** High

If Pacifica returns a tick in scientific notation or as an integer string, `round_to_decimals` snaps the tick-aligned price to 0 (order rejected / opportunity missed) or a wrong integer level (could post a buy far above / sell far below intent, crossing the book → immediate loss / unhedged fill). Worst for low-priced symbols.

**Fix.** Derive decimals from the parsed value when the string isn't plain-decimal, and guard:
```rust
#[inline]
pub fn decimals_from_step_value(step: f64) -> usize {
    if !step.is_finite() || step <= 0.0 || step >= 1.0 { return 0; }
    for d in 0..=12usize {
        let f = 10_f64.powi(d as i32);
        if ((step * f).round() / f - step).abs() < step * 1e-9 { return d; }
    }
    12
}
let decimals = if tick_text.contains('.') && !tick_text.contains(['e','E']) {
    decimals_from_step_text(tick_text)
} else { decimals_from_step_value(tick) };
```
Keep a half-tick `debug_assert!((r - snapped).abs() <= step * 0.5 + f64::EPSILON, ...)` in `floor_to_step`/`ceil_to_step`. **Most durable:** normalize/validate `tick_size` at ingestion in `get_market_info` (`trading.rs:268-271`) — parse to f64, reject non-finite/≤0, re-emit a canonical plain-decimal string.

## M4. Residual hedge retry loop reuses stale `hl_bid/hl_ask` captured at hedge start
**Location:** `src/services/hedge.rs:807-819` (residual loop) vs primary refresh at `:361-366`
**Category:** money-math · **Severity:** Medium · **Confidence:** High

The residual leg anchors its IOC limit to the mid captured when the hedge began. In a fast move it repeatedly fails to cross → the maker fill stays partially unhedged (naked) until the slow reconciler catches it.

**Fix.** Refresh inside the residual loop, non-fatally (don't abort — the residual is already exposed and must be retried):
```rust
if let Some(c) = self.hyperliquid_prices.usable_snapshot(Duration::from_millis(self.config.hedge_quote_max_age_ms)) {
    if c.bid > 0.0 && c.ask > 0.0 { hl_bid = c.bid; hl_ask = c.ask; }
} else if let Ok(Some((bid, ask))) = self.hyperliquid_trading.get_l2_snapshot(&self.config.symbol).await {
    if bid > 0.0 && ask > 0.0 { hl_bid = bid; hl_ask = ask; self.hyperliquid_prices.store(bid, ask); }
}
// else: keep last-known positive snapshot and proceed
```

## M5. REST order placement ignores body-level `success:false` (rejection misclassified, wedges the bot)
**Location:** `src/connector/pacifica/trading.rs:584-596` and `:688-699`; `OrderResponse` at `:66-70`
**Category:** parsing · **Severity:** Medium · **Confidence:** High

A normal post-only-would-cross or margin rejection (frequent for a maker bot) is treated as an unknown submit → trading blocked + ~10s recovery instead of a simple re-quote. Worse, a `success:false` with a populated `data` would be treated as a live order (phantom active order).

**Fix.**
```rust
#[derive(Debug, Deserialize)]
pub struct OrderResponse { pub success: Option<bool>, pub data: Option<OrderData>, pub error: Option<String> }

// in both place_limit_order_with_client_order_id and place_market_order, before extracting data:
let order_response: OrderResponse = response.json().await?;
if matches!(order_response.success, Some(false)) {
    anyhow::bail!("Order rejected: {}", order_response.error.unwrap_or_else(|| "unknown".into()));
}
let order_data = order_response.data.context("No order data in response")?;
```
`"rejected"` is already a `RejectedDefinitely` keyword (`app.rs:94`), so this routes common maker rejections to clear-and-re-quote.

## M6. Aggregator re-emits sub-dust terminal residuals HL rounds to zero → perpetual failing-hedge loop
**Location:** `src/services/fill_aggregator.rs:272-286` (`should_emit`), `:357-368` (`flush_idle`); contrast `hedge.rs:757`
**Category:** fill-dedup · **Severity:** Medium · **Confidence:** High

A terminal accumulator left short of target by `EPSILON..neutral_dust_base` endlessly re-emits sub-min hedges HL keeps rejecting → order-rate/log spam, repeated Reconciling transitions, entry never GCs.

**Fix.** Plumb a `neutral_dust` floor (matching the reconciler's `effective_dust`) into the aggregator:
- Add `pub neutral_dust: f64`, set in `with_thresholds` from `config.neutral_dust_base.max(fallback_rules(&symbol).min_size)`.
- `should_emit` (`:280`): lead with `residual > self.neutral_dust.max(1e-9) && (...)`.
- `flush_idle` (`:362`): `if acc.residual() > self.neutral_dust.max(1e-9) && ...`.
- `gc` (`:375-378`): reap terminal entries with `acc.residual() <= self.neutral_dust.max(1e-9) && acc.cumulative_hedge_pending <= f64::EPSILON && age >= ttl`.

## M7. HL hedge REST-fallback accepts non-finite / absurd bid+ask
**Location:** `src/connector/hyperliquid/trading.rs:161-230` (`get_l2_snapshot`), `:419-442` (build order); `hedge.rs:267-298`
**Category:** parsing · **Severity:** Medium · **Confidence:** High

A corrupted/adversarial REST L2 response yields a malformed or wildly mispriced hedge order. Best case it's rejected and the bot wedges in Reconciling while naked; worst case a sell hedge gets a non-marketable low limit (missed hedge).

**Fix.** Three layers:
```rust
// 1) get_l2_snapshot parse (:209, :218):
.and_then(|s| s.parse::<f64>().ok()).filter(|v| v.is_finite() && *v > 0.0)
// 2) build_market_order_request_with_cloid, before math:
anyhow::ensure!(bid_price.is_finite() && ask_price.is_finite() && bid_price > 0.0 && ask_price > 0.0 && ask_price > bid_price,
    "Invalid bid/ask for HL order: bid={bid_price} ask={ask_price}");
// and after computing limit_price_str:
anyhow::ensure!(limit_price_str.parse::<f64>().map_or(false, |p| p.is_finite() && p > 0.0),
    "Computed HL limit price not finite/positive: {limit_price_str}");
// 3) hedge.rs REST fallback (~:268): gate via the existing helper
Ok(Some((bid, ask))) if crate::util::price::prices_valid(bid, ask) => { ... }
```
Do not `store()` an unvalidated snapshot.

## M8. `round_size` floors to szDecimals → can produce `s="0"` for small fills
**Location:** `src/connector/hyperliquid/trading.rs:284-288` (+`:443`); `market_rules.rs:98-102`
**Category:** fill-dedup · **Severity:** Medium · **Confidence:** High

Cross-exchange step mismatch: a fill below one HL size step floors to zero → systematic small under-hedge (drift) or a permanently un-hedgeable residual that wedges Reconciling.

**Fix.**
1. Floor the **maker** size to `step = max(pacifica_lot_step, hl_sz_step)` at placement (`hl_sz_step = 10^-szDecimals`, already pre-fetched at `app.rs:303`), so any fill is an integer multiple of the HL step.
2. Hard guard in `build_market_order_request_with_cloid`: `if hyperliquid_size_floor(size, sz_decimals) <= 0.0 { anyhow::bail!("HL hedge size {} floors to 0 at szDecimals={}", size, sz_decimals); }`.
3. In the residual loop: if outstanding residual < one HL step, stop retrying, settle as filled-with-tracked-residual, and feed `(size - confirmed)` into `position_reconciler` rather than `settle_hedge(unknown)` + `mark_reconciling`.

## M9. Reconciler hard-error escalation timer reset indefinitely by any shrinking exposure
**Location:** `src/services/position_reconciler.rs:180-189, 267-280`
**Category:** lifecycle · **Severity:** Medium · **Confidence:** High

The 30s terminal safety-stop can be starved while the bot stays chronically naked under the base/USD caps (`max_unhedged_base 0.05`, `max_unhedged_usd $10`), because any progress (shrinking exposure) resets `unhedged_since`. A small chronic naked position bleeds via funding/adverse moves without ever tripping the hard stop.

**Fix.** Use two clocks. Keep `unhedged_since` (resettable) for the soft retry/grace cadence. Add `hard_unhedged_since: Option<Instant>` cleared **only** at the genuine neutral/dust reset sites (`:81`, `:153`, `:171/173`), never in the progress block (`:182-186`). Feed `hard_unhedged_since.elapsed()` into the `>= max_unhedged_hard_ms` ceiling. Add a unit test feeding oscillating `effective_net` (e.g. 0.045/0.025 alternating) and assert the hard clock still elapses.

## M10. No read/staleness watchdog on WS loops → half-open socket appears healthy
**Location:** `src/connector/pacifica/fill_detection.rs:357-394`, `client.rs:127-170`, `ws_trading.rs:318-356`
**Category:** lifecycle · **Severity:** Medium · **Confidence:** High

A dead-but-open fill socket reports `is_ready()`/`is_connected()` healthy while no fills arrive → undetected fills → unhedged exposure.

**Fix.** Add an inbound-traffic watchdog in each loop:
```rust
let mut last_inbound = Instant::now();
let mut stale_check = interval(Duration::from_secs(self.config.ping_interval_secs));
// in the read arm: on every Some(Ok(_)) set last_inbound = Instant::now();
_ = stale_check.tick() => {
    if last_inbound.elapsed() > Duration::from_secs(3 * self.config.ping_interval_secs) {
        warn!("[PACIFICA_FILL] No inbound frame for {:?}; reconnecting", last_inbound.elapsed());
        self.ready.store(false, Ordering::Release);
        break; // outer loop reconnects + re-subscribes
    }
}
```
Use 3× so a quiet (no-fill) account isn't falsely reconnected — pongs reset the timer.

## M11. Signature `expiry_window` (5s) shorter than HTTP timeout (10s)
**Location:** `src/connector/pacifica/trading.rs:229` (10s timeout), `:539/647/723/789` (`expiry_window = 5000`); also `ws_trading.rs:100, 192`
**Category:** signing · **Severity:** Medium · **Confidence:** High

Under a latency spike a valid order outlives its signature → rejected as expired → misclassified as unknown-submit → trading needlessly blocked.

**Fix.**
1. `const SIGNED_EXPIRY_WINDOW_MS: i64 = 30_000;` (matches API docs) used at all six sites.
2. In `classify_placement_failure` (`app.rs:~88-98`) add `expired`, `invalid message`, `verification failed` to the `RejectedDefinitely` keyword set (the order is known un-placed → safe). Add a unit test.

## M12. Shutdown hedge-drain timeout (5s) can expire mid-hedge → false "unresolved exposure" bail
**Location:** `src/app.rs:1264-1323`; timing in `hedge.rs:343-359, 769-892`; defaults `config.rs:260-302`
**Category:** lifecycle · **Severity:** Medium · **Confidence:** High

If the 5s drain expires while a hedge is still executing, order cancellation + the final exposure check run mid-hedge and persist a spurious "unresolved exposure" record → next startup blocked in Reconciling. Direction is fail-safe (errs toward halting), hence Medium.

**Fix.** At `app.rs:1267`, set `drain_confirmed = true` only on the Ok arms (false on timeout); keep `dual_cancel`; run the exposure persist + bail **only when `!drain_confirmed`**.

## M13. Lifecycle-log channel backpressure aborts hedge enqueue → journal stall wedges trading
**Location:** `src/services/hedge_store.rs:116-123` (`try_send`), `services/mod.rs:182-194` (`enqueue_hedge_intent`)
**Category:** lifecycle · **Severity:** Medium · **Confidence:** High

Under a disk stall / log-channel saturation, new hedges are refused and the bot enters Error/Reconciling while a just-detected fill is unhedged — a non-trading subsystem (journaling) wedging the trading subsystem.

**Fix.** Add a non-fatal `try_append_lifecycle_update` that maps `try_send` `Full`/`Closed` to a metric + `warn` and never errors. In `mod.rs:182-194` replace the `set_error`+`bail` with a best-effort `Created` write so the flow always reaches `hedge_tx.try_send` and lets the **hedge** channel state decide (Queued / QueueFull→`mark_reconciling` / QueueClosed→`set_error`). Switch the later writes at `:198, :214, :227` to the same best-effort variant.

---

# 🟡 LOW — hardening (20)

### Brittle parsing / panics (make exchange data non-fatal)
- **L1.** `pacifica/client.rs:189` byte-slices a network text frame at offset 100 → panic on non-char-boundary. Use `text.chars().take(100).collect::<String>()`.
- **L2.** `pacifica/client.rs:176-216` — a single malformed `book` frame tears down the orderbook connection (`self.handle_message(...)?` at `:132-135`). Log-and-continue instead of `?` (mirror `fill_detection.rs:363`).
- **L3.** `pacifica/trading.rs:715-764` — `cancel_order` ignores body `success:false` (a failed cancel reported as success). Parse the body like `cancel_all_orders` and `bail!` on `success==false`.

### Fill / hedge accounting
- **L4.** `fill_detection.rs:446-461` — cancel-residual path passes `avg_price = 0.0`, poisoning the aggregator avg-price and post-trade audit `maker_avg_price`. Capture the maker limit price in the same `bot_state` read that validates `is_our_order` and pass it instead of 0.0. **And** in `fill_aggregator.rs` `observe_fill_with_target`, only overwrite `entry.avg_price` when `avg_price > 0.0` (decouple size update from price update).
- **L5.** `fill_aggregator.rs:222-230` — `max_entries` eviction by `created_at` can drop a **live, still-pending** accumulator (loses reserved-hedge accounting → possible duplicate/re-emitted hedge). Evict only fully-settled entries (`terminal && residual≈0 && no pending reservation`).
- **L6.** `hedge.rs:843-892` — residual non-`Filled` Ok response is logged Unknown but not backed-off or escalated. Add the primary loop's exponential backoff and escalation.
- **L7.** `rest_fill_detection.rs:211-235` — disappeared-order recovery caps trade history at 100 rows, no pagination → undercounts cumulative fills, tail unhedged. Paginate via `next_cursor`/`has_more`; only mark `is_terminal=true` when cumulative ≈ order size.

### Safety / lifecycle windows
- **L8.** `safety_monitor.rs:110-117` vs `app.rs:1022-1044` — the monitor's out-of-lock atomic store can clobber the main loop's Idle→Placing reservation into Reconciling. Do the CAS under the same `bot_state` write lock as the reservation.
- **L9.** `supervisor.rs:85-109` — restartable supervisor leaves a window where a dead task doesn't yet hold `ServiceDown`. Call `trade_gate.mark_service_down()` immediately after the incarnation dies, before backoff bookkeeping.
- **L10.** `post_trade_auditor.rs:362-379` — final safety cancel uses a single REST cancel, not the dual REST+WS used elsewhere. Add `pacifica_ws_trading` to the service and call `util::cancel::dual_cancel(...)` (or wrap the REST cancel in a bounded retry).
- **L11.** `position_reconciler.rs:79-120` (+`bot/state.rs:301-307`) — can complete the cycle and zero tracked position on two transiently-neutral REST samples. Require 2 consecutive neutral ticks (with no resting order) before `mark_cycle_complete_and_idle()`.

### Money math / connector
- **L12.** `hyperliquid/trading.rs:237-281` — `round_price` rounds to **nearest** (ignores aggressive side); small slippage can round an IOC to the non-marketable side → missed hedge. After computing `limit_price_str` in the build path, if `is_buy && rounded < ask` (or `!is_buy && rounded > bid`), bump by one grid step in the aggressive direction.
- **L13.** `hyperliquid/trading.rs:478-481` — nonce from wall-clock ms is non-monotonic / collidable within a ms. Add a shared `AtomicU64` monotonic nonce that fast-forwards to wall clock but never goes backward.
- **L14.** `hyperliquid/trading.rs:45-147` — `meta_cache` has no TTL/invalidation; a stale asset index would route orders to the wrong coin. Pin `asset_id` for the configured coin at startup; assert returned `coin == symbol` on the fill path and trip the fail-closed latch on mismatch.
- **L15.** `strategy/opportunity.rs:122-173` + `ws_trading.rs:94-97` — no min-notional / min-size gate before placing (only `rounded_size <= 0.0`); `is_dust_or_below_min` is used only by the reconciler. Skip opportunities below venue minimums before transitioning to Placing.

### Config / secrets / rate limit
- **L16.** `config.rs:407-410` — `order_notional_usd` not validated finite nor ≥ venue min-notional. Add `ensure!(finite && > 0)` and `ensure!(>= fallback_rules(symbol).min_notional_usd)`.
- **L17.** `config.rs:493-498, 431-434` — `partial_hedge_min_fraction` unbounded; hedge slippage bps lack an upper bound. `ensure!((0.0..=1.0).contains(&partial_hedge_min_fraction))`; warn/clamp `hedge_slippage_bps_*` > 1000 (10%).
- **L18.** `config.json:13` — deployed active-order poll = **250ms**, below the documented rate-limit-safe 500ms (`config.rs:216-218`). Warn at startup when below the default; consider raising config.json to 500.
- **L19.** `util/rate_limit.rs:131-146` + `app.rs:975` — whole-second backoff granularity and a log gate that can hide backoff status. Minor: log at `debug!` unconditionally or compare on `remaining.ceil()`.
- **L20.** *(covered by H6 deploy exclusion)* — ensure dev/secret-touching utilities under `examples/` and `standalone-utils/` aren't shipped to the VPS.

---

# ✅ Refuted by verification — no action

- **R1.** `fill_dedup.rs:48-54` cross-layer dedup float-precision collision — **false**. `quantize_size` `.round()`s to 1e-9; verifier compiled and ran it: `1.6-1.2`, `0.3-(-0.1)`, `123.45-123.05` all map to the same key `400000000`.
- **R2.** `opportunity.rs:132/181` `> 0.0` profitability gate admits unprofitable trades — **false**; premise inverted, rounding *raises* the realized margin here.
- **R3.** `safety_monitor.rs:63-94` `update_open_orders` over/under-gate — snapshot skew is real but the **unsafe** direction (clearing the gate while a foreign order exists) is structurally impossible.
- **R4.** `position_reconciler.rs:239-247` synthetic `source_order_id` CLOID collision with a real Pacifica order_id — theoretically possible but astronomically unlikely (XOR with `hedge_seq<<32` + global seq); not actionable.

---

## Implementation checklist
- [ ] H1 latch authoritative on read path (+ regression test)
- [ ] H2 cancel-vs-fill: fill-aware verify before clearing active_order
- [ ] H3 per-item parse + `#[serde(other)]` enums
- [ ] H4 supervise order_monitor cancel loop
- [ ] H5 log local UUID, not server cloid
- [ ] H6 remove key prints + deploy exclude + **rotate HL key**
- [ ] M1–M13
- [ ] L1–L20
- [ ] `cargo build` + `cargo test` green after each item
