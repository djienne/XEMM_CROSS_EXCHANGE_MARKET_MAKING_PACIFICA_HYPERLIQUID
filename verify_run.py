#!/usr/bin/env python3
"""
verify_run.py - post-run behavioral validation for the XEMM market-making bot.

After running ONE small-size mainnet cycle (see the "Validating a Run" section
in README.md), this validates that the cycle behaved correctly, so the migrated
(maker-abstraction) build can be trusted before trading at normal size.

It reads:
  - the captured stdout log (e.g. output.log)   - the human-visible event sequence
  - data/hedge_lifecycle.jsonl                  - authoritative hedge-intent records
  - <symbol>_trades.csv                         - post-trade audit accounting (one row/cycle)
  - data/unresolved_exposure.jsonl              - must be empty/absent

and asserts: a full place -> fill -> hedge -> clean-exit sequence, every hedge
intent reached a terminal SUCCESS, the trade CSV accounting is self-consistent, a
net-neutral end state, and no anomalous recovery paths fired.

Primary source of truth = the structured journals (their schema is pinned by Rust
unit tests). The stdout log corroborates the sequence and surfaces error lines.

The journals are append-only and accumulate across runs, so journal checks are
scoped to a time window (the most recent cycle by default; override with
--since-ms). The log checks need no window - capture a fresh log per run.

Stdlib only. Exit code: 0 = PASS, 1 = FAIL (a hard check failed), 2 = usage error.

Usage:
  python verify_run.py --log output.log [--symbol SOL] [--data-dir data]
                       [--trades-csv sol_trades.csv] [--since-ms MS]
                       [--window-min 15] [--json]
"""

import argparse
import csv as csvmod
import json
import os
import re
import sys

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Canonical trade CSV columns, in the exact serialization order of
# `TradeRecord` (src/csv_logger.rs). NOTE: the Rust writer creates a fresh
# csv::Writer per append, so the HEADER row repeats before every data row -
# the parser below skips any row whose first cell is "timestamp".
CSV_COLUMNS = [
    "timestamp", "latency_ms", "symbol", "pacifica_side", "hyperliquid_side",
    "pacifica_price", "pacifica_size", "pacifica_notional", "pacifica_fee",
    "hyperliquid_price", "hyperliquid_size", "hyperliquid_notional", "hyperliquid_fee",
    "total_fees", "expected_profit_bps", "actual_profit_bps", "actual_profit_usd",
    "gross_pnl",
]
CSV_FLOAT_COLUMNS = [c for c in CSV_COLUMNS if c not in ("timestamp", "symbol",
                                                         "pacifica_side", "hyperliquid_side")]

# hedge_lifecycle.jsonl status semantics (src/services/hedge_store.rs).
# success-terminal: the hedge finished (or a reconciler intent was correctly
# dropped because the exposure was already covered).
SUCCESS_TERMINAL = {"complete", "skipped"}
FAILURE_TERMINAL = {"error", "unknown", "queue_closed"}
# Everything else (created, queued, queue_full, submitted, filled,
# partially_filled) is intermediate: an intent that ends there never finished.

PASS, FAIL, SKIP, WARN = "PASS", "FAIL", "SKIP", "WARN"

# Anomaly log substrings. Hard = a definitively broken/stuck cycle even allowing
# for the bot's own retries/recovery. Soft = can appear transiently and recover
# (the primary verdict comes from the structured checks, not these).
HARD_ANOMALIES = [
    "panicked",                              # supervisor caught a task panic (real crash)
    "Shutdown left unresolved net exposure", # exited non-neutral
    "Placement remains unknown",             # placement stuck after bounded recovery
    "cannot auto-hedge",                     # reconciler could not cover a residual
]
SOFT_ANOMALIES = [
    "Hedge order FAILED",                    # a hedge attempt failed (retries may recover)
    "entering placement recovery",           # placement response ambiguous (verify/adopt path)
    "Residual hedge attempt failed",
    "Startup found live net exposure",       # a PRIOR run left exposure (not necessarily this run)
    "Hedge drain timed out",
]


def strip_ansi(s):
    return ANSI_RE.sub("", s)


def parse_ts_to_ms(s):
    """Best-effort RFC3339 -> epoch milliseconds. Returns None on failure.
    chrono's to_rfc3339() can emit 9 fractional digits + a numeric offset; trim
    to microseconds and normalize 'Z' so datetime.fromisoformat accepts it."""
    if not s:
        return None
    import datetime
    t = s.strip().replace("Z", "+00:00")
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?([+-]\d{2}:?\d{2})?$", t)
    if not m:
        return None
    base, frac, off = m.group(1), m.group(2), m.group(3)
    if frac:
        base += "." + frac[:6]
    if off:
        if len(off) == 5:  # +0000 -> +00:00
            off = off[:3] + ":" + off[3:]
        base += off
    else:
        base += "+00:00"
    try:
        dt = datetime.datetime.fromisoformat(base)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def load_log_lines(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return [strip_ansi(line.rstrip("\n")) for line in f]


def load_jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def load_trades_csv(path):
    """Parse <symbol>_trades.csv, tolerating the repeated-header quirk."""
    rows = []
    if not path or not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        for raw in csvmod.reader(f):
            if not raw:
                continue
            if raw[0].strip() == "timestamp":   # a (repeated) header row
                continue
            if len(raw) != len(CSV_COLUMNS):     # malformed / partial line
                continue
            row = dict(zip(CSV_COLUMNS, raw))
            for c in CSV_FLOAT_COLUMNS:
                try:
                    row[c] = float(row[c])
                except (ValueError, KeyError):
                    row[c] = None
            row["_ts_ms"] = parse_ts_to_ms(row["timestamp"])
            rows.append(row)
    return rows


# --------------------------------------------------------------------------
# Checks. Each returns (name, status, detail).
# --------------------------------------------------------------------------

def first_index(lines, predicate):
    for i, ln in enumerate(lines):
        if predicate(ln):
            return i
    return None


def check_cycle_sequence(lines):
    is_place = lambda l: "ORDER]" in l and "Placed" in l
    is_fill = lambda l: "FILL_DETECTION]" in l and ("FULL FILL" in l or "PARTIAL FILL" in l)
    is_hedge_recv = lambda l: "HEDGE]" in l and "FAST HEDGE RECEIVED" in l
    is_hedge_ok = lambda l: "HEDGE]" in l and "Hedge executed successfully" in l

    n_place = sum(1 for l in lines if is_place(l))
    n_fill = sum(1 for l in lines if is_fill(l))
    n_recv = sum(1 for l in lines if is_hedge_recv(l))
    n_ok = sum(1 for l in lines if is_hedge_ok(l))
    counts = "placed=%d fill=%d hedge_received=%d hedge_ok=%d" % (n_place, n_fill, n_recv, n_ok)

    if min(n_place, n_fill, n_recv, n_ok) < 1:
        return ("cycle_sequence", FAIL, "incomplete cycle in log (%s)" % counts)

    i_place = first_index(lines, is_place)
    i_fill = first_index(lines, is_fill)
    i_recv = first_index(lines, is_hedge_recv)
    i_ok = first_index(lines, is_hedge_ok)
    if not (i_place < i_fill <= i_recv < i_ok):
        return ("cycle_sequence", FAIL,
                "events out of order (place@%s fill@%s recv@%s ok@%s)" % (i_place, i_fill, i_recv, i_ok))
    return ("cycle_sequence", PASS, counts)


def check_clean_exit(lines):
    if any("Bot terminated with error" in l for l in lines):
        bad = next(l for l in lines if "Bot terminated with error" in l)
        return ("clean_exit", FAIL, "bot exited with error: %s" % bad.strip()[-160:])
    if any(("Bot stopped cleanly" in l or "Bot completed successfully" in l) for l in lines):
        return ("clean_exit", PASS, "clean shutdown logged")
    return ("clean_exit", WARN, "no clean-exit line found (log truncated or still running?)")


def check_anomalies(lines):
    hard, soft = [], []
    for pat in HARD_ANOMALIES:
        if any(pat in l for l in lines):
            hard.append(pat)
    for pat in SOFT_ANOMALIES:
        if any(pat in l for l in lines):
            soft.append(pat)
    if hard:
        return ("no_anomalies", FAIL, "hard anomalies: %s%s" % (
            ", ".join(hard), ("; soft: " + ", ".join(soft)) if soft else ""))
    if soft:
        return ("no_anomalies", WARN, "soft anomalies (review): %s" % ", ".join(soft))
    return ("no_anomalies", PASS, "none")


def check_hedge_lifecycle(records, since_ms):
    scoped = [r for r in records if r.get("ts_ms", 0) >= since_ms]
    if not scoped:
        return ("hedge_lifecycle", SKIP, "no hedge_lifecycle records in window (since_ms=%d)" % since_ms), {}

    by_intent = {}
    for r in scoped:
        by_intent.setdefault(r.get("intent_id", "?"), []).append(r)

    problems = []
    summary = {"intents": len(by_intent), "completed": 0, "skipped": 0}
    for iid, recs in by_intent.items():
        recs.sort(key=lambda r: r.get("ts_ms", 0))
        statuses = [r.get("status") for r in recs]
        last = recs[-1]
        reached_success = any(s in SUCCESS_TERMINAL for s in statuses)
        had_failure = any(s in FAILURE_TERMINAL for s in statuses)
        if "complete" in statuses:
            summary["completed"] += 1
        if "skipped" in statuses:
            summary["skipped"] += 1

        if had_failure:
            problems.append("%s ended in failure (%s)" % (iid, [s for s in statuses if s in FAILURE_TERMINAL]))
            continue
        if not reached_success:
            problems.append("%s never reached a terminal success (last=%s)" % (iid, last.get("status")))
            continue
        # side coherence for maker-sourced hedges: maker BUY -> hedge sell, etc.
        ms, hs = (last.get("maker_side") or "").lower(), (last.get("hedge_side") or "").lower()
        if last.get("source") == "maker_fill" and ms and hs and ms == hs:
            problems.append("%s maker_side==hedge_side (%s/%s) - not a hedge" % (iid, ms, hs))
            continue
        # filled vs intended size (when the record carries filled_qty)
        fq, sz = last.get("filled_qty"), last.get("size")
        if isinstance(fq, (int, float)) and isinstance(sz, (int, float)) and sz > 0:
            if abs(fq - sz) > max(1e-9, 0.02 * sz):
                problems.append("%s filled_qty %.8g != size %.8g" % (iid, fq, sz))

    if problems:
        return ("hedge_lifecycle", FAIL, "; ".join(problems)), summary
    return ("hedge_lifecycle", PASS,
            "%d intent(s): %d complete, %d skipped" % (summary["intents"], summary["completed"], summary["skipped"])), summary


def check_trade_accounting(rows, since_ms):
    scoped = [r for r in rows if (r.get("_ts_ms") is None or r["_ts_ms"] >= since_ms)]
    # If timestamps were unparseable, fall back to the single most recent row.
    if rows and not any(r.get("_ts_ms") for r in rows):
        scoped = rows[-1:]
    if not scoped:
        return ("trade_accounting", SKIP, "no trade CSV row in window (cycle may not have audited yet)"), {}

    problems = []
    last = scoped[-1]
    for r in scoped:
        ps, hs = (r.get("pacifica_side") or "").upper(), (r.get("hyperliquid_side") or "").upper()
        if {ps, hs} != {"BUY", "SELL"}:
            problems.append("sides not opposite (pacifica=%s hyperliquid=%s)" % (ps, hs))
        psz, hsz = r.get("pacifica_size"), r.get("hyperliquid_size")
        if isinstance(psz, float) and isinstance(hsz, float) and psz > 0:
            if abs(psz - hsz) > max(1e-9, 0.02 * psz):
                problems.append("sizes differ (pacifica=%.8g hyperliquid=%.8g)" % (psz, hsz))
        # Recompute gross_pnl from notionals and compare to the logged value.
        pn, hn = r.get("pacifica_notional"), r.get("hyperliquid_notional")
        gp = r.get("gross_pnl")
        if all(isinstance(x, float) for x in (pn, hn, gp)):
            expect = (hn - pn) if ps == "BUY" else (pn - hn)
            if abs(expect - gp) > max(1e-6, 0.01 * abs(expect) if expect else 1e-6):
                problems.append("gross_pnl %.6g != recomputed %.6g" % (gp, expect))
        # Fees should be present and non-negative.
        for fee in ("pacifica_fee", "hyperliquid_fee", "total_fees"):
            v = r.get(fee)
            if not isinstance(v, float) or v < 0:
                problems.append("%s missing/negative (%r)" % (fee, v))

    summary = {
        "rows": len(scoped),
        "pacifica_side": last.get("pacifica_side"),
        "pacifica_price": last.get("pacifica_price"),
        "pacifica_size": last.get("pacifica_size"),
        "total_fees": last.get("total_fees"),
        "actual_profit_bps": last.get("actual_profit_bps"),
        "actual_profit_usd": last.get("actual_profit_usd"),
        "latency_ms": last.get("latency_ms"),
    }
    if problems:
        return ("trade_accounting", FAIL, "; ".join(problems)), summary
    return ("trade_accounting", PASS, "%d row(s) self-consistent" % len(scoped)), summary


def check_net_neutral(unresolved_path, lines, since_ms):
    recs = load_jsonl(unresolved_path)
    scoped = [r for r in recs if r.get("ts_ms", 0) >= since_ms]
    if scoped:
        return ("net_neutral", FAIL, "unresolved_exposure.jsonl has %d record(s) in window: %s"
                % (len(scoped), scoped[-1]))
    if any("Shutdown left unresolved net exposure" in l for l in lines):
        return ("net_neutral", FAIL, "log reports unresolved net exposure at shutdown")
    return ("net_neutral", PASS, "no unresolved exposure")


# --------------------------------------------------------------------------

def autodetect_symbol():
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f).get("symbol")
    except (OSError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser(description="Validate one XEMM bot cycle from logs + journals.")
    ap.add_argument("--log", required=True, help="captured bot stdout (e.g. output.log)")
    ap.add_argument("--symbol", help="trading symbol (default: read from config.json)")
    ap.add_argument("--data-dir", default="data", help="dir holding the JSONL journals (default: data)")
    ap.add_argument("--trades-csv", help="trade CSV path (default: <symbol>_trades.csv)")
    ap.add_argument("--since-ms", type=int, help="only consider journal records at/after this epoch-ms")
    ap.add_argument("--window-min", type=float, default=15.0,
                    help="if --since-ms is absent, journal window = (latest record - this many minutes)")
    ap.add_argument("--json", action="store_true", help="emit a machine-readable summary on stdout")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print("error: log file not found: %s" % args.log, file=sys.stderr)
        return 2

    symbol = args.symbol or autodetect_symbol()
    trades_csv = args.trades_csv or (("%s_trades.csv" % symbol.lower()) if symbol else None)
    hedge_path = os.path.join(args.data_dir, "hedge_lifecycle.jsonl")
    unresolved_path = os.path.join(args.data_dir, "unresolved_exposure.jsonl")

    lines = load_log_lines(args.log)
    hedge_records = load_jsonl(hedge_path)
    trade_rows = load_trades_csv(trades_csv)

    # Journal window: explicit --since-ms wins; else (latest journal ts - window).
    if args.since_ms is not None:
        since_ms = args.since_ms
    else:
        latest = max([r.get("ts_ms", 0) for r in hedge_records] + [0])
        since_ms = max(0, latest - int(args.window_min * 60_000)) if latest else 0

    checks = []
    checks.append(check_cycle_sequence(lines))
    checks.append(check_clean_exit(lines))
    checks.append(check_anomalies(lines))
    lc_check, lc_summary = check_hedge_lifecycle(hedge_records, since_ms)
    checks.append(lc_check)
    tr_check, tr_summary = check_trade_accounting(trade_rows, since_ms)
    checks.append(tr_check)
    checks.append(check_net_neutral(unresolved_path, lines, since_ms))

    failed = any(s == FAIL for _, s, _ in checks)
    warned = any(s == WARN for _, s, _ in checks)
    verdict = FAIL if failed else (WARN if warned else PASS)

    if args.json:
        print(json.dumps({
            "verdict": verdict,
            "symbol": symbol,
            "since_ms": since_ms,
            "checks": [{"name": n, "status": s, "detail": d} for n, s, d in checks],
            "hedge_lifecycle": lc_summary,
            "trade": tr_summary,
        }, indent=2, sort_keys=True))
    else:
        mark = {PASS: "PASS", FAIL: "FAIL", SKIP: "SKIP", WARN: "WARN"}
        print("=" * 72)
        print("XEMM run verification  -  symbol=%s  log=%s" % (symbol or "?", args.log))
        print("journal window: ts_ms >= %d" % since_ms)
        print("=" * 72)
        for name, status, detail in checks:
            print("  [%s] %-18s %s" % (mark[status], name, detail))
        print("-" * 72)
        if tr_summary:
            print("  cycle: %s %s @ %s  size=%s  fees=$%s  pnl=$%s (%s bps)  latency=%sms" % (
                tr_summary.get("pacifica_side"), symbol, tr_summary.get("pacifica_price"),
                tr_summary.get("pacifica_size"), tr_summary.get("total_fees"),
                tr_summary.get("actual_profit_usd"), tr_summary.get("actual_profit_bps"),
                tr_summary.get("latency_ms")))
        print("  VERDICT: %s" % verdict)
        print("=" * 72)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
