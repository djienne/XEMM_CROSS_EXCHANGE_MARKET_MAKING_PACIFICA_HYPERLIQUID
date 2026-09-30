#!/usr/bin/env python3
"""
verify_run.py - post-run validation of one XEMM bot cycle (see README
"Validating a Run").

Reads the captured stdout log plus the structured journals in --data-dir
(hedge_lifecycle.jsonl, unresolved_exposure.jsonl) and asserts: a full
place -> fill -> hedge sequence, a clean exit, no anomalous recovery paths,
every hedge intent of this run ending in success, and a net-neutral end state.

Journal records are scoped to this run: only records at/after the first
timestamp in the log count (the journals are append-only across runs). Missing
evidence is a FAIL, never a silent pass.

Stdlib only. Exit code: 0 = PASS/WARN, 1 = FAIL, 2 = usage error.

Usage:
  python verify_run.py --log output.log [--data-dir data]
"""

import argparse
import datetime
import json
import os
import re
import sys

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
LOG_TS_RE = re.compile(r"^\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)")

# hedge_lifecycle.jsonl statuses (src/services/hedge_store.rs). An intent passes
# only if its LAST record is success-terminal; earlier error/unknown records
# mean an attempt failed and was retried (WARN).
SUCCESS_TERMINAL = {"complete", "skipped"}
RETRY_MARKERS = {"error", "unknown", "queue_full"}

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"

# Hard = the cycle is broken or unverified even allowing for the bot's retries.
# Soft = can appear transiently and recover.
HARD_ANOMALIES = [
    "panicked",                                        # supervisor caught a task panic
    "Shutdown left unresolved net exposure",           # exited non-neutral
    "Placement remains unknown",                       # placement stuck after recovery
    "cannot auto-hedge",                               # reconciler could not cover a residual
    "Hedge drain timed out",                           # shutdown exposure check was skipped
    "Skipping exposure bail after drain timeout",
    "Using fill event data (trade history unavailable)",  # maker fill summary failed
    "Final position verification failed",
]
SOFT_ANOMALIES = [
    "Hedge order FAILED",
    "entering placement recovery",
    "Residual hedge attempt failed",
    "Startup found live net exposure",                 # a PRIOR run left exposure
]


def parse_ts_to_ms(s):
    """RFC3339 -> epoch ms, or None. Trims fractions to microseconds and
    normalizes 'Z' / '+0000' so datetime.fromisoformat accepts it."""
    m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?([+-]\d{2}:?\d{2})?$",
                 s.strip().replace("Z", "+00:00"))
    if not m:
        return None
    base, frac, off = m.groups()
    if frac:
        base += "." + frac[:6]
    if off and len(off) == 5:
        off = off[:3] + ":" + off[3:]
    try:
        return int(datetime.datetime.fromisoformat(base + (off or "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def load_log_lines(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return [ANSI_RE.sub("", line.rstrip("\n")) for line in f]


def log_start_ms(lines):
    for ln in lines:
        m = LOG_TS_RE.match(ln)
        if m:
            ts = parse_ts_to_ms(m.group(1))
            if ts is not None:
                return ts
    return None


def load_jsonl(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


# --------------------------------------------------------------------------
# Checks. Each returns (name, status, detail).
# --------------------------------------------------------------------------

def first_index(lines, predicate):
    return next((i for i, ln in enumerate(lines) if predicate(ln)), None)


def check_cycle_sequence(lines):
    is_place = lambda l: "ORDER]" in l and "Placed" in l
    is_fill = lambda l: "FILL_DETECTION]" in l and ("FULL FILL" in l or "PARTIAL FILL" in l)
    is_recv = lambda l: "HEDGE]" in l and "FAST HEDGE RECEIVED" in l
    is_ok = lambda l: "HEDGE]" in l and "Hedge executed successfully" in l

    idx = [first_index(lines, p) for p in (is_place, is_fill, is_recv, is_ok)]
    detail = "place@%s fill@%s hedge_received@%s hedge_ok@%s" % tuple(idx)
    if None in idx:
        return ("cycle_sequence", FAIL, "incomplete cycle in log (%s)" % detail)
    if not (idx[0] < idx[1] <= idx[2] < idx[3]):
        return ("cycle_sequence", FAIL, "events out of order (%s)" % detail)
    return ("cycle_sequence", PASS, detail)


def check_clean_exit(lines):
    bad = next((l for l in lines if "Bot terminated with error" in l), None)
    if bad:
        return ("clean_exit", FAIL, "bot exited with error: %s" % bad.strip()[-160:])
    if any("Bot stopped cleanly" in l for l in lines):
        return ("clean_exit", PASS, "clean shutdown logged")
    return ("clean_exit", WARN, "no clean-exit line (log truncated, or tee killed by Ctrl+C?)")


def check_anomalies(lines):
    hard = [p for p in HARD_ANOMALIES if any(p in l for l in lines)]
    soft = [p for p in SOFT_ANOMALIES if any(p in l for l in lines)]
    if hard:
        return ("no_anomalies", FAIL, "hard: %s%s" % (", ".join(hard), ("; soft: " + ", ".join(soft)) if soft else ""))
    if soft:
        return ("no_anomalies", WARN, "soft (review): %s" % ", ".join(soft))
    return ("no_anomalies", PASS, "none")


def check_hedge_lifecycle(records, since_ms):
    by_intent = {}
    for r in records:
        if r.get("ts_ms", 0) >= since_ms:
            by_intent.setdefault(r.get("intent_id", "?"), []).append(r)
    if not by_intent:
        return ("hedge_lifecycle", FAIL, "no hedge journal records since log start (wrong --data-dir?)")

    problems, retried = [], []
    for iid, recs in by_intent.items():
        recs.sort(key=lambda r: r.get("ts_ms", 0))  # stable: ties keep write order
        statuses = [r.get("status") for r in recs]
        last = recs[-1]
        if last.get("status") not in SUCCESS_TERMINAL:
            problems.append("%s ended %s (%s)" % (iid, last.get("status"), statuses))
            continue
        ms, hs = (last.get("maker_side") or "").lower(), (last.get("hedge_side") or "").lower()
        if last.get("source") == "maker_fill" and ms and ms == hs:
            problems.append("%s maker_side==hedge_side (%s) - not a hedge" % (iid, ms))
        fq, sz = last.get("filled_qty"), last.get("size")
        if isinstance(fq, (int, float)) and isinstance(sz, (int, float)) and sz > 0:
            if abs(fq - sz) > max(1e-9, 0.02 * sz):
                problems.append("%s filled_qty %.8g != size %.8g" % (iid, fq, sz))
        if RETRY_MARKERS.intersection(statuses):
            retried.append(iid)

    if problems:
        return ("hedge_lifecycle", FAIL, "; ".join(problems))
    if retried:
        return ("hedge_lifecycle", WARN, "%d intent(s) succeeded, after retries: %s"
                % (len(by_intent), ", ".join(retried)))
    return ("hedge_lifecycle", PASS, "%d intent(s) succeeded" % len(by_intent))


def check_net_neutral(unresolved_path, since_ms):
    scoped = [r for r in load_jsonl(unresolved_path) if r.get("ts_ms", 0) >= since_ms]
    if scoped:
        return ("net_neutral", FAIL, "%d unresolved-exposure record(s): %s" % (len(scoped), scoped[-1]))
    return ("net_neutral", PASS, "no unresolved exposure")


# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Validate one XEMM bot cycle from logs + journals.")
    ap.add_argument("--log", required=True, help="captured bot stdout+stderr (e.g. output.log)")
    ap.add_argument("--data-dir", default="data", help="dir holding the JSONL journals (default: data)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print("error: log file not found: %s" % args.log, file=sys.stderr)
        return 2

    lines = load_log_lines(args.log)
    since_ms = log_start_ms(lines)
    checks = [check_cycle_sequence(lines), check_clean_exit(lines), check_anomalies(lines)]
    if since_ms is None:
        checks.append(("journals", FAIL, "no timestamp in log; cannot scope journals to this run"))
    else:
        checks.append(check_hedge_lifecycle(
            load_jsonl(os.path.join(args.data_dir, "hedge_lifecycle.jsonl")), since_ms))
        checks.append(check_net_neutral(os.path.join(args.data_dir, "unresolved_exposure.jsonl"), since_ms))

    failed = any(s == FAIL for _, s, _ in checks)
    verdict = FAIL if failed else (WARN if any(s == WARN for _, s, _ in checks) else PASS)
    for name, status, detail in checks:
        print("[%s] %-16s %s" % (status, name, detail))
    print("VERDICT: %s" % verdict)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
