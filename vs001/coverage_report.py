#!/usr/bin/env python3
"""VS-001 coverage / latency report (read-only, stdlib only, BLINDED: no PnL).

Works on the runner's working DB or on a DB loaded by gha_pull.py.
Definitions (as in the study protocol):
  * slot          = wall-clock 5-min boundary (scheduled scan time)
  * covered slot  = at least one scan attempt in the slot with >= 90% of its GeckoTerminal pages OK
  * coverage %    = covered slots / all slots in the window
  * max gap       = largest time between consecutive covered scans (fetch end -> fetch end)
  * detection delay: data time -> detection, using (a) HTTP Date - Age of the page (origin), (b) latest pool trade
    (GT /trades), and detection -> gate quote
  * entry lateness = actual entry quote fetch time - due (detection + 5 min); protocol_ok if <= 120 s;
    > 3600 s => missed (no quote). exit lateness = actual exit fetch - due (entry + 2 h); protocol_ok if <= 300 s.

Usage: python3 coverage_report.py --db PATH [--hours 24] [--since-utc 2026-10-08T13:00:00] [--json]
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import time
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
SLOT_S = 300


def fmt(ts):
    if ts is None:
        return None
    return (datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") + " / " +
            datetime.fromtimestamp(ts, JST).strftime("%Y-%m-%d %H:%M:%S JST"))


def dist(vals):
    v = sorted(x for x in vals if x is not None)
    if not v:
        return {"n": 0}
    q = lambda p: v[min(len(v) - 1, max(0, int(math.ceil(p * len(v))) - 1))]  # noqa: E731
    return {"n": len(v), "min": round(v[0], 1), "p50": round(statistics.median(v), 1), "p90": round(q(0.9), 1),
            "p99": round(q(0.99), 1), "max": round(v[-1], 1), "mean": round(statistics.mean(v), 1)}


def has_table(c, t):
    return c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone() is not None


def report(db, since=None, until=None):
    c = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    out = {}
    cyc = [dict(r) for r in c.execute("SELECT * FROM cycles ORDER BY slot_epoch, cycle_id")] if has_table(c, "cycles") else []
    if since is None:
        since = min((r["slot_epoch"] for r in cyc), default=time.time())
    until = until or time.time()
    first_slot = math.floor(since / SLOT_S) * SLOT_S
    last_slot = math.floor(until / SLOT_S) * SLOT_S - SLOT_S      # last fully elapsed slot
    # slot grid may be offset (local tests); derive offset from data
    off = (cyc[0]["slot_epoch"] % SLOT_S) if cyc else 0
    first_slot += off
    last_slot += off
    slots = {}
    for r in cyc:
        if first_slot <= r["slot_epoch"] <= last_slot:
            s = slots.setdefault(r["slot_epoch"], {"covered": 0, "attempts": 0, "statuses": []})
            s["statuses"].append(r["status"])
            if r["status"] and not r["status"].startswith("missed"):
                s["attempts"] += 1
            s["covered"] = max(s["covered"], r["covered"] or 0)
    n_slots = int(round((last_slot - first_slot) / SLOT_S)) + 1 if last_slot >= first_slot else 0
    n_cov = sum(1 for s in slots.values() if s["covered"])
    n_attempted = sum(1 for s in slots.values() if s["attempts"])
    n_unrecorded = n_slots - len(slots)
    ok_ends = sorted(r["fetch_end_epoch"] for r in cyc if r["covered"] and r["fetch_end_epoch"]
                     and since <= r["fetch_end_epoch"] <= until + SLOT_S)
    gaps = [(b - a, a, b) for a, b in zip(ok_ends, ok_ends[1:])]
    gmax = max(gaps, default=None)
    lag = [r["start_lag_s"] for r in cyc if r["start_lag_s"] is not None and r["status"] not in (None,)
           and not str(r["status"]).startswith("missed") and first_slot <= r["slot_epoch"] <= last_slot]
    dur = [r["fetch_end_epoch"] - r["fetch_start_epoch"] for r in cyc if r["fetch_end_epoch"] and r["fetch_start_epoch"]
           and first_slot <= r["slot_epoch"] <= last_slot]
    status_counts = {}
    for s in slots.values():
        for st in s["statuses"]:
            status_counts[st] = status_counts.get(st, 0) + 1
    out["window"] = {"from": fmt(first_slot), "to_last_full_slot": fmt(last_slot), "slots": n_slots}
    out["coverage"] = {"covered_slots": n_cov, "coverage_pct": round(100 * n_cov / n_slots, 2) if n_slots else None,
                       "slots_with_attempt": n_attempted, "slots_without_any_row": n_unrecorded,
                       "cycle_status_counts": status_counts,
                       "max_gap_between_covered_scans_s": round(gmax[0], 1) if gmax else None,
                       "max_gap_from_to": [fmt(gmax[1]), fmt(gmax[2])] if gmax else None,
                       "gaps_over_10min": [{"gap_s": round(g, 1), "from": fmt(a), "to": fmt(b)} for g, a, b in gaps if g > 600],
                       "scan_start_lag_s": dist(lag), "scan_fetch_duration_s": dist(dur)}
    if has_table(c, "scan_requests"):
        r = c.execute("SELECT COUNT(*), SUM(ok), SUM(http_status=429), SUM(http_status IS NULL), SUM(attempts>1) "
                      "FROM scan_requests WHERE resp_epoch BETWEEN ? AND ?", (since, until + SLOT_S)).fetchone()
        out["gt_requests"] = {"n": r[0], "ok": r[1], "fail": (r[0] or 0) - (r[1] or 0),
                              "fail_pct": round(100 * (1 - (r[1] or 0) / r[0]), 2) if r[0] else None,
                              "http_429_final": r[2], "network_final": r[3], "retried": r[4]}
    if has_table(c, "quotes"):
        rows = c.execute("SELECT err_class, COUNT(*) FROM quotes WHERE ts_epoch BETWEEN ? AND ? GROUP BY 1",
                         (since, until + SLOT_S)).fetchall()
        tot = sum(r[1] for r in rows)
        out["quote_requests"] = {"n": tot, "by_class": {r[0]: r[1] for r in rows},
                                 "data_error_pct": round(100 * sum(r[1] for r in rows if r[0] in
                                                     ("rate_limit", "network", "server", "bad_response")) / tot, 2) if tot else None}
    if has_table(c, "events"):
        ev = [dict(r) for r in c.execute("SELECT * FROM events WHERE detected_epoch BETWEEN ? AND ?", (since, until))]
        st = {}
        for e in ev:
            st[e["status"]] = st.get(e["status"], 0) + 1
        out["detection"] = {"events": len(ev), "by_status": st,
                            "origin_to_detect_s": dist([e["delay_origin_to_detect_s"] for e in ev]),
                            "latest_trade_to_detect_s": dist([e["delay_trade_to_detect_s"] for e in ev]),
                            "detect_to_gate_quote_s": dist([e["delay_detect_to_gate_s"] for e in ev]),
                            "slot_to_detect_s": dist([e["detected_epoch"] - (c.execute(
                                "SELECT slot_epoch FROM scans WHERE scan_id=?", (e["scan_id"],)).fetchone() or [None])[0]
                                for e in ev if c.execute("SELECT slot_epoch FROM scans WHERE scan_id=?",
                                                         (e["scan_id"],)).fetchone()])}
    if has_table(c, "legs"):
        legs = [dict(r) for r in c.execute(
            "SELECT l.* FROM legs l JOIN events e ON e.event_id=l.event_id WHERE e.detected_epoch BETWEEN ? AND ?",
            (since, until))]
        ent, ex = {}, {}
        for l in legs:
            ent[l["entry_status"]] = ent.get(l["entry_status"], 0) + 1
            if l["exit_status"]:
                ex[l["exit_status"]] = ex.get(l["exit_status"], 0) + 1
        done_e = [l for l in legs if l["entry_status"] == "done"]
        done_x = [l for l in legs if l["exit_epoch"] is not None]
        out["entry_exit"] = {
            "legs": len(legs), "entry_status": ent, "exit_status": ex,
            "entry_late_s": dist([l["entry_late_s"] for l in done_e]),
            "entry_protocol_ok": sum(1 for l in done_e if l["entry_protocol_ok"] == 1),
            "entry_late_flagged": sum(1 for l in done_e if l["entry_protocol_ok"] == 0),
            "entry_missed": ent.get("missed", 0),
            "exit_late_s": dist([l["exit_late_s"] for l in done_x]),
            "exit_protocol_ok": sum(1 for l in done_x if l["exit_protocol_ok"] == 1),
            "exit_late_flagged": sum(1 for l in done_x if l["exit_protocol_ok"] == 0),
            "exit_pending": ex.get("pending", 0)}
    if has_table(c, "gha_runs"):
        out["runs"] = [{k: r[k] for k in ("gha_run_id", "gha_event", "runner_name", "started_utc", "ended_utc",
                                          "end_reason", "n_scans", "n_jobs", "missed_slots_recorded",
                                          "overdue_jobs_at_start", "gap_since_prev_activity_s", "run_created_at",
                                          "run_started_at", "n_commits_failed")}
                       for r in c.execute("SELECT * FROM gha_runs WHERE started_epoch BETWEEN ? AND ? ORDER BY id",
                                          (since - 6 * 3600, until))]
    if has_table(c, "failures"):
        out["failures_by_kind"] = {f"{r[0]}:{r[1]}": r[2] for r in c.execute(
            "SELECT where_, kind, COUNT(*) FROM failures WHERE ts_epoch BETWEEN ? AND ? GROUP BY 1,2 ORDER BY 3 DESC",
            (since, until + SLOT_S))}
    pend = c.execute("SELECT kind, COUNT(*) FROM jobs WHERE status IN ('pending','running') GROUP BY 1").fetchall() \
        if has_table(c, "jobs") else []
    out["pending_jobs"] = {r[0]: r[1] for r in pend}
    c.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--hours", type=float, default=None, help="window = last N hours (default: all data)")
    ap.add_argument("--since-utc", default=None)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    since = None
    if a.since_utc:
        since = datetime.fromisoformat(a.since_utc).replace(tzinfo=timezone.utc).timestamp()
    elif a.hours:
        since = time.time() - a.hours * 3600
    r = report(a.db, since)
    if a.json:
        print(json.dumps(r, indent=1, default=str))
        return
    print("=" * 78)
    print("VS-001 coverage / latency report (BLINDED: operational metrics only, no PnL)")
    print("=" * 78)
    for k, v in r.items():
        print(f"\n[{k}]")
        if isinstance(v, dict):
            for kk, vv in v.items():
                print(f"  {kk}: {vv}")
        elif isinstance(v, list):
            for x in v:
                print(f"  - {x}")
        else:
            print(f"  {v}")


if __name__ == "__main__":
    main()
