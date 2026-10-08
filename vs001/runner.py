"""VS-001 collector loop for GitHub Actions (READ-ONLY paper study; no signing, no swaps, no keys).

One invocation = one bounded run (default <= 340 min, under the 6 h hosted-job cap):
  1. rebuild the working SQLite from the committed append-only row log (store.py) -> pending jobs survive runs
  2. recovery: in-flight scans -> 'aborted_by_restart', running jobs -> pending, overdue jobs run late (late_s
     recorded; entries > 60 min late -> 'missed' by the unchanged protocol rule)
  3. record every 5-min slot since the last cycle that had no scan attempt as an explicit 'missed' cycle
  4. scanner thread: GeckoTerminal scan per wall-clock 5-min slot  -> cycles row per attempt
     worker thread : due gate/control/entry/exit/side jobs (Jupiter quotes at ACTUAL fetch time)
     main thread   : heartbeat, export changed rows + git commit/push after every cycle (and >= every 5 min)
  5. soft stop near the deadline right after a completed scan with no entry/exit due soon; final export+push.

Usage (GitHub Actions):  python3 -m vs001.runner --data-repo ../datarepo --branch data --push
Local dry run (no push):  python3 -m vs001.runner --data-repo /tmp/x --max-minutes 12
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import signal
import sys
import threading
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from . import config as C
from . import db, jobs, scanner, store
from .common import GT_LIMITER, JUP_LIMITER, PauseDetector, in_jup_quiet_window, jst, now, setup_logging, utc_iso

STOP = threading.Event()          # stop everything
NO_NEW_SCANS = threading.Event()  # trial ended / soft stop: scanner idles, worker keeps draining
CYCLE_DONE = threading.Event()    # scanner -> main: a cycle row was written, export now
STATE = {"local_run_id": None, "gha_row": None, "scans": 0, "jobs": 0, "last_error": None, "worker_busy": None,
         "scan_in_flight": False, "last_cycle_slot": None, "last_ok_end": None, "run_start": None}
SCAN_START_WINDOW_S = C.SCAN_INTERVAL_S - 60     # same rule as the box collector: no scan in last minute of a slot
MISSED_SLOT_CAP = 2016                           # at most 7 days of explicit missed-slot rows per start
log = None


def _sig(signum, frame):  # noqa: ARG001
    log.info("signal %s -> graceful stop", signum)
    STOP.set()


def gha_identity() -> dict:
    e = os.environ
    rid = e.get("GITHUB_RUN_ID")
    return {"gha_run_id": rid or f"local-{os.getpid()}-{int(time.time())}", "gha_run_number": e.get("GITHUB_RUN_NUMBER"),
            "gha_run_attempt": e.get("GITHUB_RUN_ATTEMPT"), "gha_event": e.get("GITHUB_EVENT_NAME") or "local",
            "gha_workflow": e.get("GITHUB_WORKFLOW"), "gha_sha": e.get("GITHUB_SHA"),
            "runner_name": e.get("RUNNER_NAME") or os.uname().nodename, "runner_os": e.get("RUNNER_OS") or sys.platform}


def run_timing_from_api(ident: dict) -> dict:
    """Public-repo run metadata (created_at = trigger/queue time). Best effort, unauthenticated, 10 s timeout."""
    repo, rid = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_RUN_ID")
    if not (repo and rid):
        return {}
    url = f"{os.environ.get('GITHUB_API_URL', 'https://api.github.com')}/repos/{repo}/actions/runs/{rid}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json",
                                                   "User-Agent": "vs001-collector"})
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        return {"run_created_at": d.get("created_at"), "run_started_at": d.get("run_started_at")}
    except Exception as e:  # noqa: BLE001
        return {"run_created_at": None, "run_started_at": None, "note_api": f"{type(e).__name__}"}


def code_sha256() -> str:
    h = hashlib.sha256()
    files = sorted(Path(__file__).parent.glob("*.py")) + sorted((C.ROOT / ".github" / "workflows").glob("*.yml"))
    for f in files:
        h.update(f.name.encode())
        h.update(f.read_bytes())
    return h.hexdigest()[:16]


# ------------------------------------------------------------------------------------------- cycles
# Ops-only, default 0: shifts the 5-min slot grid (used for local tests so they do not overlap another collector
# on the same IP). Never set in the workflow.
SLOT_OFFSET_S = float(os.environ.get("VS001_SLOT_OFFSET_S", "0") or 0)


def slot_of(t: float) -> float:
    return math.floor((t - SLOT_OFFSET_S) / C.SCAN_INTERVAL_S) * C.SCAN_INTERVAL_S + SLOT_OFFSET_S


def record_missed_slots(conn, upto_slot: float, reason_alive: str) -> int:
    """Insert explicit 'missed' cycles for slots after the last recorded cycle slot and before upto_slot."""
    last = conn.execute("SELECT MAX(slot_epoch) FROM cycles").fetchone()[0]
    if last is None:
        return 0
    n = 0
    s = last + C.SCAN_INTERVAL_S
    first = max(s, upto_slot - MISSED_SLOT_CAP * C.SCAN_INTERVAL_S)
    s = first
    run_start = STATE["run_start"] or now()
    while s < upto_slot:
        reason = "missed_no_runner" if run_start > s + SCAN_START_WINDOW_S else reason_alive
        db.insert(conn, "cycles", {"slot_epoch": s, "slot_utc": utc_iso(s), "slot_jst": jst(s), "status": reason,
                                   "local_run_id": STATE["local_run_id"], "gha_run_id": STATE["ident"]["gha_run_id"],
                                   "gha_run_attempt": STATE["ident"]["gha_run_attempt"],
                                   "runner_name": STATE["ident"]["runner_name"], "covered": 0,
                                   "note": "no scan attempt started in this slot's start window"})
        n += 1
        s += C.SCAN_INTERVAL_S
    return n


def write_cycle(conn, slot: float, scan_id: int | None, status: str, t_start: float, note: str | None = None):
    sc = conn.execute("SELECT * FROM scans WHERE scan_id=?", (scan_id,)).fetchone() if scan_id else None
    fe = conn.execute("SELECT MAX(resp_epoch) FROM scan_requests WHERE scan_id=?", (scan_id,)).fetchone()[0] \
        if scan_id else None
    fs = sc["started_epoch"] if sc else t_start
    n_req = sc["n_req"] if sc else None
    n_ok = sc["n_ok"] if sc else None
    covered = int(bool(n_req) and n_ok is not None and n_ok / n_req >= 0.9)
    prev = conn.execute("SELECT MAX(fetch_end_epoch) FROM cycles WHERE covered=1").fetchone()[0]
    row = {"slot_epoch": slot, "slot_utc": utc_iso(slot), "slot_jst": jst(slot), "status": status, "scan_id": scan_id,
           "local_run_id": STATE["local_run_id"], "gha_run_id": STATE["ident"]["gha_run_id"],
           "gha_run_attempt": STATE["ident"]["gha_run_attempt"], "runner_name": STATE["ident"]["runner_name"],
           "fetch_start_epoch": fs, "fetch_start_utc": utc_iso(fs), "fetch_start_jst": jst(fs),
           "fetch_end_epoch": fe, "fetch_end_utc": utc_iso(fe), "fetch_end_jst": jst(fe),
           "scan_finished_epoch": sc["finished_epoch"] if sc else now(), "start_lag_s": round(fs - slot, 3),
           "n_req": n_req, "n_ok": n_ok, "n_err": sc["n_err"] if sc else None, "n_429": sc["n_429"] if sc else None,
           "covered": covered, "prev_ok_end_epoch": prev,
           "gap_from_prev_ok_s": (fe - prev) if (covered and fe and prev) else None, "note": note}
    db.insert(conn, "cycles", row)
    return row


def scanner_loop(deadline_hard: float):
    conn = db.connect(C.DB_MAIN)
    while not STOP.is_set():
        if NO_NEW_SCANS.is_set():
            STOP.wait(2)
            continue
        t = now()
        slot = slot_of(t)
        done = conn.execute("SELECT 1 FROM scans WHERE slot_epoch=? AND status IN ('ok','partial')", (slot,)).fetchone()
        if not done and t - slot < SCAN_START_WINDOW_S and deadline_hard - t > 240:
            record_missed_slots(conn, slot, "missed_runner_alive")
            STATE["scan_in_flight"] = True
            sid = None
            try:
                sid, evs = scanner.run_scan(conn, STATE["local_run_id"], log, mode="main", slot_epoch=slot)
                st = conn.execute("SELECT status FROM scans WHERE scan_id=?", (sid,)).fetchone()[0]
                cy = write_cycle(conn, slot, sid, st, t)
                STATE["scans"] += 1
                STATE["last_cycle_slot"] = slot
                if cy["covered"]:
                    STATE["last_ok_end"] = cy["fetch_end_epoch"]
                log.info("CYCLE slot=%s status=%s lag=%.1fs fetch=%s..%s covered=%s gap_prev_ok=%s",
                         jst(slot), st, cy["start_lag_s"], cy["fetch_start_jst"], cy["fetch_end_jst"], cy["covered"],
                         cy["gap_from_prev_ok_s"] and round(cy["gap_from_prev_ok_s"], 1))
            except Exception as e:  # noqa: BLE001
                STATE["last_error"] = f"scan: {e}"
                log.error("scan failed: %s\n%s", e, traceback.format_exc())
                db.failure(conn, "scan", slot, "scan_exception", traceback.format_exc()[-2000:])
                try:
                    write_cycle(conn, slot, sid, "error", t, note=str(e)[:300])
                except Exception:  # noqa: BLE001
                    log.error("could not write error cycle: %s", traceback.format_exc())
                STATE["scan_in_flight"] = False
                CYCLE_DONE.set()
                STOP.wait(30)
                continue
            STATE["scan_in_flight"] = False
            CYCLE_DONE.set()
        nxt = slot + C.SCAN_INTERVAL_S + 2
        STOP.wait(max(1.0, min(nxt - now(), 30)))
    conn.close()


def worker_loop():
    c = db.connect(C.DB_MAIN)
    while not STOP.is_set():
        t = now()
        quiet, _ = in_jup_quiet_window(t)
        q = "SELECT * FROM jobs WHERE status='pending' AND due_epoch<=?"
        if quiet:
            q += " AND kind NOT IN ('gate','control_select','control_liq_select','entry','exit')"
        job = c.execute(q + " ORDER BY due_epoch, job_id LIMIT 1", (t,)).fetchone()
        if job is None:
            STOP.wait(1.0)
            continue
        c.execute("UPDATE jobs SET status='running', started_epoch=?, attempts=attempts+1 WHERE job_id=?",
                  (now(), job["job_id"]))
        # NOTE: handlers get the row as read BEFORE the attempts increment (identical to the box collector; the
        # retry-limit / no-route-delay indexing in jobs.py depends on it).
        STATE["worker_busy"] = f"{job['kind']}:{job['job_id']}"
        late = now() - job["due_epoch"]
        try:
            status, next_due, res = jobs.HANDLERS[job["kind"]](c, job, log)
        except Exception as e:  # noqa: BLE001
            tb = traceback.format_exc()
            log.error("job %s %s crashed: %s\n%s", job["job_id"], job["kind"], e, tb)
            db.failure(c, "job", f"main:{job['job_id']}", "job_exception", tb[-2000:])
            status, next_due, res = ("retry", now() + 60, {"exception": str(e)}) if job["attempts"] < 20 else \
                ("failed", None, {"exception": str(e)})
            if status == "failed":
                jobs.mark_job_failed(c, job)
        if status == "retry":
            c.execute("UPDATE jobs SET status='pending', due_epoch=?, last_error=? WHERE job_id=?",
                      (next_due, json.dumps(res, default=str)[:1000], job["job_id"]))
        else:
            c.execute("UPDATE jobs SET status=?, finished_epoch=?, late_s=?, result_json=? WHERE job_id=?",
                      (status, now(), late, json.dumps(res, default=str)[:4000], job["job_id"]))
            STATE["jobs"] += 1
        STATE["worker_busy"] = None
    c.close()


# ------------------------------------------------------------------------------------------- helpers
def pending(conn):
    return conn.execute("SELECT job_id,kind,ref_id,due_epoch,status FROM jobs WHERE status IN ('pending','running') "
                        "ORDER BY due_epoch").fetchall()


def write_heartbeat(conn, hb_row: bool):
    t = now()
    pend = pending(conn)
    hb = {"study": C.VERSION, "config_hash": C.config_hash(), "code_sha256": STATE.get("code_sha256"),
          "ts_utc": utc_iso(t), "ts_jst": jst(t), **STATE["ident"], "local_run_id": STATE["local_run_id"],
          "scans_this_run": STATE["scans"], "jobs_done_this_run": STATE["jobs"], "worker_busy": STATE["worker_busy"],
          "pending_jobs": len(pend), "no_new_scans": NO_NEW_SCANS.is_set(),
          "limiter": {"gt": GT_LIMITER.stats, "jup": JUP_LIMITER.stats}, "last_error": STATE["last_error"],
          "read_only": True}
    C.HEARTBEAT.write_text(json.dumps(hb, indent=1, default=str), encoding="utf-8")
    if hb_row:
        db.insert(conn, "heartbeats", {"ts_epoch": t, "pid": os.getpid(), "run_id": STATE["local_run_id"],
                                       "last_scan_ok_epoch": STATE["last_ok_end"], "pending_jobs": len(pend),
                                       "note": STATE["ident"]["gha_run_id"]})


def update_gha_row(conn, **extra):
    t = now()
    row = {"ended_epoch": t, "ended_utc": utc_iso(t), "ended_jst": jst(t), "n_scans": STATE["scans"],
           "n_jobs": STATE["jobs"], **extra}
    db.update(conn, "gha_runs", "id", STATE["gha_row"], row)


def build_state(conn, prev: dict, active: bool, end_reason: str | None) -> dict:
    t = now()
    pend = pending(conn)
    cov = conn.execute("SELECT COUNT(DISTINCT slot_epoch), SUM(covered) FROM cycles").fetchone()
    gaps = [r[0] for r in conn.execute("SELECT gap_from_prev_ok_s FROM cycles WHERE gap_from_prev_ok_s IS NOT NULL")]
    st = dict(prev)
    st.update({
        "schema": 1, "study": C.VERSION, "config_hash": C.config_hash(), "code_sha256": STATE.get("code_sha256"),
        "updated_utc": utc_iso(t), "updated_jst": jst(t), "updated_epoch": t,
        "writer": {"gha_run_id": STATE["ident"]["gha_run_id"], "active": active, "end_reason": end_reason,
                   "runner_name": STATE["ident"]["runner_name"]},
        "pending_jobs": [{"job_id": r[0], "kind": r[1], "ref_id": r[2], "due_utc": utc_iso(r[3]),
                          "due_jst": jst(r[3]), "status": r[4]} for r in pend[:50]],
        "n_pending_jobs": len(pend), "ops_overrides": STATE.get("ops_overrides"),
        "cycles_slots_seen": cov[0], "cycles_covered_attempts": cov[1] or 0,
        "max_gap_between_covered_scans_s": round(max(gaps), 1) if gaps else None,
        "export_policy": {"dropped_columns": {k: sorted(v) for k, v in store.EXPORT_DROP_COLUMNS.items()},
                          "pool_snaps": store.POOL_SNAPS_WHERE, "raw_bodies": "never exported"},
    })
    return st


def lease_wait(data_dir: Path, git: store.GitSync, me: str, max_wait_s: float = 400) -> str | None:
    """Belt-and-braces single-writer guard (the workflow's concurrency group is the primary one).

    If state.json says another writer is active and it committed < 6 min ago, wait (re-pulling) up to max_wait_s.
    Returns None when clear, or a note string ('stale_writer_...' / 'lease_override_...') that is recorded.
    """
    t_end = now() + max_wait_s
    while True:
        st = store.read_state(data_dir)
        w = st.get("writer") or {}
        other = w.get("active") and w.get("gha_run_id") != me
        if not other:
            return None
        age = now() - float(st.get("updated_epoch") or 0)
        if age > 360:
            return f"stale_writer_{w.get('gha_run_id')}_age{age:.0f}s"
        if now() > t_end:
            return f"lease_override_{w.get('gha_run_id')}"
        log.warning("another writer %s active (updated %.0fs ago); waiting", w.get("gha_run_id"), age)
        time.sleep(30)
        if git.is_repo() and git.push_enabled:
            git._git("pull", "-q", "--rebase", git.remote, git.branch, check=False)


# ------------------------------------------------------------------------------------------- main
def main(argv=None):
    global log
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-repo", required=True, help="checkout of the data branch (or plain dir)")
    ap.add_argument("--branch", default="data")
    ap.add_argument("--push", action="store_true", help="git push after each commit (GitHub Actions)")
    ap.add_argument("--max-minutes", type=float, default=float(os.environ.get("VS001_MAX_MINUTES", 340)))
    ap.add_argument("--soft-stop-minutes", type=float, default=float(os.environ.get("VS001_SOFT_STOP_MINUTES", 6)),
                    help="stop up to this many minutes early right after a completed scan")
    ap.add_argument("--trial-hours", type=float, default=float(os.environ.get("VS001_TRIAL_HOURS", 0)),
                    help="0 = unlimited. After trial end: no new scans; drain pending jobs")
    ap.add_argument("--drain-hours", type=float, default=float(os.environ.get("VS001_DRAIN_HOURS", 3)))
    ap.add_argument("--export-interval-s", type=float, default=300)
    args = ap.parse_args(argv)

    data_dir = Path(args.data_repo).resolve()
    C.DATA_DIR.mkdir(parents=True, exist_ok=True)
    log = setup_logging("vs001_runner", "runner.log")
    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)
    t_start = now()
    STATE["run_start"] = t_start
    STATE["ident"] = ident = gha_identity()
    STATE["code_sha256"] = code_sha256()
    # Ops-only pacing override (does not change config.py / config_hash; recorded in meta + gha_runs).
    # First GitHub-hosted run (2026-10-08): GeckoTerminal answered HTTP 429 after ~12 requests at the frozen
    # 2.2 s spacing (runner IPs appear limited to ~10 calls/min), so the workflow sets 6.5 s (~9.2/min).
    ops_overrides = {}
    ov = os.environ.get("VS001_GT_MIN_INTERVAL_S")
    if ov:
        GT_LIMITER.min_interval = float(ov)
        ops_overrides["GT_MIN_INTERVAL_S"] = {"frozen": C.GT_MIN_INTERVAL_S, "used": float(ov),
                                              "reason": "GeckoTerminal 429 at frozen pacing on GitHub-hosted runner IPs"}
        log.warning("OPS OVERRIDE GT_MIN_INTERVAL_S %.2f -> %.2f (config_hash unchanged; recorded)",
                    C.GT_MIN_INTERVAL_S, float(ov))
    STATE["ops_overrides"] = ops_overrides
    git = store.GitSync(data_dir, args.branch, args.push, log)
    lease_note = lease_wait(data_dir, git, ident["gha_run_id"])
    prev_state = store.read_state(data_dir)

    # ---- trial window
    trial_start = prev_state.get("trial_started_epoch")
    mode = "collect"
    if args.trial_hours > 0:
        if trial_start is None:
            trial_start = t_start
        trial_end = float(trial_start) + args.trial_hours * 3600
        drain_end = trial_end + args.drain_hours * 3600
        if t_start >= trial_end:
            mode = "drain"
    else:
        trial_end = drain_end = None

    # ---- rebuild working DB from the committed row log
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(C.DB_MAIN) + suffix)
        if p.exists():
            p.unlink()
    conn = db.connect(C.DB_MAIN)
    rowlog = store.RowLog(C.DB_MAIN, data_dir, ident["gha_run_id"].replace("/", "_"))
    rep = rowlog.replay(conn)
    n_pend0 = conn.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')").fetchone()[0]
    if mode == "drain" and (t_start >= drain_end or n_pend0 == 0):
        log.info("trial window over (trial_end=%s drain_end=%s pending=%d) -> nothing to do", jst(trial_end),
                 jst(drain_end), n_pend0)
        conn.close()
        return 0

    conn.execute("INSERT OR REPLACE INTO meta VALUES ('config_version',?)", (C.VERSION,))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('config_hash',?)", (C.config_hash(),))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('frozen_params',?)", (json.dumps(C.frozen_params(), default=str),))
    if trial_start is not None:
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('trial_started_epoch',?)", (str(trial_start),))
    conn.execute("INSERT OR REPLACE INTO meta VALUES ('ops_overrides',?)", (json.dumps(ops_overrides),))
    prev_act = conn.execute("SELECT MAX(x) FROM (SELECT MAX(ts_epoch) x FROM heartbeats UNION ALL "
                            "SELECT MAX(scan_finished_epoch) FROM cycles)").fetchone()[0]
    n_abort = conn.execute("UPDATE scans SET status='aborted_by_restart', error='process ended mid-scan' "
                           "WHERE status='running'").rowcount
    n_run = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='running'").fetchone()[0]
    conn.execute("UPDATE jobs SET status='pending', last_error='recovered after run boundary (was running)' "
                 "WHERE status='running'")
    overdue = conn.execute("SELECT job_id,kind,ref_id,due_epoch FROM jobs WHERE status='pending' AND due_epoch<? "
                           "ORDER BY due_epoch", (t_start,)).fetchall()
    for r in overdue:
        log.info("RECOVERY overdue job %s %s ref=%s due=%s overdue_by=%.0fs -> runs now, lateness recorded",
                 r[0], r[1], r[2], jst(r[3]), t_start - r[3])
    STATE["local_run_id"] = db.insert(conn, "runs", {
        "pid": os.getpid(), "started_epoch": t_start, "started_jst": jst(t_start), "prev_heartbeat_epoch": prev_act,
        "gap_s": (t_start - prev_act) if prev_act else None, "config_version": C.VERSION,
        "config_hash": C.config_hash(), "recovered_running_jobs": n_run, "pending_jobs_at_start": n_pend0,
        "overdue_jobs_at_start": len(overdue),
        "note": f"gha_run_id={ident['gha_run_id']} code_sha256={STATE['code_sha256']} mode={mode}"})
    n_missed = record_missed_slots(conn, slot_of(t_start), "missed_runner_alive")
    timing = run_timing_from_api(ident)
    STATE["gha_row"] = db.insert(conn, "gha_runs", {
        "local_run_id": STATE["local_run_id"], **ident, "run_created_at": timing.get("run_created_at"),
        "run_started_at": timing.get("run_started_at"), "started_epoch": t_start, "started_utc": utc_iso(t_start),
        "started_jst": jst(t_start), "mode": mode, "replay_files": rep["files"], "replay_rows": rep["rows"],
        "pending_jobs_at_start": n_pend0, "overdue_jobs_at_start": len(overdue), "recovered_running_jobs": n_run,
        "aborted_scans_at_start": n_abort, "missed_slots_recorded": n_missed, "prev_activity_epoch": prev_act,
        "gap_since_prev_activity_s": (t_start - prev_act) if prev_act else None,
        "note": json.dumps({"code_sha256": STATE["code_sha256"], "lease": lease_note, "ops_overrides": ops_overrides,
                            "trial_end_utc": utc_iso(trial_end) if trial_end else None,
                            "api_note": timing.get("note_api")})})
    if lease_note:
        db.failure(conn, "runner", ident["gha_run_id"], "writer_lease", lease_note)
    last = conn.execute("SELECT MAX(fetch_end_epoch) FROM cycles WHERE covered=1").fetchone()[0]
    STATE["last_ok_end"] = last
    log.info("START gha_run=%s local_run=%s mode=%s replay=%s pending=%d overdue=%d recovered_running=%d "
             "aborted_scans=%d missed_slots=%d prev_activity=%s config_hash=%s code=%s trial_end=%s",
             ident["gha_run_id"], STATE["local_run_id"], mode, rep, n_pend0, len(overdue), n_run, n_abort, n_missed,
             jst(prev_act), C.config_hash(), STATE["code_sha256"], jst(trial_end) if trial_end else None)

    state = build_state(conn, prev_state, True, None)
    if trial_start is not None:
        state["trial_started_epoch"] = trial_start
        state["trial_started_utc"] = utc_iso(float(trial_start))
        state["trial_end_utc"] = utc_iso(trial_end)
        state["drain_end_utc"] = utc_iso(drain_end)
    store.write_state(data_dir, state)
    path, cnt = rowlog.export()
    git.commit_and_push(f"vs001 start run {ident['gha_run_id']} ({mode}) {utc_iso(t_start)}")

    deadline_hard = t_start + args.max_minutes * 60
    deadline_soft = deadline_hard - args.soft_stop_minutes * 60
    if mode == "drain":
        NO_NEW_SCANS.set()
    th = [threading.Thread(target=scanner_loop, args=(deadline_hard,), name="scanner", daemon=True),
          threading.Thread(target=worker_loop, name="worker", daemon=True)]
    for x in th:
        x.start()
    pdet = PauseDetector(300)
    last_hb_row = last_export = 0.0
    end_reason = "deadline"
    while not STOP.is_set():
        t = now()
        jump = pdet.check()
        if jump:
            log.warning("pause/stall detected kind=%s wall_jump=%.0fs", jump["kind"], jump["wall_delta_s"])
            db.failure(conn, "host", ident["gha_run_id"], "host_pause_detected", jump)
        if not all(x.is_alive() for x in th):
            end_reason = "thread_died"
            log.error("a worker thread died -> stopping run")
            break
        if trial_end and t >= trial_end and not NO_NEW_SCANS.is_set():
            log.info("trial window ended at %s -> no new scans; draining pending jobs until %s", jst(trial_end),
                     jst(drain_end))
            NO_NEW_SCANS.set()
            mode = "drain"
        if mode == "drain" and not STATE["scan_in_flight"]:
            n_p = conn.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')").fetchone()[0]
            if n_p == 0 or t >= drain_end:
                end_reason = "trial_complete" if n_p == 0 else "drain_deadline"
                break
        if t >= deadline_hard:
            end_reason = "deadline"
            break
        if t >= deadline_soft and not STATE["scan_in_flight"] and STATE["worker_busy"] is None:
            cur_done = conn.execute("SELECT 1 FROM cycles WHERE slot_epoch=? AND covered=1", (slot_of(t),)).fetchone()
            due_soon = conn.execute("SELECT COUNT(*) FROM jobs WHERE status='pending' AND kind IN "
                                    "('gate','entry','exit','control_select','control_liq_select') AND due_epoch<?",
                                    (t + 240,)).fetchone()[0]
            if cur_done and not due_soon:
                end_reason = "soft_stop_after_scan"
                break
        try:
            row = t - last_hb_row >= 60
            write_heartbeat(conn, row)
            if row:
                last_hb_row = t
        except Exception as e:  # noqa: BLE001
            log.error("heartbeat failed: %s", e)
        if CYCLE_DONE.is_set() or t - last_export >= args.export_interval_s:
            CYCLE_DONE.clear()
            try:
                update_gha_row(conn, end_reason="running", n_commits_ok=git.ok, n_commits_failed=git.failed)
                store.write_state(data_dir, build_state(conn, store.read_state(data_dir), True, None))
                path, cnt = rowlog.export()
                git.commit_and_push(f"vs001 data {utc_iso(t)} run {ident['gha_run_id']} {cnt}")
            except Exception as e:  # noqa: BLE001
                log.error("export/commit failed: %s\n%s", e, traceback.format_exc())
            last_export = t
        STOP.wait(15)
    if STOP.is_set() and end_reason == "deadline" and now() < deadline_hard:
        end_reason = "signal"
    NO_NEW_SCANS.set()
    STOP.set()
    for x in th:
        x.join(timeout=150)
    alive = [x.name for x in th if x.is_alive()]
    if alive:
        log.warning("threads still alive at exit: %s (in-flight scan/job will be recovered next run)", alive)
    write_heartbeat(conn, True)
    update_gha_row(conn, end_reason=end_reason, n_commits_ok=git.ok, n_commits_failed=git.failed)
    st = build_state(conn, store.read_state(data_dir), False, end_reason)
    store.write_state(data_dir, st)
    rowlog.export()
    ok = git.commit_and_push(f"vs001 end run {ident['gha_run_id']} reason={end_reason} {utc_iso(now())}")
    log.info("EXIT gha_run=%s reason=%s scans=%d jobs=%d commits_ok=%d failed=%d final_push_ok=%s",
             ident["gha_run_id"], end_reason, STATE["scans"], STATE["jobs"], git.ok, git.failed, ok)
    conn.close()
    return 0 if ok else 4


if __name__ == "__main__":
    sys.exit(main())
