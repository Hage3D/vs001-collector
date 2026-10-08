"""SQLite schema + helpers.

The SQLite file is a per-run working store. The durable record is the append-only row log written by store.py
(data branch); at every start the DB is rebuilt from that log, so rowids / ids continue across runs.
"""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

SCHEMA = r"""
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
CREATE TABLE IF NOT EXISTS runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT, pid INTEGER, started_epoch REAL, started_jst TEXT,
  prev_heartbeat_epoch REAL, gap_s REAL, config_version TEXT, config_hash TEXT,
  recovered_running_jobs INTEGER, pending_jobs_at_start INTEGER, overdue_jobs_at_start INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS heartbeats (ts_epoch REAL, pid INTEGER, run_id INTEGER, last_scan_ok_epoch REAL,
  pending_jobs INTEGER, note TEXT);
CREATE INDEX IF NOT EXISTS hb_ts ON heartbeats(ts_epoch);
CREATE TABLE IF NOT EXISTS scans (
  scan_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, phase TEXT, slot_epoch REAL,
  started_epoch REAL, finished_epoch REAL, n_req INTEGER, n_ok INTEGER, n_err INTEGER, n_429 INTEGER,
  n_pools_unique INTEGER, n_universe_pre INTEGER, n_surge_pools INTEGER, n_m5_pc_null INTEGER,
  n_new_events INTEGER, n_repeats INTEGER, origin_epoch_median REAL, cdn_age_s_median REAL,
  sol_px_usd REAL, status TEXT, error TEXT, raw_path TEXT);
CREATE TABLE IF NOT EXISTS pool_snaps (
  scan_id INTEGER, pool TEXT, mint TEXT, symbol TEXT, name TEXT, dex TEXT, source TEXT,
  price_usd REAL, reserve_usd REAL, vol_m5 REAL, vol_m15 REAL, vol_h1 REAL, vol_h6 REAL, vol_h24 REAL,
  pc_m5 REAL, pc_h1 REAL, pc_h6 REAL, pc_h24 REAL, buys_m5 INTEGER, sells_m5 INTEGER,
  age_h REAL, universe_pre INTEGER, excl_reason TEXT, vol_ratio REAL, surge INTEGER, surge_branch TEXT,
  origin_epoch REAL, fetched_epoch REAL, PRIMARY KEY(scan_id, pool));
CREATE INDEX IF NOT EXISTS ps_mint ON pool_snaps(mint);
CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT, mode TEXT, phase TEXT, status TEXT, status_reason TEXT,
  scan_id INTEGER, mint TEXT, pool TEXT, symbol TEXT, name TEXT, dex TEXT, surge_branch TEXT,
  detected_epoch REAL, detected_jst TEXT, origin_epoch REAL, fetched_epoch REAL, latest_trade_epoch REAL,
  gate_epoch REAL, delay_origin_to_detect_s REAL, delay_trade_to_detect_s REAL, delay_detect_to_gate_s REAL,
  snapshot_json TEXT, gate_buy_out_raw TEXT, gate_sell_usdc REAL, gate_rt_cost_pct REAL,
  gate_buy_quote_id INTEGER, gate_sell_quote_id INTEGER,
  control_seed INTEGER, control_candidates_json TEXT, control_attempts_json TEXT,
  config_version TEXT, config_hash TEXT, test_note TEXT);
CREATE INDEX IF NOT EXISTS ev_mint ON events(mint, detected_epoch);
CREATE TABLE IF NOT EXISTS repeats (
  id INTEGER PRIMARY KEY AUTOINCREMENT, scan_id INTEGER, detected_epoch REAL, mint TEXT, pool TEXT,
  first_event_id INTEGER, metrics_json TEXT);
CREATE TABLE IF NOT EXISTS legs (
  leg_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER, leg_type TEXT, mint TEXT, pool TEXT, symbol TEXT,
  det_price_usd REAL, det_out_raw TEXT, sol_px_usd_det REAL,
  entry_due_epoch REAL, entry_status TEXT, entry_epoch REAL, entry_late_s REAL, entry_protocol_ok INTEGER,
  entry_out_raw TEXT, entry_price_impact_pct REAL, entry_route TEXT, entry_quote_id INTEGER,
  entry_rt_usdc REAL, entry_rt_cost_pct REAL, entry_rt_quote_id INTEGER, entry_fail_reason TEXT,
  exit_due_epoch REAL, exit_status TEXT, exit_epoch REAL, exit_late_s REAL, exit_protocol_ok INTEGER,
  exit_usdc REAL, exit_price_impact_pct REAL, exit_route TEXT, exit_quote_id INTEGER, exit_attempts INTEGER DEFAULT 0,
  exit_fail_reason TEXT, gt_price_exit_usd REAL, gt_pool_alive_exit INTEGER,
  sol_px_usd_exit REAL, est_fee_usd REAL, est_pnl_usd REAL, est_pnl_pct REAL, pnl_basis TEXT,
  move_det_to_entry_pct REAL, note TEXT);
CREATE TABLE IF NOT EXISTS jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, ref_id INTEGER, due_epoch REAL, status TEXT,
  attempts INTEGER DEFAULT 0, last_error TEXT, created_epoch REAL, started_epoch REAL, finished_epoch REAL,
  late_s REAL, result_json TEXT);
CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status, due_epoch);
CREATE TABLE IF NOT EXISTS quotes (
  quote_id INTEGER PRIMARY KEY AUTOINCREMENT, ts_epoch REAL, purpose TEXT, ref TEXT, input_mint TEXT,
  output_mint TEXT, amount_in TEXT, http_status INTEGER, ok INTEGER, err_class TEXT, out_amount TEXT,
  price_impact_pct REAL, route TEXT, context_slot INTEGER, latency_ms INTEGER, error TEXT, raw_json TEXT);
CREATE TABLE IF NOT EXISTS failures (
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_epoch REAL, where_ TEXT, ref TEXT, kind TEXT, detail TEXT);
CREATE TABLE IF NOT EXISTS cycles (
  cycle_id INTEGER PRIMARY KEY AUTOINCREMENT, slot_epoch REAL, slot_utc TEXT, slot_jst TEXT, status TEXT,
  scan_id INTEGER, local_run_id INTEGER, gha_run_id TEXT, gha_run_attempt TEXT, runner_name TEXT,
  fetch_start_epoch REAL, fetch_start_utc TEXT, fetch_start_jst TEXT,
  fetch_end_epoch REAL, fetch_end_utc TEXT, fetch_end_jst TEXT, scan_finished_epoch REAL,
  start_lag_s REAL, n_req INTEGER, n_ok INTEGER, n_err INTEGER, n_429 INTEGER, covered INTEGER,
  prev_ok_end_epoch REAL, gap_from_prev_ok_s REAL, note TEXT);
CREATE INDEX IF NOT EXISTS cycles_slot ON cycles(slot_epoch);
CREATE TABLE IF NOT EXISTS scan_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT, scan_id INTEGER, source TEXT, page INTEGER, url TEXT,
  http_status INTEGER, ok INTEGER, attempts INTEGER, req_epoch REAL, resp_epoch REAL, resp_utc TEXT,
  latency_ms INTEGER, cdn_age REAL, origin_epoch REAL, error TEXT, attempt_errors_json TEXT);
CREATE INDEX IF NOT EXISTS sr_scan ON scan_requests(scan_id);
CREATE TABLE IF NOT EXISTS gha_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT, local_run_id INTEGER, gha_run_id TEXT, gha_run_number TEXT,
  gha_run_attempt TEXT, gha_event TEXT, gha_workflow TEXT, gha_sha TEXT, runner_name TEXT, runner_os TEXT,
  run_created_at TEXT, run_started_at TEXT, started_epoch REAL, started_utc TEXT, started_jst TEXT,
  ended_epoch REAL, ended_utc TEXT, ended_jst TEXT, end_reason TEXT, mode TEXT,
  replay_files INTEGER, replay_rows INTEGER, pending_jobs_at_start INTEGER, overdue_jobs_at_start INTEGER,
  recovered_running_jobs INTEGER, aborted_scans_at_start INTEGER, missed_slots_recorded INTEGER,
  prev_activity_epoch REAL, gap_since_prev_activity_s REAL, n_scans INTEGER, n_jobs INTEGER,
  n_commits_ok INTEGER, n_commits_failed INTEGER, note TEXT);
CREATE TABLE IF NOT EXISTS side_theme (
  id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER, related_mint TEXT, related_pool TEXT, symbol TEXT,
  name TEXT, relation TEXT, source TEXT, price_usd_det REAL, det_epoch REAL,
  price_usd_eval REAL, eval_epoch REAL, eval_status TEXT);
"""


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(path), timeout=60, isolation_level=None, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=60000")
    c.executescript(SCHEMA)
    have = {r[1] for r in c.execute("PRAGMA table_info(events)")}
    for col in ("control_liq_seed INTEGER", "control_liq_attempts_json TEXT", "event_reserve_usd REAL"):
        if col.split()[0] not in have:
            c.execute(f"ALTER TABLE events ADD COLUMN {col}")
    have = {r[1] for r in c.execute("PRAGMA table_info(quotes)")}
    for col in ("req_epoch REAL", "attempts INTEGER"):
        if col.split()[0] not in have:
            c.execute(f"ALTER TABLE quotes ADD COLUMN {col}")
    return c


def insert(c: sqlite3.Connection, table: str, row: dict) -> int:
    cols = ",".join(row.keys())
    qs = ",".join("?" for _ in row)
    cur = c.execute(f"INSERT INTO {table} ({cols}) VALUES ({qs})", list(row.values()))
    return cur.lastrowid


def update(c: sqlite3.Connection, table: str, key: str, key_val, row: dict) -> None:
    sets = ",".join(f"{k}=?" for k in row)
    c.execute(f"UPDATE {table} SET {sets} WHERE {key}=?", list(row.values()) + [key_val])


def add_job(c, kind: str, ref_id: int, due_epoch: float) -> int:
    return insert(c, "jobs", {"kind": kind, "ref_id": ref_id, "due_epoch": due_epoch, "status": "pending",
                              "attempts": 0, "created_epoch": time.time()})


def failure(c, where: str, ref, kind: str, detail) -> None:
    insert(c, "failures", {"ts_epoch": time.time(), "where_": where, "ref": str(ref), "kind": kind,
                           "detail": detail if isinstance(detail, str) else json.dumps(detail, default=str)[:4000]})
