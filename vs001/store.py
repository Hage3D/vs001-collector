"""Durable, append-only row log + git sync for the GitHub Actions runner.

Layout on the data branch (or any directory):
    data/state.json                                  small human-readable state (trial window, writer lease,
                                                     pending jobs, last export) -- informational + lease
    data/rows/YYYY-MM-DD/<UTCstamp>_<run>_<seq>.jsonl.gz
        one JSON object per line: {"t": table, "id": rowid, "r": {column: value, ...}}
        Files are never rewritten. A row that changes (job status, leg exit, ...) is appended again as a new
        version; replay applies files in filename (= UTC time) order and the LAST version of (table, rowid) wins.

Publication policy (what leaves the runner): see EXPORT_DROP_COLUMNS / pool_snaps filter below. Raw API
response bodies, Jupiter latency/rate-limit data and quote raw JSON are NEVER exported.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROWS_SUBDIR = "data/rows"
STATE_FILE = "data/state.json"

# insert-only tables: export rows with rowid > last exported rowid
APPEND_TABLES = ["heartbeats", "pool_snaps", "repeats", "quotes", "failures", "scan_requests"]
# mutable tables: export rows whose content hash changed since the last export
MUTABLE_TABLES = ["meta", "runs", "gha_runs", "scans", "cycles", "events", "legs", "jobs", "side_theme"]
ALL_TABLES = MUTABLE_TABLES + APPEND_TABLES

# Columns never exported (raw third-party payloads / Jupiter performance data; see README "Data policy").
EXPORT_DROP_COLUMNS = {
    "quotes": {"raw_json", "latency_ms", "context_slot"},
}
# pool_snaps: only universe pools (the rows the study logic can use) are exported.
POOL_SNAPS_WHERE = "universe_pre=1"


def utc_stamp(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _row_hash(d: dict) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def _filter(table: str, d: dict) -> dict:
    drop = EXPORT_DROP_COLUMNS.get(table)
    if drop:
        d = {k: v for k, v in d.items() if k not in drop}
    return d


class RowLog:
    """Tracks what has been exported from a SQLite DB and writes incremental row files."""

    def __init__(self, db_path: Path, data_dir: Path, run_tag: str):
        self.db_path = Path(db_path)
        self.data_dir = Path(data_dir)
        self.run_tag = run_tag
        self.seq = 0
        self.hashes: dict[tuple[str, int], str] = {}
        self.max_rowid: dict[str, int] = {}

    # ------------------------------------------------------------------ replay
    def replay(self, conn: sqlite3.Connection) -> dict:
        """Apply every row file (filename order) to an EMPTY schema'd DB. Returns stats."""
        files = sorted((self.data_dir / ROWS_SUBDIR).glob("*/*.jsonl.gz"), key=lambda p: p.name)
        n_rows = 0
        cols_cache: dict[str, set] = {}
        conn.execute("BEGIN")
        for f in files:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    t, rid, r = rec["t"], int(rec["id"]), rec["r"]
                    have = cols_cache.get(t)
                    if have is None:
                        have = cols_cache[t] = {x[1] for x in conn.execute(f"PRAGMA table_info({t})")}
                    for k in r:
                        if k not in have:   # forward-compatible: column added by a newer code version
                            conn.execute(f'ALTER TABLE {t} ADD COLUMN "{k}"')
                            have.add(k)
                    keys = list(r.keys())
                    conn.execute(f'INSERT OR REPLACE INTO {t} (rowid,{",".join(chr(34)+k+chr(34) for k in keys)}) '
                                 f'VALUES (?{",?" * len(keys)})', [rid] + [r[k] for k in keys])
                    n_rows += 1
        conn.execute("COMMIT")
        self.prime(conn)
        return {"files": len(files), "rows": n_rows}

    def prime(self, conn: sqlite3.Connection) -> None:
        """Mark the current DB content as already exported (after replay)."""
        for t in MUTABLE_TABLES:
            for row in conn.execute(f"SELECT rowid AS _rid, * FROM {t}"):
                d = dict(row)
                rid = d.pop("_rid")
                self.hashes[(t, rid)] = _row_hash(_filter(t, d))
        for t in APPEND_TABLES:
            self.max_rowid[t] = conn.execute(f"SELECT COALESCE(MAX(rowid),0) FROM {t}").fetchone()[0]

    # ------------------------------------------------------------------ export
    def export(self, ts: float | None = None) -> tuple[Path | None, dict]:
        """Write one incremental file with every new/changed row. Returns (path or None, counts)."""
        ts = ts or time.time()
        conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=60)
        conn.row_factory = sqlite3.Row
        out, counts = [], {}
        new_hashes, new_max = {}, {}
        try:
            conn.execute("BEGIN")          # one consistent snapshot across all tables
            for t in MUTABLE_TABLES:
                for row in conn.execute(f"SELECT rowid AS _rid, * FROM {t}"):
                    d = dict(row)
                    rid = d.pop("_rid")
                    d = _filter(t, d)
                    h = _row_hash(d)
                    if self.hashes.get((t, rid)) != h:
                        out.append({"t": t, "id": rid, "r": d})
                        new_hashes[(t, rid)] = h
                        counts[t] = counts.get(t, 0) + 1
            for t in APPEND_TABLES:
                last = self.max_rowid.get(t, 0)
                mx = conn.execute(f"SELECT COALESCE(MAX(rowid),0) FROM {t}").fetchone()[0]
                if mx > last:
                    where = f" AND {POOL_SNAPS_WHERE}" if t == "pool_snaps" else ""
                    for row in conn.execute(f"SELECT rowid AS _rid, * FROM {t} WHERE rowid>? AND rowid<=?{where} "
                                            f"ORDER BY rowid", (last, mx)):
                        d = dict(row)
                        rid = d.pop("_rid")
                        out.append({"t": t, "id": rid, "r": _filter(t, d)})
                        counts[t] = counts.get(t, 0) + 1
                    new_max[t] = mx
            conn.execute("COMMIT")
        finally:
            conn.close()
        if not out:
            self.max_rowid.update(new_max)
            return None, counts
        self.seq += 1
        day = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
        d = self.data_dir / ROWS_SUBDIR / day
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{utc_stamp(ts)}_{self.run_tag}_{self.seq:05d}.jsonl.gz"
        tmp = path.with_suffix(".tmp")
        with open(tmp, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as fh:
            for rec in out:
                fh.write((json.dumps(rec, separators=(",", ":"), default=str) + "\n").encode())
        os.replace(tmp, path)
        self.hashes.update(new_hashes)
        self.max_rowid.update(new_max)
        return path, counts


# ---------------------------------------------------------------------- state.json
def read_state(data_dir: Path) -> dict:
    p = Path(data_dir) / STATE_FILE
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def write_state(data_dir: Path, state: dict) -> None:
    p = Path(data_dir) / STATE_FILE
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, p)


# ---------------------------------------------------------------------- git
class GitSync:
    """Commit (and optionally push) the data directory. Push retries with pull --rebase."""

    def __init__(self, repo_dir: Path, branch: str, push: bool, log, remote: str = "origin",
                 author: tuple[str, str] = ("github-actions[bot]",
                                            "41898282+github-actions[bot]@users.noreply.github.com")):
        self.repo = Path(repo_dir)
        self.branch = branch
        self.push_enabled = push
        self.log = log
        self.remote = remote
        self.author = author
        self.ok = 0
        self.failed = 0

    def _git(self, *args, check=True, timeout=120):
        cmd = ["git", "-C", str(self.repo), "-c", f"user.name={self.author[0]}",
               "-c", f"user.email={self.author[1]}", *args]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} -> rc={r.returncode}: {r.stderr.strip()[-400:]}")
        return r

    def is_repo(self) -> bool:
        return (self.repo / ".git").exists()

    def commit_and_push(self, message: str, attempts: int = 5) -> bool:
        if not self.is_repo():
            return True     # plain directory mode (tests / local dry run without git)
        try:
            self._git("add", "-A", "data")
            st = self._git("status", "--porcelain", "--", "data").stdout.strip()
            if st:
                self._git("commit", "-q", "-m", message)
        except Exception as e:  # noqa: BLE001
            self.failed += 1
            self.log.error("git commit failed: %s", e)
            return False
        if not self.push_enabled:
            self.ok += 1
            return True
        for i in range(attempts):
            try:
                self._git("push", "-q", self.remote, f"HEAD:refs/heads/{self.branch}", timeout=90)
                self.ok += 1
                return True
            except Exception as e:  # noqa: BLE001
                self.log.warning("git push attempt %d failed: %s", i + 1, e)
                try:
                    self._git("pull", "-q", "--rebase", self.remote, self.branch, timeout=90)
                except Exception as e2:  # noqa: BLE001
                    self.log.warning("git pull --rebase failed: %s", e2)
                    self._git("rebase", "--abort", check=False)
                time.sleep(min(30, 3 * (i + 1)))
        self.failed += 1
        self.log.error("git push gave up after %d attempts (data kept in local commits; next push retries)", attempts)
        return False
