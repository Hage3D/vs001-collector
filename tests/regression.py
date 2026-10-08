"""VS-001 offline regression (throwaway temp DBs, mocked HTTP; never touches the network or real data).

Run: python3 -m tests.regression     (also runs as a workflow step before every collection loop)
Covers: frozen config hash; F1 no-route exit/entry; scan page failures recorded (never zero-filled);
transient retry/backoff + per-scan retry budget; HTTP total deadline; explicit timeouts on every urlopen;
PauseDetector; row-log export/replay round trip (pending jobs survive a run boundary); entry executed late
after a boundary is flagged protocol_ok=0 / missed using ACTUAL fetch time; missed slots recorded explicitly;
export policy (no raw quote JSON / Jupiter latency); bundle hygiene (no secrets / private references).
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import re
import sys
import tempfile
import time
import urllib.error
from pathlib import Path

from vs001 import common, config as C, db, jobs, runner, scanner, store

EXPECTED_CONFIG_HASH = "736577b44e7c605a"
ROOT = Path(__file__).resolve().parent.parent
RESULTS = []
log = logging.getLogger("vs001_regression")
log.addHandler(logging.StreamHandler(io.StringIO()))   # silent
log.propagate = False


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(("PASS " if cond else "FAIL ") + name + (f"  [{detail}]" if detail else ""))


def fake_result(status, body=None, error=None, t=None):
    r = common.HttpResult("mock://")
    r.status, r.body, r.error = status, body, error
    r.t_req = r.t_resp = t or time.time()
    r.latency_ms = 1
    r.attempts = 1
    return r


class Job(dict):
    pass


def t_frozen():
    check("config_hash unchanged (frozen study params)", C.config_hash() == EXPECTED_CONFIG_HASH, C.config_hash())


def t_static_timeouts():
    bad = []
    for f in sorted((ROOT / "vs001").glob("*.py")):
        for i, line in enumerate(f.read_text().splitlines(), 1):
            if "urlopen(" in line and "timeout=" not in line and "def " not in line:
                bad.append(f"{f.name}:{i}")
            if re.search(r"\bimport requests\b|\bhttpx\b|\baiohttp\b|http\.client\.HTTPS?Connection\(", line):
                bad.append(f"{f.name}:{i} other-http-lib")
    check("every urlopen has explicit timeout; no other HTTP client", not bad, ",".join(bad))
    check("http_get default timeout + total deadline set", common.HTTP_TOTAL_DEADLINE_S > 0 and
          common.http_get.__defaults__[0] == 25)


def t_f1_no_route(tmp: Path):
    conn = db.connect(tmp / "f1.sqlite")
    nr = lambda *a, **k: fake_result(400, None, {"error": "No routes found", "errorCode": "COULD_NOT_FIND_ANY_ROUTE"})  # noqa: E731
    jobs.http_get = nr
    jobs.gt_get = lambda *a, **k: fake_result(404)
    conn.execute("INSERT INTO scans (scan_id, sol_px_usd, status) VALUES (1, 100.0, 'ok')")
    t = time.time()
    lid = db.insert(conn, "legs", {"event_id": 1, "leg_type": "event", "mint": "FAKEmint", "pool": "FAKEpool",
                                   "entry_status": "done", "entry_out_raw": "12345", "exit_due_epoch": t - 1,
                                   "exit_status": "pending"})
    delays, res = [], None
    for _ in range(6):
        st, nd, res = jobs.h_exit(conn, Job(job_id=1, kind="exit", ref_id=lid, attempts=1), log)
        if st != "retry":
            break
        delays.append(round(nd - time.time()))
    leg = conn.execute("SELECT * FROM legs WHERE leg_id=?", (lid,)).fetchone()
    check("F1 exit no-route retries +60/+180/+600", delays == [60, 180, 600], str(delays))
    check("F1 exit -> unsellable_no_route, total loss kept",
          leg["exit_status"] == "unsellable_no_route" and leg["est_pnl_usd"] is not None and leg["est_pnl_usd"] <= -5.0,
          f"{leg['exit_status']} pnl={leg['est_pnl_usd']}")
    lid2 = db.insert(conn, "legs", {"event_id": 1, "leg_type": "event", "mint": "FAKEmint2", "pool": "P",
                                    "entry_due_epoch": time.time() - 1, "entry_status": "pending"})
    st = None
    for a in range(1, 8):
        st, nd, res = jobs.h_entry(conn, Job(job_id=2, kind="entry", ref_id=lid2, attempts=a), log)
        if st != "retry":
            break
    leg2 = conn.execute("SELECT * FROM legs WHERE leg_id=?", (lid2,)).fetchone()
    check("F1 entry no-route -> failed_no_route", st == "failed" and leg2["entry_status"] == "failed_no_route",
          f"{st} {leg2['entry_status']}")
    conn.close()


class _Resp:
    def __init__(self, body: bytes, slow_s=0.0):
        self._b, self.status, self.slow = io.BytesIO(body), 200, slow_s
        self.headers = {"Date": "Thu, 08 Oct 2026 12:00:00 GMT"}

    def read(self, n=-1):
        if self.slow:
            time.sleep(self.slow)
        return self._b.read(1 if self.slow else n)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _install_urlopen(plan):
    calls = []

    def fake(req, timeout=None):
        assert timeout is not None, "urlopen without timeout"
        url = req.full_url
        calls.append(url)
        m = re.search(r"page=(\d+)", url)
        key = int(m.group(1)) if m else 0
        seq = plan.get(key)
        out = seq.pop(0) if seq else "ok"
        if out == "dns":
            raise urllib.error.URLError(OSError(-3, "Temporary failure in name resolution"))
        if out == "429":
            raise urllib.error.HTTPError(url, 429, "Too Many Requests", {"retry-after": "0"}, io.BytesIO(b"{}"))
        if out == "slow":
            return _Resp(json.dumps({"data": []}).encode(), slow_s=0.05)
        return _Resp(json.dumps({"data": []}).encode())
    common.urllib.request.urlopen = fake
    return calls


def t_scan_failures_and_retry(tmp: Path):
    common.GT_LIMITER.min_interval = 0.0
    common.RETRY_BACKOFF_S = (0.0, 0.0)
    src = [("networks/solana/pools", {"sort": "h24_volume_usd_desc"}, 6)]
    calls = _install_urlopen({1: ["dns"], 2: ["dns", "dns", "dns"], 3: ["429"], 4: ["429", "429", "429"]})
    conn = db.connect(tmp / "scan.sqlite")
    sid, _ = scanner.run_scan(conn, 1, log, mode="test", slot_epoch=None, sources=src)
    sc = conn.execute("SELECT * FROM scans WHERE scan_id=?", (sid,)).fetchone()
    fails = conn.execute("SELECT kind, detail FROM failures WHERE ref=?", (str(sid),)).fetchall()
    kinds = sorted(f["kind"] for f in fails)
    check("scan: permanently failed pages stay failures (n_ok=4/6, status partial, n_429=1)",
          sc["n_req"] == 6 and sc["n_ok"] == 4 and sc["n_err"] == 2 and sc["status"] == "partial" and sc["n_429"] == 1,
          f"req={sc['n_req']} ok={sc['n_ok']} err={sc['n_err']} 429={sc['n_429']} st={sc['status']}")
    check("scan: failures table has 2 gt_page_error + 2 gt_page_retried_ok",
          kinds.count("gt_page_error") == 2 and kinds.count("gt_page_retried_ok") == 2, str(kinds))
    check("scan: retries bounded (6 pages + 1 + 2 + 1 + 2 = 12 calls)", len(calls) == 12, str(len(calls)))
    reqs = conn.execute("SELECT page, ok, http_status, attempts FROM scan_requests WHERE scan_id=? ORDER BY page",
                        (sid,)).fetchall()
    check("scan: one scan_requests row per request with explicit ok/fail",
          [tuple(r) for r in reqs] == [(1, 1, 200, 2), (2, 0, None, 3), (3, 1, 200, 2), (4, 0, 429, 3),
                                       (5, 1, 200, 1), (6, 1, 200, 1)], str([tuple(r) for r in reqs]))
    n_snaps = conn.execute("SELECT COUNT(*) FROM pool_snaps WHERE scan_id=?", (sid,)).fetchone()[0]
    check("scan: no zero-filled snapshots from failed pages", n_snaps == 0, str(n_snaps))
    calls = _install_urlopen({i: ["dns"] * 9 for i in range(1, 11)})
    pages = scanner.fetch_scan([("networks/solana/pools", {"sort": "x"}, 10)])
    check("scan: per-scan retry budget caps extra calls",
          len(calls) == 10 + scanner.SCAN_RETRY_BUDGET and all(p["status"] is None and p["body"] is None for p in pages),
          f"calls={len(calls)}")
    conn.close()


def t_total_deadline():
    common.HTTP_TOTAL_DEADLINE_S = 0.2
    _install_urlopen({1: ["slow"]})
    r = common.gt_get("networks/solana/pools", {"page": 1})
    check("http total deadline aborts a trickling body -> failure recorded (not ok)",
          (not r.ok) and r.body is None and "deadline" in str(r.error), str(r.error)[:80])
    common.HTTP_TOTAL_DEADLINE_S = 45.0


def t_pause_detector():
    real_time, real_mono = common.time.time, common.time.monotonic
    w, m = [1000.0], [50.0]
    common.time.time, common.time.monotonic = (lambda: w[0]), (lambda: m[0])
    try:
        d = common.PauseDetector(300)
        w[0] += 15; m[0] += 15
        a = d.check()
        w[0] += 4500; m[0] += 10
        b = d.check()
        w[0] += 400; m[0] += 400
        c = d.check()
    finally:
        common.time.time, common.time.monotonic = real_time, real_mono
    check("PauseDetector: no event on normal 15 s step", a is None)
    check("PauseDetector: 75-min pause -> host_pause", b and b["kind"] == "host_pause" and b["wall_delta_s"] == 4500)
    check("PauseDetector: wall+mono both jump -> stall", c and c["kind"] == "stall")


def _ok_quote(out_amount, t=None):
    return fake_result(200, {"outAmount": str(out_amount), "priceImpactPct": "0.001",
                             "routePlan": [{"swapInfo": {"label": "MockDex"}, "percent": 100}],
                             "contextSlot": 1}, t=t)


def t_boundary_roundtrip(tmp: Path):
    """Run A creates legs+jobs, exports; run B replays into a fresh DB and executes them late."""
    data = tmp / "datarepo"
    dbA = tmp / "A.sqlite"
    cA = db.connect(dbA)
    logA = store.RowLog(dbA, data, "runA")
    logA.prime(cA)
    t0 = time.time()
    cA.execute("INSERT INTO scans (scan_id, slot_epoch, sol_px_usd, status) VALUES (1, ?, 100.0, 'ok')", (t0 - 7000,))
    cA.execute("INSERT INTO events (event_id, mode, phase, status, mint, detected_epoch) VALUES (1,'main','x','active','M',?)",
               (t0 - 4000,))
    # leg 1: entry due 90 min ago (run boundary longer than 60 min) -> must become 'missed'
    l1 = db.insert(cA, "legs", {"event_id": 1, "leg_type": "event", "mint": "M1", "entry_due_epoch": t0 - 5400,
                                "entry_status": "pending", "det_out_raw": "1000"})
    # leg 2: entry due 10 min ago -> executed late, protocol_ok=0, lateness = ACTUAL fetch time - due
    l2 = db.insert(cA, "legs", {"event_id": 1, "leg_type": "event", "mint": "M2", "entry_due_epoch": t0 - 600,
                                "entry_status": "pending", "det_out_raw": "1000"})
    # leg 3: entry due in 1 min (not overdue) -> stays pending
    l3 = db.insert(cA, "legs", {"event_id": 1, "leg_type": "sol", "mint": C.SOL_MINT, "entry_due_epoch": t0 + 60,
                                "entry_status": "pending"})
    j1, j2, j3 = (db.add_job(cA, "entry", l, d) for l, d in ((l1, t0 - 5400), (l2, t0 - 600), (l3, t0 + 60)))
    cA.execute("UPDATE jobs SET status='running' WHERE job_id=?", (j2,))     # crashed mid-job in run A
    jobs.http_get = lambda *a, **k: _ok_quote(990)
    db.insert(cA, "quotes", {"ts_epoch": t0, "purpose": "x", "ok": 1, "raw_json": "{\"RAWPAYLOADMARKER\":1}", "latency_ms": 5})
    p1, _ = logA.export(t0)
    cA.execute("INSERT INTO cycles (slot_epoch, status, covered) VALUES (?, 'ok', 1)", (runner.slot_of(t0) - 3600,))
    p2, cnt2 = logA.export(t0 + 1)
    check("rowlog: incremental export only writes changed rows", cnt2 == {"cycles": 1}, str(cnt2))
    p3, cnt3 = logA.export(t0 + 2)
    check("rowlog: nothing changed -> no file", p3 is None, str(cnt3))
    dumped = b"".join(gzip.open(p).read() for p in (p1, p2))
    check("export policy: no quote raw_json / latency_ms / context_slot leaves the runner",
          b"RAWPAYLOADMARKER" not in dumped and b"latency_ms\":5" not in dumped and b"context_slot" not in dumped)
    cA.close()
    # ---- run B
    dbB = tmp / "B.sqlite"
    cB = db.connect(dbB)
    logB = store.RowLog(dbB, data, "runB")
    rep = logB.replay(cB)
    check("replay: all rows restored", rep["files"] == 2 and
          cB.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')").fetchone()[0] == 3, str(rep))
    cB.execute("UPDATE jobs SET status='pending' WHERE status='running'")          # runner recovery step
    nxt = db.insert(cB, "legs", {"event_id": 1, "leg_type": "x", "entry_status": "pending"})
    check("replay: AUTOINCREMENT ids continue (no id reuse across runs)", nxt == l3 + 1, f"{nxt} vs {l3 + 1}")
    tq = time.time() + 2.0                                                    # actual fetch time of the quote
    jobs.http_get = lambda *a, **k: _ok_quote(990, t=tq)
    for jid in (j1, j2):
        job = cB.execute("SELECT * FROM jobs WHERE job_id=?", (jid,)).fetchone()
        jobs.h_entry(cB, job, log)
    r1 = cB.execute("SELECT * FROM legs WHERE leg_id=?", (l1,)).fetchone()
    r2 = cB.execute("SELECT * FROM legs WHERE leg_id=?", (l2,)).fetchone()
    check("boundary: entry > 60 min late -> 'missed', no quote taken", r1["entry_status"] == "missed" and
          r1["entry_quote_id"] is None and r1["entry_late_s"] > 3600, f"{r1['entry_status']} {r1['entry_late_s']}")
    check("boundary: late entry uses ACTUAL fetch time; protocol_ok=0; exit due = actual + 2h",
          r2["entry_status"] == "done" and r2["entry_epoch"] == tq and abs(r2["entry_late_s"] - (tq - (t0 - 600))) < 1e-6
          and r2["entry_protocol_ok"] == 0 and abs(r2["exit_due_epoch"] - (tq + C.HOLD_S)) < 1e-6,
          f"late={r2['entry_late_s']:.1f} ok={r2['entry_protocol_ok']}")
    j3row = cB.execute("SELECT status FROM jobs WHERE job_id=?", (j3,)).fetchone()
    check("boundary: not-yet-due job survives as pending", j3row["status"] == "pending")
    # exported again after B -> replay into C reproduces B's state exactly
    logB.export(time.time() + 10)
    cC = db.connect(tmp / "C.sqlite")
    store.RowLog(tmp / "C.sqlite", data, "runC").replay(cC)
    a = [tuple(r) for r in cB.execute("SELECT leg_id, entry_status, entry_epoch, entry_late_s, entry_protocol_ok FROM legs ORDER BY 1")]
    b = [tuple(r) for r in cC.execute("SELECT leg_id, entry_status, entry_epoch, entry_late_s, entry_protocol_ok FROM legs ORDER BY 1")]
    check("replay of B's export reproduces B (last version wins)", a == b, f"{len(a)} rows")
    cB.close()
    cC.close()


def t_missed_slots(tmp: Path):
    c = db.connect(tmp / "slots.sqlite")
    t = time.time()
    s0 = runner.slot_of(t) - 6 * C.SCAN_INTERVAL_S
    c.execute("INSERT INTO cycles (slot_epoch, status, covered) VALUES (?, 'ok', 1)", (s0,))
    runner.STATE.update(local_run_id=1, run_start=t, ident={"gha_run_id": "test", "gha_run_attempt": "1",
                                                            "runner_name": "t"})
    n = runner.record_missed_slots(c, runner.slot_of(t), "missed_runner_alive")
    st = [r[0] for r in c.execute("SELECT status FROM cycles ORDER BY slot_epoch")]
    check("missed slots between runs recorded explicitly as 'missed_no_runner'",
          n == 5 and st[1:] == ["missed_no_runner"] * 5, f"n={n} {st}")
    c.close()


# sha256(lower(term))[:20] of terms that must never appear in this public bundle (known wallet addresses,
# private project/routine names, key-material words, box paths). Stored as hashes so the test itself does
# not publish them.
FORBIDDEN_TERM_HASHES = {
    "43d950c58649e8203a6c", "e1901cdcbff527a7983a", "748bbcafedfd04debe23", "1968796cb46cdd4ac8d4",
    "17ca64792deffc783102", "00e09d27ea8343e0feb3", "12b0cd140a3c0393e347", "ab87bf72a6a78a0e3507",
    "cb6ae6910892fded205f", "0e79e35a854fe805107e", "dc11e50b21840c5edd47", "21a3230e03772a58aff1"}
TOKEN_RX = re.compile(r"[A-Za-z0-9_]+(?:-[A-Za-z0-9_]+)*")


def t_bundle_hygiene():
    import hashlib
    hits = []
    for f in ROOT.rglob("*"):
        if f.is_dir() or any(p in f.parts for p in (".git", "_dryrun", "work", "__pycache__")):
            continue
        txt = f.read_text(errors="replace")
        toks = set(TOKEN_RX.findall(txt))
        toks |= {t.replace("_", "-") for t in toks}
        for tok in toks:
            if hashlib.sha256(tok.lower().encode()).hexdigest()[:20] in FORBIDDEN_TERM_HASHES:
                hits.append(f"{f.relative_to(ROOT)}:{tok[:4]}...")
        for rx in (r"ghp_[A-Za-z0-9]{20,}", r"github_pat_[A-Za-z0-9_]{20,}", r"-----BEGIN [A-Z ]*KEY-----"):
            if re.search(rx, txt):
                hits.append(f"{f.relative_to(ROOT)}:token-like")
    check("bundle hygiene: no known wallet addresses / private project refs / key material / box paths",
          not hits, ",".join(hits))


def main():
    t_frozen()
    t_static_timeouts()
    t_bundle_hygiene()
    with tempfile.TemporaryDirectory(prefix="vs001_regr_") as d:
        tmp = Path(d)
        C.RAW_DIR, C.SIDE_THEME_JSONL, C.DB_MAIN, C.DB_TEST = tmp / "raw", tmp / "side.jsonl", tmp / "main.sqlite", tmp / "test.sqlite"
        orig = (jobs.http_get, jobs.gt_get, common.urllib.request.urlopen)
        try:
            t_f1_no_route(tmp)
            t_scan_failures_and_retry(tmp)
            t_total_deadline()
            t_pause_detector()
            jobs.http_get, jobs.gt_get, common.urllib.request.urlopen = orig
            t_boundary_roundtrip(tmp)
            t_missed_slots(tmp)
        finally:
            jobs.http_get, jobs.gt_get, common.urllib.request.urlopen = orig
    n_fail = sum(1 for r in RESULTS if not r[1])
    print(f"\n{len(RESULTS) - n_fail}/{len(RESULTS)} PASS")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
