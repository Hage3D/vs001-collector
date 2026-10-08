"""GeckoTerminal scan -> pool snapshots -> universe(pre-Jupiter) -> surge detection -> events + gate jobs."""
from __future__ import annotations

import gzip
import re
import json
import statistics
import time
from datetime import datetime, timezone

from . import config as C
from . import db
from .common import gt_get, jst, now


def _f(x):
    try:
        return float(x) if x is not None and x != "" else None
    except (TypeError, ValueError):
        return None


# Ops-only (2026-10-08): transient page failures (DNS/TLS/timeout/429/5xx) are retried in-call with backoff,
# capped per scan so a network outage cannot stretch a scan into the next 5-min slot. Pages that still fail are
# recorded as failures (status/error kept, body None) exactly as before -- never zero-filled.
SCAN_PAGE_RETRIES = 2
SCAN_RETRY_BUDGET = 8


def fetch_scan(sources=None):
    """Fetch all list pages. Returns pages[list of dict]."""
    sources = sources or C.SCAN_SOURCES
    pages = []
    budget = SCAN_RETRY_BUDGET
    for path, params, npages in sources:
        for p in range(1, npages + 1):
            prm = dict(params)
            prm["page"] = p
            r = gt_get(path, prm, retries=min(SCAN_PAGE_RETRIES, budget))
            budget -= max(0, r.attempts - 1)
            pages.append({
                "source": f"{path.split('/')[-1]}:{','.join(f'{k}={v}' for k, v in params.items())}",
                "page": p, "url": r.url, "status": r.status, "fetched_epoch": r.t_resp,
                "origin_epoch": r.origin_epoch(), "cdn_age": r.headers.get("age"),
                "date_hdr": r.headers.get("date"), "error": None if r.ok else (r.error or f"http {r.status}"),
                "attempts": r.attempts, "attempt_errors": r.attempt_errors or None, "latency_ms": r.latency_ms,
                "body": r.body,
            })
    return pages


def write_raw(scan_id: int, pages: list, tag: str = "gt_scans") -> str:
    d = C.RAW_DIR / datetime.now().strftime("%Y-%m-%d")
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{tag}.jsonl.gz"
    with gzip.open(path, "at", encoding="utf-8") as fh:
        for pg in pages:
            fh.write(json.dumps({"scan_id": scan_id, **pg}, separators=(",", ":")) + "\n")
    return str(path)


def record_requests(conn, scan_id: int, pages: list) -> None:
    """One row per GeckoTerminal request of this scan: success/failure, status, attempts, timing (no body)."""
    conn.execute("BEGIN")
    for pg in pages:
        try:
            age = float(pg.get("cdn_age")) if pg.get("cdn_age") is not None else None
        except (TypeError, ValueError):
            age = None
        lat = pg.get("latency_ms")
        resp = pg.get("fetched_epoch")
        db.insert(conn, "scan_requests", {
            "scan_id": scan_id, "source": pg["source"], "page": pg["page"], "url": pg["url"],
            "http_status": pg["status"], "ok": int(pg["status"] == 200 and pg["body"] is not None),
            "attempts": pg.get("attempts"), "req_epoch": (resp - lat / 1000.0) if (resp and lat is not None) else None,
            "resp_epoch": resp,
            "resp_utc": datetime.fromtimestamp(resp, timezone.utc).isoformat(timespec="seconds") if resp else None,
            "latency_ms": lat, "cdn_age": age, "origin_epoch": pg.get("origin_epoch"),
            "error": (str(pg["error"])[:300] if pg.get("error") else None),
            "attempt_errors_json": json.dumps(pg["attempt_errors"], default=str)[:1000] if pg.get("attempt_errors") else None})
    conn.execute("COMMIT")


def parse_pools(pages: list, t_ref: float):
    pools = {}
    sol_px = None
    for pg in pages:
        body = pg.get("body") or {}
        for p in body.get("data") or []:
            a = p.get("attributes") or {}
            addr = a.get("address")
            if not addr or addr in pools:
                continue
            rel = p.get("relationships") or {}
            base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id") or ""
            quote_id = ((rel.get("quote_token") or {}).get("data") or {}).get("id") or ""
            dex = ((rel.get("dex") or {}).get("data") or {}).get("id")
            mint = base_id.split("_", 1)[1] if "_" in base_id else base_id
            qmint = quote_id.split("_", 1)[1] if "_" in quote_id else quote_id
            name = a.get("name") or ""
            sym = name.split(" / ")[0].strip()
            vol = a.get("volume_usd") or {}
            pc = a.get("price_change_percentage") or {}
            tx = (a.get("transactions") or {}).get("m5") or {}
            created = a.get("pool_created_at")
            age_h = None
            if created:
                try:
                    age_h = (t_ref - datetime.fromisoformat(created.replace("Z", "+00:00")).timestamp()) / 3600
                except ValueError:
                    age_h = None
            price = _f(a.get("base_token_price_usd"))
            if mint == C.SOL_MINT and qmint == C.USDC_MINT and price and sol_px is None:
                sol_px = price
            pools[addr] = {
                "pool": addr, "mint": mint, "quote_mint": qmint, "symbol": sym, "name": name, "dex": dex,
                "source": f"{pg['source']}#p{pg['page']}", "price_usd": price,
                "reserve_usd": _f(a.get("reserve_in_usd")),
                "vol_m5": _f(vol.get("m5")), "vol_m15": _f(vol.get("m15")), "vol_h1": _f(vol.get("h1")),
                "vol_h6": _f(vol.get("h6")), "vol_h24": _f(vol.get("h24")),
                "pc_m5": _f(pc.get("m5")), "pc_h1": _f(pc.get("h1")), "pc_h6": _f(pc.get("h6")),
                "pc_h24": _f(pc.get("h24")), "buys_m5": tx.get("buys"), "sells_m5": tx.get("sells"),
                "age_h": age_h, "pool_created_at": created, "fdv_usd": _f(a.get("fdv_usd")),
                "market_cap_usd": _f(a.get("market_cap_usd")),
                "origin_epoch": pg.get("origin_epoch"), "fetched_epoch": pg.get("fetched_epoch"),
            }
    return pools, sol_px


def universe_pre(p: dict, sol_px):
    """Universe filters that do not need Jupiter. Jupiter $5 buy+sell gate runs at detection (gate job)."""
    sym = (p["symbol"] or "").upper().replace(" ", "")
    if p["mint"] in C.EXCLUDE_MINTS:
        return False, "excluded_mint_major_stable_lst"
    if sym in C.EXCLUDE_SYMBOLS:
        return False, "excluded_symbol_major_stable_lst"
    raw_sym = (p["symbol"] or "").strip()
    if sym in C.TOKENIZED_SYMBOLS or any(re.match(rx, raw_sym) for rx in C.TOKENIZED_SYMBOL_REGEXES):
        return False, "excluded_tokenized_equity_preipo"
    if sym in C.BRIDGED_MAJOR_SYMBOLS:
        return False, "excluded_bridged_major"
    px = p["price_usd"]
    if px is not None and "USD" in sym and C.STABLE_PRICE_BAND[0] <= px <= C.STABLE_PRICE_BAND[1]:
        return False, "excluded_stable_heuristic"
    if px is not None and sol_px and sym.endswith("SOL") and \
            C.LST_SOL_RATIO_BAND[0] <= px / sol_px <= C.LST_SOL_RATIO_BAND[1]:
        return False, "excluded_lst_heuristic"
    if p["reserve_usd"] is None or p["reserve_usd"] < C.MIN_RESERVE_USD:
        return False, "reserve_lt_50k"
    if p["vol_h24"] is None or p["vol_h24"] < C.MIN_VOL_H24_USD:
        return False, "vol_h24_lt_100k"
    if p["age_h"] is None or p["age_h"] < C.MIN_POOL_AGE_H:
        return False, "pool_age_lt_24h_or_unknown"
    return True, None


def surge_check(p: dict, vol_mult=None, pc_m5_min=None, pc_h1_fb=None):
    vol_mult = C.SURGE_VOL_MULT if vol_mult is None else vol_mult
    pc_m5_min = C.SURGE_PC_M5_MIN if pc_m5_min is None else pc_m5_min
    pc_h1_fb = C.SURGE_PC_H1_FALLBACK if pc_h1_fb is None else pc_h1_fb
    if p["vol_m5"] is None or not p["vol_h6"]:
        return False, None, "vol_fields_missing"
    baseline = p["vol_h6"] / 72.0
    ratio = p["vol_m5"] / baseline if baseline > 0 else None
    if ratio is None:
        return False, None, "baseline_zero"
    if ratio < vol_mult:
        return False, ratio, None
    if p["pc_m5"] is not None:
        return (p["pc_m5"] >= pc_m5_min), ratio, ("m5" if p["pc_m5"] >= pc_m5_min else None)
    if C.USE_H1_FALLBACK_WHEN_M5_NULL and p["pc_h1"] is not None and p["pc_h1"] >= pc_h1_fb:
        return True, ratio, "h1_fallback_m5_null"
    return False, ratio, None


def phase_for(ts: float, mode: str) -> str:
    if mode != "main":
        return "forced_test"
    t0 = C.read_t0_epoch()
    return "study" if (t0 is not None and ts >= t0) else "pre_t0_shakedown"


def run_scan(conn, run_id: int, log, mode: str = "main", slot_epoch: float | None = None,
             force_top_n: int = 0, sources=None, surge_overrides: dict | None = None):
    """One full scan. mode='main' uses frozen v0 rules. mode='test' (separate DB!) can force triggers."""
    started = now()
    scan_id = db.insert(conn, "scans", {"run_id": run_id, "phase": phase_for(started, mode),
                                        "slot_epoch": slot_epoch, "started_epoch": started, "status": "running"})
    try:
        pages = fetch_scan(sources)
    except Exception as e:  # noqa: BLE001
        db.update(conn, "scans", "scan_id", scan_id, {"finished_epoch": now(), "status": "error", "error": str(e)})
        db.failure(conn, "scan", scan_id, "scan_exception", str(e))
        raise
    raw_path = write_raw(scan_id, pages, "gt_scans" if mode == "main" else "gt_scans_TEST")
    record_requests(conn, scan_id, pages)
    n_ok = sum(1 for p in pages if p["status"] == 200 and p["body"])
    n_429 = sum(1 for p in pages if p["status"] == 429)
    n_retry = sum(max(0, (p.get("attempts") or 1) - 1) for p in pages)
    n_429_any = n_429 + sum(1 for p in pages for a in (p.get("attempt_errors") or []) if a.get("status") == 429)
    for p in pages:
        if p.get("attempt_errors") and p["status"] == 200 and p["body"]:
            db.failure(conn, "scan", scan_id, "gt_page_retried_ok", {"source": p["source"], "page": p["page"],
                                                                     "attempts": p["attempts"],
                                                                     "earlier_errors": p["attempt_errors"]})
    for p in pages:
        if not (p["status"] == 200 and p["body"]):
            db.failure(conn, "scan", scan_id, "gt_page_error", {"source": p["source"], "page": p["page"],
                                                                "status": p["status"], "error": p["error"]})
    t_ref = now()
    pools, sol_px = parse_pools(pages, t_ref)
    origins = [p["origin_epoch"] for p in pages if p.get("origin_epoch")]
    ages = []
    for p in pages:
        try:
            ages.append(float(p["cdn_age"]))
        except (TypeError, ValueError):
            pass

    n_univ = n_surge = n_m5_null = 0
    surging = []
    univ_rows = []
    so = surge_overrides or {}
    for p in pools.values():
        inu, reason = universe_pre(p, sol_px)
        s, ratio, branch = (False, None, None)
        if inu:
            s, ratio, branch = surge_check(p, so.get("vol_mult"), so.get("pc_m5_min"), so.get("pc_h1_fb"))
            n_univ += 1
            if p["pc_m5"] is None:
                n_m5_null += 1
            univ_rows.append(p)
        else:
            if p["vol_m5"] is not None and p["vol_h6"]:
                ratio = p["vol_m5"] / (p["vol_h6"] / 72.0)
        p["universe_pre"] = int(inu)
        p["excl_reason"] = reason
        p["vol_ratio"] = ratio
        p["surge"] = int(bool(s))
        p["surge_branch"] = branch
        if s:
            n_surge += 1
            surging.append(p)
    conn.execute("BEGIN")
    for p in pools.values():
        conn.execute(
            "INSERT OR REPLACE INTO pool_snaps VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (scan_id, p["pool"], p["mint"], p["symbol"], p["name"], p["dex"], p["source"], p["price_usd"],
             p["reserve_usd"], p["vol_m5"], p["vol_m15"], p["vol_h1"], p["vol_h6"], p["vol_h24"], p["pc_m5"],
             p["pc_h1"], p["pc_h6"], p["pc_h24"], p["buys_m5"], p["sells_m5"], p["age_h"], p["universe_pre"],
             p["excl_reason"], p["vol_ratio"], p["surge"], p["surge_branch"], p["origin_epoch"], p["fetched_epoch"]))
    conn.execute("COMMIT")

    # ---- forced test triggers (TEST DB ONLY) ----
    if mode != "main" and force_top_n > 0 and not surging:
        cand = sorted([p for p in univ_rows if p["vol_ratio"] is not None],
                      key=lambda x: -(x["vol_ratio"] or 0))[:force_top_n]
        for p in cand:
            p["surge_branch"] = "FORCED_TEST_TOP_RATIO"
            surging.append(p)

    detected = now()
    new_events, repeats = [], 0
    # group by mint, first-trigger-wins; within one scan pick the pool with the largest reserve
    by_mint = {}
    for p in surging:
        q = by_mint.get(p["mint"])
        if q is None or (p["reserve_usd"] or 0) > (q["reserve_usd"] or 0):
            by_mint[p["mint"]] = p
    for mint, p in by_mint.items():
        prev = conn.execute(
            "SELECT event_id FROM events WHERE mint=? AND detected_epoch>? AND status IN ('gating','active') "
            "ORDER BY detected_epoch LIMIT 1", (mint, detected - C.DEDUPE_S)).fetchone()
        snap = {k: p[k] for k in ("pool", "mint", "symbol", "name", "dex", "source", "price_usd", "reserve_usd",
                                  "vol_m5", "vol_m15", "vol_h1", "vol_h6", "vol_h24", "pc_m5", "pc_h1", "pc_h6",
                                  "pc_h24", "buys_m5", "sells_m5", "age_h", "pool_created_at", "fdv_usd",
                                  "market_cap_usd", "vol_ratio", "surge_branch", "origin_epoch", "fetched_epoch")}
        snap["sol_px_usd"] = sol_px
        if prev:
            db.insert(conn, "repeats", {"scan_id": scan_id, "detected_epoch": detected, "mint": mint,
                                        "pool": p["pool"], "first_event_id": prev["event_id"],
                                        "metrics_json": json.dumps(snap, default=str)})
            repeats += 1
            continue
        # control candidates: same scan, universe_pre, NOT surging, no event in last 6h, distinct mint
        recent = {r["mint"] for r in conn.execute(
            "SELECT mint FROM events WHERE detected_epoch>? AND status IN ('gating','active')",
            (detected - C.DEDUPE_S,))}
        surging_mints = {q["mint"] for q in surging}
        cands = {}
        for q in univ_rows:
            if q["mint"] in surging_mints or q["mint"] in recent or q["mint"] == mint:
                continue
            if q["surge"]:
                continue
            c0 = cands.get(q["mint"])
            if c0 is None or (q["reserve_usd"] or 0) > (c0["reserve_usd"] or 0):
                cands[q["mint"]] = q
        cand_list = sorted(({"mint": q["mint"], "pool": q["pool"], "symbol": q["symbol"],
                             "price_usd": q["price_usd"], "reserve_usd": q["reserve_usd"],
                             "vol_ratio": q["vol_ratio"]} for q in cands.values()), key=lambda x: x["mint"])
        origin = p.get("origin_epoch")
        eid = db.insert(conn, "events", {
            "mode": mode, "phase": phase_for(detected, mode), "status": "gating", "scan_id": scan_id,
            "mint": mint, "pool": p["pool"], "symbol": p["symbol"], "name": p["name"], "dex": p["dex"],
            "surge_branch": p["surge_branch"], "detected_epoch": detected, "detected_jst": jst(detected),
            "origin_epoch": origin, "fetched_epoch": p.get("fetched_epoch"),
            "delay_origin_to_detect_s": (detected - origin) if origin else None,
            "snapshot_json": json.dumps(snap, default=str),
            "control_candidates_json": json.dumps(cand_list),
            "config_version": C.VERSION, "config_hash": C.config_hash(),
            "test_note": None if mode == "main" else f"FORCED TEST TRIGGER (overrides={so}, force_top_n={force_top_n})",
        })
        db.add_job(conn, "gate", eid, detected)
        new_events.append(eid)
        log.info("EVENT %s detected mode=%s mint=%s sym=%s pool=%s ratio=%.2f pc_m5=%s branch=%s",
                 eid, mode, mint, p["symbol"], p["pool"], p["vol_ratio"] or -1, p["pc_m5"], p["surge_branch"])

    fin = now()
    db.update(conn, "scans", "scan_id", scan_id, {
        "finished_epoch": fin, "n_req": len(pages), "n_ok": n_ok, "n_err": len(pages) - n_ok, "n_429": n_429,
        "n_pools_unique": len(pools), "n_universe_pre": n_univ, "n_surge_pools": n_surge,
        "n_m5_pc_null": n_m5_null, "n_new_events": len(new_events), "n_repeats": repeats,
        "origin_epoch_median": statistics.median(origins) if origins else None,
        "cdn_age_s_median": statistics.median(ages) if ages else None, "sol_px_usd": sol_px,
        "status": "ok" if n_ok == len(pages) else ("partial" if n_ok else "failed"), "raw_path": raw_path})
    log.info("SCAN %s done %.1fs req=%d ok=%d 429=%d pools=%d univ=%d surge=%d new_events=%d repeats=%d sol=%.3f "
             "retries=%d 429_any_attempt=%d",
             scan_id, fin - started, len(pages), n_ok, n_429, len(pools), n_univ, n_surge, len(new_events),
             repeats, sol_px or -1, n_retry, n_429_any)
    return scan_id, new_events
