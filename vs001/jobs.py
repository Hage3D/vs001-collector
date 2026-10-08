"""Job handlers: Jupiter classic quotes (read-only GET) + GT side lookups.

Handlers return (status, next_due_epoch_or_None, result_dict):
  status in {'done', 'retry', 'failed', 'missed'}
Quotes are ESTIMATES (no fills). No signing, no swap submission anywhere in this module.
"""
from __future__ import annotations

import hashlib
import json
import random
import urllib.parse
from datetime import datetime

from . import config as C
from . import db
from .common import JUP_LIMITER, gt_get, http_get, jst, now

NO_ROUTE_HINTS = ("ROUTE", "NOT_TRADABLE", "TOKEN_NOT_TRADABLE", "No routes", "no route", "CIRCULAR",
                  "MARKET", "not tradable", "Could not find")


def _raw_quote_log(rec: dict):
    d = C.RAW_DIR / datetime.now().strftime("%Y-%m-%d")
    d.mkdir(parents=True, exist_ok=True)
    fname = "jup_quotes.jsonl" if str(rec.get("db")) == str(C.DB_MAIN) else "jup_quotes_TEST.jsonl"
    with open(d / fname, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, separators=(",", ":"), default=str) + "\n")


def jup_quote(conn, purpose: str, ref: str, input_mint: str, output_mint: str, amount_raw: int) -> dict:
    params = {"inputMint": input_mint, "outputMint": output_mint, "amount": str(int(amount_raw)),
              "slippageBps": str(C.JUP_SLIPPAGE_BPS), "swapMode": "ExactIn"}
    url = C.JUP_QUOTE_URL + "?" + urllib.parse.urlencode(params)
    r = http_get(url, C.JUP_HEADERS, JUP_LIMITER, timeout=20)
    out = {"ok": False, "err_class": None, "out_amount": None, "price_impact_pct": None, "route": None,
           "http_status": r.status, "ts": r.t_resp, "latency_ms": r.latency_ms, "error": None}
    body = r.body
    if r.status == 200 and isinstance(body, dict) and body.get("outAmount") and int(body["outAmount"]) > 0:
        out["ok"] = True
        out["err_class"] = "ok"
        out["out_amount"] = int(body["outAmount"])
        try:
            out["price_impact_pct"] = float(body.get("priceImpactPct") or 0) * 100.0
        except (TypeError, ValueError):
            out["price_impact_pct"] = None
        rp = body.get("routePlan") or []
        out["route"] = " > ".join(f"{(x.get('swapInfo') or {}).get('label')}({x.get('percent')}%)" for x in rp)
        out["context_slot"] = body.get("contextSlot")
    else:
        err_txt = json.dumps(r.error, default=str) if r.error is not None else json.dumps(body, default=str)
        out["error"] = (err_txt or "")[:800]
        if r.status == 200 and isinstance(body, dict):
            out["err_class"] = "not_quotable"          # R1: HTTP 200 but outAmount missing/0 => not sellable
            out["error"] = ("200 but outAmount missing/0: " + (err_txt or ""))[:800]
        elif r.status == 429:
            out["err_class"] = "rate_limit"
        elif r.status in (400, 404, 422):
            out["err_class"] = "no_route" if any(h.lower() in (err_txt or "").lower() for h in NO_ROUTE_HINTS) \
                else "not_quotable"
        elif r.status is None:
            out["err_class"] = "network"
        elif r.status >= 500:
            out["err_class"] = "server"
        else:
            out["err_class"] = "bad_response"
    qid = db.insert(conn, "quotes", {
        "ts_epoch": r.t_resp, "purpose": purpose, "ref": ref, "input_mint": input_mint, "output_mint": output_mint,
        "amount_in": str(int(amount_raw)), "http_status": r.status, "ok": int(out["ok"]), "err_class": out["err_class"],
        "out_amount": str(out["out_amount"]) if out["out_amount"] is not None else None,
        "price_impact_pct": out["price_impact_pct"], "route": out["route"], "context_slot": out.get("context_slot"),
        "latency_ms": r.latency_ms, "error": out["error"], "raw_json": json.dumps(body, default=str) if body else None,
        "req_epoch": r.t_req, "attempts": r.attempts})
    out["quote_id"] = qid
    _raw_quote_log({"quote_id": qid, "db": str(conn_path(conn)), "purpose": purpose, "ref": ref,
                    "ts": r.t_resp, "ts_jst": jst(r.t_resp), "params": params, "status": r.status,
                    "body": body, "error": out["error"],
                    "ratelimit": {k: r.headers.get(k) for k in ("x-ratelimit-remaining", "x-ratelimit-reset")}})
    return out


def conn_path(conn):
    try:
        return conn.execute("PRAGMA database_list").fetchone()[2]
    except Exception:
        return "?"


def is_data_err(q):
    return q["err_class"] in ("rate_limit", "network", "server", "bad_response")


def latest_sol_px(conn):
    r = conn.execute("SELECT sol_px_usd FROM scans WHERE sol_px_usd IS NOT NULL ORDER BY scan_id DESC LIMIT 1").fetchone()
    if r:
        return r[0]
    # test DB may have no scan with SOL row; fall back to main DB value
    try:
        from . import db as _db
        m = _db.connect(C.DB_MAIN)
        r = m.execute("SELECT sol_px_usd FROM scans WHERE sol_px_usd IS NOT NULL ORDER BY scan_id DESC LIMIT 1").fetchone()
        m.close()
        return r[0] if r else None
    except Exception:
        return None


def _usd(raw):
    return raw / 1e6


# ------------------------------------------------------------------------------------------- gate
def h_gate(conn, job, log):
    ev = conn.execute("SELECT * FROM events WHERE event_id=?", (job["ref_id"],)).fetchone()
    if ev is None or ev["status"] != "gating":
        return "done", None, {"skip": "event not gating"}
    # indexer recency: latest trade time for the pool (GT, best effort)
    latest_trade = ev["latest_trade_epoch"]
    if latest_trade is None:
        tr = gt_get(f"networks/solana/pools/{ev['pool']}/trades")
        if tr.ok:
            ts = []
            for t in (tr.body.get("data") or []):
                bt = (t.get("attributes") or {}).get("block_timestamp")
                if bt:
                    try:
                        ts.append(datetime.fromisoformat(bt.replace("Z", "+00:00")).timestamp())
                    except ValueError:
                        pass
            latest_trade = max(ts) if ts else None
            if latest_trade:
                db.update(conn, "events", "event_id", ev["event_id"], {
                    "latest_trade_epoch": latest_trade,
                    "delay_trade_to_detect_s": ev["detected_epoch"] - latest_trade})
        else:
            db.failure(conn, "gate", ev["event_id"], "gt_trades_fetch_fail", {"status": tr.status, "err": tr.error})
    ref = f"event:{ev['event_id']}"
    buy = jup_quote(conn, "gate_buy", ref, C.USDC_MINT, ev["mint"], C.SIZE_USDC_RAW)
    if not buy["ok"]:
        if is_data_err(buy) and job["attempts"] < C.GATE_DATA_RETRY_MAX:
            return "retry", now() + C.DATA_RETRY_S, {"buy": buy["err_class"]}
        st = "gate_data_gap" if is_data_err(buy) else "rejected_gate_buy_" + buy["err_class"]
        db.update(conn, "events", "event_id", ev["event_id"], {"status": st, "status_reason": buy["error"],
                                                                "gate_epoch": buy["ts"],
                                                                "gate_buy_quote_id": buy["quote_id"]})
        db.failure(conn, "gate", ev["event_id"], st, buy["error"])
        return "failed", None, {"status": st}
    sell = jup_quote(conn, "gate_sell", ref, ev["mint"], C.USDC_MINT, buy["out_amount"])
    if not sell["ok"]:
        if is_data_err(sell) and job["attempts"] < C.GATE_DATA_RETRY_MAX:
            return "retry", now() + C.DATA_RETRY_S, {"sell": sell["err_class"]}
        st = "gate_data_gap" if is_data_err(sell) else "rejected_gate_sell_" + sell["err_class"]
        db.update(conn, "events", "event_id", ev["event_id"], {"status": st, "status_reason": sell["error"],
                                                                "gate_epoch": sell["ts"],
                                                                "gate_buy_quote_id": buy["quote_id"],
                                                                "gate_sell_quote_id": sell["quote_id"]})
        db.failure(conn, "gate", ev["event_id"], st, sell["error"])
        return "failed", None, {"status": st}
    gate_usdc = _usd(sell["out_amount"])
    db.update(conn, "events", "event_id", ev["event_id"], {
        "status": "active", "gate_epoch": buy["ts"], "delay_detect_to_gate_s": buy["ts"] - ev["detected_epoch"],
        "gate_buy_out_raw": str(buy["out_amount"]), "gate_sell_usdc": gate_usdc,
        "gate_rt_cost_pct": (gate_usdc / _usd(C.SIZE_USDC_RAW) - 1) * 100,
        "gate_buy_quote_id": buy["quote_id"], "gate_sell_quote_id": sell["quote_id"]})
    snap = json.loads(ev["snapshot_json"])
    due = ev["detected_epoch"] + C.ENTRY_DELAY_S
    sol_px = snap.get("sol_px_usd") or latest_sol_px(conn)
    lid = db.insert(conn, "legs", {"event_id": ev["event_id"], "leg_type": "event", "mint": ev["mint"],
                                   "pool": ev["pool"], "symbol": ev["symbol"], "det_price_usd": snap.get("price_usd"),
                                   "det_out_raw": str(buy["out_amount"]), "sol_px_usd_det": sol_px,
                                   "entry_due_epoch": due, "entry_status": "pending"})
    db.add_job(conn, "entry", lid, due)
    sid = db.insert(conn, "legs", {"event_id": ev["event_id"], "leg_type": "sol", "mint": C.SOL_MINT,
                                   "pool": None, "symbol": "SOL", "det_price_usd": sol_px, "sol_px_usd_det": sol_px,
                                   "entry_due_epoch": due, "entry_status": "pending"})
    db.add_job(conn, "entry", sid, due)
    db.add_job(conn, "control_select", ev["event_id"], now())
    db.add_job(conn, "control_liq_select", ev["event_id"], now())
    db.add_job(conn, "side_theme", ev["event_id"], now())
    log.info("GATE ok event=%s rt_cost=%.3f%% entry_due=%s legs=%s,%s", ev["event_id"],
             (gate_usdc / 5 - 1) * 100, jst(due), lid, sid)
    return "done", None, {"event_leg": lid, "sol_leg": sid, "rt_cost_pct": (gate_usdc / 5 - 1) * 100}


# ---------------------------------------------------------------------------------- control select
def h_control_select(conn, job, log):
    ev = conn.execute("SELECT * FROM events WHERE event_id=?", (job["ref_id"],)).fetchone()
    cands = json.loads(ev["control_candidates_json"] or "[]")
    seed = int(hashlib.sha256(f"{C.CONTROL_GLOBAL_SEED}:{ev['mode']}:{ev['event_id']}".encode()).hexdigest(), 16) % (2 ** 32)
    order = list(cands)
    random.Random(seed).shuffle(order)
    attempts = json.loads(ev["control_attempts_json"] or "[]")
    tried = {a["mint"] for a in attempts}
    due = ev["detected_epoch"] + C.ENTRY_DELAY_S
    for c in order:
        if len(attempts) >= C.CONTROL_MAX_ATTEMPTS:
            break
        if c["mint"] in tried:
            continue
        ref = f"control:{ev['event_id']}"
        buy = jup_quote(conn, "control_gate_buy", ref, C.USDC_MINT, c["mint"], C.SIZE_USDC_RAW)
        att = {"mint": c["mint"], "symbol": c["symbol"], "pool": c["pool"], "buy": buy["err_class"],
               "buy_quote_id": buy["quote_id"]}
        if buy["ok"]:
            sell = jup_quote(conn, "control_gate_sell", ref, c["mint"], C.USDC_MINT, buy["out_amount"])
            att.update(sell=sell["err_class"], sell_quote_id=sell["quote_id"])
            if sell["ok"]:
                att["accepted"] = True
                attempts.append(att)
                lid = db.insert(conn, "legs", {"event_id": ev["event_id"], "leg_type": "control", "mint": c["mint"],
                                               "pool": c["pool"], "symbol": c["symbol"],
                                               "det_price_usd": c.get("price_usd"), "det_out_raw": str(buy["out_amount"]),
                                               "sol_px_usd_det": latest_sol_px(conn), "entry_due_epoch": due,
                                               "entry_status": "pending",
                                               "note": json.dumps({"control_rt_cost_pct_at_select": (_usd(sell["out_amount"]) / 5 - 1) * 100})})
                db.add_job(conn, "entry", lid, due)
                db.update(conn, "events", "event_id", ev["event_id"], {"control_seed": seed,
                                                                        "control_attempts_json": json.dumps(attempts)})
                log.info("CONTROL event=%s -> %s (%s) leg=%s seed=%s attempts=%d", ev["event_id"], c["symbol"],
                         c["mint"], lid, seed, len(attempts))
                return "done", None, {"control_leg": lid, "mint": c["mint"]}
        attempts.append(att)
        if is_data_err(buy):
            # data error is not a property of the candidate; stop and retry later rather than burn candidates
            attempts.pop()
            db.update(conn, "events", "event_id", ev["event_id"], {"control_seed": seed,
                                                                    "control_attempts_json": json.dumps(attempts)})
            if job["attempts"] < C.DATA_RETRY_MAX:
                return "retry", now() + C.DATA_RETRY_S, {"data_err": buy["err_class"]}
            break
    db.update(conn, "events", "event_id", ev["event_id"], {"control_seed": seed,
                                                            "control_attempts_json": json.dumps(attempts)})
    db.failure(conn, "control_select", ev["event_id"], "control_unavailable",
               {"n_candidates": len(cands), "attempts": attempts})
    return "failed", None, {"control": None, "n_candidates": len(cands)}


# ------------------------------------------------------------------- liquidity-matched control (v0.1)
def h_control_liq_select(conn, job, log):
    """Nearest |ln(reserve_usd)| to the event pool among same-scan non-surge universe candidates; seeded tie-break."""
    import math
    ev = conn.execute("SELECT * FROM events WHERE event_id=?", (job["ref_id"],)).fetchone()
    snap = json.loads(ev["snapshot_json"])
    ev_res = snap.get("reserve_usd")
    cands = [c for c in json.loads(ev["control_candidates_json"] or "[]") if (c.get("reserve_usd") or 0) > 0]
    seed = int(hashlib.sha256(f"{C.CONTROL_GLOBAL_SEED}:liq:{ev['mode']}:{ev['event_id']}".encode()).hexdigest(), 16) % (2 ** 32)
    rng = random.Random(seed)
    keyed = [(abs(math.log(c["reserve_usd"]) - math.log(ev_res)), rng.random(), c) for c in cands] if ev_res else []
    keyed.sort(key=lambda x: (x[0], x[1]))
    attempts = json.loads(ev["control_liq_attempts_json"] or "[]") if "control_liq_attempts_json" in ev.keys() else []
    tried = {a["mint"] for a in attempts}
    due = ev["detected_epoch"] + C.ENTRY_DELAY_S
    upd = {"control_liq_seed": seed, "event_reserve_usd": ev_res}
    for dist, _tb, c in keyed:
        if len(attempts) >= C.CONTROL_LIQ_MAX_ATTEMPTS:
            break
        if c["mint"] in tried:
            continue
        ref = f"control_liq:{ev['event_id']}"
        buy = jup_quote(conn, "control_liq_gate_buy", ref, C.USDC_MINT, c["mint"], C.SIZE_USDC_RAW)
        if is_data_err(buy):
            upd["control_liq_attempts_json"] = json.dumps(attempts)
            db.update(conn, "events", "event_id", ev["event_id"], upd)
            if job["attempts"] < C.DATA_RETRY_MAX:
                return "retry", now() + C.DATA_RETRY_S, {"data_err": buy["err_class"]}
            break
        att = {"mint": c["mint"], "symbol": c["symbol"], "pool": c["pool"], "reserve_usd": c["reserve_usd"],
               "abs_ln_reserve_diff": round(dist, 4), "buy": buy["err_class"], "buy_quote_id": buy["quote_id"]}
        if buy["ok"]:
            sell = jup_quote(conn, "control_liq_gate_sell", ref, c["mint"], C.USDC_MINT, buy["out_amount"])
            att.update(sell=sell["err_class"], sell_quote_id=sell["quote_id"])
            if sell["ok"]:
                att["accepted"] = True
                attempts.append(att)
                lid = db.insert(conn, "legs", {"event_id": ev["event_id"], "leg_type": "control_liq", "mint": c["mint"],
                                               "pool": c["pool"], "symbol": c["symbol"], "det_price_usd": c.get("price_usd"),
                                               "det_out_raw": str(buy["out_amount"]), "sol_px_usd_det": latest_sol_px(conn),
                                               "entry_due_epoch": due, "entry_status": "pending",
                                               "note": json.dumps({"control_rt_cost_pct_at_select": (_usd(sell["out_amount"]) / 5 - 1) * 100,
                                                                   "event_reserve_usd": ev_res, "control_reserve_usd": c["reserve_usd"],
                                                                   "abs_ln_reserve_diff": dist})})
                db.add_job(conn, "entry", lid, due)
                upd["control_liq_attempts_json"] = json.dumps(attempts)
                db.update(conn, "events", "event_id", ev["event_id"], upd)
                log.info("CONTROL_LIQ event=%s -> %s (%s) res=%.0f vs event %.0f leg=%s seed=%s attempts=%d", ev["event_id"],
                         c["symbol"], c["mint"], c["reserve_usd"], ev_res, lid, seed, len(attempts))
                return "done", None, {"control_liq_leg": lid, "mint": c["mint"]}
        attempts.append(att)
    upd["control_liq_attempts_json"] = json.dumps(attempts)
    db.update(conn, "events", "event_id", ev["event_id"], upd)
    db.failure(conn, "control_liq_select", ev["event_id"], "control_liq_unavailable",
               {"n_candidates": len(cands), "attempts": attempts})
    return "failed", None, {"control_liq": None, "n_candidates": len(cands)}


def mark_job_failed(conn, job):
    """R2: a job that exhausted exception retries leaves an explicit terminal status on its leg/event."""
    if job["kind"] == "entry":
        conn.execute("UPDATE legs SET entry_status='job_failed' WHERE leg_id=? AND entry_status='pending'", (job["ref_id"],))
    elif job["kind"] == "exit":
        conn.execute("UPDATE legs SET exit_status='job_failed' WHERE leg_id=? AND exit_status='pending'", (job["ref_id"],))
    elif job["kind"] == "gate":
        conn.execute("UPDATE events SET status='gate_job_failed' WHERE event_id=? AND status='gating'", (job["ref_id"],))


# ------------------------------------------------------------------------------------------- entry
def h_entry(conn, job, log):
    leg = conn.execute("SELECT * FROM legs WHERE leg_id=?", (job["ref_id"],)).fetchone()
    t = now()
    late = t - leg["entry_due_epoch"]
    if late > C.ENTRY_MAX_LATE_S:
        reason = f"entry late {late:.0f}s > max {C.ENTRY_MAX_LATE_S}s (process down / queue)"
        db.update(conn, "legs", "leg_id", leg["leg_id"], {"entry_status": "missed", "entry_late_s": late,
                                                          "entry_fail_reason": reason})
        db.failure(conn, "entry", leg["leg_id"], "entry_missed_late", reason)
        return "missed", None, {"reason": reason}
    ref = f"leg:{leg['leg_id']}:{leg['leg_type']}"
    buy = jup_quote(conn, "entry_buy", ref, C.USDC_MINT, leg["mint"], C.SIZE_USDC_RAW)
    if not buy["ok"]:
        if is_data_err(buy) and job["attempts"] < C.DATA_RETRY_MAX:
            return "retry", now() + C.DATA_RETRY_S, {"buy": buy["err_class"]}
        if not is_data_err(buy) and job["attempts"] < len(C.NO_ROUTE_RETRY_DELAYS_S):
            return "retry", now() + C.NO_ROUTE_RETRY_DELAYS_S[job["attempts"]], {"buy": buy["err_class"]}
        st = "failed_data_gap" if is_data_err(buy) else "failed_" + buy["err_class"]
        db.update(conn, "legs", "leg_id", leg["leg_id"], {"entry_status": st, "entry_late_s": late,
                                                          "entry_fail_reason": buy["error"],
                                                          "entry_quote_id": buy["quote_id"]})
        db.failure(conn, "entry", leg["leg_id"], st, buy["error"])
        return "failed", None, {"status": st}
    late = buy["ts"] - leg["entry_due_epoch"]
    rt = jup_quote(conn, "entry_rt_sell", ref, leg["mint"], C.USDC_MINT, buy["out_amount"])
    upd = {"entry_status": "done", "entry_epoch": buy["ts"], "entry_late_s": late,
           "entry_protocol_ok": int(late <= C.ENTRY_PROTOCOL_LATE_S), "entry_out_raw": str(buy["out_amount"]),
           "entry_price_impact_pct": buy["price_impact_pct"], "entry_route": buy["route"],
           "entry_quote_id": buy["quote_id"], "entry_rt_quote_id": rt["quote_id"],
           "exit_due_epoch": buy["ts"] + C.HOLD_S, "exit_status": "pending"}
    if rt["ok"]:
        upd["entry_rt_usdc"] = _usd(rt["out_amount"])
        upd["entry_rt_cost_pct"] = (_usd(rt["out_amount"]) / _usd(C.SIZE_USDC_RAW) - 1) * 100
    else:
        db.failure(conn, "entry_rt", leg["leg_id"], "rt_sell_quote_fail_" + str(rt["err_class"]), rt["error"])
    if leg["det_out_raw"]:
        # price move detection-gate -> entry (unit-free): out-per-$5 fell => price rose
        upd["move_det_to_entry_pct"] = (int(leg["det_out_raw"]) / buy["out_amount"] - 1) * 100
    db.update(conn, "legs", "leg_id", leg["leg_id"], upd)
    jid = db.add_job(conn, "exit", leg["leg_id"], buy["ts"] + C.HOLD_S)
    log.info("ENTRY leg=%s type=%s mint=%s out=%s late=%.0fs rt_cost=%s exit_due=%s exit_job=%s", leg["leg_id"],
             leg["leg_type"], leg["mint"], buy["out_amount"], late, upd.get("entry_rt_cost_pct"),
             jst(buy["ts"] + C.HOLD_S), jid)
    return "done", None, {"out": buy["out_amount"], "late_s": late, "exit_job": jid}


# -------------------------------------------------------------------------------------------- exit
def h_exit(conn, job, log):
    leg = conn.execute("SELECT * FROM legs WHERE leg_id=?", (job["ref_id"],)).fetchone()
    t = now()
    late = t - leg["exit_due_epoch"]
    ref = f"leg:{leg['leg_id']}:{leg['leg_type']}"
    sell = jup_quote(conn, "exit_sell", ref, leg["mint"], C.USDC_MINT, int(leg["entry_out_raw"]))
    n_att = (leg["exit_attempts"] or 0) + 1
    sol_px = latest_sol_px(conn)
    fee2 = C.SWAPS_PER_ROUNDTRIP * C.NET_FEE_LAMPORTS_PER_SWAP / 1e9 * (sol_px or 0)
    gt_px = gt_alive = None
    if leg["leg_type"] in ("event", "control") and leg["pool"]:
        g = gt_get(f"networks/solana/pools/{leg['pool']}")
        if g.ok:
            gt_alive = 1
            try:
                gt_px = float(g.body["data"]["attributes"]["base_token_price_usd"])
            except Exception:  # noqa: BLE001
                gt_px = None
        elif g.status == 404:
            gt_alive = 0
    if sell["ok"]:
        late = sell["ts"] - leg["exit_due_epoch"]
        usdc = _usd(sell["out_amount"])
        pnl = usdc - _usd(C.SIZE_USDC_RAW) - fee2
        db.update(conn, "legs", "leg_id", leg["leg_id"], {
            "exit_status": "done", "exit_epoch": sell["ts"], "exit_late_s": late,
            "exit_protocol_ok": int(late <= C.EXIT_PROTOCOL_LATE_S), "exit_usdc": usdc,
            "exit_price_impact_pct": sell["price_impact_pct"], "exit_route": sell["route"],
            "exit_quote_id": sell["quote_id"], "exit_attempts": n_att, "gt_price_exit_usd": gt_px,
            "gt_pool_alive_exit": gt_alive, "sol_px_usd_exit": sol_px, "est_fee_usd": fee2,
            "est_pnl_usd": pnl, "est_pnl_pct": pnl / _usd(C.SIZE_USDC_RAW) * 100,
            "pnl_basis": "ESTIMATED from Jupiter classic quotes (not fills); fee=2x%d lamports; ATA rent excluded" %
                         C.NET_FEE_LAMPORTS_PER_SWAP})
        log.info("EXIT leg=%s type=%s usdc=%.6f est_pnl=%.4f (%.2f%%) late=%.0fs", leg["leg_id"], leg["leg_type"],
                 usdc, pnl, pnl / 5 * 100, late)
        return "done", None, {"exit_usdc": usdc, "est_pnl_usd": pnl}
    db.update(conn, "legs", "leg_id", leg["leg_id"], {"exit_attempts": n_att, "exit_fail_reason": sell["error"],
                                                      "gt_price_exit_usd": gt_px, "gt_pool_alive_exit": gt_alive})
    if is_data_err(sell):
        if late < C.EXIT_MAX_LATE_S:
            return "retry", now() + (60 if late < 1800 else 300), {"sell": sell["err_class"]}
        db.update(conn, "legs", "leg_id", leg["leg_id"], {"exit_status": "exit_data_gap", "exit_late_s": late})
        db.failure(conn, "exit", leg["leg_id"], "exit_data_gap", sell["error"])
        return "failed", None, {"status": "exit_data_gap"}
    no_route_tries = conn.execute("SELECT COUNT(*) FROM quotes WHERE purpose='exit_sell' AND ref=? AND ok=0 AND "
                                  "err_class IN ('no_route','not_quotable')", (ref,)).fetchone()[0]
    if no_route_tries <= len(C.NO_ROUTE_RETRY_DELAYS_S):
        return "retry", now() + C.NO_ROUTE_RETRY_DELAYS_S[no_route_tries - 1], {"sell": sell["err_class"]}
    fee1 = C.NET_FEE_LAMPORTS_PER_SWAP / 1e9 * (sol_px or 0)
    pnl = -_usd(C.SIZE_USDC_RAW) - fee1
    db.update(conn, "legs", "leg_id", leg["leg_id"], {
        "exit_status": "unsellable_" + sell["err_class"], "exit_epoch": sell["ts"], "exit_late_s": late,
        "exit_protocol_ok": int(late <= C.EXIT_PROTOCOL_LATE_S + sum(C.NO_ROUTE_RETRY_DELAYS_S)),
        "exit_usdc": 0.0, "est_fee_usd": fee1, "est_pnl_usd": pnl, "est_pnl_pct": -100.0,
        "pnl_basis": "UNSELLABLE at exit after retries -> conservative total loss (kept, never dropped)"})
    db.failure(conn, "exit", leg["leg_id"], "unsellable_at_exit", sell["error"])
    log.warning("EXIT UNSELLABLE leg=%s mint=%s -> total loss recorded", leg["leg_id"], leg["mint"])
    return "done", None, {"unsellable": True}


# ------------------------------------------------------------------------------ side observation
def _append_side(rec):
    with open(C.SIDE_THEME_JSONL, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, default=str) + "\n")


def h_side_theme(conn, job, log):
    ev = conn.execute("SELECT * FROM events WHERE event_id=?", (job["ref_id"],)).fetchone()
    sym = (ev["symbol"] or "").upper()
    rel = {}
    for r in conn.execute("SELECT mint,pool,symbol,name,price_usd,reserve_usd FROM pool_snaps WHERE scan_id=? AND mint!=?",
                          (ev["scan_id"], ev["mint"])):
        s2 = (r["symbol"] or "").upper()
        how = None
        if s2 == sym:
            how = "same_symbol_in_scan"
        elif len(sym) >= C.SIDE_MIN_SYMBOL_LEN_FOR_CONTAINS and len(s2) >= C.SIDE_MIN_SYMBOL_LEN_FOR_CONTAINS and \
                (sym in s2 or s2 in sym):
            how = "symbol_contains_in_scan"
        if how and r["mint"] not in rel and r["mint"] not in C.EXCLUDE_MINTS:
            rel[r["mint"]] = {"related_mint": r["mint"], "related_pool": r["pool"], "symbol": r["symbol"],
                              "name": r["name"], "relation": how, "source": "gt_scan", "price_usd_det": r["price_usd"]}
    g = gt_get("search/pools", {"query": ev["symbol"] or "", "network": "solana"})
    if g.ok:
        for p in (g.body.get("data") or []):
            a = p.get("attributes") or {}
            bid = (((p.get("relationships") or {}).get("base_token") or {}).get("data") or {}).get("id") or ""
            m = bid.split("_", 1)[1] if "_" in bid else bid
            if not m or m == ev["mint"] or m in rel or m in C.EXCLUDE_MINTS:
                continue
            try:
                px = float(a.get("base_token_price_usd"))
            except (TypeError, ValueError):
                px = None
            rel[m] = {"related_mint": m, "related_pool": a.get("address"), "symbol": (a.get("name") or "").split(" / ")[0],
                      "name": a.get("name"), "relation": "gt_search_symbol", "source": "gt_search", "price_usd_det": px}
    else:
        db.failure(conn, "side_theme", ev["event_id"], "gt_search_fail", {"status": g.status, "err": g.error})
    rows = list(rel.values())[:C.SIDE_MAX_RELATED]
    t = now()
    for r in rows:
        r.update(event_id=ev["event_id"], det_epoch=t, eval_status="pending")
        db.insert(conn, "side_theme", r)
        _append_side({"kind": "side_theme_det", "db": "main" if ev["mode"] == "main" else "TEST",
                      "event_id": ev["event_id"], "event_mint": ev["mint"], "event_symbol": ev["symbol"],
                      "det_jst": jst(t), **r,
                      "note": "candidates saved BEFORE outcome; same-deployer not available from free GT API"})
    db.add_job(conn, "side_eval", ev["event_id"], ev["detected_epoch"] + C.ENTRY_DELAY_S + C.HOLD_S)
    return "done", None, {"n_related": len(rows)}


def h_side_eval(conn, job, log):
    rows = conn.execute("SELECT * FROM side_theme WHERE event_id=? AND eval_status='pending'", (job["ref_id"],)).fetchall()
    if not rows:
        return "done", None, {"n": 0}
    mints = [r["related_mint"] for r in rows]
    prices = {}
    for i in range(0, len(mints), 30):
        g = gt_get("networks/solana/tokens/multi/" + ",".join(mints[i:i + 30]))
        if not g.ok:
            if job["attempts"] < 5:
                return "retry", now() + 120, {"err": g.status}
            break
        for tkn in g.body.get("data") or []:
            a = tkn.get("attributes") or {}
            try:
                prices[a.get("address")] = float(a.get("price_usd"))
            except (TypeError, ValueError):
                pass
    t = now()
    for r in rows:
        px = prices.get(r["related_mint"])
        st = "done" if px is not None else "no_price"
        db.update(conn, "side_theme", "id", r["id"], {"price_usd_eval": px, "eval_epoch": t, "eval_status": st})
        _append_side({"kind": "side_theme_eval", "db": "main" if str(conn_path(conn)) == str(C.DB_MAIN) else "TEST",
                      "event_id": r["event_id"], "related_mint": r["related_mint"],
                      "symbol": r["symbol"], "relation": r["relation"], "price_usd_det": r["price_usd_det"],
                      "price_usd_eval": px, "eval_jst": jst(t),
                      "move_pct": ((px / r["price_usd_det"] - 1) * 100) if (px and r["price_usd_det"]) else None})
    return "done", None, {"n": len(rows), "priced": len(prices)}


HANDLERS = {"gate": h_gate, "control_select": h_control_select, "control_liq_select": h_control_liq_select, "entry": h_entry, "exit": h_exit,
            "side_theme": h_side_theme, "side_eval": h_side_eval}
JUPITER_KINDS = {"gate", "control_select", "control_liq_select", "entry", "exit"}
