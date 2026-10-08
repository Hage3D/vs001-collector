"""Shared helpers: time, rate-limited HTTP GET (thread-safe), logging."""
from __future__ import annotations

import email.utils
import json
import logging
import logging.handlers
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from . import config as C

JST = timezone(timedelta(hours=9))


def now() -> float:
    return time.time()


def jst(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, JST).strftime("%Y-%m-%d %H:%M:%S JST")


def utc_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds")


def parse_jst(s: str) -> float:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=JST).timestamp()


def in_jup_quiet_window(ts: float | None = None) -> tuple[bool, float | None]:
    ts = ts or now()
    for a, b in C.JUP_QUIET_WINDOWS_JST:
        ta, tb = parse_jst(a), parse_jst(b)
        if ta <= ts < tb:
            return True, tb
    return False, None


class RateLimiter:
    """Minimum spacing between calls + honour server reset hints. Thread-safe."""

    def __init__(self, min_interval_s: float, name: str):
        self.min_interval = min_interval_s
        self.name = name
        self._lock = threading.Lock()
        self._next_ok = 0.0
        self.stats = {"calls": 0, "http_429": 0, "errors": 0}

    def wait(self):
        with self._lock:
            t = time.time()
            if t < self._next_ok:
                time.sleep(self._next_ok - t)
            self._next_ok = time.time() + self.min_interval
            self.stats["calls"] += 1

    def push_back(self, until_epoch: float):
        with self._lock:
            self._next_ok = max(self._next_ok, until_epoch)


GT_LIMITER = RateLimiter(C.GT_MIN_INTERVAL_S, "geckoterminal")
JUP_LIMITER = RateLimiter(C.JUP_MIN_INTERVAL_S, "jupiter")


class HttpResult:
    __slots__ = ("url", "status", "body", "error", "headers", "t_req", "t_resp", "latency_ms", "attempts",
                 "attempt_errors")

    def __init__(self, url):
        self.url = url
        self.status = None
        self.body = None
        self.error = None
        self.headers = {}
        self.t_req = None
        self.t_resp = None
        self.latency_ms = None
        self.attempts = 0
        self.attempt_errors = []   # errors of earlier (retried) attempts; final error stays in .error

    @property
    def ok(self):
        return self.status == 200 and self.body is not None

    def origin_epoch(self):
        """Data generation time estimate = HTTP Date - Age (CDN cache age)."""
        d = self.headers.get("date")
        if not d:
            return None
        try:
            t = email.utils.parsedate_to_datetime(d).timestamp()
        except Exception:
            return None
        try:
            age = float(self.headers.get("age") or 0)
        except ValueError:
            age = 0.0
        return t - age


# HTTP robustness (ops-only; no study rule touched):
#  - urllib's `timeout` is per socket operation (connect / each recv), so a trickling body could exceed it.
#    HTTP_TOTAL_DEADLINE_S additionally caps the whole body read. (DNS getaddrinfo is bounded by the system
#    resolver: /etc/resolv.conf defaults = 5 s x 2 attempts.)
#  - optional in-call retries with backoff for TRANSIENT errors only (network/DNS/TLS, 429, 5xx). Each retry
#    goes through the same RateLimiter (pacing unchanged). If all attempts fail the result is still a failure
#    (status/error of the last attempt) -> callers record it as a failure exactly as before; nothing zero-filled.
HTTP_TOTAL_DEADLINE_S = 45.0
RETRY_BACKOFF_S = (4.0, 10.0)        # sleep before retry #1, #2 (429 additionally honours server reset hint)


def _transient(r: HttpResult) -> bool:
    # network/DNS/TLS/timeout (no status), 429, 5xx, or a 200 whose body could not be read/parsed
    return r.status is None or r.status == 429 or r.status >= 500 or (r.status == 200 and r.body is None)


def _http_once(r: HttpResult, req, limiter: RateLimiter, timeout: float):
    """One attempt. Fills r.status/headers/body/error. Never raises."""
    r.status, r.body, r.error, r.headers = None, None, None, {}
    t_start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            r.status = resp.status
            r.headers = {k.lower(): v for k, v in resp.headers.items()}
            chunks = []
            while True:
                if time.time() - t_start > HTTP_TOTAL_DEADLINE_S:
                    raise TimeoutError(f"total deadline {HTTP_TOTAL_DEADLINE_S:.0f}s exceeded while reading body")
                b = resp.read(65536)
                if not b:
                    break
                chunks.append(b)
            r.body = json.loads(b"".join(chunks))
    except urllib.error.HTTPError as e:
        r.status = e.code
        r.headers = {k.lower(): v for k, v in (e.headers.items() if e.headers else [])}
        try:
            txt = e.read().decode("utf-8", "replace")
            try:
                r.body = None
                r.error = json.loads(txt)
            except Exception:
                r.error = txt[:500]
        except Exception as ee:  # noqa: BLE001
            r.error = str(ee)
        if e.code == 429:
            limiter.stats["http_429"] += 1
            reset = r.headers.get("x-ratelimit-reset") or r.headers.get("retry-after")
            until = time.time() + 15
            try:
                v = float(reset)
                until = v + 1 if v > 1e9 else time.time() + v + 1
            except (TypeError, ValueError):
                pass
            limiter.push_back(min(until, time.time() + 90))
    except Exception as e:  # noqa: BLE001  timeout / DNS / TLS / JSON
        r.body = None
        r.error = f"{type(e).__name__}: {e}"
        limiter.stats["errors"] += 1


def http_get(url: str, headers: dict, limiter: RateLimiter, timeout: int = 25, retries: int = 0) -> HttpResult:
    """GET with explicit socket timeout + total body deadline. `retries` > 0 retries transient failures only."""
    r = HttpResult(url)
    req = urllib.request.Request(url, headers=headers, method="GET")
    for attempt in range(retries + 1):
        if attempt:
            r.attempt_errors.append({"status": r.status, "error": str(r.error)[:300] if r.error else None,
                                     "latency_ms": r.latency_ms})
            limiter.stats["retries"] = limiter.stats.get("retries", 0) + 1
            time.sleep(RETRY_BACKOFF_S[min(attempt - 1, len(RETRY_BACKOFF_S) - 1)])
        limiter.wait()
        r.t_req = time.time()
        r.attempts = attempt + 1
        _http_once(r, req, limiter, timeout)
        r.t_resp = time.time()
        r.latency_ms = int((r.t_resp - r.t_req) * 1000)
        if r.ok or not _transient(r):
            break
    # proactive back-off when Jupiter says the window is nearly exhausted
    if limiter is JUP_LIMITER and r.headers.get("x-ratelimit-remaining") is not None:
        try:
            if int(r.headers["x-ratelimit-remaining"]) <= 1:
                rs = float(r.headers.get("x-ratelimit-reset") or 0)
                if rs > time.time():
                    limiter.push_back(min(rs + 0.5, time.time() + 15))
        except ValueError:
            pass
    return r


def gt_get(path: str, params: dict | None = None, retries: int = 0) -> HttpResult:
    url = f"{C.GT_BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return http_get(url, C.GT_HEADERS, GT_LIMITER, retries=retries)


class PauseDetector:
    """Detects host pauses / process stalls: wall clock jumped forward between two loop iterations.

    wall_delta - monotonic_delta ~= time the host was suspended (kind='host_pause'); a large wall jump with an
    equally large monotonic delta means the process itself was stalled (kind='stall'). Recorded as a failure row.
    """

    def __init__(self, jump_s: float = 300.0):
        self.jump_s = jump_s
        self.last_wall = time.time()
        self.last_mono = time.monotonic()

    def check(self):
        w, m = time.time(), time.monotonic()
        dw, dm = w - self.last_wall, m - self.last_mono
        out = None
        if dw > self.jump_s:
            out = {"from_epoch": self.last_wall, "to_epoch": w, "from_jst": jst(self.last_wall), "to_jst": jst(w),
                   "wall_delta_s": round(dw, 1), "mono_delta_s": round(dm, 1),
                   "unaccounted_s": round(dw - dm, 1), "kind": "host_pause" if (dw - dm) > self.jump_s / 2 else "stall"}
        self.last_wall, self.last_mono = w, m
        return out


def setup_logging(name: str = "vs001", filename: str = "collector.log") -> logging.Logger:
    C.LOG_DIR.mkdir(parents=True, exist_ok=True)
    lg = logging.getLogger(name)
    if lg.handlers:
        return lg
    lg.setLevel(logging.INFO)
    fh = logging.handlers.RotatingFileHandler(C.LOG_DIR / filename, maxBytes=10_000_000, backupCount=10)
    fmt = logging.Formatter("%(asctime)s JST %(levelname)s [%(threadName)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh.setFormatter(fmt)
    lg.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    lg.addHandler(sh)
    return lg
