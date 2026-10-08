"""VS-001 frozen study parameters (v0.1). Changing any study value here = protocol deviation.

config_hash() covers every UPPER_CASE value below except paths and HTTP headers; the expected value is
736577b44e7c605a (checked by tests/regression.py). Paths come from environment variables so the same code
runs on a GitHub Actions runner or locally.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

VERSION = "VS-001-v0.1"   # v0.1: tokenized/bridged exclusion + liquidity-matched control
# ---- paths (excluded from config_hash). WORK_DIR = scratch on the runner (never committed). ----
ROOT = Path(__file__).resolve().parent.parent
PREREG_JSON = Path(os.environ.get("VS001_T0_FILE", str(ROOT / "study" / "t0.json")))   # t0 only (null = not stamped)
DATA_DIR = Path(os.environ.get("VS001_WORK_DIR", str(ROOT / "work")))
LOG_DIR = DATA_DIR / "logs"
RAW_DIR = DATA_DIR / "raw"                     # raw API bodies: local scratch only, NEVER committed/published
DB_MAIN = DATA_DIR / "vs001.sqlite"            # working DB, rebuilt from the committed row log at every start
DB_TEST = DATA_DIR / "vs001_test.sqlite"       # never created by the runner (kept for code compatibility)
HEARTBEAT = DATA_DIR / "heartbeat.json"
COLLECTOR_PID = DATA_DIR / "collector.pid"
SUPERVISOR_PID = DATA_DIR / "supervisor.pid"
LOCK_FILE = DATA_DIR / "collector.lock"
STOP_FILE = DATA_DIR / "STOP_VS001"
SIDE_THEME_JSONL = DATA_DIR / "side_theme.jsonl"  # side observation; never in main stats

# ---- mints ----
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

# ---- scanner (GeckoTerminal public, keyless) ----
GT_BASE = "https://api.geckoterminal.com/api/v2"
GT_HEADERS = {"Accept": "application/json;version=20230302",
              "User-Agent": "vs001-collector/0.1 (read-only paper research; github.com/Hage3D/vs001-collector)"}
GT_MIN_INTERVAL_S = 2.2          # <= ~27 req/min burst inside a scan; documented public limit ~30/min
SCAN_INTERVAL_S = 300            # target cadence 5 min, aligned to wall-clock 5-min boundaries
SCAN_SOURCES = [                 # (path, params, pages)  GT hard max page = 10 (page 11 -> 401)
    ("networks/solana/pools", {"sort": "h24_volume_usd_desc"}, 10),
    ("networks/solana/pools", {"sort": "h24_tx_count_desc"}, 10),
    ("networks/solana/trending_pools", {"duration": "5m"}, 10),
    ("networks/solana/trending_pools", {"duration": "1h"}, 5),
]

# ---- universe (evaluated per scan, saved per event) ----
MIN_RESERVE_USD = 50_000.0
MIN_VOL_H24_USD = 100_000.0
MIN_POOL_AGE_H = 24.0
EXCLUDE_MINTS = {
    SOL_MINT, USDC_MINT, USDT_MINT,
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
    "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1",   # bSOL
    "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",  # jitoSOL
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",  # stSOL
    "5oVNBeEEQvYi1cX3ir8Dx5n1P7pdxydbGF2X4TxVusJm",  # INF
    "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v",   # jupSOL
    "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",  # PYUSD
    "USD1ttGY1N17NEEHLmELoaybftRJJOJwHNWUcb6ZTPc",   # USD1
    "3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh",  # WBTC (wormhole)
    "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij",   # cbBTC
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # WETH (wormhole)
}
EXCLUDE_SYMBOLS = {
    "SOL", "WSOL", "USDC", "USDT", "USD1", "PYUSD", "USDP", "DAI", "UXD", "USDH", "USDY", "EURC",
    "USDS", "USDE", "SUSDE", "FDUSD", "USDG", "AUSD", "USX", "USDU", "CASH",
    "MSOL", "BSOL", "JITOSOL", "STSOL", "JSOL", "BONKSOL", "HSOL", "INF", "JUPSOL", "VSOL",
    "LAINESOL", "CGNTSOL", "DSOL", "BNSOL", "HUBSOL", "PICOSOL", "STRONGSOL", "SSOL", "LST",
    "WBTC", "CBBTC", "ZBTC", "TBTC", "XBTC", "WETH", "ETH",
}
# v0.1 (pre-t0 decision): tokenized equities / pre-IPO tokens / bridged non-Solana majors are not
# "small Solana tokens"; their price is set off-chain or on another chain. Excluded from the universe
# (hence from events AND controls). Symbol-based (GT base symbol, case-sensitive regex / upper-case set).
TOKENIZED_SYMBOL_REGEXES = [r"^[A-Z0-9]{1,6}x$",          # xStocks e.g. GOOGLx NVDAx SPYx SPCXx
                            r"^t[A-Z][A-Za-z0-9]+$"]      # pre-IPO tokens e.g. tSpaceX tOpenAI tKalshi
TOKENIZED_SYMBOLS = {"SPCX"}
BRIDGED_MAJOR_SYMBOLS = {"ZEC", "HYPE", "TRX", "INJ", "WNEAR", "NEAR", "DOGE", "PEPE", "BNB", "XRP", "ADA", "AVAX",
                         "SUI", "TON", "LTC", "BCH", "LINK", "DOT", "APT", "ATOM", "XLM", "ETC", "XMR", "SHIB"}
# heuristics (data reason: stables/LSTs/wrapped majors not all enumerable by mint/symbol)
STABLE_PRICE_BAND = (0.97, 1.03)     # symbol containing 'USD' AND price in band -> stable
LST_SOL_RATIO_BAND = (0.95, 1.6)     # symbol ending 'SOL' AND price/SOL in band -> LST

# ---- surge rule v0 ----
SURGE_VOL_MULT = 5.0                 # vol_m5 >= 5 * (vol_h6 / 72)
SURGE_PC_M5_MIN = 2.0                # price_change_percentage.m5 >= +2%
SURGE_PC_H1_FALLBACK = 5.0           # used ONLY when m5 price change field is null for that pool
USE_H1_FALLBACK_WHEN_M5_NULL = True
DEDUPE_S = 6 * 3600                  # same mint -> one event per 6h, first wins

# ---- quotes (Jupiter classic swap/v1, keyless) ----
JUP_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
JUP_HEADERS = {"Accept": "application/json",
               "User-Agent": "vs001-collector/0.1 (read-only quote; github.com/Hage3D/vs001-collector)"}
JUP_MIN_INTERVAL_S = 3.0             # 20 req/min = 2/3 of measured keyless bucket (5 req / 10 s ~ 30 RPM)
JUP_SLIPPAGE_BPS = 50                # quote param only; does not change outAmount estimate
SIZE_USDC_RAW = 5_000_000            # $5 USDC
ENTRY_DELAY_S = 300                  # planned entry = detection + 5 min
HOLD_S = 7200                        # planned exit = actual entry quote time + 2 h
# Frozen protocol parameter: Jupiter quiet window(s) (JST) during which no quote calls are made; jobs due inside
# the window run afterwards and are flagged late by the normal lateness rules.
JUP_QUIET_WINDOWS_JST = [("2026-10-10 09:00:00", "2026-10-10 09:30:00")]

# lateness / retry policy
ENTRY_PROTOCOL_LATE_S = 120          # entry quote later than due+2min -> protocol_ok=0 (kept, sensitivity only)
ENTRY_MAX_LATE_S = 3600              # later than due+60min -> 'missed' (no quote taken)
EXIT_PROTOCOL_LATE_S = 300           # exit later than due+5min -> protocol_ok=0
EXIT_MAX_LATE_S = 24 * 3600          # exits are always attempted up to 24h late (flagged)
DATA_RETRY_S = 30                    # 429/5xx/timeout retry spacing
DATA_RETRY_MAX = 8
NO_ROUTE_RETRY_DELAYS_S = [60, 180, 600]   # exit/entry no-route retries before final classification
GATE_DATA_RETRY_MAX = 4
CONTROL_MAX_ATTEMPTS = 5
CONTROL_LIQ_MAX_ATTEMPTS = 5         # v0.1 liquidity-matched control: nearest |ln reserve| first, seeded tie-break
PRIMARY_COMPARISON = "event_minus_control_liq"          # v0.1 decision
SECONDARY_COMPARISONS = ["event_minus_control_random", "event_minus_sol"]

# ---- costs ----
NET_FEE_LAMPORTS_PER_SWAP = 10_000   # conservative; a measured classic-route swap fee was 5,275
NET_FEE_LAMPORTS_PER_SWAP_MEASURED = 5_275
SWAPS_PER_ROUNDTRIP = 2
ATA_RENT_LAMPORTS_NOTE = 2_039_280   # SPL ATA rent; recoverable on close -> EXCLUDED from PnL, noted

# ---- control RNG ----
CONTROL_GLOBAL_SEED = 20261008

# ---- side observation (theme / related tokens) ----
SIDE_MAX_RELATED = 15
SIDE_MIN_SYMBOL_LEN_FOR_CONTAINS = 4


def frozen_params() -> dict:
    keys = [k for k in globals() if k.isupper() and not k.startswith("GT_HEADERS") and k not in (
        "ROOT", "PREREG_JSON", "DATA_DIR", "LOG_DIR", "RAW_DIR", "DB_MAIN", "DB_TEST", "HEARTBEAT",
        "COLLECTOR_PID", "SUPERVISOR_PID", "LOCK_FILE", "STOP_FILE", "SIDE_THEME_JSONL", "JUP_HEADERS")]
    out = {}
    for k in sorted(keys):
        v = globals()[k]
        if isinstance(v, set):
            v = sorted(v)
        out[k] = v
    return out


def config_hash() -> str:
    return hashlib.sha256(json.dumps(frozen_params(), sort_keys=True, default=str).encode()).hexdigest()[:16]


def read_t0_epoch():
    """t0 from study/t0.json ('t0_epoch' or 't0_jst'). None (default) => events are phase 'pre_t0_shakedown'."""
    try:
        d = json.loads(PREREG_JSON.read_text(encoding="utf-8"))
    except Exception:
        return None
    t0 = d.get("t0_epoch")
    if t0:
        return float(t0)
    s = d.get("t0_jst")
    if s:
        from datetime import datetime, timedelta, timezone
        try:
            return datetime.strptime(s.replace(" JST", ""), "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone(timedelta(hours=9))).timestamp()
        except Exception:
            return None
    return None
