# vs001-collector

Read-only **paper study** collector (VS-001: "small-token volume surge with price rise on Solana — does a
move remain after realistic detection delay and round-trip cost?"). It runs on free GitHub-hosted Actions
runners and commits its observations to the `data` branch.

**No trading.** No wallet, no keys, no signing, no swap submission, no paid API, no repository secrets.
Quotes are estimates from public quote endpoints, never fills. Nothing here is investment advice.

## What it does (protocol summary, frozen; `config_hash` = `736577b44e7c605a`)

* Every wall-clock 5-min slot: scan the public GeckoTerminal API (Solana pool lists: top by 24h volume
  p1–10, top by 24h tx count p1–10, trending 5m p1–10, trending 1h p1–5; 35 requests at ≥2.2 s spacing).
* Universe: reserve ≥ $50k, 24h volume ≥ $100k, pool age ≥ 24 h; majors / stables / LSTs / wrapped,
  tokenized-equity and bridged-major symbols excluded.
* Surge: `volume_m5 ≥ 5 × volume_h6/72` AND `price_change_m5 ≥ +2 %` (h1 ≥ +5 % only if m5 is null).
  One event per mint per 6 h (first wins).
* Gate at detection: $5 USDC→token quote and sell-back quote must both succeed.
* Entry at detection + 5 min: $5 USDC→token quote (+ immediate sell-back quote = round-trip cost).
  Exit at **actual** entry quote time + 2 h: quote selling exactly the entry amount back to USDC.
* Comparisons: SOL leg; liquidity-matched control (nearest |ln reserve|, seeded); random control (seeded).
* Lateness rules (all measured at the **actual fetch time**): entry `protocol_ok` if ≤ 120 s late, > 60 min
  late ⇒ `missed` (no quote taken); exit `protocol_ok` if ≤ 300 s late, exits attempted up to 24 h late.
  No-route at exit after retries (+1/+3/+10 min) ⇒ `unsellable` = total loss, kept. API errors are recorded
  as failures and are **never zero-filled**.

## How it runs on Actions

`.github/workflows/collect.yml`:

* Triggers: cron every 30 min (`7,37 * * * *`), push to `main` (code updates), `workflow_dispatch`.
* `concurrency: {group: vs001, cancel-in-progress: false}`: at most one run active and one queued. A run
  loops up to 340 min (job `timeout-minutes: 355`, under the 6 h cap) and soft-stops a few minutes early
  right after a completed scan when no entry/exit is due soon; the queued run then takes over (≈1–3 min).
* No third-party actions: plain `git` + system `python3` (stdlib only, no dependencies).
* Permissions: `contents: write` only (automatic `GITHUB_TOKEN`), used to push the `data` branch.
* Trial window: `VS001_TRIAL_HOURS=24` counted from the first run (stored in `data/state.json`). After
  it, no new scans; pending entry/exit jobs are drained for up to `VS001_DRAIN_HOURS=3`, then runs exit
  immediately. Disable the workflow afterwards.

### Timing honesty (Actions does not guarantee 5-min spacing)

Every scan attempt writes a `cycles` row: scheduled slot time, actual fetch start / end (UTC + JST),
start lag, GitHub run id / attempt / runner name, pages requested / ok / failed / 429, `covered`
(≥ 90 % pages ok) and the gap since the previous covered scan. Slots with no attempt (run hand-over,
dropped cron, runner outage) are written as explicit `missed_no_runner` / `missed_runner_alive` rows.
Every GeckoTerminal request has a `scan_requests` row (status, ok, attempts, timing, error class).
`gha_runs` records each run (trigger time from the public API when available, start/end, end reason,
overdue jobs at start, commits ok/failed).

### State across runs

The working SQLite DB lives only on the runner. Durable state is the append-only row log on the `data`
branch (`data/rows/YYYY-MM-DD/*.jsonl.gz`, one `{"t","id","r"}` object per line; changed rows are
appended again, last version wins). Every start rebuilds the DB from the log, so pending gate / control /
entry / exit jobs survive run boundaries; overdue jobs execute late with their lateness recorded
(entries > 60 min late become `missed`). The log is exported and pushed after every scan cycle (and at
least every 5 min) with `pull --rebase` retries; `data/state.json` carries a single-writer lease.

## Data policy and attribution

* **On-chain data provided by GeckoTerminal** — https://www.geckoterminal.com (Powered by CoinGecko API).
* **Quote estimates from the Jupiter Metis Swap API** (`api.jup.ag/swap/v1/quote`, keyless) —
  Powered by Jupiter. These are Metis routing-engine quotes, not the jup.ag product and not fills.

Raw API responses are **not** published (they stay in runner scratch space and are discarded). Only the
derived fields the study needs are committed: timestamps, pool / mint ids, symbol, price, volume,
liquidity (reserve), price-change, pool age, surge ratio for universe pools; quote out-amounts,
price impact, route labels, ok / fail and error class; job / leg statuses and lateness. Jupiter request
latency, rate-limit headers and raw quote JSON are never exported. Neither provider's terms grant an
explicit right to redistribute API data; this repository is a non-commercial research log, data is
provided as-is, and it will be removed on request of either provider.

## Repository layout

```
.github/workflows/collect.yml   workflow (cron + dispatch, concurrency, bounded loop)
vs001/config.py                 frozen study parameters (config_hash checked by tests)
vs001/scanner.py, jobs.py       scan / detection / gate / control / entry / exit logic
vs001/common.py, db.py          HTTP (timeouts, retries, rate limits), SQLite schema
vs001/store.py                  append-only row log export / replay + git commit/push
vs001/runner.py                 bounded GitHub Actions loop
vs001/coverage_report.py        coverage %, max gap, detection delay, entry/exit lateness, failure rates
tests/regression.py             offline regression tests (run before every loop)
study/t0.json                   study start stamp (null during this trial)
```

Local use: `python3 -m tests.regression`;
`python3 -m vs001.runner --data-repo <checkout-of-data-branch> --max-minutes 12` (add `--push` only on
the runner); `python3 vs001/coverage_report.py --db <sqlite>`.
