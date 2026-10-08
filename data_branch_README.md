# VS-001 collector data (paper study, no trading)

Written only by the `vs001-collect` workflow on the `main` branch. Append-only:

- `data/rows/YYYY-MM-DD/*.jsonl.gz`: one JSON object per line, `{"t": table, "id": rowid, "r": {...}}`.
  Rows that change (e.g. a job finishing) are appended again; the last version of `(t, id)` wins.
- `data/state.json`: trial window, writer lease, pending jobs, max gap between covered scans.

Derived fields only (no raw API responses). On-chain data provided by GeckoTerminal
(https://www.geckoterminal.com). Quote estimates from the Jupiter Metis Swap API (`swap/v1/quote`),
Powered by Jupiter. See README.md on `main` for the data policy and field list.
