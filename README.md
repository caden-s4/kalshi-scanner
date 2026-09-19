# Kalshi scanner

A full-firehose capture and query pipeline for Kalshi (`elections.kalshi.com`)
public market data:

- **recorder** — subscribes to the public WebSocket channels and writes every
  message to hourly, zstd-compressed NDJSON.
- **compactor** — turns raw NDJSON into partitioned, explicitly-typed Parquet.
- **reader** — a typed query layer over the Parquet (DuckDB, no import step).
- **backfill** — historical trades via the REST API, checkpointed and rate-limited.
- **deployment tooling** — systemd units, hourly compaction + retention, a
  free-space monitor, and verified sync scripts (see `DEPLOY.md`).

Data lives under `KALSHI_DATA_DIR` (default `./data`):
`data/raw/<date>/<hour>.ndjson.zst` and
`data/parquet/type=<type>/date=<date>/<hour>.parquet`.

## Measured load (design target)

On `ticker`+`trade` with no market filter: **~1,100 msg/s**, **~186 MB/hour**
compressed raw, **~135 MB/hour** Parquet, ~13,000 distinct tickers. Recorder RSS
peaked at ~187 MB. That is roughly **4.5 GB/day raw + 3.2 GB/day Parquet**; size
the data volume accordingly.

## Setup

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt   # Windows: .venv/Scripts/python.exe
```

Configuration is read from `.env` (see `config.py`):

| Variable                 | Required | Default        | Meaning                                        |
|--------------------------|----------|----------------|------------------------------------------------|
| `KALSHI_KEY_ID`          | yes      | —              | Kalshi API key id (UUID)                        |
| `KALSHI_PEM_PATH`        | yes      | —              | Path to the RSA private key (`.pem`)            |
| `KALSHI_DATA_DIR`        | no       | `./data`       | Root for raw + parquet data                     |
| `KALSHI_CHANNELS`        | no       | `ticker,trade` | WS channels to subscribe to                     |
| `KALSHI_TICKER_PREFIXES` | no       | (all)          | Only record tickers with these prefixes         |
| `MIN_FREE_GB_START`      | no       | `5`            | Refuse to start below this many GB free         |
| `MIN_FREE_GB_HALT`       | no       | `2`            | Halt cleanly below this many GB free            |
| `MIN_FREE_GB_WARN`       | no       | `10`           | Free-space monitor warning threshold            |
| `RETENTION_MIN_AGE_DAYS` | no       | `7`            | Minimum data age before retention may delete    |

---

## Entry points

### `recorder.py` — the live recorder

```bash
.venv/bin/python recorder.py
```

No flags; everything comes from `.env`. Runs until Ctrl-C / `SIGTERM`, which
triggers a clean drain-and-flush. Refuses to start below `MIN_FREE_GB_START`;
halts cleanly (exit non-zero) below `MIN_FREE_GB_HALT`.

- **Time:** runs continuously.
- **Disk:** ~186 MB/hour raw (see load table). Grows without bound until
  compaction + retention run.

### `run_recorder.py` — recorder supervisor (used by systemd)

```bash
.venv/bin/python run_recorder.py
```

Wraps `recorder.py` and translates an intentional disk-space halt into exit code
**75** so systemd can refuse to restart into a full disk; propagates any other
non-zero exit as a crash. This is what `kalshi-recorder.service` runs. No flags.

- **Time / disk:** same as the recorder it supervises.

### `compactor.py` — raw → Parquet

```bash
.venv/bin/python compactor.py all              # all unprocessed raw files
.venv/bin/python compactor.py date 2026-09-15  # one date's hour-files
.venv/bin/python compactor.py file data/raw/2026-09-15/02.ndjson.zst
.venv/bin/python compactor.py all --dry-run    # report what would run, no writes
```

Idempotent (manifest-checkpointed by path+size+mtime), streaming, atomic. **Note:
`all` includes the current, still-open hour-file** — for unattended use prefer
`compact_completed.py`, which excludes it.

- **Time:** seconds per hour-file (CPU-bound zstd+Parquet encode).
- **Disk:** output ≈ 0.68–0.85 × input; adds ~135 MB per compacted hour. Does
  **not** delete raw (that is retention's job).

### `compact_completed.py` — hourly compaction driver (used by the timer)

```bash
.venv/bin/python compact_completed.py            # compact every completed hour
.venv/bin/python compact_completed.py --dry-run  # list what would be compacted
```

Compacts every raw hour-file whose UTC hour is strictly in the past and **never**
the hour the recorder is currently writing. Exits non-zero if any file fails
reconciliation. This is what `kalshi-compactor.service` runs first.

- **Time / disk:** as `compactor.py`, summed over the completed hours found.

### `retention.py` — delete fully-processed, aged-out raw files (used by the timer)

```bash
.venv/bin/python retention.py --dry-run                 # per-file reasoning, no deletes
.venv/bin/python retention.py                           # delete eligible files (min age 7d)
.venv/bin/python retention.py --min-age-days 14
```

Deletes a raw file **only** if all hold: (1) the manifest records it processed
and the on-disk bytes still match (size+mtime); (2) all its Parquet outputs
(and rejects sidecar, if any) exist; (3) reconciliation held; and (4) the hour is
at least `--min-age-days` old. Never deletes on age alone; logs every deletion
with the reconciliation numbers. Runs (after a successful compaction) as
`ExecStartPost` in `kalshi-compactor.service`.

- **Time:** milliseconds (reads the manifest + stats files; no data scan).
- **Disk:** *frees* space — up to ~186 MB per deleted raw hour.

### `free_space_monitor.py` — independent low-disk warning (used by the timer)

```bash
.venv/bin/python free_space_monitor.py                    # warn below MIN_FREE_GB_WARN (10)
.venv/bin/python free_space_monitor.py --threshold-gb 15
```

Logs a `WARNING` when free space on the data volume is below the threshold,
independent of the recorder's own guard. Observes only — never stops or deletes.
Runs every 5 minutes as `kalshi-freespace.service`.

- **Time:** milliseconds. **Disk:** none.

### `backfill.py` — historical trades via REST

```bash
.venv/bin/python backfill.py --status                       # checkpoint summary
.venv/bin/python backfill.py --discover --series KXNFLGAME  # enumerate a series' markets
.venv/bin/python backfill.py --pull --series KXNFLGAME      # pull that series' trades
.venv/bin/python backfill.py --pull --series KXNFLGAME --dry-run
.venv/bin/python backfill.py --retry-failed                 # re-pull markets that failed
```

Key flags (see `backfill.py --help` for the full census/parlay set):

| Flag                       | Meaning                                                        |
|----------------------------|----------------------------------------------------------------|
| `--discover`               | Enumerate closed/settled markets into `type=market_meta`       |
| `--pull [PREFIX]`          | Pull trades for all markets, or those starting with `PREFIX`   |
| `--series PREFIX`          | Scope discovery/pull to a series/ticker prefix                 |
| `--status`                 | Print checkpoint summary and exit                              |
| `--retry-failed`           | Reset failed markets, then pull them again                     |
| `--dry-run`                | Report intended work (and a cost preview) without changes      |
| `--series-from-census`     | Scoped discovery+pull of the top census series                 |
| `--top-n N`                | Select the N highest-estimated-tradeable census series         |
| `--exclude PREFIX`         | Exclude census series by prefix (repeatable)                   |
| `--min-volume V`           | Skip markets with traded volume below V contracts              |
| `--safety-margin-gb GB`    | Free-space margin for the cost preview                         |

- **Scoped pull** (`--pull --series KX...`): minutes to hours; single-digit GB.
  Example measured: 408 KXHIGHNY markets, 611,941 trades in ~85s.
- **Discovery** alone is a **many-hours** job — the settled+closed universe is
  tens of millions of markets and a full crawl has not completed in one sitting.

> ⚠️ **`backfill.py --pull` WITHOUT `--series` (or a `PREFIX`) is a multi-day,
> hundreds-of-GB job.** It pulls the entire trade history of every discovered
> market. Do not run it unscoped unless you truly intend a full-history download
> and have the days and disk for it. Always scope with `--series`/`PREFIX`, or
> use `--series-from-census --top-n N`, and check `--dry-run` first.
>
> Per-series counts in the census cost preview are a **lower bound** — a scoped
> pull can be 100× larger than previewed (the census sampled an incomplete
> discovery). Treat the preview as a floor, not a firm estimate.

### `reader.py` — typed query layer (library, no CLI)

```python
from reader import Reader
r = Reader()  # reads KALSHI_DATA_DIR/parquet
print(r.list_tickers(date="2026-09-15"))
prices = r.price_series("KXNFLGAME-...", date="2026-09-15")
trades = r.trades("KXNFLGAME-...", date="2026-09-15")  # live/backfill de-duped by default
```

Prices/quantities come back as `Decimal` with the scale read from Parquet field
metadata. Any coverage gap overlapping the window raises `CoverageError` unless
`allow_gaps=True`. Live+backfill trades are de-duplicated (live authoritative);
pass `raw=True` for the un-deduped view.

- **Time:** query-time; partition pruning scans only the requested `date=`.
- **Disk:** read-only.

### `migrate_trades.py` — one-shot schema migration

```bash
.venv/bin/python migrate_trades.py
```

Idempotent migration of pre-`source`-column trade Parquet to the current schema.
Already applied to existing data; safe no-op on a fresh capture.

---

## Deployment

For unattended operation on a VPS (systemd units, hourly compaction + retention,
free-space monitor, verified sync), follow `DEPLOY.md`.
