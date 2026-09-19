"""Phase 4: REST backfill of historical Kalshi trades.

A standalone process (never touches the recorder, compactor, or reader) that:

1. **Discovers** closed/settled markets via the ``/markets`` endpoint with cursor
   pagination and persists them to their own ``type=market_meta`` Parquet dataset
   *before* any trades are pulled. A finished discovery scope is never re-crawled.
2. **Pulls** each market's full trade history, paginated to exhaustion, into the
   exact ``TRADE_SCHEMA`` the compactor produces so backfilled and live trades are
   queryable through the Phase-3 reader with no special-casing.
3. **Checkpoints** every market in a SQLite (WAL) state store. Killing the process
   at any instant and restarting resumes exactly where it stopped — no duplicated
   and no skipped rows (see the exactly-once note on :func:`pull_market`).
4. **Rate-limits** with a token bucket sized from the account's *reported* limits
   (``/account/limits``) times a safety margin; 429/5xx/transport errors back off
   exponentially and are never counted as a market failure.

Provenance: backfill trades are written into the reader-visible
``type=trade/date=<D>/`` directories (filename prefix ``backfill_``) using the
same ``TRADE_SCHEMA`` as live data, but with an explicit ``source="backfill"``
column and a NULL ``ts_ns`` (the recorder's monotonic clock does not apply to
rows pulled from REST). Live rows carry ``source="live"`` and a real ``ts_ns``.
Callers filter on ``source`` and must order backfilled rows by ``ts_wall``.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import heapq
import json
import os
import shutil
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from statistics import median
from typing import Any

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from auth import Signer, signer_from_config
from config import CONFIG
from schema import (
    DOLLAR_DECIMALS,
    FP_DECIMALS,
    FP_SCALE,
    SOURCE_BACKFILL,
    TRADE_SCHEMA,
    SchemaViolation,
    parse_scaled,
)

HOST = "https://api.elections.kalshi.com"
PREFIX = "/trade-api/v2"
TRADES_LIMIT = 1000
MARKETS_LIMIT = 1000
RATE_SAFETY_MARGIN = 0.5
PROGRESS_INTERVAL_S = 30.0
MAX_RETRIES = 8
BACKOFF_BASE = 0.5
BACKOFF_CAP = 60.0
DISCOVERY_STATUSES = ("settled", "closed")

# Phase 4.3 — census-driven scoped backfill.
PARLAY_SERIES = "KXMVECROSSCATEGORY"          # the combinatorial multivariate series
DEFAULT_CENSUS_ROWS = "_census_rows.json"     # ranked census produced by _census.py
MIN_TRADES_FOR_MEANCOUNT = 100                # min rows before a per-series mean is trusted
PARLAY_SCAN_PAGES_DEFAULT = 50                # bounded ranking crawl of the parlay universe
GB = 1024**3

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class BackfillError(RuntimeError):
    """Raised for backfill-specific anomalies (e.g. a cursor that fails to advance)."""


# --------------------------------------------------------------------------- #
# Time helpers (integer nanoseconds, no float precision loss)
# --------------------------------------------------------------------------- #
def _iso_to_ns(iso: str) -> int:
    """Exact epoch nanoseconds for an ISO-8601 UTC timestamp (handles a 'Z' suffix)."""
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    delta = dt - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _ns_to_date(ns: int) -> str:
    """UTC calendar date (YYYY-MM-DD) of an epoch-ns instant."""
    return (_EPOCH + timedelta(microseconds=ns // 1000)).date().isoformat()


def _series_of(ticker: str) -> str:
    """The series ticker: everything before the first dash (e.g. KXHIGHNY-...-T85 -> KXHIGHNY)."""
    return ticker.split("-", 1)[0]


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #
class TokenBucket:
    """A monotonic-clock token bucket. ``acquire`` blocks until a token is free."""

    def __init__(self, rate: float, capacity: float) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self._rate
            time.sleep(wait)


class RateLimits:
    """The account's reported read/write bucket sizes."""

    def __init__(
        self, read_capacity: int, read_refill: int, write_capacity: int, write_refill: int
    ) -> None:
        self.read_capacity = read_capacity
        self.read_refill = read_refill
        self.write_capacity = write_capacity
        self.write_refill = write_refill


def _sleep_backoff(attempt: int, retry_after: str | None = None) -> None:
    wait = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1)))
    if retry_after is not None:
        try:
            wait = max(wait, float(retry_after))
        except ValueError:
            pass
    time.sleep(wait)


# --------------------------------------------------------------------------- #
# HTTP client (signed GET with retry/backoff)
# --------------------------------------------------------------------------- #
class RestClient:
    """Signed GET client. Every request passes through the token bucket (once set)."""

    def __init__(
        self,
        signer: Signer,
        bucket: TokenBucket | None = None,
        http: httpx.Client | None = None,
    ) -> None:
        self._signer = signer
        self._bucket = bucket
        self._http = http if http is not None else httpx.Client(timeout=30.0)
        self.request_count = 0

    def set_bucket(self, bucket: TokenBucket) -> None:
        self._bucket = bucket

    def close(self) -> None:
        self._http.close()

    def get_json(self, endpoint: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        """GET ``PREFIX+endpoint``; retry 429/5xx/transport with exponential backoff."""
        attempt = 0
        while True:
            if self._bucket is not None:
                self._bucket.acquire(1.0)
            headers = self._signer.headers("GET", PREFIX + endpoint)
            self.request_count += 1
            try:
                resp = self._http.get(HOST + PREFIX + endpoint, headers=headers, params=params)
            except httpx.HTTPError:
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                _sleep_backoff(attempt)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                attempt += 1
                if attempt > MAX_RETRIES:
                    resp.raise_for_status()
                _sleep_backoff(attempt, resp.headers.get("Retry-After"))
                continue
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise BackfillError(f"expected a JSON object from {endpoint}, got {type(payload)}")
            return payload


def fetch_rate_limits(client: RestClient) -> RateLimits:
    data = client.get_json("/account/limits")
    read = data["read"]
    write = data["write"]
    return RateLimits(
        read_capacity=int(read["bucket_capacity"]),
        read_refill=int(read["refill_rate"]),
        write_capacity=int(write["bucket_capacity"]),
        write_refill=int(write["refill_rate"]),
    )


# --------------------------------------------------------------------------- #
# Checkpoint state store (SQLite, WAL)
# --------------------------------------------------------------------------- #
class Status(StrEnum):
    NOT_STARTED = "not_started"
    IN_PROGRESS = "in_progress"
    COMPLETE = "complete"
    FAILED = "failed"


class MarketState:
    def __init__(
        self,
        ticker: str,
        series: str,
        status: str,
        cursor: str | None,
        pages: int,
        trades: int,
        reason: str | None,
    ) -> None:
        self.ticker = ticker
        self.series = series
        self.status = status
        self.cursor = cursor
        self.pages = pages
        self.trades = trades
        self.reason = reason


class StateStore:
    """Crash-safe per-market checkpoint store. All writes commit atomically."""

    def __init__(self, path: Path) -> None:
        self._con = sqlite3.connect(str(path))
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
                ticker  TEXT PRIMARY KEY,
                series  TEXT NOT NULL,
                status  TEXT NOT NULL,
                cursor  TEXT,
                pages   INTEGER NOT NULL DEFAULT 0,
                trades  INTEGER NOT NULL DEFAULT 0,
                reason  TEXT,
                updated REAL NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS markets_status ON markets(status);
            CREATE INDEX IF NOT EXISTS markets_series ON markets(series);
            CREATE TABLE IF NOT EXISTS discovery (
                scope   TEXT PRIMARY KEY,
                cursor  TEXT,
                pages   INTEGER NOT NULL DEFAULT 0,
                done    INTEGER NOT NULL DEFAULT 0,
                updated REAL NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self._migrate()
        self._con.commit()

    def _migrate(self) -> None:
        """Additive migrations that keep an existing state DB usable across phases.

        Phase 4.3 adds a nullable ``volume_fp`` column so scoped pulls can filter
        markets by traded volume (the column is populated by scoped discovery).
        Markets seeded by an earlier, volume-blind discovery keep NULL until the
        series is scoped-discovered again.
        """
        cols = {str(r[1]) for r in self._con.execute("PRAGMA table_info(markets)").fetchall()}
        if "volume_fp" not in cols:
            self._con.execute("ALTER TABLE markets ADD COLUMN volume_fp INTEGER")

    def close(self) -> None:
        self._con.close()

    def txn(self) -> sqlite3.Connection:
        """Return the connection for use as a ``with`` block (commits, or rolls back on error)."""
        return self._con

    # -- discovery -------------------------------------------------------- #
    def seed_market(self, ticker: str, series: str) -> None:
        """Insert a market as not_started; never disturbs one already being pulled."""
        self._con.execute(
            "INSERT OR IGNORE INTO markets(ticker, series, status, cursor, pages, trades, updated) "
            "VALUES (?, ?, ?, NULL, 0, 0, ?)",
            (ticker, series, Status.NOT_STARTED.value, time.time()),
        )

    def get_discovery(self, scope: str) -> dict[str, Any] | None:
        row = self._con.execute(
            "SELECT cursor, pages, done FROM discovery WHERE scope = ?", (scope,)
        ).fetchone()
        if row is None:
            return None
        return {"cursor": row[0], "pages": int(row[1]), "done": bool(row[2])}

    def all_discovery(self) -> list[tuple[str, int, bool]]:
        rows = self._con.execute(
            "SELECT scope, pages, done FROM discovery ORDER BY scope"
        ).fetchall()
        return [(str(s), int(p), bool(d)) for s, p, d in rows]

    def set_discovery(self, scope: str, cursor: str, pages: int, done: bool) -> None:
        self._con.execute(
            "INSERT INTO discovery(scope, cursor, pages, done, updated) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(scope) DO UPDATE SET cursor=excluded.cursor, pages=excluded.pages, "
            "done=excluded.done, updated=excluded.updated",
            (scope, cursor, pages, 1 if done else 0, time.time()),
        )

    # -- meta (rolling observed metrics) ---------------------------------- #
    def get_meta(self, key: str) -> str | None:
        row = self._con.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def set_meta(self, key: str, value: str) -> None:
        with self._con:
            self._con.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # -- scoped seeding / selection --------------------------------------- #
    def seed_market_vol(self, ticker: str, series: str, volume_fp: int) -> None:
        """Seed a market (as not_started) or, if it already exists, refresh its volume.

        Unlike :meth:`seed_market`, this records the market's traded volume so a
        later scoped pull can filter on it. An already-in-progress or complete
        market keeps its status; only ``volume_fp`` is updated.
        """
        self._con.execute(
            "INSERT INTO markets"
            "(ticker, series, status, cursor, pages, trades, volume_fp, updated) "
            "VALUES (?, ?, ?, NULL, 0, 0, ?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET "
            "volume_fp=excluded.volume_fp, updated=excluded.updated",
            (ticker, series, Status.NOT_STARTED.value, volume_fp, time.time()),
        )

    def select_to_pull_series(self, series: list[str], min_volume_fp: int) -> list[str]:
        """Pullable markets (not_started/in_progress) in the given series.

        When ``min_volume_fp`` > 0 only markets whose recorded ``volume_fp`` meets
        the threshold are returned (rows with unknown volume are excluded). When it
        is 0 every pullable market in the series is returned regardless of volume.
        """
        if not series:
            return []
        placeholders = ",".join("?" for _ in series)
        query = (
            f"SELECT ticker FROM markets WHERE series IN ({placeholders}) "
            "AND status IN (?, ?)"
        )
        params: list[Any] = [*series, Status.NOT_STARTED.value, Status.IN_PROGRESS.value]
        if min_volume_fp > 0:
            query += " AND volume_fp IS NOT NULL AND volume_fp >= ?"
            params.append(min_volume_fp)
        query += " ORDER BY ticker"
        return [str(r[0]) for r in self._con.execute(query, params).fetchall()]

    def pending_tickers(self, tickers: list[str]) -> list[str]:
        """Of the given tickers, those still needing a pull (not already complete)."""
        out: list[str] = []
        for tk in tickers:
            st = self.get(tk)
            if st is None or st.status != Status.COMPLETE.value:
                out.append(tk)
        return out

    def summary_by_series(self, series: list[str] | None) -> dict[str, dict[str, tuple[int, int]]]:
        """Per-series (status -> (market_count, trade_count)).

        With ``series=None`` reports every series that has been scoped-touched
        (any market with a recorded volume or a non not_started status), so the
        millions of volume-blind, never-pulled markets do not drown the output.
        """
        query = (
            "SELECT series, status, count(*), COALESCE(sum(trades), 0) FROM markets "
        )
        params: list[Any] = []
        if series:
            placeholders = ",".join("?" for _ in series)
            query += f"WHERE series IN ({placeholders}) "
            params.extend(series)
        else:
            query += "WHERE volume_fp IS NOT NULL OR status <> ? "
            params.append(Status.NOT_STARTED.value)
        query += "GROUP BY series, status"
        out: dict[str, dict[str, tuple[int, int]]] = {}
        for s, status, count, trades in self._con.execute(query, params).fetchall():
            out.setdefault(str(s), {})[str(status)] = (int(count), int(trades))
        return out

    # -- per-market pull -------------------------------------------------- #
    def get(self, ticker: str) -> MarketState | None:
        row = self._con.execute(
            "SELECT ticker, series, status, cursor, pages, trades, reason "
            "FROM markets WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if row is None:
            return None
        return MarketState(row[0], row[1], row[2], row[3], int(row[4]), int(row[5]), row[6])

    def record_page(self, ticker: str, cursor: str, pages: int, trades: int) -> None:
        """Advance the checkpoint after a page file is written (use inside ``with txn()``)."""
        self._con.execute(
            "UPDATE markets SET status=?, cursor=?, pages=?, trades=?, reason=NULL, updated=? "
            "WHERE ticker=?",
            (Status.IN_PROGRESS.value, cursor, pages, trades, time.time(), ticker),
        )

    def set_complete(self, ticker: str) -> None:
        with self._con:
            self._con.execute(
                "UPDATE markets SET status=?, cursor=NULL, reason=NULL, updated=? WHERE ticker=?",
                (Status.COMPLETE.value, time.time(), ticker),
            )

    def set_failed(self, ticker: str, reason: str) -> None:
        with self._con:
            self._con.execute(
                "UPDATE markets SET status=?, reason=?, updated=? WHERE ticker=?",
                (Status.FAILED.value, reason[:500], time.time(), ticker),
            )

    def reset_failed(self, prefix: str) -> int:
        query = (
            "UPDATE markets SET status=?, cursor=NULL, pages=0, trades=0, reason=NULL, updated=? "
            "WHERE status=?"
        )
        params: list[Any] = [Status.NOT_STARTED.value, time.time(), Status.FAILED.value]
        if prefix:
            query += " AND ticker LIKE ?"
            params.append(prefix + "%")
        with self._con:
            cur = self._con.execute(query, params)
            return cur.rowcount

    def select_to_pull(self, prefix: str) -> list[str]:
        query = "SELECT ticker FROM markets WHERE status IN (?, ?)"
        params: list[Any] = [Status.NOT_STARTED.value, Status.IN_PROGRESS.value]
        if prefix:
            query += " AND ticker LIKE ?"
            params.append(prefix + "%")
        query += " ORDER BY ticker"
        return [str(r[0]) for r in self._con.execute(query, params).fetchall()]

    def count_markets(self) -> int:
        row = self._con.execute("SELECT count(*) FROM markets").fetchone()
        return int(row[0])

    def summary(self) -> dict[str, tuple[int, int]]:
        out: dict[str, tuple[int, int]] = {s.value: (0, 0) for s in Status}
        rows = self._con.execute(
            "SELECT status, count(*), COALESCE(sum(trades), 0) FROM markets GROUP BY status"
        ).fetchall()
        for status, count, trades in rows:
            out[str(status)] = (int(count), int(trades))
        return out


# --------------------------------------------------------------------------- #
# Parquet writers
# --------------------------------------------------------------------------- #
MARKET_META_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string(), nullable=False),
        pa.field("series", pa.string(), nullable=False),
        pa.field("event_ticker", pa.string(), nullable=True),
        pa.field("market_type", pa.string(), nullable=True),
        pa.field("status", pa.string(), nullable=True),
        pa.field("result", pa.string(), nullable=True),
        pa.field("title", pa.string(), nullable=True),
        pa.field("open_time", pa.string(), nullable=True),
        pa.field("close_time", pa.string(), nullable=True),
        pa.field("expiration_time", pa.string(), nullable=True),
        pa.field("discovered_ts_ns", pa.int64(), nullable=False),
    ]
)


def _write_table_atomic(table: pa.Table, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, dest)


def _opt(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def _trade_row(t: dict[str, Any]) -> dict[str, Any]:
    ns = _iso_to_ns(str(t["created_time"]))
    return {
        "ts_ns": None,  # the recorder's monotonic clock does not apply to backfill
        "ts_wall": ns,
        "sid": 0,
        "seq": 0,
        "trade_id": str(t["trade_id"]),
        "market_ticker": str(t["ticker"]),
        "yes_price_dollars": parse_scaled(t["yes_price_dollars"], DOLLAR_DECIMALS),
        "no_price_dollars": parse_scaled(t["no_price_dollars"], DOLLAR_DECIMALS),
        "count_fp": parse_scaled(t["count_fp"], FP_DECIMALS),
        "taker_side": str(t["taker_side"]),
        "taker_outcome_side": str(t["taker_outcome_side"]),
        "taker_book_side": str(t["taker_book_side"]),
        "is_block_trade": bool(t["is_block_trade"]),
        "ts": ns // 1_000_000_000,
        "ts_ms": ns // 1_000_000,
        "source": SOURCE_BACKFILL,
    }


def _columnar(rows: list[dict[str, Any]], schema: pa.Schema) -> dict[str, list[Any]]:
    cols: dict[str, list[Any]] = {name: [] for name in schema.names}
    for row in rows:
        for name in cols:
            cols[name].append(row[name])
    return cols


def write_trade_page(root: Path, ticker: str, page: int, trades: list[dict[str, Any]]) -> None:
    """Write one page of a market's trades, grouped by UTC date, to reader-visible files.

    Files are named deterministically by (ticker, page), so re-running the same
    page (after a crash) overwrites byte-identical data — never a duplicate row.
    """
    by_date: dict[str, list[dict[str, Any]]] = {}
    for t in trades:
        row = _trade_row(t)
        by_date.setdefault(_ns_to_date(row["ts_wall"]), []).append(row)
    digest = hashlib.sha1(ticker.encode("utf-8")).hexdigest()[:16]
    for date, rows in by_date.items():
        dest = root / "type=trade" / f"date={date}" / f"backfill_{digest}_{page:05d}.parquet"
        table = pa.table(_columnar(rows, TRADE_SCHEMA), schema=TRADE_SCHEMA)
        _write_table_atomic(table, dest)


def _market_meta_row(m: dict[str, Any]) -> dict[str, Any]:
    ticker = str(m["ticker"])
    return {
        "ticker": ticker,
        "series": _series_of(ticker),
        "event_ticker": _opt(m.get("event_ticker")),
        "market_type": _opt(m.get("market_type")),
        "status": _opt(m.get("status")),
        "result": _opt(m.get("result")),
        "title": _opt(m.get("title")),
        "open_time": _opt(m.get("open_time")),
        "close_time": _opt(m.get("close_time")),
        "expiration_time": _opt(m.get("expiration_time")),
        "discovered_ts_ns": time.time_ns(),
    }


def write_market_meta(root: Path, scope: str, page: int, markets: list[dict[str, Any]]) -> None:
    rows = [_market_meta_row(m) for m in markets]
    dest = root / "type=market_meta" / f"{scope}_{page:05d}.parquet"
    table = pa.table(_columnar(rows, MARKET_META_SCHEMA), schema=MARKET_META_SCHEMA)
    _write_table_atomic(table, dest)


# --------------------------------------------------------------------------- #
# Progress reporting
# --------------------------------------------------------------------------- #
class Progress:
    def __init__(self, total: int) -> None:
        self.total = total
        self.complete = 0
        self.failed = 0
        self.trades = 0
        self._lock = threading.Lock()

    def add_trades(self, n: int) -> None:
        with self._lock:
            self.trades += n

    def market_done(self) -> None:
        with self._lock:
            self.complete += 1

    def market_failed(self) -> None:
        with self._lock:
            self.failed += 1

    def snapshot(self) -> tuple[int, int, int, int]:
        with self._lock:
            return self.total, self.complete, self.failed, self.trades


def _fmt_eta(seconds: float) -> str:
    if seconds != seconds or seconds == float("inf"):  # NaN or inf
        return "?"
    total = int(seconds)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _reporter(
    progress: Progress,
    client: RestClient,
    stop: threading.Event,
    start: float,
    interval: float,
) -> None:
    last_req = client.request_count
    last_t = start
    while not stop.wait(interval):
        now = time.monotonic()
        total, complete, failed, trades = progress.snapshot()
        reqs = client.request_count
        rate = (reqs - last_req) / (now - last_t) if now > last_t else 0.0
        last_req, last_t = reqs, now
        done = complete + failed
        remaining = max(0, total - done)
        overall = complete / (now - start) if now > start and complete else 0.0
        eta = remaining / overall if overall > 0 else float("inf")
        print(
            f"[backfill] {complete}/{total} complete, {failed} failed, {trades} trades, "
            f"{rate:.1f} req/s, ETA {_fmt_eta(eta)}",
            flush=True,
        )


# --------------------------------------------------------------------------- #
# Core operations
# --------------------------------------------------------------------------- #
def pull_market(
    client: RestClient, store: StateStore, root: Path, ticker: str, progress: Progress
) -> int | None:
    """Pull one market's trades to exhaustion. Returns the trade count, or None on failure.

    Exactly-once resume: the page file is written and durably ``os.replace``-d
    *before* the checkpoint transaction advances the cursor/page counter. A crash
    between the two leaves the stored cursor unchanged, so restart re-fetches the
    same cursor and overwrites the same deterministically-named file — no gap, no
    duplicate. Per-market exceptions are recorded as failures and never fatal;
    ``KeyboardInterrupt`` (a BaseException) is deliberately *not* caught, so a kill
    mid-pull leaves the market resumable from its last committed page.
    """
    state = store.get(ticker)
    if state is None:
        store.set_failed(ticker, "not seeded (run --discover first)")
        progress.market_failed()
        return None
    cursor = state.cursor
    pages = state.pages
    total = state.trades
    try:
        while True:
            params = {"ticker": ticker, "limit": str(TRADES_LIMIT)}
            if cursor:
                params["cursor"] = cursor
            data = client.get_json("/markets/trades", params)
            page_trades = data.get("trades") or []
            next_cursor = str(data.get("cursor") or "")
            if page_trades:
                write_trade_page(root, ticker, pages, page_trades)
            new_pages = pages + 1
            new_total = total + len(page_trades)
            with store.txn():
                store.record_page(ticker, next_cursor, new_pages, new_total)
            pages, total = new_pages, new_total
            progress.add_trades(len(page_trades))
            if not next_cursor:
                break
            if next_cursor == cursor:
                raise BackfillError(f"cursor did not advance for {ticker}")
            cursor = next_cursor
        store.set_complete(ticker)
        progress.market_done()
        return total
    except Exception as exc:  # per-market failure; keep crawling other markets
        store.set_failed(ticker, repr(exc))
        progress.market_failed()
        return None


def discover(
    client: RestClient, store: StateStore, root: Path, dry_run: bool, series: str = ""
) -> None:
    """Enumerate settled+closed markets, persist market_meta, and seed the state store.

    If ``series`` is given, only that series is crawled (via the ``series_ticker``
    query param); each (status, series) pair is checkpointed as its own scope.
    """
    suffix = f":{series}" if series else ""
    for scope in DISCOVERY_STATUSES:
        key = scope + suffix
        if dry_run:
            disc = store.get_discovery(key)
            state = "complete" if disc and disc["done"] else "pending"
            print(f"[dry-run] would crawl status={scope} series={series or '*'} ({state})")
            continue
        disc = store.get_discovery(key)
        if disc and disc["done"]:
            print(f"discovery[{key}] already complete; skipping")
            continue
        cursor = str(disc["cursor"]) if disc and disc["cursor"] else ""
        pages = disc["pages"] if disc else 0
        seen = 0
        while True:
            params = {"limit": str(MARKETS_LIMIT), "status": scope}
            if series:
                params["series_ticker"] = series
            if cursor:
                params["cursor"] = cursor
            data = client.get_json("/markets", params)
            markets = data.get("markets") or []
            next_cursor = str(data.get("cursor") or "")
            if markets:
                write_market_meta(root, key.replace(":", "_"), pages, markets)
            new_pages = pages + 1
            done = not next_cursor
            with store.txn():
                for m in markets:
                    ticker = str(m["ticker"])
                    store.seed_market(ticker, _series_of(ticker))
                store.set_discovery(key, next_cursor, new_pages, done)
            pages = new_pages
            seen += len(markets)
            if done:
                break
            if next_cursor == cursor:
                raise BackfillError(f"discovery cursor did not advance for scope={key}")
            cursor = next_cursor
        print(f"discovery[{key}] complete: {seen} markets over {pages} pages")


def run_pull(
    client: RestClient,
    store: StateStore,
    root: Path,
    prefix: str,
    dry_run: bool,
    interval: float = PROGRESS_INTERVAL_S,
) -> None:
    tickers = store.select_to_pull(prefix)
    scope = f" matching {prefix!r}" if prefix else ""
    if dry_run:
        print(f"[dry-run] would pull {len(tickers)} market(s){scope}")
        return
    if not tickers:
        print(f"nothing to pull (no not_started/in_progress markets{scope}).")
        return
    print(f"pulling {len(tickers)} market(s){scope}...")
    progress = Progress(len(tickers))
    stop = threading.Event()
    start = time.monotonic()
    reporter = threading.Thread(
        target=_reporter, args=(progress, client, stop, start, interval), daemon=True
    )
    reporter.start()
    try:
        for ticker in tickers:
            pull_market(client, store, root, ticker, progress)
    finally:
        stop.set()
        reporter.join(timeout=2.0)
    _, complete, failed, trades = progress.snapshot()
    elapsed = time.monotonic() - start
    print(
        f"[backfill] done: {complete}/{len(tickers)} complete, {failed} failed, "
        f"{trades} trades in {elapsed:.1f}s ({client.request_count} requests)"
    )


def print_status(store: StateStore) -> None:
    summary = store.summary()
    total_markets = sum(count for count, _ in summary.values())
    total_trades = sum(trades for _, trades in summary.values())
    print("backfill status:")
    for s in Status:
        count, trades = summary[s.value]
        print(f"  {s.value:12} markets={count:7} trades={trades}")
    print(f"  {'TOTAL':12} markets={total_markets:7} trades={total_trades}")
    scopes = store.all_discovery()
    if not scopes:
        print("  discovery: not started")
    for scope, pages, done in scopes:
        state = "complete" if done else "in progress"
        print(f"  discovery[{scope}]: {state} (pages={pages})")
    print_status_scoped(store, None)


# --------------------------------------------------------------------------- #
# Phase 4.3 — census-driven scoped backfill
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CostBasis:
    """Cost constants measured from the on-disk trade dataset (never guessed)."""

    bytes_per_row: float
    basis_rows: int
    basis_bytes: int
    basis_label: str
    mean_count_by_series: dict[str, float]
    fallback_mean_count: float

    def mean_count(self, series: str) -> float:
        mc = self.mean_count_by_series.get(series, self.fallback_mean_count)
        return mc if mc > 0 else (self.fallback_mean_count or 1.0)


def _measure_bytes_per_row(root: Path) -> tuple[float, int, int, str]:
    """(bytes/row, rows, bytes, label) for the size estimate.

    Prefers the ``backfill_*.parquet`` files a scoped pull actually produces: those
    are one small file per market/date/page, so their per-row byte cost (Parquet
    footer + schema overhead amortised over few rows) is several times that of the
    compactor's large live files. Estimating from them keeps the disk-refusal guard
    honest for this workload. Falls back to the whole trade set before any backfill
    output exists.
    """
    bf = glob.glob(str(root / "type=trade" / "**" / "backfill_*.parquet"), recursive=True)
    if bf:
        files, label = bf, "rows across backfill files"
        expr = str(root / "type=trade" / "**" / "backfill_*.parquet").replace("\\", "/")
    else:
        files = glob.glob(str(root / "type=trade" / "**" / "*.parquet"), recursive=True)
        label = "rows across all trade files"
        expr = str(root / "type=trade" / "**" / "*.parquet").replace("\\", "/")
    if not files:
        raise BackfillError(
            f"no type=trade parquet under {root}; cannot derive bytes/row (pull some data first)"
        )
    total_bytes = sum(os.path.getsize(f) for f in files)
    con = duckdb.connect()
    try:
        row = con.execute(f"SELECT count(*) FROM read_parquet('{expr}')").fetchone()
        rows = int(row[0]) if row else 0
    finally:
        con.close()
    if rows == 0:
        raise BackfillError("trade dataset has zero rows; cannot derive bytes/row")
    return total_bytes / rows, rows, total_bytes, label


def measure_cost_basis(root: Path) -> CostBasis:
    """Derive bytes-per-row and per-series contracts-per-trade from the ``type=trade`` set.

    bytes/row is measured from real files (see :func:`_measure_bytes_per_row`), so
    the size estimate is grounded in output, not a guess. Per series, the mean
    ``count_fp``/FP_SCALE (contracts per trade) turns a series' traded *volume*
    (== summed contract counts) into an expected *trade-row* count. A per-series
    mean is trusted only with >= MIN_TRADES_FOR_MEANCOUNT observed rows; otherwise
    the median of the trusted means is used.
    """
    bytes_per_row, basis_rows, basis_bytes, label = _measure_bytes_per_row(root)
    glob_expr = str(root / "type=trade" / "**" / "*.parquet").replace("\\", "/")
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT split_part(market_ticker, '-', 1) AS s, count(*) AS n, avg(count_fp) AS ac "
            f"FROM read_parquet('{glob_expr}') GROUP BY s"
        ).fetchall()
    finally:
        con.close()
    means: dict[str, float] = {}
    for s, n, ac in rows:
        if int(n) >= MIN_TRADES_FOR_MEANCOUNT and ac is not None and float(ac) > 0:
            means[str(s)] = float(ac) / FP_SCALE
    reliable = sorted(means.values())
    fallback = median(reliable) if reliable else 1.0
    return CostBasis(
        bytes_per_row=bytes_per_row,
        basis_rows=basis_rows,
        basis_bytes=basis_bytes,
        basis_label=label,
        mean_count_by_series=means,
        fallback_mean_count=fallback,
    )


def est_rows_for_series(est_tradeable: int, mean_vol: float, basis: CostBasis, series: str) -> int:
    """Expected trade rows for a whole series: tradeable_markets * mean_vol / mean_count."""
    return int(round(est_tradeable * mean_vol / basis.mean_count(series)))


def est_rows_for_market(volume_fp: int, basis: CostBasis, series: str) -> int:
    """Expected trade rows for a single market: its volume (contracts) / mean_count."""
    return int(round((volume_fp / FP_SCALE) / basis.mean_count(series)))


@dataclass(frozen=True)
class CensusRow:
    series: str
    total: int
    est_tradeable: int
    mean_vol: float


def load_census(path: Path) -> list[CensusRow]:
    with open(path) as fh:
        raw = json.load(fh)
    return [
        CensusRow(
            series=str(r["series"]),
            total=int(r["total"]),
            est_tradeable=int(r["est_tradeable"]),
            mean_vol=float(r["mean_vol"]),
        )
        for r in raw
    ]


def select_from_census(
    rows: list[CensusRow], top_n: int, excludes: list[str], include_parlay: bool
) -> list[CensusRow]:
    """Rank census rows by estimated tradeable markets and take the top N.

    Series whose ticker starts with any ``excludes`` prefix are dropped. The parlay
    series is auto-excluded unless ``include_parlay`` (it belongs to --parlay-top-n).
    """
    ex = [p for p in excludes if p]
    if not include_parlay:
        ex.append(PARLAY_SERIES)
    kept = [
        r
        for r in rows
        if r.est_tradeable > 0 and not any(r.series.startswith(p) for p in ex)
    ]
    kept.sort(key=lambda r: r.est_tradeable, reverse=True)
    return kept[:top_n] if top_n > 0 else kept


@dataclass(frozen=True)
class SeriesPlan:
    series: str
    tradeable: int
    est_rows: int


DEFAULT_ASSUMED_REQ_S = 12.0  # conservative fallback before any real pull has been measured


def free_bytes(path: Path) -> int:
    """Free bytes on the volume holding ``path`` (walk up to the nearest existing ancestor)."""
    p = Path(os.path.realpath(path))
    while not p.exists():
        if p.parent == p:
            break
        p = p.parent
    return int(shutil.disk_usage(p).free)


def observed_req_s(store: StateStore) -> tuple[float, bool]:
    """(requests/sec, measured?) — the rolling rate persisted after real pulls, else assumed."""
    raw = store.get_meta("observed_req_s")
    if raw:
        try:
            v = float(raw)
            if v > 0:
                return v, True
        except ValueError:
            pass
    return DEFAULT_ASSUMED_REQ_S, False


def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024.0:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PiB"


def _pages_for(rows: int) -> int:
    return (rows + TRADES_LIMIT - 1) // TRADES_LIMIT


def print_cost_preview(
    title: str,
    plans: list[SeriesPlan],
    basis: CostBasis,
    store: StateStore,
    safety_margin_gb: float,
    root: Path,
) -> bool:
    """Print the mandatory pre-flight cost estimate; return False if disk is insufficient.

    Every figure is derived: rows from the volume/mean-count model, bytes from the
    measured bytes/row of the existing trade dataset, wall time from the observed
    (or assumed) request rate. Refuses (returns False) when the estimated output
    would not fit in free space minus the safety margin.
    """
    total_rows = sum(p.est_rows for p in plans)
    total_tradeable = sum(p.tradeable for p in plans)
    est_bytes = int(total_rows * basis.bytes_per_row)
    req_s, measured = observed_req_s(store)
    est_requests = total_tradeable + sum(_pages_for(p.est_rows) for p in plans)
    est_seconds = est_requests / req_s if req_s > 0 else float("inf")
    free = free_bytes(root)
    margin = int(safety_margin_gb * GB)
    usable = free - margin

    print(f"\n=== {title} ===")
    print(f"{'series':<30}{'tradeable':>12}{'est_rows':>16}{'est_size':>14}")
    for p in sorted(plans, key=lambda x: x.est_rows, reverse=True):
        print(
            f"{p.series:<30}{p.tradeable:>12}{p.est_rows:>16,}"
            f"{_fmt_bytes(p.est_rows * basis.bytes_per_row):>14}"
        )
    print("-" * 72)
    print(f"{'TOTAL':<30}{total_tradeable:>12}{total_rows:>16,}{_fmt_bytes(est_bytes):>14}")
    print(
        f"\nbytes/row (measured over {basis.basis_rows:,} {basis.basis_label}): "
        f"{basis.bytes_per_row:.1f}"
    )
    rate_label = "observed" if measured else "assumed"
    print(
        f"request rate ({rate_label}): {req_s:.1f} req/s  ->  est. wall time "
        f"{_fmt_eta(est_seconds)} ({est_requests:,} requests)"
    )
    print(f"free disk: {_fmt_bytes(free)}   safety margin: {safety_margin_gb:.1f} GiB")
    if est_bytes > usable:
        print(
            f"REFUSING: estimated output {_fmt_bytes(est_bytes)} exceeds usable free space "
            f"{_fmt_bytes(usable)} (free minus margin). Free space or lower --top-n.",
            flush=True,
        )
        return False
    print(f"OK: estimated {_fmt_bytes(est_bytes)} fits within usable {_fmt_bytes(usable)}.")
    return True


def _volume_fp(m: dict[str, Any]) -> int:
    v = m.get("volume_fp")
    if not isinstance(v, str) or v == "":
        return 0
    try:
        return parse_scaled(v, FP_DECIMALS)
    except (SchemaViolation, ValueError):
        return 0


def fetch_market_volumes(client: RestClient, tickers: list[str]) -> dict[str, int]:
    """Batch ``/markets?tickers=`` lookup -> {ticker: volume_fp}; missing markets are absent."""
    out: dict[str, int] = {}
    for i in range(0, len(tickers), 100):
        chunk = tickers[i : i + 100]
        data = client.get_json("/markets", {"tickers": ",".join(chunk), "limit": "100"})
        for m in data.get("markets") or []:
            out[str(m["ticker"])] = _volume_fp(m)
    return out


def discover_series_scoped(
    client: RestClient, store: StateStore, root: Path, series: str, min_volume_fp: int
) -> int:
    """Crawl one series' settled+closed markets, persist meta, and seed WITH volume.

    Reuses the market_meta write path and per-scope discovery checkpoint of
    :func:`discover`, but records each market's ``volume_fp`` so a later scoped pull
    can filter on it. Returns how many seeded markets meet ``min_volume_fp`` (0 keeps
    all). The scope key is namespaced (``<status>:census:<series>``) so it never
    collides with a prior full-discovery checkpoint.
    """
    kept = 0
    for scope in DISCOVERY_STATUSES:
        key = f"{scope}:census:{series}"
        disc = store.get_discovery(key)
        if disc and disc["done"]:
            continue
        cursor = str(disc["cursor"]) if disc and disc["cursor"] else ""
        pages = disc["pages"] if disc else 0
        while True:
            params = {"limit": str(MARKETS_LIMIT), "status": scope, "series_ticker": series}
            if cursor:
                params["cursor"] = cursor
            data = client.get_json("/markets", params)
            markets = data.get("markets") or []
            next_cursor = str(data.get("cursor") or "")
            if markets:
                write_market_meta(root, key.replace(":", "_"), pages, markets)
            new_pages = pages + 1
            done = not next_cursor
            with store.txn():
                for m in markets:
                    ticker = str(m["ticker"])
                    vol = _volume_fp(m)
                    store.seed_market_vol(ticker, _series_of(ticker), vol)
                    if vol >= min_volume_fp:
                        kept += 1
                store.set_discovery(key, next_cursor, new_pages, done)
            pages = new_pages
            if done:
                break
            if next_cursor == cursor:
                raise BackfillError(f"discovery cursor did not advance for scope={key}")
            cursor = next_cursor
    return kept


def _pull_ticker_list(
    client: RestClient,
    store: StateStore,
    root: Path,
    tickers: list[str],
    label: str,
    interval: float = PROGRESS_INTERVAL_S,
) -> None:
    """Pull an explicit list of markets (shared by scoped-series and parlay modes).

    Persists a rolling ``observed_req_s`` into the meta table on completion, so
    later cost previews use a measured rate instead of the assumed default.
    """
    if not tickers:
        print(f"nothing to pull for {label}.")
        return
    print(f"pulling {len(tickers)} market(s) for {label}...")
    progress = Progress(len(tickers))
    stop = threading.Event()
    start = time.monotonic()
    req0 = client.request_count
    reporter = threading.Thread(
        target=_reporter, args=(progress, client, stop, start, interval), daemon=True
    )
    reporter.start()
    try:
        for ticker in tickers:
            pull_market(client, store, root, ticker, progress)
    finally:
        stop.set()
        reporter.join(timeout=2.0)
    elapsed = time.monotonic() - start
    reqs = client.request_count - req0
    if elapsed > 0 and reqs > 0:
        store.set_meta("observed_req_s", f"{reqs / elapsed:.4f}")
    _, complete, failed, trades = progress.snapshot()
    rate = reqs / elapsed if elapsed > 0 else 0.0
    print(
        f"[backfill] {label} done: {complete}/{len(tickers)} complete, {failed} failed, "
        f"{trades} trades in {elapsed:.1f}s ({reqs} requests, {rate:.1f} req/s)"
    )


def run_pull_series(
    client: RestClient, store: StateStore, root: Path, series: str, min_volume_fp: int
) -> None:
    tickers = store.select_to_pull_series([series], min_volume_fp)
    _pull_ticker_list(client, store, root, tickers, f"series={series}")


def _extract_legs(m: dict[str, Any]) -> list[str] | None:
    """Constituent single-event leg tickers of a multivariate/parlay market, or None.

    Kalshi exposes a multivariate market's legs as ``mve_selected_legs[].market_ticker``.
    Returns the deduped leg tickers, or None when the market carries no derivable legs
    (an orphan combo that must not be silently pulled).
    """
    legs = m.get("mve_selected_legs")
    seen: dict[str, None] = {}
    if isinstance(legs, list):
        for leg in legs:
            if isinstance(leg, dict):
                mt = leg.get("market_ticker")
                if isinstance(mt, str) and mt:
                    seen.setdefault(mt, None)
    return list(seen) if seen else None


@dataclass(frozen=True)
class ParlayMarket:
    ticker: str
    volume_fp: int
    legs: list[str] | None


def rank_parlay_by_volume(
    client: RestClient, top_n: int, scan_pages: int
) -> tuple[list[ParlayMarket], int]:
    """Top-N parlay markets by volume within a bounded scan. Returns (ranked, scanned).

    The parlay universe (millions of combos) is too large to enumerate, so this
    crawls at most ``scan_pages`` pages per (settled, closed) scope and keeps a
    running min-heap of the highest-volume markets seen. The result is
    'top-N among the scanned pool', and is labelled as such in the preview.
    """
    heap: list[tuple[int, int, ParlayMarket]] = []
    counter = 0
    scanned = 0
    for scope in DISCOVERY_STATUSES:
        cursor = ""
        pages = 0
        while pages < scan_pages:
            params = {"limit": str(MARKETS_LIMIT), "status": scope, "series_ticker": PARLAY_SERIES}
            if cursor:
                params["cursor"] = cursor
            data = client.get_json("/markets", params)
            markets = data.get("markets") or []
            for m in markets:
                pm = ParlayMarket(str(m["ticker"]), _volume_fp(m), _extract_legs(m))
                scanned += 1
                counter += 1
                if len(heap) < top_n:
                    heapq.heappush(heap, (pm.volume_fp, counter, pm))
                elif pm.volume_fp > heap[0][0]:
                    heapq.heapreplace(heap, (pm.volume_fp, counter, pm))
            cursor = str(data.get("cursor") or "")
            pages += 1
            if not cursor:
                break
    ranked = [pm for _, _, pm in sorted(heap, key=lambda x: x[0], reverse=True)]
    return ranked, scanned


def run_parlay(
    client: RestClient,
    store: StateStore,
    root: Path,
    top_n: int,
    scan_pages: int,
    basis: CostBasis,
    safety_margin_gb: float,
    dry_run: bool,
) -> None:
    """Rank the parlay series by volume, then pull the top N combos and their legs.

    For each parlay we identify constituent single-event legs from
    ``mve_selected_legs`` and pull them too, so combos can be decomposed against
    their legs. Parlays with no derivable legs are reported and NOT pulled.
    """
    print(
        f"ranking parlay series {PARLAY_SERIES} by volume "
        f"(scan cap {scan_pages} pages/scope)..."
    )
    ranked, scanned = rank_parlay_by_volume(client, top_n, scan_pages)
    print(f"scanned {scanned} parlay markets; top {len(ranked)} selected by volume.")
    with_legs = [pm for pm in ranked if pm.legs]
    orphans = [pm for pm in ranked if not pm.legs]
    if orphans:
        print(
            f"WARNING: {len(orphans)}/{len(ranked)} top parlays expose no derivable legs "
            f"(mve_selected_legs empty); these orphan combos will NOT be pulled:"
        )
        for pm in orphans[:10]:
            print(f"    orphan {pm.ticker} (vol {pm.volume_fp / FP_SCALE:.0f})")
        if len(orphans) > 10:
            print(f"    ... and {len(orphans) - 10} more")

    leg_tickers: dict[str, None] = {}
    for pm in with_legs:
        for leg in pm.legs or []:
            leg_tickers.setdefault(leg, None)
    leg_list = list(leg_tickers)
    print(f"parlays with legs: {len(with_legs)}; distinct constituent legs: {len(leg_list)}")
    leg_vols = fetch_market_volumes(client, leg_list) if leg_list else {}

    parlay_rows = sum(est_rows_for_market(pm.volume_fp, basis, PARLAY_SERIES) for pm in with_legs)
    leg_rows = sum(est_rows_for_market(v, basis, _series_of(t)) for t, v in leg_vols.items())
    plans = [
        SeriesPlan(f"{PARLAY_SERIES} (parlays)", len(with_legs), parlay_rows),
        SeriesPlan("constituent legs", len(leg_vols), leg_rows),
    ]
    ok = print_cost_preview(
        f"PARLAY BACKFILL PREVIEW (top {top_n} of scanned)",
        plans,
        basis,
        store,
        safety_margin_gb,
        root,
    )
    if not ok:
        return
    if dry_run:
        print("\n[dry-run] parlay -> leg mapping (first 20):")
        for pm in with_legs[:20]:
            print(
                f"  {pm.ticker} (vol {pm.volume_fp / FP_SCALE:.0f})  ->  "
                f"{', '.join(pm.legs or [])}"
            )
        if len(with_legs) > 20:
            print(f"  ... and {len(with_legs) - 20} more")
        print("[dry-run] stopping before any pull.")
        return

    with store.txn():
        for pm in with_legs:
            store.seed_market_vol(pm.ticker, _series_of(pm.ticker), pm.volume_fp)
        for t, v in leg_vols.items():
            store.seed_market_vol(t, _series_of(t), v)
    to_pull = store.pending_tickers([pm.ticker for pm in with_legs] + leg_list)
    _pull_ticker_list(client, store, root, to_pull, f"parlay top-{top_n} + legs")


def run_census_backfill(
    client: RestClient,
    store: StateStore,
    root: Path,
    census_path: Path,
    top_n: int,
    excludes: list[str],
    include_parlay: bool,
    min_volume_fp: int,
    safety_margin_gb: float,
    dry_run: bool,
) -> None:
    rows = load_census(census_path)
    selected = select_from_census(rows, top_n, excludes, include_parlay)
    if not selected:
        print("census selection is empty (check --top-n/--exclude).")
        return
    basis = measure_cost_basis(root)
    plans = [
        SeriesPlan(
            r.series,
            r.est_tradeable,
            est_rows_for_series(r.est_tradeable, r.mean_vol, basis, r.series),
        )
        for r in selected
    ]
    ok = print_cost_preview(
        f"CENSUS BACKFILL PREVIEW (top {top_n})", plans, basis, store, safety_margin_gb, root
    )
    if not ok:
        return
    if dry_run:
        print("\n[dry-run] stopping before scoped discovery/pull.")
        return
    for r in selected:
        print(f"\n-- scoped discovery: {r.series} --")
        kept = discover_series_scoped(client, store, root, r.series, min_volume_fp)
        print(f"   {r.series}: {kept} market(s) meet the volume threshold; pulling...")
        run_pull_series(client, store, root, r.series, min_volume_fp)
    print_status_scoped(store, [r.series for r in selected])


def print_status_scoped(store: StateStore, series: list[str] | None) -> None:
    by_series = store.summary_by_series(series)
    if not by_series:
        if series:
            print("  (no scoped activity for the given series)")
        return
    print("\nper-series scoped progress:")
    print(
        f"  {'series':<28}{'complete':>9}{'inprog':>8}{'pending':>8}"
        f"{'failed':>8}{'trades':>14}"
    )
    for s in sorted(by_series):
        st = by_series[s]
        comp = st.get(Status.COMPLETE.value, (0, 0))
        inp = st.get(Status.IN_PROGRESS.value, (0, 0))
        pend = st.get(Status.NOT_STARTED.value, (0, 0))
        fail = st.get(Status.FAILED.value, (0, 0))
        trades = sum(v[1] for v in st.values())
        print(
            f"  {s:<28}{comp[0]:>9}{inp[0]:>8}{pend[0]:>8}{fail[0]:>8}{trades:>14,}"
        )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill",
        description="REST backfill of historical Kalshi trades (checkpointed, rate-limited).",
    )
    parser.add_argument("--discover", action="store_true", help="enumerate closed/settled markets")
    parser.add_argument(
        "--pull",
        nargs="?",
        const="",
        default=None,
        metavar="PREFIX",
        help="pull trades for all markets, or only those whose ticker starts with PREFIX",
    )
    parser.add_argument(
        "--series", default=None, metavar="PREFIX", help="ticker/series prefix filter for --pull"
    )
    parser.add_argument("--status", action="store_true", help="print checkpoint summary and exit")
    parser.add_argument(
        "--retry-failed", action="store_true", help="reset failed markets, then pull them again"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="report intended work without making changes"
    )
    # Phase 4.3 — census-driven scoped backfill.
    parser.add_argument(
        "--series-from-census",
        action="store_true",
        help="scoped discovery+pull of the top census series (see --top-n/--exclude/--min-volume)",
    )
    parser.add_argument(
        "--top-n", type=int, default=0, metavar="N",
        help="select the N highest-estimated-tradeable census series",
    )
    parser.add_argument(
        "--exclude", action="append", default=[], metavar="PREFIX",
        help="exclude census series whose ticker starts with PREFIX (repeatable)",
    )
    parser.add_argument(
        "--include-parlay", action="store_true",
        help=f"do not auto-exclude {PARLAY_SERIES} from census selection",
    )
    parser.add_argument(
        "--min-volume", type=float, default=0.0, metavar="V",
        help="skip markets whose traded volume (contracts) is below V",
    )
    parser.add_argument(
        "--parlay-top-n", type=int, default=None, metavar="N",
        help=f"rank {PARLAY_SERIES} by volume and pull the top N combos with their legs",
    )
    parser.add_argument(
        "--parlay-scan-cap", type=int, default=PARLAY_SCAN_PAGES_DEFAULT, metavar="PAGES",
        help="max pages/scope to scan when ranking parlays (bounded crawl)",
    )
    parser.add_argument(
        "--safety-margin-gb", type=float, default=None, metavar="GB",
        help="free-space safety margin for the cost preview (default: config min_free_gb_start)",
    )
    parser.add_argument(
        "--census-file", default=DEFAULT_CENSUS_ROWS, metavar="PATH",
        help="ranked census JSON produced by the census pass",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = CONFIG.data_dir / "parquet"
    db_path = CONFIG.data_dir / "backfill_state.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    prefix = args.series if args.series is not None else (args.pull or "")
    want_pull = args.pull is not None or args.retry_failed
    want_census = args.series_from_census
    want_parlay = args.parlay_top_n is not None
    thr = round(args.min_volume * FP_SCALE) if args.min_volume > 0 else 0
    margin = (
        args.safety_margin_gb if args.safety_margin_gb is not None else CONFIG.min_free_gb_start
    )
    if not (args.discover or want_pull or args.status or want_census or want_parlay):
        build_parser().print_help()
        return 2

    store = StateStore(db_path)
    signer = signer_from_config(CONFIG.key_id, CONFIG.pem_path)
    client = RestClient(signer)
    try:
        limits = fetch_rate_limits(client)
        rate = limits.read_refill * RATE_SAFETY_MARGIN
        capacity = limits.read_capacity * RATE_SAFETY_MARGIN
        client.set_bucket(TokenBucket(rate, capacity))
        print(
            f"account read limits: capacity={limits.read_capacity} refill={limits.read_refill}/s; "
            f"write capacity={limits.write_capacity} refill={limits.write_refill}/s"
        )
        print(
            f"token bucket (safety margin {RATE_SAFETY_MARGIN}): "
            f"rate={rate:.1f} tok/s capacity={capacity:.1f}"
        )

        if args.status:
            print_status(store)
        if args.discover:
            discover(client, store, root, args.dry_run, args.series or "")
        if args.retry_failed:
            n = store.reset_failed(prefix)
            print(f"reset {n} failed market(s) to not_started")
        if want_pull:
            if store.count_markets() == 0:
                print("no markets in the state store; run --discover first.", file=sys.stderr)
                return 1
            run_pull(client, store, root, prefix, args.dry_run)
        if want_census:
            run_census_backfill(
                client, store, root, Path(args.census_file), args.top_n,
                list(args.exclude), args.include_parlay, thr, margin, args.dry_run,
            )
        if want_parlay:
            basis = measure_cost_basis(root)
            run_parlay(
                client, store, root, args.parlay_top_n, args.parlay_scan_cap,
                basis, margin, args.dry_run,
            )
    finally:
        client.close()
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
