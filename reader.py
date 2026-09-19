"""Phase 3: the reader — a typed query layer over the compactor's Parquet datasets.

Backtest scripts import :class:`Reader` and call typed methods; they never write
SQL and never see scaled integers. Design rules enforced here:

* **DuckDB over Parquet directly** — no import/copy step. Queries filter on the
  hive ``date`` partition column so a single-day query prunes to that day's
  files (prove it with :meth:`QueryResult.explain`).
* **Units are the reader's job.** Monetary columns are returned as
  :class:`decimal.Decimal` USD and quantity columns as ``Decimal`` contracts.
  The scale is read from each Parquet field's ``scale`` metadata, never
  hardcoded, so the reader stays correct if the compactor's scaling changes.
* **Coverage is enforced, not advisory.** Every query consults the ``gap``
  dataset for its window. If any gap overlaps, you cannot materialize a frame
  or stream batches without passing ``allow_gaps=True`` — otherwise
  :class:`CoverageError` is raised.
* **Streaming.** Results are produced from a DuckDB Arrow ``RecordBatchReader``;
  :meth:`QueryResult.iter_batches` never holds more than one batch in RAM, so a
  multi-month all-ticker scan does not materialize in memory.
* **Trades are deduplicated by default.** The same exchange trade can be present
  both live (WS capture) and backfilled (REST re-pull) under one ``trade_id``.
  :meth:`Reader.trades` collapses those to one row per ``trade_id``, keeping the
  live copy when it exists (see that method for the justification). The raw,
  double-counting view is available only via ``raw=True``.

Absolute time filtering uses ``ts_wall`` (epoch-ns wall clock). ``ts_ns`` is the
recorder's monotonic receive clock and only orders rows *within a single run*;
it is returned for tie-breaking but never used as an absolute filter.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv

from schema import SOURCE_BACKFILL, SOURCE_LIVE

TimeArg = datetime | str

_BATCH_ROWS = 65_536
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class CoverageError(RuntimeError):
    """Raised when a query window overlaps a recorded gap and gaps were not acknowledged."""


class SchemaError(RuntimeError):
    """Raised when a decimal column is missing the ``scale`` metadata it needs to be unscaled."""


@dataclass(frozen=True)
class _Col:
    """One output column: its name, the source column in the scan, and how to type it."""

    out: str
    src: str
    kind: str  # 'decimal' | 'int' | 'string' | 'bool' | 'wall_ts'


_EMPTY_DTYPE = {
    "decimal": "object",
    "int": "int64",
    "nint": "Int64",  # nullable int (backfill trades have NULL ts_ns)
    "string": "string",
    "bool": "boolean",
    "wall_ts": "datetime64[ns, UTC]",
}

# price_series output. Prices are Decimal USD; sizes/volume/open_interest are
# Decimal contracts. ts_wall is tz-aware UTC; ts_ns is monotonic (ordering only).
PRICE_SERIES_COLS: tuple[_Col, ...] = (
    _Col("ts_wall", "ts_wall", "wall_ts"),
    _Col("ts_ns", "ts_ns", "int"),
    _Col("market_ticker", "market_ticker", "string"),
    _Col("price", "price_dollars", "decimal"),
    _Col("yes_bid", "yes_bid_dollars", "decimal"),
    _Col("yes_ask", "yes_ask_dollars", "decimal"),
    _Col("yes_bid_size", "yes_bid_size_fp", "decimal"),
    _Col("yes_ask_size", "yes_ask_size_fp", "decimal"),
    _Col("last_trade_size", "last_trade_size_fp", "decimal"),
    _Col("volume", "volume_fp", "decimal"),
    _Col("open_interest", "open_interest_fp", "decimal"),
)

# trades output. yes_price/no_price are Decimal USD; count is Decimal contracts.
# ts_ns is nullable: backfilled trades (source="backfill") carry NULL — order by ts_wall.
TRADES_COLS: tuple[_Col, ...] = (
    _Col("ts_wall", "ts_wall", "wall_ts"),
    _Col("ts_ns", "ts_ns", "nint"),
    _Col("source", "source", "string"),
    _Col("market_ticker", "market_ticker", "string"),
    _Col("trade_id", "trade_id", "string"),
    _Col("yes_price", "yes_price_dollars", "decimal"),
    _Col("no_price", "no_price_dollars", "decimal"),
    _Col("count", "count_fp", "decimal"),
    _Col("taker_side", "taker_side", "string"),
    _Col("taker_outcome_side", "taker_outcome_side", "string"),
    _Col("taker_book_side", "taker_book_side", "string"),
    _Col("is_block_trade", "is_block_trade", "bool"),
)

# list_tickers output: one string column of distinct market tickers.
TICKERS_COLS: tuple[_Col, ...] = (_Col("market_ticker", "market_ticker", "string"),)

# coverage output. gap_start/gap_end are tz-aware UTC; *_wall are raw epoch ns.
COVERAGE_COLS: tuple[_Col, ...] = (
    _Col("ts_wall", "ts_wall", "wall_ts"),
    _Col("gap_start", "gap_start_wall", "wall_ts"),
    _Col("gap_end", "gap_end_wall", "wall_ts"),
    _Col("gap_start_wall", "gap_start_wall", "int"),
    _Col("gap_end_wall", "gap_end_wall", "int"),
)


def _resolve_parquet_root() -> Path:
    """Locate the Parquet root the same way the compactor does (``$KALSHI_DATA_DIR/parquet``)."""
    load_dotenv()
    return Path(os.environ.get("KALSHI_DATA_DIR", "data")).resolve() / "parquet"


def _to_utc(value: TimeArg) -> datetime:
    """Normalize a datetime or ISO-8601 string to an aware UTC datetime.

    Naive inputs are assumed to already be UTC; aware inputs are converted.
    """
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value)
    else:
        raise TypeError(f"expected datetime or ISO-8601 str, got {type(value).__name__}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _to_ns(dt: datetime) -> int:
    """Exact epoch nanoseconds for an aware datetime (no float, so no precision loss)."""
    delta = dt - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _sql_literal_path(path: str) -> str:
    """Escape a filesystem path for embedding as a single-quoted SQL string literal."""
    return path.replace("'", "''")


def _select_sources(cols: tuple[_Col, ...]) -> list[str]:
    """Distinct source columns needed to build ``cols``, preserving order."""
    out: list[str] = []
    for c in cols:
        if c.src not in out:
            out.append(c.src)
    return out


def _empty_frame(cols: tuple[_Col, ...]) -> pd.DataFrame:
    """An empty DataFrame with exactly the declared columns and dtypes."""
    return pd.DataFrame({c.out: pd.Series([], dtype=_EMPTY_DTYPE[c.kind]) for c in cols})


def _batch_to_frame(
    batch: Any, cols: tuple[_Col, ...], scale_map: dict[str, int]
) -> pd.DataFrame:
    """Convert one Arrow ``RecordBatch`` into a typed pandas frame.

    Decimal columns are unscaled with the per-column scale from Parquet metadata;
    wall-clock columns become tz-aware UTC timestamps.
    """
    data: dict[str, Any] = {}
    for c in cols:
        arr = batch.column(batch.schema.get_field_index(c.src))
        raw = arr.to_pylist()
        if c.kind == "decimal":
            divisor = Decimal(scale_map[c.src])
            data[c.out] = pd.array(
                [None if v is None else Decimal(v) / divisor for v in raw], dtype="object"
            )
        elif c.kind == "int":
            data[c.out] = pd.array(raw, dtype="int64")
        elif c.kind == "nint":
            data[c.out] = pd.array(raw, dtype="Int64")
        elif c.kind == "string":
            data[c.out] = pd.array(raw, dtype="string")
        elif c.kind == "bool":
            data[c.out] = pd.array(raw, dtype="boolean")
        elif c.kind == "wall_ts":
            data[c.out] = pd.to_datetime(raw, unit="ns", utc=True)
        else:  # pragma: no cover - guarded by _Col construction
            raise SchemaError(f"unknown column kind {c.kind!r}")
    return pd.DataFrame(data)


@dataclass(frozen=True)
class Coverage:
    """Gap rows overlapping a query window.

    Attributes:
        gaps: DataFrame with columns ``ts_wall`` (when the gap was recorded,
            datetime64[ns, UTC]), ``gap_start`` / ``gap_end`` (datetime64[ns,
            UTC]) and ``gap_start_wall`` / ``gap_end_wall`` (int64 epoch ns).
    """

    gaps: pd.DataFrame

    @property
    def has_gaps(self) -> bool:
        return len(self.gaps) > 0


class QueryResult:
    """A prepared query plus the coverage of its window.

    Nothing runs until you call :meth:`frame`, :meth:`iter_batches` or
    :meth:`explain`. If the window overlaps a gap, ``frame`` and
    ``iter_batches`` raise :class:`CoverageError` unless ``allow_gaps=True``.
    """

    def __init__(
        self,
        con: Any,
        sql: str,
        params: list[Any],
        cols: tuple[_Col, ...],
        scale_map: dict[str, int],
        coverage: Coverage,
        has_files: bool,
    ) -> None:
        self._con = con
        self._sql = sql
        self._params = params
        self._cols = cols
        self._scale_map = scale_map
        self._coverage = coverage
        self._has_files = has_files

    @property
    def coverage(self) -> Coverage:
        """The gaps overlapping this query's window (may be empty)."""
        return self._coverage

    def _guard(self, allow_gaps: bool) -> None:
        if self._coverage.has_gaps and not allow_gaps:
            n = len(self._coverage.gaps)
            raise CoverageError(
                f"query window overlaps {n} recorded gap(s); the data is incomplete. "
                "Inspect result.coverage.gaps, then pass allow_gaps=True to proceed."
            )

    def frame(self, *, allow_gaps: bool = False) -> pd.DataFrame:
        """Materialize the full result as one typed DataFrame.

        Raises CoverageError if the window has gaps and allow_gaps is False.
        Returns an empty (but correctly typed) frame when no rows match.
        """
        self._guard(allow_gaps)
        if not self._has_files:
            return _empty_frame(self._cols)
        reader = self._con.execute(self._sql, self._params).fetch_record_batch(_BATCH_ROWS)
        frames = [_batch_to_frame(b, self._cols, self._scale_map) for b in reader]
        if not frames:
            return _empty_frame(self._cols)
        return pd.concat(frames, ignore_index=True)

    def iter_batches(
        self, batch_rows: int = _BATCH_ROWS, *, allow_gaps: bool = False
    ) -> Iterator[pd.DataFrame]:
        """Stream the result as typed DataFrames of at most ``batch_rows`` rows each.

        The coverage guard is applied eagerly (before any batch is produced).
        Only one batch is held in memory at a time.
        """
        self._guard(allow_gaps)
        if not self._has_files:
            return iter(())
        return self._stream(batch_rows)

    def _stream(self, batch_rows: int) -> Iterator[pd.DataFrame]:
        reader = self._con.execute(self._sql, self._params).fetch_record_batch(batch_rows)
        for batch in reader:
            yield _batch_to_frame(batch, self._cols, self._scale_map)

    def explain(self, *, analyze: bool = False) -> str:
        """Return the DuckDB query plan (or profiled plan with ``analyze=True``)."""
        if not self._has_files:
            return "(no parquet files for this dataset/window)"
        keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        rows = self._con.execute(f"{keyword} {self._sql}", self._params).fetchall()
        return "\n".join(str(r[-1]) for r in rows)


class Reader:
    """Typed query layer over the compactor's partitioned Parquet datasets.

    Args:
        parquet_root: Path to the ``parquet`` directory. Defaults to
            ``$KALSHI_DATA_DIR/parquet`` (matching the compactor).

    All time arguments accept a ``datetime`` (naive assumed UTC) or an ISO-8601
    string and are normalized to UTC at the boundary.
    """

    def __init__(self, parquet_root: Path | str | None = None) -> None:
        self._root = Path(parquet_root) if parquet_root is not None else _resolve_parquet_root()
        self._con = duckdb.connect(":memory:")
        self._scale_cache: dict[str, dict[str, int]] = {}

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> Reader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _type_dir(self, dataset: str) -> Path:
        return self._root / f"type={dataset}"

    def _type_files(self, dataset: str) -> list[Path]:
        return sorted(self._type_dir(dataset).glob("date=*/*.parquet"))

    def _glob(self, dataset: str) -> str:
        return (self._type_dir(dataset) / "date=*" / "*.parquet").as_posix()

    def _scale_map(self, dataset: str) -> dict[str, int]:
        """Per-column scale factors read from a representative file's Parquet metadata."""
        cached = self._scale_cache.get(dataset)
        if cached is not None:
            return cached
        result: dict[str, int] = {}
        files = self._type_files(dataset)
        if files:
            schema = pq.read_schema(files[0])
            for field in schema:
                meta = field.metadata or {}
                if b"scale" in meta:
                    result[field.name] = int(meta[b"scale"])
        self._scale_cache[dataset] = result
        return result

    def _window_ns(self, start: TimeArg, end: TimeArg) -> tuple[int, int, str, str]:
        """Return (start_ns, end_ns, date_lo, date_hi) for an inclusive UTC window."""
        s = _to_utc(start)
        e = _to_utc(end)
        if e < s:
            raise ValueError(f"end {e.isoformat()} precedes start {s.isoformat()}")
        return _to_ns(s), _to_ns(e), s.date().isoformat(), e.date().isoformat()

    def coverage(self, start: TimeArg, end: TimeArg) -> Coverage:
        """Gap rows whose interval overlaps ``[start, end]``.

        The gap dataset is tiny, so it is scanned without a date-partition filter
        to avoid ever pruning away a gap whose recording lands on a neighboring
        date. A gap overlaps the window when
        ``gap_start_wall <= end_ns AND gap_end_wall >= start_ns``.
        """
        start_ns, end_ns, _, _ = self._window_ns(start, end)
        if not self._type_files("gap"):
            return Coverage(_empty_frame(COVERAGE_COLS))
        path = _sql_literal_path(self._glob("gap"))
        sql = (
            "SELECT ts_wall, gap_start_wall, gap_end_wall "
            f"FROM read_parquet('{path}', hive_partitioning=true) "
            "WHERE gap_start_wall <= ? AND gap_end_wall >= ? "
            "ORDER BY gap_start_wall"
        )
        reader = self._con.execute(sql, [end_ns, start_ns]).fetch_record_batch(_BATCH_ROWS)
        frames = [_batch_to_frame(b, COVERAGE_COLS, {}) for b in reader]
        gaps = pd.concat(frames, ignore_index=True) if frames else _empty_frame(COVERAGE_COLS)
        return Coverage(gaps)

    def _build(
        self,
        dataset: str,
        cols: tuple[_Col, ...],
        start: TimeArg,
        end: TimeArg,
        *,
        extra_sql: str = "",
        extra_params: list[Any] | None = None,
        order: str = "",
        distinct: bool = False,
        qualify: str = "",
    ) -> QueryResult:
        start_ns, end_ns, date_lo, date_hi = self._window_ns(start, end)
        cov = self.coverage(start, end)
        files = self._type_files(dataset)
        scale_map = self._scale_map(dataset)
        if files:
            for c in cols:
                if c.kind == "decimal" and c.src not in scale_map:
                    raise SchemaError(
                        f"column {c.src!r} in dataset {dataset!r} has no 'scale' metadata; "
                        "cannot unscale to Decimal"
                    )
        select = ("SELECT DISTINCT " if distinct else "SELECT ") + ", ".join(_select_sources(cols))
        path = _sql_literal_path(self._glob(dataset))
        sql = (
            f"{select} FROM read_parquet('{path}', hive_partitioning=true) "
            "WHERE date BETWEEN ? AND ? AND ts_wall BETWEEN ? AND ? "
            f"{extra_sql} {qualify} {order}"
        ).strip()
        params: list[Any] = [date_lo, date_hi, start_ns, end_ns, *(extra_params or [])]
        return QueryResult(self._con, sql, params, cols, scale_map, cov, bool(files))

    def list_tickers(
        self, start: TimeArg, end: TimeArg, prefix: str | None = None
    ) -> QueryResult:
        """Distinct market tickers with ticker updates in ``[start, end]``.

        Args:
            prefix: if given, only tickers starting with this string.

        Result frame column:
            market_ticker (string): the distinct ticker, ordered ascending.
        """
        extra_sql = ""
        extra_params: list[Any] = []
        if prefix is not None:
            extra_sql = "AND starts_with(market_ticker, ?)"
            extra_params = [prefix]
        return self._build(
            "ticker",
            TICKERS_COLS,
            start,
            end,
            extra_sql=extra_sql,
            extra_params=extra_params,
            order="ORDER BY market_ticker",
            distinct=True,
        )

    def price_series(self, ticker: str, start: TimeArg, end: TimeArg) -> QueryResult:
        """Ordered ticker-channel time series for a single market in ``[start, end]``.

        Result frame columns:
            ts_wall (datetime64[ns, UTC]): recorder wall-clock receive time.
            ts_ns (int64): recorder monotonic receive time; orders rows within a run.
            market_ticker (string): the market.
            price (Decimal, USD): last price.
            yes_bid, yes_ask (Decimal, USD): top of book.
            yes_bid_size, yes_ask_size (Decimal, contracts): top-of-book sizes.
            last_trade_size (Decimal, contracts): size of the last trade.
            volume (Decimal, contracts): cumulative volume.
            open_interest (Decimal, contracts): open interest.
        Ordered by (ts_wall, ts_ns).
        """
        return self._build(
            "ticker",
            PRICE_SERIES_COLS,
            start,
            end,
            extra_sql="AND market_ticker = ?",
            extra_params=[ticker],
            order="ORDER BY ts_wall, ts_ns",
        )

    def trades(
        self,
        ticker_or_prefix: str,
        start: TimeArg,
        end: TimeArg,
        *,
        source: str | None = None,
        raw: bool = False,
    ) -> QueryResult:
        """Trades for one market or a ticker prefix in ``[start, end]``.

        Deduplicated by ``trade_id`` by default: the same exchange trade can be
        recorded both live (WS) and backfilled (REST re-pull), and the two copies
        share a ``trade_id``. A field-by-field comparison of the overlap shows
        every economic and exchange field (price, count, sides, block flag, ``ts``,
        ``ts_ms``) is identical between the copies; only ``ts_wall`` differs — the
        live row's is the recorder's WS *receive* time (~0.2s later), the backfill
        row's is the exchange ``created_time``. When both exist the **live** copy
        is kept: it loses no economic information, carries a real ``ts_ns``, and
        keeps ``ts_wall`` on the same receive-time basis as the surrounding live
        series. Backfill-only trades (from gaps in live capture) are kept as-is.

        Args:
            ticker_or_prefix: exact ticker or a prefix; an empty string matches all.
            source: if given, restrict to ``"live"`` (WS-captured) or ``"backfill"``
                (REST-pulled) trades. Default returns both, always with the
                ``source`` column present so a caller can tell them apart.
            raw: if True, return the un-deduplicated rows — every stored copy,
                including trades that appear under both sources. Off by default so
                the safe (non-double-counting) path requires no ceremony.

        Result frame columns:
            ts_wall (datetime64[ns, UTC]): exchange/recorder wall-clock time.
            ts_ns (Int64, nullable): recorder monotonic receive time. NULL for
                backfilled rows — the recorder's clock does not apply to them.
            source (string): "live" or "backfill" provenance.
            market_ticker (string): the market.
            trade_id (string): exchange trade id.
            yes_price, no_price (Decimal, USD): execution prices.
            count (Decimal, contracts): trade size.
            taker_side, taker_outcome_side, taker_book_side (string).
            is_block_trade (boolean).
        Ordered by (ts_wall, ts_ns). Because backfilled rows have NULL ts_ns,
        order by ts_wall for any cross-source time series (ts_ns only tie-breaks
        within a single live run).
        """
        extra_sql = "AND starts_with(market_ticker, ?)"
        extra_params: list[Any] = [ticker_or_prefix]
        if source is not None:
            if source not in (SOURCE_LIVE, SOURCE_BACKFILL):
                raise ValueError(
                    f"source must be {SOURCE_LIVE!r} or {SOURCE_BACKFILL!r}, got {source!r}"
                )
            extra_sql += " AND source = ?"
            extra_params.append(source)
        qualify = "" if raw else (
            f"QUALIFY row_number() OVER "
            f"(PARTITION BY trade_id ORDER BY (source = '{SOURCE_LIVE}') DESC) = 1"
        )
        return self._build(
            "trade",
            TRADES_COLS,
            start,
            end,
            extra_sql=extra_sql,
            extra_params=extra_params,
            order="ORDER BY ts_wall, ts_ns",
            qualify=qualify,
        )
