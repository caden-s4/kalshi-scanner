"""Explicit Parquet schemas and row extractors for the compactor.

Schemas are derived from a real captured hour-file (see inventory in the project
notes), not from the Kalshi docs. Observed facts that drive the typing:

  * Monetary fields arrive as decimal-dollar STRINGS with exactly 4 decimal
    places (e.g. "0.6700", "0.0030"). They are stored as int64 scaled by 1e4
    (units of 1/10000 USD). This is lossless and avoids floats entirely.
  * Quantity fields (``*_fp``) arrive as fixed-point STRINGS with 2 decimal
    places (e.g. "2.00", "40998.00"). They are stored as int64 scaled by 1e2
    (units of 1/100 contract).
  * ``dollar_volume`` / ``dollar_open_interest`` already arrive as integers.
  * ``ts`` (unix seconds), ``ts_ms`` (unix ms), and the recorder's ``ts_ns`` /
    ``ts_wall`` are int64.
  * ``time`` is an ISO-8601 UTC string and is preserved verbatim.

Every field's scale is recorded in the Parquet field metadata so downstream
consumers can recover real units without guessing.

NOTE: the Phase-2 brief assumed prices were integer *cents* and counts integer
*contracts*. The live API instead sends 4-decimal dollar strings and 2-decimal
fixed-point quantity strings, so the integer units here are 1e-4 USD and 1e-2
contracts respectively. The principle (integers, no float precision loss) holds.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

DOLLAR_DECIMALS = 4
DOLLAR_SCALE = 10**DOLLAR_DECIMALS
FP_DECIMALS = 2
FP_SCALE = 10**FP_DECIMALS


class SchemaViolation(ValueError):
    """Raised when a line does not match the expected, explicit schema."""


def parse_scaled(value: object, decimals: int) -> int:
    """Parse a decimal string into a lossless scaled int64 without using float.

    Raises SchemaViolation if the value is not a decimal string or carries more
    fractional digits than the fixed scale allows.
    """
    if not isinstance(value, str):
        raise SchemaViolation(f"expected decimal string, got {type(value).__name__}")
    s = value.strip()
    neg = s.startswith("-")
    if neg:
        s = s[1:]
    if "." in s:
        int_part, frac_part = s.split(".", 1)
    else:
        int_part, frac_part = s, ""
    if not int_part.isdigit() or (frac_part and not frac_part.isdigit()):
        raise SchemaViolation(f"not a decimal: {value!r}")
    if len(frac_part) > decimals:
        raise SchemaViolation(f"too many decimals in {value!r} (max {decimals})")
    frac_part = frac_part.ljust(decimals, "0")
    magnitude: int = int(int_part) * (10**decimals) + int(frac_part or "0")
    return -magnitude if neg else magnitude


def _req_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise SchemaViolation(f"expected int, got {value!r}")
    return value


def _req_str(value: object) -> str:
    if not isinstance(value, str):
        raise SchemaViolation(f"expected str, got {value!r}")
    return value


def _req_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise SchemaViolation(f"expected bool, got {value!r}")
    return value


def _f(name: str, typ: pa.DataType, **meta: str) -> pa.Field:
    return pa.field(name, typ, nullable=False, metadata=meta or None)


def _dollar(name: str) -> pa.Field:
    return _f(name, pa.int64(), scale=str(DOLLAR_SCALE), unit="1e-4 USD", source="decimal-string")


def _fp(name: str) -> pa.Field:
    return _f(name, pa.int64(), scale=str(FP_SCALE), unit="1e-2 contract", source="decimal-string")


_TS_NS = _f("ts_ns", pa.int64(), unit="ns", source="recorder monotonic")
_TS_WALL = _f("ts_wall", pa.int64(), unit="ns", source="recorder wall clock")

# The trade dataset mixes provenance: live rows carry the recorder's monotonic
# ``ts_ns``; backfilled rows have no such clock and store NULL (never 0). The
# ``source`` column makes the provenance explicit instead of overloading ts_ns.
_TS_NS_TRADE = pa.field(
    "ts_ns",
    pa.int64(),
    nullable=True,
    metadata={"unit": "ns", "source": "recorder monotonic; NULL for backfill"},
)
SOURCE_LIVE = "live"
SOURCE_BACKFILL = "backfill"
_SOURCE = _f(
    "source", pa.dictionary(pa.int32(), pa.string()), values="live|backfill", source="provenance"
)


TICKER_SCHEMA = pa.schema(
    [
        _TS_NS,
        _TS_WALL,
        _f("sid", pa.int64()),
        _f("market_ticker", pa.string()),
        _f("market_id", pa.string()),
        _dollar("price_dollars"),
        _dollar("yes_bid_dollars"),
        _dollar("yes_ask_dollars"),
        _fp("yes_bid_size_fp"),
        _fp("yes_ask_size_fp"),
        _fp("last_trade_size_fp"),
        _fp("volume_fp"),
        _fp("open_interest_fp"),
        _f("dollar_volume", pa.int64(), source="native-int"),
        _f("dollar_open_interest", pa.int64(), source="native-int"),
        _f("ts", pa.int64(), unit="s", source="exchange"),
        _f("ts_ms", pa.int64(), unit="ms", source="exchange"),
        _f("time", pa.string(), source="iso-8601"),
    ]
)

TRADE_SCHEMA = pa.schema(
    [
        _TS_NS_TRADE,
        _TS_WALL,
        _f("sid", pa.int64()),
        _f("seq", pa.int64()),
        _f("trade_id", pa.string()),
        _f("market_ticker", pa.string()),
        _dollar("yes_price_dollars"),
        _dollar("no_price_dollars"),
        _fp("count_fp"),
        _f("taker_side", pa.string()),
        _f("taker_outcome_side", pa.string()),
        _f("taker_book_side", pa.string()),
        _f("is_block_trade", pa.bool_()),
        _f("ts", pa.int64(), unit="s", source="exchange"),
        _f("ts_ms", pa.int64(), unit="ms", source="exchange"),
        _SOURCE,
    ]
)

SUBSCRIBED_SCHEMA = pa.schema(
    [
        _TS_NS,
        _TS_WALL,
        _f("id", pa.int64()),
        _f("channel", pa.string()),
        _f("sid", pa.int64()),
    ]
)

GAP_SCHEMA = pa.schema(
    [
        _TS_NS,
        _TS_WALL,
        _f("gap_start_ns", pa.int64(), unit="ns", source="recorder monotonic"),
        _f("gap_start_wall", pa.int64(), unit="ns", source="recorder wall clock"),
        _f("gap_end_ns", pa.int64(), unit="ns", source="recorder monotonic"),
        _f("gap_end_wall", pa.int64(), unit="ns", source="recorder wall clock"),
    ]
)


Row = dict[str, Any]
Extractor = Callable[[Mapping[str, Any], Mapping[str, Any]], Row]


def extract_ticker(outer: Mapping[str, Any], inner: Mapping[str, Any]) -> Row:
    msg = inner["msg"]
    return {
        "ts_ns": _req_int(outer["ts_ns"]),
        "ts_wall": _req_int(outer["ts_wall"]),
        "sid": _req_int(inner["sid"]),
        "market_ticker": _req_str(msg["market_ticker"]),
        "market_id": _req_str(msg["market_id"]),
        "price_dollars": parse_scaled(msg["price_dollars"], DOLLAR_DECIMALS),
        "yes_bid_dollars": parse_scaled(msg["yes_bid_dollars"], DOLLAR_DECIMALS),
        "yes_ask_dollars": parse_scaled(msg["yes_ask_dollars"], DOLLAR_DECIMALS),
        "yes_bid_size_fp": parse_scaled(msg["yes_bid_size_fp"], FP_DECIMALS),
        "yes_ask_size_fp": parse_scaled(msg["yes_ask_size_fp"], FP_DECIMALS),
        "last_trade_size_fp": parse_scaled(msg["last_trade_size_fp"], FP_DECIMALS),
        "volume_fp": parse_scaled(msg["volume_fp"], FP_DECIMALS),
        "open_interest_fp": parse_scaled(msg["open_interest_fp"], FP_DECIMALS),
        "dollar_volume": _req_int(msg["dollar_volume"]),
        "dollar_open_interest": _req_int(msg["dollar_open_interest"]),
        "ts": _req_int(msg["ts"]),
        "ts_ms": _req_int(msg["ts_ms"]),
        "time": _req_str(msg["time"]),
    }


def extract_trade(outer: Mapping[str, Any], inner: Mapping[str, Any]) -> Row:
    msg = inner["msg"]
    return {
        "ts_ns": _req_int(outer["ts_ns"]),
        "ts_wall": _req_int(outer["ts_wall"]),
        "sid": _req_int(inner["sid"]),
        "seq": _req_int(inner["seq"]),
        "trade_id": _req_str(msg["trade_id"]),
        "market_ticker": _req_str(msg["market_ticker"]),
        "yes_price_dollars": parse_scaled(msg["yes_price_dollars"], DOLLAR_DECIMALS),
        "no_price_dollars": parse_scaled(msg["no_price_dollars"], DOLLAR_DECIMALS),
        "count_fp": parse_scaled(msg["count_fp"], FP_DECIMALS),
        "taker_side": _req_str(msg["taker_side"]),
        "taker_outcome_side": _req_str(msg["taker_outcome_side"]),
        "taker_book_side": _req_str(msg["taker_book_side"]),
        "is_block_trade": _req_bool(msg["is_block_trade"]),
        "ts": _req_int(msg["ts"]),
        "ts_ms": _req_int(msg["ts_ms"]),
        "source": SOURCE_LIVE,
    }


def extract_subscribed(outer: Mapping[str, Any], inner: Mapping[str, Any]) -> Row:
    msg = inner["msg"]
    return {
        "ts_ns": _req_int(outer["ts_ns"]),
        "ts_wall": _req_int(outer["ts_wall"]),
        "id": _req_int(inner["id"]),
        "channel": _req_str(msg["channel"]),
        "sid": _req_int(msg["sid"]),
    }


def extract_gap(outer: Mapping[str, Any], inner: Mapping[str, Any]) -> Row:
    return {
        "ts_ns": _req_int(outer["ts_ns"]),
        "ts_wall": _req_int(outer["ts_wall"]),
        "gap_start_ns": _req_int(outer["gap_start_ns"]),
        "gap_start_wall": _req_int(outer["gap_start_wall"]),
        "gap_end_ns": _req_int(outer["gap_end_ns"]),
        "gap_end_wall": _req_int(outer["gap_end_wall"]),
    }


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    schema: pa.Schema
    dict_columns: tuple[str, ...]
    extract: Extractor


SPECS: dict[str, DatasetSpec] = {
    "ticker": DatasetSpec(
        "ticker", TICKER_SCHEMA, ("market_ticker", "market_id"), extract_ticker
    ),
    "trade": DatasetSpec(
        "trade",
        TRADE_SCHEMA,
        ("market_ticker", "taker_side", "taker_outcome_side", "taker_book_side", "source"),
        extract_trade,
    ),
    "subscribed": DatasetSpec(
        "subscribed", SUBSCRIBED_SCHEMA, ("channel",), extract_subscribed
    ),
    "gap": DatasetSpec("gap", GAP_SCHEMA, (), extract_gap),
}
