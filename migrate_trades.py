"""One-shot migration: stamp an explicit ``source`` column onto trade Parquet.

Before Phase 4.1 the trade dataset carried provenance implicitly: backfilled
rows were written with ``ts_ns == 0`` (the recorder's monotonic clock never
returns 0, so it doubled as a "not live" sentinel). That overloaded a timestamp
field with source semantics — any sort/filter/aggregate on ``ts_ns`` silently
pinned every backfilled row to the epoch.

This rewrites every ``type=trade`` file to the new :data:`schema.TRADE_SCHEMA`:

  * rows with ``ts_ns == 0`` become ``source="backfill"`` with ``ts_ns`` NULL
    (the honest representation of "the recorder's clock does not apply");
  * every other row becomes ``source="live"`` with ``ts_ns`` unchanged.

It is idempotent (files already carrying a ``source`` column are left alone),
writes each file atomically (temp + ``os.replace``), and verifies the row count
of every rewritten file before committing. It refuses to finish if any row
still holds ``ts_ns == 0`` afterwards.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv

from schema import SOURCE_BACKFILL, SOURCE_LIVE, SPECS, TRADE_SCHEMA

_TRADE_SPEC = SPECS["trade"]
_ROW_GROUP_SIZE = 128 * 1024


def _resolve_trade_root() -> Path:
    load_dotenv()
    data_dir = Path(os.environ.get("KALSHI_DATA_DIR", "data")).resolve()
    return data_dir / "parquet" / "type=trade"


def _migrate_table(table: pa.Table) -> pa.Table:
    """Add ``source`` and null out sentinel ``ts_ns`` values, then cast to schema."""
    ts_ns = table.column("ts_ns")
    is_backfill = pc.equal(ts_ns, 0)
    new_ts_ns = pc.if_else(is_backfill, pa.scalar(None, type=pa.int64()), ts_ns)
    source = pc.if_else(
        is_backfill, pa.scalar(SOURCE_BACKFILL), pa.scalar(SOURCE_LIVE)
    )

    arrays: list[pa.ChunkedArray | pa.Array] = []
    for name in TRADE_SCHEMA.names:
        if name == "ts_ns":
            arrays.append(new_ts_ns)
        elif name == "source":
            arrays.append(source)
        else:
            arrays.append(table.column(name))
    plain = pa.table(arrays, names=list(TRADE_SCHEMA.names))
    return plain.cast(TRADE_SCHEMA)


def _rewrite_file(path: Path) -> tuple[int, int]:
    """Rewrite one trade file in place. Returns (rows_before, rows_after)."""
    table = pq.read_table(path)
    rows_before = table.num_rows
    migrated = _migrate_table(table)
    if migrated.num_rows != rows_before:
        raise RuntimeError(
            f"{path}: row count changed in memory {rows_before} -> {migrated.num_rows}"
        )

    tmp = path.parent / f"{path.name}.{os.getpid()}.migrate.tmp"
    use_dict = list(_TRADE_SPEC.dict_columns)
    pq.write_table(
        migrated, tmp, compression="zstd", use_dictionary=use_dict,
        row_group_size=_ROW_GROUP_SIZE,
    )
    rows_after = pq.read_metadata(tmp).num_rows
    if rows_after != rows_before:
        tmp.unlink()
        raise RuntimeError(
            f"{path}: row count changed on disk {rows_before} -> {rows_after}"
        )
    os.replace(tmp, path)
    return rows_before, rows_after


@dataclass
class MigrationReport:
    files_total: int = 0
    files_migrated: int = 0
    files_skipped: int = 0
    rows_before: int = 0
    rows_after: int = 0


def run(trade_root: Path) -> MigrationReport:
    files = sorted(trade_root.glob("date=*/*.parquet"))
    report = MigrationReport(files_total=len(files))
    for i, path in enumerate(files, start=1):
        if "source" in pq.read_schema(path).names:
            meta = pq.read_metadata(path)
            report.files_skipped += 1
            report.rows_before += meta.num_rows
            report.rows_after += meta.num_rows
            continue
        before, after = _rewrite_file(path)
        report.files_migrated += 1
        report.rows_before += before
        report.rows_after += after
        if i % 500 == 0 or i == len(files):
            print(f"  ...{i}/{len(files)} files", flush=True)
    return report


def _count_sentinel(trade_root: Path) -> int:
    total = 0
    for path in sorted(trade_root.glob("date=*/*.parquet")):
        table = pq.read_table(path, columns=["ts_ns"])
        ts_ns = table.column("ts_ns")
        total += pc.sum(pc.cast(pc.equal(ts_ns, 0), pa.int64())).as_py() or 0
    return total


def main() -> int:
    trade_root = _resolve_trade_root()
    if not trade_root.exists():
        print(f"no trade dataset at {trade_root}", flush=True)
        return 0
    print(f"migrating trade Parquet under {trade_root}", flush=True)
    report = run(trade_root)
    print("\n====== migration report ======", flush=True)
    print(f"  files total     = {report.files_total}", flush=True)
    print(f"  files migrated  = {report.files_migrated}", flush=True)
    print(f"  files skipped   = {report.files_skipped} (already had source column)", flush=True)
    print(f"  rows before     = {report.rows_before}", flush=True)
    print(f"  rows after      = {report.rows_after}", flush=True)

    if report.rows_before != report.rows_after:
        print("ROW COUNT MISMATCH — migration is not lossless", file=sys.stderr, flush=True)
        return 1

    remaining = _count_sentinel(trade_root)
    print(f"  rows with ts_ns==0 remaining = {remaining}", flush=True)
    if remaining != 0:
        print("SENTINEL ROWS REMAIN — migration incomplete", file=sys.stderr, flush=True)
        return 1

    print("  OK: row counts match and no ts_ns==0 sentinel remains", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
