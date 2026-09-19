"""Phase 2 compactor: raw NDJSON.zst -> partitioned, explicitly-typed Parquet.

The compactor is a pure function of the raw files. It never mutates raw data,
is idempotent (manifest-checkpointed), streams in bounded memory, routes
malformed lines to a rejects file, and commits every output via atomic rename.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd
from dotenv import load_dotenv

from schema import SPECS, SchemaViolation

ROW_GROUP_SIZE = 128 * 1024
RAW_NAME_RE = re.compile(r"^(?P<hour>\d{2})\.ndjson\.zst$")
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Rough multiplier over the compressed input size, used only for --dry-run.
EST_OUTPUT_RATIO = 0.85


def resolve_data_dir() -> Path:
    load_dotenv()
    return Path(os.environ.get("KALSHI_DATA_DIR", "data")).resolve()


@dataclass
class FileResult:
    source: Path
    total_lines: int
    rows_by_type: dict[str, int]
    rejects: int
    input_bytes: int
    output_bytes_by_type: dict[str, int]

    @property
    def rows_written(self) -> int:
        return sum(self.rows_by_type.values())

    @property
    def output_bytes(self) -> int:
        return sum(self.output_bytes_by_type.values())

    @property
    def reconciled(self) -> bool:
        return self.rows_written + self.rejects == self.total_lines


class DatasetWriter:
    """Streams row-group-sized batches for one message type to one parquet file."""

    def __init__(self, spec_name: str, parquet_root: Path, date: str, hour: str) -> None:
        self._spec = SPECS[spec_name]
        self._final = parquet_root / f"type={spec_name}" / f"date={date}" / f"{hour}.parquet"
        self._tmp = self._final.parent / f"{self._final.name}.{os.getpid()}.tmp"
        self._cols: dict[str, list[Any]] = {n: [] for n in self._spec.schema.names}
        self._buffered = 0
        self.rows_written = 0
        self._writer: pq.ParquetWriter | None = None

    def add(self, row: dict[str, Any]) -> None:
        for name, bucket in self._cols.items():
            bucket.append(row[name])
        self._buffered += 1
        if self._buffered >= ROW_GROUP_SIZE:
            self._flush()

    def _flush(self) -> None:
        if self._buffered == 0:
            return
        arrays = [
            pa.array(self._cols[f.name], type=f.type) for f in self._spec.schema
        ]
        table = pa.Table.from_arrays(arrays, schema=self._spec.schema)
        if self._writer is None:
            self._final.parent.mkdir(parents=True, exist_ok=True)
            use_dict: list[str] | bool = list(self._spec.dict_columns) or False
            self._writer = pq.ParquetWriter(
                self._tmp, self._spec.schema, compression="zstd", use_dictionary=use_dict
            )
        self._writer.write_table(table, row_group_size=ROW_GROUP_SIZE)
        self.rows_written += self._buffered
        for bucket in self._cols.values():
            bucket.clear()
        self._buffered = 0

    def commit(self) -> int:
        """Finalize. Returns bytes written (0 if no rows). Removes stale output."""
        self._flush()
        if self._writer is not None:
            self._writer.close()
            os.replace(self._tmp, self._final)
            return self._final.stat().st_size
        # No rows for this type: ensure no stale output remains (pure function).
        if self._final.exists():
            self._final.unlink()
        return 0

    def abort(self) -> None:
        if self._writer is not None:
            self._writer.close()
        if self._tmp.exists():
            self._tmp.unlink()


class RejectWriter:
    """Appends malformed lines (with source + line number + reason) as NDJSON."""

    def __init__(self, parquet_root: Path, date: str, hour: str) -> None:
        self._final = parquet_root / "rejects" / f"date={date}" / f"{hour}.rejects.ndjson"
        self._tmp = self._final.parent / f"{self._final.name}.{os.getpid()}.tmp"
        self._fh: TextIO | None = None
        self.count = 0

    def write(self, source: str, line_number: int, reason: str, line: str) -> None:
        if self._fh is None:
            self._final.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._tmp, "w", encoding="utf-8")
        record = {
            "source": source,
            "line_number": line_number,
            "reason": reason,
            "line": line[:2000],
        }
        self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        self.count += 1

    def commit(self) -> None:
        if self._fh is not None:
            self._fh.close()
            os.replace(self._tmp, self._final)
        elif self._final.exists():
            self._final.unlink()

    def abort(self) -> None:
        if self._fh is not None:
            self._fh.close()
        if self._tmp.exists():
            self._tmp.unlink()


class Manifest:
    """Checkpoint of processed source files keyed by path + size + mtime."""

    def __init__(self, parquet_root: Path) -> None:
        self._path = parquet_root / "_manifest.json"
        self._entries: dict[str, dict[str, Any]] = {}
        if self._path.exists():
            with open(self._path, encoding="utf-8") as fh:
                self._entries = json.load(fh)

    @staticmethod
    def _key(raw_path: Path) -> str:
        return str(raw_path.resolve())

    def is_processed(self, raw_path: Path) -> bool:
        entry = self._entries.get(self._key(raw_path))
        if entry is None:
            return False
        stat = raw_path.stat()
        return bool(
            entry["size"] == stat.st_size and entry["mtime_ns"] == stat.st_mtime_ns
        )

    def record(self, result: FileResult) -> None:
        stat = result.source.stat()
        self._entries[self._key(result.source)] = {
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "processed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_lines": result.total_lines,
            "rows_by_type": result.rows_by_type,
            "rejects": result.rejects,
        }
        self._flush()

    def _flush(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.parent / f"{self._path.name}.{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._entries, fh, indent=2, sort_keys=True)
        os.replace(tmp, self._path)


def parse_partition(raw_path: Path) -> tuple[str, str]:
    name_match = RAW_NAME_RE.match(raw_path.name)
    date = raw_path.parent.name
    if name_match is None or not DATE_DIR_RE.match(date):
        raise ValueError(
            f"{raw_path} does not match data/raw/YYYY-MM-DD/HH.ndjson.zst layout"
        )
    return date, name_match.group("hour")


def iter_lines(raw_path: Path) -> Iterator[str]:
    with open(raw_path, "rb") as fh:
        reader = zstd.ZstdDecompressor().stream_reader(fh)
        text = io.TextIOWrapper(reader, encoding="utf-8")
        yield from text


def classify(outer: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return (message type, parsed inner message). Parses ``raw`` exactly once."""
    if outer.get("marker") == "gap":
        return "gap", {}
    inner = json.loads(outer["raw"])
    msg_type = inner.get("type")
    if not isinstance(msg_type, str) or msg_type not in SPECS:
        raise SchemaViolation(f"unhandled type {msg_type!r}")
    return msg_type, inner


def compact_file(raw_path: Path, parquet_root: Path) -> FileResult:
    date, hour = parse_partition(raw_path)
    writers = {name: DatasetWriter(name, parquet_root, date, hour) for name in SPECS}
    rejects = RejectWriter(parquet_root, date, hour)
    source_str = str(raw_path.resolve())
    total_lines = 0

    try:
        for physical_no, line in enumerate(iter_lines(raw_path), start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            total_lines += 1
            try:
                outer = json.loads(line)
                if not isinstance(outer, dict):
                    raise SchemaViolation("outer line is not an object")
            except ValueError as exc:
                rejects.write(source_str, physical_no, f"bad_outer_json:{exc}", line)
                continue
            try:
                msg_type, inner = classify(outer)
            except SchemaViolation as exc:
                rejects.write(source_str, physical_no, f"unhandled_type:{exc}", line)
                continue
            except ValueError as exc:
                rejects.write(source_str, physical_no, f"bad_inner_json:{exc}", line)
                continue
            except KeyError as exc:
                rejects.write(source_str, physical_no, f"missing_field:{exc}", line)
                continue
            try:
                row = SPECS[msg_type].extract(outer, inner)
            except (SchemaViolation, KeyError, ValueError, TypeError) as exc:
                rejects.write(source_str, physical_no, f"schema_violation:{exc}", line)
                continue
            writers[msg_type].add(row)
    except Exception:
        for writer in writers.values():
            writer.abort()
        rejects.abort()
        raise

    output_bytes = {name: w.commit() for name, w in writers.items()}
    rejects.commit()

    return FileResult(
        source=raw_path,
        total_lines=total_lines,
        rows_by_type={name: w.rows_written for name, w in writers.items()},
        rejects=rejects.count,
        input_bytes=raw_path.stat().st_size,
        output_bytes_by_type=output_bytes,
    )


def discover(data_dir: Path, mode: str, target: str | None) -> list[Path]:
    raw_root = data_dir / "raw"
    if mode == "file":
        assert target is not None
        return [Path(target).resolve()]
    if mode == "date":
        assert target is not None
        return sorted((raw_root / target).glob("*.ndjson.zst"))
    return sorted(raw_root.glob("*/*.ndjson.zst"))


@dataclass
class RunSummary:
    processed: list[FileResult] = field(default_factory=list)
    skipped: list[Path] = field(default_factory=list)


def _human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}TB"


def run(data_dir: Path, mode: str, target: str | None, dry_run: bool) -> int:
    parquet_root = data_dir / "parquet"
    manifest = Manifest(parquet_root)
    files = discover(data_dir, mode, target)
    if not files:
        print("no matching raw files found", flush=True)
        return 0

    pending = [f for f in files if f.exists() and not manifest.is_processed(f)]
    already = [f for f in files if f.exists() and manifest.is_processed(f)]

    if dry_run:
        print(f"[dry-run] {len(pending)} file(s) would be processed, "
              f"{len(already)} already done", flush=True)
        est_total = 0
        for f in pending:
            in_bytes = f.stat().st_size
            est = int(in_bytes * EST_OUTPUT_RATIO)
            est_total += est
            print(f"  {f}  in={_human(in_bytes)}  est_out~={_human(est)}", flush=True)
        print(f"[dry-run] estimated total output ~= {_human(est_total)}", flush=True)
        return 0

    summary = RunSummary(skipped=already)
    exit_code = 0
    for f in pending:
        result = compact_file(f, parquet_root)
        if not result.reconciled:
            print(
                f"RECONCILIATION FAILED for {f}: "
                f"rows={result.rows_written} + rejects={result.rejects} "
                f"!= lines={result.total_lines}",
                file=sys.stderr,
                flush=True,
            )
            exit_code = 1
            continue
        manifest.record(result)
        summary.processed.append(result)
        _print_file_report(result)

    _print_summary(summary)
    return exit_code


def _print_file_report(r: FileResult) -> None:
    ratio = (r.output_bytes / r.input_bytes) if r.input_bytes else 0.0
    reject_rate = (r.rejects / r.total_lines * 100) if r.total_lines else 0.0
    print(f"\n=== {r.source} ===", flush=True)
    print(f"  input={_human(r.input_bytes)}  output={_human(r.output_bytes)}  "
          f"ratio={ratio:.3f} (output/input)", flush=True)
    print(f"  source_lines={r.total_lines}  rows_written={r.rows_written}  "
          f"rejects={r.rejects} ({reject_rate:.4f}%)", flush=True)
    print("  by type:", flush=True)
    for name in SPECS:
        print(f"    {name:<11} rows={r.rows_by_type[name]:>9}  "
              f"out={_human(r.output_bytes_by_type[name])}", flush=True)
    print(f"  reconciliation: {r.rows_written} + {r.rejects} == {r.total_lines}  "
          f"-> {'OK' if r.reconciled else 'MISMATCH'}", flush=True)


def _print_summary(s: RunSummary) -> None:
    total_in = sum(r.input_bytes for r in s.processed)
    total_out = sum(r.output_bytes for r in s.processed)
    total_lines = sum(r.total_lines for r in s.processed)
    total_rejects = sum(r.rejects for r in s.processed)
    print("\n====== run summary ======", flush=True)
    print(f"  processed={len(s.processed)}  skipped(no-op)={len(s.skipped)}", flush=True)
    if s.processed:
        ratio = (total_out / total_in) if total_in else 0.0
        print(f"  total input={_human(total_in)}  output={_human(total_out)}  "
              f"ratio={ratio:.3f}", flush=True)
        print(f"  total source_lines={total_lines}  rejects={total_rejects}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dry-run", action="store_true",
                        help="report what would be processed and estimated output size")
    parser = argparse.ArgumentParser(
        description="Kalshi raw->Parquet compactor", parents=[common]
    )
    sub = parser.add_subparsers(dest="mode", required=True)
    p_file = sub.add_parser("file", parents=[common], help="compact a single raw file")
    p_file.add_argument("path")
    p_date = sub.add_parser("date", parents=[common], help="compact all hour-files for a date")
    p_date.add_argument("date")
    sub.add_parser("all", parents=[common], help="compact all unprocessed files")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    data_dir = resolve_data_dir()
    target = getattr(args, "path", None) or getattr(args, "date", None)
    return run(data_dir, args.mode, target, args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
