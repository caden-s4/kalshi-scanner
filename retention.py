"""Retention: delete a raw hour-file only when its data is provably safe to lose.

A raw ``data/raw/YYYY-MM-DD/HH.ndjson.zst`` file is removed only if ALL hold:

  1. the compactor manifest records it as processed, and the file on disk still
     matches the recorded size + mtime (so the Parquet was built from THESE bytes);
  2. every Parquet output the manifest says it produced still exists on disk
     (plus the rejects sidecar, if any rejects were recorded);
  3. reconciliation held: rows_written + rejects == source lines; and
  4. the hour is at least ``--min-age-days`` old (default 7).

Age is necessary but never sufficient -- a file is never deleted on age alone.
Every deletion is logged with the reconciliation numbers that justified it.

The compactor manifest is treated as read-only here: a deleted file's manifest
entry is left intact (harmless -- compaction discovery only globs existing files,
so a stale entry is never stat'd again). Run ``--dry-run`` to see the per-file
reasoning without deleting anything.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import compactor

logger = logging.getLogger("retention")

DEFAULT_MIN_AGE_DAYS = 7.0


@dataclass
class Assessment:
    path: Path
    in_manifest: bool = False
    matches_disk: bool = False
    outputs_exist: bool = False
    reconciled: bool = False
    old_enough: bool = False
    age_days: float = 0.0
    total_lines: int = 0
    rows_written: int = 0
    rejects: int = 0
    missing_outputs: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)

    @property
    def deletable(self) -> bool:
        return (
            self.in_manifest
            and self.matches_disk
            and self.outputs_exist
            and self.reconciled
            and self.old_enough
        )


def load_manifest(parquet_root: Path) -> dict[str, dict[str, Any]]:
    path = parquet_root / "_manifest.json"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        data: dict[str, dict[str, Any]] = json.load(fh)
    return data


def _expected_outputs(
    parquet_root: Path, date: str, hour: str, entry: dict[str, Any]
) -> list[Path]:
    """Every output file the manifest says this source produced."""
    outputs: list[Path] = []
    rows_by_type: dict[str, int] = entry.get("rows_by_type", {})
    for type_name, count in rows_by_type.items():
        if count > 0:
            outputs.append(parquet_root / f"type={type_name}" / f"date={date}" / f"{hour}.parquet")
    if entry.get("rejects", 0) > 0:
        outputs.append(parquet_root / "rejects" / f"date={date}" / f"{hour}.rejects.ndjson")
    return outputs


def assess(
    path: Path,
    entry: dict[str, Any] | None,
    parquet_root: Path,
    min_age_days: float,
    now: datetime,
) -> Assessment:
    a = Assessment(path=path)

    try:
        date, hour = compactor.parse_partition(path)
    except ValueError:
        a.reasons.append("unrecognised raw path layout")
        return a

    hour_end = datetime.strptime(f"{date} {hour}", "%Y-%m-%d %H").replace(tzinfo=UTC) + timedelta(
        hours=1
    )
    a.age_days = (now - hour_end).total_seconds() / 86400.0
    a.old_enough = a.age_days >= min_age_days
    if not a.old_enough:
        a.reasons.append(f"too young ({a.age_days:.1f}d < {min_age_days:.0f}d min age)")

    if entry is None:
        a.reasons.append("not recorded as processed in manifest")
        return a
    a.in_manifest = True

    a.total_lines = int(entry.get("total_lines", 0))
    a.rejects = int(entry.get("rejects", 0))
    a.rows_written = sum(int(v) for v in entry.get("rows_by_type", {}).values())
    a.reconciled = a.rows_written + a.rejects == a.total_lines
    if not a.reconciled:
        a.reasons.append(
            f"manifest reconciliation mismatch "
            f"({a.rows_written} rows + {a.rejects} rejects != {a.total_lines} lines)"
        )

    stat = path.stat()
    a.matches_disk = (
        int(entry.get("size", -1)) == stat.st_size
        and int(entry.get("mtime_ns", -1)) == stat.st_mtime_ns
    )
    if not a.matches_disk:
        a.reasons.append("raw file changed since it was processed (size/mtime differ)")

    expected = _expected_outputs(parquet_root, date, hour, entry)
    a.missing_outputs = [str(p) for p in expected if not p.exists()]
    a.outputs_exist = not a.missing_outputs
    if not a.outputs_exist:
        a.reasons.append(f"missing {len(a.missing_outputs)} Parquet output(s)")

    return a


def assess_all(
    data_dir: Path, min_age_days: float, now: datetime | None = None
) -> list[Assessment]:
    moment = now or datetime.now(UTC)
    parquet_root = data_dir / "parquet"
    manifest = load_manifest(parquet_root)
    resolved = {str(Path(k).resolve()): v for k, v in manifest.items()}

    results: list[Assessment] = []
    for path in sorted((data_dir / "raw").glob("*/*.ndjson.zst")):
        entry = resolved.get(str(path.resolve()))
        results.append(assess(path, entry, parquet_root, min_age_days, moment))
    return results


def run(dry_run: bool, min_age_days: float, now: datetime | None = None) -> int:
    data_dir = compactor.resolve_data_dir()
    assessments = assess_all(data_dir, min_age_days, now)
    if not assessments:
        logger.info("no raw hour-files found under %s", data_dir / "raw")
        return 0

    deletable = [a for a in assessments if a.deletable]
    kept = [a for a in assessments if not a.deletable]

    for a in kept:
        logger.info("KEEP  %s -- %s", a.path, "; ".join(a.reasons) or "conditions not met")

    freed = 0
    for a in deletable:
        size = a.path.stat().st_size
        verb = "would DELETE" if dry_run else "DELETED"
        logger.info(
            "%s %s -- rows=%d + rejects=%d == lines=%d (reconciled); age=%.1fd; size=%s",
            verb,
            a.path,
            a.rows_written,
            a.rejects,
            a.total_lines,
            a.age_days,
            _human(size),
        )
        if not dry_run:
            os.unlink(a.path)
            freed += size

    logger.info(
        "%s: %d file(s) %s, %d kept%s",
        "dry-run" if dry_run else "retention",
        len(deletable),
        "deletable" if dry_run else "deleted",
        len(kept),
        "" if dry_run else f", {_human(freed)} freed",
    )
    return 0


def _human(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}TB"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Delete raw hour-files that are fully compacted, verified, and aged out."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the per-file decision and reasoning without deleting anything",
    )
    parser.add_argument(
        "--min-age-days",
        type=float,
        default=float(os.environ.get("RETENTION_MIN_AGE_DAYS", DEFAULT_MIN_AGE_DAYS)),
        help="minimum data age before a fully-verified file may be deleted (default 7)",
    )
    args = parser.parse_args(argv)
    if args.min_age_days < 0:
        parser.error("--min-age-days must be >= 0")
    return run(dry_run=args.dry_run, min_age_days=args.min_age_days)


if __name__ == "__main__":
    raise SystemExit(main())
