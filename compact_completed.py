"""Hourly compaction driver for unattended operation.

Compacts every *completed* raw hour-file -- one whose UTC hour is strictly in the
past -- and never the hour the recorder is currently appending to. The recorder
partitions by UTC (data/raw/YYYY-MM-DD/HH.ndjson.zst), so the current UTC hour
key identifies the open file; every raw file with an earlier (date, hour) is
closed and safe to compact. Each is delegated to the Phase-2 compactor unchanged
(idempotent, manifest-checkpointed, atomically committed).

Exit status is non-zero if any file failed reconciliation, so the systemd unit
surfaces the failure and the retention step (which runs only on success) is held
back.
"""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

import compactor

logger = logging.getLogger("compact_completed")


def current_hour_key(now: datetime | None = None) -> tuple[str, str]:
    """UTC (date, hour) of the hour-file the recorder is currently writing."""
    moment = now or datetime.now(UTC)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%H")


def completed_raw_files(data_dir: Path, now: datetime | None = None) -> list[Path]:
    """Closed raw hour-files, oldest first, excluding the current UTC hour."""
    raw_root = data_dir / "raw"
    current = current_hour_key(now)
    files: list[Path] = []
    for path in sorted(raw_root.glob("*/*.ndjson.zst")):
        try:
            part = compactor.parse_partition(path)
        except ValueError:
            logger.warning("skipping unrecognised raw path %s", path)
            continue
        # Zero-padded hours + ISO dates make lexical tuple order == chronological.
        if part < current:
            files.append(path)
    return files


def run(dry_run: bool = False, now: datetime | None = None) -> int:
    data_dir = compactor.resolve_data_dir()
    files = completed_raw_files(data_dir, now)
    date, hour = current_hour_key(now)
    if not files:
        logger.info("no completed raw hour-files to compact (current UTC hour %s/%s)", date, hour)
        return 0
    logger.info(
        "compacting %d completed hour-file(s); current UTC hour %s/%s is excluded",
        len(files),
        date,
        hour,
    )
    exit_code = 0
    for path in files:
        rc = compactor.run(data_dir, "file", str(path), dry_run)
        if rc != 0:
            exit_code = rc
    return exit_code


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Compact all completed raw hour-files (never the open one)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="report which completed files would be compacted, without writing",
    )
    args = parser.parse_args(argv)
    return run(dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
