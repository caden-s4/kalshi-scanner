"""Independent free-space monitor.

Logs a WARNING when free space on the data volume drops below a configurable
threshold. This is deliberately separate from the recorder's in-process disk
guard so that low-space is visible in the journal even when the recorder is not
running (e.g. it already halted, or is being redeployed). It only observes and
logs; it never stops or deletes anything.
"""

from __future__ import annotations

import argparse
import logging
import os

from config import CONFIG
from recorder import _resolve_volume, free_gb

logger = logging.getLogger("free_space_monitor")

DEFAULT_WARN_GB = 10.0


def run(threshold_gb: float) -> int:
    free = free_gb(CONFIG.data_dir)
    volume = _resolve_volume(CONFIG.data_dir)
    if free < threshold_gb:
        logger.warning(
            "LOW DISK: %.2f GB free on %s (warn threshold %.2f GB)",
            free,
            volume,
            threshold_gb,
        )
    else:
        logger.info(
            "disk OK: %.2f GB free on %s (warn threshold %.2f GB)",
            free,
            volume,
            threshold_gb,
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Log a warning when free space on the data volume is low."
    )
    parser.add_argument(
        "--threshold-gb",
        type=float,
        default=float(os.environ.get("MIN_FREE_GB_WARN", DEFAULT_WARN_GB)),
        help="warn when free space falls below this many GB (default 10)",
    )
    args = parser.parse_args(argv)
    return run(args.threshold_gb)


if __name__ == "__main__":
    raise SystemExit(main())
