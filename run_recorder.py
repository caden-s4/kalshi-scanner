"""Recorder supervisor: translate an intentional disk-space halt into a distinct
exit code so systemd can refuse to restart the recorder into a full disk.

The recorder (recorder.py) exits non-zero for two very different reasons:

  * an intentional disk-space stop -- it refuses to start, or halts cleanly at an
    hourly rotation, when free space on the data volume falls below the configured
    threshold; and
  * an unexpected crash.

Both currently surface as exit status 1, which systemd alone cannot tell apart.
This supervisor re-checks free space when the child exits non-zero: if the volume
is below the *start* threshold, restarting would either be refused immediately or
halt again within minutes, so the stop was disk-driven and we exit
``EXIT_DISK_HALT``. The unit's ``RestartPreventExitStatus=75`` then leaves the
service stopped instead of hot-looping. Any other non-zero exit is propagated so
systemd restarts with backoff.

The wrapper never parses recorder data or logic; it only supervises the process
and measures the same volume the recorder measures (via ``recorder.free_gb``).
"""

from __future__ import annotations

import logging
import signal
import subprocess
import sys
from pathlib import Path
from types import FrameType

from config import CONFIG
from recorder import free_gb

logger = logging.getLogger("run_recorder")

# sysexits.h EX_TEMPFAIL. Here it marks an intentional disk-space halt that must
# NOT trigger an auto-restart; the systemd unit lists it in RestartPreventExitStatus.
EXIT_DISK_HALT = 75

RECORDER = Path(__file__).resolve().parent / "recorder.py"


def _run() -> int:
    proc = subprocess.Popen([sys.executable, str(RECORDER)])

    def _forward(signum: int, _frame: FrameType | None) -> None:
        # Relay stop signals so the recorder runs its own clean drain-and-flush
        # shutdown, then keep waiting for it to exit.
        proc.send_signal(signum)

    for name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, _forward)

    rc = proc.wait()
    if rc == 0:
        return 0

    free = free_gb(CONFIG.data_dir)
    if free < CONFIG.min_free_gb_start:
        logger.error(
            "recorder exited %d with only %.2f GB free (start threshold %.2f GB); "
            "treating as an intentional disk-space halt and NOT requesting a restart",
            rc,
            free,
            CONFIG.min_free_gb_start,
        )
        return EXIT_DISK_HALT

    # Child killed by a signal (e.g. OOM SIGKILL) surfaces as a negative rc on
    # POSIX; map it to the conventional 128+signal so systemd sees a clean 0-255.
    exit_code = rc if rc > 0 else 128 - rc
    logger.error(
        "recorder exited %d with %.2f GB free; treating as a crash "
        "(systemd will restart with backoff)",
        rc,
        free,
    )
    return exit_code


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return _run()


if __name__ == "__main__":
    raise SystemExit(main())
