"""Recorder WebSocket-reconnect + gap-marker resilience check.

Exercises the one path that matters for unattended running: the recorder loses
its socket, backs off, reconnects, and writes a coverage-gap marker so downstream
readers know data is missing for that window. Self-contained so it runs to
completion locally even while the machine's network (and thus an agent's own API
link) is down.

Sequence:

  1. launch recorder.py in its OWN process group (so we can signal it later)
  2. warm up until it has connected and written messages
  3. optionally drop the network for DROP seconds (self-healing reconnect)
  4. wait for messages to resume after reconnect
  5. signal a clean shutdown -> the recorder drains its queue and flushes the
     final zstd frame (a hard kill would truncate the tail and lose the marker)
  6. decode the newest .zst and report whether a gap marker + resumed messages exist

Usage (from anywhere; paths resolve to the repo root):
    python tests/test_recorder_resilience.py --drop 22   # 0 = no drop, just validate flush

Re-run this on the VPS after deployment. The systemd wrapper changes behavior
this test depends on and none of it is exercised by running locally:
  * Signal handling: systemd sends SIGTERM on stop, and the unit's KillSignal /
    TimeoutStopSec decide whether the recorder gets to flush before SIGKILL. This
    driver sends SIGINT/SIGTERM directly; confirm the unit gives the recorder
    enough grace to flush the tail frame (otherwise the gap marker is lost).
  * Network stack: the default drop uses Windows `netsh wlan`. On the VPS pass
    --drop-cmd / --restore-cmd with the host's interface commands, e.g.
    --drop-cmd "sudo ip link set eth0 down" --restore-cmd "sudo ip link set eth0 up".
  * Restart-on-failure: systemd Restart=on-failure can race the recorder's own
    in-process reconnect. If the unit restarts the process, you get a fresh
    recorder (new file, no in-memory pending_gap) instead of a gap marker. Verify
    which one wins for a short outage.
"""

from __future__ import annotations

import argparse
import glob
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import zstandard as zstd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import CONFIG  # noqa: E402  (import after sys.path fix)

PY = sys.executable
IS_WIN = os.name == "nt"
PROFILE_FALLBACK = "ZyXEL_A8B0_5G"
REC_LOG = REPO_ROOT / "_wifitest_rec.log"
RECORDER = REPO_ROOT / "recorder.py"


def ts() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def net_up() -> bool:
    count_flag = "-n" if IS_WIN else "-c"
    r = subprocess.run(["ping", count_flag, "1", "-w", "1000", "8.8.8.8"],
                       capture_output=True, text=True)
    return r.returncode == 0


def active_profile() -> str:
    if not IS_WIN:
        return PROFILE_FALLBACK
    r = subprocess.run(["netsh", "wlan", "show", "interfaces"],
                       capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if line.strip().lower().startswith("profile"):
            return line.split(":", 1)[1].strip()
    return PROFILE_FALLBACK


def zst_files() -> set[str]:
    return set(glob.glob(str(CONFIG.raw_dir / "**" / "*.ndjson.zst"), recursive=True))


def decode_lines(path: str) -> list[dict]:
    import json
    dctx = zstd.ZstdDecompressor()
    out: list[dict] = []
    with open(path, "rb") as fh:
        # read_across_frames handles the concatenated frames append-mode produces
        reader = dctx.stream_reader(fh, read_across_frames=True)
        data = reader.read()
    for line in data.decode("utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def drop_network(seconds: int, profile: str,
                 drop_cmd: str | None, restore_cmd: str | None) -> None:
    """Drop connectivity for ~`seconds`, then restore and wait for internet.

    Defaults to Windows `netsh wlan` (disconnect hammered in a loop to defeat the
    OS auto-reconnect, then connect by profile). Override with --drop-cmd /
    --restore-cmd on other hosts (e.g. `sudo ip link set eth0 down` / `... up`).
    """
    if drop_cmd is None and not IS_WIN:
        log("SKIP drop: no --drop-cmd given and default is Windows-only; "
            "pass --drop-cmd/--restore-cmd for this host")
        return

    log(f"=== DROP network for ~{seconds}s ===")
    end = time.time() + seconds
    while time.time() < end:
        if drop_cmd:
            subprocess.run(drop_cmd, shell=True, capture_output=True, text=True)
        else:
            subprocess.run(["netsh", "wlan", "disconnect"],
                           capture_output=True, text=True)
        time.sleep(2.5)

    log("=== RESTORE network ===")
    for i in range(45):
        if restore_cmd:
            subprocess.run(restore_cmd, shell=True, capture_output=True, text=True)
        else:
            subprocess.run(["netsh", "wlan", "connect", f"name={profile}"],
                           capture_output=True, text=True)
        if net_up():
            log(f"internet restored after ~{i + 1}s")
            return
        time.sleep(1)
    log("WARNING: internet not confirmed restored within 45s")


def clean_shutdown(proc: subprocess.Popen) -> None:
    """Ask the recorder to stop so it flushes the zstd tail frame."""
    if IS_WIN:
        log("sending CTRL_BREAK_EVENT for clean shutdown...")
        os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
    else:
        log("sending SIGINT for clean shutdown...")
        proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=30)
        log(f"recorder exited cleanly rc={proc.returncode}")
    except subprocess.TimeoutExpired:
        log("CLEAN SHUTDOWN TIMED OUT; terminating (tail may be truncated)")
        proc.terminate()
        proc.wait(timeout=10)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--drop", type=int, default=22, help="seconds to drop network (0 = none)")
    ap.add_argument("--warmup", type=int, default=14)
    ap.add_argument("--resume", type=int, default=16)
    ap.add_argument("--drop-cmd", default=None,
                    help="shell command to drop connectivity (default: netsh wlan disconnect)")
    ap.add_argument("--restore-cmd", default=None,
                    help="shell command to restore connectivity (default: netsh wlan connect)")
    args = ap.parse_args()

    profile = active_profile()
    before = zst_files()
    log(f"host: {'windows' if IS_WIN else os.name}  active profile: {profile}")
    log(f"pre-existing zst files: {len(before)}")

    recfh = open(REC_LOG, "w", encoding="utf-8")
    log(f"launching recorder (own process group), logging to {REC_LOG}")
    popen_kwargs: dict = {"stdout": recfh, "stderr": subprocess.STDOUT, "cwd": str(REPO_ROOT)}
    if IS_WIN:
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        popen_kwargs["start_new_session"] = True
    proc = subprocess.Popen([PY, str(RECORDER)], **popen_kwargs)

    try:
        log(f"warmup {args.warmup}s (let it connect + record)...")
        time.sleep(args.warmup)
        if proc.poll() is not None:
            log(f"ERROR: recorder exited early rc={proc.returncode}")
            return 2

        if args.drop > 0:
            drop_network(args.drop, profile, args.drop_cmd, args.restore_cmd)
        else:
            log("drop=0: skipping network drop (flush-validation run)")

        log(f"resume wait {args.resume}s (let messages flow post-reconnect)...")
        time.sleep(args.resume)
    finally:
        # Always attempt a clean stop so the zstd tail frame is flushed.
        if proc.poll() is None:
            clean_shutdown(proc)
        recfh.close()

    # ---- verify ----
    after = zst_files()
    targets = sorted(after, key=os.path.getmtime, reverse=True)
    if not targets:
        log("FAIL: no .zst files found")
        return 1
    target = targets[0]
    log(f"inspecting newest zst: {target} ({os.path.getsize(target)} bytes)")
    try:
        rows = decode_lines(target)
    except Exception as exc:  # noqa: BLE001
        log(f"FAIL: could not decode zst (tail truncated?): {exc!r}")
        return 1

    raws = [r for r in rows if "raw" in r]
    gaps = [r for r in rows if r.get("marker") == "gap"]
    log(f"decoded {len(rows)} lines: {len(raws)} messages, {len(gaps)} gap marker(s)")

    if not gaps:
        log("RESULT: no gap marker (expected only on a drop run)")
        return 0 if args.drop == 0 else 1

    g = gaps[0]
    gs, ge = g["gap_start_wall"], g["gap_end_wall"]
    dur = (ge - gs) / 1e9
    log(f"gap marker: start={datetime.fromtimestamp(gs / 1e9, UTC).strftime('%H:%M:%S')} "
        f"end={datetime.fromtimestamp(ge / 1e9, UTC).strftime('%H:%M:%S')} duration={dur:.1f}s")
    before_gap = [r for r in raws if r["ts_wall"] < gs]
    after_gap = [r for r in raws if r["ts_wall"] > ge]
    log(f"messages before gap: {len(before_gap)}  after gap: {len(after_gap)}")

    ok = (
        gs < ge
        and 10 <= dur <= args.drop + 60
        and len(before_gap) > 0
        and len(after_gap) > 0
    )
    log("RESULT: " + ("PASS - gap marker present, sensible window, messages resumed"
                      if ok else "FAIL - see figures above"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
