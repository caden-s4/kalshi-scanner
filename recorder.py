"""Kalshi market-data recorder (Phase 1).

A single asyncio process that subscribes to public WebSocket channels for all
markets and writes every message to hourly, zstd-compressed NDJSON files.

Architecture:
  * A reader task owns the socket. Its only job is to stamp each message with a
    monotonic + wall-clock receive time and hand it to a bounded queue. It never
    parses JSON and never blocks on disk I/O, so it cannot fall behind the socket.
  * A writer task drains the queue, applies optional prefix filtering, serializes
    one NDJSON line per message, and streams it through zstd to the current hour's
    file. It owns all file state and all rotation.
  * A stats task prints throughput, RSS, and free disk to stdout every 10 seconds.
  * A connection loop reconnects with capped, jittered exponential backoff and
    emits a gap marker so downstream consumers can see exactly where coverage is
    missing.
  * Two resource guards -- free disk and RSS -- poll their limit and, on breach,
    trip the one shared Shutdown. Every stop path (signal, disk, memory) therefore
    converges on the same drain-and-flush, which is what keeps the final zstd
    frame intact; an untrappable SIGKILL is the one case that cannot.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, BinaryIO, cast

import websockets
import zstandard as zstd

from auth import Signer, signer_from_config
from config import CONFIG, WS_PATH, WS_URL, Config

logger = logging.getLogger("recorder")

QUEUE_MAXSIZE = 100_000
ZSTD_LEVEL = 3
STATS_INTERVAL_S = 10.0
DISK_CHECK_INTERVAL_S = 60.0
MEMORY_CHECK_INTERVAL_S = 30.0
PROJECTION_WINDOW_S = 60.0
GIB = 1024**3
MIB = 1024**2

# In-process RSS ceiling. This is the *primary* memory guard: it must fire well
# before the cgroup limits in deploy/kalshi-recorder.service, because MemoryMax
# is a hard SIGKILL that would truncate the in-flight zstd frame. Steady-state
# RSS is tens of MB, so this only trips on a genuine regression.
RSS_HALT_MB = 400.0

# sysexits-adjacent, distinct from run_recorder's EXIT_DISK_HALT (75) so this
# stop is identifiable in the journal. Not in the unit's
# RestartPreventExitStatus, so systemd restarts -- a fresh process is the cure.
EXIT_MEMORY_HALT = 76

BACKOFF_BASE_S = 0.5
BACKOFF_CAP_S = 30.0
BACKOFF_JITTER_S = 1.0


def _resolve_volume(path: Path) -> Path:
    """Nearest existing ancestor of ``path`` on its real volume.

    Follows symlinks and walks up until an existing directory is found, so
    ``shutil.disk_usage`` measures the drive the recorder will actually write to
    even before the data directory has been created.
    """
    current = Path(os.path.realpath(path))
    while not current.exists():
        if current.parent == current:
            break
        current = current.parent
    return current


def free_gb(path: Path) -> float:
    """Free space in GiB on the volume backing ``path``."""
    return shutil.disk_usage(_resolve_volume(path)).free / GIB


def _rss_reader() -> Callable[[], int]:
    """Pick a resident-set-size reader for this platform, once, at import time.

    Linux (production) reads ``/proc/self/status``; Windows (development) calls
    ``K32GetProcessMemoryInfo``. Returning a bound callable keeps the per-sample
    cost to a single read with no branching.
    """
    status = Path("/proc/self/status")
    if status.exists():

        def _linux_rss() -> int:
            # A live process always reports VmRSS, in kB.
            after = status.read_text().partition("VmRSS:")[2]
            return int(after.split(maxsplit=1)[0]) * 1024

        return _linux_rss

    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class _ProcessMemoryCounters(ctypes.Structure):
            _fields_ = (
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            )

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.K32GetProcessMemoryInfo.restype = wintypes.BOOL
        kernel32.K32GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(_ProcessMemoryCounters),
            wintypes.DWORD,
        ]

        def _windows_rss() -> int:
            counters = _ProcessMemoryCounters()
            counters.cb = ctypes.sizeof(_ProcessMemoryCounters)
            ok = kernel32.K32GetProcessMemoryInfo(
                kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
            )
            if not ok:
                raise ctypes.WinError(ctypes.get_last_error())
            return int(counters.WorkingSetSize)

        return _windows_rss

    def _unsupported_rss() -> int:
        return 0

    return _unsupported_rss


rss_bytes = _rss_reader()


@dataclass(slots=True)
class Shutdown:
    """Single clean-shutdown trigger shared by signals and the disk guard.

    ``reason`` records *why* the first caller asked to stop so ``main`` can pick
    the right exit code (disk-full halts exit non-zero).
    """

    event: asyncio.Event
    reason: str = ""

    def request(self, reason: str) -> None:
        if not self.reason:
            self.reason = reason
        self.event.set()


@dataclass(slots=True)
class RawMessage:
    ts_ns: int
    ts_wall: int
    raw: str


@dataclass(slots=True)
class GapMarker:
    gap_start_ns: int
    gap_start_wall: int
    gap_end_ns: int
    gap_end_wall: int


class _Sentinel:
    """Enqueued after the reader stops to tell the writer to finish draining."""


SENTINEL = _Sentinel()


class Stats:
    """Plain counters mutated only from the single asyncio thread."""

    def __init__(self) -> None:
        self.start_ns = time.monotonic_ns()
        self.total = 0
        self.dropped = 0
        self.reconnects = 0
        self.bytes_written = 0


class _CountingFile:
    """Wraps a binary file so we can measure compressed bytes hitting disk."""

    def __init__(self, fh: BinaryIO, stats: Stats) -> None:
        self._fh = fh
        self._stats = stats

    def write(self, data: bytes) -> int:
        self._stats.bytes_written += len(data)
        return self._fh.write(data)

    def flush(self) -> None:
        self._fh.flush()


def _extract_ticker(raw: str) -> str | None:
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    msg = obj.get("msg")
    if isinstance(msg, dict):
        ticker = msg.get("market_ticker")
        if isinstance(ticker, str):
            return ticker
    return None


class Writer:
    """Owns the zstd stream and hourly file rotation."""

    def __init__(
        self,
        cfg: Config,
        queue: asyncio.Queue[object],
        stats: Stats,
        shutdown: Shutdown,
    ) -> None:
        self._cfg = cfg
        self._queue = queue
        self._stats = stats
        self._shutdown = shutdown
        self._prefixes: tuple[str, ...] = tuple(cfg.ticker_prefixes)
        self._cctx = zstd.ZstdCompressor(level=ZSTD_LEVEL)
        self._current_hour: str | None = None
        self._fh: BinaryIO | None = None
        self._stream: zstd.ZstdCompressionWriter | None = None

    async def run(self) -> None:
        try:
            while True:
                item = await self._queue.get()
                if isinstance(item, _Sentinel):
                    break
                if isinstance(item, RawMessage):
                    self._write_raw(item)
                elif isinstance(item, GapMarker):
                    self._write_marker(item)
        finally:
            self._close_stream()

    def _write_raw(self, item: RawMessage) -> None:
        if self._prefixes:
            ticker = _extract_ticker(item.raw)
            if ticker is not None and not ticker.startswith(self._prefixes):
                return
        self._emit(
            item.ts_wall,
            {"ts_ns": item.ts_ns, "ts_wall": item.ts_wall, "raw": item.raw},
        )

    def _write_marker(self, item: GapMarker) -> None:
        self._emit(
            item.gap_end_wall,
            {
                "ts_ns": item.gap_end_ns,
                "ts_wall": item.gap_end_wall,
                "marker": "gap",
                "gap_start_ns": item.gap_start_ns,
                "gap_start_wall": item.gap_start_wall,
                "gap_end_ns": item.gap_end_ns,
                "gap_end_wall": item.gap_end_wall,
            },
        )

    def _emit(self, ts_wall: int, obj: dict[str, object]) -> None:
        stream = self._rotate_if_needed(ts_wall)
        line = json.dumps(obj, separators=(",", ":")) + "\n"
        stream.write(line.encode("utf-8"))

    def _rotate_if_needed(self, ts_wall: int) -> zstd.ZstdCompressionWriter:
        dt = datetime.fromtimestamp(ts_wall / 1e9, tz=UTC)
        hour_key = dt.strftime("%Y-%m-%d/%H")
        if hour_key != self._current_hour or self._stream is None:
            self._close_stream()
            path = self._cfg.raw_dir / f"{hour_key}.ndjson.zst"
            path.parent.mkdir(parents=True, exist_ok=True)
            # Append mode keeps any data from a prior run of this same hour;
            # concatenated zstd frames decompress cleanly.
            self._fh = open(path, "ab")
            counting = cast(IO[bytes], _CountingFile(self._fh, self._stats))
            self._stream = self._cctx.stream_writer(counting, closefd=False)
            self._current_hour = hour_key
            logger.info("rotated to %s", path)
            self._check_disk()
        return self._stream

    def _check_disk(self) -> None:
        free = free_gb(self._cfg.data_dir)
        if free < self._cfg.min_free_gb_halt:
            logger.error(
                "free space %.2f GB below halt threshold %.2f GB at rotation; halting cleanly",
                free,
                self._cfg.min_free_gb_halt,
            )
            self._shutdown.request("disk")

    def _close_stream(self) -> None:
        if self._stream is not None:
            # Flushes the final zstd frame; without this the tail frame is
            # truncated and the file cannot fully decompress.
            self._stream.close()
            self._stream = None
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _build_subscribe(cfg: Config) -> str:
    return json.dumps(
        {"id": 1, "cmd": "subscribe", "params": {"channels": cfg.channels}}
    )


async def connection_loop(
    cfg: Config,
    signer: Signer,
    queue: asyncio.Queue[object],
    stats: Stats,
) -> None:
    subscribe = _build_subscribe(cfg)
    attempt = 0
    ever_connected = False
    pending_gap: tuple[int, int] | None = None

    while True:
        try:
            headers = signer.ws_headers(WS_PATH)
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                max_size=None,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                await ws.send(subscribe)
                attempt = 0
                ever_connected = True
                if pending_gap is not None:
                    start_ns, start_wall = pending_gap
                    await queue.put(
                        GapMarker(
                            gap_start_ns=start_ns,
                            gap_start_wall=start_wall,
                            gap_end_ns=time.monotonic_ns(),
                            gap_end_wall=time.time_ns(),
                        )
                    )
                    pending_gap = None
                logger.info("connected and subscribed to %s", cfg.channels)

                async for message in ws:
                    ts_ns = time.monotonic_ns()
                    ts_wall = time.time_ns()
                    raw = (
                        message
                        if isinstance(message, str)
                        else message.decode("utf-8")
                    )
                    stats.total += 1
                    try:
                        queue.put_nowait(RawMessage(ts_ns, ts_wall, raw))
                    except asyncio.QueueFull:
                        stats.dropped += 1
                        if stats.dropped % 100 == 1:
                            logger.warning(
                                "queue full, dropped %d messages so far",
                                stats.dropped,
                            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("connection error: %s", exc)

        # Reached only on disconnect or a failed connect attempt.
        if ever_connected and pending_gap is None:
            pending_gap = (time.monotonic_ns(), time.time_ns())
            stats.reconnects += 1
            logger.warning("disconnected; coverage gap started")

        delay = min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2**attempt))
        delay += random.uniform(0, BACKOFF_JITTER_S)
        attempt += 1
        logger.info("reconnecting in %.1fs", delay)
        await asyncio.sleep(delay)


async def stats_loop(
    stats: Stats, queue: asyncio.Queue[object], stop: asyncio.Event, cfg: Config
) -> None:
    last_total = 0
    last_ns = time.monotonic_ns()
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=STATS_INTERVAL_S)
            break
        except TimeoutError:
            pass
        now_ns = time.monotonic_ns()
        interval_s = (now_ns - last_ns) / 1e9
        delta = stats.total - last_total
        rate = delta / interval_s if interval_s > 0 else 0.0
        uptime_s = (now_ns - stats.start_ns) / 1e9
        print(
            f"[stats] uptime={uptime_s:8.1f}s "
            f"msgs={stats.total:>10} "
            f"rate={rate:8.1f}/s "
            f"bytes={_human_bytes(stats.bytes_written):>10} "
            f"rss={rss_bytes() / MIB:7.1f}MB "
            f"free={free_gb(cfg.data_dir):7.2f}GB "
            f"qdepth={queue.qsize():>6} "
            f"dropped={stats.dropped:>6} "
            f"reconnects={stats.reconnects:>4}",
            flush=True,
        )
        last_total = stats.total
        last_ns = now_ns


async def disk_guard_loop(cfg: Config, shutdown: Shutdown) -> None:
    """Poll free space every DISK_CHECK_INTERVAL_S and halt cleanly if it drops below halt."""
    while not shutdown.event.is_set():
        try:
            await asyncio.wait_for(shutdown.event.wait(), timeout=DISK_CHECK_INTERVAL_S)
            return
        except TimeoutError:
            pass
        free = free_gb(cfg.data_dir)
        if free < cfg.min_free_gb_halt:
            logger.error(
                "free space %.2f GB below halt threshold %.2f GB; halting cleanly",
                free,
                cfg.min_free_gb_halt,
            )
            shutdown.request("disk")
            return


async def memory_guard_loop(shutdown: Shutdown) -> None:
    """Halt cleanly if RSS crosses RSS_HALT_MB.

    Defence in depth against a memory regression. The cgroup ``MemoryMax`` in the
    systemd unit is a hard kernel OOM kill (SIGKILL, untrappable), which would
    truncate the in-flight zstd frame and lose the hour's tail -- exactly what the
    clean-shutdown path exists to prevent. So we watch RSS ourselves and trip the
    *same* Shutdown the disk guard and the signal handlers use, giving the writer
    its normal drain-and-flush before systemd restarts us.
    """
    while not shutdown.event.is_set():
        try:
            await asyncio.wait_for(shutdown.event.wait(), timeout=MEMORY_CHECK_INTERVAL_S)
            return
        except TimeoutError:
            pass
        rss_mb = rss_bytes() / MIB
        if rss_mb >= RSS_HALT_MB:
            logger.error(
                "RSS %.1f MB at or above halt threshold %.1f MB; halting cleanly "
                "so the zstd tail frame is flushed before systemd restarts us",
                rss_mb,
                RSS_HALT_MB,
            )
            shutdown.request("memory")
            return


async def log_projected_runtime(cfg: Config, stats: Stats, shutdown: Shutdown) -> None:
    """Estimate bytes/hour from the first PROJECTION_WINDOW_S of real traffic and log runtime."""
    start_bytes = stats.bytes_written
    start_ns = time.monotonic_ns()
    try:
        await asyncio.wait_for(shutdown.event.wait(), timeout=PROJECTION_WINDOW_S)
        return  # shutting down before a useful sample was gathered
    except TimeoutError:
        pass
    elapsed_s = (time.monotonic_ns() - start_ns) / 1e9
    observed = stats.bytes_written - start_bytes
    bytes_per_hour = observed / elapsed_s * 3600.0 if elapsed_s > 0 else 0.0
    free_bytes = free_gb(cfg.data_dir) * GIB
    if bytes_per_hour <= 0:
        logger.info(
            "projected runtime: no bytes written in first %.0fs; cannot estimate", elapsed_s
        )
        return
    hours = free_bytes / bytes_per_hour
    logger.info(
        "projected runtime at current channels: %.1f h "
        "(%.2f GB free / %.1f MB/h observed over %.0fs)",
        hours,
        free_bytes / GIB,
        bytes_per_hour / (1024**2),
        elapsed_s,
    )


def _human_bytes(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{value:.1f}TB"


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    cfg = CONFIG

    # Refuse to start on a volume that is already low; do not start and hope.
    free = free_gb(cfg.data_dir)
    if free < cfg.min_free_gb_start:
        logger.error(
            "insufficient free space to start: %.2f GB free on %s, need >= %.2f GB",
            free,
            _resolve_volume(cfg.data_dir),
            cfg.min_free_gb_start,
        )
        raise SystemExit(1)
    logger.info(
        "disk space OK to start: %.2f GB free on %s (start=%.2f GB, halt=%.2f GB)",
        free,
        _resolve_volume(cfg.data_dir),
        cfg.min_free_gb_start,
        cfg.min_free_gb_halt,
    )

    signer = signer_from_config(cfg.key_id, cfg.pem_path)
    queue: asyncio.Queue[object] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    stats = Stats()
    shutdown = Shutdown(asyncio.Event())

    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop, shutdown)

    writer = Writer(cfg, queue, stats, shutdown)
    writer_task = asyncio.create_task(writer.run(), name="writer")
    stats_task = asyncio.create_task(stats_loop(stats, queue, shutdown.event, cfg), name="stats")
    conn_task = asyncio.create_task(
        connection_loop(cfg, signer, queue, stats), name="connection"
    )
    guard_task = asyncio.create_task(disk_guard_loop(cfg, shutdown), name="disk-guard")
    memory_task = asyncio.create_task(memory_guard_loop(shutdown), name="memory-guard")
    projection_task = asyncio.create_task(
        log_projected_runtime(cfg, stats, shutdown), name="projection"
    )

    await shutdown.event.wait()
    logger.info(
        "shutdown requested (%s); stopping reader and draining queue",
        shutdown.reason or "unknown",
    )

    # 1) Stop producing. Cancelling the connection loop closes the socket.
    conn_task.cancel()
    try:
        await conn_task
    except asyncio.CancelledError:
        pass

    # 2) Drain everything already queued, then close the stream cleanly.
    await queue.put(SENTINEL)
    await writer_task

    # 3) Stop the background reporters/guards.
    for task in (stats_task, guard_task, memory_task, projection_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    logger.info(
        "clean shutdown: total=%d dropped=%d reconnects=%d bytes=%d rss=%.1fMB",
        stats.total,
        stats.dropped,
        stats.reconnects,
        stats.bytes_written,
        rss_bytes() / MIB,
    )

    # Both guards stop cleanly but abnormally: exit non-zero so supervisors notice.
    # Disk halts must NOT restart (run_recorder maps them to 75); memory halts
    # should, because a fresh process reclaims the memory.
    if shutdown.reason == "disk":
        raise SystemExit(1)
    if shutdown.reason == "memory":
        raise SystemExit(EXIT_MEMORY_HALT)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, shutdown: Shutdown) -> None:
    def _handler(*_: object) -> None:
        loop.call_soon_threadsafe(shutdown.request, "signal")

    # SIGINT covers Ctrl-C; SIGBREAK (Windows only) covers Ctrl-Break and console
    # close. Both must lead to the same clean drain-and-flush path.
    sig_names = ["SIGINT", "SIGTERM", "SIGBREAK"]
    signals = [getattr(signal, name) for name in sig_names if hasattr(signal, name)]
    for sig in signals:
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:
            # Windows event loops do not support add_signal_handler; fall back to
            # signal.signal, which delivers the signal to the main thread.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(shutdown.request, "signal"))


if __name__ == "__main__":
    asyncio.run(main())
