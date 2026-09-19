"""Configuration loaded from the environment (.env).

Required variables cause a loud failure at import time if missing or invalid,
so the recorder never starts in a half-configured state.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


class ConfigError(RuntimeError):
    """Raised when configuration is missing or invalid."""


def _require(name: str) -> str:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ConfigError(
            f"Required environment variable {name!r} is missing or empty. "
            f"Set it in .env before starting the recorder."
        )
    return value.strip()


def _csv(name: str, default: str) -> list[str]:
    raw = os.environ.get(name, default)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw.strip())
    except ValueError:
        raise ConfigError(f"{name} must be a number of GB, got {raw!r}.") from None
    if value < 0:
        raise ConfigError(f"{name} must be >= 0, got {value}.")
    return value


@dataclass(frozen=True)
class Config:
    key_id: str
    pem_path: Path
    data_dir: Path
    channels: list[str]
    ticker_prefixes: list[str] = field(default_factory=list)
    # Disk-space guard thresholds, in GiB of free space on the data volume.
    min_free_gb_start: float = 5.0
    min_free_gb_halt: float = 2.0

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"


def load_config() -> Config:
    key_id = _require("KALSHI_KEY_ID")

    pem_path = Path(_require("KALSHI_PEM_PATH"))
    if not pem_path.is_file():
        raise ConfigError(
            f"KALSHI_PEM_PATH points to {pem_path}, which is not a readable file."
        )

    data_dir = Path(os.environ.get("KALSHI_DATA_DIR", "data")).resolve()

    channels = _csv("KALSHI_CHANNELS", "ticker,trade")
    if not channels:
        raise ConfigError("KALSHI_CHANNELS resolved to an empty channel list.")

    # Empty list means "record everything" (no prefix filtering).
    ticker_prefixes = _csv("KALSHI_TICKER_PREFIXES", "")

    min_free_gb_start = _float("MIN_FREE_GB_START", 5.0)
    min_free_gb_halt = _float("MIN_FREE_GB_HALT", 2.0)

    return Config(
        key_id=key_id,
        pem_path=pem_path,
        data_dir=data_dir,
        channels=channels,
        ticker_prefixes=ticker_prefixes,
        min_free_gb_start=min_free_gb_start,
        min_free_gb_halt=min_free_gb_halt,
    )


CONFIG = load_config()
