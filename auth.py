"""Kalshi request signing (RSA-PSS, SHA256).

The signature covers ``timestamp + method + path`` where ``path`` has any query
string stripped. Timestamps are milliseconds since the epoch.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def load_private_key(pem_path: Path) -> rsa.RSAPrivateKey:
    key = serialization.load_pem_private_key(pem_path.read_bytes(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError(
            f"Expected an RSA private key at {pem_path}, got {type(key).__name__}."
        )
    return key


def _strip_query(path: str) -> str:
    return urlsplit(path).path


def _timestamp_ms() -> str:
    return str(int(time.time() * 1000))


class Signer:
    """Signs requests with a loaded RSA private key."""

    def __init__(self, key_id: str, private_key: rsa.RSAPrivateKey) -> None:
        self._key_id = key_id
        self._key = private_key

    def _sign(self, message: str) -> str:
        signature = self._key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("ascii")

    def headers(self, method: str, path: str) -> dict[str, str]:
        timestamp = _timestamp_ms()
        clean_path = _strip_query(path)
        signature = self._sign(timestamp + method.upper() + clean_path)
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    def ws_headers(self, path: str) -> dict[str, str]:
        return self.headers("GET", path)


def signer_from_config(key_id: str, pem_path: Path) -> Signer:
    return Signer(key_id, load_private_key(pem_path))
