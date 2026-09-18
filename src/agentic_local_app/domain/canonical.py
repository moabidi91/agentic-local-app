"""Canonical JSON serialisation and hashing (ADR-017).

Used for: outbound protocol messages, audit payloads and the audit hash chain, context summaries,
and every size measurement (payload limits of ADR-010, context bytes of ADR-013).
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any

GENESIS_HASH = "0" * 64


def _default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    if isinstance(obj, (set, frozenset, tuple)):
        return sorted(obj) if isinstance(obj, (set, frozenset)) else list(obj)
    raise TypeError(f"not JSON serialisable: {type(obj).__name__}")


def canonical_json(obj: Any) -> str:
    """Deterministic JSON: sorted keys, compact separators, UTF-8 preserved."""
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=_default
    )


def canonical_bytes(obj: Any) -> bytes:
    return canonical_json(obj).encode("utf-8")


def size_bytes(obj: Any) -> int:
    """Size in bytes of the canonical serialisation."""
    return len(canonical_bytes(obj))


def sha256_hex(data: bytes | str) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def chain_hash(previous_event_hash: str, event_without_hash: dict[str, Any]) -> str:
    """``sha256(previous_event_hash + canonical(event_without_hash))`` - ADR-017."""
    return sha256_hex(previous_event_hash + canonical_json(event_without_hash))
