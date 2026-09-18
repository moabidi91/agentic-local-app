"""Injectable identifier generation (ADR-017).

``plan_id`` and ``task_id`` come from the model and are never generated here.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Protocol


class IdGenerator(Protocol):
    def session_id(self) -> str: ...
    def conversation_id(self) -> str: ...
    def message_id(self) -> str: ...
    def cycle_id(self) -> str: ...
    def event_id(self) -> str: ...
    def blob_id(self) -> str: ...
    def failure_id(self) -> str: ...
    def summary_id(self) -> str: ...
    def decision_id(self) -> str: ...


class UuidIdGenerator:
    """Production generator: prefixed random UUID4 (hex, no dashes)."""

    @staticmethod
    def _new(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"

    def session_id(self) -> str:
        return self._new("sess")

    def conversation_id(self) -> str:
        return self._new("conv")

    def message_id(self) -> str:
        return self._new("msg")

    def cycle_id(self) -> str:
        return self._new("cyc")

    def event_id(self) -> str:
        return self._new("evt")

    def blob_id(self) -> str:
        return self._new("blob")

    def failure_id(self) -> str:
        return self._new("fail")

    def summary_id(self) -> str:
        return self._new("sum")

    def decision_id(self) -> str:
        return self._new("dec")


class SequentialIdGenerator:
    """Test generator: ``conv-0001``, ``msg-0001``, ... byte-for-byte reproducible."""

    def __init__(self) -> None:
        self._counters: defaultdict[str, int] = defaultdict(int)

    def _next(self, prefix: str) -> str:
        self._counters[prefix] += 1
        return f"{prefix}-{self._counters[prefix]:04d}"

    def session_id(self) -> str:
        return self._next("sess")

    def conversation_id(self) -> str:
        return self._next("conv")

    def message_id(self) -> str:
        return self._next("msg")

    def cycle_id(self) -> str:
        return self._next("cyc")

    def event_id(self) -> str:
        return self._next("evt")

    def blob_id(self) -> str:
        return self._next("blob")

    def failure_id(self) -> str:
        return self._next("fail")

    def summary_id(self) -> str:
        return self._next("sum")

    def decision_id(self) -> str:
        return self._next("dec")
