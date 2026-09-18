"""Injectable clock (ADR-017). No component may call ``datetime.now()`` or ``time.*`` directly."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Current wall-clock time, timezone-aware UTC (persisted timestamps)."""
        ...

    def monotonic_ms(self) -> int:
        """Monotonic milliseconds (durations, timeouts, budgets, backoff)."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic_ms(self) -> int:
        return int(time.monotonic() * 1000)


class FakeClock:
    """Deterministic clock for tests: time only moves when the test says so."""

    def __init__(self, start: datetime | None = None, monotonic_ms: int = 0) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=UTC)
        self._monotonic_ms = monotonic_ms

    def now(self) -> datetime:
        return self._now

    def monotonic_ms(self) -> int:
        return self._monotonic_ms

    def advance(self, ms: int) -> None:
        """Advance both wall-clock and monotonic time by ``ms`` milliseconds."""
        if ms < 0:
            raise ValueError("cannot move the clock backwards")
        self._monotonic_ms += ms
        self._now = self._now + timedelta(milliseconds=ms)
