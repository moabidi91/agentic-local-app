"""``RetryController`` — bounded, deterministic exponential backoff (§3.14, §7.3, ADR-017).

``delay_ms(attempt)`` is the delay to wait **after** the failure of attempt ``attempt`` (1-based)
and before attempt ``attempt + 1``::

    delay = min(base_delay_ms * 2 ** (attempt - 1), max_delay_ms)

With the defaults (500 ms, cap 8 000 ms, 4 attempts) the schedule is ``[500, 1000, 2000]``.

No randomness lives here by default: the optional jitter (``±jitter_ratio`` around the capped
delay) is applied only when a ``random_source`` returning a float in ``[0, 1]`` is injected, so
that tests and production runs without jitter are byte-for-byte reproducible (module map §2.4).
"""

from __future__ import annotations

from collections.abc import Callable

from agentic_local_app.config import RetrySection

__all__ = ["RetryController"]


class RetryController:
    def __init__(
        self, config: RetrySection, random_source: Callable[[], float] | None = None
    ) -> None:
        self._config = config
        self._random_source = random_source

    @property
    def max_attempts(self) -> int:
        return self._config.max_attempts

    def can_retry(self, attempt: int) -> bool:
        """``True`` when attempt ``attempt`` (1-based, just failed) is not the last allowed one."""
        return attempt < self._config.max_attempts

    def delay_ms(self, attempt: int) -> int:
        """Backoff delay after the failure of attempt ``attempt`` (``ValueError`` if ``attempt < 1``)."""
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt}")
        # 1 << (attempt - 1) == 2 ** (attempt - 1), typed as int (int ** int is Any for mypy)
        base = min(self._config.base_delay_ms * (1 << (attempt - 1)), self._config.max_delay_ms)
        ratio = self._config.jitter_ratio
        if self._random_source is None or ratio <= 0:
            return base
        draw = min(1.0, max(0.0, float(self._random_source())))
        factor = 1.0 + ratio * (2.0 * draw - 1.0)  # in [1 - ratio, 1 + ratio]
        return max(0, round(base * factor))

    def schedule(self) -> list[int]:
        """The delays between consecutive attempts: one per retry, ``max_attempts - 1`` entries."""
        return [self.delay_ms(attempt) for attempt in range(1, self._config.max_attempts)]
