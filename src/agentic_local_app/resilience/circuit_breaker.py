"""``CircuitBreaker`` — CLOSED / OPEN / HALF_OPEN protection of the remote endpoint (§3.15, §7.4).

- ``CLOSED``: every call is allowed; ``failure_threshold`` **consecutive** failures open the breaker.
- ``OPEN``: calls are refused until ``open_duration_ms`` has elapsed on the injected clock; the next
  ``allow()`` after that moves to ``HALF_OPEN``.
- ``HALF_OPEN``: up to ``half_open_max_calls`` trial calls are allowed; a success closes the breaker
  (failure counter reset), a failure reopens it for a full ``open_duration_ms``.

Every state change goes through ``assert_transition(CIRCUIT_TRANSITIONS, ...)`` (the table is the
only source of truth) and is published as ``breaker.state_changed`` (§7.4 "emit audit event") when
a bus **and** a session id are known — an ``Event`` needs a session. ``degraded`` is the §7.4 "mark
conversation degraded" signal: ``True`` whenever the breaker is not ``CLOSED``.

A late ``record_success()`` while ``OPEN`` (a call that started before the breaker opened) does not
close it: only the open duration and a successful trial call do. Late failures while ``OPEN`` are
counted but keep the original ``opened_at``.
"""

from __future__ import annotations

from agentic_local_app.config import CircuitBreakerSection
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.states import CircuitState
from agentic_local_app.domain.transitions import CIRCUIT_TRANSITIONS, assert_transition
from agentic_local_app.observability.event_bus import EventBus

__all__ = ["CIRCUIT_ENTITY", "CircuitBreaker"]

#: ``entity`` carried by :class:`~agentic_local_app.domain.errors.InvalidTransitionError`.
CIRCUIT_ENTITY = "circuit_breaker"


class CircuitBreaker:
    def __init__(
        self,
        config: CircuitBreakerSection,
        clock: Clock,
        bus: EventBus | None = None,
        *,
        session_id: str | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._bus = bus
        self.session_id = session_id
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at_ms: int | None = None
        self._half_open_calls = 0

    # ------------------------------------------------------------------ read ---------------
    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def degraded(self) -> bool:
        """§7.4 "mark conversation degraded": the breaker is not CLOSED."""
        return self._state is not CircuitState.CLOSED

    # ------------------------------------------------------------------ decisions ----------
    def allow(self) -> bool:
        """May a remote call be attempted now?"""
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            opened_at = self._opened_at_ms if self._opened_at_ms is not None else 0
            if self._clock.monotonic_ms() - opened_at < self._config.open_duration_ms:
                return False
            self._half_open_calls = 0
            self._transition(CircuitState.HALF_OPEN)
        # HALF_OPEN: a bounded number of trial calls
        if self._half_open_calls < self._config.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False

    def record_success(self) -> None:
        if self._state is CircuitState.HALF_OPEN:
            self._consecutive_failures = 0
            self._half_open_calls = 0
            self._transition(CircuitState.CLOSED)
        elif self._state is CircuitState.CLOSED:
            self._consecutive_failures = 0
        # OPEN: a late success does not close the breaker (see module docstring)

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._state is CircuitState.CLOSED:
            if self._consecutive_failures >= self._config.failure_threshold:
                self._open()
        elif self._state is CircuitState.HALF_OPEN:
            self._open()
        # OPEN: counted, opened_at unchanged

    # ------------------------------------------------------------------ internals ----------
    def _open(self) -> None:
        self._opened_at_ms = self._clock.monotonic_ms()
        self._half_open_calls = 0
        self._transition(CircuitState.OPEN)

    def _transition(self, target: CircuitState) -> None:
        current = self._state
        assert_transition(CIRCUIT_TRANSITIONS, current, target, entity=CIRCUIT_ENTITY)
        self._state = target
        if self._bus is None or self.session_id is None:
            return
        payload = state_change_payload(current.value, target.value)
        payload["consecutive_failures"] = self._consecutive_failures
        self._bus.publish(
            Event(
                event_type=EventType.BREAKER_STATE_CHANGED,
                timestamp=self._clock.now(),
                session_id=self.session_id,
                payload=payload,
            )
        )
