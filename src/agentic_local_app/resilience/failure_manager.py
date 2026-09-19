"""``FailureManager`` — classify failures and apply the deterministic policy of §7 (§3.13).

Classification (:meth:`FailureManager.classify`): an :class:`AppError` already carries its
``NormalizedError``; native exceptions are mapped — ``httpx.TimeoutException`` / ``TimeoutError``
-> TIMEOUT_ERROR, ``httpx.TransportError`` / ``OSError`` -> NETWORK_ERROR (both retryable), anything
else -> SYSTEM_ERROR (not retryable, not recoverable, ``details.type`` / ``details.message``).

Decision (:meth:`FailureManager.decide`) — **the policy is keyed on ``error_type``, never on the
``retryable`` flag set by the producer** (§7.1 lists the retryable types exhaustively):

| error_type                                        | decision |
|---------------------------------------------------|----------|
| MODEL_CONTEXT_WINDOW_ERROR                        | rotate (ADR-013) |
| AUTHN_ERROR                                       | pause (ADR-025, ``credentials_required``) |
| NETWORK / TIMEOUT / RATE_LIMIT, SYSTEM transient  | retry while ``can_retry(attempt)`` and the breaker allows, else fail (``max_attempts_exhausted`` / ``circuit_open``) |
| INTERRUPTED                                       | abort |
| everything else                                   | fail (``non_retryable:<type>``) |

``pause`` is for the one failure a **user** can repair without losing anything: a 401 means the
token is missing or expired, and only the token. ``AUTHZ_ERROR`` (403) is deliberately **not**
paused — the credentials were accepted and the operation was refused, so another token of the same
identity changes nothing and the session fails as before (ADR-025).

The retry delay is ``max(backoff, details.retry_after_ms)``: a server ``Retry-After`` hint is
honoured even above ``max_delay_ms``. ``can_retry`` is checked before ``breaker.allow()`` so that a
HALF_OPEN trial slot is only consumed when a retry will actually happen.

Persistence (ADR-015, §7.3 "retry decisions persisted in state"): ``record`` writes a
``FailureRecord`` then publishes ``failure.recorded``; ``record_decision`` writes a
``RetryDecisionRecord`` for **every** decision and publishes ``retry.scheduled`` for retries only.
``handle`` chains ``classify -> record -> feed the breaker -> decide -> record_decision``. Only
transport-class failures (NETWORK, TIMEOUT, RATE_LIMIT, transient SYSTEM) feed the breaker (§7.4
"repeated transport failures"); ``note_success()`` is the orchestrator's signal after a successful
remote call. A ``PersistenceError`` raised by the store propagates to the caller unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, get_args

import httpx

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import (
    RETRYABLE_ERROR_TYPES,
    AppError,
    ErrorType,
    NormalizedError,
    Severity,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import FailureRecord, RetryDecisionRecord
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.resilience.circuit_breaker import CircuitBreaker
from agentic_local_app.resilience.retry_controller import RetryController

__all__ = [
    "BREAKER_FED_ERROR_TYPES",
    "CREDENTIALS_REQUIRED_REASON",
    "PAUSING_ERROR_TYPES",
    "Decision",
    "DecisionKind",
    "FailureManager",
]

DecisionKind = Literal["retry", "abort", "rotate", "pause", "fail"]
_DECISION_KINDS: frozenset[str] = frozenset(get_args(DecisionKind))

#: ``reason`` of a ``pause`` decision (ADR-025): the user has a new token to provide.
CREDENTIALS_REQUIRED_REASON = "credentials_required"

#: The error types that pause the session instead of ending it (ADR-025). AUTHZ_ERROR is **not**
#: one of them: a 403 is a permission problem that a new token of the same identity does not fix.
PAUSING_ERROR_TYPES: frozenset[ErrorType] = frozenset({ErrorType.AUTHN_ERROR})

#: Transport-class failures that count towards opening the breaker (§7.4). SYSTEM_ERROR counts
#: only when ``details["transient"] is True``.
BREAKER_FED_ERROR_TYPES: frozenset[ErrorType] = RETRYABLE_ERROR_TYPES | {ErrorType.SYSTEM_ERROR}

_ORIGIN = "FailureManager"


@dataclass(frozen=True)
class Decision:
    """What to do about a failure (§3.13): ``retry`` (with ``delay_ms``), ``abort``, ``rotate``,
    ``pause`` (ADR-025) or ``fail``."""

    kind: DecisionKind
    delay_ms: int | None
    reason: str

    def __post_init__(self) -> None:
        if self.kind not in _DECISION_KINDS:
            raise ValueError(f"unknown decision kind {self.kind!r}")
        if self.kind == "retry" and (self.delay_ms is None or self.delay_ms < 0):
            raise ValueError("a retry decision requires a non-negative delay_ms")
        if self.kind != "retry" and self.delay_ms is not None:
            raise ValueError(f"delay_ms is only meaningful for a retry, not for {self.kind!r}")


def _is_transient(error: NormalizedError) -> bool:
    return error.details.get("transient") is True


def _is_policy_retryable(error: NormalizedError) -> bool:
    if error.error_type in RETRYABLE_ERROR_TYPES:
        return True
    return error.error_type is ErrorType.SYSTEM_ERROR and _is_transient(error)


class FailureManager:
    def __init__(
        self,
        config: AppConfig,
        store: ConversationStore,
        bus: EventBus,
        clock: Clock,
        ids: IdGenerator,
        retry: RetryController | None = None,
        breaker: CircuitBreaker | None = None,
    ) -> None:
        self._config = config
        self._store = store
        self._bus = bus
        self._clock = clock
        self._ids = ids
        self.retry = retry if retry is not None else RetryController(config.retry)
        self.breaker = (
            breaker if breaker is not None else CircuitBreaker(config.circuit_breaker, clock, bus)
        )

    # ------------------------------------------------------------------ classify -----------
    def classify(self, exc: BaseException) -> NormalizedError:
        """The normalized error for any exception (§6): AppError -> carried error; natives mapped."""
        if isinstance(exc, AppError):
            return exc.error
        name = type(exc).__name__
        message = str(exc)
        # asyncio.TimeoutError is an alias of the builtin TimeoutError since Python 3.11;
        # TimeoutError is checked before OSError because it is one of its subclasses.
        if isinstance(exc, httpx.TimeoutException | TimeoutError):
            return NormalizedError(
                error_type=ErrorType.TIMEOUT_ERROR,
                error_code="OPERATION_TIMEOUT",
                origin=_ORIGIN,
                retryable=True,
                recoverable=True,
                details={"exception": name, "message": message},
            )
        if isinstance(exc, httpx.TransportError | OSError):
            return NormalizedError(
                error_type=ErrorType.NETWORK_ERROR,
                error_code="NETWORK_FAILURE",
                origin=_ORIGIN,
                retryable=True,
                recoverable=True,
                details={"exception": name, "message": message},
            )
        return NormalizedError(
            error_type=ErrorType.SYSTEM_ERROR,
            error_code="UNHANDLED_EXCEPTION",
            severity=Severity.CRITICAL,
            origin=_ORIGIN,
            retryable=False,
            recoverable=False,
            details={"type": name, "message": message, "exception": name},
        )

    # ------------------------------------------------------------------ decide -------------
    def decide(self, error: NormalizedError, attempt: int, *, operation: str) -> Decision:
        """Apply the §7 policy table (see module docstring). ``operation`` is kept for the reason
        strings of future refinements and for symmetry with :meth:`record_decision`."""
        if error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR:
            return Decision(kind="rotate", delay_ms=None, reason="context_window_exceeded")
        if error.error_type in PAUSING_ERROR_TYPES:
            return Decision(kind="pause", delay_ms=None, reason=CREDENTIALS_REQUIRED_REASON)
        if _is_policy_retryable(error):
            if not self.retry.can_retry(attempt):
                return Decision(kind="fail", delay_ms=None, reason="max_attempts_exhausted")
            if not self.breaker.allow():
                return Decision(kind="fail", delay_ms=None, reason="circuit_open")
            delay = self.retry.delay_ms(attempt)
            retry_after = error.details.get("retry_after_ms")
            if isinstance(retry_after, int) and not isinstance(retry_after, bool):
                delay = max(delay, retry_after)
            return Decision(kind="retry", delay_ms=delay, reason="retryable_error")
        if error.error_type is ErrorType.INTERRUPTED:
            return Decision(kind="abort", delay_ms=None, reason="interrupted")
        return Decision(
            kind="fail", delay_ms=None, reason=f"non_retryable:{error.error_type.value}"
        )

    # ------------------------------------------------------------------ persist ------------
    def record(
        self,
        error: NormalizedError,
        *,
        session_id: str,
        conversation_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
    ) -> FailureRecord:
        """Persist a ``FailureRecord`` (§16) then publish ``failure.recorded`` (ADR-015)."""
        now = self._clock.now()
        record = FailureRecord(
            failure_id=self._ids.failure_id(),
            session_id=session_id,
            conversation_id=conversation_id,
            plan_id=plan_id,
            task_id=task_id,
            error_type=error.error_type,
            error_code=error.error_code,
            severity=error.severity,
            origin=error.origin,
            retryable=error.retryable,
            recoverable=error.recoverable,
            attempt=error.attempt,
            max_attempts=error.max_attempts,
            details=dict(error.details),
            timestamp=now,
        )
        self._store.save_failure(record)
        self._bus.publish(
            Event(
                event_type=EventType.FAILURE_RECORDED,
                timestamp=now,
                session_id=session_id,
                conversation_id=conversation_id,
                plan_id=plan_id,
                task_id=task_id,
                payload={
                    "failure_id": record.failure_id,
                    "error_type": error.error_type.value,
                    "error_code": error.error_code,
                    "severity": error.severity.value,
                    "origin": error.origin,
                    "retryable": error.retryable,
                    "recoverable": error.recoverable,
                    "attempt": error.attempt,
                    "max_attempts": error.max_attempts,
                    "details": dict(error.details),
                },
            )
        )
        return record

    def record_decision(
        self,
        decision: Decision,
        error: NormalizedError,
        *,
        session_id: str,
        conversation_id: str | None,
        cycle_id: str | None,
        operation: str,
        attempt: int,
    ) -> RetryDecisionRecord:
        """Persist the decision (§7.3) then publish ``retry.scheduled`` when it is a retry."""
        now = self._clock.now()
        record = RetryDecisionRecord(
            decision_id=self._ids.decision_id(),
            session_id=session_id,
            conversation_id=conversation_id,
            cycle_id=cycle_id,
            operation=operation,
            error_type=error.error_type,
            error_code=error.error_code,
            attempt=attempt,
            max_attempts=self.retry.max_attempts,
            decision=decision.kind,
            delay_ms=decision.delay_ms,
            created_at=now,
        )
        self._store.save_retry_decision(record)
        if decision.kind == "retry":
            self._bus.publish(
                Event(
                    event_type=EventType.RETRY_SCHEDULED,
                    timestamp=now,
                    session_id=session_id,
                    conversation_id=conversation_id,
                    cycle_id=cycle_id,
                    payload={
                        "decision_id": record.decision_id,
                        "operation": operation,
                        "attempt": attempt,
                        "max_attempts": record.max_attempts,
                        "delay_ms": decision.delay_ms,
                        "error_type": error.error_type.value,
                        "error_code": error.error_code,
                        "reason": decision.reason,
                    },
                )
            )
        return record

    # ------------------------------------------------------------------ orchestration ------
    def handle(
        self,
        exc: BaseException,
        attempt: int,
        *,
        operation: str,
        session_id: str,
        conversation_id: str | None = None,
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
    ) -> tuple[NormalizedError, Decision]:
        """classify -> record -> feed the breaker -> decide -> record the decision."""
        error = self.classify(exc).with_attempt(attempt, self.retry.max_attempts)
        self.record(
            error,
            session_id=session_id,
            conversation_id=conversation_id,
            plan_id=plan_id,
            task_id=task_id,
        )
        if self._feeds_breaker(error):
            self.breaker.session_id = session_id
            self.breaker.record_failure()
        decision = self.decide(error, attempt, operation=operation)
        self.record_decision(
            decision,
            error,
            session_id=session_id,
            conversation_id=conversation_id,
            cycle_id=cycle_id,
            operation=operation,
            attempt=attempt,
        )
        return error, decision

    def note_success(self) -> None:
        """To be called by the orchestrator after a successful remote call (closes a HALF_OPEN breaker)."""
        self.breaker.record_success()

    @staticmethod
    def _feeds_breaker(error: NormalizedError) -> bool:
        """§7.4 "repeated transport failures": NETWORK / TIMEOUT / RATE_LIMIT, or a transient SYSTEM_ERROR."""
        if error.error_type not in BREAKER_FED_ERROR_TYPES:
            return False
        return error.error_type is not ErrorType.SYSTEM_ERROR or _is_transient(error)
