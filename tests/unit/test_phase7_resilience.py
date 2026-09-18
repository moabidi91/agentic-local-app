"""Phase 7 — resilience: RetryController, CircuitBreaker, FailureManager (§7, §18.2 phase 7).

Everything is deterministic: ``FakeClock`` drives the breaker's open duration, ``SequentialIdGenerator``
names the records, the optional jitter of the retry controller uses an injected random source.
"""

from __future__ import annotations

import inspect
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from agentic_local_app.config import AppConfig, CircuitBreakerSection, RetrySection
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import (
    RETRYABLE_ERROR_TYPES,
    AppError,
    BudgetExceededError,
    ErrorType,
    GenericSystemError,
    NormalizedError,
    PersistenceError,
    ProtocolError,
    RotationFailedError,
    SessionInterruptedError,
    Severity,
    TaskExecutionError,
    TransportError,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.states import CircuitState
from agentic_local_app.domain.transitions import CIRCUIT_TRANSITIONS, can_transition
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.resilience.circuit_breaker import CircuitBreaker
from agentic_local_app.resilience.failure_manager import Decision, FailureManager
from agentic_local_app.resilience.retry_controller import RetryController

pytestmark = pytest.mark.phase7

SESSION = "sess-0001"
CONVERSATION = "conv-0001"
CYCLE = "cyc-0001"

NON_RETRYABLE_TYPES = [
    ErrorType.AUTHN_ERROR,
    ErrorType.AUTHZ_ERROR,
    ErrorType.MODEL_PROTOCOL_ERROR,
    ErrorType.BUDGET_EXCEEDED,
    ErrorType.ROTATION_FAILED,
    ErrorType.PERSISTENCE_ERROR,
    ErrorType.TASK_EXECUTION_ERROR,
]


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def _error(
    error_type: ErrorType,
    code: str = "CODE",
    *,
    retryable: bool = False,
    recoverable: bool = True,
    **details: Any,
) -> NormalizedError:
    return NormalizedError(
        error_type=error_type,
        error_code=code,
        origin="Test",
        retryable=retryable,
        recoverable=recoverable,
        details=details,
    )


def _retry(**overrides: Any) -> RetryController:
    return RetryController(RetrySection(**overrides))


def _breaker(
    clock: FakeClock,
    bus: EventBus | None = None,
    session_id: str | None = SESSION,
    **overrides: Any,
) -> CircuitBreaker:
    return CircuitBreaker(CircuitBreakerSection(**overrides), clock, bus, session_id=session_id)


def _manager(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    *,
    retry: RetryController | None = None,
    breaker: CircuitBreaker | None = None,
    config: AppConfig | None = None,
) -> FailureManager:
    cfg = config or AppConfig()
    retry = retry or RetryController(cfg.retry)
    breaker = breaker or CircuitBreaker(cfg.circuit_breaker, clock, bus, session_id=SESSION)
    return FailureManager(cfg, store, bus, clock, ids, retry, breaker)


# ================================================================================================
# RetryController
# ================================================================================================
def given_default_config_when_schedule_then_500_1000_2000() -> None:
    assert _retry().schedule() == [500, 1_000, 2_000]


@pytest.mark.parametrize(("attempt", "expected"), [(1, 500), (2, 1_000), (3, 2_000), (4, 4_000)])
def given_default_config_when_delay_computed_then_base_doubles_per_attempt(
    attempt: int, expected: int
) -> None:
    assert _retry().delay_ms(attempt) == expected


@pytest.mark.parametrize("attempt", [5, 6, 10, 40])
def given_exponential_beyond_cap_when_delay_computed_then_capped_at_max_delay(attempt: int) -> None:
    assert _retry().delay_ms(attempt) == 8_000


def given_custom_base_and_cap_when_schedule_then_sequence_follows_config() -> None:
    controller = _retry(max_attempts=6, base_delay_ms=100, max_delay_ms=1_000)
    assert controller.schedule() == [100, 200, 400, 800, 1_000]


@pytest.mark.parametrize("attempt", [0, -1, -10])
def given_attempt_below_one_when_delay_computed_then_value_error(attempt: int) -> None:
    with pytest.raises(ValueError):
        _retry().delay_ms(attempt)


@pytest.mark.parametrize(
    ("attempt", "expected"), [(1, True), (2, True), (3, True), (4, False), (5, False)]
)
def given_max_attempts_4_when_can_retry_evaluated_then_true_only_below_max(
    attempt: int, expected: bool
) -> None:
    assert _retry().can_retry(attempt) is expected


def given_max_attempts_1_when_schedule_then_empty_and_first_failure_final() -> None:
    controller = _retry(max_attempts=1)
    assert controller.schedule() == [] and controller.can_retry(1) is False


def given_zero_base_delay_when_delay_computed_then_zero() -> None:
    assert _retry(base_delay_ms=0).delay_ms(3) == 0


def given_jitter_ratio_without_random_source_when_delay_computed_then_no_jitter() -> None:
    controller = RetryController(RetrySection(jitter_ratio=0.5))
    assert controller.schedule() == [500, 1_000, 2_000]


@pytest.mark.parametrize(("draw", "expected"), [(1.0, 750), (0.0, 250), (0.5, 500), (0.75, 625)])
def given_random_source_when_jitter_ratio_set_then_delay_scaled_within_plus_minus_ratio(
    draw: float, expected: int
) -> None:
    controller = RetryController(RetrySection(jitter_ratio=0.5), random_source=lambda: draw)
    assert controller.delay_ms(1) == expected


@pytest.mark.parametrize(("draw", "expected"), [(2.0, 750), (-1.0, 250)])
def given_random_draw_out_of_unit_interval_when_used_then_clamped(
    draw: float, expected: int
) -> None:
    controller = RetryController(RetrySection(jitter_ratio=0.5), random_source=lambda: draw)
    assert controller.delay_ms(1) == expected


def given_random_source_when_jitter_ratio_zero_then_source_never_called() -> None:
    calls: list[int] = []

    def source() -> float:
        calls.append(1)
        return 0.9

    controller = RetryController(RetrySection(jitter_ratio=0.0), random_source=source)
    assert controller.schedule() == [500, 1_000, 2_000] and calls == []


def given_jittered_delay_when_computed_then_never_negative_and_capped_before_jitter() -> None:
    controller = RetryController(
        RetrySection(base_delay_ms=8_000, max_delay_ms=8_000, jitter_ratio=1.0),
        random_source=lambda: 0.0,
    )
    assert controller.delay_ms(1) == 0
    high = RetryController(
        RetrySection(base_delay_ms=8_000, max_delay_ms=8_000, jitter_ratio=1.0),
        random_source=lambda: 1.0,
    )
    assert high.delay_ms(5) == 16_000  # cap applies to the base, jitter is ±ratio around it


def given_same_inputs_when_delay_computed_twice_then_identical() -> None:
    controller = _retry()
    assert [controller.delay_ms(a) for a in range(1, 8)] == [
        controller.delay_ms(a) for a in range(1, 8)
    ]


def given_retry_controller_when_max_attempts_read_then_config_value_exposed() -> None:
    assert _retry(max_attempts=7).max_attempts == 7


# ================================================================================================
# CircuitBreaker
# ================================================================================================
def given_new_breaker_when_created_then_closed_allows_calls_and_not_degraded(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock)
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow() is True and breaker.allow() is True
    assert breaker.degraded is False
    assert breaker.consecutive_failures == 0


def given_closed_breaker_when_failures_below_threshold_then_stays_closed(clock: FakeClock) -> None:
    breaker = _breaker(clock, failure_threshold=3)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED and breaker.allow() is True
    assert breaker.consecutive_failures == 2


def given_closed_breaker_when_failures_reach_threshold_then_open_blocks_and_degraded(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=3)
    for _ in range(3):
        breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow() is False
    assert breaker.degraded is True
    assert breaker.consecutive_failures == 3


def given_closed_breaker_when_success_between_failures_then_counter_reset(clock: FakeClock) -> None:
    breaker = _breaker(clock, failure_threshold=2)
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED and breaker.consecutive_failures == 1


def given_open_breaker_when_open_duration_not_elapsed_then_allow_false(clock: FakeClock) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=30_000)
    breaker.record_failure()
    clock.advance(29_999)
    assert breaker.allow() is False and breaker.state is CircuitState.OPEN


def given_open_breaker_when_open_duration_elapsed_then_half_open_and_allows_one_trial_call(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=30_000, half_open_max_calls=1)
    breaker.record_failure()
    clock.advance(30_000)
    assert breaker.allow() is True
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.degraded is True
    assert breaker.allow() is False  # the single trial slot is taken


def given_half_open_breaker_with_two_max_calls_when_allow_called_then_two_allowed_then_blocked(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=10, half_open_max_calls=2)
    breaker.record_failure()
    clock.advance(10)
    assert [breaker.allow(), breaker.allow(), breaker.allow()] == [True, True, False]


def given_half_open_breaker_when_success_then_closed_and_failures_reset(clock: FakeClock) -> None:
    breaker = _breaker(clock, failure_threshold=2, open_duration_ms=10)
    breaker.record_failure()
    breaker.record_failure()
    clock.advance(10)
    assert breaker.allow() is True
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.consecutive_failures == 0
    assert breaker.degraded is False
    assert breaker.allow() is True and breaker.allow() is True


def given_half_open_breaker_when_failure_then_open_again_for_a_full_open_duration(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=100)
    breaker.record_failure()
    clock.advance(100)
    assert breaker.allow() is True and breaker.state is CircuitState.HALF_OPEN
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    clock.advance(99)
    assert breaker.allow() is False
    clock.advance(1)
    assert breaker.allow() is True and breaker.state is CircuitState.HALF_OPEN


def given_open_breaker_when_failure_recorded_then_stays_open_and_counts(clock: FakeClock) -> None:
    breaker = _breaker(clock, failure_threshold=1)
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN and breaker.consecutive_failures == 2


def given_open_breaker_when_late_success_recorded_then_stays_open_until_duration_elapses(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=50)
    breaker.record_failure()
    breaker.record_success()
    assert breaker.state is CircuitState.OPEN and breaker.allow() is False
    clock.advance(50)
    assert breaker.allow() is True and breaker.state is CircuitState.HALF_OPEN


def given_bus_and_session_when_state_changes_then_breaker_events_with_from_to_and_failures(
    clock: FakeClock, bus: EventBus, recorder: RecordingSubscriber
) -> None:
    breaker = _breaker(clock, bus, failure_threshold=2, open_duration_ms=10)
    breaker.record_failure()
    clock.advance(5)
    breaker.record_failure()
    opened_at = clock.now()
    clock.advance(10)
    breaker.allow()
    breaker.record_success()

    events = recorder.of_type(EventType.BREAKER_STATE_CHANGED)
    assert [(e.payload["from"], e.payload["to"]) for e in events] == [
        ("CLOSED", "OPEN"),
        ("OPEN", "HALF_OPEN"),
        ("HALF_OPEN", "CLOSED"),
    ]
    assert [e.payload["consecutive_failures"] for e in events] == [2, 2, 0]
    assert all(e.session_id == SESSION for e in events)
    assert events[0].timestamp == opened_at
    assert recorder.events == events  # nothing else was published


def given_half_open_breaker_when_failure_then_half_open_to_open_event_published(
    clock: FakeClock, bus: EventBus, recorder: RecordingSubscriber
) -> None:
    breaker = _breaker(clock, bus, failure_threshold=1, open_duration_ms=10)
    breaker.record_failure()
    clock.advance(10)
    breaker.allow()
    breaker.record_failure()
    transitions = [
        (e.payload["from"], e.payload["to"])
        for e in recorder.of_type(EventType.BREAKER_STATE_CHANGED)
    ]
    assert transitions == [("CLOSED", "OPEN"), ("OPEN", "HALF_OPEN"), ("HALF_OPEN", "OPEN")]


def given_breaker_without_bus_when_state_changes_then_no_error(clock: FakeClock) -> None:
    breaker = _breaker(clock, None, failure_threshold=1, open_duration_ms=1)
    breaker.record_failure()
    clock.advance(1)
    breaker.allow()
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


def given_bus_without_session_when_state_changes_then_nothing_published(
    clock: FakeClock, bus: EventBus, recorder: RecordingSubscriber
) -> None:
    breaker = _breaker(clock, bus, session_id=None, failure_threshold=1)
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN and recorder.events == []


def given_session_id_assigned_later_when_state_changes_then_event_carries_that_session(
    clock: FakeClock, bus: EventBus, recorder: RecordingSubscriber
) -> None:
    breaker = _breaker(clock, bus, session_id=None, failure_threshold=1)
    breaker.session_id = "sess-0042"
    breaker.record_failure()
    assert [e.session_id for e in recorder.events] == ["sess-0042"]


def given_breaker_when_walked_through_full_cycle_then_every_step_is_a_listed_circuit_transition(
    clock: FakeClock,
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=1)
    seen: list[CircuitState] = [breaker.state]

    def step(action: Callable[[], object]) -> None:
        action()
        if breaker.state is not seen[-1]:
            seen.append(breaker.state)

    step(breaker.record_failure)  # CLOSED -> OPEN
    clock.advance(1)
    step(breaker.allow)  # OPEN -> HALF_OPEN
    step(breaker.record_failure)  # HALF_OPEN -> OPEN
    clock.advance(1)
    step(breaker.allow)  # OPEN -> HALF_OPEN
    step(breaker.record_success)  # HALF_OPEN -> CLOSED
    assert seen == [
        CircuitState.CLOSED,
        CircuitState.OPEN,
        CircuitState.HALF_OPEN,
        CircuitState.OPEN,
        CircuitState.HALF_OPEN,
        CircuitState.CLOSED,
    ]
    for current, target in zip(seen, seen[1:], strict=False):
        assert can_transition(CIRCUIT_TRANSITIONS, current, target)


# ================================================================================================
# FailureManager.classify
# ================================================================================================
@pytest.mark.parametrize("error_type", list(ErrorType))
def given_app_error_of_each_type_when_classified_then_its_normalized_error_returned(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    error_type: ErrorType,
) -> None:
    error = _error(error_type, "SOME_CODE", retryable=error_type in RETRYABLE_ERROR_TYPES, k="v")
    classified = _manager(store, bus, clock, ids).classify(AppError(error))
    assert classified == error and classified.error_type is error_type


@pytest.mark.parametrize(
    "exc",
    [
        TransportError(ErrorType.RATE_LIMIT_ERROR, "HTTP_429", retryable=True),
        ProtocolError("UNEXPECTED_MESSAGE_TYPE", got="x"),
        PersistenceError("DISK_FULL"),
        BudgetExceededError("max_cycles", 20, 21),
        RotationFailedError("SUMMARY_TOO_LARGE"),
        SessionInterruptedError(),
        TaskExecutionError("SPAWN_FAILED"),
        GenericSystemError("BOOM", "Test", transient=True),
    ],
)
def given_concrete_app_error_subclass_when_classified_then_carried_error_returned_unchanged(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: AppError,
) -> None:
    assert _manager(store, bus, clock, ids).classify(exc) is exc.error


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ReadTimeout("slow"),
        httpx.ConnectTimeout("slow"),
        httpx.PoolTimeout("slow"),
        TimeoutError(),  # asyncio.TimeoutError is this very class since Python 3.11
        TimeoutError("native"),
    ],
)
def given_timeout_exception_when_classified_then_timeout_error_retryable(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: BaseException,
) -> None:
    error = _manager(store, bus, clock, ids).classify(exc)
    assert error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.retryable is True
    assert error.origin == "FailureManager"
    assert error.details["exception"] == type(exc).__name__


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("refused"),
        httpx.ReadError("reset"),
        httpx.RemoteProtocolError("disconnected"),
        httpx.ProxyError("proxy"),
        ConnectionResetError("reset"),
        OSError("network down"),
    ],
)
def given_network_exception_when_classified_then_network_error_retryable(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: BaseException,
) -> None:
    error = _manager(store, bus, clock, ids).classify(exc)
    assert error.error_type is ErrorType.NETWORK_ERROR
    assert error.retryable is True
    assert error.details["exception"] == type(exc).__name__


@pytest.mark.parametrize(
    "exc", [RuntimeError("boom"), ValueError("bad"), KeyError("missing"), ZeroDivisionError()]
)
def given_unknown_exception_when_classified_then_system_error_not_retryable_with_type_and_message(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: BaseException,
) -> None:
    error = _manager(store, bus, clock, ids).classify(exc)
    assert error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error_code == "UNHANDLED_EXCEPTION"
    assert error.retryable is False and error.recoverable is False
    assert error.details["type"] == type(exc).__name__
    assert error.details["message"] == str(exc)
    assert error.details.get("transient", False) is False


# ================================================================================================
# FailureManager.decide — the §7 policy table
# ================================================================================================
@pytest.mark.parametrize("attempt", [1, 4, 9])
def given_context_window_error_when_decided_then_rotate_whatever_the_attempt(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    attempt: int,
) -> None:
    decision = _manager(store, bus, clock, ids).decide(
        _error(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413"), attempt, operation="GET"
    )
    assert decision.kind == "rotate" and decision.delay_ms is None
    assert decision.reason == "context_window_exceeded"


@pytest.mark.parametrize("error_type", sorted(RETRYABLE_ERROR_TYPES, key=str))
@pytest.mark.parametrize(("attempt", "delay"), [(1, 500), (2, 1_000), (3, 2_000)])
def given_retryable_type_when_attempts_remain_then_retry_with_backoff_delay(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    error_type: ErrorType,
    attempt: int,
    delay: int,
) -> None:
    decision = _manager(store, bus, clock, ids).decide(
        _error(error_type, retryable=True), attempt, operation="POST"
    )
    assert decision == Decision(kind="retry", delay_ms=delay, reason="retryable_error")


@pytest.mark.parametrize("error_type", sorted(RETRYABLE_ERROR_TYPES, key=str))
@pytest.mark.parametrize("attempt", [4, 5])
def given_retryable_type_when_attempts_exhausted_then_fail_max_attempts_exhausted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    error_type: ErrorType,
    attempt: int,
) -> None:
    decision = _manager(store, bus, clock, ids).decide(
        _error(error_type, retryable=True), attempt, operation="POST"
    )
    assert decision == Decision(kind="fail", delay_ms=None, reason="max_attempts_exhausted")


@pytest.mark.parametrize(("retry_after_ms", "expected"), [(5_000, 5_000), (100, 500), (500, 500)])
def given_rate_limit_with_retry_after_when_decided_then_delay_is_max_of_backoff_and_retry_after(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    retry_after_ms: int,
    expected: int,
) -> None:
    error = _error(
        ErrorType.RATE_LIMIT_ERROR, "HTTP_429", retryable=True, retry_after_ms=retry_after_ms
    )
    decision = _manager(store, bus, clock, ids).decide(error, 1, operation="POST")
    assert decision.kind == "retry" and decision.delay_ms == expected


def given_retry_after_beyond_max_delay_when_decided_then_server_hint_honoured(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    error = _error(ErrorType.RATE_LIMIT_ERROR, retryable=True, retry_after_ms=60_000)
    decision = _manager(store, bus, clock, ids).decide(error, 1, operation="GET")
    assert decision.delay_ms == 60_000


def given_transient_system_error_when_decided_then_retry(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    error = _error(ErrorType.SYSTEM_ERROR, "HTTP_500", retryable=True, transient=True)
    decision = _manager(store, bus, clock, ids).decide(error, 2, operation="GET")
    assert decision == Decision(kind="retry", delay_ms=1_000, reason="retryable_error")


@pytest.mark.parametrize("details", [{}, {"transient": False}, {"transient": "yes"}])
def given_non_transient_system_error_when_decided_then_fail(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    details: dict[str, Any],
) -> None:
    error = _error(ErrorType.SYSTEM_ERROR, "INVALID_TRANSITION", **details)
    decision = _manager(store, bus, clock, ids).decide(error, 1, operation="POST")
    assert decision.kind == "fail" and decision.delay_ms is None
    assert decision.reason == "non_retryable:SYSTEM_ERROR"


def given_open_breaker_when_retryable_error_decided_then_fail_circuit_open(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    breaker = _breaker(clock, failure_threshold=1)
    breaker.record_failure()
    manager = _manager(store, bus, clock, ids, breaker=breaker)
    decision = manager.decide(_error(ErrorType.NETWORK_ERROR, retryable=True), 1, operation="POST")
    assert decision == Decision(kind="fail", delay_ms=None, reason="circuit_open")


def given_half_open_breaker_when_retryable_error_decided_then_trial_slot_consumed_by_retry(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=10, half_open_max_calls=1)
    breaker.record_failure()
    clock.advance(10)
    manager = _manager(store, bus, clock, ids, breaker=breaker)
    first = manager.decide(_error(ErrorType.TIMEOUT_ERROR, retryable=True), 1, operation="GET")
    second = manager.decide(_error(ErrorType.TIMEOUT_ERROR, retryable=True), 1, operation="GET")
    assert first.kind == "retry" and breaker.state is CircuitState.HALF_OPEN
    assert second == Decision(kind="fail", delay_ms=None, reason="circuit_open")


def given_exhausted_attempts_when_decided_then_breaker_not_consulted(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    breaker = _breaker(clock, failure_threshold=1, open_duration_ms=10, half_open_max_calls=1)
    breaker.record_failure()
    clock.advance(10)
    manager = _manager(store, bus, clock, ids, breaker=breaker)
    decision = manager.decide(_error(ErrorType.NETWORK_ERROR, retryable=True), 4, operation="POST")
    assert decision.reason == "max_attempts_exhausted"
    assert breaker.state is CircuitState.OPEN  # allow() was never called: still OPEN, slot intact


def given_interrupted_when_decided_then_abort(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    decision = _manager(store, bus, clock, ids).decide(
        _error(ErrorType.INTERRUPTED, "USER_INTERRUPT"), 1, operation="GET"
    )
    assert decision == Decision(kind="abort", delay_ms=None, reason="interrupted")


@pytest.mark.parametrize("error_type", NON_RETRYABLE_TYPES)
@pytest.mark.parametrize("attempt", [1, 2])
def given_each_non_retryable_type_when_decided_then_fail_without_delay(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    error_type: ErrorType,
    attempt: int,
) -> None:
    decision = _manager(store, bus, clock, ids).decide(
        _error(error_type), attempt, operation="POST"
    )
    assert decision == Decision(
        kind="fail", delay_ms=None, reason=f"non_retryable:{error_type.value}"
    )


@pytest.mark.parametrize("error_type", NON_RETRYABLE_TYPES)
def given_non_listed_type_flagged_retryable_when_decided_then_policy_wins_and_fails(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    error_type: ErrorType,
) -> None:
    error = _error(error_type, retryable=True, transient=True)
    decision = _manager(store, bus, clock, ids).decide(error, 1, operation="POST")
    assert decision.kind == "fail"


def given_transient_persistence_error_when_decided_then_fail(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    exc = PersistenceError("SQLITE_BUSY", transient=True)
    assert exc.error.retryable is True
    decision = _manager(store, bus, clock, ids).decide(exc.error, 1, operation="POST")
    assert decision.kind == "fail"


def given_listed_type_flagged_non_retryable_when_decided_then_policy_wins_and_retries(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    error = _error(ErrorType.NETWORK_ERROR, retryable=False)
    decision = _manager(store, bus, clock, ids).decide(error, 1, operation="POST")
    assert decision.kind == "retry"


def given_decision_when_built_then_frozen_and_kind_validated() -> None:
    decision = Decision(kind="retry", delay_ms=500, reason="r")
    with pytest.raises((AttributeError, TypeError)):
        decision.kind = "fail"  # type: ignore[misc]
    with pytest.raises(ValueError):
        Decision(kind="maybe", delay_ms=None, reason="r")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Decision(kind="fail", delay_ms=10, reason="r")  # a delay only makes sense for a retry


# ================================================================================================
# FailureManager.record / record_decision
# ================================================================================================
def given_error_when_recorded_then_failure_record_persisted_with_all_fields_then_event_published(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    clock.advance(1_234)
    error = NormalizedError(
        error_type=ErrorType.TIMEOUT_ERROR,
        error_code="MODEL_GET_TIMEOUT",
        severity=Severity.HIGH,
        origin="TransportGateway",
        retryable=True,
        recoverable=True,
        attempt=2,
        max_attempts=4,
        details={"operation": "GET", "timeout_ms": 15_000},
    )
    record = _manager(store, bus, clock, ids).record(
        error, session_id=SESSION, conversation_id=CONVERSATION, plan_id="plan-0", task_id="t1"
    )

    assert record.failure_id == "fail-0001"
    assert (record.session_id, record.conversation_id) == (SESSION, CONVERSATION)
    assert (record.plan_id, record.task_id) == ("plan-0", "t1")
    assert record.error_type is ErrorType.TIMEOUT_ERROR
    assert record.error_code == "MODEL_GET_TIMEOUT"
    assert record.severity is Severity.HIGH and record.origin == "TransportGateway"
    assert (record.retryable, record.recoverable) == (True, True)
    assert (record.attempt, record.max_attempts) == (2, 4)
    assert record.details == {"operation": "GET", "timeout_ms": 15_000}
    assert record.timestamp == clock.now()
    assert store.list_failures(SESSION) == [record]

    events = recorder.of_type(EventType.FAILURE_RECORDED)
    assert len(events) == 1 and recorder.events == events
    event = events[0]
    assert (event.session_id, event.conversation_id) == (SESSION, CONVERSATION)
    assert (event.plan_id, event.task_id) == ("plan-0", "t1")
    assert event.timestamp == clock.now()
    assert event.payload["failure_id"] == "fail-0001"
    assert event.payload["error_type"] == "TIMEOUT_ERROR"
    assert event.payload["error_code"] == "MODEL_GET_TIMEOUT"
    assert event.payload["retryable"] is True
    assert event.payload["attempt"] == 2 and event.payload["max_attempts"] == 4
    assert event.payload["details"] == {"operation": "GET", "timeout_ms": 15_000}


def given_subscriber_reading_store_when_failure_recorded_event_delivered_then_record_already_there(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    seen: list[int] = []
    bus.subscribe(lambda e: seen.append(len(store.list_failures(SESSION))), name="probe")
    _manager(store, bus, clock, ids).record(_error(ErrorType.NETWORK_ERROR), session_id=SESSION)
    assert seen == [1]


def given_store_failing_when_recorded_then_persistence_error_and_no_event(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        _manager(store, bus, clock, ids).record(_error(ErrorType.NETWORK_ERROR), session_id=SESSION)
    assert store.list_failures(SESSION) == [] and recorder.events == []


def given_retry_decision_when_recorded_then_record_persisted_and_retry_scheduled_published(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    decision = Decision(kind="retry", delay_ms=1_000, reason="retryable_error")
    error = _error(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True)
    record = _manager(store, bus, clock, ids).record_decision(
        decision,
        error,
        session_id=SESSION,
        conversation_id=CONVERSATION,
        cycle_id=CYCLE,
        operation="POST",
        attempt=2,
    )

    assert record.decision_id == "dec-0001"
    assert (record.session_id, record.conversation_id, record.cycle_id) == (
        SESSION,
        CONVERSATION,
        CYCLE,
    )
    assert record.operation == "POST"
    assert (record.error_type, record.error_code) == (ErrorType.NETWORK_ERROR, "HTTP_503")
    assert (record.attempt, record.max_attempts) == (2, 4)
    assert (record.decision, record.delay_ms) == ("retry", 1_000)
    assert record.created_at == clock.now()
    assert store.list_retry_decisions(SESSION) == [record]

    events = recorder.of_type(EventType.RETRY_SCHEDULED)
    assert len(events) == 1 and recorder.events == events
    assert (events[0].session_id, events[0].conversation_id, events[0].cycle_id) == (
        SESSION,
        CONVERSATION,
        CYCLE,
    )
    assert events[0].payload == {
        "decision_id": "dec-0001",
        "operation": "POST",
        "attempt": 2,
        "max_attempts": 4,
        "delay_ms": 1_000,
        "error_type": "NETWORK_ERROR",
        "error_code": "HTTP_503",
        "reason": "retryable_error",
    }


@pytest.mark.parametrize(
    "decision",
    [
        Decision(kind="fail", delay_ms=None, reason="max_attempts_exhausted"),
        Decision(kind="abort", delay_ms=None, reason="interrupted"),
        Decision(kind="rotate", delay_ms=None, reason="context_window_exceeded"),
    ],
)
def given_non_retry_decision_when_recorded_then_persisted_and_no_retry_event(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
    decision: Decision,
) -> None:
    record = _manager(store, bus, clock, ids).record_decision(
        decision,
        _error(ErrorType.NETWORK_ERROR),
        session_id=SESSION,
        conversation_id=None,
        cycle_id=None,
        operation="GET",
        attempt=4,
    )
    assert record.decision == decision.kind and record.delay_ms is None
    assert store.list_retry_decisions(SESSION) == [record]
    assert recorder.events == []


def given_store_failing_when_decision_recorded_then_persistence_error_and_no_event(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        _manager(store, bus, clock, ids).record_decision(
            Decision(kind="retry", delay_ms=500, reason="r"),
            _error(ErrorType.NETWORK_ERROR),
            session_id=SESSION,
            conversation_id=None,
            cycle_id=None,
            operation="GET",
            attempt=1,
        )
    assert store.list_retry_decisions(SESSION) == [] and recorder.events == []


# ================================================================================================
# FailureManager.handle — the whole chain, plus the breaker feed
# ================================================================================================
def given_network_exception_when_handled_then_records_events_and_retry_decision_in_order(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    manager = _manager(store, bus, clock, ids)
    error, decision = manager.handle(
        httpx.ConnectError("refused"),
        1,
        operation="POST",
        session_id=SESSION,
        conversation_id=CONVERSATION,
        cycle_id=CYCLE,
    )

    assert error.error_type is ErrorType.NETWORK_ERROR
    assert (error.attempt, error.max_attempts) == (1, 4)
    assert decision == Decision(kind="retry", delay_ms=500, reason="retryable_error")
    failures = store.list_failures(SESSION)
    decisions = store.list_retry_decisions(SESSION)
    assert [f.failure_id for f in failures] == ["fail-0001"]
    assert (failures[0].attempt, failures[0].max_attempts) == (1, 4)
    assert [d.decision_id for d in decisions] == ["dec-0001"]
    assert (decisions[0].decision, decisions[0].delay_ms, decisions[0].cycle_id) == (
        "retry",
        500,
        CYCLE,
    )
    assert [e.event_type for e in recorder.events] == [
        EventType.FAILURE_RECORDED,
        EventType.RETRY_SCHEDULED,
    ]
    assert manager.breaker.consecutive_failures == 1


def given_transport_error_when_handled_then_carried_error_kept_with_attempt_counters(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    exc = TransportError(
        ErrorType.RATE_LIMIT_ERROR,
        "HTTP_429",
        retryable=True,
        retry_after_ms=3_000,
        operation="POST",
    )
    error, decision = _manager(store, bus, clock, ids).handle(
        exc, 3, operation="POST", session_id=SESSION
    )
    assert error.error_code == "HTTP_429" and (error.attempt, error.max_attempts) == (3, 4)
    assert decision == Decision(kind="retry", delay_ms=3_000, reason="retryable_error")


def given_repeated_transport_failures_when_handled_then_breaker_opens_and_decision_fails_circuit_open(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    manager = _manager(store, bus, clock, ids)  # default threshold: 5
    decisions = [
        manager.handle(httpx.ReadTimeout("slow"), 1, operation="GET", session_id=SESSION)[1]
        for _ in range(5)
    ]
    assert [d.kind for d in decisions] == ["retry"] * 4 + ["fail"]
    assert decisions[-1].reason == "circuit_open"
    assert manager.breaker.state is CircuitState.OPEN
    breaker_events = recorder.of_type(EventType.BREAKER_STATE_CHANGED)
    assert [(e.payload["from"], e.payload["to"]) for e in breaker_events] == [("CLOSED", "OPEN")]
    assert breaker_events[0].payload["consecutive_failures"] == 5
    # the breaker event is published between the failure record and the decision of the 5th call
    kinds = [e.event_type for e in recorder.events]
    assert kinds[-2:] == [EventType.FAILURE_RECORDED, EventType.BREAKER_STATE_CHANGED]
    assert store.list_retry_decisions(SESSION)[-1].decision == "fail"


@pytest.mark.parametrize(
    "exc",
    [
        TransportError(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True),
        TransportError(ErrorType.TIMEOUT_ERROR, "HTTP_504", retryable=True),
        TransportError(ErrorType.RATE_LIMIT_ERROR, "HTTP_429", retryable=True),
        TransportError(ErrorType.SYSTEM_ERROR, "HTTP_500", retryable=True, transient=True),
    ],
)
def given_transport_failure_of_each_fed_type_when_handled_then_breaker_failure_recorded(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: AppError,
) -> None:
    manager = _manager(store, bus, clock, ids)
    manager.handle(exc, 1, operation="POST", session_id=SESSION)
    assert manager.breaker.consecutive_failures == 1


@pytest.mark.parametrize(
    "exc",
    [
        PersistenceError("DISK_FULL"),
        ProtocolError("UNEXPECTED_MESSAGE_TYPE"),
        BudgetExceededError("max_plans", 10, 11),
        SessionInterruptedError(),
        TransportError(ErrorType.AUTHN_ERROR, "HTTP_401", retryable=False),
        TransportError(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413", retryable=False),
        TransportError(ErrorType.SYSTEM_ERROR, "HTTP_404", retryable=False),
        RuntimeError("boom"),
    ],
)
def given_non_transport_or_non_transient_failure_when_handled_then_breaker_not_fed(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    exc: BaseException,
) -> None:
    manager = _manager(store, bus, clock, ids)
    for _ in range(6):
        manager.handle(exc, 1, operation="POST", session_id=SESSION)
    assert manager.breaker.consecutive_failures == 0
    assert manager.breaker.state is CircuitState.CLOSED


def given_context_window_error_when_handled_then_rotate_decision_persisted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    exc = TransportError(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413", retryable=False)
    error, decision = _manager(store, bus, clock, ids).handle(
        exc, 1, operation="GET", session_id=SESSION, conversation_id=CONVERSATION
    )
    assert decision.kind == "rotate"
    assert store.list_retry_decisions(SESSION)[0].decision == "rotate"
    assert [e.event_type for e in recorder.events] == [EventType.FAILURE_RECORDED]


def given_interrupted_when_handled_then_abort_and_no_retry_event(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    _, decision = _manager(store, bus, clock, ids).handle(
        SessionInterruptedError(), 1, operation="POST", session_id=SESSION
    )
    assert decision.kind == "abort"
    assert recorder.of_type(EventType.RETRY_SCHEDULED) == []


def given_half_open_breaker_when_note_success_then_closed(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    breaker = _breaker(clock, bus, failure_threshold=1, open_duration_ms=10)
    manager = _manager(store, bus, clock, ids, breaker=breaker)
    manager.handle(httpx.ConnectError("x"), 1, operation="POST", session_id=SESSION)
    assert breaker.state is CircuitState.OPEN
    clock.advance(10)
    assert breaker.allow() is True
    manager.note_success()
    assert breaker.state is CircuitState.CLOSED and breaker.consecutive_failures == 0


def given_manager_built_without_retry_and_breaker_when_created_then_defaults_from_config(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    config = AppConfig(
        retry=RetrySection(max_attempts=3, base_delay_ms=10, max_delay_ms=15),
        circuit_breaker=CircuitBreakerSection(failure_threshold=2),
    )
    manager = FailureManager(config, store, bus, clock, ids)
    assert manager.retry.schedule() == [10, 15]
    assert manager.breaker.state is CircuitState.CLOSED
    manager.handle(httpx.ConnectError("x"), 1, operation="POST", session_id=SESSION)
    manager.handle(httpx.ConnectError("x"), 1, operation="POST", session_id=SESSION)
    assert manager.breaker.state is CircuitState.OPEN
    # the default breaker publishes for the session it was fed with
    assert manager.breaker.session_id == SESSION


def given_handle_called_for_two_sessions_when_breaker_shared_then_events_carry_current_session(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    config = AppConfig(circuit_breaker=CircuitBreakerSection(failure_threshold=2))
    manager = FailureManager(config, store, bus, clock, ids)
    manager.handle(httpx.ConnectError("x"), 1, operation="POST", session_id="sess-A")
    manager.handle(httpx.ConnectError("x"), 1, operation="POST", session_id="sess-B")
    events = recorder.of_type(EventType.BREAKER_STATE_CHANGED)
    assert [e.session_id for e in events] == ["sess-B"]


def given_persistence_failure_during_handle_when_recording_then_error_propagates_to_caller(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    recorder: RecordingSubscriber,
) -> None:
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        _manager(store, bus, clock, ids).handle(
            httpx.ConnectError("x"), 1, operation="POST", session_id=SESSION
        )
    assert recorder.events == [] and store.list_retry_decisions(SESSION) == []


# ================================================================================================
# determinism: no wall clock / randomness in the production modules (ADR-017, module map §2.4)
# ================================================================================================
_FORBIDDEN = re.compile(
    r"datetime\.now|datetime\.utcnow|\btime\.(time|monotonic|sleep|perf_counter)\b|"
    r"\buuid4?\b|\brandom\.|import random|from random"
)


@pytest.mark.parametrize(
    "module_path",
    [
        "resilience/retry_controller.py",
        "resilience/circuit_breaker.py",
        "resilience/failure_manager.py",
        "transport/gateway.py",
        "transport/fake.py",
    ],
)
def given_production_module_when_inspected_then_no_wall_clock_or_randomness_used(
    module_path: str,
) -> None:
    import agentic_local_app

    root = Path(inspect.getfile(agentic_local_app)).parent
    source = (root / module_path).read_text(encoding="utf-8")
    code_lines = [
        line for line in source.splitlines() if not line.strip().startswith(("#", '"""', "'''"))
    ]
    assert not [line for line in code_lines if _FORBIDDEN.search(line)], module_path


def given_event_model_when_breaker_payload_built_then_json_serialisable(
    clock: FakeClock, bus: EventBus, recorder: RecordingSubscriber
) -> None:
    breaker = _breaker(clock, bus, failure_threshold=1)
    breaker.record_failure()
    event: Event = recorder.events[0]
    assert event.model_dump_json()  # frozen model, JSON-serialisable payload
