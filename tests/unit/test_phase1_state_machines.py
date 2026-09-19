"""Phase 1 — state machines (spec §5, §9, §17.1, §18.2 ; ADR-006, ADR-007, ADR-012, ADR-013, ADR-015, ADR-017).

Two layers are pinned here:

1. the transition tables of ``domain/transitions.py`` — walked **exhaustively**: every pair of the
   cartesian product of each state enumeration is a parametrised test, accepted when listed and
   rejected with ``InvalidTransitionError`` otherwise;
2. ``ConversationLifecycleManager`` — the sole owner of session and conversation transitions:
   validate → persist → publish, with the store double (``fail_next_write``) proving that a
   persistence failure leaves the state untouched and publishes nothing (ADR-015).

Only the doubles of ``tests/conftest.py`` are used (no shell, network or real database, §18.3).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
from datetime import timedelta
from enum import Enum
from itertools import product
from pathlib import Path
from typing import Any, TypeVar

import pytest

from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType, InvalidTransitionError, PersistenceError
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import ConversationRecord, SessionBudget, SessionRecord
from agentic_local_app.domain.states import (
    CircuitState,
    ContextWindowState,
    ConversationState,
    CycleState,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.domain.transitions import (
    ACTIVE_CONVERSATION_STATES,
    CIRCUIT_TRANSITIONS,
    CONTEXT_WINDOW_TRANSITIONS,
    CONVERSATION_TRANSITIONS,
    CYCLE_TRANSITIONS,
    FAILED_TASK_STATES,
    PLAN_TRANSITIONS,
    SESSION_TRANSITIONS,
    TASK_TRANSITIONS,
    TERMINAL_CONVERSATION_STATES,
    TERMINAL_PLAN_STATES,
    TERMINAL_TASK_STATES,
    assert_transition,
    can_transition,
    is_terminal,
)
from agentic_local_app.lifecycle import conversation_lifecycle
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.persistence.memory import InMemoryConversationStore

pytestmark = pytest.mark.phase1

S = TypeVar("S", bound=Enum)

# =============================================================================================
# Helpers
# =============================================================================================

#: name -> (table, enumeration) for every state machine of the specification.
TABLES: dict[str, tuple[Mapping[Any, frozenset[Any]], type[Enum]]] = {
    "conversation": (CONVERSATION_TRANSITIONS, ConversationState),
    "session": (SESSION_TRANSITIONS, SessionState),
    "plan": (PLAN_TRANSITIONS, PlanState),
    "task": (TASK_TRANSITIONS, TaskState),
    "cycle": (CYCLE_TRANSITIONS, CycleState),
    "context_window": (CONTEXT_WINDOW_TRANSITIONS, ContextWindowState),
    "circuit": (CIRCUIT_TRANSITIONS, CircuitState),
}

BUDGET = SessionBudget(max_cycles=10, max_plans=5, max_total_duration_ms=60_000)

CONVERSATION_ORDER = list(ConversationState)
NON_ACTIVE_CONVERSATION_STATES = [
    s for s in ConversationState if s not in ACTIVE_CONVERSATION_STATES
]


def _state_id(state: Enum) -> str:
    return str(state.value)


def _all_pairs(listed: bool) -> list[Any]:
    """Every (table, from, to) of the cartesian product, filtered on table membership."""
    params = []
    for name, (table, enum) in TABLES.items():
        for current, target in product(enum, enum):
            if (target in table.get(current, frozenset())) is listed:
                params.append(
                    pytest.param(
                        name, table, current, target, id=f"{name}:{current.value}->{target.value}"
                    )
                )
    return params


def _pairs_of(table: Mapping[S, frozenset[S]], enum: type[S], *, listed: bool) -> list[Any]:
    return [
        pytest.param(current, target, id=f"{current.value}->{target.value}")
        for current, target in product(enum, enum)
        if (target in table.get(current, frozenset())) is listed
    ]


def _path(table: Mapping[S, frozenset[S]], start: S, target: S) -> list[S]:
    """Shortest path ``start -> ... -> target`` through ``table`` (start excluded).

    Neighbours are visited in enumeration order so the path is the same on every run
    (frozenset iteration order depends on the string hash seed).
    """
    order = {state: index for index, state in enumerate(type(start))}
    previous: dict[S, S] = {}
    queue = deque([start])
    seen = {start}
    while queue:
        node = queue.popleft()
        if node == target:
            break
        for nxt in sorted(table.get(node, frozenset()), key=order.__getitem__):
            if nxt not in seen:
                seen.add(nxt)
                previous[nxt] = node
                queue.append(nxt)
    if target != start and target not in previous:
        raise AssertionError(f"{target!r} unreachable from {start!r}")
    path: list[S] = []
    node = target
    while node != start:
        path.append(node)
        node = previous[node]
    path.reverse()
    return path


def _new_session(
    lifecycle: ConversationLifecycleManager, *, auto_close: bool = False
) -> SessionRecord:
    return lifecycle.create_session(
        goal="diagnose disk usage",
        user_message="why is /var full?",
        user_id="local-user",
        budget=BUDGET,
        auto_close=auto_close,
    )


def _session_in(
    lifecycle: ConversationLifecycleManager, state: SessionState, *, auto_close: bool = False
) -> SessionRecord:
    """A session driven from READY to ``state`` through the manager."""
    session = _new_session(lifecycle, auto_close=auto_close)
    for step in _path(SESSION_TRANSITIONS, SessionState.READY, state):
        session = lifecycle.transition_session(session.session_id, step)
    return session


def _conversation_in(
    lifecycle: ConversationLifecycleManager,
    state: ConversationState,
    *,
    session: SessionRecord | None = None,
) -> ConversationRecord:
    """A conversation driven from NEW to ``state`` through the manager."""
    session = session or _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    for step in _path(CONVERSATION_TRANSITIONS, ConversationState.NEW, state):
        conversation = lifecycle.transition_conversation(conversation.conversation_id, step)
    return conversation


def _only_event(recorder: RecordingSubscriber) -> Event:
    assert len(recorder.events) == 1, [e.event_type for e in recorder.events]
    return recorder.events[0]


def _require_session(store: InMemoryConversationStore, session_id: str) -> SessionRecord:
    record = store.get_session(session_id)
    assert record is not None
    return record


def _require_conversation(
    store: InMemoryConversationStore, conversation_id: str
) -> ConversationRecord:
    record = store.get_conversation(conversation_id)
    assert record is not None
    return record


class _StoreFailingOnSessionWrite(InMemoryConversationStore):
    """Fails the *second* write of ``create_conversation`` (the session update) exactly once."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_next_session_write = False

    def save_session(self, record: SessionRecord) -> None:
        if self.fail_next_session_write:
            self.fail_next_session_write = False
            raise PersistenceError("SIMULATED_SESSION_WRITE_FAILURE")
        super().save_session(record)


@pytest.fixture
def lifecycle(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> ConversationLifecycleManager:
    return ConversationLifecycleManager(store=store, bus=bus, clock=clock, ids=ids)


# =============================================================================================
# A. Transition tables (domain/transitions.py) — exhaustive
# =============================================================================================


@pytest.mark.parametrize("name", list(TABLES), ids=list(TABLES))
def given_transition_table_when_compared_to_its_enum_then_every_state_has_an_entry(
    name: str,
) -> None:
    table, enum = TABLES[name]
    assert set(table) == set(enum)
    for state, targets in table.items():
        assert all(isinstance(t, enum) for t in targets), (name, state)
        assert state not in targets, f"{name}: self-transition {state} is not allowed"


@pytest.mark.parametrize(("name", "table", "current", "target"), _all_pairs(listed=True))
def given_listed_pair_when_asserted_then_accepted_and_can_transition_true(
    name: str, table: Mapping[Any, frozenset[Any]], current: Enum, target: Enum
) -> None:
    assert_transition(table, current, target, entity=name)
    assert can_transition(table, current, target) is True


@pytest.mark.parametrize(("name", "table", "current", "target"), _all_pairs(listed=False))
def given_unlisted_pair_when_asserted_then_invalid_transition_error_and_can_transition_false(
    name: str, table: Mapping[Any, frozenset[Any]], current: Enum, target: Enum
) -> None:
    assert can_transition(table, current, target) is False
    with pytest.raises(InvalidTransitionError) as exc:
        assert_transition(table, current, target, entity=name)
    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        name,
        current.value,
        target.value,
    )


def given_unlisted_pair_when_rejected_then_error_carries_normalized_system_error() -> None:
    with pytest.raises(InvalidTransitionError) as exc:
        assert_transition(
            CONVERSATION_TRANSITIONS,
            ConversationState.INTERRUPTED,
            ConversationState.ACTIVE,
            entity="conversation",
        )
    error = exc.value.error
    assert error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error_code == "INVALID_TRANSITION"
    assert error.recoverable is False and error.retryable is False
    assert error.details == {"entity": "conversation", "current": "INTERRUPTED", "target": "ACTIVE"}
    assert str(exc.value) == "invalid conversation transition INTERRUPTED -> ACTIVE"


@pytest.mark.parametrize("name", list(TABLES), ids=list(TABLES))
def given_each_table_when_is_terminal_evaluated_then_true_only_for_states_without_exit(
    name: str,
) -> None:
    table, enum = TABLES[name]
    for state in enum:
        assert is_terminal(table, state) is (len(table[state]) == 0), (name, state)


def given_conversation_table_when_active_states_read_then_match_spec_any_active_state() -> None:
    assert ACTIVE_CONVERSATION_STATES == {
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
        ConversationState.ROTATING,
    }


def given_conversation_table_when_terminal_states_read_then_interrupted_failed_closed() -> None:
    assert TERMINAL_CONVERSATION_STATES == {
        ConversationState.INTERRUPTED,
        ConversationState.FAILED,
        ConversationState.CLOSED,
    }
    assert TERMINAL_CONVERSATION_STATES == {
        s for s in ConversationState if is_terminal(CONVERSATION_TRANSITIONS, s)
    }


def given_conversation_table_when_interrupted_target_checked_then_reachable_exactly_from_active_states() -> (
    None
):
    reaching_interrupted = {
        s
        for s in ConversationState
        if can_transition(CONVERSATION_TRANSITIONS, s, ConversationState.INTERRUPTED)
    }
    assert reaching_interrupted == ACTIVE_CONVERSATION_STATES


def given_conversation_table_when_failed_target_checked_then_reachable_from_every_non_terminal_state() -> (
    None
):
    for state in ConversationState:
        expected = state not in TERMINAL_CONVERSATION_STATES
        assert (
            can_transition(CONVERSATION_TRANSITIONS, state, ConversationState.FAILED) is expected
        ), state


def given_conversation_table_when_adr007_amendments_checked_then_present() -> None:
    # added: rotation from a failing GET, closure of the rotated parent
    assert can_transition(
        CONVERSATION_TRANSITIONS,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.ROTATING,
    )
    assert can_transition(
        CONVERSATION_TRANSITIONS, ConversationState.ROTATING, ConversationState.CLOSED
    )
    # reinterpreted: the *child* enters WAITING_MODEL_RESPONSE, never the rotated parent
    assert not can_transition(
        CONVERSATION_TRANSITIONS,
        ConversationState.ROTATING,
        ConversationState.WAITING_MODEL_RESPONSE,
    )
    # moved to the session machine (ADR-006): INTERRUPTED is terminal for a conversation
    assert CONVERSATION_TRANSITIONS[ConversationState.INTERRUPTED] == frozenset()
    assert "READY" not in {s.value for s in ConversationState}


def given_session_table_when_adr006_flow_checked_then_interrupting_returns_to_ready() -> None:
    assert _path(SESSION_TRANSITIONS, SessionState.READY, SessionState.READY) == []
    assert _path(SESSION_TRANSITIONS, SessionState.RUNNING, SessionState.READY) == [
        SessionState.INTERRUPTING,
        SessionState.READY,
    ]
    assert can_transition(SESSION_TRANSITIONS, SessionState.COMPLETED, SessionState.RUNNING)
    assert is_terminal(SESSION_TRANSITIONS, SessionState.FAILED)


def given_session_table_when_adr025_pause_checked_then_reachable_and_not_terminal() -> None:
    """ADR-025: ``PAUSED`` is entered from ``RUNNING`` only, and is never an end of the road."""
    assert {
        current
        for current in SessionState
        if can_transition(SESSION_TRANSITIONS, current, SessionState.PAUSED)
    } == {SessionState.RUNNING}
    assert SESSION_TRANSITIONS[SessionState.PAUSED] == {
        SessionState.RUNNING,  # the user provided a token
        SessionState.READY,  # the user interrupted the paused session (nothing to drain)
        SessionState.FAILED,  # the pause is given up on
    }
    assert not is_terminal(SESSION_TRANSITIONS, SessionState.PAUSED)
    # a pause is not an interruption: it does not pass by INTERRUPTING, and it never ends a session
    assert not can_transition(SESSION_TRANSITIONS, SessionState.PAUSED, SessionState.INTERRUPTING)
    assert not can_transition(SESSION_TRANSITIONS, SessionState.PAUSED, SessionState.COMPLETED)
    assert _path(SESSION_TRANSITIONS, SessionState.READY, SessionState.PAUSED) == [
        SessionState.RUNNING,
        SessionState.PAUSED,
    ]


def given_paused_session_when_driven_through_the_manager_then_every_exit_is_accepted(
    lifecycle: ConversationLifecycleManager,
) -> None:
    """ADR-025: the three exits of ``PAUSED`` go through the lifecycle manager like any other."""
    for target in (SessionState.RUNNING, SessionState.READY, SessionState.FAILED):
        session = _session_in(lifecycle, SessionState.PAUSED)
        assert session.status is SessionState.PAUSED
        moved = lifecycle.transition_session(
            session.session_id, target, reason="credentials_provided"
        )
        assert moved.status is target


def given_plan_and_task_tables_when_terminal_sets_read_then_equal_states_without_exit() -> None:
    assert TERMINAL_PLAN_STATES == {
        PlanState.COMPLETED,
        PlanState.STOPPED_ON_FAILURE,
        PlanState.SHORT_CIRCUITED_ON_SUCCESS,
        PlanState.INTERRUPTED,
        PlanState.FAILED,
    }
    assert TERMINAL_TASK_STATES == {
        TaskState.COMPLETED,
        TaskState.FAILED,
        TaskState.TIMED_OUT,
        TaskState.SKIPPED,
        TaskState.CANCELLED,
        TaskState.INTERRUPTED,
    }
    assert TERMINAL_PLAN_STATES == {s for s in PlanState if is_terminal(PLAN_TRANSITIONS, s)}
    assert TERMINAL_TASK_STATES == {s for s in TaskState if is_terminal(TASK_TRANSITIONS, s)}


def given_task_table_when_failed_task_states_read_then_failed_and_timed_out() -> None:
    assert FAILED_TASK_STATES == {TaskState.FAILED, TaskState.TIMED_OUT}
    assert FAILED_TASK_STATES <= TERMINAL_TASK_STATES


def given_plan_table_when_adr007_additions_checked_then_pending_can_fail_or_be_interrupted() -> (
    None
):
    assert can_transition(PLAN_TRANSITIONS, PlanState.PENDING, PlanState.FAILED)
    assert can_transition(PLAN_TRANSITIONS, PlanState.PENDING, PlanState.INTERRUPTED)


def given_context_window_table_when_adr013_shortcut_checked_then_healthy_to_saturated_listed() -> (
    None
):
    assert can_transition(
        CONTEXT_WINDOW_TRANSITIONS, ContextWindowState.HEALTHY, ContextWindowState.SATURATED
    )
    assert not can_transition(
        CONTEXT_WINDOW_TRANSITIONS, ContextWindowState.WARNING, ContextWindowState.HEALTHY
    )


# =============================================================================================
# B. ConversationLifecycleManager — sessions
# =============================================================================================


def given_no_session_when_created_then_ready_record_persisted_and_created_event_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    session = lifecycle.create_session(
        goal="g", user_message="m", user_id="u", budget=BUDGET, auto_close=True
    )

    assert session.session_id == "sess-0001"
    assert session.status is SessionState.READY
    assert (session.goal, session.user_message, session.user_id) == ("g", "m", "u")
    assert session.auto_close_on_final_answer is True
    assert session.budget == BUDGET
    assert session.created_at == session.updated_at == clock.now()
    assert session.started_at is None and session.ended_at is None
    assert session.current_conversation_id is None
    assert store.get_session("sess-0001") == session
    assert lifecycle.get_session("sess-0001") == session

    event = _only_event(recorder)
    assert event.event_type is EventType.SESSION_CREATED
    assert event.session_id == "sess-0001" and event.conversation_id is None
    assert event.timestamp == clock.now()
    # ADR-027 §4: the two sign-in extras are always in the payload, empty when nothing was chosen
    assert event.payload == {
        "goal": "g",
        "budget": BUDGET.model_dump(),
        "skills": [],
        "effort": None,
    }


def given_store_failing_when_session_created_then_persistence_error_nothing_stored_no_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        _new_session(lifecycle)
    assert store.list_sessions() == []
    assert recorder.events == []


def given_ready_session_when_started_then_running_with_started_at_and_state_changed_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    session = _new_session(lifecycle)
    clock.advance(250)
    recorder.clear()

    running = lifecycle.transition_session(
        session.session_id, SessionState.RUNNING, reason="user_request"
    )

    assert running.status is SessionState.RUNNING
    assert running.started_at == clock.now() == running.updated_at
    assert running.created_at == session.created_at
    assert running.ended_at is None and running.interrupted_at is None
    assert store.get_session(session.session_id) == running

    event = _only_event(recorder)
    assert event.event_type is EventType.SESSION_STATE_CHANGED
    assert event.session_id == session.session_id and event.conversation_id is None
    assert event.timestamp == clock.now()
    assert event.payload == {"from": "READY", "to": "RUNNING", "reason": "user_request"}


def given_running_session_when_completed_then_ended_at_set(
    lifecycle: ConversationLifecycleManager, clock: FakeClock
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    clock.advance(1000)
    completed = lifecycle.transition_session(session.session_id, SessionState.COMPLETED)
    assert completed.status is SessionState.COMPLETED
    assert completed.ended_at == clock.now()
    assert completed.started_at == session.started_at


def given_running_session_when_failed_then_ended_at_set(
    lifecycle: ConversationLifecycleManager, clock: FakeClock
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    clock.advance(1000)
    failed = lifecycle.transition_session(
        session.session_id, SessionState.FAILED, reason="BUDGET_EXCEEDED"
    )
    assert failed.status is SessionState.FAILED
    assert failed.ended_at == clock.now()


def given_running_session_when_interrupting_then_interrupted_at_set_and_ended_at_untouched(
    lifecycle: ConversationLifecycleManager, clock: FakeClock
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    clock.advance(1000)
    interrupting = lifecycle.transition_session(
        session.session_id, SessionState.INTERRUPTING, reason="user_interrupt"
    )
    assert interrupting.status is SessionState.INTERRUPTING
    assert interrupting.interrupted_at == clock.now()
    assert interrupting.ended_at is None


def given_interrupting_session_when_reset_then_ready_and_event_from_interrupting_to_ready(
    lifecycle: ConversationLifecycleManager,
    recorder: RecordingSubscriber,
    store: InMemoryConversationStore,
) -> None:
    session = _session_in(lifecycle, SessionState.INTERRUPTING)
    recorder.clear()
    ready = lifecycle.transition_session(
        session.session_id, SessionState.READY, reason="cleanup_persisted"
    )
    assert ready.status is SessionState.READY
    assert ready.interrupted_at == session.interrupted_at  # kept for the audit
    assert store.get_session(session.session_id) == ready
    assert _only_event(recorder).payload == {
        "from": "INTERRUPTING",
        "to": "READY",
        "reason": "cleanup_persisted",
    }


def given_completed_session_with_auto_close_when_follow_up_then_rejected(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    session = _session_in(lifecycle, SessionState.COMPLETED, auto_close=True)
    recorder.clear()

    with pytest.raises(InvalidTransitionError) as exc:
        lifecycle.transition_session(session.session_id, SessionState.RUNNING, reason="follow_up")

    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        "session",
        "COMPLETED",
        "RUNNING",
    )
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


def given_auto_close_session_with_conversation_waiting_for_user_when_follow_up_then_running(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    """ADR-022: the model asked a question (``user_response`` with ``expects_reply``), the
    conversation was left ``WAITING_USER`` instead of ``CLOSED`` — the user may answer it."""
    session = _session_in(lifecycle, SessionState.RUNNING, auto_close=True)
    conversation = _conversation_in(
        lifecycle, ConversationState.WAITING_MODEL_RESPONSE, session=session
    )
    lifecycle.transition_conversation(
        conversation.conversation_id,
        ConversationState.COMPLETED,
        reason="user_response",
        final_answer_received=True,
    )
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.WAITING_USER)
    lifecycle.transition_session(session.session_id, SessionState.COMPLETED, reason="user_response")
    recorder.clear()

    running = lifecycle.transition_session(
        session.session_id, SessionState.RUNNING, reason="user_request"
    )

    assert running.status is SessionState.RUNNING and running.auto_close_on_final_answer is True
    assert store.get_session(session.session_id) == running
    assert _only_event(recorder).payload == {
        "from": "COMPLETED",
        "to": "RUNNING",
        "reason": "user_request",
    }
    # once the conversation is closed for good, COMPLETED is terminal again
    lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.COMPLETED)
    lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.CLOSED, closure_reason="auto_close"
    )
    lifecycle.transition_session(session.session_id, SessionState.COMPLETED, reason="final_answer")
    with pytest.raises(InvalidTransitionError):
        lifecycle.transition_session(session.session_id, SessionState.RUNNING)


def given_completed_session_without_auto_close_when_follow_up_then_running(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    session = _session_in(lifecycle, SessionState.COMPLETED, auto_close=False)
    assert session.ended_at is not None
    clock.advance(5000)
    recorder.clear()

    running = lifecycle.transition_session(
        session.session_id, SessionState.RUNNING, reason="follow_up"
    )

    assert running.status is SessionState.RUNNING
    assert running.started_at == session.started_at  # first RUNNING only
    assert running.ended_at is None  # the session is no longer ended
    assert running.updated_at == clock.now()
    assert store.get_session(session.session_id) == running
    assert _only_event(recorder).payload == {
        "from": "COMPLETED",
        "to": "RUNNING",
        "reason": "follow_up",
    }


@pytest.mark.parametrize(
    ("current", "target"), _pairs_of(SESSION_TRANSITIONS, SessionState, listed=True)
)
def given_session_in_each_state_when_listed_transition_applied_then_persisted_and_event_exact(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    current: SessionState,
    target: SessionState,
) -> None:
    session = _session_in(lifecycle, current)
    assert session.status is current
    recorder.clear()

    result = lifecycle.transition_session(session.session_id, target, reason="r")

    assert result.status is target
    assert store.get_session(session.session_id) == result
    event = _only_event(recorder)
    assert event.event_type is EventType.SESSION_STATE_CHANGED
    assert event.session_id == session.session_id
    assert event.payload == {"from": current.value, "to": target.value, "reason": "r"}


@pytest.mark.parametrize(
    ("current", "target"), _pairs_of(SESSION_TRANSITIONS, SessionState, listed=False)
)
def given_session_in_each_state_when_unlisted_transition_attempted_then_rejected_without_write_or_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    current: SessionState,
    target: SessionState,
) -> None:
    session = _session_in(lifecycle, current)
    recorder.clear()

    with pytest.raises(InvalidTransitionError) as exc:
        lifecycle.transition_session(session.session_id, target)

    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        "session",
        current.value,
        target.value,
    )
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


def given_running_session_when_transitioned_with_updates_then_fields_applied_in_same_write(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    failed = lifecycle.transition_session(
        session.session_id,
        SessionState.FAILED,
        reason="BUDGET_EXCEEDED",
        last_failure_id="fail-0001",
        consumed_cycles=3,
    )
    assert failed.status is SessionState.FAILED
    assert failed.last_failure_id == "fail-0001" and failed.consumed_cycles == 3
    assert store.get_session(session.session_id) == failed


def given_session_when_transition_receives_status_in_updates_then_value_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    session = _new_session(lifecycle)
    recorder.clear()
    with pytest.raises(ValueError, match="status"):
        lifecycle.transition_session(
            session.session_id, SessionState.RUNNING, status=SessionState.FAILED
        )
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


def given_session_when_updated_then_fields_changed_updated_at_refreshed_and_no_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    clock.advance(10)
    recorder.clear()

    updated = lifecycle.update_session(
        session.session_id, consumed_cycles=2, consumed_plans=1, final_answer={"text": "ok"}
    )

    assert updated.status is SessionState.RUNNING
    assert (updated.consumed_cycles, updated.consumed_plans) == (2, 1)
    assert updated.final_answer == {"text": "ok"}
    assert updated.updated_at == clock.now() != session.updated_at
    assert store.get_session(session.session_id) == updated
    assert recorder.events == []


# NB: the primary key (``session_id`` / ``conversation_id``) cannot be smuggled through ``**updates``
# at all: Python rejects the duplicate keyword before the manager runs, so it is not listed here.
@pytest.mark.parametrize(
    "updates",
    [
        pytest.param({"status": SessionState.RUNNING}, id="status"),
        pytest.param({"created_at": None}, id="created_at"),
        pytest.param({"updated_at": None}, id="updated_at"),
        pytest.param({"no_such_field": 1}, id="unknown_field"),
        pytest.param({"consumed_cycles": "not-an-int"}, id="wrong_type"),
    ],
)
def given_session_when_update_contains_forbidden_field_then_value_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    updates: dict[str, Any],
) -> None:
    session = _new_session(lifecycle)
    recorder.clear()
    with pytest.raises(ValueError):
        lifecycle.update_session(session.session_id, **updates)
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda lc: lc.transition_session("sess-9999", SessionState.RUNNING),
            id="transition_session",
        ),
        pytest.param(
            lambda lc: lc.update_session("sess-9999", consumed_cycles=1), id="update_session"
        ),
        pytest.param(lambda lc: lc.create_conversation("sess-9999"), id="create_conversation"),
    ],
)
def given_unknown_session_id_when_session_method_called_then_key_error_and_no_event(
    lifecycle: ConversationLifecycleManager,
    recorder: RecordingSubscriber,
    call: Callable[[ConversationLifecycleManager], object],
) -> None:
    with pytest.raises(KeyError, match="sess-9999"):
        call(lifecycle)
    assert recorder.events == []


def given_unknown_session_id_when_get_session_then_none(
    lifecycle: ConversationLifecycleManager,
) -> None:
    assert lifecycle.get_session("sess-9999") is None


# =============================================================================================
# C. ConversationLifecycleManager — conversations
# =============================================================================================


def given_ready_session_when_conversation_created_then_new_record_with_session_snapshot_and_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    session = _new_session(lifecycle)
    clock.advance(100)
    recorder.clear()

    conversation = lifecycle.create_conversation(session.session_id)

    assert conversation.conversation_id == "conv-0001"
    assert conversation.session_id == session.session_id
    assert conversation.status is ConversationState.NEW
    assert conversation.parent_conversation_id is None
    assert conversation.remote_conversation_id is None
    assert conversation.auto_close_on_final_answer is False
    assert conversation.context_window_state is ContextWindowState.HEALTHY
    assert conversation.context_bytes == 0
    assert conversation.session_budget_json == BUDGET.model_dump()
    assert conversation.created_at == conversation.updated_at == clock.now()
    assert conversation.interrupted_at is None and conversation.closure_reason is None
    assert store.get_conversation("conv-0001") == conversation
    assert lifecycle.get_conversation("conv-0001") == conversation

    event = _only_event(recorder)
    assert event.event_type is EventType.CONVERSATION_CREATED
    assert (event.session_id, event.conversation_id) == (session.session_id, "conv-0001")
    assert event.timestamp == clock.now()
    assert event.payload == {"parent_conversation_id": None, "context_window_state": "HEALTHY"}


def given_session_with_auto_close_when_conversation_created_then_flag_copied(
    lifecycle: ConversationLifecycleManager,
) -> None:
    session = _new_session(lifecycle, auto_close=True)
    conversation = lifecycle.create_conversation(session.session_id)
    assert conversation.auto_close_on_final_answer is True


def given_conversation_created_when_session_read_then_current_conversation_id_points_to_it(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore, clock: FakeClock
) -> None:
    session = _new_session(lifecycle)
    clock.advance(100)
    first = lifecycle.create_conversation(session.session_id)
    assert _require_session(store, session.session_id).current_conversation_id == "conv-0001"
    assert _require_session(store, session.session_id).updated_at == clock.now()
    assert _require_session(store, session.session_id).status is SessionState.READY

    second = lifecycle.create_conversation(session.session_id)
    assert (first.conversation_id, second.conversation_id) == ("conv-0001", "conv-0002")
    assert _require_session(store, session.session_id).current_conversation_id == "conv-0002"
    assert [c.conversation_id for c in store.list_conversations(session.session_id)] == [
        "conv-0001",
        "conv-0002",
    ]


def given_existing_conversation_when_child_created_then_parent_linked_and_context_state_inherited(
    lifecycle: ConversationLifecycleManager, recorder: RecordingSubscriber
) -> None:
    parent = _conversation_in(lifecycle, ConversationState.ROTATING)
    recorder.clear()

    child = lifecycle.create_conversation(
        parent.session_id,
        parent_conversation_id=parent.conversation_id,
        context_window_state=ContextWindowState.SATURATED,
    )

    assert child.parent_conversation_id == parent.conversation_id
    assert child.session_id == parent.session_id
    assert child.status is ConversationState.NEW
    assert child.context_window_state is ContextWindowState.SATURATED
    assert _only_event(recorder).payload == {
        "parent_conversation_id": parent.conversation_id,
        "context_window_state": "SATURATED",
    }


def given_unknown_parent_when_conversation_created_then_key_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    session = _new_session(lifecycle)
    recorder.clear()
    with pytest.raises(KeyError, match="conv-9999"):
        lifecycle.create_conversation(session.session_id, parent_conversation_id="conv-9999")
    assert store.list_conversations(session.session_id) == []
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


def given_parent_from_other_session_when_conversation_created_then_value_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    other = _conversation_in(lifecycle, ConversationState.INTERRUPTED)
    session = _new_session(lifecycle)
    recorder.clear()
    with pytest.raises(ValueError, match="session"):
        lifecycle.create_conversation(
            session.session_id, parent_conversation_id=other.conversation_id
        )
    assert store.list_conversations(session.session_id) == []
    assert recorder.events == []


def given_store_failing_on_session_write_when_conversation_created_then_nothing_persisted_and_no_event(
    bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator, recorder: RecordingSubscriber
) -> None:
    store = _StoreFailingOnSessionWrite()
    lifecycle = ConversationLifecycleManager(store=store, bus=bus, clock=clock, ids=ids)
    session = _new_session(lifecycle)
    recorder.clear()
    store.fail_next_session_write = True

    with pytest.raises(PersistenceError):
        lifecycle.create_conversation(session.session_id)

    # atomic: the conversation written first inside the transaction is rolled back too
    assert store.list_conversations(session.session_id) == []
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


@pytest.mark.parametrize(
    ("current", "target"), _pairs_of(CONVERSATION_TRANSITIONS, ConversationState, listed=True)
)
def given_conversation_in_each_state_when_listed_transition_applied_then_persisted_and_event_exact(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    current: ConversationState,
    target: ConversationState,
) -> None:
    conversation = _conversation_in(lifecycle, current)
    assert conversation.status is current
    clock.advance(10)
    recorder.clear()

    result = lifecycle.transition_conversation(conversation.conversation_id, target, reason="r")

    assert result.status is target
    assert result.updated_at == clock.now()
    assert result.created_at == conversation.created_at
    assert store.get_conversation(conversation.conversation_id) == result
    event = _only_event(recorder)
    assert event.event_type is EventType.CONVERSATION_STATE_CHANGED
    assert (event.session_id, event.conversation_id) == (
        conversation.session_id,
        conversation.conversation_id,
    )
    assert event.timestamp == clock.now()
    assert event.payload == {"from": current.value, "to": target.value, "reason": "r"}


@pytest.mark.parametrize(
    ("current", "target"), _pairs_of(CONVERSATION_TRANSITIONS, ConversationState, listed=False)
)
def given_conversation_in_each_state_when_unlisted_transition_attempted_then_rejected_without_write_or_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    current: ConversationState,
    target: ConversationState,
) -> None:
    conversation = _conversation_in(lifecycle, current)
    recorder.clear()

    with pytest.raises(InvalidTransitionError) as exc:
        lifecycle.transition_conversation(conversation.conversation_id, target, reason="r")

    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        "conversation",
        current.value,
        target.value,
    )
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


def given_transition_without_reason_when_published_then_payload_has_no_reason_key(
    lifecycle: ConversationLifecycleManager, recorder: RecordingSubscriber
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.NEW)
    recorder.clear()
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)
    assert _only_event(recorder).payload == {"from": "NEW", "to": "ACTIVE"}


def given_waiting_model_response_when_running_plan_with_updates_then_fields_written_in_same_record(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.WAITING_MODEL_RESPONSE)

    running = lifecycle.transition_conversation(
        conversation.conversation_id,
        ConversationState.RUNNING_PLAN,
        reason="execution_plan",
        current_plan_id="plan-1",
        current_cycle_id="cyc-0001",
        remote_conversation_id="remote-42",
        last_model_response_state="valid",
    )

    assert running.status is ConversationState.RUNNING_PLAN
    assert (running.current_plan_id, running.current_cycle_id) == ("plan-1", "cyc-0001")
    assert running.remote_conversation_id == "remote-42"
    assert running.last_model_response_state == "valid"
    assert store.get_conversation(conversation.conversation_id) == running

    back = lifecycle.transition_conversation(
        conversation.conversation_id,
        ConversationState.WAITING_MODEL_RESPONSE,
        current_plan_id=None,
        last_completed_plan_id="plan-1",
    )
    assert back.current_plan_id is None and back.last_completed_plan_id == "plan-1"
    assert back.current_cycle_id == "cyc-0001"  # untouched fields are kept


@pytest.mark.parametrize(
    "updates",
    [
        pytest.param({"status": ConversationState.FAILED}, id="status"),
        pytest.param(
            {"context_window_state": ContextWindowState.SATURATED}, id="context_window_state"
        ),
        pytest.param({"session_id": "sess-9999"}, id="session_id"),
        pytest.param({"created_at": None}, id="created_at"),
        pytest.param({"updated_at": None}, id="updated_at"),
        pytest.param({"no_such_field": 1}, id="unknown_field"),
        pytest.param({"context_bytes": "many"}, id="wrong_type"),
    ],
)
def given_conversation_when_transition_receives_forbidden_update_then_value_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    updates: dict[str, Any],
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.NEW)
    recorder.clear()
    with pytest.raises(ValueError):
        lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.ACTIVE, **updates
        )
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


def given_active_conversation_when_transitioned_to_interrupted_then_interrupted_at_set(
    lifecycle: ConversationLifecycleManager, clock: FakeClock
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.ACTIVE)
    clock.advance(1000)
    interrupted = lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.INTERRUPTED, reason="restart"
    )
    assert interrupted.status is ConversationState.INTERRUPTED
    assert interrupted.interrupted_at == clock.now() == interrupted.updated_at


def given_active_conversation_when_transitioned_elsewhere_then_interrupted_at_stays_none(
    lifecycle: ConversationLifecycleManager,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.ACTIVE)
    result = lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    assert result.interrupted_at is None


@pytest.mark.parametrize(
    "state", sorted(ACTIVE_CONVERSATION_STATES, key=CONVERSATION_ORDER.index), ids=_state_id
)
def given_each_active_state_when_user_interrupts_then_conversation_interrupted_with_reason(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    state: ConversationState,
) -> None:
    conversation = _conversation_in(lifecycle, state)
    clock.advance(42)
    recorder.clear()

    result = lifecycle.interrupt_conversation(conversation.conversation_id, reason="user_interrupt")

    assert result.status is ConversationState.INTERRUPTED
    assert result.interrupted_at == clock.now() == result.updated_at
    assert store.get_conversation(conversation.conversation_id) == result
    assert is_terminal(CONVERSATION_TRANSITIONS, result.status)
    event = _only_event(recorder)
    assert event.event_type is EventType.CONVERSATION_STATE_CHANGED
    assert (event.session_id, event.conversation_id) == (
        conversation.session_id,
        conversation.conversation_id,
    )
    assert event.payload == {"from": state.value, "to": "INTERRUPTED", "reason": "user_interrupt"}


@pytest.mark.parametrize("state", NON_ACTIVE_CONVERSATION_STATES, ids=_state_id)
def given_each_non_active_state_when_user_interrupts_then_rejected_without_write_or_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    state: ConversationState,
) -> None:
    conversation = _conversation_in(lifecycle, state)
    recorder.clear()

    with pytest.raises(InvalidTransitionError) as exc:
        lifecycle.interrupt_conversation(conversation.conversation_id, reason="user_interrupt")

    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        "conversation",
        state.value,
        "INTERRUPTED",
    )
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


def given_store_failing_when_transition_attempted_then_state_unchanged_and_no_event_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.ACTIVE)
    recorder.clear()
    store.fail_next_write = True

    with pytest.raises(PersistenceError):
        lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
        )

    assert store.get_conversation(conversation.conversation_id) == conversation
    assert _require_conversation(store, conversation.conversation_id).status is (
        ConversationState.ACTIVE
    )
    assert recorder.events == []

    # the manager is still usable once the store recovers
    after = lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    assert after.status is ConversationState.WAITING_MODEL_RESPONSE
    assert _only_event(recorder).payload == {"from": "ACTIVE", "to": "WAITING_MODEL_RESPONSE"}


def given_store_failing_when_session_transition_attempted_then_state_unchanged_and_no_event_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    session = _new_session(lifecycle)
    recorder.clear()
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    assert store.get_session(session.session_id) == session
    assert recorder.events == []


def given_store_failing_when_interrupt_attempted_then_state_unchanged_and_no_event_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.RUNNING_PLAN)
    recorder.clear()
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        lifecycle.interrupt_conversation(conversation.conversation_id, reason="user_interrupt")
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


def given_store_failing_when_update_attempted_then_state_unchanged(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.ACTIVE)
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        lifecycle.update_conversation(conversation.conversation_id, context_bytes=10)
    assert store.get_conversation(conversation.conversation_id) == conversation


def given_interrupted_session_when_new_user_request_then_new_conversation_becomes_active(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    # a session running its first conversation, mid-plan
    session = _session_in(lifecycle, SessionState.RUNNING)
    first = _conversation_in(lifecycle, ConversationState.RUNNING_PLAN, session=session)

    # §9 amended by ADR-006: session INTERRUPTING, conversation INTERRUPTED (terminal), session READY
    lifecycle.transition_session(
        session.session_id, SessionState.INTERRUPTING, reason="user_interrupt"
    )
    lifecycle.interrupt_conversation(first.conversation_id, reason="user_interrupt")
    ready = lifecycle.transition_session(
        session.session_id, SessionState.READY, reason="cleanup_persisted"
    )
    assert ready.status is SessionState.READY
    assert ready.current_conversation_id == first.conversation_id
    recorder.clear()

    # a new user_request: the SAME session runs again, a NEW conversation is opened
    lifecycle.transition_session(session.session_id, SessionState.RUNNING, reason="user_request")
    second = lifecycle.create_conversation(
        session.session_id, parent_conversation_id=first.conversation_id
    )
    second = lifecycle.transition_conversation(second.conversation_id, ConversationState.ACTIVE)

    assert second.status is ConversationState.ACTIVE
    assert second.conversation_id != first.conversation_id
    assert second.parent_conversation_id == first.conversation_id
    assert second.session_id == session.session_id
    # the old conversation is preserved, terminal, untouched
    old = _require_conversation(store, first.conversation_id)
    assert old.status is ConversationState.INTERRUPTED
    assert old.interrupted_at is not None
    # the session points to the new conversation and kept its budget counters
    current = _require_session(store, session.session_id)
    assert current.status is SessionState.RUNNING
    assert current.current_conversation_id == second.conversation_id
    assert current.started_at == session.started_at
    assert [
        (e.event_type, e.payload.get("from"), e.payload.get("to")) for e in recorder.events
    ] == [
        (EventType.SESSION_STATE_CHANGED, "READY", "RUNNING"),
        (EventType.CONVERSATION_CREATED, None, None),
        (EventType.CONVERSATION_STATE_CHANGED, "NEW", "ACTIVE"),
    ]


def given_conversation_when_updated_then_fields_changed_updated_at_refreshed_and_no_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.WAITING_MODEL_RESPONSE)
    clock.advance(10)
    recorder.clear()

    updated = lifecycle.update_conversation(
        conversation.conversation_id,
        context_bytes=1234,
        remote_conversation_id="remote-1",
        get_cursor="c-9",
        last_outbound_message_id="msg-0001",
        protocol_error_count=1,
    )

    assert updated.status is ConversationState.WAITING_MODEL_RESPONSE
    assert updated.context_bytes == 1234
    assert updated.remote_conversation_id == "remote-1"
    assert updated.get_cursor == "c-9"
    assert updated.last_outbound_message_id == "msg-0001"
    assert updated.protocol_error_count == 1
    assert updated.updated_at == clock.now() != conversation.updated_at
    assert store.get_conversation(conversation.conversation_id) == updated
    assert recorder.events == []


@pytest.mark.parametrize(
    "updates",
    [
        pytest.param({"status": ConversationState.ACTIVE}, id="status"),
        pytest.param(
            {"context_window_state": ContextWindowState.WARNING}, id="context_window_state"
        ),
        pytest.param({"session_id": "sess-9999"}, id="session_id"),
        pytest.param({"no_such_field": 1}, id="unknown_field"),
        pytest.param({"protocol_error_count": "two"}, id="wrong_type"),
    ],
)
def given_conversation_when_update_contains_forbidden_field_then_value_error_and_nothing_written(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    updates: dict[str, Any],
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.NEW)
    recorder.clear()
    with pytest.raises(ValueError):
        lifecycle.update_conversation(conversation.conversation_id, **updates)
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(
            lambda lc: lc.transition_conversation("conv-9999", ConversationState.ACTIVE),
            id="transition_conversation",
        ),
        pytest.param(
            lambda lc: lc.update_conversation("conv-9999", context_bytes=1),
            id="update_conversation",
        ),
        pytest.param(
            lambda lc: lc.interrupt_conversation("conv-9999", reason="user_interrupt"),
            id="interrupt_conversation",
        ),
        pytest.param(
            lambda lc: lc.transition_context_window("conv-9999", ContextWindowState.WARNING),
            id="transition_context_window",
        ),
    ],
)
def given_unknown_conversation_id_when_conversation_method_called_then_key_error_and_no_event(
    lifecycle: ConversationLifecycleManager,
    recorder: RecordingSubscriber,
    call: Callable[[ConversationLifecycleManager], object],
) -> None:
    with pytest.raises(KeyError, match="conv-9999"):
        call(lifecycle)
    assert recorder.events == []


def given_unknown_conversation_id_when_get_conversation_then_none(
    lifecycle: ConversationLifecycleManager,
) -> None:
    assert lifecycle.get_conversation("conv-9999") is None


# =============================================================================================
# D. Context window (§5.4, ADR-013)
# =============================================================================================


def given_healthy_window_when_warning_then_saturated_then_healthy_then_each_step_persisted_and_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    clock: FakeClock,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.WAITING_MODEL_RESPONSE)
    lifecycle.update_conversation(conversation.conversation_id, context_bytes=280_000)
    recorder.clear()

    steps = (
        (ContextWindowState.WARNING, "warning_ratio_reached"),
        (ContextWindowState.SATURATED, "saturation_ratio_reached"),
        (ContextWindowState.HEALTHY, "context_resume_ack"),
    )
    previous = ContextWindowState.HEALTHY
    for target, reason in steps:
        clock.advance(10)
        result = lifecycle.transition_context_window(
            conversation.conversation_id, target, reason=reason
        )
        assert result.context_window_state is target
        assert result.status is ConversationState.WAITING_MODEL_RESPONSE  # unchanged
        assert result.updated_at == clock.now()
        assert store.get_conversation(conversation.conversation_id) == result
        event = recorder.events[-1]
        assert event.event_type is EventType.CONTEXT_WINDOW_STATE_CHANGED
        assert (event.session_id, event.conversation_id) == (
            conversation.session_id,
            conversation.conversation_id,
        )
        assert event.timestamp == clock.now()
        assert event.payload == {
            "from": previous.value,
            "to": target.value,
            "reason": reason,
            "context_bytes": 280_000,
        }
        previous = target
    assert len(recorder.events) == 3


def given_healthy_window_when_context_window_error_then_direct_saturated(
    lifecycle: ConversationLifecycleManager, recorder: RecordingSubscriber
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.WAITING_MODEL_RESPONSE)
    recorder.clear()
    result = lifecycle.transition_context_window(
        conversation.conversation_id,
        ContextWindowState.SATURATED,
        reason="MODEL_CONTEXT_WINDOW_ERROR",
    )
    assert result.context_window_state is ContextWindowState.SATURATED
    assert _only_event(recorder).payload == {
        "from": "HEALTHY",
        "to": "SATURATED",
        "reason": "MODEL_CONTEXT_WINDOW_ERROR",
        "context_bytes": 0,
    }


def given_warning_window_when_healthy_requested_then_rejected_without_write_or_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.WAITING_MODEL_RESPONSE)
    warned = lifecycle.transition_context_window(
        conversation.conversation_id, ContextWindowState.WARNING
    )
    recorder.clear()

    with pytest.raises(InvalidTransitionError) as exc:
        lifecycle.transition_context_window(
            conversation.conversation_id, ContextWindowState.HEALTHY
        )

    assert (exc.value.entity, exc.value.current, exc.value.target) == (
        "context_window",
        "WARNING",
        "HEALTHY",
    )
    assert store.get_conversation(conversation.conversation_id) == warned
    assert recorder.events == []


@pytest.mark.parametrize(
    ("current", "target"),
    _pairs_of(CONTEXT_WINDOW_TRANSITIONS, ContextWindowState, listed=False),
)
def given_window_in_each_state_when_unlisted_transition_attempted_then_rejected(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
    current: ContextWindowState,
    target: ContextWindowState,
) -> None:
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id, context_window_state=current)
    recorder.clear()
    with pytest.raises(InvalidTransitionError):
        lifecycle.transition_context_window(conversation.conversation_id, target)
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


def given_store_failing_when_window_transition_attempted_then_state_unchanged_and_no_event_published(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    recorder: RecordingSubscriber,
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.ACTIVE)
    recorder.clear()
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        lifecycle.transition_context_window(
            conversation.conversation_id, ContextWindowState.WARNING
        )
    assert store.get_conversation(conversation.conversation_id) == conversation
    assert recorder.events == []


# =============================================================================================
# E. Ordering, persist-before-publish, determinism, end-to-end scenarios
# =============================================================================================


def given_new_conversation_when_driven_to_running_plan_then_events_published_in_exact_order(
    lifecycle: ConversationLifecycleManager, recorder: RecordingSubscriber
) -> None:
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    for state in (
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
    ):
        lifecycle.transition_conversation(conversation.conversation_id, state)

    assert [
        (e.event_type, e.payload.get("from"), e.payload.get("to")) for e in recorder.events
    ] == [
        (EventType.SESSION_CREATED, None, None),
        (EventType.CONVERSATION_CREATED, None, None),
        (EventType.CONVERSATION_STATE_CHANGED, "NEW", "ACTIVE"),
        (EventType.CONVERSATION_STATE_CHANGED, "ACTIVE", "WAITING_MODEL_RESPONSE"),
        (EventType.CONVERSATION_STATE_CHANGED, "WAITING_MODEL_RESPONSE", "RUNNING_PLAN"),
    ]
    assert all(e.session_id == session.session_id for e in recorder.events)
    assert recorder.events[0].conversation_id is None
    assert all(e.conversation_id == conversation.conversation_id for e in recorder.events[1:])


def given_subscriber_reading_store_when_state_changed_event_received_then_new_state_already_persisted(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore, bus: EventBus
) -> None:
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    seen: list[tuple[str, str]] = []

    def observe(event: Event) -> None:
        if event.event_type is EventType.CONVERSATION_STATE_CHANGED:
            assert event.conversation_id is not None
            seen.append(
                (
                    event.payload["to"],
                    _require_conversation(store, event.conversation_id).status.value,
                )
            )
        elif event.event_type is EventType.SESSION_STATE_CHANGED:
            seen.append(
                (event.payload["to"], _require_session(store, event.session_id).status.value)
            )

    bus.subscribe(observe, name="observer")
    lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)

    assert seen == [("RUNNING", "RUNNING"), ("ACTIVE", "ACTIVE")]


def given_advanced_clock_when_transition_applied_then_timestamps_follow_the_injected_clock(
    lifecycle: ConversationLifecycleManager, recorder: RecordingSubscriber, clock: FakeClock
) -> None:
    conversation = _conversation_in(lifecycle, ConversationState.NEW)
    t0 = conversation.created_at
    clock.advance(1500)
    recorder.clear()

    active = lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.ACTIVE
    )

    assert active.created_at == t0
    assert active.updated_at == t0 + timedelta(milliseconds=1500) == clock.now()
    assert _only_event(recorder).timestamp == active.updated_at


def given_sequential_ids_when_sessions_and_conversations_created_then_ids_deterministic(
    lifecycle: ConversationLifecycleManager,
) -> None:
    first = _new_session(lifecycle)
    second = _new_session(lifecycle)
    conversations = [
        lifecycle.create_conversation(first.session_id),
        lifecycle.create_conversation(second.session_id),
        lifecycle.create_conversation(first.session_id),
    ]
    assert (first.session_id, second.session_id) == ("sess-0001", "sess-0002")
    assert [c.conversation_id for c in conversations] == ["conv-0001", "conv-0002", "conv-0003"]
    assert [c.session_id for c in conversations] == ["sess-0001", "sess-0002", "sess-0001"]


def given_running_plan_when_rotation_scenario_played_then_parent_closed_and_child_healthy(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING)
    parent = _conversation_in(lifecycle, ConversationState.RUNNING_PLAN, session=session)
    lifecycle.transition_context_window(parent.conversation_id, ContextWindowState.WARNING)
    lifecycle.transition_context_window(parent.conversation_id, ContextWindowState.SATURATED)

    # ADR-014 steps 1-3 as seen by the lifecycle manager
    parent = lifecycle.transition_conversation(
        parent.conversation_id, ConversationState.ROTATING, reason="context_saturated"
    )
    child = lifecycle.create_conversation(
        session.session_id,
        parent_conversation_id=parent.conversation_id,
        context_window_state=ContextWindowState.SATURATED,
    )
    lifecycle.transition_conversation(child.conversation_id, ConversationState.ACTIVE)
    lifecycle.transition_conversation(
        child.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    child = lifecycle.transition_context_window(
        child.conversation_id, ContextWindowState.HEALTHY, reason="context_resume_ack"
    )
    parent = lifecycle.transition_conversation(
        parent.conversation_id, ConversationState.CLOSED, reason="rotated", closure_reason="rotated"
    )

    assert parent.status is ConversationState.CLOSED and parent.closure_reason == "rotated"
    assert parent.context_window_state is ContextWindowState.SATURATED  # parent keeps its history
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert child.parent_conversation_id == parent.conversation_id
    assert (
        _require_session(store, session.session_id).current_conversation_id == child.conversation_id
    )
    assert _require_session(store, session.session_id).status is SessionState.RUNNING


@pytest.mark.parametrize("auto_close", [True, False], ids=["auto_close", "reusable"])
def given_waiting_model_response_when_final_answer_then_completed_then_closed_or_reusable(
    lifecycle: ConversationLifecycleManager, store: InMemoryConversationStore, auto_close: bool
) -> None:
    session = _session_in(lifecycle, SessionState.RUNNING, auto_close=auto_close)
    conversation = _conversation_in(
        lifecycle, ConversationState.WAITING_MODEL_RESPONSE, session=session
    )

    completed = lifecycle.transition_conversation(
        conversation.conversation_id,
        ConversationState.COMPLETED,
        reason="final_answer",
        final_answer_received=True,
    )
    lifecycle.transition_session(session.session_id, SessionState.COMPLETED, reason="final_answer")
    assert completed.final_answer_received is True
    assert completed.auto_close_on_final_answer is auto_close

    if auto_close:
        closed = lifecycle.transition_conversation(
            conversation.conversation_id,
            ConversationState.CLOSED,
            reason="auto_close_on_final_answer",
            closure_reason="auto_close_on_final_answer",
        )
        assert closed.status is ConversationState.CLOSED
        assert is_terminal(CONVERSATION_TRANSITIONS, closed.status)
        with pytest.raises(InvalidTransitionError):
            lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    else:
        waiting = lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.WAITING_USER
        )
        assert waiting.status is ConversationState.WAITING_USER
        lifecycle.transition_session(session.session_id, SessionState.RUNNING, reason="follow_up")
        again = lifecycle.transition_conversation(
            conversation.conversation_id,
            ConversationState.WAITING_MODEL_RESPONSE,
            reason="follow_up",
        )
        assert again.status is ConversationState.WAITING_MODEL_RESPONSE
        assert _require_session(store, session.session_id).status is SessionState.RUNNING


def given_lifecycle_source_when_inspected_then_no_wall_clock_or_randomness_used() -> None:
    source = Path(conversation_lifecycle.__file__).read_text(encoding="utf-8")
    for forbidden in ("datetime.now", "time.time", "time.monotonic", "uuid", "random"):
        assert forbidden not in source, forbidden
