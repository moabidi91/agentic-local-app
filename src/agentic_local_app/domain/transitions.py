"""Transition tables for every state machine (spec §5, amended by ADR-006 / ADR-007 / ADR-013).

These tables are **the only source of truth** for what a valid transition is.
``ConversationLifecycleManager`` (conversations, sessions) and ``PlanRunner`` (plans, tasks,
cycles) must go through :func:`assert_transition` / :func:`can_transition`; they never hard-code
state changes. Phase 1 tests walk these tables exhaustively: every pair listed here must be
accepted and every pair absent must be rejected.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import Enum
from typing import TypeVar

from agentic_local_app.domain.errors import InvalidTransitionError
from agentic_local_app.domain.states import (
    CircuitState,
    ContextWindowState,
    ConversationState,
    CycleState,
    PlanState,
    SessionState,
    TaskState,
)

S = TypeVar("S", bound=Enum)

# --------------------------------------------------------------------------------------------
# Conversation (§5.1 + ADR-007)
#   - WAITING_MODEL_RESPONSE -> ROTATING added (rotation on GET context error, §14)
#   - ROTATING -> CLOSED added (parent ends terminal once the child acknowledged, reason "rotated")
#   - WAITING_USER -> ROTATING added (ADR-019: a follow-up user_request projected over the budget)
#   - INTERRUPTED -> READY and READY -> ACTIVE moved to the session machine (ADR-006)
#   - ANY (non terminal) -> FAILED
# --------------------------------------------------------------------------------------------
CONVERSATION_TRANSITIONS: Mapping[ConversationState, frozenset[ConversationState]] = {
    ConversationState.NEW: frozenset({ConversationState.ACTIVE, ConversationState.FAILED}),
    ConversationState.ACTIVE: frozenset(
        {
            ConversationState.WAITING_MODEL_RESPONSE,
            ConversationState.INTERRUPTED,
            ConversationState.FAILED,
        }
    ),
    ConversationState.WAITING_MODEL_RESPONSE: frozenset(
        {
            ConversationState.RUNNING_PLAN,
            ConversationState.COMPLETED,
            ConversationState.ROTATING,
            ConversationState.INTERRUPTED,
            ConversationState.FAILED,
        }
    ),
    ConversationState.RUNNING_PLAN: frozenset(
        {
            ConversationState.WAITING_MODEL_RESPONSE,
            ConversationState.ROTATING,
            ConversationState.INTERRUPTED,
            ConversationState.FAILED,
        }
    ),
    ConversationState.ROTATING: frozenset(
        {ConversationState.CLOSED, ConversationState.INTERRUPTED, ConversationState.FAILED}
    ),
    ConversationState.WAITING_USER: frozenset(
        {
            ConversationState.WAITING_MODEL_RESPONSE,
            ConversationState.ROTATING,  # ADR-019: follow-up request projected over the budget
            ConversationState.FAILED,
        }
    ),
    ConversationState.COMPLETED: frozenset(
        {ConversationState.WAITING_USER, ConversationState.CLOSED, ConversationState.FAILED}
    ),
    ConversationState.INTERRUPTED: frozenset(),
    ConversationState.FAILED: frozenset(),
    ConversationState.CLOSED: frozenset(),
}

#: "ANY_ACTIVE_STATE" of the specification (§5.1): the states from which a user interrupt applies.
ACTIVE_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
        ConversationState.ROTATING,
    }
)

TERMINAL_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {ConversationState.INTERRUPTED, ConversationState.FAILED, ConversationState.CLOSED}
)

# --------------------------------------------------------------------------------------------
# Session (ADR-006 / ADR-007 / ADR-025)
#   - RUNNING -> PAUSED added (ADR-025: an authentication error waits for new credentials)
# --------------------------------------------------------------------------------------------
SESSION_TRANSITIONS: Mapping[SessionState, frozenset[SessionState]] = {
    SessionState.READY: frozenset({SessionState.RUNNING}),
    SessionState.RUNNING: frozenset(
        {
            SessionState.COMPLETED,
            SessionState.PAUSED,
            SessionState.INTERRUPTING,
            SessionState.FAILED,
        }
    ),
    # ADR-025: nothing runs while the session is PAUSED and nothing was lost. It resumes when the
    # user provides a token (RUNNING), is abandoned (FAILED), or is interrupted — and an
    # interruption has nothing to drain here, so it lands on READY without passing by INTERRUPTING.
    SessionState.PAUSED: frozenset({SessionState.RUNNING, SessionState.READY, SessionState.FAILED}),
    SessionState.INTERRUPTING: frozenset({SessionState.READY, SessionState.FAILED}),
    # COMPLETED -> RUNNING: follow-up user message on a reusable conversation (§11).
    # When auto_close_on_final_answer is true the lifecycle manager refuses it (COMPLETED is then
    # terminal for that session) - see ConversationLifecycleManager.
    SessionState.COMPLETED: frozenset({SessionState.RUNNING}),
    SessionState.FAILED: frozenset(),
}

# --------------------------------------------------------------------------------------------
# Plan (§5.2 + ADR-007: PENDING -> FAILED, PENDING -> INTERRUPTED)
# --------------------------------------------------------------------------------------------
PLAN_TRANSITIONS: Mapping[PlanState, frozenset[PlanState]] = {
    PlanState.PENDING: frozenset({PlanState.RUNNING, PlanState.FAILED, PlanState.INTERRUPTED}),
    PlanState.RUNNING: frozenset(
        {
            PlanState.COMPLETED,
            PlanState.STOPPED_ON_FAILURE,
            PlanState.SHORT_CIRCUITED_ON_SUCCESS,
            PlanState.INTERRUPTED,
            PlanState.FAILED,
        }
    ),
    PlanState.COMPLETED: frozenset(),
    PlanState.STOPPED_ON_FAILURE: frozenset(),
    PlanState.SHORT_CIRCUITED_ON_SUCCESS: frozenset(),
    PlanState.INTERRUPTED: frozenset(),
    PlanState.FAILED: frozenset(),
}

TERMINAL_PLAN_STATES: frozenset[PlanState] = frozenset(
    s for s, targets in PLAN_TRANSITIONS.items() if not targets
)

# --------------------------------------------------------------------------------------------
# Task (§5.3, unchanged)
# --------------------------------------------------------------------------------------------
TASK_TRANSITIONS: Mapping[TaskState, frozenset[TaskState]] = {
    TaskState.PENDING: frozenset(
        {TaskState.WAITING_DEPENDENCY, TaskState.RUNNING, TaskState.SKIPPED, TaskState.INTERRUPTED}
    ),
    TaskState.WAITING_DEPENDENCY: frozenset(
        {TaskState.PENDING, TaskState.SKIPPED, TaskState.INTERRUPTED}
    ),
    TaskState.RUNNING: frozenset(
        {
            TaskState.COMPLETED,
            TaskState.FAILED,
            TaskState.TIMED_OUT,
            TaskState.CANCELLED,
            TaskState.INTERRUPTED,
        }
    ),
    TaskState.COMPLETED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.TIMED_OUT: frozenset(),
    TaskState.SKIPPED: frozenset(),
    TaskState.CANCELLED: frozenset(),
    TaskState.INTERRUPTED: frozenset(),
}

TERMINAL_TASK_STATES: frozenset[TaskState] = frozenset(
    s for s, targets in TASK_TRANSITIONS.items() if not targets
)

#: Task outcomes that count as a failure for the stop conditions of §8.3 (ADR-008: TIMED_OUT too).
FAILED_TASK_STATES: frozenset[TaskState] = frozenset({TaskState.FAILED, TaskState.TIMED_OUT})

# --------------------------------------------------------------------------------------------
# Cycle (ADR-007)
# --------------------------------------------------------------------------------------------
CYCLE_TRANSITIONS: Mapping[CycleState, frozenset[CycleState]] = {
    CycleState.RUNNING: frozenset(
        {CycleState.COMPLETED, CycleState.FAILED, CycleState.INTERRUPTED}
    ),
    CycleState.COMPLETED: frozenset(),
    CycleState.FAILED: frozenset(),
    CycleState.INTERRUPTED: frozenset(),
}

# --------------------------------------------------------------------------------------------
# Context window (§5.4 + ADR-013: direct HEALTHY -> SATURATED on MODEL_CONTEXT_WINDOW_ERROR)
# --------------------------------------------------------------------------------------------
CONTEXT_WINDOW_TRANSITIONS: Mapping[ContextWindowState, frozenset[ContextWindowState]] = {
    ContextWindowState.HEALTHY: frozenset(
        {ContextWindowState.WARNING, ContextWindowState.SATURATED}
    ),
    ContextWindowState.WARNING: frozenset({ContextWindowState.SATURATED}),
    ContextWindowState.SATURATED: frozenset({ContextWindowState.HEALTHY}),
}

# --------------------------------------------------------------------------------------------
# Circuit breaker (§7.4)
# --------------------------------------------------------------------------------------------
CIRCUIT_TRANSITIONS: Mapping[CircuitState, frozenset[CircuitState]] = {
    CircuitState.CLOSED: frozenset({CircuitState.OPEN}),
    CircuitState.OPEN: frozenset({CircuitState.HALF_OPEN}),
    CircuitState.HALF_OPEN: frozenset({CircuitState.CLOSED, CircuitState.OPEN}),
}


def can_transition(table: Mapping[S, frozenset[S]], current: S, target: S) -> bool:
    """Return ``True`` when ``current -> target`` is listed in ``table``."""
    return target in table.get(current, frozenset())


def assert_transition(
    table: Mapping[S, frozenset[S]], current: S, target: S, *, entity: str
) -> None:
    """Raise :class:`InvalidTransitionError` unless ``current -> target`` is listed in ``table``."""
    if not can_transition(table, current, target):
        raise InvalidTransitionError(
            entity=entity, current=str(current.value), target=str(target.value)
        )


def is_terminal(table: Mapping[S, frozenset[S]], state: S) -> bool:
    """A state is terminal when no transition leaves it."""
    return not table.get(state, frozenset())
