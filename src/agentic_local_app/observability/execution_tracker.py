"""``ExecutionTracker`` — the runtime snapshot of §4.1, at two levels (§3.19 ; ADR-006, ADR-012,
ADR-013, ADR-015).

The tracker is a **non-critical** subscriber of the EventBus (``execution_tracker``, registered
right after the AuditLog, ADR-015). It keeps one in-memory :class:`RuntimeSnapshot` source per
session, but that cache is never the truth: on every event it re-reads the records the event
concerns from the ``ConversationStore`` (the publisher persisted them *before* publishing, ADR-015),
and :meth:`ExecutionTracker.rebuild` reconstructs everything from the store. The phase 10 tests
check ``snapshot(sid) == rebuild(sid)`` after every transition.

What comes from the store: the session (budget limits and consumed counters, ADR-012), the
conversation chain and the current conversation (``session.current_conversation_id``), the current
cycle (``conversation.current_cycle_id``), the current plan (``conversation.current_plan_id``) with
its tasks — whose counters are **recomputed from the task records** — and the RUNNING tasks of the
session. What comes from the events only: the model interaction (HTTP statuses are not persisted)
and the position of the last event; a cold tracker rebuilds them as far as the store allows
(``MessageRecord`` types and validation status, the audit trail for the last event).

Contract of the ``message.*`` payloads read here (phases 7 and 9 publish them):

- ``message.outbound`` / ``message.retransmitted``: ``{"message_type": str, "message_id": str,
  "post_status": int | None, "size_bytes": int}`` (+ ``"retransmission_of": str`` for the latter);
- ``message.inbound``: ``{"message_type": str, "message_id": str, "get_status": int | None,
  "validation_status": "valid", "size_bytes": int}``;
- ``message.rejected``: ``{"message_type": str | None, "message_id": str | None,
  "get_status": int | None, "validation_status": "invalid", "error_code": str}``.

Time-derived values (``consumed_duration_ms``, ``snapshot_at``) are computed when the snapshot is
read, with the injected ``Clock``: the snapshot is exact "at any instant" (§4).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict

from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    PlanRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
    ExecutionPolicy,
    MessageDirection,
    MessageType,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
    TaskType,
)
from agentic_local_app.domain.transitions import FAILED_TASK_STATES
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "TRACKER_SUBSCRIBER_NAME",
    "BudgetView",
    "ConversationSummary",
    "ConversationView",
    "CycleView",
    "ExecutionTracker",
    "ModelInteractionView",
    "PlanView",
    "RuntimeSnapshot",
    "SessionView",
    "TaskView",
]

#: Name under which the tracker registers on the bus (ADR-015 order: second, after ``audit_log``).
TRACKER_SUBSCRIBER_NAME = "execution_tracker"

#: Everything but the live output chunks (volume, ADR-018): they never change a state.
_TRACKED_EVENT_TYPES: frozenset[EventType] = frozenset(EventType) - {EventType.TASK_OUTPUT}

#: Events after which the cycle / plan / tasks of the session are re-read even when the event
#: carries no ``cycle_id`` / ``plan_id`` / ``task_id`` and the conversation pointers did not move.
_EXECUTION_EVENT_TYPES: frozenset[EventType] = frozenset(
    {
        EventType.CYCLE_STARTED,
        EventType.CYCLE_ENDED,
        EventType.PLAN_RECEIVED,
        EventType.PLAN_STATE_CHANGED,
        EventType.TASK_STATE_CHANGED,
        EventType.ROTATION_STARTED,
        EventType.ROTATION_COMPLETED,
        EventType.ROTATION_FAILED,
        EventType.BUDGET_EXCEEDED,
        EventType.INTERRUPTION_REQUESTED,
        EventType.INTERRUPTION_COMPLETED,
        EventType.RECOVERY_STARTED,
        EventType.RECOVERY_ACTION,
        EventType.RECOVERY_COMPLETED,
    }
)

_OUTBOUND_MESSAGE_EVENTS: frozenset[EventType] = frozenset(
    {EventType.MESSAGE_OUTBOUND, EventType.MESSAGE_RETRANSMITTED}
)
_INBOUND_MESSAGE_EVENTS: frozenset[EventType] = frozenset(
    {EventType.MESSAGE_INBOUND, EventType.MESSAGE_REJECTED}
)


# ------------------------------------------------------------------------------------------------
# Views (§4.1) — frozen pydantic models, JSON-serialisable for the API (ADR-018)
# ------------------------------------------------------------------------------------------------
class _View(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BudgetView(_View):
    """``session_budget`` of §4.1: the three limits and the three consumed values (ADR-012)."""

    max_cycles: int
    max_plans: int
    max_total_duration_ms: int
    consumed_cycles: int
    consumed_plans: int
    consumed_duration_ms: int


class SessionView(_View):
    """Session level (ADR-006 / ADR-012)."""

    session_id: str
    status: SessionState
    goal: str
    auto_close_on_final_answer: bool
    session_budget: BudgetView
    rotations_count: int
    current_conversation_id: str | None
    started_at: datetime | None
    ended_at: datetime | None
    interrupted_at: datetime | None
    created_at: datetime
    updated_at: datetime


class ConversationView(_View):
    """Conversation level of §4.1 (+ ``context_bytes``, ADR-013)."""

    conversation_id: str
    parent_conversation_id: str | None
    status: ConversationState
    auto_close_on_final_answer: bool
    context_window_state: ContextWindowState
    context_bytes: int
    last_model_response_state: str
    current_cycle_id: str | None
    current_plan_id: str | None
    last_completed_plan_id: str | None
    final_answer_received: bool
    interrupted_at: datetime | None
    session_budget: BudgetView
    created_at: datetime
    updated_at: datetime


class ConversationSummary(_View):
    """One link of the conversation chain (interruptions ADR-006, rotations ADR-014)."""

    conversation_id: str
    parent_conversation_id: str | None
    status: ConversationState


class CycleView(_View):
    """Cycle level of §4.1."""

    cycle_id: str
    cycle_type: CycleType
    status: CycleState
    started_at: datetime
    ended_at: datetime | None
    retry_count: int
    conversation_id: str


class PlanView(_View):
    """Plan level of §4.1. Counters are recomputed from the task records when there are any."""

    plan_id: str
    plan_type: PlanType
    objective: str
    execution_policy: ExecutionPolicy
    max_parallel_workers: int
    status: PlanState
    stop_reason: str | None
    task_count: int
    completed_task_count: int
    failed_task_count: int
    skipped_task_count: int
    cancelled_task_count: int
    interrupted_task_count: int
    started_at: datetime | None
    ended_at: datetime | None


class TaskView(_View):
    """Task level of §4.1 (+ ``timed_out`` ADR-008 and ``reason`` ADR-009)."""

    task_id: str
    plan_id: str
    type: TaskType
    cmd: str | None
    status: TaskState
    critical: bool
    continue_on_error: bool
    stop_plan_on_failure: bool
    stop_plan_on_success: bool
    depends_on: list[str]
    resource_lock: str | None
    max_output_bytes: int | None
    attempt_count: int
    exit_code: int | None
    truncated: bool
    original_size_bytes: int | None
    started_at: datetime | None
    ended_at: datetime | None
    duration_ms: int | None
    timed_out: bool
    reason: str | None


class ModelInteractionView(_View):
    """Model interaction level of §4.1, per session, fed by the ``message.*`` events.

    ``correction_attempt`` / ``correction_max_attempts`` say whether a correction is **in flight**
    (ADR-023): non-zero while the application is waiting for a corrected reply, back to zero as
    soon as a valid message comes in. They are derived, never persisted.
    """

    last_outbound_message_type: str | None = None
    last_inbound_message_type: str | None = None
    last_post_status: int | None = None
    last_get_status: int | None = None
    last_protocol_validation_status: str | None = None
    correction_attempt: int = 0
    correction_max_attempts: int = 0


class RuntimeSnapshot(_View):
    """The consistent runtime snapshot of §4 for one session."""

    session: SessionView
    conversation: ConversationView | None
    conversations: list[ConversationSummary]
    cycle: CycleView | None
    plan: PlanView | None
    tasks: list[TaskView]
    running_task_ids: list[str]
    model_interaction: ModelInteractionView
    last_event_type: str | None
    last_event_sequence: int | None
    snapshot_at: datetime


# ------------------------------------------------------------------------------------------------
# Cache
# ------------------------------------------------------------------------------------------------
@dataclass
class _Tracked:
    """Per-session cache: the records (from the store) and the event-derived state."""

    session: SessionRecord
    conversations: list[ConversationRecord]
    conversation: ConversationRecord | None
    cycle: CycleRecord | None
    plan: PlanRecord | None
    tasks: list[TaskRecord]
    running: list[TaskRecord]
    interaction: ModelInteractionView
    last_event_type: str | None
    last_event_sequence: int | None
    events_seen: int


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


class ExecutionTracker:
    """Instant visibility on session, conversation, cycle, plan, tasks and model interaction."""

    def __init__(self, store: ConversationStore, clock: Clock) -> None:
        self._store = store
        self._clock = clock
        self._tracked: dict[str, _Tracked] = {}

    # ------------------------------------------------------------------ bus ----------------
    def subscribe(self, bus: EventBus) -> None:
        """Register as the non-critical subscriber ``execution_tracker`` (all but ``task.output``)."""
        bus.subscribe(self.handle, name=TRACKER_SUBSCRIBER_NAME, event_types=_TRACKED_EVENT_TYPES)

    # ------------------------------------------------------------------ events -------------
    def handle(self, event: Event) -> None:
        """Refresh the cache of ``event.session_id`` from the store and from the event.

        A store failure invalidates the cache entry (the next ``snapshot`` rebuilds it) and
        propagates: the bus isolates it because this subscriber is not critical.
        """
        if event.event_type not in _TRACKED_EVENT_TYPES:
            return
        session_id = event.session_id
        tracked = self._tracked.get(session_id)
        try:
            tracked = (
                self._load(session_id, previous=None)
                if tracked is None
                else self._refresh(tracked, event)
            )
        except Exception:
            self._tracked.pop(session_id, None)
            raise
        if tracked is None:
            # unknown session: nothing persisted to show (the publisher violated ADR-015)
            self._tracked.pop(session_id, None)
            return
        tracked.events_seen += 1
        tracked.last_event_type = event.event_type.value
        tracked.last_event_sequence = self._sequence_of(event, tracked)
        if event.event_type in _OUTBOUND_MESSAGE_EVENTS | _INBOUND_MESSAGE_EVENTS | {
            EventType.CORRECTION_REQUESTED
        }:
            tracked.interaction = _apply_message(tracked.interaction, event)
        self._tracked[session_id] = tracked

    # ------------------------------------------------------------------ reads --------------
    def snapshot(self, session_id: str) -> RuntimeSnapshot:
        """The snapshot of ``session_id`` (rebuilt from the store when not cached). ``KeyError`` if unknown."""
        tracked = self._tracked.get(session_id)
        if tracked is None:
            tracked = self._load(session_id, previous=None)
            if tracked is None:
                raise KeyError(f"unknown session: {session_id}")
            self._tracked[session_id] = tracked
        return self._assemble(tracked)

    def rebuild(self, session_id: str) -> RuntimeSnapshot:
        """Force a full reconstruction from the store; the event-fed interaction state is kept."""
        tracked = self._load(session_id, previous=self._tracked.get(session_id))
        if tracked is None:
            self._tracked.pop(session_id, None)
            raise KeyError(f"unknown session: {session_id}")
        self._tracked[session_id] = tracked
        return self._assemble(tracked)

    # ------------------------------------------------------------------ store reads --------
    def _load(self, session_id: str, *, previous: _Tracked | None) -> _Tracked | None:
        """Everything from the store. ``previous`` keeps the event-derived state across a rebuild."""
        session = self._store.get_session(session_id)
        if session is None:
            return None
        conversations = self._store.list_conversations(session_id)
        conversation = _current(conversations, session.current_conversation_id)
        cycle, plan, tasks, running = self._execution_records(session_id, conversation)
        if previous is not None:
            interaction = previous.interaction
            last_type, last_sequence, seen = (
                previous.last_event_type,
                previous.last_event_sequence,
                previous.events_seen,
            )
        else:
            # cold start: the audit trail gives the position, the messages the interaction
            interaction = self._interaction_from_store(conversation)
            last_audit = self._store.get_last_audit_event(session_id)
            last_type = last_audit.event_type if last_audit is not None else None
            last_sequence = last_audit.sequence if last_audit is not None else None
            seen = 0  # events handled by this tracker (fallback when no audit trail exists)
        return _Tracked(
            session=session,
            conversations=conversations,
            conversation=conversation,
            cycle=cycle,
            plan=plan,
            tasks=tasks,
            running=running,
            interaction=interaction,
            last_event_type=last_type,
            last_event_sequence=last_sequence,
            events_seen=seen,
        )

    def _refresh(self, tracked: _Tracked, event: Event) -> _Tracked | None:
        """Re-read what ``event`` may have changed: always the session and the conversation chain;
        the cycle / plan / tasks when the event concerns them or the conversation pointers moved."""
        session = self._store.get_session(event.session_id)
        if session is None:
            return None
        conversations = self._store.list_conversations(event.session_id)
        conversation = _current(conversations, session.current_conversation_id)
        if _execution_may_have_changed(tracked.conversation, conversation, event):
            tracked.cycle, tracked.plan, tracked.tasks, tracked.running = self._execution_records(
                event.session_id, conversation
            )
        tracked.session = session
        tracked.conversations = conversations
        tracked.conversation = conversation
        return tracked

    def _execution_records(
        self, session_id: str, conversation: ConversationRecord | None
    ) -> tuple[CycleRecord | None, PlanRecord | None, list[TaskRecord], list[TaskRecord]]:
        cycle = plan = None
        tasks: list[TaskRecord] = []
        if conversation is not None and conversation.current_cycle_id is not None:
            cycle = self._store.get_cycle(conversation.current_cycle_id)
        if conversation is not None and conversation.current_plan_id is not None:
            plan = self._store.get_plan(session_id, conversation.current_plan_id)
            if plan is not None:
                tasks = self._store.list_tasks(session_id, plan_id=plan.plan_id)
        running = self._store.list_tasks(session_id, statuses=[TaskState.RUNNING])
        return cycle, plan, tasks, running

    def _interaction_from_store(
        self, conversation: ConversationRecord | None
    ) -> ModelInteractionView:
        """What the persisted messages tell (types, validation); HTTP statuses are not persisted."""
        if conversation is None:
            return ModelInteractionView()
        messages = self._store.list_messages(conversation.conversation_id)
        outbound = [m for m in messages if m.direction is MessageDirection.OUTBOUND]
        inbound = [m for m in messages if m.direction is MessageDirection.INBOUND]
        attempt, max_attempts = _correction_in_flight(messages)
        return ModelInteractionView(
            last_outbound_message_type=outbound[-1].message_type.value if outbound else None,
            last_inbound_message_type=inbound[-1].message_type.value if inbound else None,
            last_protocol_validation_status=inbound[-1].validation_status if inbound else None,
            correction_attempt=attempt,
            correction_max_attempts=max_attempts,
        )

    def _sequence_of(self, event: Event, tracked: _Tracked) -> int:
        """The payload's ``sequence`` if present, else the audit sequence (the AuditLog ran first,
        ADR-015), else the number of events this tracker has seen for the session."""
        carried = _int_or_none(event.payload.get("sequence"))
        if carried is not None:
            return carried
        last_audit = self._store.get_last_audit_event(event.session_id)
        if last_audit is not None:
            return last_audit.sequence
        return tracked.events_seen

    # ------------------------------------------------------------------ assembly -----------
    def _assemble(self, tracked: _Tracked) -> RuntimeSnapshot:
        now = self._clock.now()
        budget = _budget_view(tracked.session, now)
        session = tracked.session
        return RuntimeSnapshot(
            session=SessionView(
                session_id=session.session_id,
                status=session.status,
                goal=session.goal,
                auto_close_on_final_answer=session.auto_close_on_final_answer,
                session_budget=budget,
                rotations_count=session.rotations_count,
                current_conversation_id=session.current_conversation_id,
                started_at=session.started_at,
                ended_at=session.ended_at,
                interrupted_at=session.interrupted_at,
                created_at=session.created_at,
                updated_at=session.updated_at,
            ),
            conversation=(
                _conversation_view(tracked.conversation, budget)
                if tracked.conversation is not None
                else None
            ),
            conversations=[
                ConversationSummary(
                    conversation_id=c.conversation_id,
                    parent_conversation_id=c.parent_conversation_id,
                    status=c.status,
                )
                for c in tracked.conversations
            ],
            cycle=_cycle_view(tracked.cycle) if tracked.cycle is not None else None,
            plan=_plan_view(tracked.plan, tracked.tasks) if tracked.plan is not None else None,
            tasks=[_task_view(t) for t in tracked.tasks],
            running_task_ids=[t.task_id for t in tracked.running],
            model_interaction=tracked.interaction,
            last_event_type=tracked.last_event_type,
            last_event_sequence=tracked.last_event_sequence,
            snapshot_at=now,
        )


# ------------------------------------------------------------------------------------------------
# Pure helpers
# ------------------------------------------------------------------------------------------------
def _current(
    conversations: list[ConversationRecord], current_id: str | None
) -> ConversationRecord | None:
    if current_id is None:
        return None
    for conversation in conversations:
        if conversation.conversation_id == current_id:
            return conversation
    return None


def _execution_may_have_changed(
    before: ConversationRecord | None, after: ConversationRecord | None, event: Event
) -> bool:
    if event.cycle_id is not None or event.plan_id is not None or event.task_id is not None:
        return True
    if event.event_type in _EXECUTION_EVENT_TYPES:
        return True
    return _pointers(before) != _pointers(after)


def _pointers(conversation: ConversationRecord | None) -> tuple[str | None, str | None, str | None]:
    if conversation is None:
        return (None, None, None)
    return (
        conversation.conversation_id,
        conversation.current_cycle_id,
        conversation.current_plan_id,
    )


def _correction_in_flight(messages: list[MessageRecord]) -> tuple[int, int]:
    """``(attempt, max_attempts)`` of the correction the conversation is waiting on, or ``(0, 0)``.

    Derived from the persisted messages (ADR-023 keeps no column): a correction is in flight when
    the last outbound message is a ``protocol_correction_request`` and no valid reply followed it.
    """
    for record in reversed(messages):
        if record.direction is MessageDirection.INBOUND:
            if record.validation_status != "invalid":
                return 0, 0
            continue
        if record.message_type is not MessageType.PROTOCOL_CORRECTION_REQUEST:
            return 0, 0
        content = record.payload.get("content", {})
        if not isinstance(content, dict):
            return 0, 0
        return _int_or_none(content.get("attempt")) or 0, (
            _int_or_none(content.get("max_attempts")) or 0
        )
    return 0, 0


def _apply_message(view: ModelInteractionView, event: Event) -> ModelInteractionView:
    payload = event.payload
    message_type = _str_or_none(payload.get("message_type"))
    if event.event_type is EventType.CORRECTION_REQUESTED:
        # ADR-023: a correction is in flight until a valid message comes back
        return view.model_copy(
            update={
                "correction_attempt": _int_or_none(payload.get("attempt")) or 0,
                "correction_max_attempts": _int_or_none(payload.get("max_attempts")) or 0,
            }
        )
    if event.event_type is EventType.MESSAGE_INBOUND:
        view = view.model_copy(update={"correction_attempt": 0, "correction_max_attempts": 0})
    if event.event_type in _OUTBOUND_MESSAGE_EVENTS:
        return view.model_copy(
            update={
                "last_outbound_message_type": message_type or view.last_outbound_message_type,
                "last_post_status": _int_or_none(payload.get("post_status")),
            }
        )
    default_status = "valid" if event.event_type is EventType.MESSAGE_INBOUND else "invalid"
    return view.model_copy(
        update={
            "last_inbound_message_type": message_type or view.last_inbound_message_type,
            "last_get_status": _int_or_none(payload.get("get_status")),
            "last_protocol_validation_status": _str_or_none(payload.get("validation_status"))
            or default_status,
        }
    )


def _budget_view(session: SessionRecord, now: datetime) -> BudgetView:
    """Limits and consumed values (ADR-012). The duration runs from ``started_at`` to ``ended_at``
    when the session is over, to ``now`` otherwise; ``0`` before the first ``RUNNING``."""
    consumed_duration_ms = 0
    if session.started_at is not None:
        end = session.ended_at if session.ended_at is not None else now
        consumed_duration_ms = max(0, (end - session.started_at) // timedelta(milliseconds=1))
    return BudgetView(
        max_cycles=session.budget.max_cycles,
        max_plans=session.budget.max_plans,
        max_total_duration_ms=session.budget.max_total_duration_ms,
        consumed_cycles=session.consumed_cycles,
        consumed_plans=session.consumed_plans,
        consumed_duration_ms=consumed_duration_ms,
    )


def _conversation_view(conversation: ConversationRecord, budget: BudgetView) -> ConversationView:
    return ConversationView(
        conversation_id=conversation.conversation_id,
        parent_conversation_id=conversation.parent_conversation_id,
        status=conversation.status,
        auto_close_on_final_answer=conversation.auto_close_on_final_answer,
        context_window_state=conversation.context_window_state,
        context_bytes=conversation.context_bytes,
        last_model_response_state=conversation.last_model_response_state,
        current_cycle_id=conversation.current_cycle_id,
        current_plan_id=conversation.current_plan_id,
        last_completed_plan_id=conversation.last_completed_plan_id,
        final_answer_received=conversation.final_answer_received,
        interrupted_at=conversation.interrupted_at,
        session_budget=budget,
        created_at=conversation.created_at,
        updated_at=conversation.updated_at,
    )


def _cycle_view(cycle: CycleRecord) -> CycleView:
    return CycleView(
        cycle_id=cycle.cycle_id,
        cycle_type=cycle.cycle_type,
        status=cycle.status,
        started_at=cycle.started_at,
        ended_at=cycle.ended_at,
        retry_count=cycle.retry_count,
        conversation_id=cycle.conversation_id,
    )


def _plan_view(plan: PlanRecord, tasks: list[TaskRecord]) -> PlanView:
    """The task records are the truth: with tasks, every counter is recomputed from them
    (``TIMED_OUT`` counts as failed, ADR-008); without tasks the record's counters are kept."""
    if tasks:
        by_status = Counter(task.status for task in tasks)
        counters = {
            "task_count": len(tasks),
            "completed_task_count": by_status[TaskState.COMPLETED],
            "failed_task_count": sum(by_status[state] for state in FAILED_TASK_STATES),
            "skipped_task_count": by_status[TaskState.SKIPPED],
            "cancelled_task_count": by_status[TaskState.CANCELLED],
            "interrupted_task_count": by_status[TaskState.INTERRUPTED],
        }
    else:
        counters = {
            "task_count": plan.task_count,
            "completed_task_count": plan.completed_task_count,
            "failed_task_count": plan.failed_task_count,
            "skipped_task_count": plan.skipped_task_count,
            "cancelled_task_count": plan.cancelled_task_count,
            "interrupted_task_count": plan.interrupted_task_count,
        }
    return PlanView(
        plan_id=plan.plan_id,
        plan_type=plan.plan_type,
        objective=plan.objective,
        execution_policy=plan.execution_policy,
        max_parallel_workers=plan.max_parallel_workers,
        status=plan.status,
        stop_reason=plan.stop_reason,
        started_at=plan.started_at,
        ended_at=plan.ended_at,
        **counters,
    )


def _task_view(task: TaskRecord) -> TaskView:
    return TaskView(
        task_id=task.task_id,
        plan_id=task.plan_id,
        type=task.type,
        cmd=task.cmd,
        status=task.status,
        critical=task.critical,
        continue_on_error=task.continue_on_error,
        stop_plan_on_failure=task.stop_plan_on_failure,
        stop_plan_on_success=task.stop_plan_on_success,
        depends_on=list(task.depends_on),
        resource_lock=task.resource_lock,
        max_output_bytes=task.max_output_bytes,
        attempt_count=task.attempt_count,
        exit_code=task.exit_code,
        truncated=task.truncated,
        original_size_bytes=task.original_size_bytes,
        started_at=task.started_at,
        ended_at=task.ended_at,
        duration_ms=task.duration_ms,
        timed_out=task.timed_out,
        reason=task.reason,
    )
