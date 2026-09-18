"""``RecoveryCoordinator`` — the restart policy of ADR-016 (spec §3.18, §7.5, §17.4).

Run **synchronously at start-up**, before any request is accepted (ADR-016 §4). It re-reads the
persisted state — the last stable checkpoint is simply the store, ADR-015 §5 — and applies the
policy table of ADR-016 §2 **in this order**, every step persisted then published (``recovery.*``
events and the usual ``*.state_changed`` events with ``reason = "restart"``):

1. tasks ``RUNNING`` → orphan terminated when a platform adapter is given and the record carries a
   ``pid`` (the adapter refuses reused pids, ADR-016 §1) → ``INTERRUPTED`` (``reason = restart``),
   never re-executed, no blob (nothing was captured);
2. tasks ``PENDING`` / ``WAITING_DEPENDENCY`` of a ``RUNNING`` or ``PENDING`` plan → ``INTERRUPTED``;
3. plans ``RUNNING`` / ``PENDING`` → ``INTERRUPTED`` (``stop_reason = restart``), counters recomputed
   from the task records;
4. cycles ``RUNNING`` → ``INTERRUPTED``, except the cycle of a **resumable** conversation;
5. conversations ``RUNNING_PLAN`` / ``ACTIVE`` / ``ROTATING`` → ``INTERRUPTED`` through
   ``ConversationLifecycleManager.interrupt_conversation`` (the same path as a user interrupt,
   ADR-006, nothing sent to the model); a ``WAITING_MODEL_RESPONSE`` conversation whose last message
   is a persisted, unanswered ``user_request`` / ``execution_result`` is **resumable**: it is left
   untouched and listed so that ``ConversationManager.resume_session`` replays the POST when it was
   not confirmed and does the GET first (§7.5); a ``WAITING_MODEL_RESPONSE`` conversation that was
   waiting for a ``context_resume_ack`` (rotation in flight) is interrupted with its parent;
6. sessions ``RUNNING`` / ``INTERRUPTING``: the conversation ended ``INTERRUPTED`` (or never left
   ``NEW``) → ``READY`` (through ``INTERRUPTING``); resumable → stays ``RUNNING``; conversation
   ``FAILED`` → ``FAILED``; conversation ``COMPLETED`` / ``WAITING_USER`` / ``CLOSED`` (crash after
   the final answer) → ``COMPLETED``, the conversation finished per ``auto_close_on_final_answer``.

Events: one ``recovery.started`` and one ``recovery.completed`` per session touched — or one pair
for the pseudo session ``"*"`` when nothing is open (an empty store is an audited no-op) — and one
``recovery.action`` per action. Terminal entities are never touched: a second run is a no-op.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    PlanRecord,
    Record,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    MessageDirection,
    MessageType,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.domain.transitions import (
    CYCLE_TRANSITIONS,
    FAILED_TASK_STATES,
    PLAN_TRANSITIONS,
    TASK_TRANSITIONS,
    assert_transition,
)
from agentic_local_app.execution.plan_runner import PLAN_ENTITY, TASK_ENTITY
from agentic_local_app.execution.platform import PlatformAdapter
from agentic_local_app.interruption.handler import CYCLE_ENTITY
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "NO_SESSION",
    "ORPHAN_TERMINATED_REASON",
    "RESTART_REASON",
    "RESUMABLE_REASON",
    "RecoveryAction",
    "RecoveryCoordinator",
    "RecoveryReport",
]

#: ``reason`` of every transition applied by the recovery (ADR-009 §3, ADR-016 §2).
RESTART_REASON = "restart"
#: ``reason`` of the action recorded for a conversation left for ``resume_session``.
RESUMABLE_REASON = "resumable"
#: ``reason`` of the action recorded when an orphan process was signalled (ADR-016 §1).
ORPHAN_TERMINATED_REASON = "orphan_terminated"
#: ``session_id`` of the ``recovery.*`` events when no session is touched (audited no-op).
NO_SESSION = "*"

_OPEN_PLAN_STATES: frozenset[PlanState] = frozenset({PlanState.PENDING, PlanState.RUNNING})
_OPEN_TASK_STATES: frozenset[TaskState] = frozenset(
    {TaskState.PENDING, TaskState.WAITING_DEPENDENCY}
)
_OPEN_SESSION_STATES: frozenset[SessionState] = frozenset(
    {SessionState.RUNNING, SessionState.INTERRUPTING}
)
_ACTIVE_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
        ConversationState.ROTATING,
    }
)
_RESUMABLE_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.USER_REQUEST, MessageType.EXECUTION_RESULT}
)
_FINISHED_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {ConversationState.COMPLETED, ConversationState.WAITING_USER, ConversationState.CLOSED}
)
_ONE_MS = timedelta(milliseconds=1)

R = TypeVar("R", bound=Record)


class RecoveryAction(BaseModel):
    """One step of the recovery: which entity went from which state to which, and why."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    entity: str  # task | plan | cycle | conversation | session
    id: str
    from_state: str
    to_state: str
    reason: str
    session_id: str
    details: dict[str, Any] = Field(default_factory=dict)


class RecoveryReport(BaseModel):
    """What :meth:`RecoveryCoordinator.recover` did (ADR-016 §3): exposed by the CLI and ``/health``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    actions: list[RecoveryAction] = Field(default_factory=list)
    orphans_terminated: list[int] = Field(default_factory=list)
    sessions_ready: list[str] = Field(default_factory=list)
    sessions_resumable: list[str] = Field(default_factory=list)
    started_at: datetime
    ended_at: datetime
    sessions_failed: list[str] = Field(default_factory=list)
    sessions_completed: list[str] = Field(default_factory=list)

    @property
    def elapsed_ms(self) -> int:
        return max(0, (self.ended_at - self.started_at) // _ONE_MS)

    def summary(self) -> dict[str, Any]:
        """The ``recovery.completed`` payload."""
        return {
            "actions": len(self.actions),
            "orphans_terminated": list(self.orphans_terminated),
            "sessions_ready": list(self.sessions_ready),
            "sessions_resumable": list(self.sessions_resumable),
            "sessions_failed": list(self.sessions_failed),
            "sessions_completed": list(self.sessions_completed),
            "elapsed_ms": self.elapsed_ms,
        }


@dataclass
class _Findings:
    """What the store holds for one session before the recovery acts on it."""

    running_tasks: int = 0
    open_plans: int = 0
    running_cycles: int = 0
    active_conversations: int = 0
    open_sessions: int = 0

    def as_payload(self) -> dict[str, int]:
        return {
            "running_tasks": self.running_tasks,
            "open_plans": self.open_plans,
            "running_cycles": self.running_cycles,
            "active_conversations": self.active_conversations,
            "open_sessions": self.open_sessions,
        }

    @property
    def empty(self) -> bool:
        return not any(self.as_payload().values())


@dataclass
class _Run:
    """The mutable state of one ``recover`` call."""

    started_at: datetime
    actions: list[RecoveryAction] = field(default_factory=list)
    orphans: list[int] = field(default_factory=list)
    ready: list[str] = field(default_factory=list)
    resumable: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    resumable_conversations: set[str] = field(default_factory=set)


def _apply(record: R, changes: dict[str, Any]) -> R:
    """A new, fully validated record (``model_copy`` validates nothing)."""
    data = record.model_dump()
    data.update(changes)
    return type(record).model_validate(data)


def _counters(tasks: Sequence[TaskRecord]) -> dict[str, int]:
    statuses = [task.status for task in tasks]
    return {
        "task_count": len(statuses),
        "completed_task_count": statuses.count(TaskState.COMPLETED),
        "failed_task_count": sum(1 for status in statuses if status in FAILED_TASK_STATES),
        "skipped_task_count": statuses.count(TaskState.SKIPPED),
        "cancelled_task_count": statuses.count(TaskState.CANCELLED),
        "interrupted_task_count": statuses.count(TaskState.INTERRUPTED),
    }


class RecoveryCoordinator:
    """Apply the ADR-016 policy table to the persisted state at start-up."""

    def __init__(
        self,
        store: ConversationStore,
        lifecycle: ConversationLifecycleManager,
        bus: EventBus,
        clock: Clock,
        ids: IdGenerator,
        config: AppConfig,
        *,
        platform: PlatformAdapter | None = None,
    ) -> None:
        self._store = store
        self._lifecycle = lifecycle
        self._bus = bus
        self._clock = clock
        self._ids = ids  # no identifier is generated here (kept for the wiring symmetry)
        self._config = config
        self._platform = platform

    # ------------------------------------------------------------------ public -------------
    def recover(self) -> RecoveryReport:
        """Run the policy table once; idempotent (terminal entities are never touched)."""
        run = _Run(started_at=self._clock.now())
        findings = self._collect()
        touched = sorted(findings)
        for session_id in touched or [NO_SESSION]:
            payload = findings.get(session_id, _Findings()).as_payload()
            self._publish(
                EventType.RECOVERY_STARTED,
                session_id,
                payload={"findings": payload, "sessions_found": len(touched)},
            )

        self._interrupt_running_tasks(run)
        self._interrupt_pending_tasks_and_plans(run)
        self._settle_conversations(run)
        self._settle_sessions(run)

        report = RecoveryReport(
            actions=list(run.actions),
            orphans_terminated=list(run.orphans),
            sessions_ready=list(run.ready),
            sessions_resumable=list(run.resumable),
            sessions_failed=list(run.failed),
            sessions_completed=list(run.completed),
            started_at=run.started_at,
            ended_at=self._clock.now(),
        )
        for session_id in touched or [NO_SESSION]:
            self._publish(EventType.RECOVERY_COMPLETED, session_id, payload=report.summary())
        return report

    # ------------------------------------------------------------------ findings -----------
    def _collect(self) -> dict[str, _Findings]:
        findings: dict[str, _Findings] = {}

        def of(session_id: str) -> _Findings:
            entry = findings.get(session_id)
            if entry is None:
                entry = findings[session_id] = _Findings()
            return entry

        for task in self._store.find_tasks_in_states([TaskState.RUNNING]):
            of(task.session_id).running_tasks += 1
        for plan in self._store.find_plans_in_states(_OPEN_PLAN_STATES):
            of(plan.session_id).open_plans += 1
        for conversation in self._store.find_conversations_in_states(_ACTIVE_CONVERSATION_STATES):
            of(conversation.session_id).active_conversations += 1
        for session in self._open_sessions():
            of(session.session_id).open_sessions += 1
            for conversation in self._store.list_conversations(session.session_id):
                for cycle in self._store.list_cycles(conversation.conversation_id):
                    if cycle.status is CycleState.RUNNING:
                        of(session.session_id).running_cycles += 1
        return {sid: entry for sid, entry in findings.items() if not entry.empty}

    def _open_sessions(self) -> list[SessionRecord]:
        sessions: list[SessionRecord] = []
        offset = 0
        page = 500
        while True:
            batch = self._store.list_sessions(
                statuses=_OPEN_SESSION_STATES, limit=page, offset=offset
            )
            sessions.extend(batch)
            if len(batch) < page:
                return sessions
            offset += page

    # ------------------------------------------------------------------ 1. running tasks ---
    def _interrupt_running_tasks(self, run: _Run) -> None:
        for task in self._store.find_tasks_in_states([TaskState.RUNNING]):
            plan = self._store.get_plan(task.session_id, task.plan_id)
            if self._platform is not None and task.pid is not None and task.started_at is not None:
                if self._platform.terminate_orphan(
                    task.pid, task.process_group_id, task.started_at
                ):
                    run.orphans.append(task.pid)
                    self._record(
                        run,
                        entity="task",
                        entity_id=task.task_id,
                        from_state=task.status.value,
                        to_state=task.status.value,
                        reason=ORPHAN_TERMINATED_REASON,
                        session_id=task.session_id,
                        conversation_id=task.conversation_id,
                        cycle_id=plan.cycle_id if plan is not None else None,
                        plan_id=task.plan_id,
                        task_id=task.task_id,
                        details={"pid": task.pid, "process_group_id": task.process_group_id},
                    )
            self._interrupt_task(run, task, plan)

    # ------------------------------------------------------------------ 2/3. plans ---------
    def _interrupt_pending_tasks_and_plans(self, run: _Run) -> None:
        plans = self._store.find_plans_in_states(_OPEN_PLAN_STATES)
        for plan in plans:
            for task in self._store.list_tasks(plan.session_id, plan_id=plan.plan_id):
                if task.status in _OPEN_TASK_STATES:
                    self._interrupt_task(run, task, plan)
        for plan in plans:
            self._interrupt_plan(run, plan)

    def _interrupt_task(self, run: _Run, task: TaskRecord, plan: PlanRecord | None) -> None:
        assert_transition(TASK_TRANSITIONS, task.status, TaskState.INTERRUPTED, entity=TASK_ENTITY)
        now = self._clock.now()
        fields: dict[str, Any] = {
            "status": TaskState.INTERRUPTED,
            "reason": RESTART_REASON,
            "ended_at": now,
            "updated_at": now,
        }
        if task.status is TaskState.RUNNING and task.started_at is not None:
            fields["duration_ms"] = max(0, (now - task.started_at) // _ONE_MS)
        record = _apply(task, fields)
        with self._store.transaction():
            self._store.save_task(record)
        payload = state_change_payload(task.status.value, record.status.value, RESTART_REASON)
        if task.status is TaskState.RUNNING:
            payload["exit_code"] = record.exit_code
            payload["duration_ms"] = record.duration_ms
            payload["timed_out"] = record.timed_out
            payload["truncated"] = record.truncated
        cycle_id = plan.cycle_id if plan is not None else None
        self._publish(
            EventType.TASK_STATE_CHANGED,
            task.session_id,
            conversation_id=task.conversation_id,
            cycle_id=cycle_id,
            plan_id=task.plan_id,
            task_id=task.task_id,
            payload=payload,
            timestamp=now,
        )
        self._record(
            run,
            entity="task",
            entity_id=task.task_id,
            from_state=task.status.value,
            to_state=record.status.value,
            reason=RESTART_REASON,
            session_id=task.session_id,
            conversation_id=task.conversation_id,
            cycle_id=cycle_id,
            plan_id=task.plan_id,
            task_id=task.task_id,
        )

    def _interrupt_plan(self, run: _Run, plan: PlanRecord) -> None:
        current = self._store.get_plan(plan.session_id, plan.plan_id) or plan
        assert_transition(
            PLAN_TRANSITIONS, current.status, PlanState.INTERRUPTED, entity=PLAN_ENTITY
        )
        now = self._clock.now()
        tasks = self._store.list_tasks(current.session_id, plan_id=current.plan_id)
        record = _apply(
            current,
            {
                "status": PlanState.INTERRUPTED,
                "stop_reason": RESTART_REASON,
                "ended_at": now,
                "updated_at": now,
                **_counters(tasks),
            },
        )
        with self._store.transaction():
            self._store.save_plan(record)
        payload = state_change_payload(current.status.value, record.status.value, RESTART_REASON)
        payload["stop_reason"] = RESTART_REASON
        self._publish(
            EventType.PLAN_STATE_CHANGED,
            current.session_id,
            conversation_id=current.conversation_id,
            cycle_id=current.cycle_id,
            plan_id=current.plan_id,
            payload=payload,
            timestamp=now,
        )
        self._record(
            run,
            entity="plan",
            entity_id=current.plan_id,
            from_state=current.status.value,
            to_state=record.status.value,
            reason=RESTART_REASON,
            session_id=current.session_id,
            conversation_id=current.conversation_id,
            cycle_id=current.cycle_id,
            plan_id=current.plan_id,
        )

    # ------------------------------------------------------------------ 4/5. conversations -
    def _settle_conversations(self, run: _Run) -> None:
        """Cycles first (ADR-016 order), then the conversations themselves."""
        conversations = self._store.find_conversations_in_states(_ACTIVE_CONVERSATION_STATES)
        resumable = {c.conversation_id for c in conversations if self._is_resumable(c)}
        run.resumable_conversations = resumable

        # 4. every RUNNING cycle of an open session, except the one a resumable conversation waits on
        seen: set[str] = set()
        conversation_ids = [c.conversation_id for c in conversations]
        for session in self._open_sessions():
            conversation_ids.extend(
                c.conversation_id for c in self._store.list_conversations(session.session_id)
            )
        for conversation_id in conversation_ids:
            if conversation_id in seen:
                continue
            seen.add(conversation_id)
            if conversation_id in resumable:
                continue
            for cycle in self._store.list_cycles(conversation_id):
                if cycle.status is CycleState.RUNNING:
                    self._interrupt_cycle(run, cycle)

        # 5. conversations
        for conversation in conversations:
            if conversation.conversation_id in resumable:
                self._record(
                    run,
                    entity="conversation",
                    entity_id=conversation.conversation_id,
                    from_state=conversation.status.value,
                    to_state=conversation.status.value,
                    reason=RESUMABLE_REASON,
                    session_id=conversation.session_id,
                    conversation_id=conversation.conversation_id,
                    cycle_id=conversation.current_cycle_id,
                )
                continue
            interrupted = self._lifecycle.interrupt_conversation(
                conversation.conversation_id, reason=RESTART_REASON
            )
            self._record(
                run,
                entity="conversation",
                entity_id=conversation.conversation_id,
                from_state=conversation.status.value,
                to_state=interrupted.status.value,
                reason=RESTART_REASON,
                session_id=conversation.session_id,
                conversation_id=conversation.conversation_id,
                cycle_id=conversation.current_cycle_id,
                plan_id=conversation.current_plan_id,
            )

    def _is_resumable(self, conversation: ConversationRecord) -> bool:
        """``WAITING_MODEL_RESPONSE`` with a persisted, unanswered ``user_request`` /
        ``execution_result`` as its last message, and no rotation in flight (ADR-016 §2)."""
        if conversation.status is not ConversationState.WAITING_MODEL_RESPONSE:
            return False
        session = self._store.get_session(conversation.session_id)
        if session is None or session.status is not SessionState.RUNNING:
            return False  # an interruption was in progress: its cleanup is finished instead
        if session.current_conversation_id != conversation.conversation_id:
            return False
        if conversation.parent_conversation_id is not None:
            parent = self._store.get_conversation(conversation.parent_conversation_id)
            if parent is not None and parent.status is ConversationState.ROTATING:
                return False
        pending = self.pending_outbound(conversation)
        return pending is not None

    def pending_outbound(self, conversation: ConversationRecord) -> MessageRecord | None:
        """The unanswered outbound message a resumable conversation waits on, or ``None``."""
        messages = self._store.list_messages(conversation.conversation_id)
        if not messages:
            return None
        last = messages[-1]
        if last.direction is not MessageDirection.OUTBOUND:
            return None
        if last.message_type not in _RESUMABLE_MESSAGE_TYPES:
            return None
        return last

    def _interrupt_cycle(self, run: _Run, cycle: CycleRecord) -> None:
        assert_transition(
            CYCLE_TRANSITIONS, cycle.status, CycleState.INTERRUPTED, entity=CYCLE_ENTITY
        )
        ended_at = self._clock.now()
        record = _apply(cycle, {"status": CycleState.INTERRUPTED, "ended_at": ended_at})
        with self._store.transaction():
            self._store.save_cycle(record)
        payload = state_change_payload(cycle.status.value, record.status.value, RESTART_REASON)
        payload.update(
            {
                "status": record.status.value,
                "duration_ms": max(0, (ended_at - cycle.started_at) // _ONE_MS),
                "retry_count": record.retry_count,
                "inbound_message_type": None,
            }
        )
        self._publish(
            EventType.CYCLE_ENDED,
            cycle.session_id,
            conversation_id=cycle.conversation_id,
            cycle_id=cycle.cycle_id,
            plan_id=cycle.plan_id,
            payload=payload,
            timestamp=ended_at,
        )
        self._record(
            run,
            entity="cycle",
            entity_id=cycle.cycle_id,
            from_state=cycle.status.value,
            to_state=record.status.value,
            reason=RESTART_REASON,
            session_id=cycle.session_id,
            conversation_id=cycle.conversation_id,
            cycle_id=cycle.cycle_id,
            plan_id=cycle.plan_id,
        )

    # ------------------------------------------------------------------ 6. sessions --------
    def _settle_sessions(self, run: _Run) -> None:
        for session in self._open_sessions():
            sid = session.session_id
            conversation = (
                self._store.get_conversation(session.current_conversation_id)
                if session.current_conversation_id is not None
                else None
            )
            if (
                conversation is not None
                and conversation.conversation_id in run.resumable_conversations
                and session.status is SessionState.RUNNING
            ):
                run.resumable.append(sid)
                continue
            if session.status is SessionState.RUNNING and conversation is not None:
                if conversation.status is ConversationState.FAILED:
                    self._transition_session(run, session, SessionState.FAILED)
                    run.failed.append(sid)
                    continue
                if conversation.status in _FINISHED_CONVERSATION_STATES:
                    self._finish_conversation(run, conversation)
                    self._transition_session(run, session, SessionState.COMPLETED)
                    run.completed.append(sid)
                    continue
            current = session
            if current.status is SessionState.RUNNING:
                current = self._transition_session(run, current, SessionState.INTERRUPTING)
            self._transition_session(run, current, SessionState.READY)
            run.ready.append(sid)

    def _finish_conversation(self, run: _Run, conversation: ConversationRecord) -> None:
        """A conversation left ``COMPLETED`` by a crash between the final answer and the §11
        closure decision: finish it per ``auto_close_on_final_answer``."""
        if conversation.status is not ConversationState.COMPLETED:
            return
        target = (
            ConversationState.CLOSED
            if conversation.auto_close_on_final_answer
            else ConversationState.WAITING_USER
        )
        updates: dict[str, Any] = {}
        if target is ConversationState.CLOSED:
            updates["closure_reason"] = "auto_close"
        finished = self._lifecycle.transition_conversation(
            conversation.conversation_id, target, reason=RESTART_REASON, **updates
        )
        self._record(
            run,
            entity="conversation",
            entity_id=conversation.conversation_id,
            from_state=conversation.status.value,
            to_state=finished.status.value,
            reason=RESTART_REASON,
            session_id=conversation.session_id,
            conversation_id=conversation.conversation_id,
        )

    def _transition_session(
        self, run: _Run, session: SessionRecord, to: SessionState
    ) -> SessionRecord:
        updated = self._lifecycle.transition_session(session.session_id, to, reason=RESTART_REASON)
        self._record(
            run,
            entity="session",
            entity_id=session.session_id,
            from_state=session.status.value,
            to_state=updated.status.value,
            reason=RESTART_REASON,
            session_id=session.session_id,
            conversation_id=session.current_conversation_id,
        )
        return updated

    # ------------------------------------------------------------------ bookkeeping --------
    def _record(
        self,
        run: _Run,
        *,
        entity: str,
        entity_id: str,
        from_state: str,
        to_state: str,
        reason: str,
        session_id: str,
        conversation_id: str | None = None,
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Append the action to the report and publish ``recovery.action`` (after the write)."""
        action = RecoveryAction(
            entity=entity,
            id=entity_id,
            from_state=from_state,
            to_state=to_state,
            reason=reason,
            session_id=session_id,
            details=dict(details or {}),
        )
        run.actions.append(action)
        payload: dict[str, Any] = {
            "entity": entity,
            "entity_id": entity_id,
            "id": entity_id,
            "from": from_state,
            "to": to_state,
            "reason": reason,
        }
        if details:
            payload["details"] = dict(details)
        self._publish(
            EventType.RECOVERY_ACTION,
            session_id,
            conversation_id=conversation_id,
            cycle_id=cycle_id,
            plan_id=plan_id,
            task_id=task_id,
            payload=payload,
        )

    def _publish(
        self,
        event_type: EventType,
        session_id: str,
        *,
        payload: dict[str, Any],
        conversation_id: str | None = None,
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        self._bus.publish(
            Event(
                event_type=event_type,
                timestamp=timestamp if timestamp is not None else self._clock.now(),
                session_id=session_id,
                conversation_id=conversation_id,
                cycle_id=cycle_id,
                plan_id=plan_id,
                task_id=task_id,
                payload=payload,
            )
        )
