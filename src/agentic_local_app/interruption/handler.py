"""``InterruptionHandler`` — the user interrupt procedure (§2.9, §3.4, §8.4, §9 ; ADR-006, ADR-007,
ADR-014, ADR-015, ADR-016, ADR-017).

Two levels (ADR-006): the **conversation** ends ``INTERRUPTED`` (terminal, kept for the audit) and
the **session** goes ``RUNNING -> INTERRUPTING -> READY``; a new ``user_request`` then opens a new
child conversation in the same session. Nothing is ever sent to the model (§8.4): the remote
conversation is closed in *best effort*, outside the critical path.

What the orchestrator relies on:

- :meth:`InterruptionHandler.token_for` — one :class:`CancellationToken` per session, created on
  demand and **replaced** by a fresh one when the session is back to ``READY``: the orchestrator
  hands it to ``PlanRunner.run(..., interrupt=token)`` and watches it around its transport calls
  (:meth:`raise_if_interrupted` raises ``SessionInterruptedError``);
- :meth:`InterruptionHandler.register_loop` — the ``asyncio.Event`` the orchestrator sets in the
  ``finally`` of its protocol loop; :meth:`loop_finished` sets it too;
- :meth:`InterruptionHandler.interrupt` — the procedure itself, described step by step in its
  docstring, returning an :class:`InterruptionReport`.

Every transition is ``validate -> persist (store) -> publish (bus)`` (ADR-015): session and
conversation through the ``ConversationLifecycleManager``, tasks / plan / cycle here through the
tables of :mod:`agentic_local_app.domain.transitions`. The sweep re-reads the store before each
transition and leaves terminal entities alone, so it is idempotent and safe next to a
``PlanRunner`` that already marked its tasks. Timestamps come from ``clock.now()``, durations from
``clock.monotonic_ms()``; only the bounded wait itself uses the event loop (ADR-017).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, TypeVar

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import PersistenceError, SessionInterruptedError
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    PlanRecord,
    Record,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.domain.transitions import (
    ACTIVE_CONVERSATION_STATES,
    CYCLE_TRANSITIONS,
    FAILED_TASK_STATES,
    PLAN_TRANSITIONS,
    TASK_TRANSITIONS,
    assert_transition,
)
from agentic_local_app.execution.executor import CancellationToken
from agentic_local_app.execution.plan_runner import INTERRUPT_REASON, PLAN_ENTITY, TASK_ENTITY
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.transport.gateway import TransportGateway

__all__ = [
    "CYCLE_ENTITY",
    "INTERRUPTION_FAILED_REASON",
    "USER_INTERRUPT_REASON",
    "InterruptionHandler",
    "InterruptionReport",
]

#: Default ``reason`` of an interruption requested by the user (§2.9); the recovery of ADR-016
#: reuses the same procedure with ``reason = "restart"``.
USER_INTERRUPT_REASON = INTERRUPT_REASON
#: ``reason`` of the best-effort ``INTERRUPTING -> FAILED`` after a persistence failure.
INTERRUPTION_FAILED_REASON = "interruption_failed"
#: ``entity`` carried by ``InvalidTransitionError`` for a cycle transition (plan and task reuse
#: the constants of the plan runner).
CYCLE_ENTITY = "cycle"

_IDLE_SESSION_STATES: frozenset[SessionState] = frozenset(
    {SessionState.READY, SessionState.COMPLETED, SessionState.FAILED}
)
_OPEN_PLAN_STATES: frozenset[PlanState] = frozenset({PlanState.PENDING, PlanState.RUNNING})
_INTERRUPTIBLE_TASK_STATES: frozenset[TaskState] = frozenset(
    {TaskState.RUNNING, TaskState.PENDING, TaskState.WAITING_DEPENDENCY}
)
_ONE_MS = timedelta(milliseconds=1)

R = TypeVar("R", bound=Record)


def _apply(record: R, changes: dict[str, Any]) -> R:
    """A new, fully validated record (``model_copy`` validates nothing)."""
    data = record.model_dump()
    data.update(changes)
    return type(record).model_validate(data)


@dataclass(frozen=True)
class InterruptionReport:
    """What :meth:`InterruptionHandler.interrupt` returns (and publishes in ``interruption.completed``).

    ``within_timeout`` is ``duration_ms <= execution.interrupt_drain_timeout_ms`` measured on the
    injected clock; ``loop_drained`` tells whether the registered loop signalled its end before the
    sweep (``True`` when no loop was registered); ``interrupted_task_ids`` lists, in plan order, the
    tasks of the current plan that are ``INTERRUPTED`` once the sweep is done — whether the
    ``PlanRunner`` or the handler marked them; ``plan_id`` / ``cycle_id`` are the current plan and
    cycle when they ended ``INTERRUPTED``, ``None`` otherwise.
    """

    session_id: str
    reason: str
    requested_at: datetime
    completed_at: datetime
    duration_ms: int
    within_timeout: bool
    loop_drained: bool
    nothing_to_interrupt: bool
    interrupted_task_ids: list[str]
    plan_id: str | None
    cycle_id: str | None
    conversation_id: str | None
    session_status: SessionState


@dataclass
class _Sweep:
    """What the sweep found and did for the current conversation of the session."""

    conversation_id: str | None = None
    plan_id: str | None = None
    cycle_id: str | None = None
    interrupted_task_ids: list[str] = field(default_factory=list)
    #: conversations this sweep transitioned to INTERRUPTED (their remote side gets closed).
    interrupted: list[ConversationRecord] = field(default_factory=list)


class InterruptionHandler:
    def __init__(
        self,
        store: ConversationStore,
        bus: EventBus,
        lifecycle: ConversationLifecycleManager,
        clock: Clock,
        ids: IdGenerator,
        config: AppConfig,
        *,
        transport: TransportGateway | None = None,
    ) -> None:
        self._store = store
        self._bus = bus
        self._lifecycle = lifecycle
        self._clock = clock
        self._ids = ids  # the handler generates no identifier itself (kept for the wiring)
        self._config = config
        self._transport = transport
        self._tokens: dict[str, CancellationToken] = {}
        self._loops: dict[str, asyncio.Event] = {}
        self._in_flight: dict[str, asyncio.Future[InterruptionReport]] = {}

    # ------------------------------------------------------------------ tokens and loops ---
    def token_for(self, session_id: str) -> CancellationToken:
        """The interruption token of ``session_id`` (created on demand, renewed on ``READY``)."""
        token = self._tokens.get(session_id)
        if token is None:
            token = self._tokens[session_id] = CancellationToken()
        return token

    def is_interrupting(self, session_id: str) -> bool:
        """``True`` while the current token of ``session_id`` is cancelled."""
        token = self._tokens.get(session_id)
        return token is not None and token.is_cancelled

    def raise_if_interrupted(self, session_id: str) -> None:
        """What the orchestrator calls around its transport calls (§3.2)."""
        token = self._tokens.get(session_id)
        if token is not None and token.is_cancelled:
            raise SessionInterruptedError(session_id=session_id, reason=token.reason)

    def register_loop(self, session_id: str) -> asyncio.Event:
        """A fresh "loop finished" event for ``session_id``; the caller sets it in its ``finally``."""
        event = asyncio.Event()
        self._loops[session_id] = event
        return event

    def loop_finished(self, session_id: str) -> None:
        """Set the registered loop event of ``session_id``, if any."""
        event = self._loops.get(session_id)
        if event is not None:
            event.set()

    # ------------------------------------------------------------------ the procedure ------
    async def interrupt(
        self, session_id: str, *, reason: str = USER_INTERRUPT_REASON
    ) -> InterruptionReport:
        """Interrupt ``session_id`` (§9 amended by ADR-006) and return the report.

        1. unknown session -> ``KeyError``; ``READY`` / ``COMPLETED`` / ``FAILED`` -> nothing to
           interrupt (no write, no event); an interruption already in progress -> its report;
        2. session ``RUNNING -> INTERRUPTING`` (a session left ``INTERRUPTING`` resumes the
           cleanup, ADR-016), ``interruption.requested``;
        3. token cancelled, in-flight transport calls abandoned;
        4. bounded wait for the registered loop (``interrupt_drain_timeout_ms``);
        5. idempotent sweep from the store: tasks, plan, cycle, conversation (and the rotating
           parent, ADR-014), each persisted then published;
        6. best-effort remote close;
        7. session ``INTERRUPTING -> READY``, fresh token, ``interruption.completed``.

        A ``PersistenceError`` after step 2 leaves the session ``FAILED`` (best effort) and
        propagates; the recovery finishes the job at the next start (ADR-016).
        """
        pending = self._in_flight.get(session_id)
        if pending is not None:
            return await asyncio.shield(pending)
        session = self._store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        if session.status in _IDLE_SESSION_STATES:
            return self._idle_report(session, reason)

        future: asyncio.Future[InterruptionReport] = asyncio.get_running_loop().create_future()
        self._in_flight[session_id] = future
        try:
            report = await self._run(session, reason)
        except Exception as exc:
            if not future.done():
                future.set_exception(exc)
                future.exception()  # retrieved: no "never retrieved" noise when nobody waits
            raise
        except BaseException:
            if not future.done():
                future.cancel()
            raise
        else:
            if not future.done():
                future.set_result(report)
            return report
        finally:
            self._in_flight.pop(session_id, None)

    async def _run(self, session: SessionRecord, reason: str) -> InterruptionReport:
        session_id = session.session_id
        requested_at = self._clock.now()
        t0 = self._clock.monotonic_ms()
        conversation = self._current_conversation(session)

        # 2. session RUNNING -> INTERRUPTING (persisted, published), then the request event
        if session.status is SessionState.RUNNING:
            self._lifecycle.transition_session(session_id, SessionState.INTERRUPTING, reason=reason)
        try:
            self._publish(
                EventType.INTERRUPTION_REQUESTED,
                requested_at,
                session_id=session_id,
                conversation_id=conversation.conversation_id if conversation else None,
                payload=_request_payload(reason, conversation),
            )

            # 3. signal the loop and the plan runner, abandon the transport calls in flight
            self.token_for(session_id).cancel(reason)
            if self._transport is not None:
                self._transport.abandon()

            # 4. bounded drain of the registered loop
            loop_drained = await self._drain(session_id)

            # 5. idempotent sweep from the store
            sweep = self._sweep(session_id, reason)

            # 6. best effort remote close, outside the critical path
            await self._close_remote(sweep.interrupted)

            # 7. session INTERRUPTING -> READY, fresh token, completion event
            ready = self._lifecycle.transition_session(
                session_id, SessionState.READY, reason=reason
            )
        except PersistenceError:
            self._fail_session(session_id)
            raise
        self._tokens[session_id] = CancellationToken()
        completed_at = self._clock.now()
        duration_ms = max(0, self._clock.monotonic_ms() - t0)
        report = InterruptionReport(
            session_id=session_id,
            reason=reason,
            requested_at=requested_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
            within_timeout=duration_ms <= self._config.execution.interrupt_drain_timeout_ms,
            loop_drained=loop_drained,
            nothing_to_interrupt=False,
            interrupted_task_ids=list(sweep.interrupted_task_ids),
            plan_id=sweep.plan_id,
            cycle_id=sweep.cycle_id,
            conversation_id=sweep.conversation_id,
            session_status=ready.status,
        )
        self._publish(
            EventType.INTERRUPTION_COMPLETED,
            completed_at,
            session_id=session_id,
            conversation_id=sweep.conversation_id,
            payload=_completion_payload(report),
        )
        return report

    def _idle_report(self, session: SessionRecord, reason: str) -> InterruptionReport:
        now = self._clock.now()
        return InterruptionReport(
            session_id=session.session_id,
            reason=reason,
            requested_at=now,
            completed_at=now,
            duration_ms=0,
            within_timeout=True,
            loop_drained=True,
            nothing_to_interrupt=True,
            interrupted_task_ids=[],
            plan_id=None,
            cycle_id=None,
            conversation_id=session.current_conversation_id,
            session_status=session.status,
        )

    def _fail_session(self, session_id: str) -> None:
        """Best effort ``INTERRUPTING -> FAILED`` so that nothing runs before the recovery."""
        with contextlib.suppress(Exception):
            self._lifecycle.transition_session(
                session_id, SessionState.FAILED, reason=INTERRUPTION_FAILED_REASON
            )

    # ------------------------------------------------------------------ 4. drain -----------
    async def _drain(self, session_id: str) -> bool:
        """Wait for the registered loop at most ``interrupt_drain_timeout_ms``; ``True`` when it
        ended in time (or when no loop was registered)."""
        event = self._loops.pop(session_id, None)
        if event is None or event.is_set():
            return True
        timeout_s = self._config.execution.interrupt_drain_timeout_ms / 1000
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout_s)
        except TimeoutError:
            return False
        return True

    # ------------------------------------------------------------------ 5. sweep -----------
    def _sweep(self, session_id: str, reason: str) -> _Sweep:
        """Tasks, plan, cycle and conversation of the current conversation — and of its rotating
        parent (ADR-014) — from the fresh state of the store; terminal entities are left alone."""
        session = self._require_session(session_id)
        sweep = _Sweep(conversation_id=session.current_conversation_id)
        conversation = self._current_conversation(session)
        if conversation is None:
            return sweep
        sweep.plan_id, sweep.interrupted_task_ids = self._sweep_plan(conversation, reason)
        sweep.cycle_id = self._sweep_cycle(conversation, reason)
        interrupted = self._sweep_conversation(conversation, reason)
        if interrupted is not None:
            sweep.interrupted.append(interrupted)
        parent = self._rotating_parent(conversation)
        if parent is not None:
            self._sweep_plan(parent, reason)
            self._sweep_cycle(parent, reason)
            interrupted_parent = self._sweep_conversation(parent, reason)
            if interrupted_parent is not None:
                sweep.interrupted.append(interrupted_parent)
        return sweep

    def _sweep_plan(
        self, conversation: ConversationRecord, reason: str
    ) -> tuple[str | None, list[str]]:
        """Open tasks then the plan -> ``INTERRUPTED``; returns the plan id when it ended
        ``INTERRUPTED`` and the ids of its ``INTERRUPTED`` tasks in plan order."""
        plan_id = conversation.current_plan_id
        if plan_id is None:
            return None, []
        session_id = conversation.session_id
        plan = self._store.get_plan(session_id, plan_id)
        if plan is None:
            return None, []
        if plan.status in _OPEN_PLAN_STATES:
            for task in self._store.list_tasks(session_id, plan_id=plan_id):
                if task.status in _INTERRUPTIBLE_TASK_STATES:
                    self._interrupt_task(task, plan, reason)
            plan = self._interrupt_plan(plan, reason)
        if plan.status is not PlanState.INTERRUPTED:
            return None, []
        interrupted = [
            task.task_id
            for task in self._store.list_tasks(session_id, plan_id=plan_id)
            if task.status is TaskState.INTERRUPTED
        ]
        return plan_id, interrupted

    def _interrupt_task(self, task: TaskRecord, plan: PlanRecord, reason: str) -> TaskRecord:
        assert_transition(TASK_TRANSITIONS, task.status, TaskState.INTERRUPTED, entity=TASK_ENTITY)
        now = self._clock.now()
        fields: dict[str, Any] = {
            "status": TaskState.INTERRUPTED,
            "reason": reason,
            "ended_at": now,
            "updated_at": now,
        }
        if task.status is TaskState.RUNNING and task.started_at is not None:
            fields["duration_ms"] = max(0, (now - task.started_at) // _ONE_MS)
        record = _apply(task, fields)
        with self._store.transaction():
            self._store.save_task(record)
        payload = state_change_payload(task.status.value, record.status.value, reason)
        if task.status is TaskState.RUNNING:
            payload["exit_code"] = record.exit_code
            payload["duration_ms"] = record.duration_ms
            payload["timed_out"] = record.timed_out
            payload["truncated"] = record.truncated
        self._publish(
            EventType.TASK_STATE_CHANGED,
            now,
            session_id=plan.session_id,
            conversation_id=plan.conversation_id,
            cycle_id=plan.cycle_id,
            plan_id=plan.plan_id,
            task_id=record.task_id,
            payload=payload,
        )
        return record

    def _interrupt_plan(self, plan: PlanRecord, reason: str) -> PlanRecord:
        assert_transition(PLAN_TRANSITIONS, plan.status, PlanState.INTERRUPTED, entity=PLAN_ENTITY)
        now = self._clock.now()
        tasks = self._store.list_tasks(plan.session_id, plan_id=plan.plan_id)
        record = _apply(
            plan,
            {
                "status": PlanState.INTERRUPTED,
                "stop_reason": reason,
                "ended_at": now,
                "updated_at": now,
                **_counters(tasks),
            },
        )
        with self._store.transaction():
            self._store.save_plan(record)
        payload = state_change_payload(plan.status.value, record.status.value, reason)
        payload["stop_reason"] = reason
        self._publish(
            EventType.PLAN_STATE_CHANGED,
            now,
            session_id=plan.session_id,
            conversation_id=plan.conversation_id,
            cycle_id=plan.cycle_id,
            plan_id=plan.plan_id,
            payload=payload,
        )
        return record

    def _sweep_cycle(self, conversation: ConversationRecord, reason: str) -> str | None:
        """Current cycle ``RUNNING -> INTERRUPTED``; returns its id when it ended ``INTERRUPTED``."""
        cycle_id = conversation.current_cycle_id
        if cycle_id is None:
            return None
        cycle = self._store.get_cycle(cycle_id)
        if cycle is None:
            return None
        if cycle.status is CycleState.RUNNING:
            cycle = self._interrupt_cycle(cycle, reason)
        return cycle_id if cycle.status is CycleState.INTERRUPTED else None

    def _interrupt_cycle(self, cycle: CycleRecord, reason: str) -> CycleRecord:
        assert_transition(
            CYCLE_TRANSITIONS, cycle.status, CycleState.INTERRUPTED, entity=CYCLE_ENTITY
        )
        ended_at = self._clock.now()
        record = _apply(cycle, {"status": CycleState.INTERRUPTED, "ended_at": ended_at})
        with self._store.transaction():
            self._store.save_cycle(record)
        payload = state_change_payload(cycle.status.value, record.status.value, reason)
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
            ended_at,
            session_id=cycle.session_id,
            conversation_id=cycle.conversation_id,
            cycle_id=cycle.cycle_id,
            payload=payload,
        )
        return record

    def _sweep_conversation(
        self, conversation: ConversationRecord, reason: str
    ) -> ConversationRecord | None:
        """``ANY_ACTIVE_STATE -> INTERRUPTED`` through the lifecycle manager (fresh read first);
        ``None`` when the conversation is not active any more (already terminal: untouched)."""
        fresh = self._store.get_conversation(conversation.conversation_id)
        if fresh is None or fresh.status not in ACTIVE_CONVERSATION_STATES:
            return None
        return self._lifecycle.interrupt_conversation(fresh.conversation_id, reason=reason)

    def _rotating_parent(self, conversation: ConversationRecord) -> ConversationRecord | None:
        """The parent of a child born from a rotation still in progress (ADR-014)."""
        parent_id = conversation.parent_conversation_id
        if parent_id is None:
            return None
        parent = self._store.get_conversation(parent_id)
        if parent is None or parent.status is not ConversationState.ROTATING:
            return None
        return parent

    # ------------------------------------------------------------------ 6. remote close ----
    async def _close_remote(self, conversations: Sequence[ConversationRecord]) -> None:
        """ADR-006: best effort, no retry, never on the critical path — every error is swallowed."""
        if self._transport is None:
            return
        for conversation in conversations:
            remote = conversation.remote_conversation_id
            if remote is None:
                continue
            with contextlib.suppress(Exception):
                await self._transport.close_conversation(remote)

    # ------------------------------------------------------------------ helpers ------------
    def _require_session(self, session_id: str) -> SessionRecord:
        session = self._store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session

    def _current_conversation(self, session: SessionRecord) -> ConversationRecord | None:
        if session.current_conversation_id is None:
            return None
        return self._store.get_conversation(session.current_conversation_id)

    def _publish(
        self,
        event_type: EventType,
        timestamp: datetime,
        *,
        session_id: str,
        conversation_id: str | None = None,
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
        payload: dict[str, Any],
    ) -> None:
        self._bus.publish(
            Event(
                event_type=event_type,
                timestamp=timestamp,
                session_id=session_id,
                conversation_id=conversation_id,
                cycle_id=cycle_id,
                plan_id=plan_id,
                task_id=task_id,
                payload=payload,
            )
        )


def _request_payload(reason: str, conversation: ConversationRecord | None) -> dict[str, Any]:
    """``interruption.requested``: the reason and where the session stood (phase 10 contract)."""
    return {
        "reason": reason,
        "conversation_id": conversation.conversation_id if conversation else None,
        "conversation_state": conversation.status.value if conversation else None,
        "plan_id": conversation.current_plan_id if conversation else None,
        "cycle_id": conversation.current_cycle_id if conversation else None,
    }


def _completion_payload(report: InterruptionReport) -> dict[str, Any]:
    """``interruption.completed``: the report, plus the field names of the phase 10 contract."""
    return {
        "reason": report.reason,
        "duration_ms": report.duration_ms,
        "within_timeout": report.within_timeout,
        "loop_drained": report.loop_drained,
        "interrupted_task_ids": list(report.interrupted_task_ids),
        "plan_id": report.plan_id,
        "cycle_id": report.cycle_id,
        "conversation_id": report.conversation_id,
        "interrupted_tasks": len(report.interrupted_task_ids),
        "interrupted_plan_id": report.plan_id,
        "interrupted_cycle_id": report.cycle_id,
        "within_drain_timeout": report.within_timeout,
    }


def _counters(tasks: Sequence[TaskRecord]) -> dict[str, int]:
    """The plan counters of §4.1 recomputed from the task records (``TIMED_OUT`` counts as failed)."""
    statuses = [task.status for task in tasks]
    return {
        "task_count": len(statuses),
        "completed_task_count": statuses.count(TaskState.COMPLETED),
        "failed_task_count": sum(1 for status in statuses if status in FAILED_TASK_STATES),
        "skipped_task_count": statuses.count(TaskState.SKIPPED),
        "cancelled_task_count": statuses.count(TaskState.CANCELLED),
        "interrupted_task_count": statuses.count(TaskState.INTERRUPTED),
    }
