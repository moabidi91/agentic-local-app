"""Phase 6 — interruption (spec §2.9, §3.4, §5.1–5.3, §8.4, §9, §17.1, §17.2, §18.2 ; ADR-006,
ADR-007, ADR-014, ADR-015, ADR-016, ADR-017 ; acceptance criteria 15 and 16).

Everything runs on the doubles of §18.3 — ``FakeCommandExecutor`` (no process),
``FakeTransportGateway`` (no network), ``FakeClock``, ``InMemoryConversationStore``, ``EventBus`` +
``RecordingSubscriber``, ``SequentialIdGenerator`` — and the real ``ConversationLifecycleManager``,
``PlanRunner``, ``AuditLog`` and ``ExecutionTracker``. Drains are configured at 200 ms so that the
bounded-wait tests stay short; every real wait is capped by ``asyncio.wait_for``.

Sections: harness · nothing to interrupt · every active conversation state · running plan with a
real PlanRunner · bounded drain and timing · sweep from the store (order, payloads, idempotence)
· audit and tracker · transport · concurrency and persistence failures · rotation · tokens and
loops · new user request after READY · hygiene.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

import pytest

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import (
    ErrorType,
    PersistenceError,
    SessionInterruptedError,
    TransportError,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    CycleType,
    MessageType,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
)
from agentic_local_app.execution.executor import CancellationToken
from agentic_local_app.execution.payload_guard import PayloadGuard
from agentic_local_app.execution.plan_runner import PlanOutcome, PlanRunner
from agentic_local_app.interruption import handler as handler_module
from agentic_local_app.interruption.handler import (
    INTERRUPTION_FAILED_REASON,
    USER_INTERRUPT_REASON,
    InterruptionHandler,
    InterruptionReport,
)
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.observability.execution_tracker import ExecutionTracker
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import InboundMessage, ProtocolAdapter
from agentic_local_app.protocol.messages import Envelope, PlanContent
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.fake import FakeTransportGateway

pytestmark = pytest.mark.phase6

#: Real-time bound of any test that waits on the handler or the runner.
BOUND_S = 1.0
#: Drains configured for the tests (real ``asyncio`` waits stay short).
DRAIN_MS = 200
PLAN_ID = "plan-1"
REASON = USER_INTERRUPT_REASON

ACTIVE_STATES: tuple[ConversationState, ...] = (
    ConversationState.ACTIVE,
    ConversationState.WAITING_MODEL_RESPONSE,
    ConversationState.RUNNING_PLAN,
    ConversationState.ROTATING,
)

_PATHS: dict[ConversationState, list[ConversationState]] = {
    ConversationState.ACTIVE: [ConversationState.ACTIVE],
    ConversationState.WAITING_MODEL_RESPONSE: [
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
    ],
    ConversationState.RUNNING_PLAN: [
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
    ],
    ConversationState.ROTATING: [
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
        ConversationState.ROTATING,
    ],
}


# ================================================================================================
# harness
# ================================================================================================
def _cmd(task_id: str, **fields: Any) -> dict[str, Any]:
    task: dict[str, Any] = {"task_id": task_id, "type": "cmd", "cmd": f"run {task_id}"}
    task.update(fields)
    return task


def _with_drains(config: AppConfig, ms: int = DRAIN_MS) -> AppConfig:
    execution = config.execution.model_copy(
        update={"interrupt_drain_timeout_ms": ms, "cancel_drain_timeout_ms": ms}
    )
    return config.model_copy(update={"execution": execution})


async def _settle(rounds: int = 25) -> None:
    """Let every ready callback of the loop run."""
    for _ in range(rounds):
        await asyncio.sleep(0)


def _kinds(events: list[Event]) -> list[tuple[str, str | None]]:
    """``(event_type, to)`` of every event, ``to`` from the payload when present."""
    return [(e.event_type.value, e.payload.get("to")) for e in events]


def _interrupted_events(events: list[Event]) -> list[Event]:
    return [e for e in events if e.payload.get("to") == "INTERRUPTED"]


class _ExplodingCloseGateway(FakeTransportGateway):
    """A transport whose remote close raises something that is not a ``TransportError``."""

    async def close_conversation(self, remote_conversation_id: str) -> None:
        raise RuntimeError("close exploded")


@dataclass
class Harness:
    store: InMemoryConversationStore
    bus: EventBus
    recorder: RecordingSubscriber
    clock: FakeClock
    ids: SequentialIdGenerator
    config: AppConfig
    lifecycle: ConversationLifecycleManager
    fake: FakeCommandExecutor
    runner: PlanRunner
    transport: FakeTransportGateway
    handler: InterruptionHandler

    @classmethod
    def build(
        cls,
        store: InMemoryConversationStore,
        bus: EventBus,
        recorder: RecordingSubscriber,
        clock: FakeClock,
        ids: SequentialIdGenerator,
        config: AppConfig,
        *,
        transport: FakeTransportGateway | None = None,
        with_transport: bool = True,
    ) -> Harness:
        config = _with_drains(config)
        lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
        fake = FakeCommandExecutor(clock)
        runner = PlanRunner(
            store,
            bus,
            fake,
            PayloadGuard(config.payload),
            clock,
            ids,
            config,
            translator=ShellTranslator(ShellDialect.POSIX),  # scripted machine, ADR-030
        )
        gateway = transport or FakeTransportGateway(clock)
        handler = InterruptionHandler(
            store,
            bus,
            lifecycle,
            clock,
            ids,
            config,
            transport=gateway if with_transport else None,
        )
        return cls(
            store, bus, recorder, clock, ids, config, lifecycle, fake, runner, gateway, handler
        )

    # ---- sessions and conversations ------------------------------------------------------
    def session(self, state: SessionState = SessionState.RUNNING) -> SessionRecord:
        session = self.lifecycle.create_session(
            "goal",
            "message",
            "local-user",
            SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=60_000),
            False,
        )
        if state is SessionState.READY:
            return session
        session = self.lifecycle.transition_session(
            session.session_id, SessionState.RUNNING, reason="user_request"
        )
        if state is SessionState.RUNNING:
            return session
        if state is SessionState.INTERRUPTING:
            return self.lifecycle.transition_session(session.session_id, state, reason=REASON)
        return self.lifecycle.transition_session(session.session_id, state)

    def conversation(
        self,
        session: SessionRecord,
        state: ConversationState,
        *,
        remote: str | None = "remote-0001",
        parent: str | None = None,
    ) -> ConversationRecord:
        """A conversation of ``session`` driven from NEW to ``state`` through the manager."""
        conversation = self.lifecycle.create_conversation(
            session.session_id, parent_conversation_id=parent
        )
        for step in _PATHS.get(state, []):
            conversation = self.lifecycle.transition_conversation(
                conversation.conversation_id, step
            )
        if remote is not None:
            conversation = self.lifecycle.update_conversation(
                conversation.conversation_id, remote_conversation_id=remote
            )
        return conversation

    def running_conversation(
        self, state: ConversationState = ConversationState.RUNNING_PLAN
    ) -> tuple[SessionRecord, ConversationRecord]:
        session = self.session()
        return session, self.conversation(session, state)

    # ---- cycles and plans ----------------------------------------------------------------
    def cycle(self, conversation: ConversationRecord) -> CycleRecord:
        cycle = CycleRecord(
            cycle_id=self.ids.cycle_id(),
            conversation_id=conversation.conversation_id,
            session_id=conversation.session_id,
            cycle_type=CycleType.EXECUTION,
            status=CycleState.RUNNING,
            started_at=self.clock.now(),
        )
        self.store.save_cycle(cycle)
        self.lifecycle.update_conversation(
            conversation.conversation_id, current_cycle_id=cycle.cycle_id
        )
        return cycle

    def plan(
        self,
        conversation: ConversationRecord,
        tasks: list[dict[str, Any]],
        *,
        policy: str = "sequential",
        workers: int | None = None,
        cycle_id: str | None = None,
    ) -> tuple[PlanRecord, list[TaskRecord]]:
        """PENDING records exactly as the orchestrator gets them from the adapter, persisted, and
        pointed to by ``conversation.current_plan_id``."""
        raw: dict[str, Any] = {
            "plan_id": PLAN_ID,
            "objective": "objective",
            "execution_policy": policy,
            "tasks": tasks,
        }
        if workers is not None:
            raw["max_parallel_workers"] = workers
        content = PlanContent.model_validate(raw)
        inbound = InboundMessage(
            envelope=Envelope(
                type=MessageType.EXECUTION_PLAN,
                conversation_id=conversation.conversation_id,
                message_id="msg-in-1",
                content=content.model_dump(mode="json", exclude_none=True),
            ),
            content=content,
            message_type=MessageType.EXECUTION_PLAN,
            plan_type=PlanType.EXECUTION_PLAN,
        )
        session = self.store.get_session(conversation.session_id)
        assert session is not None
        current = self.store.get_conversation(conversation.conversation_id)
        assert current is not None
        plan, records = ProtocolAdapter(self.config).plan_to_records(
            inbound,
            session=session,
            conversation=current,
            cycle_id=cycle_id or current.current_cycle_id or "cyc-none",
            clock=self.clock,
        )
        self.store.save_plan(plan)
        self.store.save_tasks(records)
        self.lifecycle.update_conversation(
            conversation.conversation_id, current_plan_id=plan.plan_id
        )
        return plan, records

    def persist_states(
        self, plan: PlanRecord, states: dict[str, TaskState], plan_state: PlanState
    ) -> None:
        """Write task and plan states directly (a plan mid-execution or already marked)."""
        now = self.clock.now()
        for task_id, state in states.items():
            task = self.task(plan.session_id, task_id)
            fields: dict[str, Any] = {"status": state, "updated_at": now}
            if state is TaskState.RUNNING:
                fields.update(started_at=now, attempt_count=1, pid=4_000)
            self.store.save_task(task.model_copy(update=fields))
        changes: dict[str, Any] = {"status": plan_state, "updated_at": now}
        if plan_state is not PlanState.PENDING:
            changes["started_at"] = now
        self.store.save_plan(plan.model_copy(update=changes))

    # ---- running a plan through the handler's token, like the orchestrator ---------------
    def start_loop(
        self, session: SessionRecord, plan: PlanRecord, tasks: list[TaskRecord]
    ) -> asyncio.Task[PlanOutcome]:
        """The orchestrator's loop: registered, run with the session token, signalled in ``finally``."""
        done = self.handler.register_loop(session.session_id)
        token = self.handler.token_for(session.session_id)

        async def loop() -> PlanOutcome:
            try:
                return await self.runner.run(plan, tasks, session, interrupt=token)
            finally:
                done.set()

        return asyncio.ensure_future(loop())

    async def interrupt(self, session_id: str, *, reason: str = REASON) -> InterruptionReport:
        return await asyncio.wait_for(
            self.handler.interrupt(session_id, reason=reason), BOUND_S + DRAIN_MS / 1000
        )

    # ---- reading back --------------------------------------------------------------------
    def read_session(self, session_id: str) -> SessionRecord:
        record = self.store.get_session(session_id)
        assert record is not None
        return record

    def read_conversation(self, conversation_id: str) -> ConversationRecord:
        record = self.store.get_conversation(conversation_id)
        assert record is not None
        return record

    def read_cycle(self, cycle_id: str) -> CycleRecord:
        record = self.store.get_cycle(cycle_id)
        assert record is not None
        return record

    def read_plan(self, session_id: str, plan_id: str = PLAN_ID) -> PlanRecord:
        record = self.store.get_plan(session_id, plan_id)
        assert record is not None
        return record

    def task(self, session_id: str, task_id: str) -> TaskRecord:
        record = self.store.get_task(session_id, task_id)
        assert record is not None
        return record

    def statuses(self, session_id: str, plan_id: str = PLAN_ID) -> dict[str, TaskState]:
        return {t.task_id: t.status for t in self.store.list_tasks(session_id, plan_id=plan_id)}


@pytest.fixture
def harness(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> Harness:
    return Harness.build(store, bus, recorder, clock, ids, config)


def _assert_interrupted_and_ready(
    harness: Harness, session: SessionRecord, conversation: ConversationRecord, from_state: str
) -> None:
    stored_conversation = harness.read_conversation(conversation.conversation_id)
    assert stored_conversation.status is ConversationState.INTERRUPTED
    assert stored_conversation.interrupted_at == harness.clock.now()
    stored_session = harness.read_session(session.session_id)
    assert stored_session.status is SessionState.READY
    assert stored_session.interrupted_at == harness.clock.now()
    assert stored_session.current_conversation_id == conversation.conversation_id
    assert stored_session.started_at == session.started_at
    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "INTERRUPTING"),
        ("interruption.requested", None),
        ("conversation.state_changed", "INTERRUPTED"),
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]
    conversation_event = harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)[0]
    assert conversation_event.payload == {"from": from_state, "to": "INTERRUPTED", "reason": REASON}
    assert conversation_event.conversation_id == conversation.conversation_id


# ================================================================================================
# 1. nothing to interrupt, unknown session
# ================================================================================================
async def given_unknown_session_when_user_interrupts_then_key_error(harness: Harness) -> None:
    with pytest.raises(KeyError):
        await harness.handler.interrupt("sess-unknown")
    assert harness.recorder.events == []


@pytest.mark.parametrize(
    "state", [SessionState.READY, SessionState.COMPLETED, SessionState.FAILED], ids=str
)
async def given_idle_session_when_user_interrupts_then_nothing_to_interrupt_and_no_event(
    harness: Harness, state: SessionState
) -> None:
    session = harness.session(state)
    before = harness.store.get_session(session.session_id)
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert report.nothing_to_interrupt is True
    assert report.session_status is state and report.session_id == session.session_id
    assert report.reason == REASON and report.duration_ms == 0 and report.within_timeout is True
    assert report.interrupted_task_ids == [] and report.plan_id is None and report.cycle_id is None
    assert report.requested_at == harness.clock.now() == report.completed_at
    assert harness.recorder.events == []
    assert harness.store.get_session(session.session_id) == before
    assert harness.handler.token_for(session.session_id).is_cancelled is False


async def given_ready_session_when_user_interrupts_then_no_write_at_all(harness: Harness) -> None:
    session = harness.session(SessionState.READY)
    harness.store.fail_next_write = True  # any write would raise

    report = await harness.interrupt(session.session_id)

    assert report.nothing_to_interrupt is True
    assert harness.store.fail_next_write is True  # the hook was never consumed


# ================================================================================================
# 2. every active conversation state (§5.1, §18.2 phase 6, ADR-006)
# ================================================================================================
async def given_active_conversation_when_user_interrupts_then_conversation_interrupted_and_session_ready(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.ACTIVE)
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    _assert_interrupted_and_ready(harness, session, conversation, "ACTIVE")
    assert report.nothing_to_interrupt is False and report.session_status is SessionState.READY
    assert report.conversation_id == conversation.conversation_id
    assert report.plan_id is None and report.cycle_id is None and report.interrupted_task_ids == []


async def given_waiting_model_response_conversation_when_user_interrupts_then_conversation_interrupted_and_session_ready(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.WAITING_MODEL_RESPONSE)
    harness.recorder.clear()

    await harness.interrupt(session.session_id)

    _assert_interrupted_and_ready(harness, session, conversation, "WAITING_MODEL_RESPONSE")


async def given_running_plan_conversation_when_user_interrupts_then_conversation_interrupted_and_session_ready(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.RUNNING_PLAN)
    harness.recorder.clear()

    await harness.interrupt(session.session_id)

    _assert_interrupted_and_ready(harness, session, conversation, "RUNNING_PLAN")


async def given_rotating_conversation_when_user_interrupts_then_conversation_interrupted_and_session_ready(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.ROTATING)
    harness.recorder.clear()

    await harness.interrupt(session.session_id)

    _assert_interrupted_and_ready(harness, session, conversation, "ROTATING")


async def given_running_session_without_conversation_when_user_interrupts_then_session_ready_with_session_events_only(
    harness: Harness,
) -> None:
    session = harness.session()
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert harness.read_session(session.session_id).status is SessionState.READY
    assert report.conversation_id is None and report.nothing_to_interrupt is False
    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "INTERRUPTING"),
        ("interruption.requested", None),
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]


# ================================================================================================
# 3. running plan with a real PlanRunner (§2.9, §8.4, §9, §18.4 example)
# ================================================================================================
async def given_running_plan_when_user_interrupts_then_all_tasks_marked_interrupted(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    cycle = harness.cycle(conversation)
    harness.fake.script(task_id="t1", stdout=b"partial", hang_until_cancelled=True)
    plan, tasks = harness.plan(
        conversation,
        [_cmd("t1"), _cmd("t2"), _cmd("t3", depends_on=["t2"])],
        policy="parallel",
        workers=1,
    )
    running = harness.start_loop(session, plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)
    await _settle()
    assert harness.statuses(session.session_id) == {
        "t1": TaskState.RUNNING,
        "t2": TaskState.PENDING,
        "t3": TaskState.WAITING_DEPENDENCY,
    }
    harness.recorder.clear()
    started = time.perf_counter()

    report = await harness.interrupt(session.session_id)
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert time.perf_counter() - started < 0.5
    # the runner did its part (§8.4): token honoured, no execution_result
    assert harness.fake.cancellations == [("t1", REASON)]
    assert outcome.interrupted is True and outcome.execution_result is None
    assert outcome.plan.status is PlanState.INTERRUPTED and outcome.stop_reason == REASON
    # every entity is terminal INTERRUPTED in the store (§2.9)
    assert harness.statuses(session.session_id) == {
        "t1": TaskState.INTERRUPTED,
        "t2": TaskState.INTERRUPTED,
        "t3": TaskState.INTERRUPTED,
    }
    assert all(
        t.reason == REASON for t in harness.store.list_tasks(session.session_id, plan_id=PLAN_ID)
    )
    stored_plan = harness.read_plan(session.session_id)
    assert stored_plan.status is PlanState.INTERRUPTED and stored_plan.stop_reason == REASON
    assert stored_plan.interrupted_task_count == 3
    stored_cycle = harness.read_cycle(cycle.cycle_id)
    assert stored_cycle.status is CycleState.INTERRUPTED
    assert stored_cycle.ended_at == harness.clock.now()
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )
    assert harness.read_session(session.session_id).status is SessionState.READY
    # the report
    assert report.session_id == session.session_id and report.reason == REASON
    assert report.nothing_to_interrupt is False and report.loop_drained is True
    assert report.within_timeout is True and report.duration_ms == 0
    assert report.interrupted_task_ids == ["t1", "t2", "t3"]
    assert report.plan_id == PLAN_ID and report.cycle_id == cycle.cycle_id
    assert report.conversation_id == conversation.conversation_id
    assert report.session_status is SessionState.READY
    # one INTERRUPTED event per entity, none twice (the runner marked tasks and plan, the
    # handler's sweep left them alone)
    interrupted = _interrupted_events(harness.recorder.events)
    assert [(e.event_type.value, e.task_id) for e in interrupted] == [
        ("task.state_changed", "t1"),
        ("task.state_changed", "t2"),
        ("task.state_changed", "t3"),
        ("plan.state_changed", None),
        ("cycle.ended", None),
        ("conversation.state_changed", None),
    ]
    assert harness.transport.posted == []


async def given_interrupted_plan_when_transport_inspected_then_no_execution_result_posted(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    harness.fake.script(task_id="t1", hang_until_cancelled=True)
    plan, tasks = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    running = harness.start_loop(session, plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)

    await harness.interrupt(session.session_id)
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert outcome.execution_result is None
    assert harness.transport.posted == [] and harness.transport.get_calls == []
    assert harness.transport.inits == []


async def given_plan_already_marked_by_runner_when_sweep_runs_then_no_duplicate_events(
    harness: Harness,
) -> None:
    """The runner marked everything before the sweep: the store is the truth, nothing is
    transitioned twice, but the report still lists the interrupted tasks."""
    session, conversation = harness.running_conversation()
    cycle = harness.cycle(conversation)
    plan, _ = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.persist_states(
        plan, {"t1": TaskState.INTERRUPTED, "t2": TaskState.INTERRUPTED}, PlanState.INTERRUPTED
    )
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert harness.recorder.of_type(EventType.TASK_STATE_CHANGED) == []
    assert harness.recorder.of_type(EventType.PLAN_STATE_CHANGED) == []
    assert len(harness.recorder.of_type(EventType.CYCLE_ENDED)) == 1
    assert report.interrupted_task_ids == ["t1", "t2"]
    assert report.plan_id == PLAN_ID and report.cycle_id == cycle.cycle_id
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_task_ignoring_soft_signal_when_interrupted_then_forced_after_drain_timeout(
    harness: Harness,
) -> None:
    """A process that ignores SIGTERM: the runner forces it after its own drain while the
    handler's drain (same length, armed a few hundred microseconds earlier) elapses too. Whoever
    marks first, the persisted end state is the same, every entity is terminal with at least one
    INTERRUPTED event, the session is READY and the audit chain verifies (the runner may re-mark
    tasks and plan from its in-memory copies: see the guide's open points)."""
    audit = AuditLog(harness.store, harness.clock, harness.ids)
    audit.subscribe(harness.bus)
    session, conversation = harness.running_conversation()
    cycle = harness.cycle(conversation)
    harness.fake.script(task_id="t1", hold=True, ignore_cancel=True)
    plan, tasks = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    running = harness.start_loop(session, plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)
    await _settle()
    harness.recorder.clear()
    started = time.perf_counter()

    report = await harness.interrupt(session.session_id)
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert time.perf_counter() - started < DRAIN_MS / 1000 + 0.5
    assert outcome.interrupted is True and outcome.execution_result is None
    assert harness.fake.active == set()
    assert harness.statuses(session.session_id) == {
        "t1": TaskState.INTERRUPTED,
        "t2": TaskState.INTERRUPTED,
    }
    t1 = harness.task(session.session_id, "t1")
    assert t1.reason == REASON and t1.exit_code is None
    stored_plan = harness.read_plan(session.session_id)
    assert stored_plan.status is PlanState.INTERRUPTED and stored_plan.stop_reason == REASON
    assert stored_plan.interrupted_task_count == 2
    assert harness.read_cycle(cycle.cycle_id).status is CycleState.INTERRUPTED
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )
    assert harness.read_session(session.session_id).status is SessionState.READY
    assert report.interrupted_task_ids == ["t1", "t2"] and report.plan_id == PLAN_ID
    assert report.cycle_id == cycle.cycle_id and report.session_status is SessionState.READY
    interrupted = _interrupted_events(harness.recorder.events)
    touched = {
        (e.event_type.value, e.task_id or e.plan_id or e.cycle_id or e.conversation_id)
        for e in interrupted
    }
    assert touched == {
        ("task.state_changed", "t1"),
        ("task.state_changed", "t2"),
        ("plan.state_changed", PLAN_ID),
        ("cycle.ended", cycle.cycle_id),
        ("conversation.state_changed", conversation.conversation_id),
    }
    assert len(harness.recorder.of_type(EventType.CYCLE_ENDED)) == 1
    assert len(harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)) == 1
    assert len(harness.recorder.of_type(EventType.INTERRUPTION_COMPLETED)) == 1
    assert [e.payload["to"] for e in harness.recorder.of_type(EventType.SESSION_STATE_CHANGED)] == [
        "INTERRUPTING",
        "READY",
    ]
    assert audit.verify(session.session_id).valid is True
    assert harness.transport.posted == []


# ================================================================================================
# 4. bounded drain and timing (§2.9, §3.4, §17.2, criterion 15)
# ================================================================================================
async def given_loop_never_signalling_end_when_user_interrupts_then_returns_after_drain_timeout_with_sweep_done(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    plan, _ = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    harness.handler.register_loop(session.session_id)  # never set
    token = harness.handler.token_for(session.session_id)
    started = time.perf_counter()

    report = await harness.interrupt(session.session_id)

    elapsed = time.perf_counter() - started
    assert DRAIN_MS / 1000 <= elapsed < DRAIN_MS / 1000 + 0.5
    assert report.loop_drained is False and report.nothing_to_interrupt is False
    assert token.is_cancelled is True and token.reason == REASON
    assert harness.statuses(session.session_id) == {
        "t1": TaskState.INTERRUPTED,
        "t2": TaskState.INTERRUPTED,
    }
    assert harness.read_plan(session.session_id).status is PlanState.INTERRUPTED
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_interruption_when_completed_then_session_ready_within_drain_timeout(
    harness: Harness,
) -> None:
    """The loop ends after 50 ms of (fake) time: ``duration_ms`` is measured on the injected
    clock and ``within_timeout`` compares it to ``interrupt_drain_timeout_ms``."""
    session, _ = harness.running_conversation()
    done = harness.handler.register_loop(session.session_id)
    token = harness.handler.token_for(session.session_id)

    async def loop() -> None:
        await token.wait()
        harness.clock.advance(50)
        done.set()

    running = asyncio.ensure_future(loop())
    requested_at = harness.clock.now()

    report = await harness.interrupt(session.session_id)
    await running

    assert report.loop_drained is True
    assert report.duration_ms == 50 and report.within_timeout is True
    assert report.requested_at == requested_at
    assert report.completed_at == requested_at + timedelta(milliseconds=50)
    assert harness.read_session(session.session_id).status is SessionState.READY
    completed = harness.recorder.of_type(EventType.INTERRUPTION_COMPLETED)[0]
    assert completed.payload["duration_ms"] == 50
    assert completed.payload["within_timeout"] is True
    assert completed.payload["within_drain_timeout"] is True


async def given_loop_draining_slower_than_timeout_when_measured_on_the_clock_then_within_timeout_false(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    done = harness.handler.register_loop(session.session_id)
    token = harness.handler.token_for(session.session_id)

    async def loop() -> None:
        await token.wait()
        harness.clock.advance(DRAIN_MS + 1)
        done.set()

    running = asyncio.ensure_future(loop())

    report = await harness.interrupt(session.session_id)
    await running

    assert report.loop_drained is True
    assert report.duration_ms == DRAIN_MS + 1 and report.within_timeout is False
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_no_loop_registered_when_user_interrupts_then_no_wait_and_loop_drained(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    started = time.perf_counter()

    report = await harness.interrupt(session.session_id)

    assert time.perf_counter() - started < 0.1
    assert report.loop_drained is True


async def given_loop_already_finished_when_user_interrupts_then_drained_without_waiting(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    harness.handler.loop_finished(session.session_id)  # nothing registered: harmless
    done = harness.handler.register_loop(session.session_id)
    harness.handler.loop_finished(session.session_id)
    assert done.is_set() is True
    started = time.perf_counter()

    report = await harness.interrupt(session.session_id)

    assert time.perf_counter() - started < 0.1
    assert report.loop_drained is True


# ================================================================================================
# 5. the sweep from the store: order, payloads, idempotence (§9, ADR-015, phase 10 contract)
# ================================================================================================
async def given_persisted_running_plan_when_user_interrupts_then_events_ordered_tasks_plan_cycle_conversation_session(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    cycle = harness.cycle(conversation)
    plan, _ = harness.plan(
        conversation,
        [_cmd("t1"), _cmd("t2"), _cmd("t3", depends_on=["t2"]), _cmd("t4")],
        policy="parallel",
        workers=2,
    )
    harness.persist_states(
        plan,
        {
            "t1": TaskState.RUNNING,
            "t2": TaskState.COMPLETED,
            "t3": TaskState.WAITING_DEPENDENCY,
            "t4": TaskState.PENDING,
        },
        PlanState.RUNNING,
    )
    harness.clock.advance(1_500)  # t1 has been running for 1.5 s
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "INTERRUPTING"),
        ("interruption.requested", None),
        ("task.state_changed", "INTERRUPTED"),
        ("task.state_changed", "INTERRUPTED"),
        ("task.state_changed", "INTERRUPTED"),
        ("plan.state_changed", "INTERRUPTED"),
        ("cycle.ended", "INTERRUPTED"),
        ("conversation.state_changed", "INTERRUPTED"),
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]
    events = harness.recorder.events
    assert [e.task_id for e in harness.recorder.of_type(EventType.TASK_STATE_CHANGED)] == [
        "t1",
        "t3",
        "t4",
    ]
    t1_event, t3_event, t4_event = harness.recorder.of_type(EventType.TASK_STATE_CHANGED)
    assert t1_event.payload == {
        "from": "RUNNING",
        "to": "INTERRUPTED",
        "reason": REASON,
        "exit_code": None,
        "duration_ms": 1_500,
        "timed_out": False,
        "truncated": False,
    }
    assert t3_event.payload == {"from": "WAITING_DEPENDENCY", "to": "INTERRUPTED", "reason": REASON}
    assert t4_event.payload == {"from": "PENDING", "to": "INTERRUPTED", "reason": REASON}
    for event in (t1_event, t3_event, t4_event):
        assert event.plan_id == PLAN_ID and event.cycle_id == cycle.cycle_id
        assert event.conversation_id == conversation.conversation_id
        assert event.timestamp == harness.clock.now()
    plan_event = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)[0]
    assert plan_event.payload == {
        "from": "RUNNING",
        "to": "INTERRUPTED",
        "reason": REASON,
        "stop_reason": REASON,
    }
    assert plan_event.plan_id == PLAN_ID and plan_event.cycle_id == cycle.cycle_id
    cycle_event = harness.recorder.of_type(EventType.CYCLE_ENDED)[0]
    assert cycle_event.cycle_id == cycle.cycle_id
    assert cycle_event.payload == {
        "from": "RUNNING",
        "to": "INTERRUPTED",
        "reason": REASON,
        "status": "INTERRUPTED",
        "duration_ms": 1_500,
        "retry_count": 0,
        "inbound_message_type": None,
    }
    requested = events[1]
    assert requested.session_id == session.session_id
    assert requested.conversation_id == conversation.conversation_id
    assert requested.payload == {
        "reason": REASON,
        "conversation_id": conversation.conversation_id,
        "conversation_state": "RUNNING_PLAN",
        "plan_id": PLAN_ID,
        "cycle_id": cycle.cycle_id,
    }
    completed = events[-1]
    assert completed.conversation_id == conversation.conversation_id
    assert completed.payload == {
        "reason": REASON,
        "duration_ms": 0,
        "within_timeout": True,
        "loop_drained": True,
        "interrupted_task_ids": ["t1", "t3", "t4"],
        "plan_id": PLAN_ID,
        "cycle_id": cycle.cycle_id,
        "conversation_id": conversation.conversation_id,
        "interrupted_tasks": 3,
        "interrupted_plan_id": PLAN_ID,
        "interrupted_cycle_id": cycle.cycle_id,
        "within_drain_timeout": True,
    }
    # records
    t1 = harness.task(session.session_id, "t1")
    assert t1.status is TaskState.INTERRUPTED and t1.reason == REASON
    assert t1.ended_at == harness.clock.now() and t1.duration_ms == 1_500
    assert t1.exit_code is None and t1.stdout_ref is None and t1.pid == 4_000
    t2 = harness.task(session.session_id, "t2")
    assert t2.status is TaskState.COMPLETED and t2.reason is None
    t4 = harness.task(session.session_id, "t4")
    assert t4.ended_at == harness.clock.now() and t4.duration_ms is None
    stored_plan = harness.read_plan(session.session_id)
    assert stored_plan.status is PlanState.INTERRUPTED and stored_plan.stop_reason == REASON
    assert stored_plan.ended_at == harness.clock.now()
    assert (stored_plan.task_count, stored_plan.completed_task_count) == (4, 1)
    assert stored_plan.interrupted_task_count == 3 and stored_plan.failed_task_count == 0
    assert harness.read_cycle(cycle.cycle_id).ended_at == harness.clock.now()
    assert report.interrupted_task_ids == ["t1", "t3", "t4"]


async def given_pending_plan_when_user_interrupts_then_plan_interrupted_from_pending(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    stored_plan = harness.read_plan(session.session_id)
    assert stored_plan.status is PlanState.INTERRUPTED and stored_plan.started_at is None
    assert stored_plan.interrupted_task_count == 2
    plan_event = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)[0]
    assert plan_event.payload["from"] == "PENDING" and plan_event.payload["to"] == "INTERRUPTED"
    assert report.interrupted_task_ids == ["t1", "t2"]


async def given_terminal_plan_and_completed_cycle_when_user_interrupts_then_they_are_left_untouched(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.WAITING_MODEL_RESPONSE)
    cycle = harness.cycle(conversation)
    harness.store.save_cycle(
        cycle.model_copy(update={"status": CycleState.COMPLETED, "ended_at": harness.clock.now()})
    )
    plan, _ = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.persist_states(
        plan, {"t1": TaskState.COMPLETED, "t2": TaskState.FAILED}, PlanState.STOPPED_ON_FAILURE
    )
    before_plan = harness.read_plan(session.session_id)
    before_cycle = harness.read_cycle(cycle.cycle_id)
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert harness.read_plan(session.session_id) == before_plan
    assert harness.read_cycle(cycle.cycle_id) == before_cycle
    assert harness.recorder.of_type(EventType.TASK_STATE_CHANGED) == []
    assert harness.recorder.of_type(EventType.PLAN_STATE_CHANGED) == []
    assert harness.recorder.of_type(EventType.CYCLE_ENDED) == []
    assert report.interrupted_task_ids == [] and report.plan_id is None
    assert report.cycle_id is None
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )


async def given_conversation_already_interrupted_when_user_interrupts_then_no_second_transition(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.ACTIVE)
    interrupted = harness.lifecycle.interrupt_conversation(
        conversation.conversation_id, reason=REASON
    )
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert harness.read_conversation(conversation.conversation_id) == interrupted
    assert harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED) == []
    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "INTERRUPTING"),
        ("interruption.requested", None),
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]
    assert report.conversation_id == conversation.conversation_id
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_reason_restart_when_interrupt_called_then_reason_carried_by_every_entity(
    harness: Harness,
) -> None:
    """ADR-016: the recovery path reuses the same procedure with ``reason = restart``."""
    session, conversation = harness.running_conversation()
    harness.cycle(conversation)
    plan, _ = harness.plan(conversation, [_cmd("t1")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id, reason="restart")

    assert report.reason == "restart"
    assert harness.task(session.session_id, "t1").reason == "restart"
    assert harness.read_plan(session.session_id).stop_reason == "restart"
    assert harness.handler.token_for(session.session_id).is_cancelled is False
    reasons = {
        e.event_type.value: e.payload.get("reason")
        for e in harness.recorder.events
        if "reason" in e.payload
    }
    assert reasons == {
        "session.state_changed": "restart",
        "interruption.requested": "restart",
        "task.state_changed": "restart",
        "plan.state_changed": "restart",
        "cycle.ended": "restart",
        "conversation.state_changed": "restart",
        "interruption.completed": "restart",
    }


# ================================================================================================
# 6. audit and tracker (§2.9 "an INTERRUPTED audit event for every affected entity", §17.3)
# ================================================================================================
async def given_interrupted_plan_when_events_inspected_then_one_interrupted_event_per_task_plan_cycle_conversation(
    store: InMemoryConversationStore,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    bus = EventBus()
    audit = AuditLog(store, clock, ids)
    audit.subscribe(bus)
    tracker = ExecutionTracker(store, clock)
    tracker.subscribe(bus)
    telemetry = TelemetryService(clock)
    telemetry.subscribe(bus)
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="recorder")
    harness = Harness.build(store, bus, recorder, clock, ids, config)
    session, conversation = harness.running_conversation()
    cycle = harness.cycle(conversation)
    harness.fake.script(task_id="t1", hang_until_cancelled=True)
    plan, tasks = harness.plan(
        conversation, [_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=1
    )
    running = harness.start_loop(session, plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)
    await _settle()
    live = tracker.snapshot(session.session_id)
    assert live.session.status is SessionState.RUNNING and live.running_task_ids == ["t1"]
    recorder.clear()

    report = await harness.interrupt(session.session_id)
    await asyncio.wait_for(running, BOUND_S)

    interrupted = _interrupted_events(recorder.events)
    entities = [
        (e.event_type.value, e.task_id or e.plan_id or e.cycle_id or e.conversation_id)
        for e in interrupted
    ]
    assert entities == [
        ("task.state_changed", "t1"),
        ("task.state_changed", "t2"),
        ("task.state_changed", "t3"),
        ("plan.state_changed", PLAN_ID),
        ("cycle.ended", cycle.cycle_id),
        ("conversation.state_changed", conversation.conversation_id),
    ]
    # the runner's plan event carries stop_reason (phase 5 contract), every other one reason
    assert all(
        (e.payload.get("reason") or e.payload.get("stop_reason")) == REASON for e in interrupted
    )
    # the audit chain mirrors the bus and verifies
    verification = audit.verify(session.session_id)
    assert verification.valid is True
    trail = store.list_audit_events(session.session_id)
    assert [e.event_type for e in trail[-len(recorder.events) :]] == [
        e.event_type.value for e in recorder.events
    ]
    assert [e.payload for e in trail[-len(recorder.events) :]] == [
        e.payload for e in recorder.events
    ]
    # the snapshot reflects READY at session level and INTERRUPTED at conversation level
    snapshot = tracker.snapshot(session.session_id)
    assert snapshot.session.status is SessionState.READY
    assert snapshot.session.interrupted_at == clock.now()
    assert snapshot.conversation is not None
    assert snapshot.conversation.status is ConversationState.INTERRUPTED
    assert snapshot.conversation.conversation_id == conversation.conversation_id
    assert snapshot.plan is not None and snapshot.plan.status is PlanState.INTERRUPTED
    assert snapshot.plan.interrupted_task_count == 3 and snapshot.plan.stop_reason == REASON
    assert snapshot.cycle is not None and snapshot.cycle.status is CycleState.INTERRUPTED
    assert {t.task_id: t.status for t in snapshot.tasks} == {
        "t1": TaskState.INTERRUPTED,
        "t2": TaskState.INTERRUPTED,
        "t3": TaskState.INTERRUPTED,
    }
    assert snapshot.running_task_ids == []
    assert snapshot.last_event_type == EventType.INTERRUPTION_COMPLETED.value
    assert snapshot == tracker.rebuild(session.session_id)
    assert telemetry.metrics()["counters"]["interruptions_total"] == [{"labels": {}, "value": 1}]
    assert report.interrupted_task_ids == ["t1", "t2", "t3"]


# ================================================================================================
# 7. transport: abandon and best-effort remote close (§2.9, ADR-006)
# ================================================================================================
async def given_transport_call_in_flight_when_user_interrupts_then_abandoned_and_remote_closed(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.WAITING_MODEL_RESPONSE)
    harness.transport.hang_next("get")
    in_flight = asyncio.ensure_future(harness.transport.wait_for_reply("remote-0001", None))
    await asyncio.wait_for(harness.transport.wait_until_hanging(), BOUND_S)
    done = harness.handler.register_loop(session.session_id)

    async def loop() -> None:
        try:
            await in_flight
        except TransportError:
            pass
        finally:
            done.set()

    running = asyncio.ensure_future(loop())

    report = await harness.interrupt(session.session_id)
    await asyncio.wait_for(running, BOUND_S)

    assert in_flight.done() and isinstance(in_flight.exception(), TransportError)
    error = in_flight.exception()
    assert isinstance(error, TransportError)
    assert error.error_type is ErrorType.INTERRUPTED and error.error.error_code == "ABANDONED"
    assert harness.transport.closed == ["remote-0001"]
    assert harness.transport.posted == []
    assert report.loop_drained is True
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )


async def given_transport_close_failing_when_user_interrupts_then_error_swallowed_and_session_ready(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation(ConversationState.ACTIVE)
    harness.transport.enqueue_error(
        "close",
        TransportError(
            ErrorType.NETWORK_ERROR, "CONNECTION_ERROR", retryable=True, operation="CLOSE"
        ),
    )
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert report.session_status is SessionState.READY
    assert harness.transport.closed == []  # the scripted error fired before the record
    assert harness.read_session(session.session_id).status is SessionState.READY
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )
    assert _kinds(harness.recorder.events)[-2:] == [
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]


async def given_transport_close_raising_unexpected_exception_when_user_interrupts_then_still_ready(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(
        store, bus, recorder, clock, ids, config, transport=_ExplodingCloseGateway(clock)
    )
    session, _ = harness.running_conversation(ConversationState.RUNNING_PLAN)

    report = await harness.interrupt(session.session_id)

    assert report.session_status is SessionState.READY
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_conversation_without_remote_id_when_user_interrupts_then_no_close_attempted(
    harness: Harness,
) -> None:
    session = harness.session()
    conversation = harness.conversation(session, ConversationState.ACTIVE, remote=None)

    await harness.interrupt(session.session_id)

    assert harness.transport.closed == []
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )


async def given_no_transport_when_user_interrupts_then_cleanup_completes_without_remote_calls(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(store, bus, recorder, clock, ids, config, with_transport=False)
    session, conversation = harness.running_conversation(ConversationState.ACTIVE)

    report = await harness.interrupt(session.session_id)

    assert report.session_status is SessionState.READY
    assert harness.transport.closed == []
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )


# ================================================================================================
# 8. concurrency, persistence failures, resumed cleanup (ADR-015, ADR-016)
# ================================================================================================
async def given_interruption_in_progress_when_second_interrupt_requested_then_single_cleanup_and_identical_reports(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    harness.cycle(conversation)
    plan, _ = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    done = harness.handler.register_loop(session.session_id)
    token = harness.handler.token_for(session.session_id)

    async def loop() -> None:
        await token.wait()
        await asyncio.sleep(0.02)
        done.set()

    running = asyncio.ensure_future(loop())
    harness.recorder.clear()

    first, second = await asyncio.wait_for(
        asyncio.gather(
            harness.handler.interrupt(session.session_id),
            harness.handler.interrupt(session.session_id),
        ),
        BOUND_S,
    )
    await running

    assert first == second and first.nothing_to_interrupt is False
    assert first.interrupted_task_ids == ["t1", "t2"]
    assert len(harness.recorder.of_type(EventType.INTERRUPTION_REQUESTED)) == 1
    assert len(harness.recorder.of_type(EventType.INTERRUPTION_COMPLETED)) == 1
    assert len(harness.recorder.of_type(EventType.SESSION_STATE_CHANGED)) == 2
    assert len(harness.recorder.of_type(EventType.TASK_STATE_CHANGED)) == 2
    assert len(harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)) == 1
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_second_interrupt_waiter_cancelled_when_first_completes_then_cleanup_unaffected(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    done = harness.handler.register_loop(session.session_id)
    token = harness.handler.token_for(session.session_id)

    async def loop() -> None:
        await token.wait()
        await asyncio.sleep(0.02)
        done.set()

    running = asyncio.ensure_future(loop())
    first = asyncio.ensure_future(harness.handler.interrupt(session.session_id))
    await _settle()
    second = asyncio.ensure_future(harness.handler.interrupt(session.session_id))
    await _settle()
    second.cancel()

    report = await asyncio.wait_for(first, BOUND_S)
    await running

    with pytest.raises(asyncio.CancelledError):
        await second
    assert report.session_status is SessionState.READY
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_session_already_interrupting_after_a_failed_attempt_when_interrupt_called_then_cleanup_resumed(
    harness: Harness,
) -> None:
    """ADR-016: a session left ``INTERRUPTING`` (crash or failed attempt) resumes the cleanup
    where it stopped: no second ``RUNNING -> INTERRUPTING``, the sweep runs, ``READY`` follows."""
    session = harness.session(SessionState.INTERRUPTING)
    conversation = harness.conversation(session, ConversationState.RUNNING_PLAN)
    plan, _ = harness.plan(conversation, [_cmd("t1")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert report.nothing_to_interrupt is False and report.session_status is SessionState.READY
    assert _kinds(harness.recorder.events) == [
        ("interruption.requested", None),
        ("task.state_changed", "INTERRUPTED"),
        ("plan.state_changed", "INTERRUPTED"),
        ("conversation.state_changed", "INTERRUPTED"),
        ("session.state_changed", "READY"),
        ("interruption.completed", None),
    ]
    assert harness.read_session(session.session_id).status is SessionState.READY


async def given_store_failing_during_sweep_when_user_interrupts_then_persistence_error_and_session_failed(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    plan, _ = harness.plan(conversation, [_cmd("t1"), _cmd("t2")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    before_task = harness.task(session.session_id, "t1")

    def fail_after_request(event: Event) -> None:
        if event.event_type is EventType.INTERRUPTION_REQUESTED:
            harness.store.fail_next_write = True

    harness.bus.subscribe(fail_after_request, name="fail-after-request")
    harness.recorder.clear()

    with pytest.raises(PersistenceError):
        await harness.interrupt(session.session_id)

    # the failed transition was neither applied nor published (ADR-015)
    assert harness.task(session.session_id, "t1") == before_task
    assert harness.recorder.of_type(EventType.TASK_STATE_CHANGED) == []
    assert harness.read_plan(session.session_id).status is PlanState.RUNNING
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.RUNNING_PLAN
    )
    # best effort: the session is FAILED so that nothing runs until the recovery finishes the job
    stored_session = harness.read_session(session.session_id)
    assert stored_session.status is SessionState.FAILED
    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "INTERRUPTING"),
        ("interruption.requested", None),
        ("session.state_changed", "FAILED"),
    ]
    failed_event = harness.recorder.of_type(EventType.SESSION_STATE_CHANGED)[-1]
    assert failed_event.payload["reason"] == INTERRUPTION_FAILED_REASON
    assert harness.recorder.of_type(EventType.INTERRUPTION_COMPLETED) == []
    # the token stays cancelled: the loop must not resume
    assert harness.handler.token_for(session.session_id).is_cancelled is True


async def given_store_failing_on_session_failure_too_when_sweep_fails_then_original_error_raised(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    plan, _ = harness.plan(conversation, [_cmd("t1")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    writes = 0

    original_save_task = harness.store.save_task
    original_save_session = harness.store.save_session

    def failing_save_task(record: TaskRecord) -> None:
        raise PersistenceError("DISK_FULL")

    def failing_save_session(record: SessionRecord) -> None:
        nonlocal writes
        writes += 1
        if record.status is SessionState.FAILED:
            raise PersistenceError("DISK_FULL_AGAIN")
        original_save_session(record)

    harness.store.save_task = failing_save_task  # type: ignore[method-assign]
    harness.store.save_session = failing_save_session  # type: ignore[method-assign]
    try:
        with pytest.raises(PersistenceError) as raised:
            await harness.interrupt(session.session_id)
    finally:
        harness.store.save_task = original_save_task  # type: ignore[method-assign]
        harness.store.save_session = original_save_session  # type: ignore[method-assign]

    assert raised.value.error.error_code == "DISK_FULL"
    assert harness.read_session(session.session_id).status is SessionState.INTERRUPTING
    assert harness.recorder.of_type(EventType.INTERRUPTION_COMPLETED) == []


async def given_failed_attempt_when_interrupt_retried_then_second_call_finishes_the_cleanup(
    harness: Harness,
) -> None:
    session, conversation = harness.running_conversation()
    plan, _ = harness.plan(conversation, [_cmd("t1")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    original_save_session = harness.store.save_session

    def refuse_failed(record: SessionRecord) -> None:
        if record.status is SessionState.FAILED:
            raise PersistenceError("NO_FAILED_WRITE")
        original_save_session(record)

    def fail_after_request(event: Event) -> None:
        if event.event_type is EventType.INTERRUPTION_REQUESTED:
            harness.store.fail_next_write = True

    harness.store.save_session = refuse_failed  # type: ignore[method-assign]
    harness.bus.subscribe(fail_after_request, name="fail-after-request")
    with pytest.raises(PersistenceError):
        await harness.interrupt(session.session_id)
    harness.store.save_session = original_save_session  # type: ignore[method-assign]
    harness.bus.unsubscribe("fail-after-request")
    assert harness.read_session(session.session_id).status is SessionState.INTERRUPTING

    report = await harness.interrupt(session.session_id)

    assert report.session_status is SessionState.READY
    assert harness.task(session.session_id, "t1").status is TaskState.INTERRUPTED
    assert harness.read_conversation(conversation.conversation_id).status is (
        ConversationState.INTERRUPTED
    )


# ================================================================================================
# 9. rotation in progress (ADR-014): parent and child both INTERRUPTED
# ================================================================================================
async def given_rotating_parent_with_active_child_when_user_interrupts_then_both_conversations_interrupted(
    harness: Harness,
) -> None:
    session = harness.session()
    parent = harness.conversation(session, ConversationState.ROTATING, remote="remote-parent")
    parent_cycle = harness.cycle(parent)
    child = harness.conversation(
        session,
        ConversationState.WAITING_MODEL_RESPONSE,
        remote="remote-child",
        parent=parent.conversation_id,
    )
    child_cycle = harness.cycle(child)
    assert harness.read_session(session.session_id).current_conversation_id == child.conversation_id
    harness.recorder.clear()

    report = await harness.interrupt(session.session_id)

    assert harness.read_conversation(child.conversation_id).status is ConversationState.INTERRUPTED
    assert harness.read_conversation(parent.conversation_id).status is ConversationState.INTERRUPTED
    assert harness.read_cycle(child_cycle.cycle_id).status is CycleState.INTERRUPTED
    assert harness.read_cycle(parent_cycle.cycle_id).status is CycleState.INTERRUPTED
    assert harness.read_session(session.session_id).status is SessionState.READY
    assert report.conversation_id == child.conversation_id
    assert report.cycle_id == child_cycle.cycle_id
    conversation_events = harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)
    assert [(e.conversation_id, e.payload["from"]) for e in conversation_events] == [
        (child.conversation_id, "WAITING_MODEL_RESPONSE"),
        (parent.conversation_id, "ROTATING"),
    ]
    assert sorted(harness.transport.closed) == ["remote-child", "remote-parent"]


async def given_child_with_closed_parent_when_user_interrupts_then_only_child_interrupted(
    harness: Harness,
) -> None:
    session = harness.session()
    parent = harness.conversation(session, ConversationState.ROTATING, remote="remote-parent")
    parent = harness.lifecycle.transition_conversation(
        parent.conversation_id, ConversationState.CLOSED, reason="rotated", closure_reason="rotated"
    )
    child = harness.conversation(
        session,
        ConversationState.WAITING_MODEL_RESPONSE,
        remote="remote-child",
        parent=parent.conversation_id,
    )
    harness.recorder.clear()

    await harness.interrupt(session.session_id)

    assert harness.read_conversation(child.conversation_id).status is ConversationState.INTERRUPTED
    assert harness.read_conversation(parent.conversation_id) == parent
    assert len(harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)) == 1
    assert harness.transport.closed == ["remote-child"]


# ================================================================================================
# 10. tokens and loops (what the orchestrator relies on)
# ================================================================================================
def given_handler_when_token_requested_twice_then_same_token_until_reset(harness: Harness) -> None:
    first = harness.handler.token_for("sess-0001")
    assert isinstance(first, CancellationToken) and first.is_cancelled is False
    assert harness.handler.token_for("sess-0001") is first
    assert harness.handler.token_for("sess-0002") is not first


async def given_interrupted_session_when_token_requested_then_fresh_token_and_old_one_stays_cancelled(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    old = harness.handler.token_for(session.session_id)

    await harness.interrupt(session.session_id)

    assert old.is_cancelled is True and old.reason == REASON
    fresh = harness.handler.token_for(session.session_id)
    assert fresh is not old and fresh.is_cancelled is False and fresh.reason is None


async def given_token_cancelled_by_handler_when_checked_then_session_interrupted_error(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    token = harness.handler.token_for(session.session_id)
    harness.handler.raise_if_interrupted(session.session_id)  # not cancelled: nothing
    done = harness.handler.register_loop(session.session_id)

    async def loop() -> None:
        try:
            await token.wait()
            harness.handler.raise_if_interrupted(session.session_id)
        finally:
            done.set()

    running = asyncio.ensure_future(loop())
    await harness.interrupt(session.session_id)

    with pytest.raises(SessionInterruptedError) as raised:
        await asyncio.wait_for(running, BOUND_S)
    assert raised.value.error_type is ErrorType.INTERRUPTED
    assert raised.value.error.details == {"session_id": session.session_id, "reason": REASON}
    assert harness.handler.is_interrupting(session.session_id) is False


def given_handler_when_is_interrupting_checked_then_reflects_the_current_token(
    harness: Harness,
) -> None:
    assert harness.handler.is_interrupting("sess-0001") is False
    harness.handler.token_for("sess-0001").cancel(REASON)
    assert harness.handler.is_interrupting("sess-0001") is True


async def given_loop_registered_twice_when_interrupted_then_latest_registration_is_awaited(
    harness: Harness,
) -> None:
    session, _ = harness.running_conversation()
    stale = harness.handler.register_loop(session.session_id)
    current = harness.handler.register_loop(session.session_id)
    stale.set()
    token = harness.handler.token_for(session.session_id)

    async def loop() -> None:
        await token.wait()
        await asyncio.sleep(0.02)
        current.set()

    running = asyncio.ensure_future(loop())

    report = await harness.interrupt(session.session_id)
    await running

    assert report.loop_drained is True and current.is_set() is True


# ================================================================================================
# 11. a new user request right after READY (§19.16, ADR-006)
# ================================================================================================
async def given_interrupted_session_when_new_user_request_then_new_conversation_becomes_active(
    harness: Harness,
) -> None:
    session, first = harness.running_conversation()
    harness.cycle(first)
    plan, _ = harness.plan(first, [_cmd("t1")])
    harness.persist_states(plan, {"t1": TaskState.RUNNING}, PlanState.RUNNING)
    report = await harness.interrupt(session.session_id)
    assert report.session_status is SessionState.READY
    harness.recorder.clear()

    # immediately: the SAME session runs again, a NEW conversation is opened
    harness.lifecycle.transition_session(
        session.session_id, SessionState.RUNNING, reason="user_request"
    )
    second = harness.lifecycle.create_conversation(
        session.session_id, parent_conversation_id=first.conversation_id
    )
    second = harness.lifecycle.transition_conversation(
        second.conversation_id, ConversationState.ACTIVE
    )

    assert second.status is ConversationState.ACTIVE
    assert second.conversation_id != first.conversation_id
    assert second.parent_conversation_id == first.conversation_id
    assert second.session_id == session.session_id
    old = harness.read_conversation(first.conversation_id)
    assert old.status is ConversationState.INTERRUPTED and old.interrupted_at is not None
    assert old.current_plan_id == PLAN_ID
    current = harness.read_session(session.session_id)
    assert current.status is SessionState.RUNNING
    assert current.current_conversation_id == second.conversation_id
    assert current.started_at == session.started_at and current.interrupted_at is not None
    assert harness.handler.token_for(session.session_id).is_cancelled is False
    assert _kinds(harness.recorder.events) == [
        ("session.state_changed", "RUNNING"),
        ("conversation.created", None),
        ("conversation.state_changed", "ACTIVE"),
    ]


async def given_session_interrupted_twice_when_each_run_ends_then_each_interruption_independent(
    harness: Harness,
) -> None:
    session, first = harness.running_conversation()
    first_report = await harness.interrupt(session.session_id)
    harness.lifecycle.transition_session(
        session.session_id, SessionState.RUNNING, reason="user_request"
    )
    second = harness.conversation(session, ConversationState.ACTIVE, parent=first.conversation_id)
    harness.recorder.clear()

    second_report = await harness.interrupt(session.session_id)

    assert first_report.conversation_id == first.conversation_id
    assert second_report.conversation_id == second.conversation_id
    assert harness.read_conversation(second.conversation_id).status is (
        ConversationState.INTERRUPTED
    )
    assert harness.read_session(session.session_id).status is SessionState.READY
    assert len(harness.recorder.of_type(EventType.CONVERSATION_STATE_CHANGED)) == 1


# ================================================================================================
# 12. report and hygiene
# ================================================================================================
def given_interruption_report_when_compared_then_value_semantics(harness: Harness) -> None:
    now = harness.clock.now()
    report = InterruptionReport(
        session_id="sess-0001",
        reason=REASON,
        requested_at=now,
        completed_at=now,
        duration_ms=0,
        within_timeout=True,
        loop_drained=True,
        nothing_to_interrupt=False,
        interrupted_task_ids=["t1"],
        plan_id=PLAN_ID,
        cycle_id="cyc-0001",
        conversation_id="conv-0001",
        session_status=SessionState.READY,
    )
    assert report == replace(report)
    assert report != replace(report, duration_ms=1)
    with pytest.raises(AttributeError):
        report.duration_ms = 5  # type: ignore[misc]


def given_interruption_handler_module_when_inspected_then_no_wall_clock_or_randomness() -> None:
    source = open(handler_module.__file__ or "", encoding="utf-8").read()  # noqa: SIM115
    for forbidden in ("datetime.now(", "time.time(", "time.monotonic(", "uuid4(", "random."):
        assert forbidden not in source, forbidden


def given_interruption_package_when_imported_then_handler_and_report_exported() -> None:
    from agentic_local_app import interruption

    assert interruption.InterruptionHandler is InterruptionHandler
    assert interruption.InterruptionReport is InterruptionReport
    assert "InterruptionHandler" in interruption.__all__
    assert "InterruptionReport" in interruption.__all__
