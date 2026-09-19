"""Phase 9 — restart recovery (spec §3.18, §7.5, §17.4 ; ADR-016 ; acceptance criterion 13).

The ``RecoveryCoordinator`` applies the policy table of ADR-016 §2 at start-up, in order, each
action persisted then audited (``recovery.*``). Two families of tests:

- **crash and restart**: an application on a ``SqliteConversationStore`` in ``tmp_path`` is driven
  into a mid-flight state (a plan with a hanging task, a POST without GET, an unconfirmed POST),
  then "crashes" — its loop task is cancelled without any cleanup — and a second application on
  the same file recovers: interrupted entities, ``READY`` sessions, resumable sessions that
  ``ConversationManager.resume_session`` continues with a GET first, never re-executing a task;
- **the policy table**, row by row, on states built by hand in an in-memory store (orphans through
  a stub platform adapter, rotating parent, conversation already terminal, idempotence, empty
  store).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    CycleRecord,
    MessageRecord,
    PlanRecord,
    SessionBudget,
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
from agentic_local_app.execution.platform import PlatformAdapter, PosixPlatformAdapter
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import RecoveryAction, RecoveryCoordinator, RecoveryReport
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.persistence.sqlite_store import SqliteConversationStore
from integration.phase9_rig import (
    REMOTE_1,
    USER_MESSAGE,
    Rig,
    cmd_task,
    discovery_plan,
    final_answer,
    make_config,
    make_rig,
)

pytestmark = pytest.mark.phase9


# ================================================================================================
# helpers
# ================================================================================================
async def _crash(rig: Rig, session_id: str) -> None:
    """Kill the loop of ``session_id`` without any cleanup, then drop the store connection."""
    task = rig.manager.loop_task(session_id)
    assert task is not None and not task.done()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    rig.store.close()


def _sqlite_rig(tmp_path: Path, clock: FakeClock | None = None, **kwargs: Any) -> Rig:
    config = make_config(str(tmp_path / "data"))
    (tmp_path / "data").mkdir(exist_ok=True)
    store = SqliteConversationStore(tmp_path / "data" / "agentic.db")
    return make_rig(config, store=store, clock=clock, **kwargs)


def _actions(report: RecoveryReport) -> list[tuple[str, str, str, str, str]]:
    return [(a.entity, a.id, a.from_state, a.to_state, a.reason) for a in report.actions]


@dataclass
class _StubPlatform(PosixPlatformAdapter):
    """A platform adapter whose orphan termination is scripted and recorded."""

    calls: list[tuple[int, int | None, datetime]] = field(default_factory=list)
    alive: set[int] = field(default_factory=set)

    def __init__(self, config: Any, alive: set[int] | None = None) -> None:
        super().__init__(config, which=lambda name: None)
        self.calls = []
        self.alive = alive or set()

    def terminate_orphan(self, pid: int, pgid: int | None, started_at: datetime) -> bool:
        self.calls.append((pid, pgid, started_at))
        return pid in self.alive


def _coordinator(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
    *,
    platform: PlatformAdapter | None = None,
) -> tuple[RecoveryCoordinator, ConversationLifecycleManager]:
    lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
    return RecoveryCoordinator(
        store, lifecycle, bus, clock, ids, config, platform=platform
    ), lifecycle


@dataclass
class _Built:
    """A session with a RUNNING conversation, cycle, plan and tasks written through the real
    lifecycle manager and the store (the state the orchestrator leaves mid-plan)."""

    session_id: str
    conversation_id: str
    cycle_id: str
    plan_id: str


def _build_mid_plan(
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
    clock: FakeClock,
    *,
    conversation_state: ConversationState = ConversationState.RUNNING_PLAN,
    session_state: SessionState = SessionState.RUNNING,
    task_states: dict[str, TaskState] | None = None,
    plan_state: PlanState = PlanState.RUNNING,
    pid: int | None = 4_242,
) -> _Built:
    session = lifecycle.create_session(
        "goal",
        "message",
        "local-user",
        SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=60_000),
        False,
    )
    sid = session.session_id
    lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
    conversation = lifecycle.create_conversation(sid)
    cid = conversation.conversation_id
    lifecycle.update_conversation(cid, remote_conversation_id=REMOTE_1)
    path = {
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
    }[conversation_state]
    for step in path:
        lifecycle.transition_conversation(cid, step)
    now = clock.now()
    cycle = CycleRecord(
        cycle_id="cyc-mid",
        conversation_id=cid,
        session_id=sid,
        cycle_type=CycleType.DISCOVERY,
        status=CycleState.RUNNING,
        outbound_message_id="msg-mid",
        started_at=now,
    )
    store.save_cycle(cycle)
    plan = PlanRecord(
        plan_id="plan-mid",
        session_id=sid,
        conversation_id=cid,
        cycle_id="cyc-mid",
        plan_type=PlanType.DISCOVERY_PLAN,
        objective="objective",
        execution_policy=ExecutionPolicy.SEQUENTIAL,
        status=plan_state,
        task_count=3,
        started_at=now if plan_state is PlanState.RUNNING else None,
        created_at=now,
        updated_at=now,
    )
    store.save_plan(plan)
    states = task_states or {
        "t1": TaskState.COMPLETED,
        "t2": TaskState.RUNNING,
        "t3": TaskState.PENDING,
    }
    for index, (task_id, state) in enumerate(states.items()):
        fields: dict[str, Any] = {}
        if state is TaskState.RUNNING:
            fields = {"started_at": now, "attempt_count": 1, "pid": pid, "process_group_id": pid}
        elif state is TaskState.COMPLETED:
            fields = {"started_at": now, "ended_at": now, "exit_code": 0, "duration_ms": 0}
        store.save_task(
            TaskRecord(
                task_id=task_id,
                plan_id="plan-mid",
                session_id=sid,
                conversation_id=cid,
                order_index=index,
                type=TaskType.CMD,
                cmd=f"run {task_id}",
                status=state,
                created_at=now,
                updated_at=now,
                **fields,
            )
        )
    lifecycle.update_conversation(cid, current_cycle_id="cyc-mid", current_plan_id="plan-mid")
    if session_state is SessionState.INTERRUPTING:
        lifecycle.transition_session(sid, SessionState.INTERRUPTING, reason="user_interrupt")
    return _Built(sid, cid, "cyc-mid", "plan-mid")


# ================================================================================================
# 1. crash while a plan runs, restart on the same SQLite file
# ================================================================================================
async def given_store_with_running_task_when_recovery_runs_then_task_interrupted_and_not_reexecuted(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    first = _sqlite_rig(tmp_path, clock, run_recovery=True)
    assert first.manager.recovery_report is not None
    assert first.manager.recovery_report.actions == []  # empty store: audited no-op
    first.executor.script(task_id="t1", stdout=b"done")
    first.executor.script(task_id="t2", hang_until_cancelled=True)
    first.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1"), cmd_task("t2"), cmd_task("t3")]))
    session = await first.start()
    sid = session.session_id
    await asyncio.wait_for(first.executor.wait_spawned("t2"), 2.0)
    events_before_crash = first.store.count_audit_events(sid)
    await _crash(first, sid)

    second = _sqlite_rig(tmp_path, clock, run_recovery=True, ids=first.ids)
    report = second.manager.recovery_report

    # ---- the report ----------------------------------------------------------------------------
    assert report is not None and isinstance(report, RecoveryReport)
    assert report.sessions_ready == [sid]
    assert report.sessions_resumable == []
    assert report.orphans_terminated == []  # no platform adapter with a fake executor
    assert report.started_at == clock.now() and report.ended_at == clock.now()
    assert _actions(report) == [
        ("task", "t2", "RUNNING", "INTERRUPTED", "restart"),
        ("task", "t3", "PENDING", "INTERRUPTED", "restart"),
        ("plan", "plan-0", "RUNNING", "INTERRUPTED", "restart"),
        ("cycle", "cyc-0001", "RUNNING", "INTERRUPTED", "restart"),
        ("conversation", "conv-0001", "RUNNING_PLAN", "INTERRUPTED", "restart"),
        ("session", sid, "RUNNING", "INTERRUPTING", "restart"),
        ("session", sid, "INTERRUPTING", "READY", "restart"),
    ]
    assert all(isinstance(a, RecoveryAction) for a in report.actions)

    # ---- the persisted state (ADR-016 §2), nothing re-executed (§17.4) ----------------------------
    assert {t.task_id: (t.status, t.reason) for t in second.tasks(sid)} == {
        "t1": (TaskState.COMPLETED, None),
        "t2": (TaskState.INTERRUPTED, "restart"),
        "t3": (TaskState.INTERRUPTED, "restart"),
    }
    t2 = second.task(sid, "t2")
    assert t2.ended_at == clock.now() and t2.exit_code is None
    assert t2.stdout_ref is None and t2.stderr_ref is None
    plan = second.plan(sid, "plan-0")
    assert (plan.status, plan.stop_reason, plan.ended_at) == (
        PlanState.INTERRUPTED,
        "restart",
        clock.now(),
    )
    assert (plan.completed_task_count, plan.interrupted_task_count) == (1, 2)
    cycle = second.cycles("conv-0001")[0]
    assert cycle.status is CycleState.INTERRUPTED and cycle.ended_at == clock.now()
    conversation = second.conversation("conv-0001")
    assert conversation.status is ConversationState.INTERRUPTED
    assert conversation.interrupted_at == clock.now()
    assert conversation.current_plan_id == "plan-0"  # kept for the reading (07 §2)
    restored = second.session(sid)
    assert restored.status is SessionState.READY
    assert restored.interrupted_at == clock.now()
    assert restored.consumed_cycles == 1 and restored.consumed_plans == 1
    assert second.executor.calls == []
    assert second.transport.posted == [] and second.transport.inits == []

    # ---- events and audit chain continued across the restart ------------------------------------
    kinds = second.event_kinds()
    assert kinds[0] == ("recovery.started", None)
    assert kinds[-1] == ("recovery.completed", None)
    assert len([k for k in kinds if k[0] == "recovery.action"]) == 7
    assert [k for k in kinds if k[0] == "task.state_changed"] == [
        ("task.state_changed", "INTERRUPTED"),
        ("task.state_changed", "INTERRUPTED"),
    ]
    assert ("plan.state_changed", "INTERRUPTED") in kinds
    assert ("cycle.ended", "INTERRUPTED") in kinds  # the handler-style payload carries "to"
    assert ("conversation.state_changed", "INTERRUPTED") in kinds
    assert [k for k in kinds if k[0] == "session.state_changed"] == [
        ("session.state_changed", "INTERRUPTING"),
        ("session.state_changed", "READY"),
    ]
    started = second.events(EventType.RECOVERY_STARTED)[0]
    assert started.session_id == sid
    assert started.payload["findings"] == {
        "running_tasks": 1,
        "open_plans": 1,
        "running_cycles": 1,
        "active_conversations": 1,
        "open_sessions": 1,
    }
    action = second.events(EventType.RECOVERY_ACTION)[0]
    assert (action.session_id, action.plan_id, action.task_id) == (sid, "plan-0", "t2")
    assert action.payload == {
        "entity": "task",
        "entity_id": "t2",
        "id": "t2",
        "from": "RUNNING",
        "to": "INTERRUPTED",
        "reason": "restart",
    }
    completed = second.events(EventType.RECOVERY_COMPLETED)[0]
    assert completed.payload["actions"] == 7
    assert completed.payload["sessions_ready"] == [sid]
    assert completed.payload["sessions_resumable"] == []
    assert all(e.session_id == sid for e in second.events())
    assert second.store.count_audit_events(sid) == events_before_crash + len(second.events())
    assert second.app.audit.verify(sid).valid is True
    assert [
        e.payload.get("reason") for e in second.events(EventType.CONVERSATION_STATE_CHANGED)
    ] == ["restart"]

    # ---- the session accepts a new request at once (criterion 16) ----------------------------------
    second.reply(
        "remote-0001",
        discovery_plan(
            "remote-0001", message_id="model-msg-0010", plan_id="plan-r0", tasks=[cmd_task("r1")]
        ),
        final_answer("remote-0001", message_id="model-msg-0011"),
    )
    await second.manager.continue_session(sid, "again")
    ended = await second.wait(sid)
    assert ended.status is SessionState.COMPLETED
    assert [c.conversation_id for c in second.conversations(sid)] == ["conv-0001", "conv-0002"]
    assert second.conversation("conv-0002").parent_conversation_id == "conv-0001"
    assert [call.cmd for call in second.executor.calls] == ["run r1"]
    second.app.close()


async def given_posted_message_without_reply_when_recovery_runs_then_get_retried_first(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    first = _sqlite_rig(tmp_path, clock)
    first.transport.hang_next("get")
    session = await first.start()
    sid = session.session_id
    await asyncio.wait_for(first.transport.wait_until_hanging(), 2.0)
    assert first.conversation("conv-0001").status is ConversationState.WAITING_MODEL_RESPONSE
    await _crash(first, sid)

    second = _sqlite_rig(tmp_path, clock, run_recovery=True, ids=first.ids)
    report = second.manager.recovery_report
    assert report is not None
    assert report.sessions_resumable == [sid]
    assert report.sessions_ready == []
    assert _actions(report) == [
        (
            "conversation",
            "conv-0001",
            "WAITING_MODEL_RESPONSE",
            "WAITING_MODEL_RESPONSE",
            "resumable",
        )
    ]
    assert second.session(sid).status is SessionState.RUNNING  # stays RUNNING (ADR-016)
    assert second.conversation("conv-0001").status is ConversationState.WAITING_MODEL_RESPONSE
    assert second.cycles("conv-0001")[0].status is CycleState.RUNNING
    second.executor.script(task_id="t1", stdout=b"ok")
    second.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )
    second.recorder.clear()

    resumed = await second.manager.resume_session(sid)
    ended = await second.wait(sid)

    assert resumed.status is SessionState.RUNNING
    assert ended.status is SessionState.COMPLETED
    # GET first, with the persisted cursor; the user_request is never POSTed again
    assert second.transport.get_calls[0] == (REMOTE_1, None)
    assert second.transport.inits == []
    assert second.posted_types() == ["execution_result"]
    assert second.posted(0)["message_id"] == "msg-0002"
    messages = second.store.list_messages("conv-0001")
    assert [(m.direction, m.message_type, m.message_id) for m in messages] == [
        (MessageDirection.OUTBOUND, MessageType.USER_REQUEST, "msg-0001"),
        (MessageDirection.INBOUND, MessageType.DISCOVERY_PLAN, "model-msg-0001"),
        (MessageDirection.OUTBOUND, MessageType.EXECUTION_RESULT, "msg-0002"),
        (MessageDirection.INBOUND, MessageType.FINAL_ANSWER, "model-msg-0002"),
    ]
    assert messages[0].post_confirmed is True
    assert ended.consumed_cycles == 2 and ended.consumed_plans == 1
    cycles = second.cycles("conv-0001")
    assert [c.status for c in cycles] == [CycleState.COMPLETED, CycleState.COMPLETED]
    assert second.event_kinds()[0] == ("message.inbound", None)
    assert second.app.audit.verify(sid).valid is True
    second.app.close()


async def given_unconfirmed_post_when_recovery_runs_then_post_replayed_with_same_message_id(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    first = _sqlite_rig(tmp_path, clock)
    first.transport.hang_next("post")
    session = await first.start()
    sid = session.session_id
    await asyncio.wait_for(first.transport.wait_until_hanging(), 2.0)
    pending = first.store.get_message("msg-0001")
    assert pending is not None and pending.post_confirmed is False
    assert first.conversation("conv-0001").status is ConversationState.WAITING_MODEL_RESPONSE
    await _crash(first, sid)

    second = _sqlite_rig(tmp_path, clock, run_recovery=True, ids=first.ids)
    report = second.manager.recovery_report
    assert report is not None and report.sessions_resumable == [sid]
    second.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )
    second.recorder.clear()

    await second.manager.resume_session(sid)
    ended = await second.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert second.posted_types() == ["user_request", "execution_result"]
    assert second.posted(0) == pending.payload  # same message_id, same content (ADR-004)
    assert second.transport.get_calls[0] == (REMOTE_1, None)
    replayed = second.store.get_message("msg-0001")
    assert replayed is not None and replayed.post_confirmed is True
    assert second.event_kinds()[0] == ("message.outbound", None)
    assert second.events(EventType.MESSAGE_OUTBOUND)[0].payload["message_id"] == "msg-0001"
    second.app.close()


async def given_resumed_session_when_get_still_unanswered_then_failure_policy_applies(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    first = _sqlite_rig(tmp_path, clock)
    first.transport.hang_next("get")
    session = await first.start()
    sid = session.session_id
    await asyncio.wait_for(first.transport.wait_until_hanging(), 2.0)
    await _crash(first, sid)
    second = _sqlite_rig(
        tmp_path,
        clock,
        run_recovery=True,
        ids=first.ids,
        reply_timeout_ms=1_000,
    )

    await second.manager.resume_session(sid)
    ended = await second.wait(sid)

    assert ended.status is SessionState.FAILED
    assert len(second.transport.get_calls) == 4
    assert [d.decision for d in second.store.list_retry_decisions(sid)] == [
        "retry",
        "retry",
        "retry",
        "fail",
    ]
    assert second.conversation("conv-0001").status is ConversationState.FAILED
    second.app.close()


async def given_non_resumable_session_when_resume_requested_then_refused(tmp_path: Path) -> None:
    rig = _sqlite_rig(tmp_path, run_recovery=True)
    rig.script_java_scenario()
    session = await rig.run()

    with pytest.raises(ValueError):
        await rig.manager.resume_session(session.session_id)  # COMPLETED: nothing to resume
    with pytest.raises(KeyError):
        await rig.manager.resume_session("sess-unknown")
    rig.app.close()


# ================================================================================================
# 2. the policy table, row by row (ADR-016 §2)
# ================================================================================================
def given_empty_store_when_recovery_runs_then_empty_report_and_noop_audited(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, _ = _coordinator(store, bus, clock, ids, config)

    report = coordinator.recover()

    assert report.actions == []
    assert report.orphans_terminated == []
    assert report.sessions_ready == [] and report.sessions_resumable == []
    assert report.started_at == clock.now() and report.ended_at == clock.now()
    assert [(e.event_type.value, e.session_id) for e in recorder.events] == [
        ("recovery.started", "*"),
        ("recovery.completed", "*"),
    ]
    assert recorder.events[0].payload["findings"] == {
        "running_tasks": 0,
        "open_plans": 0,
        "running_cycles": 0,
        "active_conversations": 0,
        "open_sessions": 0,
    }
    assert recorder.events[1].payload["actions"] == 0


def given_live_orphan_started_with_task_when_recovery_runs_then_terminated_and_task_interrupted(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    platform = _StubPlatform(config.execution, alive={4_242})
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config, platform=platform)
    built = _build_mid_plan(store, lifecycle, clock)
    recorder.clear()

    report = coordinator.recover()

    assert platform.calls == [(4_242, 4_242, clock.now())]
    assert report.orphans_terminated == [4_242]
    assert _actions(report)[:2] == [
        ("task", "t2", "RUNNING", "RUNNING", "orphan_terminated"),
        ("task", "t2", "RUNNING", "INTERRUPTED", "restart"),
    ]
    task = store.get_task(built.session_id, "t2")
    assert task is not None and task.status is TaskState.INTERRUPTED and task.pid == 4_242
    orphan_event = recorder.of_type(EventType.RECOVERY_ACTION)[0]
    assert orphan_event.payload["reason"] == "orphan_terminated"
    assert orphan_event.payload["details"] == {"pid": 4_242, "process_group_id": 4_242}
    assert orphan_event.task_id == "t2"


def given_dead_or_reused_pid_when_recovery_runs_then_nothing_killed_but_task_interrupted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    platform = _StubPlatform(config.execution, alive=set())
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config, platform=platform)
    built = _build_mid_plan(store, lifecycle, clock)

    report = coordinator.recover()

    assert platform.calls == [(4_242, 4_242, clock.now())]
    assert report.orphans_terminated == []
    assert [a.reason for a in report.actions if a.entity == "task"] == ["restart", "restart"]
    task = store.get_task(built.session_id, "t2")
    assert task is not None and task.status is TaskState.INTERRUPTED


def given_running_task_without_pid_when_recovery_runs_then_platform_not_consulted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    platform = _StubPlatform(config.execution, alive={4_242})
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config, platform=platform)
    _build_mid_plan(store, lifecycle, clock, pid=None)

    report = coordinator.recover()

    assert platform.calls == []
    assert report.orphans_terminated == []


def given_pending_plan_with_waiting_tasks_when_recovery_runs_then_plan_and_tasks_interrupted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(
        store,
        lifecycle,
        clock,
        plan_state=PlanState.PENDING,
        task_states={"t1": TaskState.PENDING, "t2": TaskState.WAITING_DEPENDENCY},
    )

    report = coordinator.recover()

    assert _actions(report)[:3] == [
        ("task", "t1", "PENDING", "INTERRUPTED", "restart"),
        ("task", "t2", "WAITING_DEPENDENCY", "INTERRUPTED", "restart"),
        ("plan", "plan-mid", "PENDING", "INTERRUPTED", "restart"),
    ]
    plan = store.get_plan(built.session_id, "plan-mid")
    assert plan is not None
    assert (plan.status, plan.stop_reason, plan.interrupted_task_count, plan.task_count) == (
        PlanState.INTERRUPTED,
        "restart",
        2,
        2,
    )
    assert store.get_session(built.session_id).status is SessionState.READY  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "state",
    [ConversationState.ACTIVE, ConversationState.WAITING_MODEL_RESPONSE],
    ids=str,
)
def given_active_conversation_without_pending_message_when_recovery_runs_then_interrupted(
    state: ConversationState,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(store, lifecycle, clock, conversation_state=state)

    report = coordinator.recover()

    conversation = store.get_conversation(built.conversation_id)
    assert conversation is not None and conversation.status is ConversationState.INTERRUPTED
    assert report.sessions_ready == [built.session_id]
    assert report.sessions_resumable == []
    assert (
        "conversation",
        built.conversation_id,
        state.value,
        "INTERRUPTED",
        "restart",
    ) in _actions(report)


def given_rotation_in_flight_when_recovery_runs_then_parent_and_child_interrupted(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(
        store, lifecycle, clock, conversation_state=ConversationState.WAITING_MODEL_RESPONSE
    )
    sid = built.session_id
    lifecycle.transition_conversation(built.conversation_id, ConversationState.ROTATING)
    child = lifecycle.create_conversation(
        sid,
        parent_conversation_id=built.conversation_id,
        context_window_state=ContextWindowState.SATURATED,
    )
    lifecycle.update_conversation(child.conversation_id, remote_conversation_id="remote-0002")
    lifecycle.transition_conversation(child.conversation_id, ConversationState.ACTIVE)
    lifecycle.transition_conversation(
        child.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    now = clock.now()
    store.save_message(
        MessageRecord(
            message_id="msg-resume",
            session_id=sid,
            conversation_id=child.conversation_id,
            direction=MessageDirection.OUTBOUND,
            message_type=MessageType.CONTEXT_RESUME_REQUEST,
            payload={"type": "context_resume_request"},
            size_bytes=30,
            post_confirmed=True,
            posted_at=now,
            created_at=now,
        )
    )

    report = coordinator.recover()

    parent = store.get_conversation(built.conversation_id)
    stored_child = store.get_conversation(child.conversation_id)
    assert parent is not None and parent.status is ConversationState.INTERRUPTED
    assert stored_child is not None and stored_child.status is ConversationState.INTERRUPTED
    assert report.sessions_ready == [sid] and report.sessions_resumable == []
    assert store.get_session(sid).status is SessionState.READY  # type: ignore[union-attr]
    conversation_actions = [a for a in report.actions if a.entity == "conversation"]
    assert [(a.id, a.from_state) for a in conversation_actions] == [
        (built.conversation_id, "ROTATING"),
        (child.conversation_id, "WAITING_MODEL_RESPONSE"),
    ]


def given_session_interrupting_at_restart_when_recovery_runs_then_cleanup_finished_and_ready(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(store, lifecycle, clock, session_state=SessionState.INTERRUPTING)

    report = coordinator.recover()

    assert store.get_session(built.session_id).status is SessionState.READY  # type: ignore[union-attr]
    assert report.sessions_ready == [built.session_id]
    session_actions = [a for a in report.actions if a.entity == "session"]
    assert [(a.from_state, a.to_state) for a in session_actions] == [("INTERRUPTING", "READY")]


def given_session_running_with_failed_conversation_when_recovery_runs_then_session_failed(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(
        store,
        lifecycle,
        clock,
        conversation_state=ConversationState.ACTIVE,
        plan_state=PlanState.PENDING,
        task_states={"t1": TaskState.PENDING},
    )
    lifecycle.transition_conversation(
        built.conversation_id, ConversationState.FAILED, reason="failure"
    )

    report = coordinator.recover()

    assert store.get_session(built.session_id).status is SessionState.FAILED  # type: ignore[union-attr]
    assert report.sessions_ready == [] and report.sessions_resumable == []
    assert report.sessions_failed == [built.session_id]
    assert ("session", built.session_id, "RUNNING", "FAILED", "restart") in _actions(report)


def given_session_running_with_completed_conversation_when_recovery_runs_then_session_completed(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(
        store,
        lifecycle,
        clock,
        conversation_state=ConversationState.WAITING_MODEL_RESPONSE,
        plan_state=PlanState.PENDING,
        task_states={"t1": TaskState.PENDING},
    )
    lifecycle.transition_conversation(
        built.conversation_id, ConversationState.COMPLETED, final_answer_received=True
    )

    report = coordinator.recover()

    assert store.get_session(built.session_id).status is SessionState.COMPLETED  # type: ignore[union-attr]
    conversation = store.get_conversation(built.conversation_id)
    assert conversation is not None and conversation.status is ConversationState.WAITING_USER
    assert report.sessions_completed == [built.session_id]
    assert ("session", built.session_id, "RUNNING", "COMPLETED", "restart") in _actions(report)


def given_recovered_store_when_recovery_runs_again_then_no_action(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    built = _build_mid_plan(store, lifecycle, clock)
    first = coordinator.recover()
    assert first.actions
    recorder.clear()

    second = coordinator.recover()

    assert second.actions == []
    assert second.sessions_ready == [] and second.sessions_resumable == []
    assert [(e.event_type.value, e.session_id) for e in recorder.events] == [
        ("recovery.started", "*"),
        ("recovery.completed", "*"),
    ]
    assert store.get_session(built.session_id).status is SessionState.READY  # type: ignore[union-attr]


def given_terminal_sessions_only_when_recovery_runs_then_untouched(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    ready = lifecycle.create_session(
        "goal", "m", "u", SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1), False
    )
    failed = lifecycle.create_session(
        "goal", "m", "u", SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1), False
    )
    lifecycle.transition_session(failed.session_id, SessionState.RUNNING)
    lifecycle.transition_session(failed.session_id, SessionState.FAILED)

    report = coordinator.recover()

    assert report.actions == []
    assert store.get_session(ready.session_id).status is SessionState.READY  # type: ignore[union-attr]
    assert store.get_session(failed.session_id).status is SessionState.FAILED  # type: ignore[union-attr]


def given_session_running_with_new_conversation_when_recovery_runs_then_session_ready(
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    coordinator, lifecycle = _coordinator(store, bus, clock, ids, config)
    session = lifecycle.create_session(
        "goal", "m", "u", SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1), False
    )
    lifecycle.transition_session(session.session_id, SessionState.RUNNING, reason="user_request")
    conversation = lifecycle.create_conversation(session.session_id)  # crash before ACTIVE

    report = coordinator.recover()

    assert report.sessions_ready == [session.session_id]
    assert store.get_session(session.session_id).status is SessionState.READY  # type: ignore[union-attr]
    stored = store.get_conversation(conversation.conversation_id)
    assert (
        stored is not None and stored.status is ConversationState.NEW
    )  # left as is (no table row)
    assert [(a.entity, a.to_state) for a in report.actions] == [
        ("session", "INTERRUPTING"),
        ("session", "READY"),
    ]


# ================================================================================================
# 3. a session opened without an opening message survives a restart (ADR-028)
# ================================================================================================
async def given_an_empty_session_when_the_application_restarts_then_untouched_and_still_usable(
    tmp_path: Path,
) -> None:
    """ADR-028 §1: a ``READY`` session with no conversation is not an open session — the recovery
    only settles ``RUNNING`` / ``INTERRUPTING`` ones (ADR-016 §2), so it survives as it was and its
    first message opens its first conversation on the other side of the restart."""
    clock = FakeClock()
    first = _sqlite_rig(tmp_path, clock, run_recovery=True)
    session = await first.manager.start_session(user_id="alice")
    sid = session.session_id
    audited_before = first.store.count_audit_events(sid)
    first.store.close()  # the process stops, nothing was running

    second = _sqlite_rig(tmp_path, clock, run_recovery=True, ids=first.ids)
    report = second.manager.recovery_report

    assert report is not None and report.actions == []
    assert report.sessions_ready == [] and report.sessions_resumable == []
    restored = second.session(sid)
    assert restored == session  # byte for byte the record the first process wrote
    assert restored.status is SessionState.READY
    assert restored.user_id == "alice"
    assert second.conversations(sid) == []
    assert second.store.count_audit_events(sid) == audited_before
    assert second.app.audit.verify(sid).valid is True

    # still usable: the first message runs the loop in the restarted process
    second.script_java_scenario()
    await second.manager.continue_session(sid, USER_MESSAGE)
    ended = await second.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.goal == USER_MESSAGE
    assert [c.conversation_id for c in second.conversations(sid)] == ["conv-0001"]
    assert second.app.audit.verify(sid).valid is True
