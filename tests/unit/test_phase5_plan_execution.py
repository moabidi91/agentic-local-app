"""Phase 5 — plan execution (spec §2.4, §3.7, §5.2, §5.3, §8, §18.2 ; ADR-003, ADR-006, ADR-008,
ADR-009, ADR-011, ADR-012 §3, ADR-015, ADR-016, ADR-017, ADR-018, ADR-019 §1, ADR-026).

Everything runs on the doubles of §18.3 — ``FakeCommandExecutor`` (no process), ``FakeClock``,
``InMemoryConversationStore``, ``EventBus`` + ``RecordingSubscriber``, ``SequentialIdGenerator`` —
and never waits more than a few milliseconds of real time. Plans are produced exactly as in
production, through ``ProtocolAdapter.plan_to_records`` from plan JSON, so the effective flags,
timeouts and budgets are the adapter's.

Sections: harness · fake barrier · sequential nominal · stop conditions (ADR-009 table) · parallel
(workers, order, depends_on, resource_lock, drain) · interruption · budget · chunk_request ·
spawn error · pid / live output / truncation · counters · persistence before publication ·
invalid transitions · determinism. The working space of ADR-026 is checked where it enters the
runner: the ``env`` overlay of the ``CommandSpec``.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app.config import AppConfig, ScratchSection
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import (
    ErrorType,
    InvalidTransitionError,
    PersistenceError,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    BlobRecord,
    ConversationRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ConversationState,
    MessageType,
    OutputStream,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
)
from agentic_local_app.execution import plan_runner as plan_runner_module
from agentic_local_app.execution.executor import CancellationToken, CommandSpec, OutputChunk
from agentic_local_app.execution.payload_guard import PayloadGuard
from agentic_local_app.execution.plan_runner import PlanOutcome, PlanRunner
from agentic_local_app.execution.scratch import (
    ENV_SCRATCH_DIR,
    ENV_SESSION_ID,
    ENV_WORKING_SPACE,
    ScratchManager,
)
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.observability.execution_tracker import ExecutionTracker
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import InboundMessage, ProtocolAdapter
from agentic_local_app.protocol.messages import Envelope, PlanContent
from agentic_local_app.resilience.failure_manager import FailureManager
from agentic_local_app.testing.fake_executor import FakeCommandExecutor

pytestmark = pytest.mark.phase5

SESSION_ID = "sess-0001"
CONVERSATION_ID = "conv-0001"
CYCLE_ID = "cyc-0001"
PLAN_ID = "plan-1"

#: Real-time bound of any test that waits on the runner (the runner itself never sleeps).
BOUND_S = 1.0
#: Small drains for the forced-termination paths (real asyncio waits, well under 50 ms).
SHORT_DRAIN_MS = 10


# ================================================================================================
# harness
# ================================================================================================
def _cmd(task_id: str, cmd: str | None = None, **fields: Any) -> dict[str, Any]:
    task: dict[str, Any] = {"task_id": task_id, "type": "cmd", "cmd": cmd or f"run {task_id}"}
    task.update(fields)
    return task


def _chunk(task_id: str, ref: str, offset: int, max_bytes: int, **fields: Any) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_id": task_id,
        "type": "chunk_request",
        "ref_task_id": ref,
        "byte_offset": offset,
        "max_bytes": max_bytes,
    }
    task.update(fields)
    return task


def _with_execution(config: AppConfig, **execution: Any) -> AppConfig:
    return config.model_copy(update={"execution": config.execution.model_copy(update=execution)})


def _is_dir(path: Path) -> bool:
    """Filesystem probe kept out of the async bodies (ruff ASYNC240 forbids them there)."""
    return path.is_dir()


async def _settle(rounds: int = 25) -> None:
    """Let every ready callback of the loop run: the runner and the fake only ever need a few."""
    for _ in range(rounds):
        await asyncio.sleep(0)


def _transitions(recorder: RecordingSubscriber, task_id: str | None) -> list[tuple[str, str]]:
    """``(from, to)`` of the state changes of one task (``task_id``) or of the plan (``None``)."""
    wanted = EventType.TASK_STATE_CHANGED if task_id is not None else EventType.PLAN_STATE_CHANGED
    return [
        (e.payload["from"], e.payload["to"])
        for e in recorder.events
        if e.event_type is wanted and e.task_id == task_id
    ]


def _terminal_event(recorder: RecordingSubscriber, task_id: str) -> Event:
    events = [
        e
        for e in recorder.of_type(EventType.TASK_STATE_CHANGED)
        if e.task_id == task_id and e.payload["from"] == "RUNNING"
    ]
    assert len(events) == 1, events
    return events[0]


@dataclass
class Harness:
    store: InMemoryConversationStore
    bus: EventBus
    recorder: RecordingSubscriber
    clock: FakeClock
    ids: SequentialIdGenerator
    config: AppConfig
    fake: FakeCommandExecutor
    runner: PlanRunner
    session: SessionRecord

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
        failure_manager: FailureManager | None = None,
        max_total_duration_ms: int = 60_000,
        session_started: bool = True,
        scratch: ScratchManager | None = None,
    ) -> Harness:
        fake = FakeCommandExecutor(clock)
        runner = PlanRunner(
            store,
            bus,
            fake,
            PayloadGuard(config.payload),
            clock,
            ids,
            config,
            failure_manager=failure_manager,
            scratch=scratch,
        )
        now = clock.now()
        session = SessionRecord(
            session_id=SESSION_ID,
            status=SessionState.RUNNING,
            goal="goal",
            user_message="message",
            user_id="local-user",
            auto_close_on_final_answer=False,
            budget=SessionBudget(
                max_cycles=20, max_plans=10, max_total_duration_ms=max_total_duration_ms
            ),
            consumed_cycles=1,
            consumed_plans=1,
            current_conversation_id=CONVERSATION_ID,
            started_at=now if session_started else None,
            created_at=now,
            updated_at=now,
        )
        store.save_session(session)
        store.save_conversation(
            ConversationRecord(
                conversation_id=CONVERSATION_ID,
                session_id=SESSION_ID,
                status=ConversationState.RUNNING_PLAN,
                auto_close_on_final_answer=False,
                created_at=now,
                updated_at=now,
            )
        )
        return cls(store, bus, recorder, clock, ids, config, fake, runner, session)

    @classmethod
    def fresh(cls, config: AppConfig) -> Harness:
        """An independent set of doubles (determinism tests)."""
        bus = EventBus()
        recorder = RecordingSubscriber()
        bus.subscribe(recorder, name="recorder")
        return cls.build(
            InMemoryConversationStore(), bus, recorder, FakeClock(), SequentialIdGenerator(), config
        )

    # ---- plans ---------------------------------------------------------------------------
    def plan(
        self,
        tasks: list[dict[str, Any]],
        *,
        policy: str = "sequential",
        workers: int | None = None,
        default_max_output_bytes: int | None = None,
        plan_id: str = PLAN_ID,
    ) -> tuple[PlanRecord, list[TaskRecord]]:
        """PENDING records exactly as the orchestrator gets them from the adapter, persisted."""
        raw: dict[str, Any] = {
            "plan_id": plan_id,
            "objective": "objective",
            "execution_policy": policy,
            "tasks": tasks,
        }
        if workers is not None:
            raw["max_parallel_workers"] = workers
        if default_max_output_bytes is not None:
            raw["default_max_output_bytes"] = default_max_output_bytes
        content = PlanContent.model_validate(raw)
        inbound = InboundMessage(
            envelope=Envelope(
                type=MessageType.EXECUTION_PLAN,
                conversation_id=CONVERSATION_ID,
                message_id="msg-in-1",
                content=content.model_dump(mode="json", exclude_none=True),
            ),
            content=content,
            message_type=MessageType.EXECUTION_PLAN,
            plan_type=PlanType.EXECUTION_PLAN,
        )
        conversation = self.store.get_conversation(CONVERSATION_ID)
        assert conversation is not None
        plan, records = ProtocolAdapter(self.config).plan_to_records(
            inbound,
            session=self.session,
            conversation=conversation,
            cycle_id=CYCLE_ID,
            clock=self.clock,
        )
        self.store.save_plan(plan)
        self.store.save_tasks(records)
        return plan, records

    # ---- running -------------------------------------------------------------------------
    async def run(
        self,
        plan: PlanRecord,
        tasks: list[TaskRecord],
        *,
        interrupt: CancellationToken | None = None,
    ) -> PlanOutcome:
        return await asyncio.wait_for(
            self.runner.run(plan, tasks, self.session, interrupt=interrupt or CancellationToken()),
            BOUND_S,
        )

    def start(
        self,
        plan: PlanRecord,
        tasks: list[TaskRecord],
        *,
        interrupt: CancellationToken | None = None,
    ) -> asyncio.Task[PlanOutcome]:
        return asyncio.ensure_future(
            self.runner.run(plan, tasks, self.session, interrupt=interrupt or CancellationToken())
        )

    # ---- reading back --------------------------------------------------------------------
    def task(self, task_id: str) -> TaskRecord:
        record = self.store.get_task(SESSION_ID, task_id)
        assert record is not None, task_id
        return record

    def stored_plan(self, plan_id: str = PLAN_ID) -> PlanRecord:
        record = self.store.get_plan(SESSION_ID, plan_id)
        assert record is not None, plan_id
        return record

    def statuses(self, plan_id: str = PLAN_ID) -> dict[str, TaskState]:
        return {t.task_id: t.status for t in self.store.list_tasks(SESSION_ID, plan_id=plan_id)}

    def blob(self, task_id: str, stream: OutputStream) -> BlobRecord | None:
        return self.store.get_blob_for_task(SESSION_ID, task_id, stream)


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


@pytest.fixture
def short_drain(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> Harness:
    """Same doubles, drains of ``SHORT_DRAIN_MS`` for the forced-termination paths."""
    return Harness.build(
        store,
        bus,
        recorder,
        clock,
        ids,
        _with_execution(
            config,
            cancel_drain_timeout_ms=SHORT_DRAIN_MS,
            interrupt_drain_timeout_ms=SHORT_DRAIN_MS,
        ),
    )


# ================================================================================================
# 0. the fake's barrier (additive extension of the phase-4 double)
# ================================================================================================
async def given_held_script_when_released_then_completes_with_scripted_result(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"done", hold=True)
    running = asyncio.ensure_future(
        fake.execute(CommandSpec("t1", "run", 1_000, "."), cancel=CancellationToken())
    )
    await asyncio.wait_for(fake.wait_spawned("t1"), BOUND_S)
    await _settle()
    assert running.done() is False
    assert fake.active == {"t1"} and fake.is_held("t1") is True
    fake.release("t1")
    raw = await asyncio.wait_for(running, BOUND_S)
    assert raw.outcome is TaskState.COMPLETED and raw.stdout == b"done"
    assert fake.active == set() and fake.max_active == 1 and fake.spawn_order == ["t1"]


async def given_held_script_when_token_cancelled_then_cancelled_like_a_hang(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"partial", hold=True)
    token = CancellationToken()
    running = asyncio.ensure_future(
        fake.execute(CommandSpec("t1", "run", 1_000, "."), cancel=token)
    )
    await asyncio.wait_for(fake.wait_spawned("t1"), BOUND_S)
    token.cancel("plan_stopped")
    raw = await asyncio.wait_for(running, BOUND_S)
    assert raw.outcome is TaskState.CANCELLED and raw.stdout == b"partial"
    assert fake.cancellations == [("t1", "plan_stopped")]


async def given_held_script_ignoring_cancel_when_token_cancelled_then_still_held_until_release(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", hold=True, ignore_cancel=True)
    token = CancellationToken()
    running = asyncio.ensure_future(
        fake.execute(CommandSpec("t1", "run", 1_000, "."), cancel=token)
    )
    await asyncio.wait_for(fake.wait_spawned("t1"), BOUND_S)
    token.cancel("plan_stopped")
    await _settle()
    assert running.done() is False and fake.cancellations == []
    fake.release("t1")
    raw = await asyncio.wait_for(running, BOUND_S)
    assert raw.outcome is TaskState.COMPLETED


async def given_two_held_scripts_when_both_running_then_active_and_peak_tracked(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(hold=True)
    token = CancellationToken()
    first = asyncio.ensure_future(fake.execute(CommandSpec("t1", "a", 1_000, "."), cancel=token))
    second = asyncio.ensure_future(fake.execute(CommandSpec("t2", "b", 1_000, "."), cancel=token))
    await asyncio.wait_for(fake.wait_spawned("t2"), BOUND_S)
    assert fake.active == {"t1", "t2"} and fake.max_active == 2
    fake.release_all()
    await asyncio.wait_for(asyncio.gather(first, second), BOUND_S)
    assert fake.active == set() and fake.spawn_order == ["t1", "t2"]


# ================================================================================================
# 1. sequential nominal path (§8.2, ADR-015, ADR-017, ADR-019 §1)
# ================================================================================================
async def given_sequential_plan_of_three_tasks_when_run_then_tasks_execute_in_order_and_plan_completed(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"one\n")
    harness.fake.script(task_id="t2", stdout=b"two\n", stderr=b"warn\n")
    harness.fake.script(task_id="t3", stdout=b"three\n")
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")])

    outcome = await harness.run(plan, tasks)

    assert [c.task_id for c in harness.fake.calls] == ["t1", "t2", "t3"]
    assert harness.fake.max_active == 1
    assert outcome.plan.status is PlanState.COMPLETED and outcome.stop_reason is None
    assert outcome.interrupted is False and outcome.budget_exceeded is False
    assert [t.task_id for t in outcome.tasks] == ["t1", "t2", "t3"]
    assert all(t.status is TaskState.COMPLETED and t.exit_code == 0 for t in outcome.tasks)
    assert all(t.attempt_count == 1 for t in outcome.tasks)
    assert outcome.tasks == harness.store.list_tasks(SESSION_ID, plan_id=PLAN_ID)
    assert outcome.plan == harness.stored_plan()
    result = outcome.execution_result
    assert result is not None
    assert result.status == "completed" and result.stop_reason is None
    assert [r.task_id for r in result.results] == ["t1", "t2", "t3"]
    assert [r.stdout for r in result.results] == ["one\n", "two\n", "three\n"]
    assert result.results[1].stderr == "warn\n"
    assert result.skipped_tasks == [] and result.cancelled_tasks == []
    assert result.interrupted_tasks == []


async def given_cmd_task_with_empty_streams_when_run_then_one_blob_per_stream_persisted_even_empty(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"", stderr=b"")
    harness.fake.script(task_id="t2", stdout=b"out", stderr=b"err")
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    t1, t2 = outcome.tasks
    assert (t1.stdout_ref, t1.stderr_ref) == ("blob-0001", "blob-0002")
    assert (t2.stdout_ref, t2.stderr_ref) == ("blob-0003", "blob-0004")
    for task, stdout, stderr in ((t1, b"", b""), (t2, b"out", b"err")):
        out_blob = harness.blob(task.task_id, OutputStream.STDOUT)
        err_blob = harness.blob(task.task_id, OutputStream.STDERR)
        assert out_blob is not None and err_blob is not None
        assert (out_blob.content, out_blob.size_bytes) == (stdout, len(stdout))
        assert (err_blob.content, err_blob.size_bytes) == (stderr, len(stderr))
        assert out_blob.blob_id == task.stdout_ref and err_blob.blob_id == task.stderr_ref
        assert out_blob.created_at == harness.clock.now()


async def given_sequential_plan_when_run_then_state_changes_published_in_order_with_ids(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"x", duration_ms=120)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    await harness.run(plan, tasks)

    kinds = [
        (e.event_type, e.task_id, e.payload["from"], e.payload["to"])
        for e in harness.recorder.events
        if e.event_type is not EventType.TASK_OUTPUT
    ]
    assert kinds == [
        (EventType.PLAN_STATE_CHANGED, None, "PENDING", "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "t1", "PENDING", "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "t1", "RUNNING", "COMPLETED"),
        (EventType.TASK_STATE_CHANGED, "t2", "PENDING", "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "t2", "RUNNING", "COMPLETED"),
        (EventType.PLAN_STATE_CHANGED, None, "RUNNING", "COMPLETED"),
    ]
    for event in harness.recorder.events:
        assert (event.session_id, event.conversation_id) == (SESSION_ID, CONVERSATION_ID)
        assert (event.cycle_id, event.plan_id) == (CYCLE_ID, PLAN_ID)
    running = harness.recorder.of_type(EventType.TASK_STATE_CHANGED)[0]
    assert running.payload == {"from": "PENDING", "to": "RUNNING"}
    terminal = _terminal_event(harness.recorder, "t1")
    assert terminal.payload == {
        "from": "RUNNING",
        "to": "COMPLETED",
        "exit_code": 0,
        "duration_ms": 120,
        "timed_out": False,
        "truncated": False,
    }
    assert terminal.timestamp == harness.task("t1").ended_at
    plan_events = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)
    assert plan_events[0].payload == {"from": "PENDING", "to": "RUNNING"}
    assert plan_events[-1].payload == {"from": "RUNNING", "to": "COMPLETED"}
    assert plan_events[-1].timestamp == harness.stored_plan().ended_at


async def given_plan_when_run_then_timestamps_and_durations_come_from_the_injected_clock(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", duration_ms=300)
    harness.fake.script(task_id="t2", duration_ms=200)
    start = harness.clock.now()
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.started_at == start
    assert outcome.plan.ended_at == start + timedelta(milliseconds=500)
    t1, t2 = outcome.tasks
    assert (t1.started_at, t1.ended_at) == (start, start + timedelta(milliseconds=300))
    assert (t2.started_at, t2.ended_at) == (t1.ended_at, start + timedelta(milliseconds=500))
    assert (t1.duration_ms, t2.duration_ms) == (300, 200)
    assert outcome.execution_result is not None
    assert [r.duration_ms for r in outcome.execution_result.results] == [300, 200]


async def given_cmd_task_when_run_then_command_spec_carries_applied_timeout_cwd_and_shell(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(
        store, bus, recorder, clock, ids, _with_execution(config, shell="/bin/zsh", cwd="/work")
    )
    plan, tasks = harness.plan(
        [_cmd("t1", "echo hi", timeout_ms=5_000), _cmd("t2", "echo cap", timeout_ms=10**9)]
    )

    await harness.run(plan, tasks)

    first, second = harness.fake.calls
    assert first == CommandSpec("t1", "echo hi", timeout_ms=5_000, cwd="/work", shell="/bin/zsh")
    assert second.timeout_ms == config.execution.max_task_timeout_ms  # capped by the adapter
    assert harness.task("t2").timeout_ms_applied == config.execution.max_task_timeout_ms


async def given_default_shell_when_run_then_spec_shell_is_none(harness: Harness) -> None:
    plan, tasks = harness.plan([_cmd("t1")])
    await harness.run(plan, tasks)
    spec = harness.fake.calls[0]
    assert spec.shell is None and spec.cwd == "." and spec.timeout_ms == 60_000
    assert spec.env is None  # ADR-026: no scratch manager wired, no overlay at all


async def given_scratch_manager_when_cmd_task_runs_then_spec_env_carries_the_working_space(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
    tmp_path: Path,
) -> None:
    """ADR-026: the working space reaches the command through ``env``; ``cwd`` is left alone."""
    scratch = ScratchManager(
        ScratchSection(root=str(tmp_path / "scratch"), archive_root=str(tmp_path / "archive")),
        clock,
    )
    harness = Harness.build(store, bus, recorder, clock, ids, config, scratch=scratch)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    await harness.run(plan, tasks)

    created = tmp_path / "scratch" / SESSION_ID
    assert _is_dir(created)
    folder = str(created)
    for spec in harness.fake.calls:
        assert spec.env == {
            ENV_SCRATCH_DIR: folder,
            ENV_WORKING_SPACE: folder,
            ENV_SESSION_ID: SESSION_ID,
        }
        assert spec.cwd == config.execution.cwd  # the folder is offered, never imposed


async def given_disabled_scratch_when_cmd_task_runs_then_spec_env_stays_empty(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
    tmp_path: Path,
) -> None:
    scratch = ScratchManager(
        ScratchSection(
            enabled=False,
            root=str(tmp_path / "scratch"),
            archive_root=str(tmp_path / "archive"),
        ),
        clock,
    )
    harness = Harness.build(store, bus, recorder, clock, ids, config, scratch=scratch)
    plan, tasks = harness.plan([_cmd("t1")])

    await harness.run(plan, tasks)

    assert harness.fake.calls[0].env is None
    assert not _is_dir(tmp_path / "scratch")  # nothing created at all


# ================================================================================================
# 2. stop conditions (§8.3, ADR-008 §4, ADR-009)
# ================================================================================================
async def given_failed_task_with_continue_on_error_when_plan_runs_then_next_tasks_run_and_plan_completed(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=2, stderr=b"boom")
    plan, tasks = harness.plan([_cmd("t1", continue_on_error=True), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    assert [c.task_id for c in harness.fake.calls] == ["t1", "t2"]
    assert harness.statuses() == {"t1": TaskState.FAILED, "t2": TaskState.COMPLETED}
    assert outcome.plan.status is PlanState.COMPLETED and outcome.stop_reason is None
    assert outcome.plan.failed_task_count == 1 and outcome.plan.completed_task_count == 1
    result = outcome.execution_result
    assert result is not None and result.status == "completed"
    assert [(r.task_id, r.status, r.exit_code) for r in result.results] == [
        ("t1", "failed", 2),
        ("t2", "completed", 0),
    ]
    assert result.results[0].stderr == "boom"


async def given_failed_task_without_continue_on_error_when_plan_runs_then_stopped_on_failure_and_remaining_skipped(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t2", exit_code=1)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3"), _cmd("t4")])

    outcome = await harness.run(plan, tasks)

    assert [c.task_id for c in harness.fake.calls] == ["t1", "t2"]
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t2" == outcome.plan.stop_reason
    assert harness.statuses() == {
        "t1": TaskState.COMPLETED,
        "t2": TaskState.FAILED,
        "t3": TaskState.SKIPPED,
        "t4": TaskState.SKIPPED,
    }
    assert harness.task("t3").reason == "plan_stopped:task_failed:t2"
    assert harness.task("t4").reason == "plan_stopped:task_failed:t2"
    assert _transitions(harness.recorder, "t3") == [("PENDING", "SKIPPED")]
    result = outcome.execution_result
    assert result is not None
    assert result.status == "stopped_on_failure" and result.stop_reason == "task_failed:t2"
    assert [r.task_id for r in result.results] == ["t1", "t2"]
    assert [(s.task_id, s.reason) for s in result.skipped_tasks] == [
        ("t3", "plan_stopped:task_failed:t2"),
        ("t4", "plan_stopped:task_failed:t2"),
    ]
    assert result.cancelled_tasks == []
    plan_terminal = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)[-1]
    assert plan_terminal.payload == {
        "from": "RUNNING",
        "to": "STOPPED_ON_FAILURE",
        "stop_reason": "task_failed:t2",
    }


async def given_critical_task_failing_when_plan_runs_then_stop_reason_is_critical_task_failed(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1)
    plan, tasks = harness.plan(
        [_cmd("t1", critical=True, continue_on_error=True, stop_plan_on_failure=True), _cmd("t2")]
    )

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "critical_task_failed:t1"
    assert harness.statuses()["t2"] is TaskState.SKIPPED
    assert harness.task("t2").reason == "plan_stopped:critical_task_failed:t1"


async def given_stop_plan_on_failure_task_failing_when_plan_runs_then_stop_reason_is_stop_plan_on_failure(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1)
    plan, tasks = harness.plan(
        [_cmd("t1", stop_plan_on_failure=True, continue_on_error=True), _cmd("t2")]
    )

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "stop_plan_on_failure:t1"


async def given_stop_plan_on_success_task_completing_when_plan_runs_then_short_circuited_and_remaining_skipped(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2", stop_plan_on_success=True), _cmd("t3")])

    outcome = await harness.run(plan, tasks)

    assert [c.task_id for c in harness.fake.calls] == ["t1", "t2"]
    assert outcome.plan.status is PlanState.SHORT_CIRCUITED_ON_SUCCESS
    assert outcome.stop_reason == "stop_plan_on_success:t2"
    assert harness.statuses() == {
        "t1": TaskState.COMPLETED,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.SKIPPED,
    }
    assert harness.task("t3").reason == "plan_stopped:stop_plan_on_success:t2"
    result = outcome.execution_result
    assert result is not None and result.status == "short_circuited_on_success"
    assert result.stop_reason == "stop_plan_on_success:t2"


async def given_stop_plan_on_success_task_failing_with_continue_on_error_when_plan_runs_then_plan_continues(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1)
    plan, tasks = harness.plan(
        [_cmd("t1", stop_plan_on_success=True, continue_on_error=True), _cmd("t2")]
    )
    outcome = await harness.run(plan, tasks)
    assert outcome.plan.status is PlanState.COMPLETED
    assert harness.statuses()["t2"] is TaskState.COMPLETED


async def given_timed_out_task_with_stop_on_failure_when_plan_runs_then_plan_stopped_on_failure(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"partial", duration_ms=10_000)
    plan, tasks = harness.plan([_cmd("t1", timeout_ms=300), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    t1 = harness.task("t1")
    assert t1.status is TaskState.TIMED_OUT
    assert t1.timed_out is True and t1.exit_code is None and t1.duration_ms == 300
    assert t1.timeout_ms_applied == 300
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"
    assert outcome.plan.failed_task_count == 1  # TIMED_OUT counts as failed (ADR-008)
    assert harness.statuses()["t2"] is TaskState.SKIPPED
    terminal = _terminal_event(harness.recorder, "t1")
    assert terminal.payload["to"] == "TIMED_OUT" and terminal.payload["timed_out"] is True
    assert terminal.payload["exit_code"] is None and terminal.payload["duration_ms"] == 300
    result = outcome.execution_result
    assert result is not None and result.status == "stopped_on_failure"
    first = result.results[0]
    assert (first.status, first.timed_out, first.exit_code) == ("timed_out", True, None)
    assert first.stdout == "partial" and first.timeout_ms_applied == 300


async def given_timed_out_task_with_continue_on_error_when_plan_runs_then_plan_continues(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", duration_ms=10_000)
    plan, tasks = harness.plan([_cmd("t1", timeout_ms=300, continue_on_error=True), _cmd("t2")])
    outcome = await harness.run(plan, tasks)
    assert outcome.plan.status is PlanState.COMPLETED
    assert harness.statuses() == {"t1": TaskState.TIMED_OUT, "t2": TaskState.COMPLETED}


def _expected_stop(
    critical: bool,
    continue_on_error: bool,
    stop_on_failure: bool,
    stop_on_success: bool,
    fails: bool,
) -> tuple[PlanState, str | None]:
    """ADR-009 §3: first applicable row of the stop_reason table."""
    if fails:
        if critical or stop_on_failure or not continue_on_error:
            label = (
                "critical_task_failed"
                if critical
                else "stop_plan_on_failure"
                if stop_on_failure
                else "task_failed"
            )
            return PlanState.STOPPED_ON_FAILURE, f"{label}:t1"
        return PlanState.COMPLETED, None
    if stop_on_success:
        return PlanState.SHORT_CIRCUITED_ON_SUCCESS, "stop_plan_on_success:t1"
    return PlanState.COMPLETED, None


_FLAG_CASES = [
    (critical, coe, spof, spos, fails)
    for critical in (False, True)
    for coe in (False, True)
    for spof in (False, True)
    for spos in (False, True)
    for fails in (False, True)
]


@pytest.mark.parametrize(
    ("critical", "continue_on_error", "stop_on_failure", "stop_on_success", "fails"), _FLAG_CASES
)
async def given_flag_combination_when_first_task_ends_then_plan_status_and_stop_reason_follow_adr_009(
    harness: Harness,
    critical: bool,
    continue_on_error: bool,
    stop_on_failure: bool,
    stop_on_success: bool,
    fails: bool,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1 if fails else 0)
    plan, tasks = harness.plan(
        [
            _cmd(
                "t1",
                critical=critical,
                continue_on_error=continue_on_error,
                stop_plan_on_failure=stop_on_failure,
                stop_plan_on_success=stop_on_success,
            ),
            _cmd("t2"),
        ]
    )

    outcome = await harness.run(plan, tasks)

    status, stop_reason = _expected_stop(
        critical, continue_on_error, stop_on_failure, stop_on_success, fails
    )
    assert (outcome.plan.status, outcome.stop_reason) == (status, stop_reason)
    stopped = status is not PlanState.COMPLETED
    assert harness.statuses()["t2"] is (TaskState.SKIPPED if stopped else TaskState.COMPLETED)
    assert outcome.execution_result is not None
    assert outcome.execution_result.status == status.protocol_value


async def given_task_without_any_flag_when_it_fails_then_plan_stops_with_task_failed(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=3)
    plan, tasks = harness.plan([{"task_id": "t1", "type": "cmd", "cmd": "x"}, _cmd("t2")])
    outcome = await harness.run(plan, tasks)
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"


# ================================================================================================
# 3. parallel execution (§2.4, §8.2, ADR-003, ADR-009 §5, ADR-017)
# ================================================================================================
async def given_parallel_plan_with_two_workers_when_run_then_never_more_than_two_tasks_running(
    harness: Harness,
) -> None:
    harness.fake.script(hold=True)
    plan, tasks = harness.plan(
        [_cmd("t1"), _cmd("t2"), _cmd("t3"), _cmd("t4")], policy="parallel", workers=2
    )
    running = harness.start(plan, tasks)

    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    await _settle()
    assert harness.fake.active == {"t1", "t2"}
    assert harness.statuses() == {
        "t1": TaskState.RUNNING,
        "t2": TaskState.RUNNING,
        "t3": TaskState.PENDING,
        "t4": TaskState.PENDING,
    }
    harness.fake.release("t1")
    await asyncio.wait_for(harness.fake.wait_spawned("t3"), BOUND_S)
    await _settle()
    assert harness.fake.active == {"t2", "t3"}
    assert harness.statuses()["t1"] is TaskState.COMPLETED
    assert harness.statuses()["t4"] is TaskState.PENDING
    harness.fake.release("t2")
    await asyncio.wait_for(harness.fake.wait_spawned("t4"), BOUND_S)
    harness.fake.release_all()

    outcome = await asyncio.wait_for(running, BOUND_S)
    assert harness.fake.max_active == 2
    assert harness.fake.spawn_order == ["t1", "t2", "t3", "t4"]
    assert outcome.plan.status is PlanState.COMPLETED
    assert outcome.plan.max_parallel_workers == 2


async def given_parallel_plan_when_tasks_complete_out_of_order_then_results_follow_plan_order(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"first", hold=True)
    harness.fake.script(task_id="t2", stdout=b"second", hold=True)
    harness.fake.script(task_id="t3", stdout=b"third", hold=True)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=3)
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t3"), BOUND_S)
    assert harness.fake.spawn_order == ["t1", "t2", "t3"]  # deterministic launch order

    harness.fake.release("t3")
    harness.clock.advance(10)
    await _settle()
    harness.fake.release("t2")
    harness.clock.advance(10)
    await _settle()
    harness.fake.release("t1")
    outcome = await asyncio.wait_for(running, BOUND_S)

    finished = [
        e.task_id
        for e in harness.recorder.of_type(EventType.TASK_STATE_CHANGED)
        if e.payload["to"] == "COMPLETED"
    ]
    assert finished == ["t3", "t2", "t1"]  # real completion order
    assert outcome.execution_result is not None
    assert [r.task_id for r in outcome.execution_result.results] == ["t1", "t2", "t3"]
    assert [r.stdout for r in outcome.execution_result.results] == ["first", "second", "third"]
    assert [t.task_id for t in outcome.tasks] == ["t1", "t2", "t3"]


async def given_parallel_plan_with_dependency_when_run_then_dependent_waits_then_pending_then_runs(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", hold=True)
    plan, tasks = harness.plan(
        [_cmd("t1"), _cmd("t2", depends_on=["t1"]), _cmd("t3")], policy="parallel", workers=3
    )
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t3"), BOUND_S)
    await _settle()

    assert harness.statuses()["t2"] is TaskState.WAITING_DEPENDENCY
    assert "t2" not in harness.fake.spawn_order
    harness.fake.release("t1")
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert _transitions(harness.recorder, "t2") == [
        ("PENDING", "WAITING_DEPENDENCY"),
        ("WAITING_DEPENDENCY", "PENDING"),
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
    ]
    assert harness.fake.spawn_order == ["t1", "t3", "t2"]
    assert outcome.plan.status is PlanState.COMPLETED
    assert outcome.plan.completed_task_count == 3


async def given_dependency_failed_with_continue_on_error_when_plan_runs_then_dependent_skipped_and_plan_completed(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1)
    plan, tasks = harness.plan(
        [
            _cmd("t1", continue_on_error=True),
            _cmd("t2", depends_on=["t1"]),
            _cmd("t3", depends_on=["t2"]),
            _cmd("t4"),
        ],
        policy="parallel",
        workers=2,
    )

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.COMPLETED and outcome.stop_reason is None
    assert harness.statuses() == {
        "t1": TaskState.FAILED,
        "t2": TaskState.SKIPPED,
        "t3": TaskState.SKIPPED,
        "t4": TaskState.COMPLETED,
    }
    assert harness.task("t2").reason == "dependency_failed:t1"
    assert harness.task("t3").reason == "dependency_skipped:t2"
    assert _transitions(harness.recorder, "t2") == [
        ("PENDING", "WAITING_DEPENDENCY"),
        ("WAITING_DEPENDENCY", "SKIPPED"),
    ]
    assert sorted(c.task_id for c in harness.fake.calls) == ["t1", "t4"]
    result = outcome.execution_result
    assert result is not None and result.status == "completed"
    assert [(s.task_id, s.reason) for s in result.skipped_tasks] == [
        ("t2", "dependency_failed:t1"),
        ("t3", "dependency_skipped:t2"),
    ]


async def given_sequential_plan_with_failed_dependency_when_run_then_dependent_skipped_without_waiting_state(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", duration_ms=10_000)  # TIMED_OUT
    plan, tasks = harness.plan(
        [
            _cmd("t1", timeout_ms=100, continue_on_error=True),
            _cmd("t2", depends_on=["t1"]),
            _cmd("t3"),
        ]
    )

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.COMPLETED
    assert harness.statuses() == {
        "t1": TaskState.TIMED_OUT,
        "t2": TaskState.SKIPPED,
        "t3": TaskState.COMPLETED,
    }
    assert harness.task("t2").reason == "dependency_failed:t1"
    assert _transitions(harness.recorder, "t2") == [("PENDING", "SKIPPED")]
    assert [c.task_id for c in harness.fake.calls] == ["t1", "t3"]


async def given_two_tasks_sharing_a_resource_lock_when_run_in_parallel_then_never_running_together(
    harness: Harness,
) -> None:
    harness.fake.script(hold=True)
    plan, tasks = harness.plan(
        [_cmd("t1", resource_lock="pom"), _cmd("t2", resource_lock="pom"), _cmd("t3")],
        policy="parallel",
        workers=3,
    )
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t3"), BOUND_S)
    await _settle()

    assert harness.fake.active == {"t1", "t3"}
    assert harness.statuses()["t2"] is TaskState.PENDING  # waiting for the lock: no state change
    harness.fake.release("t1")
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    await _settle()
    assert harness.fake.active == {"t2", "t3"}
    assert _transitions(harness.recorder, "t2") == [("PENDING", "RUNNING")]
    harness.fake.release_all()

    outcome = await asyncio.wait_for(running, BOUND_S)
    assert outcome.plan.status is PlanState.COMPLETED
    assert harness.fake.spawn_order == ["t1", "t3", "t2"]


async def given_lock_held_by_failed_task_when_released_then_next_holder_runs(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1)
    plan, tasks = harness.plan(
        [
            _cmd("t1", resource_lock="db", continue_on_error=True),
            _cmd("t2", resource_lock="db"),
        ],
        policy="parallel",
        workers=2,
    )
    outcome = await harness.run(plan, tasks)
    assert outcome.plan.status is PlanState.COMPLETED
    assert harness.statuses() == {"t1": TaskState.FAILED, "t2": TaskState.COMPLETED}
    assert harness.fake.spawn_order == ["t1", "t2"]


async def given_parallel_stop_condition_when_a_task_fails_then_running_cancelled_after_drain_and_pending_skipped(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1, hold=True)
    harness.fake.script(task_id="t2", stdout=b"partial", hang_until_cancelled=True)
    plan, tasks = harness.plan(
        [_cmd("t1"), _cmd("t2"), _cmd("t3"), _cmd("t4", depends_on=["t2"])],
        policy="parallel",
        workers=2,
    )
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    await _settle()
    harness.fake.release("t1")

    outcome = await asyncio.wait_for(running, BOUND_S)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"
    assert harness.fake.cancellations == [("t2", "plan_stopped")]
    assert harness.statuses() == {
        "t1": TaskState.FAILED,
        "t2": TaskState.CANCELLED,
        "t3": TaskState.SKIPPED,
        "t4": TaskState.SKIPPED,
    }
    t2 = harness.task("t2")
    assert t2.reason == "plan_stopped:task_failed:t1" and t2.exit_code is None
    out_blob = harness.blob("t2", OutputStream.STDOUT)
    assert out_blob is not None and out_blob.content == b"partial"  # output kept until the end
    assert harness.task("t3").reason == "plan_stopped:task_failed:t1"
    assert harness.task("t4").reason == "plan_stopped:task_failed:t1"
    assert _transitions(harness.recorder, "t4") == [
        ("PENDING", "WAITING_DEPENDENCY"),
        ("WAITING_DEPENDENCY", "SKIPPED"),
    ]
    terminal = _terminal_event(harness.recorder, "t2")
    assert terminal.payload["to"] == "CANCELLED"
    assert terminal.payload["reason"] == "plan_stopped:task_failed:t1"
    plan_events = _transitions(harness.recorder, None)
    assert plan_events == [("PENDING", "RUNNING"), ("RUNNING", "STOPPED_ON_FAILURE")]
    # the plan goes terminal only after every cancellation came back (§17.2)
    order = [
        (e.event_type, e.task_id, e.payload["to"])
        for e in harness.recorder.events
        if e.event_type is not EventType.TASK_OUTPUT
    ]
    assert order.index((EventType.TASK_STATE_CHANGED, "t2", "CANCELLED")) < order.index(
        (EventType.PLAN_STATE_CHANGED, None, "STOPPED_ON_FAILURE")
    )
    result = outcome.execution_result
    assert result is not None and result.status == "stopped_on_failure"
    assert [(c.task_id, c.reason) for c in result.cancelled_tasks] == [
        ("t2", "plan_stopped:task_failed:t1")
    ]
    assert [(s.task_id, s.reason) for s in result.skipped_tasks] == [
        ("t3", "plan_stopped:task_failed:t1"),
        ("t4", "plan_stopped:task_failed:t1"),
    ]
    assert outcome.plan.cancelled_task_count == 1 and outcome.plan.skipped_task_count == 2


async def given_parallel_short_circuit_when_a_task_succeeds_then_running_cancelled_with_success_reason(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", hold=True)
    harness.fake.script(task_id="t2", hang_until_cancelled=True)
    plan, tasks = harness.plan(
        [_cmd("t1", stop_plan_on_success=True), _cmd("t2"), _cmd("t3")],
        policy="parallel",
        workers=2,
    )
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    harness.fake.release("t1")

    outcome = await asyncio.wait_for(running, BOUND_S)

    assert outcome.plan.status is PlanState.SHORT_CIRCUITED_ON_SUCCESS
    assert outcome.stop_reason == "stop_plan_on_success:t1"
    assert harness.task("t2").status is TaskState.CANCELLED
    assert harness.task("t2").reason == "plan_stopped:stop_plan_on_success:t1"
    assert harness.task("t3").status is TaskState.SKIPPED
    assert harness.fake.cancellations == [("t2", "plan_stopped")]


async def given_task_ignoring_cancellation_when_plan_stops_then_forced_after_cancel_drain_timeout(
    short_drain: Harness,
) -> None:
    short_drain.fake.script(task_id="t1", exit_code=1, hold=True)
    short_drain.fake.script(task_id="t2", hold=True, ignore_cancel=True)
    plan, tasks = short_drain.plan([_cmd("t1"), _cmd("t2")], policy="parallel", workers=2)
    running = short_drain.start(plan, tasks)
    await asyncio.wait_for(short_drain.fake.wait_spawned("t2"), BOUND_S)
    short_drain.fake.release("t1")
    started = time.perf_counter()

    outcome = await asyncio.wait_for(running, BOUND_S)

    assert time.perf_counter() - started < SHORT_DRAIN_MS / 1000 + 0.4
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    t2 = short_drain.task("t2")
    assert t2.status is TaskState.CANCELLED and t2.reason == "plan_stopped:task_failed:t1"
    assert t2.exit_code is None and t2.stdout_ref is None and t2.stderr_ref is None
    assert short_drain.blob("t2", OutputStream.STDOUT) is None
    assert short_drain.fake.active == set()  # the forced future released the fake
    result = outcome.execution_result
    assert result is not None
    assert [(c.task_id, c.reason) for c in result.cancelled_tasks] == [
        ("t2", "plan_stopped:task_failed:t1")
    ]


async def given_two_tasks_ending_in_the_same_round_when_first_stops_the_plan_then_second_keeps_its_real_outcome(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", exit_code=1, hold=True)
    harness.fake.script(task_id="t2", stdout=b"ok", hold=True)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=2)
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    harness.fake.release("t1")
    harness.fake.release("t2")

    outcome = await asyncio.wait_for(running, BOUND_S)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"
    assert harness.statuses() == {
        "t1": TaskState.FAILED,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.SKIPPED,
    }
    assert harness.fake.cancellations == []


# ================================================================================================
# 4. interruption (§2.9, §8.4, ADR-003, ADR-006)
# ================================================================================================
async def given_interrupt_signalled_before_first_task_when_run_then_everything_interrupted_without_result(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2", depends_on=["t1"])], policy="parallel")
    interrupt = CancellationToken()
    interrupt.cancel("user_interrupt")

    outcome = await harness.run(plan, tasks, interrupt=interrupt)

    assert harness.fake.calls == []
    assert outcome.interrupted is True and outcome.execution_result is None
    assert outcome.budget_exceeded is False
    assert outcome.plan.status is PlanState.INTERRUPTED
    assert outcome.stop_reason == "user_interrupt" == outcome.plan.stop_reason
    assert outcome.plan.started_at is None and outcome.plan.ended_at == harness.clock.now()
    assert outcome.plan.interrupted_task_count == 2 and outcome.plan.task_count == 2
    assert harness.statuses() == {"t1": TaskState.INTERRUPTED, "t2": TaskState.INTERRUPTED}
    assert all(t.reason == "user_interrupt" for t in outcome.tasks)
    assert _transitions(harness.recorder, None) == [("PENDING", "INTERRUPTED")]
    assert _transitions(harness.recorder, "t1") == [("PENDING", "INTERRUPTED")]
    assert _transitions(harness.recorder, "t2") == [("PENDING", "INTERRUPTED")]
    plan_event = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)[0]
    assert plan_event.payload == {
        "from": "PENDING",
        "to": "INTERRUPTED",
        "stop_reason": "user_interrupt",
    }


async def given_running_plan_when_user_interrupts_then_all_tasks_marked_interrupted(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"partial", hang_until_cancelled=True)
    plan, tasks = harness.plan(
        [_cmd("t1"), _cmd("t2"), _cmd("t3", depends_on=["t2"])], policy="parallel", workers=1
    )
    interrupt = CancellationToken()
    running = harness.start(plan, tasks, interrupt=interrupt)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)
    await _settle()
    assert harness.statuses() == {
        "t1": TaskState.RUNNING,
        "t2": TaskState.PENDING,
        "t3": TaskState.WAITING_DEPENDENCY,
    }
    started = time.perf_counter()

    interrupt.cancel("user_interrupt")
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert time.perf_counter() - started < 0.4
    assert harness.fake.cancellations == [("t1", "user_interrupt")]
    assert outcome.interrupted is True and outcome.execution_result is None
    assert outcome.plan.status is PlanState.INTERRUPTED
    assert outcome.plan.stop_reason == "user_interrupt"
    assert harness.statuses() == {
        "t1": TaskState.INTERRUPTED,
        "t2": TaskState.INTERRUPTED,
        "t3": TaskState.INTERRUPTED,
    }
    t1 = harness.task("t1")
    assert t1.reason == "user_interrupt" and t1.exit_code is None and t1.timed_out is False
    out_blob = harness.blob("t1", OutputStream.STDOUT)
    assert out_blob is not None and out_blob.content == b"partial"
    assert _transitions(harness.recorder, "t1") == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "INTERRUPTED"),
    ]
    assert _transitions(harness.recorder, "t3") == [
        ("PENDING", "WAITING_DEPENDENCY"),
        ("WAITING_DEPENDENCY", "INTERRUPTED"),
    ]
    assert _transitions(harness.recorder, None) == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "INTERRUPTED"),
    ]
    assert outcome.plan.interrupted_task_count == 3
    terminal = _terminal_event(harness.recorder, "t1")
    assert (
        terminal.payload["to"] == "INTERRUPTED" and terminal.payload["reason"] == "user_interrupt"
    )
    assert harness.fake.calls[0].task_id == "t1" and len(harness.fake.calls) == 1


async def given_task_ignoring_cancellation_when_user_interrupts_then_forced_within_interrupt_drain_timeout(
    short_drain: Harness,
) -> None:
    short_drain.fake.script(task_id="t1", hold=True, ignore_cancel=True)
    plan, tasks = short_drain.plan([_cmd("t1"), _cmd("t2")])
    interrupt = CancellationToken()
    running = short_drain.start(plan, tasks, interrupt=interrupt)
    await asyncio.wait_for(short_drain.fake.wait_spawned("t1"), BOUND_S)
    started = time.perf_counter()

    interrupt.cancel("user_interrupt")
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert time.perf_counter() - started < SHORT_DRAIN_MS / 1000 + 0.4
    assert outcome.interrupted is True and outcome.plan.status is PlanState.INTERRUPTED
    t1 = short_drain.task("t1")
    assert t1.status is TaskState.INTERRUPTED and t1.reason == "user_interrupt"
    assert t1.exit_code is None and t1.stdout_ref is None
    assert short_drain.task("t2").status is TaskState.INTERRUPTED
    assert short_drain.fake.active == set()


async def given_interrupt_between_two_sequential_tasks_when_next_task_due_then_not_started(
    harness: Harness,
) -> None:
    interrupt = CancellationToken()
    harness.bus.subscribe(
        lambda e: (
            interrupt.cancel("user_interrupt")
            if e.event_type is EventType.TASK_STATE_CHANGED and e.payload["to"] == "COMPLETED"
            else None
        ),
        name="interrupt-after-first",
    )
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks, interrupt=interrupt)

    assert [c.task_id for c in harness.fake.calls] == ["t1"]
    assert outcome.interrupted is True
    assert harness.statuses() == {"t1": TaskState.COMPLETED, "t2": TaskState.INTERRUPTED}
    assert outcome.plan.completed_task_count == 1 and outcome.plan.interrupted_task_count == 1


# ================================================================================================
# 5. duration budget between two tasks (ADR-012 §3)
# ================================================================================================
async def given_deadline_passed_between_tasks_when_next_task_due_then_plan_failed_and_remaining_skipped(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(store, bus, recorder, clock, ids, config, max_total_duration_ms=1_000)
    harness.fake.script(task_id="t1", stdout=b"slow", duration_ms=1_500)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")])

    outcome = await harness.run(plan, tasks)

    assert [c.task_id for c in harness.fake.calls] == ["t1"]
    assert harness.statuses() == {
        "t1": TaskState.COMPLETED,  # a started task is never killed for budget
        "t2": TaskState.SKIPPED,
        "t3": TaskState.SKIPPED,
    }
    assert harness.task("t2").reason == "budget_exceeded"
    assert harness.task("t3").reason == "budget_exceeded"
    assert outcome.budget_exceeded is True and outcome.interrupted is False
    assert outcome.plan.status is PlanState.FAILED
    assert outcome.stop_reason == "budget_exceeded:max_total_duration_ms"
    assert outcome.plan.stop_reason == "budget_exceeded:max_total_duration_ms"
    result = outcome.execution_result
    assert result is not None and result.status == "failed"
    assert result.stop_reason == "budget_exceeded:max_total_duration_ms"
    assert [r.task_id for r in result.results] == ["t1"]
    assert [(s.task_id, s.reason) for s in result.skipped_tasks] == [
        ("t2", "budget_exceeded"),
        ("t3", "budget_exceeded"),
    ]
    assert _transitions(harness.recorder, None) == [("PENDING", "RUNNING"), ("RUNNING", "FAILED")]


async def given_deadline_already_passed_when_run_then_no_task_started_and_plan_failed_from_pending(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(store, bus, recorder, clock, ids, config, max_total_duration_ms=1_000)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")], policy="parallel", workers=2)
    clock.advance(1_000)  # exactly the budget: the deadline is inclusive

    outcome = await harness.run(plan, tasks)

    assert harness.fake.calls == []
    assert outcome.plan.status is PlanState.FAILED and outcome.budget_exceeded is True
    assert outcome.plan.started_at is None
    assert harness.statuses() == {"t1": TaskState.SKIPPED, "t2": TaskState.SKIPPED}
    assert _transitions(harness.recorder, None) == [("PENDING", "FAILED")]
    assert outcome.execution_result is not None
    assert outcome.execution_result.status == "failed"


async def given_session_without_started_at_when_run_then_duration_budget_not_checked(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(
        store, bus, recorder, clock, ids, config, max_total_duration_ms=10, session_started=False
    )
    harness.fake.script(duration_ms=500)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])
    outcome = await harness.run(plan, tasks)
    assert outcome.plan.status is PlanState.COMPLETED and outcome.budget_exceeded is False
    assert [c.task_id for c in harness.fake.calls] == ["t1", "t2"]


async def given_parallel_plan_over_deadline_when_running_task_finishes_then_it_completes_and_no_new_task_starts(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    harness = Harness.build(store, bus, recorder, clock, ids, config, max_total_duration_ms=1_000)
    harness.fake.script(task_id="t1", hold=True)
    harness.fake.script(task_id="t2", duration_ms=1_500)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=2)
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    await _settle()  # t2 ends and pushes the clock past the deadline; t1 is still held

    assert harness.statuses() == {
        "t1": TaskState.RUNNING,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.PENDING,
    }
    assert "t3" not in harness.fake.spawn_order
    harness.fake.release("t1")
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert harness.statuses() == {
        "t1": TaskState.COMPLETED,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.SKIPPED,
    }
    assert harness.task("t3").reason == "budget_exceeded"
    assert outcome.plan.status is PlanState.FAILED and outcome.budget_exceeded is True
    assert harness.fake.cancellations == []


async def given_task_pushing_past_deadline_when_it_fails_with_stop_flag_then_stop_condition_wins_over_budget(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    """§8.2: the stop conditions of the task that just ended (step 10) are evaluated before the
    budget gate of the next task (ADR-012 §3, 'between two tasks')."""
    harness = Harness.build(store, bus, recorder, clock, ids, config, max_total_duration_ms=1_000)
    harness.fake.script(task_id="t1", exit_code=1, duration_ms=1_500)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1" and outcome.budget_exceeded is False
    assert harness.task("t2").reason == "plan_stopped:task_failed:t1"


async def given_deadline_observed_when_a_still_running_task_later_fails_with_stop_flag_then_plan_failed_for_budget(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    """Once the runner observed the deadline (no launch possible any more) the plan's fate is
    FAILED / budget_exceeded: a task still running finishes normally and keeps its real outcome,
    but its stop flags no longer change the plan status (nothing is left to stop)."""
    harness = Harness.build(store, bus, recorder, clock, ids, config, max_total_duration_ms=1_000)
    harness.fake.script(task_id="t1", exit_code=1, hold=True)  # stops the plan on failure
    harness.fake.script(task_id="t2", duration_ms=1_500)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=2)
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t2"), BOUND_S)
    await _settle()  # t2 ends past the deadline: t3 is never launched
    assert harness.statuses()["t3"] is TaskState.PENDING and "t3" not in harness.fake.spawn_order
    harness.fake.release("t1")

    outcome = await asyncio.wait_for(running, BOUND_S)

    assert harness.statuses() == {
        "t1": TaskState.FAILED,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.SKIPPED,
    }
    assert harness.task("t3").reason == "budget_exceeded"
    assert outcome.plan.status is PlanState.FAILED and outcome.budget_exceeded is True
    assert outcome.stop_reason == "budget_exceeded:max_total_duration_ms"
    assert harness.fake.cancellations == []
    result = outcome.execution_result
    assert result is not None and result.status == "failed"
    assert [(r.task_id, r.status) for r in result.results] == [
        ("t1", "failed"),
        ("t2", "completed"),
    ]


# ================================================================================================
# 6. chunk_request tasks (ADR-011, ADR-019 §1)
# ================================================================================================
def _stored_blob(harness: Harness, task_id: str, content: bytes) -> None:
    harness.store.save_blob(
        BlobRecord(
            blob_id=f"blob-prev-{task_id}",
            session_id=SESSION_ID,
            task_id=task_id,
            blob_type=OutputStream.STDOUT,
            content=content,
            size_bytes=len(content),
            created_at=harness.clock.now(),
        )
    )


async def given_chunk_request_on_stored_blob_when_run_then_completed_with_exact_bytes(
    harness: Harness,
) -> None:
    _stored_blob(harness, "t0", b"0123456789")
    plan, tasks = harness.plan([_chunk("tc", "t0", 4, 3)])

    outcome = await harness.run(plan, tasks)

    assert harness.fake.calls == []  # a local read never reaches the executor
    tc = harness.task("tc")
    assert tc.status is TaskState.COMPLETED and tc.exit_code is None and tc.reason is None
    assert tc.attempt_count == 1 and tc.duration_ms == 0
    assert _transitions(harness.recorder, "tc") == [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
    ]
    assert outcome.plan.status is PlanState.COMPLETED
    result = outcome.execution_result
    assert result is not None
    chunk = result.results[0]
    assert (chunk.task_id, chunk.status) == ("tc", "completed")
    assert (chunk.ref_task_id, chunk.stream) == ("t0", OutputStream.STDOUT)
    assert (chunk.range, chunk.total, chunk.eof, chunk.data) == ((4, 7), 10, False, "456")


async def given_chunk_request_on_known_task_without_blob_when_run_then_failed_chunk_ref_not_found(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_chunk("tc", "t0", 0, 8, continue_on_error=True), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    tc = harness.task("tc")
    assert tc.status is TaskState.FAILED and tc.reason == "CHUNK_REF_NOT_FOUND"
    assert tc.exit_code is None
    assert outcome.plan.status is PlanState.COMPLETED  # continue_on_error applies to chunks too
    assert harness.statuses()["t2"] is TaskState.COMPLETED
    result = outcome.execution_result
    assert result is not None
    failed = result.results[0]
    assert (failed.status, failed.reason, failed.data) == ("failed", "CHUNK_REF_NOT_FOUND", None)
    terminal = _terminal_event(harness.recorder, "tc")
    assert (
        terminal.payload["reason"] == "CHUNK_REF_NOT_FOUND" and terminal.payload["to"] == "FAILED"
    )


async def given_chunk_request_with_offset_beyond_total_when_run_then_failed_chunk_range_invalid_and_plan_stops(
    harness: Harness,
) -> None:
    _stored_blob(harness, "t0", b"0123456789")
    plan, tasks = harness.plan([_chunk("tc", "t0", 10, 4), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    tc = harness.task("tc")
    assert tc.status is TaskState.FAILED and tc.reason == "CHUNK_RANGE_INVALID"
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:tc"
    assert harness.statuses()["t2"] is TaskState.SKIPPED


async def given_chunk_request_on_empty_stream_of_executed_task_when_run_then_range_invalid_not_ref_not_found(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"", stderr=b"")
    plan, tasks = harness.plan(
        [_cmd("t1"), _chunk("tc", "t1", 0, 4, stream="stderr", continue_on_error=True)]
    )
    outcome = await harness.run(plan, tasks)
    assert harness.task("tc").reason == "CHUNK_RANGE_INVALID"
    assert outcome.plan.status is PlanState.COMPLETED


async def given_chunk_request_referencing_earlier_task_of_same_plan_when_run_then_served_from_fresh_blob(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"hello world", stderr=b"e1")
    plan, tasks = harness.plan(
        [_cmd("t1"), _chunk("tc", "t1", 6, 5), _chunk("te", "t1", 0, 100, stream="stderr")]
    )

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.COMPLETED
    result = outcome.execution_result
    assert result is not None
    _, tc, te = result.results
    assert (tc.data, tc.range, tc.total, tc.eof) == ("world", (6, 11), 11, True)
    assert (te.data, te.range, te.total, te.eof) == ("e1", (0, 2), 2, True)
    assert te.stream is OutputStream.STDERR


async def given_chunk_request_with_max_bytes_over_hard_limit_when_run_then_capped_by_adapter(
    harness: Harness,
) -> None:
    content = bytes(range(256)) * 600  # 153 600 bytes > hard_max_output_bytes (131 072)
    _stored_blob(harness, "t0", content)
    plan, tasks = harness.plan([_chunk("tc", "t0", 0, 10**9)])
    outcome = await harness.run(plan, tasks)
    assert outcome.execution_result is not None
    chunk = outcome.execution_result.results[0]
    hard = harness.config.payload.hard_max_output_bytes
    assert chunk.range == (0, hard) and chunk.eof is False and chunk.total == len(content)


# ================================================================================================
# 7. spawn error and executor defects (ADR-008 §4)
# ================================================================================================
async def given_spawn_error_when_task_runs_then_failed_with_spawn_failed_reason_and_null_exit_code(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", spawn_error="FileNotFoundError: /bin/zsh")
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    t1 = harness.task("t1")
    assert t1.status is TaskState.FAILED and t1.reason == "SPAWN_FAILED"
    assert t1.exit_code is None and t1.pid is None and t1.timed_out is False
    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"
    terminal = _terminal_event(harness.recorder, "t1")
    assert terminal.payload["reason"] == "SPAWN_FAILED" and terminal.payload["exit_code"] is None
    assert harness.store.list_failures(SESSION_ID) == []  # no failure manager: no record
    assert harness.recorder.of_type(EventType.FAILURE_RECORDED) == []
    result = outcome.execution_result
    assert result is not None
    failed = result.results[0]
    assert (failed.status, failed.reason, failed.exit_code) == ("failed", "SPAWN_FAILED", None)


async def given_spawn_error_with_failure_manager_when_task_runs_then_failure_record_task_execution_error(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    failure_manager = FailureManager(config, store, bus, clock, ids)
    harness = Harness.build(
        store, bus, recorder, clock, ids, config, failure_manager=failure_manager
    )
    harness.fake.script(task_id="t1", spawn_error="FileNotFoundError: /bin/zsh")
    plan, tasks = harness.plan([_cmd("t1", "zsh -c x", continue_on_error=True), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.COMPLETED
    failures = harness.store.list_failures(SESSION_ID)
    assert len(failures) == 1
    failure = failures[0]
    assert failure.error_type is ErrorType.TASK_EXECUTION_ERROR
    assert failure.error_code == "SPAWN_FAILED" and failure.origin == "CommandExecutor"
    assert (failure.plan_id, failure.task_id) == (PLAN_ID, "t1")
    assert (failure.session_id, failure.conversation_id) == (SESSION_ID, CONVERSATION_ID)
    assert failure.details["error"] == "FileNotFoundError: /bin/zsh"
    assert failure.details["cmd"] == "zsh -c x"
    recorded = harness.recorder.of_type(EventType.FAILURE_RECORDED)
    assert len(recorded) == 1 and recorded[0].task_id == "t1"
    kinds = [
        (e.event_type, e.task_id, e.payload.get("to"))
        for e in harness.recorder.events
        if e.task_id == "t1"
    ]
    assert kinds == [
        (EventType.TASK_STATE_CHANGED, "t1", "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "t1", "FAILED"),
        (EventType.FAILURE_RECORDED, "t1", None),
    ]


async def given_executor_raising_task_execution_error_when_task_runs_then_task_failed_with_error_code(
    harness: Harness,
) -> None:
    """A defect of the executor itself (``OUTPUT_READ_FAILED``, executor.py) is a ``FAILED`` task
    with ``reason`` = the error code and no output (ADR-008 §4), never a plan left RUNNING."""
    harness.fake.script(task_id="t1", executor_error="OUTPUT_READ_FAILED")
    plan, tasks = harness.plan([_cmd("t1", continue_on_error=True), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    t1 = harness.task("t1")
    assert t1.status is TaskState.FAILED and t1.reason == "OUTPUT_READ_FAILED"
    assert t1.exit_code is None and t1.timed_out is False
    assert t1.pid == 4_000  # the process existed: the pid persisted at spawn is kept
    assert t1.stdout_ref is None and t1.stderr_ref is None
    assert harness.blob("t1", OutputStream.STDOUT) is None
    assert t1.ended_at == harness.clock.now() and t1.duration_ms == 0
    assert outcome.plan.status is PlanState.COMPLETED  # continue_on_error applies
    assert harness.statuses()["t2"] is TaskState.COMPLETED
    assert harness.fake.active == set()
    terminal = _terminal_event(harness.recorder, "t1")
    assert terminal.payload["to"] == "FAILED"
    assert terminal.payload["reason"] == "OUTPUT_READ_FAILED"
    assert terminal.payload["exit_code"] is None
    result = outcome.execution_result
    assert result is not None
    failed = result.results[0]
    assert (failed.status, failed.reason, failed.exit_code) == (
        "failed",
        "OUTPUT_READ_FAILED",
        None,
    )
    assert (failed.stdout, failed.stderr) == ("", "")


async def given_executor_raising_task_execution_error_with_failure_manager_when_task_runs_then_failure_recorded_and_plan_stopped(
    store: InMemoryConversationStore,
    bus: EventBus,
    recorder: RecordingSubscriber,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    failure_manager = FailureManager(config, store, bus, clock, ids)
    harness = Harness.build(
        store, bus, recorder, clock, ids, config, failure_manager=failure_manager
    )
    harness.fake.script(task_id="t1", executor_error="OUTPUT_READ_FAILED")
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert outcome.stop_reason == "task_failed:t1"
    assert harness.statuses()["t2"] is TaskState.SKIPPED
    failures = harness.store.list_failures(SESSION_ID)
    assert len(failures) == 1
    failure = failures[0]
    assert failure.error_type is ErrorType.TASK_EXECUTION_ERROR
    assert failure.error_code == "OUTPUT_READ_FAILED" and failure.origin == "CommandExecutor"
    assert (failure.plan_id, failure.task_id) == (PLAN_ID, "t1")
    recorded = harness.recorder.of_type(EventType.FAILURE_RECORDED)
    assert len(recorded) == 1 and recorded[0].task_id == "t1"
    kinds = [
        (e.event_type, e.payload.get("to")) for e in harness.recorder.events if e.task_id == "t1"
    ]
    assert kinds == [
        (EventType.TASK_STATE_CHANGED, "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "FAILED"),
        (EventType.FAILURE_RECORDED, None),
    ]


async def given_executor_raising_in_parallel_when_task_fails_then_other_running_task_cancelled_after_drain(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", executor_error="OUTPUT_READ_FAILED")
    harness.fake.script(task_id="t2", hang_until_cancelled=True)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")], policy="parallel", workers=2)

    outcome = await harness.run(plan, tasks)

    assert outcome.plan.status is PlanState.STOPPED_ON_FAILURE
    assert harness.statuses() == {"t1": TaskState.FAILED, "t2": TaskState.CANCELLED}
    assert harness.fake.cancellations == [("t2", "plan_stopped")]
    assert harness.task("t2").reason == "plan_stopped:task_failed:t1"


# ================================================================================================
# 8. pid persistence (ADR-016), live output (ADR-018), truncation (ADR-010/011)
# ================================================================================================
async def given_spawned_task_when_on_spawn_called_then_pid_persisted_silently(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", hold=True)
    plan, tasks = harness.plan([_cmd("t1")])
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t1"), BOUND_S)
    await _settle()

    stored = harness.task("t1")
    assert stored.status is TaskState.RUNNING
    assert (stored.pid, stored.process_group_id) == (4_000, 4_000)
    assert stored.started_at == harness.clock.now() and stored.attempt_count == 1
    assert _transitions(harness.recorder, "t1") == [("PENDING", "RUNNING")]  # no event for the pid
    harness.fake.release("t1")
    outcome = await asyncio.wait_for(running, BOUND_S)

    assert (outcome.tasks[0].pid, outcome.tasks[0].process_group_id) == (4_000, 4_000)


async def given_scripted_output_chunks_when_task_runs_then_task_output_events_in_order_with_offsets(
    harness: Harness,
) -> None:
    chunks = [
        OutputChunk(OutputStream.STDOUT, 0, b"hel"),
        OutputChunk(OutputStream.STDERR, 0, b"w"),
        OutputChunk(OutputStream.STDOUT, 3, b"lo\n"),
        OutputChunk(OutputStream.STDERR, 1, b"arn \xff\n"),
    ]
    harness.fake.script(
        task_id="t1", stdout=b"hello\n", stderr=b"warn \xff\n", output_chunks=chunks
    )
    plan, tasks = harness.plan([_cmd("t1")])

    await harness.run(plan, tasks)

    outputs = harness.recorder.of_type(EventType.TASK_OUTPUT)
    assert [e.payload for e in outputs] == [
        {"stream": "stdout", "offset": 0, "size": 3, "data": "hel"},
        {"stream": "stderr", "offset": 0, "size": 1, "data": "w"},
        {"stream": "stdout", "offset": 3, "size": 3, "data": "lo\n"},
        {"stream": "stderr", "offset": 1, "size": 6, "data": "arn �\n"},
    ]
    for event in outputs:
        assert (event.session_id, event.plan_id, event.task_id) == (SESSION_ID, PLAN_ID, "t1")
        assert event.conversation_id == CONVERSATION_ID and event.cycle_id == CYCLE_ID
        assert event.audited is False and event.timestamp == harness.clock.now()
    kinds = [
        (e.event_type, e.payload.get("to")) for e in harness.recorder.events if e.task_id == "t1"
    ]
    assert kinds[0] == (EventType.TASK_STATE_CHANGED, "RUNNING")
    assert kinds[-1] == (EventType.TASK_STATE_CHANGED, "COMPLETED")
    assert all(k == (EventType.TASK_OUTPUT, None) for k in kinds[1:-1])


async def given_output_over_declared_budget_when_task_runs_then_truncation_metadata_persisted_and_reported(
    harness: Harness,
) -> None:
    stdout = bytes(range(65, 85))  # 20 bytes "ABC...T"
    harness.fake.script(task_id="t1", stdout=stdout, stderr=b"err")
    plan, tasks = harness.plan([_cmd("t1", max_output_bytes=8)])

    outcome = await harness.run(plan, tasks)

    t1 = harness.task("t1")
    assert t1.max_output_bytes_applied == 8 and t1.truncated is True
    assert t1.original_size_bytes == 23
    assert (t1.stdout_total, t1.stderr_total) == (20, 3)
    assert (t1.stdout_range, t1.stderr_range) == ((15, 20), (0, 3))
    out_blob = harness.blob("t1", OutputStream.STDOUT)
    assert out_blob is not None and out_blob.content == stdout  # the blob is never truncated
    result = outcome.execution_result
    assert result is not None
    reported = result.results[0]
    assert (reported.stdout, reported.stderr) == ("PQRST", "err")
    assert reported.truncated is True and reported.original_size_bytes == 23
    assert (reported.stdout_range, reported.stderr_range) == ((15, 20), (0, 3))
    assert reported.max_output_bytes_applied == 8
    assert _terminal_event(harness.recorder, "t1").payload["truncated"] is True


async def given_plan_default_max_output_bytes_when_task_declares_none_then_plan_default_applied(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"x" * 100)
    plan, tasks = harness.plan([_cmd("t1")], default_max_output_bytes=16)
    outcome = await harness.run(plan, tasks)
    assert outcome.execution_result is not None
    reported = outcome.execution_result.results[0]
    assert reported.max_output_bytes_applied == 16 and len(reported.stdout) == 16
    assert reported.truncated is True and reported.stdout_range == (84, 100)


# ================================================================================================
# 9. counters (§4.1) and outcome shape
# ================================================================================================
async def given_mixed_outcomes_when_plan_ends_then_plan_counters_recomputed_and_persisted(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t2", exit_code=1)
    harness.fake.script(task_id="t5", duration_ms=10_000)
    plan, tasks = harness.plan(
        [
            _cmd("t1"),
            _cmd("t2", continue_on_error=True),
            _cmd("t3", depends_on=["t2"]),
            _cmd("t4"),
            _cmd("t5", timeout_ms=50, continue_on_error=True),
        ],
        policy="parallel",
        workers=2,
    )

    outcome = await harness.run(plan, tasks)

    plan_record = harness.stored_plan()
    assert plan_record == outcome.plan
    assert plan_record.task_count == 5
    assert plan_record.completed_task_count == 2
    assert plan_record.failed_task_count == 2  # FAILED + TIMED_OUT
    assert plan_record.skipped_task_count == 1
    assert plan_record.cancelled_task_count == 0 and plan_record.interrupted_task_count == 0
    assert plan_record.status is PlanState.COMPLETED
    running_plan = harness.recorder.of_type(EventType.PLAN_STATE_CHANGED)[0]
    assert running_plan.payload["to"] == "RUNNING"
    assert harness.store.list_tasks(SESSION_ID, statuses=[TaskState.RUNNING]) == []


async def given_plan_outcome_when_returned_then_tasks_in_plan_order_and_equal_to_store(
    harness: Harness,
) -> None:
    harness.fake.script(hold=True)
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2"), _cmd("t3")], policy="parallel", workers=3)
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t3"), BOUND_S)
    for task_id in ("t2", "t3", "t1"):
        harness.fake.release(task_id)
        await _settle()
    outcome = await asyncio.wait_for(running, BOUND_S)
    assert [t.task_id for t in outcome.tasks] == ["t1", "t2", "t3"]
    assert outcome.tasks == harness.store.list_tasks(SESSION_ID, plan_id=PLAN_ID)
    assert isinstance(outcome, PlanOutcome)


# ================================================================================================
# 10. persistence before publication (ADR-015)
# ================================================================================================
async def given_store_failing_on_first_write_when_run_then_persistence_error_and_no_event(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1")])
    harness.store.fail_next_write = True

    with pytest.raises(PersistenceError):
        await harness.run(plan, tasks)

    assert harness.recorder.events == []
    assert harness.stored_plan().status is PlanState.PENDING
    assert harness.task("t1").status is TaskState.PENDING
    assert harness.fake.calls == []


async def given_store_failing_on_terminal_write_when_task_ends_then_error_propagates_without_terminal_event(
    harness: Harness,
) -> None:
    harness.fake.script(task_id="t1", stdout=b"out")
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])

    def arm(event: Event) -> None:
        if event.event_type is EventType.TASK_OUTPUT:
            harness.store.fail_next_write = True  # next write: the terminal transaction

    harness.bus.subscribe(arm, name="arm")
    with pytest.raises(PersistenceError):
        await harness.run(plan, tasks)

    kinds = [(e.event_type, e.task_id, e.payload.get("to")) for e in harness.recorder.events]
    assert kinds == [
        (EventType.PLAN_STATE_CHANGED, None, "RUNNING"),
        (EventType.TASK_STATE_CHANGED, "t1", "RUNNING"),
        (EventType.TASK_OUTPUT, "t1", None),
    ]
    stored = harness.task("t1")
    assert stored.status is TaskState.RUNNING and stored.pid == 4_000
    assert harness.blob("t1", OutputStream.STDOUT) is None  # the transaction rolled back
    assert harness.task("t2").status is TaskState.PENDING
    assert harness.stored_plan().status is PlanState.RUNNING


async def given_store_failing_on_pid_write_when_spawned_then_error_propagates_and_task_stays_running(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1")])

    def arm(event: Event) -> None:
        if event.event_type is EventType.TASK_STATE_CHANGED and event.payload["to"] == "RUNNING":
            harness.store.fail_next_write = True  # next write: the silent pid update

    harness.bus.subscribe(arm, name="arm")
    with pytest.raises(PersistenceError):
        await harness.run(plan, tasks)

    assert _transitions(harness.recorder, "t1") == [("PENDING", "RUNNING")]
    stored = harness.task("t1")
    assert stored.status is TaskState.RUNNING and stored.pid is None


# ================================================================================================
# 11. invalid transitions (§5.2, §5.3)
# ================================================================================================
@pytest.mark.parametrize(
    "status",
    [
        PlanState.RUNNING,
        PlanState.COMPLETED,
        PlanState.STOPPED_ON_FAILURE,
        PlanState.SHORT_CIRCUITED_ON_SUCCESS,
        PlanState.INTERRUPTED,
        PlanState.FAILED,
    ],
)
async def given_plan_not_pending_when_run_then_invalid_transition_error_and_nothing_happens(
    harness: Harness, status: PlanState
) -> None:
    plan, tasks = harness.plan([_cmd("t1")])
    not_pending = plan.model_copy(update={"status": status})

    with pytest.raises(InvalidTransitionError) as info:
        await harness.run(not_pending, tasks)

    assert (info.value.entity, info.value.current) == ("plan", status.value)
    assert harness.recorder.events == [] and harness.fake.calls == []
    assert harness.stored_plan().status is PlanState.PENDING


async def given_task_not_pending_when_run_then_invalid_transition_error_before_any_write(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])
    tasks[1] = tasks[1].model_copy(update={"status": TaskState.COMPLETED})
    with pytest.raises(InvalidTransitionError) as info:
        await harness.run(plan, tasks)
    assert info.value.entity == "task"
    assert harness.recorder.events == [] and harness.stored_plan().status is PlanState.PENDING


async def given_task_of_another_plan_when_run_then_value_error(harness: Harness) -> None:
    plan, tasks = harness.plan([_cmd("t1")])
    foreign = tasks[0].model_copy(update={"plan_id": "plan-other"})
    with pytest.raises(ValueError, match="plan-other"):
        await harness.run(plan, [foreign])
    assert harness.recorder.events == []


async def given_parallel_plan_with_dependency_cycle_when_run_then_value_error_before_any_write(
    harness: Harness,
) -> None:
    """Hand-built records (the adapter refuses cycles, ADR-007): refused upfront, never a plan
    left RUNNING with tasks stuck in WAITING_DEPENDENCY."""
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2", depends_on=["t1"])], policy="parallel")
    cyclic = [tasks[0].model_copy(update={"depends_on": ("t2",)}), tasks[1]]

    with pytest.raises(ValueError, match="cycle"):
        await harness.run(plan, cyclic)

    assert harness.recorder.events == [] and harness.fake.calls == []
    assert harness.stored_plan().status is PlanState.PENDING
    assert harness.statuses() == {"t1": TaskState.PENDING, "t2": TaskState.PENDING}


async def given_sequential_plan_with_forward_dependency_when_run_then_value_error_before_any_write(
    harness: Harness,
) -> None:
    """A sequential plan only depends backwards (ADR-007): a forward edge can never be satisfied
    in order and is refused upfront instead of stalling the scheduler."""
    plan, tasks = harness.plan([_cmd("t1"), _cmd("t2")])
    forward = [tasks[0].model_copy(update={"depends_on": ("t2",)}), tasks[1]]

    with pytest.raises(ValueError, match="sequential"):
        await harness.run(plan, forward)

    assert harness.recorder.events == [] and harness.fake.calls == []
    assert harness.stored_plan().status is PlanState.PENDING


async def given_task_depending_on_itself_when_run_then_value_error_before_any_write(
    harness: Harness,
) -> None:
    plan, tasks = harness.plan([_cmd("t1")], policy="parallel")
    selfish = [tasks[0].model_copy(update={"depends_on": ("t1",)})]
    with pytest.raises(ValueError, match="cycle"):
        await harness.run(plan, selfish)
    assert harness.recorder.events == []


# ================================================================================================
# 12. determinism (ADR-017) and hygiene
# ================================================================================================
async def given_same_plan_run_twice_on_fresh_doubles_when_compared_then_events_and_result_identical(
    config: AppConfig,
) -> None:
    def scenario(h: Harness) -> tuple[list[tuple[Any, ...]], dict[str, Any] | None]:
        h.fake.script(task_id="t1", stdout=b"one", duration_ms=10)
        h.fake.script(task_id="t2", exit_code=1, stderr=b"bad", duration_ms=5)
        return ([], None)

    async def run(h: Harness) -> tuple[list[tuple[Any, ...]], dict[str, Any] | None]:
        scenario(h)
        plan, tasks = h.plan(
            [
                _cmd("t1"),
                _cmd("t2", continue_on_error=True),
                _cmd("t3", depends_on=["t2"]),
                _cmd("t4"),
            ],
            policy="parallel",
            workers=2,
        )
        outcome = await h.run(plan, tasks)
        events = [
            (e.event_type, e.task_id, e.timestamp, tuple(sorted(e.payload.items())))
            for e in h.recorder.events
        ]
        result = (
            outcome.execution_result.model_dump(mode="json")
            if outcome.execution_result is not None
            else None
        )
        return events, result

    first = await run(Harness.fresh(config))
    second = await run(Harness.fresh(config))
    assert first == second
    assert first[1] is not None and first[1]["status"] == "completed"


async def given_phase5_events_when_consumed_by_audit_tracker_and_telemetry_then_phase10_contract_honoured(
    store: InMemoryConversationStore,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    config: AppConfig,
) -> None:
    """The cross-check recommended by the phase 10 guide (§11.2): the runner's events replayed
    through the real subscribers, in the production order (ADR-015 §4)."""
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
    harness.fake.script(task_id="t1", stdout=b"one", hold=True)
    harness.fake.script(task_id="t2", exit_code=1, stderr=b"bad", duration_ms=40)
    harness.fake.script(task_id="t4", stdout=b"four", duration_ms=25)
    plan, tasks = harness.plan(
        [_cmd("t1"), _cmd("t2", continue_on_error=True), _cmd("t3", depends_on=["t2"]), _cmd("t4")],
        policy="parallel",
        workers=2,
    )
    conversation = store.get_conversation(CONVERSATION_ID)
    assert conversation is not None
    store.save_conversation(conversation.model_copy(update={"current_plan_id": PLAN_ID}))
    running = harness.start(plan, tasks)
    await asyncio.wait_for(harness.fake.wait_spawned("t4"), BOUND_S)
    await _settle()

    live = tracker.snapshot(SESSION_ID)  # consistent "at any instant" (§4): t1 held, t4 done
    assert live.running_task_ids == ["t1"]
    assert live.plan is not None and live.plan.status is PlanState.RUNNING
    assert {t.task_id: t.status for t in live.tasks} == {
        "t1": TaskState.RUNNING,
        "t2": TaskState.FAILED,
        "t3": TaskState.SKIPPED,
        "t4": TaskState.COMPLETED,
    }
    assert live.last_event_type == EventType.TASK_STATE_CHANGED.value
    harness.fake.release("t1")
    outcome = await asyncio.wait_for(running, BOUND_S)

    audited = [e for e in recorder.events if e.audited]
    assert len(audited) == len(recorder.events) - 3  # three task.output events, not audited
    verification = audit.verify(SESSION_ID)
    assert verification.valid is True and verification.checked == len(audited)
    trail = store.list_audit_events(SESSION_ID)
    assert [e.sequence for e in trail] == list(range(1, len(audited) + 1))
    assert [(e.event_type, e.task_id, e.payload) for e in trail] == [
        (e.event_type.value, e.task_id, e.payload) for e in audited
    ]
    final = tracker.snapshot(SESSION_ID)
    assert final.plan is not None and final.plan.status is PlanState.COMPLETED
    assert final.running_task_ids == []
    assert (final.plan.completed_task_count, final.plan.failed_task_count) == (2, 1)
    assert final.plan.skipped_task_count == 1 and final.plan.stop_reason is None
    assert final.last_event_type == EventType.PLAN_STATE_CHANGED.value
    assert final.last_event_sequence == len(audited)
    assert final == tracker.rebuild(SESSION_ID)
    metrics = telemetry.metrics()
    assert metrics["counters"]["task_terminal_total"] == [
        {"labels": {"status": "COMPLETED"}, "value": 2},
        {"labels": {"status": "FAILED"}, "value": 1},
        {"labels": {"status": "SKIPPED"}, "value": 1},
    ]
    assert metrics["counters"]["plan_terminal_total"] == [
        {"labels": {"status": "COMPLETED"}, "value": 1}
    ]
    durations = metrics["histograms"]["task_duration_ms"]
    # t2: 40 ms · t4: 25 ms · t1: held while the others ran on the shared clock, 65 ms
    assert (durations["count"], durations["min"], durations["max"]) == (3, 25, 65)
    assert outcome.plan.status is PlanState.COMPLETED


def given_plan_runner_module_when_inspected_then_no_wall_clock_or_randomness() -> None:
    source = open(plan_runner_module.__file__ or "", encoding="utf-8").read()  # noqa: SIM115
    for forbidden in ("datetime.now(", "time.time(", "time.monotonic(", "uuid4(", "random."):
        assert forbidden not in source, forbidden


def given_execution_package_when_imported_then_plan_runner_exported() -> None:
    from agentic_local_app import execution

    assert execution.PlanRunner is PlanRunner and execution.PlanOutcome is PlanOutcome
    assert "PlanRunner" in execution.__all__ and "PlanOutcome" in execution.__all__
