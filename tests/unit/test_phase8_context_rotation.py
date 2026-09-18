"""Phase 8 — context rotation (spec §2.6, §3.11, §5.4, §10, §12.8/§12.9, §18.2 phase 8).

Three components are pinned here, with the doubles of ``tests/conftest.py`` only (§18.3):

1. ``ContextWindowMonitor`` (ADR-013, ADR-019 §2): pure evaluation of the window state from the
   persisted byte counter, the projected outbound size and the classified error;
2. ``ContextReducer`` (ADR-005): the structured summary assembled from the store (verbatim copy of
   the model's last ``state_summary``, plan ledger, pending outputs, budget), its deterministic
   reduction steps and its explicit failure;
3. ``RotationCoordinator`` (ADR-014, ADR-007, ADR-012, ADR-019 §5): the full rotation sequence,
   parent ``ROTATING -> CLOSED``, child ``SATURATED -> HEALTHY`` on the ack, the resume cycle, the
   retransmission of the pending message, the events (phase 10 payload contract) and the state
   left behind at every failure point.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from agentic_local_app.config import AppConfig, ContextSection
from agentic_local_app.context import (
    ContextReducer,
    ContextThresholds,
    ContextWindowMonitor,
    PendingOutbound,
    RotationCoordinator,
    RotationResult,
    SummaryDraft,
)
from agentic_local_app.domain.canonical import canonical_json, size_bytes
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import (
    BudgetExceededError,
    ErrorType,
    InvalidTransitionError,
    NormalizedError,
    PersistenceError,
    ProtocolError,
    RotationFailedError,
    TransportError,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    BlobRecord,
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    PlanRecord,
    SessionBudget,
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
    OutputStream,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
    TaskType,
)
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.observability.execution_tracker import ExecutionTracker
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import OutboundMessage, ProtocolAdapter
from agentic_local_app.protocol.messages import ExecutionResultContent, TaskResult
from agentic_local_app.transport.fake import FakeTransportGateway

pytestmark = pytest.mark.phase8

# =============================================================================================
# Constants
# =============================================================================================

INSTRUCTIONS = "PROTOCOL v1.1 — réponds uniquement en JSON, un message par tour."
GOAL = "Understand the root cause of a Java build failure"
USER_MESSAGE = "Please debug the Java error in my project."
USER_ID = "local-user"
BUDGET = SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=300_000)
T0 = datetime(2026, 1, 1, tzinfo=UTC)

#: remote ids handed out by the FakeTransportGateway (first init = parent, second = child)
PARENT_REMOTE = "remote-0001"
CHILD_REMOTE = "remote-0002"

FIRST_SUMMARY = {
    "environment": {"os": "Linux x86_64", "shell": "/bin/bash", "cwd": "/workspace/project"},
    "findings": ["Java runtime = 17.0.12"],
    "current_state": "Environment discovered",
    "next_expected_step": "Inspect pom.xml",
}
LAST_SUMMARY = {
    "environment": {"os": "Linux x86_64", "shell": "/bin/bash", "cwd": "/workspace/project"},
    "findings": ["Java runtime = 17.0.12", "pom.xml targets Java 21"],
    "current_state": "Version mismatch suspected, confirming Maven runtime",
    "next_expected_step": "Confirm with mvn -version then conclude",
}
LONG_CMD = "mvn -B -q clean install -DskipTests " + "-Dproperty.number.{0}=value{0} " * 8

SUMMARY_KEY_ORDER = [
    "goal",
    "user_message",
    "environment",
    "findings",
    "current_state",
    "next_expected_step",
    "plan_ledger",
    "pending_outputs",
    "budget",
    "pending_message_type",
    "original_conversation_id",
]


# =============================================================================================
# Helpers — records
# =============================================================================================


def _error(
    error_type: ErrorType, code: str = "CODE", *, retryable: bool = False
) -> NormalizedError:
    return NormalizedError(
        error_type=error_type, error_code=code, origin="Test", retryable=retryable
    )


def _conversation(
    *,
    context_bytes: int = 0,
    window: ContextWindowState = ContextWindowState.HEALTHY,
    conversation_id: str = "conv-x",
) -> ConversationRecord:
    return ConversationRecord(
        conversation_id=conversation_id,
        session_id="sess-x",
        remote_conversation_id="remote-x",
        status=ConversationState.WAITING_MODEL_RESPONSE,
        auto_close_on_final_answer=False,
        context_window_state=window,
        context_bytes=context_bytes,
        created_at=T0,
        updated_at=T0,
    )


def _plan_record(
    session_id: str,
    conversation_id: str,
    plan_id: str,
    *,
    status: PlanState,
    plan_type: PlanType = PlanType.DISCOVERY_PLAN,
    objective: str = "Discover execution environment and build context",
    stop_reason: str | None = None,
    state_summary: dict[str, Any] | None = None,
    cycle_id: str = "cyc-plan",
) -> PlanRecord:
    return PlanRecord(
        plan_id=plan_id,
        session_id=session_id,
        conversation_id=conversation_id,
        cycle_id=cycle_id,
        plan_type=plan_type,
        objective=objective,
        execution_policy=ExecutionPolicy.SEQUENTIAL,
        status=status,
        stop_reason=stop_reason,
        state_summary=state_summary,
        created_at=T0,
        updated_at=T0,
    )


def _task_record(
    session_id: str,
    conversation_id: str,
    plan_id: str,
    task_id: str,
    order_index: int,
    *,
    cmd: str | None = "echo hello",
    task_type: TaskType = TaskType.CMD,
    status: TaskState = TaskState.COMPLETED,
    exit_code: int | None = 0,
    truncated: bool = False,
    original_size_bytes: int | None = 12,
    stdout_total: int | None = None,
    stderr_total: int | None = None,
    stdout_range: tuple[int, int] | None = None,
    stderr_range: tuple[int, int] | None = None,
    ref_task_id: str | None = None,
) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        plan_id=plan_id,
        session_id=session_id,
        conversation_id=conversation_id,
        order_index=order_index,
        type=task_type,
        cmd=cmd,
        status=status,
        exit_code=exit_code,
        truncated=truncated,
        original_size_bytes=original_size_bytes,
        stdout_total=stdout_total,
        stderr_total=stderr_total,
        stdout_range=stdout_range,
        stderr_range=stderr_range,
        ref_task_id=ref_task_id,
        stream=OutputStream.STDOUT if task_type is TaskType.CHUNK_REQUEST else None,
        byte_offset=8192 if task_type is TaskType.CHUNK_REQUEST else None,
        max_bytes=4096 if task_type is TaskType.CHUNK_REQUEST else None,
        created_at=T0,
        updated_at=T0,
    )


def _blob(
    session_id: str, task_id: str, stream: OutputStream, content: bytes, blob_id: str
) -> BlobRecord:
    return BlobRecord(
        blob_id=blob_id,
        session_id=session_id,
        task_id=task_id,
        blob_type=stream,
        content=content,
        size_bytes=len(content),
        created_at=T0,
    )


def _ack(
    child_remote: str = CHILD_REMOTE,
    original_remote: str = PARENT_REMOTE,
    *,
    acknowledged: bool = True,
    message_id: str = "model-ack-1",
) -> dict[str, Any]:
    return {
        "type": "context_resume_ack",
        "conversation_id": child_remote,
        "message_id": message_id,
        "content": {"original_conversation_id": original_remote, "acknowledged": acknowledged},
    }


def _events(recorder: RecordingSubscriber) -> list[tuple[str, str | None]]:
    return [(e.event_type.value, e.conversation_id) for e in recorder.events]


def _only(recorder: RecordingSubscriber, event_type: EventType) -> Event:
    events = recorder.of_type(event_type)
    assert len(events) == 1, f"expected exactly one {event_type.value}, got {len(events)}"
    return events[0]


# =============================================================================================
# Helpers — a running session whose conversation is saturated with a pending message
# =============================================================================================


@dataclass
class Scenario:
    session: SessionRecord
    source: ConversationRecord
    pending: PendingOutbound
    pending_content: ExecutionResultContent
    original_message_id: str


def _populate_store(
    store: InMemoryConversationStore, session_id: str, conversation_id: str
) -> None:
    """Two plans (one completed with a state_summary, one running), tasks and one truncated blob."""
    store.save_plan(
        _plan_record(
            session_id,
            conversation_id,
            "plan-0",
            status=PlanState.COMPLETED,
            state_summary=LAST_SUMMARY,
        )
    )
    store.save_task(
        _task_record(
            session_id, conversation_id, "plan-0", "t1", 0, cmd="uname -a", original_size_bytes=55
        )
    )
    store.save_task(
        _task_record(
            session_id,
            conversation_id,
            "plan-0",
            "t2",
            1,
            cmd="sed -n '1,220p' pom.xml",
            truncated=True,
            original_size_bytes=48_211,
            stdout_total=48_211,
            stdout_range=(0, 8_192),
            stderr_total=0,
            stderr_range=(0, 0),
        )
    )
    store.save_blob(_blob(session_id, "t2", OutputStream.STDOUT, b"<pom>" * 9_643, "blob-t2-out"))
    store.save_blob(_blob(session_id, "t2", OutputStream.STDERR, b"", "blob-t2-err"))
    store.save_plan(
        _plan_record(
            session_id,
            conversation_id,
            "plan-1",
            status=PlanState.RUNNING,
            plan_type=PlanType.EXECUTION_PLAN,
            objective="Confirm the Maven runtime",
        )
    )
    store.save_task(
        _task_record(
            session_id,
            conversation_id,
            "plan-1",
            "t3",
            0,
            cmd="mvn -version",
            status=TaskState.RUNNING,
            exit_code=None,
            original_size_bytes=None,
        )
    )


async def _scenario(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    *,
    source_state: ConversationState = ConversationState.RUNNING_PLAN,
    window: ContextWindowState = ContextWindowState.SATURATED,
    context_bytes: int = 370_000,
    rotations_count: int = 0,
    consumed_cycles: int = 3,
    populate: bool = True,
) -> Scenario:
    session = lifecycle.create_session(GOAL, USER_MESSAGE, USER_ID, BUDGET, auto_close=False)
    sid = session.session_id
    lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
    conversation = lifecycle.create_conversation(sid)
    cid = conversation.conversation_id
    remote = await transport.init_conversation(INSTRUCTIONS, {"session_id": sid})
    assert remote == PARENT_REMOTE
    lifecycle.update_conversation(cid, remote_conversation_id=remote)
    lifecycle.transition_conversation(cid, ConversationState.ACTIVE, reason="init")
    lifecycle.transition_conversation(cid, ConversationState.WAITING_MODEL_RESPONSE)
    if source_state is ConversationState.RUNNING_PLAN:
        lifecycle.transition_conversation(cid, ConversationState.RUNNING_PLAN)
    elif source_state is ConversationState.WAITING_USER:
        lifecycle.transition_conversation(cid, ConversationState.COMPLETED)
        lifecycle.transition_conversation(cid, ConversationState.WAITING_USER)
        lifecycle.update_conversation(cid, final_answer_received=True)
    elif source_state is not ConversationState.WAITING_MODEL_RESPONSE:
        lifecycle.transition_conversation(cid, source_state)
    if window is not ContextWindowState.HEALTHY:
        lifecycle.transition_context_window(cid, window, reason="threshold")

    if populate:
        _populate_store(store, sid, cid)
    clock.advance(81_234)
    session = lifecycle.update_session(
        sid, consumed_cycles=consumed_cycles, consumed_plans=2, rotations_count=rotations_count
    )
    # the pending execution_result of plan-1, persisted (cycle open) but not yet POSTed
    original_message_id = ids.message_id()
    store.save_cycle(
        CycleRecord(
            cycle_id="cyc-orig",
            conversation_id=cid,
            session_id=sid,
            cycle_type=CycleType.EXECUTION,
            status=CycleState.RUNNING,
            outbound_message_id=original_message_id,
            plan_id="plan-1",
            started_at=clock.now(),
        )
    )
    content = ExecutionResultContent(
        plan_id="plan-1",
        status="completed",
        results=[
            TaskResult(task_id="t3", status="completed", exit_code=0, stdout="Apache Maven 3.9.6")
        ],
    )
    conversation = lifecycle.update_conversation(
        cid, context_bytes=context_bytes, current_cycle_id="cyc-orig", current_plan_id="plan-1"
    )
    original = adapter.build_execution_result(conversation, original_message_id, content)
    store.save_message(
        MessageRecord(
            message_id=original_message_id,
            session_id=sid,
            conversation_id=cid,
            direction=MessageDirection.OUTBOUND,
            message_type=MessageType.EXECUTION_RESULT,
            payload=original.payload,
            size_bytes=original.size_bytes,
            cycle_id="cyc-orig",
            created_at=clock.now(),
        )
    )
    pending = PendingOutbound(
        message_type=MessageType.EXECUTION_RESULT,
        original_message_id=original_message_id,
        build=lambda child, message_id: adapter.build_execution_result(child, message_id, content),
        cycle_id="cyc-orig",
    )
    return Scenario(
        session=session,
        source=conversation,
        pending=pending,
        pending_content=content,
        original_message_id=original_message_id,
    )


# =============================================================================================
# Fixtures
# =============================================================================================


@pytest.fixture
def adapter(config: AppConfig) -> ProtocolAdapter:
    return ProtocolAdapter(config)


@pytest.fixture
def transport(clock: FakeClock) -> FakeTransportGateway:
    return FakeTransportGateway(clock)


@pytest.fixture
def monitor(config: AppConfig) -> ContextWindowMonitor:
    return ContextWindowMonitor(config.context)


@pytest.fixture
def reducer(
    config: AppConfig,
    store: InMemoryConversationStore,
    clock: FakeClock,
    ids: SequentialIdGenerator,
) -> ContextReducer:
    return ContextReducer(config, store, clock, ids)


CoordinatorFactory = Callable[[AppConfig | None], RotationCoordinator]


@pytest.fixture
def make_coordinator(
    config: AppConfig,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    lifecycle: ConversationLifecycleManager,
    adapter: ProtocolAdapter,
    transport: FakeTransportGateway,
) -> CoordinatorFactory:
    def factory(override: AppConfig | None = None) -> RotationCoordinator:
        cfg = override or config
        return RotationCoordinator(
            cfg,
            store,
            bus,
            clock,
            ids,
            lifecycle,
            ProtocolAdapter(cfg),
            transport,
            ContextReducer(cfg, store, clock, ids),
            ContextWindowMonitor(cfg.context),
            instructions=INSTRUCTIONS,
        )

    return factory


@pytest.fixture
def coordinator(make_coordinator: CoordinatorFactory) -> RotationCoordinator:
    return make_coordinator(None)


def _tiny(**context: Any) -> AppConfig:
    return AppConfig(context=ContextSection(**context))


# =============================================================================================
# ContextWindowMonitor (ADR-013, ADR-019 §2)
# =============================================================================================


def given_budget_1000_when_thresholds_computed_then_700_900_1000() -> None:
    monitor = ContextWindowMonitor(
        ContextSection(budget_bytes=1_000, warning_ratio=0.7, saturation_ratio=0.9)
    )
    assert monitor.thresholds() == ContextThresholds(700, 900, 1_000)
    assert monitor.thresholds() == (700, 900, 1_000)


def given_default_config_when_thresholds_computed_then_280000_360000_400000(
    monitor: ContextWindowMonitor,
) -> None:
    assert monitor.thresholds() == (280_000, 360_000, 400_000)


@pytest.mark.parametrize(
    ("ratio", "budget", "expected"), [(0.7, 100, 70), (0.3, 10, 3), (0.5, 3, 2)]
)
def given_ratio_and_budget_when_threshold_computed_then_no_float_drift(
    ratio: float, budget: int, expected: int
) -> None:
    monitor = ContextWindowMonitor(
        ContextSection(budget_bytes=budget, warning_ratio=ratio, saturation_ratio=0.99)
    )
    assert monitor.thresholds().warning_bytes == expected


@pytest.fixture
def tiny_monitor() -> ContextWindowMonitor:
    return ContextWindowMonitor(
        ContextSection(budget_bytes=1_000, warning_ratio=0.7, saturation_ratio=0.9)
    )


def given_bytes_below_warning_ratio_when_evaluated_then_healthy(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    assert tiny_monitor.evaluate(_conversation(context_bytes=699)) is ContextWindowState.HEALTHY


def given_bytes_at_warning_ratio_when_evaluated_then_warning(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    assert tiny_monitor.evaluate(_conversation(context_bytes=700)) is ContextWindowState.WARNING
    assert tiny_monitor.evaluate(_conversation(context_bytes=899)) is ContextWindowState.WARNING


def given_bytes_at_saturation_ratio_when_evaluated_then_saturated(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    assert tiny_monitor.evaluate(_conversation(context_bytes=900)) is ContextWindowState.SATURATED
    assert tiny_monitor.evaluate(_conversation(context_bytes=5_000)) is ContextWindowState.SATURATED


def given_projected_post_over_budget_when_evaluated_then_saturated(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    healthy = _conversation(context_bytes=500)
    assert (
        tiny_monitor.evaluate(healthy, projected_outbound_bytes=501) is ContextWindowState.SATURATED
    )


def given_projected_post_within_budget_when_evaluated_then_ratios_apply_to_current_bytes_only(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    # ADR-013 §3: the ratios apply to context_bytes; the projection is compared to the budget.
    assert (
        tiny_monitor.evaluate(_conversation(context_bytes=500), projected_outbound_bytes=500)
        is ContextWindowState.HEALTHY
    )
    assert (
        tiny_monitor.evaluate(_conversation(context_bytes=650), projected_outbound_bytes=300)
        is ContextWindowState.HEALTHY
    )
    assert (
        tiny_monitor.evaluate(_conversation(context_bytes=700), projected_outbound_bytes=250)
        is ContextWindowState.WARNING
    )


def given_context_window_error_when_evaluated_then_saturated_from_healthy(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    error = _error(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "CONTEXT_WINDOW_EXCEEDED")
    assert tiny_monitor.evaluate(_conversation(context_bytes=0), error=error) is (
        ContextWindowState.SATURATED
    )


def given_context_window_error_when_evaluated_from_warning_then_saturated(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    error = _error(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413")
    conversation = _conversation(context_bytes=750, window=ContextWindowState.WARNING)
    assert tiny_monitor.evaluate(conversation, error=error) is ContextWindowState.SATURATED


@pytest.mark.parametrize(
    "error_type",
    [ErrorType.NETWORK_ERROR, ErrorType.TIMEOUT_ERROR, ErrorType.RATE_LIMIT_ERROR],
)
def given_unrelated_error_when_evaluated_then_bytes_decide(
    tiny_monitor: ContextWindowMonitor, error_type: ErrorType
) -> None:
    assert tiny_monitor.evaluate(_conversation(context_bytes=10), error=_error(error_type)) is (
        ContextWindowState.HEALTHY
    )


def given_unusable_reply_in_warning_when_evaluated_then_saturated(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    """The ADR-019 §2 rule is part of ``evaluate`` (06 §1.1 flowchart, branch E2)."""
    warning = _conversation(context_bytes=750, window=ContextWindowState.WARNING)
    protocol = _error(ErrorType.MODEL_PROTOCOL_ERROR, "SCHEMA_INVALID")
    assert tiny_monitor.evaluate(warning, error=protocol) is ContextWindowState.SATURATED
    healthy = _conversation(context_bytes=10)
    assert tiny_monitor.evaluate(healthy, error=protocol) is ContextWindowState.HEALTHY


def given_warning_conversation_when_bytes_below_warning_then_state_stays_warning(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    conversation = _conversation(context_bytes=0, window=ContextWindowState.WARNING)
    assert tiny_monitor.evaluate(conversation) is ContextWindowState.WARNING


def given_saturated_conversation_when_bytes_below_warning_then_state_stays_saturated(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    conversation = _conversation(context_bytes=0, window=ContextWindowState.SATURATED)
    assert tiny_monitor.evaluate(conversation) is ContextWindowState.SATURATED
    assert tiny_monitor.evaluate(conversation, projected_outbound_bytes=1) is (
        ContextWindowState.SATURATED
    )


def given_negative_projection_when_evaluated_then_value_error(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    with pytest.raises(ValueError):
        tiny_monitor.evaluate(_conversation(), projected_outbound_bytes=-1)


def given_warning_and_protocol_error_when_should_rotate_asked_then_true(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    warning = _conversation(window=ContextWindowState.WARNING)
    assert tiny_monitor.should_rotate_on_unusable_reply(
        warning, _error(ErrorType.MODEL_PROTOCOL_ERROR, "UNEXPECTED_MESSAGE_TYPE")
    )


def given_warning_and_get_timeout_when_should_rotate_asked_then_true(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    warning = _conversation(window=ContextWindowState.WARNING)
    timeout = _error(ErrorType.TIMEOUT_ERROR, "MODEL_GET_TIMEOUT", retryable=True)
    assert tiny_monitor.should_rotate_on_unusable_reply(warning, timeout)


def given_warning_and_request_timeout_when_should_rotate_asked_then_false(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    warning = _conversation(window=ContextWindowState.WARNING)
    timeout = _error(ErrorType.TIMEOUT_ERROR, "REQUEST_TIMEOUT", retryable=True)
    assert not tiny_monitor.should_rotate_on_unusable_reply(warning, timeout)


@pytest.mark.parametrize("window", [ContextWindowState.HEALTHY, ContextWindowState.SATURATED])
def given_non_warning_window_and_protocol_error_when_should_rotate_asked_then_false(
    tiny_monitor: ContextWindowMonitor, window: ContextWindowState
) -> None:
    conversation = _conversation(window=window)
    assert not tiny_monitor.should_rotate_on_unusable_reply(
        conversation, _error(ErrorType.MODEL_PROTOCOL_ERROR)
    )


def given_flag_disabled_when_should_rotate_asked_in_warning_then_false() -> None:
    monitor = ContextWindowMonitor(
        ContextSection(budget_bytes=1_000, rotate_on_unusable_reply_in_warning=False)
    )
    warning = _conversation(window=ContextWindowState.WARNING)
    assert not monitor.should_rotate_on_unusable_reply(
        warning, _error(ErrorType.MODEL_PROTOCOL_ERROR)
    )
    # ...and evaluate then leaves the window alone too
    assert monitor.evaluate(warning, error=_error(ErrorType.MODEL_PROTOCOL_ERROR)) is (
        ContextWindowState.WARNING
    )


@pytest.mark.parametrize(
    "error_type", [ErrorType.NETWORK_ERROR, ErrorType.RATE_LIMIT_ERROR, ErrorType.AUTHN_ERROR]
)
def given_other_error_type_when_should_rotate_asked_in_warning_then_false(
    tiny_monitor: ContextWindowMonitor, error_type: ErrorType
) -> None:
    warning = _conversation(window=ContextWindowState.WARNING)
    assert not tiny_monitor.should_rotate_on_unusable_reply(warning, _error(error_type))


def given_bytes_when_accounted_then_plain_sum(tiny_monitor: ContextWindowMonitor) -> None:
    assert tiny_monitor.account(100, 50) == 150
    assert tiny_monitor.account(0, 0) == 0


def given_negative_bytes_when_accounted_then_value_error(
    tiny_monitor: ContextWindowMonitor,
) -> None:
    with pytest.raises(ValueError):
        tiny_monitor.account(-1, 5)
    with pytest.raises(ValueError):
        tiny_monitor.account(5, -1)


def given_instructions_text_when_measured_then_utf8_bytes_of_the_text() -> None:
    assert ContextWindowMonitor.instructions_bytes("héllo") == 6
    assert ContextWindowMonitor.instructions_bytes("") == 0


# =============================================================================================
# ContextReducer (ADR-005)
# =============================================================================================


def _rich_store(
    store: InMemoryConversationStore, lifecycle: ConversationLifecycleManager
) -> tuple[SessionRecord, ConversationRecord]:
    """Three plans across two conversations; the last state_summary is on plan-1."""
    session = lifecycle.create_session(GOAL, USER_MESSAGE, USER_ID, BUDGET, auto_close=False)
    sid = session.session_id
    lifecycle.transition_session(sid, SessionState.RUNNING)
    first = lifecycle.create_conversation(sid)
    lifecycle.update_conversation(first.conversation_id, remote_conversation_id="remote-first")
    lifecycle.transition_conversation(first.conversation_id, ConversationState.ACTIVE)
    lifecycle.transition_conversation(first.conversation_id, ConversationState.INTERRUPTED)
    second = lifecycle.create_conversation(sid, parent_conversation_id=first.conversation_id)
    second = lifecycle.update_conversation(
        second.conversation_id, remote_conversation_id=PARENT_REMOTE
    )
    session = lifecycle.update_session(sid, consumed_cycles=3, consumed_plans=3)
    c1, c2 = first.conversation_id, second.conversation_id

    store.save_plan(
        _plan_record(sid, c1, "plan-0", status=PlanState.COMPLETED, state_summary=FIRST_SUMMARY)
    )
    store.save_task(_task_record(sid, c1, "plan-0", "t1", 0, cmd=LONG_CMD, original_size_bytes=55))
    store.save_task(
        _task_record(
            sid,
            c1,
            "plan-0",
            "t2",
            1,
            cmd=LONG_CMD + " | tail -400",
            truncated=True,
            original_size_bytes=48_211,
            stdout_total=48_000,
            stdout_range=(0, 8_192),
            stderr_total=211,
            stderr_range=(0, 211),
        )
    )
    store.save_blob(_blob(sid, "t2", OutputStream.STDOUT, b"o" * 48_000, "blob-t2-out"))
    store.save_blob(_blob(sid, "t2", OutputStream.STDERR, b"e" * 211, "blob-t2-err"))

    store.save_plan(
        _plan_record(
            sid,
            c2,
            "plan-1",
            status=PlanState.STOPPED_ON_FAILURE,
            plan_type=PlanType.EXECUTION_PLAN,
            objective="Build the project",
            stop_reason="critical_task_failed:t3",
            state_summary=LAST_SUMMARY,
        )
    )
    store.save_task(
        _task_record(
            sid,
            c2,
            "plan-1",
            "t3",
            0,
            cmd=LONG_CMD + " 2>&1 | tail -80",
            status=TaskState.TIMED_OUT,
            exit_code=None,
            truncated=True,
            original_size_bytes=32_000,
            stdout_total=32_000,
            stdout_range=(0, 4_096),
            stderr_total=0,
            stderr_range=(0, 0),
        )
    )
    store.save_blob(_blob(sid, "t3", OutputStream.STDOUT, b"m" * 32_000, "blob-t3-out"))
    store.save_blob(_blob(sid, "t3", OutputStream.STDERR, b"", "blob-t3-err"))
    # a truncated task whose blobs were never stored (interrupted before persistence)
    store.save_task(
        _task_record(
            sid,
            c2,
            "plan-1",
            "t4",
            1,
            cmd=LONG_CMD,
            status=TaskState.INTERRUPTED,
            exit_code=None,
            truncated=True,
            original_size_bytes=None,
        )
    )

    store.save_plan(
        _plan_record(
            sid,
            c2,
            "plan-2",
            status=PlanState.RUNNING,
            plan_type=PlanType.EXECUTION_PLAN,
            objective="Fetch the rest of the pom",
        )
    )
    store.save_task(
        _task_record(
            sid,
            c2,
            "plan-2",
            "t5",
            0,
            cmd=None,
            task_type=TaskType.CHUNK_REQUEST,
            status=TaskState.PENDING,
            exit_code=None,
            original_size_bytes=None,
            ref_task_id="t2",
        )
    )
    return session, second


def _build(
    reducer: ContextReducer,
    session: SessionRecord,
    source: ConversationRecord,
    *,
    pending: MessageType = MessageType.EXECUTION_RESULT,
    target: str = "conv-child",
) -> Any:
    return reducer.build(
        session, source, pending_message_type=pending, target_conversation_id=target
    )


def given_last_state_summary_when_summary_built_then_findings_copied_verbatim(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    record = _build(reducer, session, source)
    payload = record.summary_payload
    assert payload["findings"] == LAST_SUMMARY["findings"]
    assert payload["environment"] == LAST_SUMMARY["environment"]
    assert payload["current_state"] == LAST_SUMMARY["current_state"]
    assert payload["next_expected_step"] == LAST_SUMMARY["next_expected_step"]
    # the newest plan without a state_summary (plan-2) does not erase the last known one
    assert payload["findings"] != FIRST_SUMMARY["findings"]
    assert payload["findings"] is not LAST_SUMMARY["findings"]  # a copy, never a shared list


def given_no_state_summary_when_summary_built_then_model_sections_absent(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    for plan in store.list_plans(session.session_id):
        store.save_plan(plan.model_copy(update={"state_summary": None}))
    payload = _build(reducer, session, source).summary_payload
    for key in ("environment", "findings", "current_state", "next_expected_step"):
        assert key not in payload
    assert list(payload) == [k for k in SUMMARY_KEY_ORDER if k not in LAST_SUMMARY]


def given_populated_store_when_summary_built_then_sections_in_fixed_order(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    record = _build(reducer, session, source, target="conv-child")
    payload = record.summary_payload
    assert list(payload) == SUMMARY_KEY_ORDER
    assert payload["goal"] == GOAL
    assert payload["user_message"] == USER_MESSAGE
    assert payload["pending_message_type"] == "execution_result"
    assert payload["original_conversation_id"] == PARENT_REMOTE  # the id the model knows
    assert payload["budget"] == {
        "max_cycles": 20,
        "max_plans": 10,
        "max_total_duration_ms": 300_000,
        "consumed_cycles": 3,
        "consumed_plans": 3,
        "consumed_duration_ms": 0,
    }
    assert record.reduction_step == 0
    assert record.summary_size_bytes == size_bytes(payload)
    assert record.source_conversation_id == source.conversation_id
    assert record.target_conversation_id == "conv-child"
    assert record.session_id == session.session_id


def given_running_session_when_summary_built_then_consumed_duration_follows_the_clock(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
    clock: FakeClock,
) -> None:
    session, source = _rich_store(store, lifecycle)
    clock.advance(81_234)
    payload = _build(reducer, session, source).summary_payload
    assert payload["budget"]["consumed_duration_ms"] == 81_234


def given_plans_and_tasks_when_summary_built_then_ledger_complete_and_ordered(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    ledger = _build(reducer, session, source).summary_payload["plan_ledger"]
    assert [p["plan_id"] for p in ledger] == ["plan-0", "plan-1", "plan-2"]
    assert ledger[0] == {
        "plan_id": "plan-0",
        "plan_type": "discovery_plan",
        "objective": "Discover execution environment and build context",
        "status": "completed",
        "stop_reason": None,
        "tasks": [
            {
                "task_id": "t1",
                "cmd": LONG_CMD,
                "status": "completed",
                "exit_code": 0,
                "truncated": False,
                "original_size_bytes": 55,
            },
            {
                "task_id": "t2",
                "cmd": LONG_CMD + " | tail -400",
                "status": "completed",
                "exit_code": 0,
                "truncated": True,
                "original_size_bytes": 48_211,
            },
        ],
    }
    assert ledger[1]["status"] == "stopped_on_failure"
    assert ledger[1]["stop_reason"] == "critical_task_failed:t3"
    assert ledger[1]["plan_type"] == "execution_plan"
    assert [t["task_id"] for t in ledger[1]["tasks"]] == ["t3", "t4"]
    assert ledger[1]["tasks"][0]["status"] == "timed_out"
    assert ledger[1]["tasks"][0]["exit_code"] is None
    assert ledger[2]["status"] == "running"
    assert ledger[2]["tasks"] == [
        {
            "task_id": "t5",
            "cmd": None,
            "status": "pending",
            "exit_code": None,
            "truncated": False,
            "original_size_bytes": None,
        }
    ]


def given_truncated_tasks_when_summary_built_then_pending_outputs_list_fetchable_streams(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    pending = _build(reducer, session, source).summary_payload["pending_outputs"]
    # t2: stdout cut (8 192 of 48 000 sent), stderr fully delivered -> stdout only;
    # t3: stdout cut, stderr empty -> stdout only; t4: truncated but no blob -> nothing.
    assert pending == [
        {"task_id": "t2", "stream": "stdout", "total_bytes": 48_000},
        {"task_id": "t3", "stream": "stdout", "total_bytes": 32_000},
    ]


def given_same_store_when_summary_built_twice_then_identical_bytes(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    first = _build(reducer, session, source, target="conv-a")
    second = _build(reducer, session, source, target="conv-b")
    assert canonical_json(first.summary_payload) == canonical_json(second.summary_payload)
    assert first.summary_size_bytes == second.summary_size_bytes
    assert first.reduction_step == second.reduction_step
    assert first.summary_id != second.summary_id


def given_summary_built_when_store_read_then_record_persisted_with_generated_id(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
    clock: FakeClock,
) -> None:
    session, source = _rich_store(store, lifecycle)
    record = _build(reducer, session, source, target="conv-child")
    assert record.summary_id == "sum-0001"
    assert record.created_at == clock.now()
    assert store.get_context_summary_for_target("conv-child") == record
    assert store.list_context_summaries(session.session_id) == [record]


def given_ledger_exceeding_budget_when_reduction_applied_then_steps_recorded(
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
    clock: FakeClock,
    ids: SequentialIdGenerator,
) -> None:
    session, source = _rich_store(store, lifecycle)

    def build_with(budget: int) -> Any:
        reducer = ContextReducer(_tiny(summary_budget_bytes=budget), store, clock, ids)
        return _build(reducer, session, source)

    # the largest budget the ADR-019 §4 invariant allows (summary <= max_message_bytes)
    full = build_with(AppConfig().payload.max_message_bytes)
    assert full.reduction_step == 0
    size0 = full.summary_size_bytes

    step_a = build_with(size0 - 1)
    assert step_a.reduction_step == 1
    assert step_a.summary_size_bytes < size0
    ledger_a = step_a.summary_payload["plan_ledger"]
    assert [p["plan_id"] for p in ledger_a] == ["plan-0", "plan-1", "plan-2"]
    assert all("cmd" not in task for plan in ledger_a for task in plan["tasks"])
    assert "pending_outputs" in step_a.summary_payload
    assert step_a.summary_payload["findings"] == LAST_SUMMARY["findings"]

    step_b = build_with(step_a.summary_size_bytes - 1)
    assert step_b.reduction_step == 2
    assert step_b.summary_size_bytes < step_a.summary_size_bytes
    ledger_b = step_b.summary_payload["plan_ledger"]
    # only the non-terminal plans plus the last terminal one
    assert [p["plan_id"] for p in ledger_b] == ["plan-1", "plan-2"]
    assert all("cmd" not in task for plan in ledger_b for task in plan["tasks"])
    assert "pending_outputs" in step_b.summary_payload

    step_c = build_with(step_b.summary_size_bytes - 1)
    assert step_c.reduction_step == 3
    assert step_c.summary_size_bytes < step_b.summary_size_bytes
    assert "pending_outputs" not in step_c.summary_payload
    assert [p["plan_id"] for p in step_c.summary_payload["plan_ledger"]] == ["plan-1", "plan-2"]
    assert step_c.summary_payload["findings"] == LAST_SUMMARY["findings"]
    assert list(step_c.summary_payload) == [k for k in SUMMARY_KEY_ORDER if k != "pending_outputs"]

    exact = build_with(step_c.summary_size_bytes)
    assert exact.reduction_step == 3  # "<= budget" is enough


def given_summary_exceeding_budget_after_all_steps_when_built_then_rotation_failed(
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
    clock: FakeClock,
    ids: SequentialIdGenerator,
) -> None:
    session, source = _rich_store(store, lifecycle)
    # a one-byte budget can only fail: the error reports the size reached after the three steps
    probe = ContextReducer(_tiny(summary_budget_bytes=1), store, clock, ids)
    with pytest.raises(RotationFailedError) as probe_info:
        probe.compose(session, source, pending_message_type=MessageType.EXECUTION_RESULT)
    minimum = probe_info.value.error.details["size_bytes"]
    assert probe_info.value.error.details == {"size_bytes": minimum, "budget_bytes": 1, "step": 3}
    assert minimum > 1

    # one byte below the minimum the three steps can reach: explicit failure, nothing persisted
    reducer = ContextReducer(_tiny(summary_budget_bytes=minimum - 1), store, clock, ids)
    with pytest.raises(RotationFailedError) as exc_info:
        _build(reducer, session, source)
    error = exc_info.value.error
    assert error.error_type is ErrorType.ROTATION_FAILED
    assert error.error_code == "SUMMARY_EXCEEDS_BUDGET"
    assert error.recoverable is False
    assert error.details == {"size_bytes": minimum, "budget_bytes": minimum - 1, "step": 3}
    assert store.list_context_summaries(session.session_id) == []

    # exactly at the minimum it fits, with every step applied
    fits = ContextReducer(_tiny(summary_budget_bytes=minimum), store, clock, ids)
    draft = fits.compose(session, source, pending_message_type=MessageType.EXECUTION_RESULT)
    assert isinstance(draft, SummaryDraft)
    assert (draft.size_bytes, draft.reduction_step) == (minimum, 3)


def given_compose_when_called_then_pure_draft_without_persistence(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    draft = reducer.compose(session, source, pending_message_type=MessageType.USER_REQUEST)
    assert draft.payload["pending_message_type"] == "user_request"
    assert draft.size_bytes == size_bytes(draft.payload)
    assert draft.reduction_step == 0
    assert store.list_context_summaries(session.session_id) == []
    record = reducer.persist(
        draft,
        session_id=session.session_id,
        source_conversation_id=source.conversation_id,
        target_conversation_id="conv-child",
    )
    assert record.summary_payload == draft.payload
    assert store.get_context_summary_for_target("conv-child") == record


def given_source_without_remote_id_when_summary_built_then_local_id_used(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    source = lifecycle.update_conversation(source.conversation_id, remote_conversation_id=None)
    payload = _build(reducer, session, source).summary_payload
    assert payload["original_conversation_id"] == source.conversation_id


# =============================================================================================
# RotationCoordinator — the nominal sequence (ADR-014)
# =============================================================================================


async def given_context_saturated_when_rotation_completes_then_pending_result_retransmitted_in_child(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    source_id = scenario.source.conversation_id
    sid = scenario.session.session_id
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    recorder.clear()

    result = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)

    # ---- result -------------------------------------------------------------------------
    assert isinstance(result, RotationResult)
    child = result.child
    assert child.conversation_id != source_id
    assert result.source.conversation_id == source_id
    assert result.ack_message_id == "model-ack-1"

    # ---- parent: ROTATING then CLOSED (closure_reason rotated), window untouched ---------
    source = store.get_conversation(source_id)
    assert source is not None
    assert source.status is ConversationState.CLOSED
    assert source.closure_reason == "rotated"
    assert source.context_window_state is ContextWindowState.SATURATED
    assert result.source == source

    # ---- child: filiation, remote id, SATURATED -> HEALTHY, waiting for the model --------
    stored_child = store.get_conversation(child.conversation_id)
    assert stored_child == child
    assert child.parent_conversation_id == source_id
    assert child.session_id == sid
    assert child.remote_conversation_id == CHILD_REMOTE
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert child.get_cursor == "model-ack-1"
    assert child.last_inbound_message_id == "model-ack-1"
    assert child.last_outbound_message_id == result.retransmitted_message_id
    assert child.current_cycle_id == "cyc-orig"  # the retransmission continues M's cycle
    assert child.session_budget_json == BUDGET.model_dump()
    assert child.last_model_response_state == "awaiting"

    # ---- session: one rotation, one resume cycle, points to the child -------------------
    session = store.get_session(sid)
    assert session is not None
    assert session.rotations_count == 1
    assert session.consumed_cycles == scenario.session.consumed_cycles + 1
    assert session.consumed_plans == scenario.session.consumed_plans
    assert session.current_conversation_id == child.conversation_id
    assert session.status is SessionState.RUNNING

    # ---- summary -------------------------------------------------------------------------
    summary = result.summary
    assert store.get_context_summary_for_target(child.conversation_id) == summary
    assert summary.source_conversation_id == source_id
    assert summary.summary_payload["pending_message_type"] == "execution_result"
    assert summary.summary_payload["original_conversation_id"] == PARENT_REMOTE
    assert summary.summary_payload["findings"] == LAST_SUMMARY["findings"]

    # ---- transport: init of the child, then the two POSTs in the child ------------------
    assert len(transport.inits) == 2
    assert transport.inits[1] == {
        "instructions": INSTRUCTIONS,
        "metadata": {
            "session_id": sid,
            "parent_conversation_id": source_id,
            "rotation_index": 1,
        },
    }
    assert [remote for remote, _ in transport.posted] == [CHILD_REMOTE, CHILD_REMOTE]
    resume_payload, retransmitted_payload = (payload for _, payload in transport.posted)
    assert resume_payload["type"] == "context_resume_request"
    assert resume_payload["conversation_id"] == CHILD_REMOTE
    assert resume_payload["content"]["original_conversation_id"] == PARENT_REMOTE
    assert resume_payload["content"]["goal"] == GOAL
    assert resume_payload["content"]["pending_message_type"] == "execution_result"
    assert resume_payload["content"]["context_summary"] == summary.summary_payload
    assert retransmitted_payload["type"] == "execution_result"
    assert retransmitted_payload["conversation_id"] == CHILD_REMOTE
    assert retransmitted_payload["message_id"] == result.retransmitted_message_id
    assert retransmitted_payload["message_id"] != scenario.original_message_id
    original = store.get_message(scenario.original_message_id)
    assert original is not None
    assert retransmitted_payload["content"] == original.payload["content"]  # identical content
    assert transport.get_calls == [(CHILD_REMOTE, None)]
    assert transport.closed == [PARENT_REMOTE]  # best effort close of the remote parent

    # ---- message records of the child, in order -----------------------------------------
    messages = store.list_messages(child.conversation_id)
    assert [m.message_type for m in messages] == [
        MessageType.CONTEXT_RESUME_REQUEST,
        MessageType.CONTEXT_RESUME_ACK,
        MessageType.EXECUTION_RESULT,
    ]
    resume, ack, retransmitted = messages
    assert resume.direction is MessageDirection.OUTBOUND
    assert resume.post_confirmed is True and resume.posted_at == clock.now()
    assert resume.cycle_id == result.resume_cycle.cycle_id
    assert resume.payload == resume_payload
    assert resume.size_bytes == size_bytes(resume_payload)
    assert resume.retransmission_of is None
    assert ack.direction is MessageDirection.INBOUND
    assert ack.validation_status == "valid"
    assert ack.received_at == clock.now()
    assert ack.cycle_id == result.resume_cycle.cycle_id
    assert ack.payload == _ack()
    assert ack.size_bytes == size_bytes(_ack())
    assert retransmitted.direction is MessageDirection.OUTBOUND
    assert retransmitted.message_id == result.retransmitted_message_id
    assert retransmitted.retransmission_of == scenario.original_message_id
    assert retransmitted.cycle_id == "cyc-orig"
    assert retransmitted.post_confirmed is True
    assert retransmitted.payload == retransmitted_payload
    # the parent's copy of M is untouched
    assert store.list_messages(source_id) == [original]

    # ---- context bytes of the child: instructions + resume + ack + retransmission --------
    assert child.context_bytes == (
        ContextWindowMonitor.instructions_bytes(INSTRUCTIONS)
        + resume.size_bytes
        + ack.size_bytes
        + retransmitted.size_bytes
    )

    # ---- the resume cycle ------------------------------------------------------------------
    cycle = result.resume_cycle
    assert store.list_cycles(child.conversation_id) == [cycle]
    assert cycle.cycle_type is CycleType.RESUME
    assert cycle.status is CycleState.COMPLETED
    assert cycle.conversation_id == child.conversation_id
    assert cycle.outbound_message_id == resume.message_id
    assert cycle.inbound_message_id == "model-ack-1"
    assert cycle.ended_at == clock.now()
    assert cycle.plan_id is None
    original_cycle = store.get_cycle("cyc-orig")
    assert original_cycle is not None and original_cycle.status is CycleState.RUNNING

    # ---- events, in order, with the phase 10 payload contract ---------------------------
    child_id = child.conversation_id
    assert _events(recorder) == [
        ("conversation.state_changed", source_id),
        ("rotation.started", source_id),
        ("conversation.created", child_id),
        ("conversation.state_changed", child_id),
        ("cycle.started", child_id),
        ("conversation.state_changed", child_id),
        ("message.outbound", child_id),
        ("message.inbound", child_id),
        ("context.window_state_changed", child_id),
        ("conversation.state_changed", source_id),
        ("cycle.ended", child_id),
        ("rotation.completed", child_id),
        ("message.retransmitted", child_id),
    ]
    events = recorder.events
    assert events[0].payload == {
        "from": "RUNNING_PLAN",
        "to": "ROTATING",
        "reason": "context_saturated",
    }
    assert events[1].payload == {
        "source_conversation_id": source_id,
        "context_bytes": 370_000,
        "pending_message_type": "execution_result",
        "rotations_count": 0,
    }
    assert events[2].payload == {
        "parent_conversation_id": source_id,
        "context_window_state": "SATURATED",
    }
    assert events[3].payload["to"] == "ACTIVE"
    assert events[4].cycle_id == cycle.cycle_id
    assert events[4].payload == {
        "cycle_type": "resume",
        "outbound_message_type": "context_resume_request",
        "consumed_cycles": session.consumed_cycles,
    }
    assert events[5].payload["to"] == "WAITING_MODEL_RESPONSE"
    assert events[6].cycle_id == cycle.cycle_id
    assert events[6].payload == {
        "message_type": "context_resume_request",
        "message_id": resume.message_id,
        "post_status": 202,
        "size_bytes": resume.size_bytes,
    }
    assert events[7].cycle_id == cycle.cycle_id
    assert events[7].payload == {
        "message_type": "context_resume_ack",
        "message_id": "model-ack-1",
        "get_status": 200,
        "validation_status": "valid",
        "size_bytes": ack.size_bytes,
    }
    assert events[8].payload == {
        "from": "SATURATED",
        "to": "HEALTHY",
        "reason": "resume_acknowledged",
        "context_bytes": child.context_bytes - retransmitted.size_bytes,
    }
    assert events[9].payload == {"from": "ROTATING", "to": "CLOSED", "reason": "rotated"}
    assert events[10].cycle_id == cycle.cycle_id
    assert events[10].payload == {
        "status": "COMPLETED",
        "duration_ms": 0,
        "retry_count": 0,
        "inbound_message_type": "context_resume_ack",
    }
    assert events[11].payload == {
        "source_conversation_id": source_id,
        "target_conversation_id": child_id,
        "remote_conversation_id": CHILD_REMOTE,
        "summary_id": summary.summary_id,
        "summary_size_bytes": summary.summary_size_bytes,
        "reduction_step": 0,
    }
    assert events[12].cycle_id == "cyc-orig"
    assert events[12].payload == {
        "message_type": "execution_result",
        "message_id": result.retransmitted_message_id,
        "retransmission_of": scenario.original_message_id,
        "original_message_id": scenario.original_message_id,
        "new_message_id": result.retransmitted_message_id,
        "post_status": 202,
        "size_bytes": retransmitted.size_bytes,
        "reason": "rotation",
    }
    assert all(e.session_id == sid for e in events)
    assert all(e.timestamp == clock.now() for e in events)


async def given_pending_user_request_when_rotation_completes_then_user_request_retransmitted(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(
        lifecycle,
        store,
        transport,
        adapter,
        ids,
        clock,
        source_state=ConversationState.WAITING_USER,
    )
    follow_up = "Now also check the Gradle wrapper."
    pending = PendingOutbound(
        message_type=MessageType.USER_REQUEST,
        original_message_id="msg-follow-up",
        build=lambda child, message_id: adapter.build_user_request(
            child, message_id, GOAL, follow_up, BUDGET
        ),
    )
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    recorder.clear()

    result = await coordinator.rotate(scenario.session, scenario.source, pending)

    resume_payload, retransmitted_payload = (payload for _, payload in transport.posted)
    assert resume_payload["content"]["pending_message_type"] == "user_request"
    assert result.summary.summary_payload["pending_message_type"] == "user_request"
    assert retransmitted_payload["type"] == "user_request"
    assert retransmitted_payload["content"]["user_message"] == follow_up
    assert retransmitted_payload["message_id"] == result.retransmitted_message_id
    assert recorder.events[0].payload == {
        "from": "WAITING_USER",
        "to": "ROTATING",
        "reason": "context_saturated",
    }
    retransmitted = store.get_message(result.retransmitted_message_id)
    assert retransmitted is not None
    assert retransmitted.retransmission_of == "msg-follow-up"
    assert retransmitted.cycle_id is None
    assert result.child.current_cycle_id is None
    assert result.child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert result.source.status is ConversationState.CLOSED
    retransmitted_event = _only(recorder, EventType.MESSAGE_RETRANSMITTED)
    assert retransmitted_event.cycle_id is None
    assert retransmitted_event.payload["message_type"] == "user_request"


async def given_source_from_waiting_model_response_when_rotated_then_allowed(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    scenario = await _scenario(
        lifecycle,
        store,
        transport,
        adapter,
        ids,
        clock,
        source_state=ConversationState.WAITING_MODEL_RESPONSE,
    )
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    result = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert result.source.status is ConversationState.CLOSED
    assert result.child.context_window_state is ContextWindowState.HEALTHY


async def given_source_window_in_warning_when_rotated_then_saturated_first(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(
        lifecycle,
        store,
        transport,
        adapter,
        ids,
        clock,
        window=ContextWindowState.WARNING,
        context_bytes=300_000,
    )
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    recorder.clear()
    result = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    first = recorder.events[0]
    assert first.event_type is EventType.CONTEXT_WINDOW_STATE_CHANGED
    assert first.conversation_id == scenario.source.conversation_id
    assert first.payload == {
        "from": "WARNING",
        "to": "SATURATED",
        "reason": "rotation_requested",
        "context_bytes": 300_000,
    }
    assert recorder.events[1].payload["to"] == "ROTATING"
    assert result.source.context_window_state is ContextWindowState.SATURATED


async def given_child_conversation_when_rotated_again_then_chain_grows_and_counters_follow(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    first = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    # the child now waits for the model; pretend its window saturated again on the next GET
    child = lifecycle.transition_context_window(
        first.child.conversation_id, ContextWindowState.SATURATED, reason="context_error"
    )
    session = store.get_session(scenario.session.session_id)
    assert session is not None
    grandchild_remote = "remote-0003"
    transport.enqueue_messages(
        grandchild_remote, [_ack(grandchild_remote, CHILD_REMOTE, message_id="model-ack-2")]
    )
    second = await coordinator.rotate(session, child, scenario.pending)
    assert second.child.parent_conversation_id == first.child.conversation_id
    assert second.child.remote_conversation_id == grandchild_remote
    assert [c.status for c in store.list_conversations(session.session_id)] == [
        ConversationState.CLOSED,
        ConversationState.CLOSED,
        ConversationState.WAITING_MODEL_RESPONSE,
    ]
    final_session = store.get_session(session.session_id)
    assert final_session is not None
    assert final_session.rotations_count == 2
    assert final_session.consumed_cycles == scenario.session.consumed_cycles + 2
    assert transport.inits[2]["metadata"]["rotation_index"] == 2
    assert transport.inits[2]["metadata"]["parent_conversation_id"] == first.child.conversation_id


async def given_rotation_events_when_replayed_in_phase10_subscribers_then_audit_snapshot_and_metrics_consistent(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    bus: EventBus,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    audit = AuditLog(store, clock, ids)
    tracker = ExecutionTracker(store, clock)
    telemetry = TelemetryService(clock)
    audit.subscribe(bus)
    tracker.subscribe(bus)
    telemetry.subscribe(bus)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])

    result = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)

    sid = scenario.session.session_id
    verification = audit.verify(sid)
    assert verification.valid and verification.checked == 13
    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.session.rotations_count == 1
    assert snapshot.session.current_conversation_id == result.child.conversation_id
    assert snapshot.session.session_budget.consumed_cycles == scenario.session.consumed_cycles + 1
    assert snapshot.conversation is not None
    assert snapshot.conversation.conversation_id == result.child.conversation_id
    assert snapshot.conversation.status is ConversationState.WAITING_MODEL_RESPONSE
    assert snapshot.conversation.context_window_state is ContextWindowState.HEALTHY
    assert snapshot.conversation.parent_conversation_id == scenario.source.conversation_id
    assert [(c.conversation_id, c.status) for c in snapshot.conversations] == [
        (scenario.source.conversation_id, ConversationState.CLOSED),
        (result.child.conversation_id, ConversationState.WAITING_MODEL_RESPONSE),
    ]
    assert snapshot.model_interaction.last_outbound_message_type == "execution_result"
    assert snapshot.model_interaction.last_inbound_message_type == "context_resume_ack"
    assert snapshot.model_interaction.last_post_status == 202
    assert snapshot.model_interaction.last_protocol_validation_status == "valid"
    counters = telemetry.metrics()["counters"]
    assert counters["rotations_total"] == [{"labels": {"outcome": "completed"}, "value": 1}]
    assert sorted(
        (entry["labels"]["direction"], entry["value"]) for entry in counters["messages_total"]
    ) == [("inbound", 1), ("outbound", 2)]
    assert counters["messages_rejected_total"] == [{"labels": {}, "value": 0}]


# =============================================================================================
# RotationCoordinator — guards and failures, with the state left behind
# =============================================================================================


async def given_max_rotations_reached_when_saturated_then_rotation_failed(
    make_coordinator: CoordinatorFactory,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    coordinator = make_coordinator(_tiny(max_rotations_per_session=2))
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock, rotations_count=2)
    recorder.clear()
    with pytest.raises(RotationFailedError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    error = exc_info.value.error
    assert error.error_code == "ROTATION_LIMIT_REACHED"
    assert error.details == {"rotations_count": 2, "max_rotations_per_session": 2}
    # nothing changed: no ROTATING, no child, no init, only rotation.failed for observability
    source = store.get_conversation(scenario.source.conversation_id)
    assert source is not None and source.status is ConversationState.RUNNING_PLAN
    assert len(store.list_conversations(scenario.session.session_id)) == 1
    assert len(transport.inits) == 1 and transport.posted == []
    assert _events(recorder) == [("rotation.failed", scenario.source.conversation_id)]
    assert recorder.events[0].payload == {
        "source_conversation_id": scenario.source.conversation_id,
        "error_code": "ROTATION_LIMIT_REACHED",
        "rotations_count": 2,
        "max_rotations_per_session": 2,
    }


async def given_cycle_budget_exhausted_when_rotation_requested_then_budget_exceeded_before_any_change(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(
        lifecycle, store, transport, adapter, ids, clock, consumed_cycles=BUDGET.max_cycles
    )
    recorder.clear()
    with pytest.raises(BudgetExceededError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "BUDGET_MAX_CYCLES"
    assert exc_info.value.error.details == {
        "limit": "max_cycles",
        "limit_value": 20,
        "consumed": 20,
    }
    source = store.get_conversation(scenario.source.conversation_id)
    assert source is not None and source.status is ConversationState.RUNNING_PLAN
    assert recorder.events == []
    assert len(transport.inits) == 1


@pytest.mark.parametrize(
    "state", [ConversationState.ACTIVE, ConversationState.COMPLETED, ConversationState.FAILED]
)
async def given_source_not_rotatable_when_rotation_requested_then_invalid_transition(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
    state: ConversationState,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    cid = scenario.source.conversation_id
    if state is ConversationState.ACTIVE:
        # a fresh conversation that never left ACTIVE (06 points ouverts n°4)
        fresh = lifecycle.create_conversation(scenario.session.session_id)
        lifecycle.transition_conversation(fresh.conversation_id, ConversationState.ACTIVE)
        source = lifecycle.transition_context_window(
            fresh.conversation_id, ContextWindowState.SATURATED
        )
    else:
        lifecycle.transition_conversation(cid, ConversationState.WAITING_MODEL_RESPONSE)
        source = lifecycle.transition_conversation(cid, state)
    recorder.clear()
    with pytest.raises(InvalidTransitionError) as exc_info:
        await coordinator.rotate(scenario.session, source, scenario.pending)
    assert exc_info.value.current == state.value
    assert exc_info.value.target == "ROTATING"
    stored = store.get_conversation(source.conversation_id)
    assert stored is not None and stored.status is state
    assert recorder.events == []
    assert len(transport.inits) == 1


async def given_stale_source_record_when_rotated_then_store_state_is_used(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    """The caller's copy says HEALTHY / 0 bytes; the store says SATURATED: no spurious transition."""
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    stale = scenario.source.model_copy(
        update={"context_window_state": ContextWindowState.HEALTHY, "context_bytes": 0}
    )
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    recorder.clear()
    result = await coordinator.rotate(scenario.session, stale, scenario.pending)
    assert result.source.status is ConversationState.CLOSED
    assert recorder.events[0].event_type is EventType.CONVERSATION_STATE_CHANGED
    assert recorder.of_type(EventType.ROTATION_STARTED)[0].payload["context_bytes"] == 370_000


async def given_stale_session_record_when_limit_reached_in_store_then_rotation_failed(
    make_coordinator: CoordinatorFactory,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    coordinator = make_coordinator(_tiny(max_rotations_per_session=1))
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    lifecycle.update_session(scenario.session.session_id, rotations_count=1)
    assert scenario.session.rotations_count == 0  # the caller's copy is behind the store
    with pytest.raises(RotationFailedError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "ROTATION_LIMIT_REACHED"
    assert exc_info.value.error.details["rotations_count"] == 1


def given_pending_with_inbound_type_when_constructed_then_value_error(
    adapter: ProtocolAdapter,
) -> None:
    with pytest.raises(ValueError):
        PendingOutbound(
            message_type=MessageType.DISCOVERY_PLAN,
            original_message_id="msg-x",
            build=lambda child, message_id: adapter.build_user_request(
                child, message_id, GOAL, USER_MESSAGE, BUDGET
            ),
        )
    with pytest.raises(ValueError):
        PendingOutbound(
            message_type=MessageType.CONTEXT_RESUME_REQUEST,
            original_message_id="msg-x",
            build=lambda child, message_id: adapter.build_user_request(
                child, message_id, GOAL, USER_MESSAGE, BUDGET
            ),
        )


async def given_context_saturated_when_summary_exceeds_budget_then_rotation_fails_explicitly(
    make_coordinator: CoordinatorFactory,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    coordinator = make_coordinator(_tiny(summary_budget_bytes=1))
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    source_id = scenario.source.conversation_id
    recorder.clear()
    with pytest.raises(RotationFailedError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    error = exc_info.value.error
    assert error.error_type is ErrorType.ROTATION_FAILED
    assert error.error_code == "SUMMARY_EXCEEDS_BUDGET"
    assert error.details["step"] == 3
    assert error.details["budget_bytes"] == 1
    assert error.details["size_bytes"] > 1

    source = store.get_conversation(source_id)
    assert source is not None
    assert source.status is ConversationState.FAILED  # explicit, no silent loop (§2.6)
    assert len(store.list_conversations(scenario.session.session_id)) == 1  # no child
    assert len(transport.inits) == 1 and transport.posted == []  # nothing sent
    assert store.list_context_summaries(scenario.session.session_id) == []
    session = store.get_session(scenario.session.session_id)
    assert session is not None and session.rotations_count == 0
    assert session.consumed_cycles == scenario.session.consumed_cycles

    assert _events(recorder) == [
        ("conversation.state_changed", source_id),
        ("rotation.started", source_id),
        ("conversation.state_changed", source_id),
        ("rotation.failed", source_id),
    ]
    assert recorder.events[2].payload == {
        "from": "ROTATING",
        "to": "FAILED",
        "reason": "rotation_failed",
    }
    assert recorder.events[3].payload == {
        "source_conversation_id": source_id,
        "error_code": "SUMMARY_EXCEEDS_BUDGET",
        "summary_size_bytes": error.details["size_bytes"],
        "summary_budget_bytes": 1,
        "reduction_step": 3,
    }


async def given_remote_init_failing_when_rotation_requested_then_parent_stays_rotating_without_child(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_error(
        "init",
        TransportError(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True, operation="INIT"),
    )
    recorder.clear()
    with pytest.raises(TransportError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "HTTP_503"
    source = store.get_conversation(scenario.source.conversation_id)
    assert source is not None and source.status is ConversationState.ROTATING
    assert len(store.list_conversations(scenario.session.session_id)) == 1
    assert store.list_context_summaries(scenario.session.session_id) == []
    session = store.get_session(scenario.session.session_id)
    assert session is not None
    assert session.rotations_count == 0
    assert session.consumed_cycles == scenario.session.consumed_cycles
    assert session.current_conversation_id == scenario.source.conversation_id
    assert _events(recorder) == [
        ("conversation.state_changed", scenario.source.conversation_id),
        ("rotation.started", scenario.source.conversation_id),
    ]


async def given_resume_post_failing_when_rotation_requested_then_child_waits_and_parent_rotating(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_error(
        "post",
        TransportError(
            ErrorType.TIMEOUT_ERROR, "REQUEST_TIMEOUT", retryable=True, operation="POST"
        ),
    )
    recorder.clear()
    with pytest.raises(TransportError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "REQUEST_TIMEOUT"

    sid = scenario.session.session_id
    session = store.get_session(sid)
    assert session is not None
    conversations = store.list_conversations(sid)
    assert len(conversations) == 2
    source, child = conversations
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.SATURATED
    assert session.current_conversation_id == child.conversation_id
    assert session.rotations_count == 1
    assert session.consumed_cycles == scenario.session.consumed_cycles + 1
    cycles = store.list_cycles(child.conversation_id)
    assert len(cycles) == 1 and cycles[0].status is CycleState.RUNNING
    assert child.current_cycle_id == cycles[0].cycle_id
    messages = store.list_messages(child.conversation_id)
    assert len(messages) == 1
    assert messages[0].message_type is MessageType.CONTEXT_RESUME_REQUEST
    assert messages[0].post_confirmed is False and messages[0].posted_at is None
    assert child.context_bytes == ContextWindowMonitor.instructions_bytes(INSTRUCTIONS)
    assert transport.posted == []
    assert store.get_context_summary_for_target(child.conversation_id) is not None
    assert recorder.of_type(EventType.MESSAGE_OUTBOUND) == []
    assert recorder.of_type(EventType.ROTATION_COMPLETED) == []
    assert _events(recorder)[-1] == ("conversation.state_changed", child.conversation_id)


async def given_rotation_when_ack_missing_then_failure_policy_applies_in_child(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    before = clock.monotonic_ms()
    recorder.clear()
    with pytest.raises(TransportError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    error = exc_info.value.error
    assert error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.error_code == "MODEL_GET_TIMEOUT"
    assert error.retryable is True
    assert clock.monotonic_ms() == before + transport.reply_timeout_ms

    sid = scenario.session.session_id
    source, child = store.list_conversations(sid)
    # the orchestrator applies the §7 policy in the child: retry the GET, else fail both
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.SATURATED
    assert child.remote_conversation_id == CHILD_REMOTE
    assert child.get_cursor is None
    assert child.last_model_response_state == "awaiting"
    cycles = store.list_cycles(child.conversation_id)
    assert len(cycles) == 1 and cycles[0].status is CycleState.RUNNING
    assert child.current_cycle_id == cycles[0].cycle_id
    session = store.get_session(sid)
    assert session is not None and session.current_conversation_id == child.conversation_id
    messages = store.list_messages(child.conversation_id)
    assert [m.message_type for m in messages] == [MessageType.CONTEXT_RESUME_REQUEST]
    assert messages[0].post_confirmed is True
    assert len(transport.posted) == 1  # no retransmission
    assert transport.get_calls == [(CHILD_REMOTE, None)]
    assert transport.closed == []
    types = [e.event_type for e in recorder.events]
    assert types[-1] is EventType.MESSAGE_OUTBOUND
    assert EventType.ROTATION_COMPLETED not in types
    assert EventType.MESSAGE_RETRANSMITTED not in types


async def given_ack_not_acknowledged_when_rotation_awaits_ack_then_protocol_error_and_message_rejected(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    refused = _ack(acknowledged=False, message_id="model-nack")
    transport.enqueue_messages(CHILD_REMOTE, [refused])
    recorder.clear()
    with pytest.raises(ProtocolError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    error = exc_info.value.error
    assert error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error_code == "ACK_NOT_ACKNOWLEDGED"

    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.SATURATED
    assert child.protocol_error_count == 1
    assert child.get_cursor == "model-nack"  # a rejected message is never read again
    assert child.last_model_response_state == "received_invalid"
    resume = store.list_messages(child.conversation_id)[0]
    assert child.context_bytes == (
        ContextWindowMonitor.instructions_bytes(INSTRUCTIONS)
        + resume.size_bytes
        + size_bytes(refused)  # counted even though rejected (ADR-013)
    )
    assert [m.message_type for m in store.list_messages(child.conversation_id)] == [
        MessageType.CONTEXT_RESUME_REQUEST
    ]
    rejected = _only(recorder, EventType.MESSAGE_REJECTED)
    assert rejected.conversation_id == child.conversation_id
    assert rejected.cycle_id == child.current_cycle_id
    assert rejected.payload == {
        "message_type": "context_resume_ack",
        "message_id": "model-nack",
        "get_status": 200,
        "validation_status": "invalid",
        "error_code": "ACK_NOT_ACKNOWLEDGED",
        "size_bytes": size_bytes(refused),
    }
    assert recorder.events[-1] is rejected
    assert recorder.of_type(EventType.MESSAGE_INBOUND) == []
    assert recorder.of_type(EventType.ROTATION_COMPLETED) == []
    assert len(transport.posted) == 1


async def given_unexpected_message_instead_of_ack_when_rotation_awaits_ack_then_rejected_with_type(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    plan = {
        "type": "discovery_plan",
        "conversation_id": CHILD_REMOTE,
        "message_id": "model-plan",
        "content": {
            "plan_id": "plan-9",
            "objective": "x",
            "execution_policy": "sequential",
            "tasks": [{"task_id": "t9", "cmd": "true"}],
        },
    }
    transport.enqueue_messages(CHILD_REMOTE, [plan])
    recorder.clear()
    with pytest.raises(ProtocolError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "UNEXPECTED_MESSAGE_TYPE"
    rejected = _only(recorder, EventType.MESSAGE_REJECTED)
    assert rejected.payload["message_type"] == "discovery_plan"
    assert rejected.payload["message_id"] == "model-plan"
    assert rejected.payload["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    assert rejected.payload["validation_status"] == "invalid"


async def given_ack_for_wrong_original_when_rotation_awaits_ack_then_rejected(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack(original_remote="remote-9999")])
    recorder.clear()
    with pytest.raises(ProtocolError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "ACK_WRONG_ORIGINAL"
    assert _only(recorder, EventType.MESSAGE_REJECTED).payload["error_code"] == "ACK_WRONG_ORIGINAL"


async def given_malformed_message_without_id_when_rotation_awaits_ack_then_rejected_with_nulls(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [{"garbage": True}])
    recorder.clear()
    with pytest.raises(ProtocolError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "SCHEMA_INVALID"
    rejected = _only(recorder, EventType.MESSAGE_REJECTED)
    assert rejected.payload["message_type"] is None
    assert rejected.payload["message_id"] is None
    assert rejected.payload["error_code"] == "SCHEMA_INVALID"
    assert rejected.payload["size_bytes"] == size_bytes({"garbage": True})
    _, child = store.list_conversations(scenario.session.session_id)
    assert child.protocol_error_count == 1


async def given_retransmission_post_failing_when_rotation_completed_then_child_keeps_unconfirmed_copy(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    bus: EventBus,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])

    def fail_next_post(event: Event) -> None:
        transport.enqueue_error(
            "post",
            TransportError(ErrorType.NETWORK_ERROR, "HTTP_502", retryable=True, operation="POST"),
        )

    # armed once the rotation is complete: the very next POST is the retransmission
    bus.subscribe(fail_next_post, name="saboteur", event_types=[EventType.ROTATION_COMPLETED])
    recorder.clear()
    with pytest.raises(TransportError) as exc_info:
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert exc_info.value.error.error_code == "HTTP_502"

    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.CLOSED
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.HEALTHY
    messages = store.list_messages(child.conversation_id)
    assert [m.message_type for m in messages] == [
        MessageType.CONTEXT_RESUME_REQUEST,
        MessageType.CONTEXT_RESUME_ACK,
        MessageType.EXECUTION_RESULT,
    ]
    copy_of_m = messages[2]
    assert copy_of_m.post_confirmed is False
    assert copy_of_m.retransmission_of == scenario.original_message_id
    assert child.last_outbound_message_id == copy_of_m.message_id
    assert child.current_cycle_id == "cyc-orig"
    assert len(transport.posted) == 1
    assert recorder.of_type(EventType.MESSAGE_RETRANSMITTED) == []
    assert len(recorder.of_type(EventType.ROTATION_COMPLETED)) == 1


async def given_build_returning_another_type_when_retransmitting_then_value_error_after_completion(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    wrong = PendingOutbound(
        message_type=MessageType.EXECUTION_RESULT,
        original_message_id=scenario.original_message_id,
        build=lambda child, message_id: adapter.build_user_request(
            child, message_id, GOAL, USER_MESSAGE, BUDGET
        ),
    )
    with pytest.raises(ValueError):
        await coordinator.rotate(scenario.session, scenario.source, wrong)
    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.CLOSED
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert len(store.list_messages(child.conversation_id)) == 2  # nothing persisted for M'
    assert len(transport.posted) == 1


async def given_remote_close_failing_when_rotation_completes_then_best_effort_ignored(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    transport.enqueue_error(
        "close",
        TransportError(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True, operation="CLOSE"),
    )
    result = await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    assert result.source.status is ConversationState.CLOSED
    assert transport.closed == []  # the close raised before being recorded
    assert len(transport.posted) == 2  # the retransmission still happened


async def given_rotation_completed_when_remote_parent_closed_then_after_the_retransmission(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    """The close is outside the critical path: M' is already POSTed when it happens."""
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    transport.hang_next("close")
    task = asyncio.create_task(
        coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    )
    await transport.wait_until_hanging()
    assert [payload["type"] for _, payload in transport.posted] == [
        "context_resume_request",
        "execution_result",
    ]
    _, child = store.list_conversations(scenario.session.session_id)
    assert store.list_messages(child.conversation_id)[2].post_confirmed is True
    transport.abandon()
    with pytest.raises(TransportError) as exc_info:
        await task
    assert exc_info.value.error.error_code == "ABANDONED"


async def given_remote_close_abandoned_when_rotation_completes_then_interruption_not_swallowed(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    """Only a plain transport failure of the close is ignored; ``abandon()`` must surface."""
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])
    transport.hang_next("close")
    task = asyncio.create_task(
        coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    )
    await transport.wait_until_hanging()
    transport.abandon()
    with pytest.raises(TransportError) as exc_info:
        await task
    assert exc_info.value.error.error_type is ErrorType.INTERRUPTED
    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.CLOSED
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert len(transport.posted) == 2


# =============================================================================================
# RotationCoordinator — persist before publish (ADR-015)
# =============================================================================================


async def given_store_failing_on_summary_write_when_child_created_then_no_cycle_event_and_child_new(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    bus: EventBus,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])

    def sabotage(event: Event) -> None:
        store.fail_next_write = True

    bus.subscribe(sabotage, name="saboteur", event_types=[EventType.CONVERSATION_CREATED])
    recorder.clear()
    with pytest.raises(PersistenceError):
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.NEW
    assert store.list_context_summaries(scenario.session.session_id) == []
    assert _events(recorder) == [
        ("conversation.state_changed", source.conversation_id),
        ("rotation.started", source.conversation_id),
        ("conversation.created", child.conversation_id),
    ]
    assert transport.posted == []


async def given_store_failing_on_cycle_write_when_child_active_then_no_cycle_started_and_counters_unchanged(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    bus: EventBus,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])

    def sabotage(event: Event) -> None:
        if event.payload.get("to") == "ACTIVE":
            store.fail_next_write = True

    bus.subscribe(sabotage, name="saboteur", event_types=[EventType.CONVERSATION_STATE_CHANGED])
    recorder.clear()
    with pytest.raises(PersistenceError):
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    sid = scenario.session.session_id
    source, child = store.list_conversations(sid)
    session = store.get_session(sid)
    assert session is not None
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.ACTIVE
    assert child.current_cycle_id is None
    # the whole group (message, cycle, counters) was rolled back together
    assert session.rotations_count == 0
    assert session.consumed_cycles == scenario.session.consumed_cycles
    assert store.list_cycles(child.conversation_id) == []
    assert store.list_messages(child.conversation_id) == []
    assert recorder.of_type(EventType.CYCLE_STARTED) == []
    assert transport.posted == []
    assert _events(recorder)[-1] == ("conversation.state_changed", child.conversation_id)


async def given_store_failing_after_post_when_confirming_then_no_outbound_event_and_post_unconfirmed(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    bus: EventBus,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.enqueue_messages(CHILD_REMOTE, [_ack()])

    def sabotage(event: Event) -> None:
        if event.payload.get("to") == "WAITING_MODEL_RESPONSE":
            store.fail_next_write = True  # the next write is the post confirmation

    bus.subscribe(sabotage, name="saboteur", event_types=[EventType.CONVERSATION_STATE_CHANGED])
    recorder.clear()
    with pytest.raises(PersistenceError):
        await coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    _, child = store.list_conversations(scenario.session.session_id)
    assert len(transport.posted) == 1  # the POST happened...
    resume = store.list_messages(child.conversation_id)[0]
    assert resume.post_confirmed is False  # ...but is not confirmed (ADR-016 "POST sans GET")
    assert child.context_bytes == ContextWindowMonitor.instructions_bytes(INSTRUCTIONS)
    assert recorder.of_type(EventType.MESSAGE_OUTBOUND) == []
    assert transport.get_calls == []


# =============================================================================================
# RotationCoordinator — cancellation and abandon (interruption is phase 6/9)
# =============================================================================================


async def given_rotation_awaiting_ack_when_task_cancelled_then_cancelled_error_and_state_consistent(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
    recorder: RecordingSubscriber,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.hang_next("get")
    recorder.clear()
    task = asyncio.create_task(
        coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    )
    await transport.wait_until_hanging()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE
    assert child.context_window_state is ContextWindowState.SATURATED
    messages = store.list_messages(child.conversation_id)
    assert len(messages) == 1 and messages[0].post_confirmed is True
    assert recorder.events[-1].event_type is EventType.MESSAGE_OUTBOUND
    assert transport.in_flight == 0


async def given_rotation_awaiting_ack_when_transport_abandoned_then_interrupted_error_propagates(
    coordinator: RotationCoordinator,
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    transport: FakeTransportGateway,
    adapter: ProtocolAdapter,
    ids: SequentialIdGenerator,
    clock: FakeClock,
) -> None:
    scenario = await _scenario(lifecycle, store, transport, adapter, ids, clock)
    transport.hang_next("get")
    task = asyncio.create_task(
        coordinator.rotate(scenario.session, scenario.source, scenario.pending)
    )
    await transport.wait_until_hanging()
    transport.abandon()
    with pytest.raises(TransportError) as exc_info:
        await task
    assert exc_info.value.error.error_type is ErrorType.INTERRUPTED
    assert exc_info.value.error.error_code == "ABANDONED"
    source, child = store.list_conversations(scenario.session.session_id)
    assert source.status is ConversationState.ROTATING
    assert child.status is ConversationState.WAITING_MODEL_RESPONSE


# =============================================================================================
# Value objects and exports
# =============================================================================================


def given_pending_outbound_when_constructed_then_frozen_value_object(
    adapter: ProtocolAdapter,
) -> None:
    def build(child: ConversationRecord, message_id: str) -> OutboundMessage:
        return adapter.build_user_request(child, message_id, GOAL, USER_MESSAGE, BUDGET)

    pending = PendingOutbound(
        message_type=MessageType.USER_REQUEST, original_message_id="msg-1", build=build
    )
    assert pending.cycle_id is None
    with pytest.raises((AttributeError, TypeError)):
        pending.message_type = MessageType.EXECUTION_RESULT  # type: ignore[misc]


def given_context_package_when_imported_then_phase8_components_exported() -> None:
    from agentic_local_app import context

    for name in (
        "ContextReducer",
        "ContextThresholds",
        "ContextWindowMonitor",
        "PendingOutbound",
        "RotationCoordinator",
        "RotationResult",
        "SummaryDraft",
    ):
        assert name in context.__all__
        assert hasattr(context, name)


def given_summary_payload_when_state_summary_mutated_afterwards_then_summary_unchanged(
    reducer: ContextReducer,
    store: InMemoryConversationStore,
    lifecycle: ConversationLifecycleManager,
) -> None:
    session, source = _rich_store(store, lifecycle)
    record = _build(reducer, session, source)
    snapshot = copy.deepcopy(record.summary_payload)
    plan = store.get_plan(session.session_id, "plan-1")
    assert plan is not None and plan.state_summary is not None
    plan.state_summary["findings"].append("mutated")
    assert record.summary_payload == snapshot
