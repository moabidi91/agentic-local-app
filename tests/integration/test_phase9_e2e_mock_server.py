"""Phase 9 — end to end against the mock model server (ADR-004 "serveur mock", §18.2 phase 9).

The real ``HttpTransportGateway`` talks HTTP to the FastAPI mock server through
``httpx.ASGITransport``: no port, no socket, no network. Commands are still executed by the
``FakeCommandExecutor`` (no process). Time is the fake clock: the gateway's polling ``sleep`` and the
orchestrator's backoff both advance it instead of waiting.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from agentic_local_app.config import (
    AppConfig,
    AppSection,
    ExecutionSection,
    ScratchSection,
    TransportSection,
)
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import Application, build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.testing.mock_model_server import (
    Fault,
    MockEngine,
    Scenario,
    Step,
    create_mock_app,
    default_analysis_scenario,
    default_java_debug_scenario,
)
from agentic_local_app.transport.gateway import HttpTransportGateway
from integration.phase9_rig import (
    BOUND_S,
    FINAL_DIAGNOSIS,
    GOAL,
    USER_MESSAGE,
    advancing_sleep,
    final_answer,
)

pytestmark = pytest.mark.phase9

BASE = "http://mock/v1/conversations"


def _config(tmp_dir: str, *, close: bool = True) -> AppConfig:
    return AppConfig(
        app=AppSection(data_dir=tmp_dir),
        execution=ExecutionSection(interrupt_drain_timeout_ms=500, cancel_drain_timeout_ms=500),
        # ADR-026: the working spaces of the run live under the temporary directory of the test,
        # never under the ``./data/scratch`` of the defaults
        scratch=ScratchSection(
            root=f"{tmp_dir}/scratch", archive_root=f"{tmp_dir}/scratch-archive"
        ),
        transport=TransportSection(
            init_url=BASE,
            post_url=BASE + "/{conversation_id}/messages",
            get_url=BASE + "/{conversation_id}/messages?after={after}",
            close_url=BASE + "/{conversation_id}/close" if close else "",
            user_id="tester",
            request_timeout_ms=1_000,
            poll_interval_ms=200,
            reply_timeout_ms=2_000,
            gzip=True,
        ),
    )


async def _application(
    scenario: Scenario, tmp_dir: str, *, close: bool = True
) -> tuple[Application, MockEngine, FakeCommandExecutor, RecordingSubscriber]:
    clock = FakeClock()
    config = _config(tmp_dir, close=close)
    mock = create_mock_app(scenario, now_ms=clock.monotonic_ms)
    gateway = HttpTransportGateway(
        config.transport,
        clock,
        transport=httpx.ASGITransport(app=mock),
        sleep=advancing_sleep(clock),
    )
    executor = FakeCommandExecutor(clock)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="e2e-recorder")
    app = build_application(
        config,
        store=InMemoryConversationStore(),
        transport=gateway,
        executor=executor,
        clock=clock,
        ids=SequentialIdGenerator(),
        bus=bus,
        run_recovery=True,
        sleep=advancing_sleep(clock),
    )
    engine: MockEngine = mock.state.engine
    return app, engine, executor, recorder


async def given_mock_model_server_when_java_scenario_runs_then_final_answer_received_and_audit_valid(
    tmp_path: Any,
) -> None:
    app, engine, executor, recorder = await _application(
        default_java_debug_scenario(), str(tmp_path)
    )
    executor.script(
        cmd="mvn clean install 2>&1 | tail -80", stderr=b"invalid target release: 21", exit_code=1
    )
    try:
        session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE)
        ended = await app.manager.wait(session.session_id, timeout_ms=int(BOUND_S * 1000))
        sid = session.session_id

        assert ended.status is SessionState.COMPLETED
        assert ended.final_answer is not None
        assert ended.final_answer["diagnosis"] == FINAL_DIAGNOSIS
        assert ended.final_answer == final_answer()["content"]
        assert ended.consumed_cycles == 3 and ended.consumed_plans == 2

        # ---- what the mock model saw, over real HTTP semantics (gzip bodies, idempotent POST) -------
        assert [body["type"] for _, body in engine.received] == [
            "user_request",
            "execution_result",
            "execution_result",
        ]
        assert {cid for cid, _ in engine.received} == {"mock-conv-0001"}
        assert engine.inits[0]["user_id"] == "tester"
        assert engine.inits[0]["metadata"] == {"session_id": sid, "parent_conversation_id": None}
        assert engine.inits[0]["instructions"] == app.instructions
        assert engine.received[0][1]["content"]["goal"] == GOAL
        assert engine.received[1][1]["content"]["plan_id"] == "plan-0"
        assert engine.received[1][1]["content"]["status"] == "stopped_on_failure"
        assert engine.received[2][1]["content"]["plan_id"] == "plan-1"
        assert engine.closed == []  # reusable conversation

        # ---- local state -----------------------------------------------------------------------------
        conversation = app.store.get_conversation("conv-0001")
        assert conversation is not None
        assert conversation.remote_conversation_id == "mock-conv-0001"
        assert conversation.status is ConversationState.WAITING_USER
        assert conversation.get_cursor == "mock-msg-0003"
        assert [c.status for c in app.store.list_cycles("conv-0001")] == [CycleState.COMPLETED] * 3
        assert app.store.get_plan(sid, "plan-0").status is PlanState.STOPPED_ON_FAILURE  # type: ignore[union-attr]
        assert app.store.get_plan(sid, "plan-1").status is PlanState.COMPLETED  # type: ignore[union-attr]
        assert {t.task_id: t.status for t in app.store.list_tasks(sid)} == {
            "t1": TaskState.COMPLETED,
            "t2": TaskState.COMPLETED,
            "t3": TaskState.COMPLETED,
            "t4": TaskState.COMPLETED,
            "t5": TaskState.FAILED,
            "t6": TaskState.COMPLETED,
            "t7": TaskState.COMPLETED,
        }
        assert app.store.list_failures(sid) == []
        assert app.audit.verify(sid).valid is True
        snapshot = app.manager.snapshot(sid)
        assert snapshot.model_interaction.last_post_status == 202
        assert snapshot.model_interaction.last_get_status == 200
        assert snapshot.session.status is SessionState.COMPLETED
        assert app.telemetry.metrics()["counters"]["messages_total"]  # something was counted
        assert app.manager.recovery_report is not None
    finally:
        await app.aclose()


async def given_mock_model_server_when_analysis_scenario_runs_then_user_response_ends_the_session(
    tmp_path: Any,
) -> None:
    app, engine, executor, recorder = await _application(default_analysis_scenario(), str(tmp_path))
    try:
        session = await app.manager.start_session(
            goal="Explain a Java build error",
            user_message="What does 'invalid target release: 21' mean? Do not run anything.",
        )
        ended = await app.manager.wait(session.session_id, timeout_ms=int(BOUND_S * 1000))
        sid = session.session_id

        assert ended.status is SessionState.COMPLETED
        assert ended.final_answer is None
        assert ended.consumed_cycles == 1 and ended.consumed_plans == 0
        assert [body["type"] for _, body in engine.received] == ["user_request"]
        assert executor.calls == []
        reply = app.manager.last_reply(sid)
        assert reply is not None and reply["type"] == "user_response"
        assert reply["message_id"] == "mock-msg-0001"
        assert reply["content"]["format"] == "markdown"
        assert "invalid target release" in reply["content"]["body"]
        assert [r["message_id"] for r in app.manager.user_responses(sid)] == ["mock-msg-0001"]
        conversation = app.store.get_conversation("conv-0001")
        assert conversation is not None
        assert conversation.status is ConversationState.WAITING_USER
        assert conversation.get_cursor == "mock-msg-0001"
        assert engine.closed == []
        assert [e.event_type.value for e in recorder.events].count("user_response.received") == 1
        assert app.audit.verify(sid).valid is True
    finally:
        await app.aclose()


async def given_mock_server_with_503_then_delayed_reply_when_loop_runs_then_retry_and_polling_succeed(
    tmp_path: Any,
) -> None:
    base = default_java_debug_scenario()
    scenario = Scenario(
        steps=[
            Step(
                on="user_request",
                respond=base.steps[0].respond,
                fault=Fault(status=503, times=1, on_operation="post"),
            ),
            Step(on="execution_result", respond=base.steps[1].respond, delay_ms=450),
            Step(on="execution_result", respond=base.steps[2].respond),
        ]
    )
    app, engine, executor, recorder = await _application(scenario, str(tmp_path))
    try:
        session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE)
        ended = await app.manager.wait(session.session_id, timeout_ms=int(BOUND_S * 1000))
        sid = session.session_id

        assert ended.status is SessionState.COMPLETED
        failures = app.store.list_failures(sid)
        assert [(f.error_type.value, f.error_code) for f in failures] == [
            ("NETWORK_ERROR", "HTTP_503")
        ]
        decisions = app.store.list_retry_decisions(sid)
        assert [(d.operation, d.decision, d.delay_ms) for d in decisions] == [
            ("POST", "retry", 500)
        ]
        assert [body["type"] for _, body in engine.received] == [
            "user_request",
            "execution_result",
            "execution_result",
        ]
        assert app.store.list_cycles("conv-0001")[0].retry_count == 1
        assert ended.final_answer == final_answer()["content"]
    finally:
        await app.aclose()


async def given_mock_server_when_auto_close_session_completes_then_remote_conversation_closed(
    tmp_path: Any,
) -> None:
    app, engine, executor, recorder = await _application(
        default_java_debug_scenario(), str(tmp_path)
    )
    try:
        session = await app.manager.start_session(
            goal=GOAL, user_message=USER_MESSAGE, auto_close=True
        )
        ended = await app.manager.wait(session.session_id, timeout_ms=int(BOUND_S * 1000))

        assert ended.status is SessionState.COMPLETED
        assert engine.closed == ["mock-conv-0001"]
        conversation = app.store.get_conversation("conv-0001")
        assert conversation is not None and conversation.status is ConversationState.CLOSED
    finally:
        await app.aclose()


async def given_mock_server_silent_when_reply_timeout_elapses_then_bounded_retries_then_failed(
    tmp_path: Any,
) -> None:
    scenario = Scenario(steps=[Step(on="user_request", respond=[])])  # the model never answers
    app, engine, executor, recorder = await _application(scenario, str(tmp_path))
    try:
        session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE)
        ended = await app.manager.wait(session.session_id, timeout_ms=int(BOUND_S * 1000))
        sid = session.session_id

        assert ended.status is SessionState.FAILED
        failures = app.store.list_failures(sid)
        assert [f.error_code for f in failures] == ["MODEL_GET_TIMEOUT"] * 4
        assert [d.decision for d in app.store.list_retry_decisions(sid)] == [
            "retry",
            "retry",
            "retry",
            "fail",
        ]
        assert engine.closed == ["mock-conv-0001"]  # best-effort close on failure
    finally:
        await app.aclose()
