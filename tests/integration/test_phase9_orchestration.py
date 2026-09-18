"""Phase 9 — protocol orchestration (spec §2.2, §3.1, §3.2, §7, §8.4, §10, §11, §14, §15, §18.2
phase 9 ; ADR-004, ADR-006, ADR-007, ADR-009, ADR-010, ADR-012, ADR-013, ADR-014, ADR-015,
ADR-016, ADR-017, ADR-019 ; acceptance criteria 1–9, 12, 14, 15, 16).

Integration tests of ``ProtocolOrchestrator`` and ``ConversationManager`` wired by
``build_application`` on the doubles of §18.3 (``phase9_rig``): the full loop of §2.2, the
finalisation policy of §11, the follow-up message, the interruption mid-plan, the session budget,
the context rotation mid-session (projection, context error, unusable reply in WARNING), the
protocol errors, the transport failure policy (retry, no retry, exhaustion, circuit breaker) and
the manager façade (reads, wait, shutdown, unexpected exceptions).

Sections: nominal loop · manager façade · interruption · budget · rotation · protocol errors ·
transport failures · follow-up and shutdown · hygiene.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.errors import ErrorType, TransportError
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.models import SessionBudget
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
    MessageDirection,
    MessageType,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
)
from agentic_local_app.execution.payload_guard import decode_output
from agentic_local_app.observability.execution_tracker import RuntimeSnapshot
from agentic_local_app.orchestration import (
    Application,
    ConversationManager,
    ProtocolOrchestrator,
    RecoveryCoordinator,
    build_application,
)
from agentic_local_app.protocol.adapter import render_instructions
from integration.phase9_rig import (
    CMD_BUILD,
    CMD_GREP,
    CMD_JAVA,
    CMD_JAVA_HOME,
    CMD_MVN,
    CMD_POM,
    CMD_UNAME,
    ERR_BUILD,
    FINAL_DIAGNOSIS,
    GOAL,
    OUT_GREP,
    OUT_JAVA,
    OUT_JAVA_HOME,
    OUT_MVN,
    OUT_POM,
    OUT_UNAME,
    REMOTE_1,
    REMOTE_2,
    USER_MESSAGE,
    Rig,
    chunk_task,
    cmd_task,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    make_rig,
    resume_ack,
    settle,
)

pytestmark = pytest.mark.phase9

TINY_INSTRUCTIONS = "PROTOCOL v1.1 (test)"


def _non_execution_kinds(rig: Rig) -> list[tuple[str, str | None]]:
    """The event stream without the plan / task events owned by the PlanRunner (phase 5)."""
    return [
        kind
        for kind in rig.event_kinds()
        if kind[0]
        not in {
            EventType.PLAN_STATE_CHANGED.value,
            EventType.TASK_STATE_CHANGED.value,
            EventType.TASK_OUTPUT.value,
        }
    ]


def _transport_error(
    error_type: ErrorType,
    code: str,
    *,
    operation: str,
    retryable: bool,
    http_status: int | None = None,
) -> TransportError:
    return TransportError(
        error_type,
        code,
        retryable=retryable,
        operation=operation,
        http_status=http_status,
        url=f"fake://{operation.lower()}",
    )


@pytest.fixture
def rig() -> Rig:
    return make_rig()


# ================================================================================================
# 1. the nominal loop: user_request -> discovery_plan -> execution_result -> execution_plan ->
#    execution_result -> final_answer (§2.2, §11, §18.2 phase 9, criteria 1, 2, 8, 9, 12, 14)
# ================================================================================================
async def given_scripted_java_scenario_when_session_runs_then_three_canonical_messages_posted_in_order(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert [remote for remote, _ in rig.transport.posted] == [REMOTE_1, REMOTE_1, REMOTE_1]
    assert rig.posted(0) == {
        "type": "user_request",
        "conversation_id": REMOTE_1,
        "message_id": "msg-0001",
        "content": {
            "goal": GOAL,
            "user_message": USER_MESSAGE,
            "session_budget": {"max_cycles": 20, "max_plans": 10, "max_total_duration_ms": 300_000},
        },
    }
    first_result = rig.posted(1)
    assert first_result["conversation_id"] == REMOTE_1
    assert first_result["message_id"] == "msg-0002"
    content = first_result["content"]
    assert content["plan_id"] == "plan-0"
    assert content["status"] == "stopped_on_failure"
    assert content["stop_reason"] == "critical_task_failed:t5"
    assert [r["task_id"] for r in content["results"]] == ["t1", "t2", "t3", "t4", "t5"]
    assert content["skipped_tasks"] == []
    assert content["cancelled_tasks"] == []
    assert content["interrupted_tasks"] == []
    t1, t2, t3, t4, t5 = content["results"]
    assert t1 == {
        "task_id": "t1",
        "status": "completed",
        "exit_code": 0,
        "stdout": decode_output(OUT_UNAME),
        "stderr": "",
        "truncated": False,
        "original_size_bytes": len(OUT_UNAME),
        "stdout_total": len(OUT_UNAME),
        "stderr_total": 0,
        "stdout_range": [0, len(OUT_UNAME)],
        "stderr_range": [0, 0],
        "max_output_bytes_applied": 2048,
        "timed_out": False,
        "timeout_ms_applied": 60_000,
        "duration_ms": 0,
    }
    assert (t2["stderr"], t2["exit_code"]) == (decode_output(OUT_JAVA), 0)
    assert (t3["stdout"], t3["exit_code"]) == (decode_output(OUT_MVN), 0)
    assert (t4["stdout"], t4["max_output_bytes_applied"]) == (decode_output(OUT_POM), 16384)
    assert (t5["status"], t5["exit_code"], t5["stderr"]) == ("failed", 1, decode_output(ERR_BUILD))
    second_result = rig.posted(2)
    assert second_result["message_id"] == "msg-0003"
    assert second_result["content"]["plan_id"] == "plan-1"
    assert second_result["content"]["status"] == "completed"
    assert "stop_reason" not in second_result["content"]  # null fields are omitted (§12.5)
    t6, t7 = second_result["content"]["results"]
    assert (t6["stdout"], t7["stdout"]) == (decode_output(OUT_JAVA_HOME), decode_output(OUT_GREP))
    # the commands were run exactly as received (§1, §2.3), in plan order
    assert [call.cmd for call in rig.executor.calls] == [
        CMD_UNAME,
        CMD_JAVA,
        CMD_MVN,
        CMD_POM,
        CMD_BUILD,
        CMD_JAVA_HOME,
        CMD_GREP,
    ]


async def given_scripted_java_scenario_when_session_completes_then_records_states_and_counters_consistent(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run()
    sid = session.session_id

    # ---- session (ADR-012 counters, §11 final answer) ---------------------------------------
    stored = rig.session(sid)
    assert stored == session
    assert stored.status is SessionState.COMPLETED
    assert stored.consumed_cycles == 3
    assert stored.consumed_plans == 2
    assert stored.rotations_count == 0
    assert stored.final_answer == final_answer()["content"]
    assert stored.final_answer is not None and stored.final_answer["diagnosis"] == FINAL_DIAGNOSIS
    assert stored.started_at == rig.clock.now() and stored.ended_at == rig.clock.now()
    assert stored.last_failure_id is None
    assert stored.current_conversation_id == "conv-0001"

    # ---- conversation (§5.1, ADR-013 context bytes) -------------------------------------------
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.remote_conversation_id == REMOTE_1
    assert conversation.final_answer_received is True
    assert conversation.current_plan_id == "plan-1"
    assert conversation.last_completed_plan_id == "plan-1"
    assert conversation.current_cycle_id == "cyc-0003"
    assert conversation.last_outbound_message_id == "msg-0003"
    assert conversation.last_inbound_message_id == "model-msg-0003"
    assert conversation.get_cursor == "model-msg-0003"
    assert conversation.context_window_state is ContextWindowState.HEALTHY
    assert conversation.protocol_error_count == 0
    assert conversation.last_model_response_state == "received_valid"
    assert conversation.closure_reason is None
    messages = rig.store.list_messages("conv-0001")
    instructions = len(render_instructions(rig.config).encode("utf-8"))
    assert rig.transport.inits[0]["instructions"] == render_instructions(rig.config)
    assert rig.transport.inits[0]["metadata"] == {
        "session_id": sid,
        "parent_conversation_id": None,
    }
    assert conversation.context_bytes == instructions + sum(m.size_bytes for m in messages)

    # ---- messages: three outbound confirmed, three inbound valid, alternating -------------------
    assert [(m.direction, m.message_type) for m in messages] == [
        (MessageDirection.OUTBOUND, MessageType.USER_REQUEST),
        (MessageDirection.INBOUND, MessageType.DISCOVERY_PLAN),
        (MessageDirection.OUTBOUND, MessageType.EXECUTION_RESULT),
        (MessageDirection.INBOUND, MessageType.EXECUTION_PLAN),
        (MessageDirection.OUTBOUND, MessageType.EXECUTION_RESULT),
        (MessageDirection.INBOUND, MessageType.FINAL_ANSWER),
    ]
    assert [m.message_id for m in messages] == [
        "msg-0001",
        "model-msg-0001",
        "msg-0002",
        "model-msg-0002",
        "msg-0003",
        "model-msg-0003",
    ]
    assert [m.cycle_id for m in messages] == [
        "cyc-0001",
        "cyc-0001",
        "cyc-0002",
        "cyc-0002",
        "cyc-0003",
        "cyc-0003",
    ]
    for message in messages:
        if message.direction is MessageDirection.OUTBOUND:
            assert message.post_confirmed is True and message.posted_at is not None
            assert message.validation_status is None and message.retransmission_of is None
            assert message.payload == rig.posted(
                ["msg-0001", "msg-0002", "msg-0003"].index(message.message_id)
            )
        else:
            assert message.validation_status == "valid" and message.received_at is not None
        assert message.size_bytes == size_bytes(message.payload)

    # ---- cycles (ADR-007): one per outbound message, all COMPLETED ------------------------------
    cycles = rig.cycles("conv-0001")
    assert [c.cycle_id for c in cycles] == ["cyc-0001", "cyc-0002", "cyc-0003"]
    assert [c.cycle_type for c in cycles] == [
        CycleType.DISCOVERY,
        CycleType.EXECUTION,
        CycleType.EXECUTION,
    ]
    assert all(c.status is CycleState.COMPLETED for c in cycles)
    assert [c.outbound_message_id for c in cycles] == ["msg-0001", "msg-0002", "msg-0003"]
    assert [c.inbound_message_id for c in cycles] == [
        "model-msg-0001",
        "model-msg-0002",
        "model-msg-0003",
    ]
    assert [c.plan_id for c in cycles] == ["plan-0", "plan-1", None]
    assert all(c.retry_count == 0 and c.ended_at is not None for c in cycles)

    # ---- plans and tasks (§5.2, §5.3, ADR-009) ----------------------------------------------------
    plan0, plan1 = rig.plan(sid, "plan-0"), rig.plan(sid, "plan-1")
    assert (plan0.status, plan0.stop_reason, plan0.cycle_id) == (
        PlanState.STOPPED_ON_FAILURE,
        "critical_task_failed:t5",
        "cyc-0001",
    )
    assert (plan0.plan_type, plan0.conversation_id) == (PlanType.DISCOVERY_PLAN, "conv-0001")
    assert (plan1.status, plan1.stop_reason, plan1.cycle_id) == (
        PlanState.COMPLETED,
        None,
        "cyc-0002",
    )
    assert (plan1.plan_type, plan1.max_parallel_workers) == (PlanType.EXECUTION_PLAN, 2)
    assert {t.task_id: t.status for t in rig.tasks(sid)} == {
        "t1": TaskState.COMPLETED,
        "t2": TaskState.COMPLETED,
        "t3": TaskState.COMPLETED,
        "t4": TaskState.COMPLETED,
        "t5": TaskState.FAILED,
        "t6": TaskState.COMPLETED,
        "t7": TaskState.COMPLETED,
    }
    assert rig.task(sid, "t5").exit_code == 1
    assert rig.store.list_failures(sid) == []
    assert rig.store.list_retry_decisions(sid) == []
    assert rig.transport.closed == []  # reusable conversation: nothing closed
    assert len(rig.transport.inits) == 1


async def given_scripted_java_scenario_when_session_completes_then_events_audited_in_order_and_chain_valid(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run()
    sid = session.session_id

    assert _non_execution_kinds(rig) == [
        ("session.created", None),
        ("session.state_changed", "RUNNING"),
        ("conversation.created", None),
        ("conversation.state_changed", "ACTIVE"),
        ("cycle.started", None),
        ("budget.updated", None),
        ("conversation.state_changed", "WAITING_MODEL_RESPONSE"),
        ("message.outbound", None),
        ("message.inbound", None),
        ("plan.received", None),
        ("budget.updated", None),
        ("conversation.state_changed", "RUNNING_PLAN"),
        ("cycle.ended", None),
        ("cycle.started", None),
        ("budget.updated", None),
        ("conversation.state_changed", "WAITING_MODEL_RESPONSE"),
        ("message.outbound", None),
        ("message.inbound", None),
        ("plan.received", None),
        ("budget.updated", None),
        ("conversation.state_changed", "RUNNING_PLAN"),
        ("cycle.ended", None),
        ("cycle.started", None),
        ("budget.updated", None),
        ("conversation.state_changed", "WAITING_MODEL_RESPONSE"),
        ("message.outbound", None),
        ("message.inbound", None),
        ("conversation.state_changed", "COMPLETED"),
        ("final_answer.received", None),
        ("cycle.ended", None),
        ("conversation.state_changed", "WAITING_USER"),
        ("session.state_changed", "COMPLETED"),
    ]
    plan_events = rig.events(EventType.PLAN_STATE_CHANGED)
    assert [(e.plan_id, e.payload["to"]) for e in plan_events] == [
        ("plan-0", "RUNNING"),
        ("plan-0", "STOPPED_ON_FAILURE"),
        ("plan-1", "RUNNING"),
        ("plan-1", "COMPLETED"),
    ]
    assert len(rig.events(EventType.TASK_STATE_CHANGED)) == 14  # 7 tasks x (RUNNING, terminal)

    # ---- payload contract of the phase 10 subscribers (docs/phases/phase-10 §4) -------------------
    cycle_started = rig.events(EventType.CYCLE_STARTED)
    assert (
        cycle_started[0].cycle_id == "cyc-0001" and cycle_started[0].conversation_id == "conv-0001"
    )
    assert cycle_started[0].payload == {
        "cycle_type": "discovery",
        "outbound_message_id": "msg-0001",
        "outbound_message_type": "user_request",
        "consumed_cycles": 1,
    }
    assert cycle_started[1].payload["cycle_type"] == "execution"
    assert cycle_started[2].payload["consumed_cycles"] == 3
    outbound = rig.events(EventType.MESSAGE_OUTBOUND)
    assert outbound[0].cycle_id == "cyc-0001"
    assert outbound[0].payload == {
        "message_type": "user_request",
        "message_id": "msg-0001",
        "post_status": 202,
        "size_bytes": size_bytes(rig.posted(0)),
        "attempts": 1,
    }
    inbound = rig.events(EventType.MESSAGE_INBOUND)
    assert inbound[0].cycle_id == "cyc-0001"
    assert inbound[0].payload == {
        "message_type": "discovery_plan",
        "message_id": "model-msg-0001",
        "get_status": 200,
        "validation_status": "valid",
        "size_bytes": size_bytes(discovery_plan()),
    }
    received = rig.events(EventType.PLAN_RECEIVED)
    assert (received[0].cycle_id, received[0].plan_id) == ("cyc-0001", "plan-0")
    assert received[0].payload == {
        "plan_type": "discovery_plan",
        "objective": "Discover execution environment and build context",
        "execution_policy": "sequential",
        "max_parallel_workers": 1,
        "task_count": 5,
        "consumed_plans": 1,
        "contradictory_flags": [],
    }
    assert received[1].payload["max_parallel_workers"] == 2
    assert received[1].payload["consumed_plans"] == 2
    ended = rig.events(EventType.CYCLE_ENDED)
    assert [e.cycle_id for e in ended] == ["cyc-0001", "cyc-0002", "cyc-0003"]
    assert ended[0].plan_id == "plan-0"
    assert ended[0].payload == {
        "status": "COMPLETED",
        "duration_ms": 0,
        "retry_count": 0,
        "inbound_message_id": "model-msg-0001",
        "inbound_message_type": "discovery_plan",
    }
    assert ended[2].payload["inbound_message_type"] == "final_answer"
    final = rig.events(EventType.FINAL_ANSWER_RECEIVED)
    assert len(final) == 1 and final[0].cycle_id == "cyc-0003"
    assert final[0].payload == {
        "message_id": "model-msg-0003",
        "status": "completed",
        "auto_close_on_final_answer": False,
        "consumed_cycles": 3,
        "consumed_plans": 2,
        "session_duration_ms": 0,
    }
    budget = rig.events(EventType.BUDGET_UPDATED)
    assert budget[-1].payload == {
        "consumed_cycles": 3,
        "consumed_plans": 2,
        "consumed_duration_ms": 0,
        "max_cycles": 20,
        "max_plans": 10,
        "max_total_duration_ms": 300_000,
    }
    transitions = rig.events(EventType.CONVERSATION_STATE_CHANGED)
    assert [e.payload.get("reason") for e in transitions] == [
        "user_request",
        "user_request",
        "plan_received",
        "execution_result",
        "plan_received",
        "execution_result",
        "final_answer",
        "reusable",
    ]
    session_transitions = rig.events(EventType.SESSION_STATE_CHANGED)
    assert [e.payload for e in session_transitions] == [
        {"from": "READY", "to": "RUNNING", "reason": "user_request"},
        {"from": "RUNNING", "to": "COMPLETED", "reason": "final_answer"},
    ]
    assert all(e.session_id == sid for e in rig.events())

    # ---- audit chain (criterion 14) --------------------------------------------------------------
    verification = rig.app.audit.verify(sid)
    assert verification.valid is True
    audited = [e for e in rig.events() if e.audited]  # task.output is not audited (ADR-018)
    assert verification.checked == len(audited)
    assert rig.store.count_audit_events(sid) == len(audited)
    assert len(rig.events()) - len(audited) == len(rig.events(EventType.TASK_OUTPUT))


async def given_scripted_java_scenario_when_session_completes_then_snapshot_reflects_final_state(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run()
    sid = session.session_id

    snapshot = rig.manager.snapshot(sid)
    assert isinstance(snapshot, RuntimeSnapshot)
    assert snapshot == rig.app.tracker.snapshot(sid)
    assert snapshot.session.status is SessionState.COMPLETED
    assert snapshot.session.session_budget.model_dump() == {
        "max_cycles": 20,
        "max_plans": 10,
        "max_total_duration_ms": 300_000,
        "consumed_cycles": 3,
        "consumed_plans": 2,
        "consumed_duration_ms": 0,
    }
    assert snapshot.conversation is not None
    assert snapshot.conversation.status is ConversationState.WAITING_USER
    assert snapshot.conversation.final_answer_received is True
    assert snapshot.conversation.context_window_state is ContextWindowState.HEALTHY
    assert [c.conversation_id for c in snapshot.conversations] == ["conv-0001"]
    assert snapshot.cycle is not None and snapshot.cycle.cycle_id == "cyc-0003"
    assert snapshot.cycle.status is CycleState.COMPLETED
    assert snapshot.plan is not None and snapshot.plan.plan_id == "plan-1"
    assert snapshot.plan.status is PlanState.COMPLETED
    assert snapshot.plan.completed_task_count == 2
    assert [t.task_id for t in snapshot.tasks] == ["t6", "t7"]
    assert snapshot.running_task_ids == []
    assert snapshot.model_interaction.model_dump() == {
        "last_outbound_message_type": "execution_result",
        "last_inbound_message_type": "final_answer",
        "last_post_status": 202,
        "last_get_status": 200,
        "last_protocol_validation_status": "valid",
    }
    assert snapshot.last_event_type == "session.state_changed"
    assert snapshot.last_event_sequence == rig.store.count_audit_events(sid)
    assert snapshot == rig.app.tracker.rebuild(sid)
    assert rig.manager.final_answer(sid) == final_answer()["content"]
    assert rig.manager.running_task_ids(sid) == []


async def given_auto_close_when_final_answer_received_then_conversation_closed_and_remote_closed(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run(auto_close=True)

    assert session.status is SessionState.COMPLETED
    assert session.auto_close_on_final_answer is True
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.CLOSED
    assert conversation.closure_reason == "auto_close"
    assert conversation.final_answer_received is True
    assert rig.transport.closed == [REMOTE_1]
    assert _non_execution_kinds(rig)[-4:] == [
        ("final_answer.received", None),
        ("cycle.ended", None),
        ("conversation.state_changed", "CLOSED"),
        ("session.state_changed", "COMPLETED"),
    ]
    with pytest.raises(ValueError):
        await rig.manager.continue_session(session.session_id, "one more thing")


async def given_config_defaults_when_session_started_without_budget_then_config_budget_applied(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.start()

    assert session.status is SessionState.RUNNING
    assert session.budget.model_dump() == {
        "max_cycles": rig.config.budget.default_max_cycles,
        "max_plans": rig.config.budget.default_max_plans,
        "max_total_duration_ms": rig.config.budget.default_max_total_duration_ms,
    }
    assert session.auto_close_on_final_answer is rig.config.budget.auto_close_on_final_answer
    assert session.user_id == rig.config.transport.user_id
    assert session.current_conversation_id == "conv-0001"
    assert session.started_at == rig.clock.now()
    # the loop runs in the background: the start returned before the first POST
    assert rig.transport.posted == []
    assert rig.conversation("conv-0001").status is ConversationState.ACTIVE
    await rig.wait(session.session_id)
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]


async def given_priority_clarification_when_received_then_cycle_type_clarification_and_loop_continues(
    rig: Rig,
) -> None:
    rig.script_spec_outputs()
    clarification = execution_plan(
        message_type="priority_clarification",
        plan_id="plan-1a",
        tasks=[
            cmd_task(
                "t8",
                CMD_MVN,
                critical=True,
                continue_on_error=False,
                stop_plan_on_failure=True,
                max_output_bytes=1024,
            )
        ],
        execution_policy="sequential",
        max_parallel_workers=None,
        objective="Immediately confirm which Java version Maven is using",
    )
    rig.reply(REMOTE_1, discovery_plan(), clarification, final_answer())

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    plan = rig.plan(session.session_id, "plan-1a")
    assert plan.plan_type is PlanType.PRIORITY_CLARIFICATION
    assert plan.status is PlanState.COMPLETED
    cycles = rig.cycles("conv-0001")
    assert [c.cycle_type for c in cycles] == [
        CycleType.DISCOVERY,
        CycleType.CLARIFICATION,
        CycleType.EXECUTION,
    ]
    assert rig.posted(2)["content"]["plan_id"] == "plan-1a"
    received = rig.events(EventType.PLAN_RECEIVED)[1]
    assert received.payload["plan_type"] == "priority_clarification"


async def given_truncated_output_when_chunk_request_received_then_range_served_from_blob(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", stdout=b"0123456789" * 10)
    rig.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1", max_output_bytes=16, continue_on_error=True)]),
        execution_plan(
            tasks=[chunk_task("t-chunk-1", "t1", offset=0, max_bytes=16)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
        final_answer(),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    first = rig.posted(1)["content"]["results"][0]
    assert first["truncated"] is True
    assert first["stdout"] == ("0123456789" * 10)[84:]  # the end of stdout is kept (ADR-011)
    assert first["stdout_range"] == [84, 100]
    assert first["stdout_total"] == 100
    chunk = rig.posted(2)["content"]["results"][0]
    assert chunk == {
        "task_id": "t-chunk-1",
        "status": "completed",
        "stdout": "",
        "stderr": "",
        "truncated": False,
        "timed_out": False,
        "duration_ms": 0,
        "ref_task_id": "t1",
        "stream": "stdout",
        "range": [0, 16],
        "total": 100,
        "eof": False,
        "data": "0123456789012345",
    }
    assert rig.task(session.session_id, "t-chunk-1").status is TaskState.COMPLETED


async def given_chunk_request_on_unknown_task_when_received_then_protocol_error_and_session_failed(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", stdout=b"hello")
    rig.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1", continue_on_error=True)]),
        execution_plan(
            tasks=[chunk_task("t-chunk-1", "t-unknown")],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
    )

    session = await rig.run()

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(session.session_id)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, "CHUNK_REF_UNKNOWN")
    ]
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == "CHUNK_REF_UNKNOWN"
    assert rig.posted_types() == ["user_request", "execution_result"]


async def given_contradictory_flags_when_plan_received_then_audit_warning_published(
    rig: Rig,
) -> None:
    rig.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1", critical=True, continue_on_error=True)]),
        final_answer(message_id="model-msg-0002"),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    warnings = rig.events(EventType.AUDIT_WARNING)
    assert len(warnings) == 1
    assert warnings[0].plan_id == "plan-0" and warnings[0].cycle_id == "cyc-0001"
    assert warnings[0].payload == {
        "code": "CONTRADICTORY_FLAGS:t1",
        "entity": "plan",
        "id": "plan-0",
        "details": {"task_id": "t1"},
    }
    assert rig.events(EventType.PLAN_RECEIVED)[0].payload["contradictory_flags"] == ["t1"]


# ================================================================================================
# 2. the manager façade (§3.1, ADR-002, ADR-018)
# ================================================================================================
async def given_running_session_when_facade_read_then_state_visible_at_any_instant(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", hang_until_cancelled=True)
    rig.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1")]))
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(rig.executor.wait_spawned("t1"), 2.0)

    assert rig.manager.get_session(sid) is not None
    assert rig.manager.get_session(sid).status is SessionState.RUNNING  # type: ignore[union-attr]
    assert rig.manager.get_session("sess-unknown") is None
    assert [s.session_id for s in rig.manager.list_sessions()] == [sid]
    assert rig.manager.list_sessions(statuses=[SessionState.RUNNING])[0].session_id == sid
    assert rig.manager.list_sessions(statuses=[SessionState.COMPLETED]) == []
    assert rig.manager.running_task_ids(sid) == ["t1"]
    assert rig.manager.final_answer(sid) is None
    snapshot = rig.manager.snapshot(sid)
    assert snapshot.session.status is SessionState.RUNNING
    assert snapshot.conversation is not None
    assert snapshot.conversation.status is ConversationState.RUNNING_PLAN
    assert snapshot.plan is not None and snapshot.plan.status is PlanState.RUNNING
    assert snapshot.running_task_ids == ["t1"]
    with pytest.raises(TimeoutError):
        await rig.manager.wait(sid, timeout_ms=10)
    with pytest.raises(KeyError):
        rig.manager.snapshot("sess-unknown")
    with pytest.raises(KeyError):
        await rig.manager.wait("sess-unknown")
    with pytest.raises(ValueError):
        await rig.manager.continue_session(sid, "not now")  # still running

    await rig.manager.interrupt(sid)
    assert (await rig.wait(sid)).status is SessionState.READY


async def given_manager_when_properties_read_then_wired_components_exposed(rig: Rig) -> None:
    manager = rig.manager
    assert isinstance(manager, ConversationManager)
    assert manager.config is rig.config
    assert manager.store is rig.store
    assert manager.bus is rig.app.bus
    assert manager.clock is rig.clock
    assert manager.tracker is rig.app.tracker
    assert manager.audit is rig.app.audit
    assert manager.telemetry is rig.app.telemetry
    assert manager.recovery_report is None  # run_recovery=False in the rig
    assert isinstance(rig.app, Application)
    assert isinstance(rig.app.orchestrator, ProtocolOrchestrator)
    assert isinstance(rig.app.recovery, RecoveryCoordinator)
    names = rig.app.bus.subscriber_names
    assert names.index("audit_log") < names.index("execution_tracker") < names.index("telemetry")


# ================================================================================================
# 3. interruption mid-plan (§2.9, §8.4, §9 ; ADR-006 ; criteria 15 and 16)
# ================================================================================================
async def given_plan_running_when_user_interrupts_then_everything_interrupted_and_nothing_sent(
    rig: Rig,
) -> None:
    rig.script_spec_outputs()
    rig.executor.script(task_id="t3", hang_until_cancelled=True)
    rig.reply(REMOTE_1, discovery_plan())
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(rig.executor.wait_spawned("t3"), 2.0)
    assert rig.manager.running_task_ids(sid) == ["t3"]

    report = await asyncio.wait_for(rig.manager.interrupt(sid), 3.0)
    ended = await rig.wait(sid)

    assert report.session_status is SessionState.READY
    assert report.nothing_to_interrupt is False
    assert report.loop_drained is True
    assert report.within_timeout is True
    assert report.interrupted_task_ids == ["t3", "t4", "t5"]
    assert (report.plan_id, report.cycle_id, report.conversation_id) == (
        "plan-0",
        "cyc-0001",
        "conv-0001",
    )
    assert ended.status is SessionState.READY
    assert ended.interrupted_at == rig.clock.now()
    assert ended.consumed_cycles == 1 and ended.consumed_plans == 1
    assert {t.task_id: (t.status, t.reason) for t in rig.tasks(sid)} == {
        "t1": (TaskState.COMPLETED, None),
        "t2": (TaskState.COMPLETED, None),
        "t3": (TaskState.INTERRUPTED, "user_interrupt"),
        "t4": (TaskState.INTERRUPTED, "user_interrupt"),
        "t5": (TaskState.INTERRUPTED, "user_interrupt"),
    }
    plan = rig.plan(sid, "plan-0")
    assert (plan.status, plan.stop_reason, plan.interrupted_task_count) == (
        PlanState.INTERRUPTED,
        "user_interrupt",
        3,
    )
    assert rig.cycles("conv-0001")[0].status is CycleState.INTERRUPTED
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.INTERRUPTED
    assert conversation.interrupted_at is not None
    assert rig.posted_types() == ["user_request"]  # no execution_result (§8.4)
    assert rig.executor.cancellations == [("t3", "user_interrupt")]
    assert rig.transport.closed == [REMOTE_1]
    assert rig.app.interruption.token_for(sid).is_cancelled is False
    kinds = rig.event_kinds()
    assert ("interruption.requested", None) in kinds
    assert kinds[-1] == ("interruption.completed", None)
    assert kinds[-2] == ("session.state_changed", "READY")
    assert rig.app.audit.verify(sid).valid is True


async def given_interrupted_session_when_new_request_then_new_child_conversation_runs_to_final_answer(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", hang_until_cancelled=True)
    rig.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1")]))
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(rig.executor.wait_spawned("t1"), 2.0)
    await rig.manager.interrupt(sid)
    await rig.wait(sid)
    rig.recorder.clear()
    rig.reply(
        REMOTE_2,
        discovery_plan(
            REMOTE_2, message_id="model-msg-0010", plan_id="plan-r0", tasks=[cmd_task("r1")]
        ),
        final_answer(REMOTE_2, message_id="model-msg-0011"),
    )

    restarted = await rig.manager.continue_session(sid, "Try again, please.")
    ended = await rig.wait(sid)

    assert restarted.status is SessionState.RUNNING
    assert restarted.session_id == sid
    assert ended.status is SessionState.COMPLETED
    assert ended.consumed_cycles == 3  # first request, second request, one execution_result
    assert ended.consumed_plans == 2
    assert ended.final_answer == final_answer(REMOTE_2, message_id="model-msg-0011")["content"]
    conversations = rig.conversations(sid)
    assert [c.conversation_id for c in conversations] == ["conv-0001", "conv-0002"]
    first, second = conversations
    assert first.status is ConversationState.INTERRUPTED
    assert second.parent_conversation_id == "conv-0001"
    assert second.remote_conversation_id == REMOTE_2
    assert second.status is ConversationState.WAITING_USER
    assert ended.current_conversation_id == "conv-0002"
    assert len(rig.transport.inits) == 2
    assert rig.transport.inits[1]["metadata"] == {
        "session_id": sid,
        "parent_conversation_id": "conv-0001",
    }
    assert [remote for remote, _ in rig.transport.posted] == [REMOTE_1, REMOTE_2, REMOTE_2]
    request = rig.posted(1)
    assert request["type"] == "user_request"
    assert request["content"]["user_message"] == "Try again, please."
    assert request["content"]["goal"] == GOAL
    assert rig.posted(2)["content"]["plan_id"] == "plan-r0"
    assert rig.event_kinds()[:4] == [
        ("session.state_changed", "RUNNING"),
        ("conversation.created", None),
        ("conversation.state_changed", "ACTIVE"),
        ("cycle.started", None),
    ]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[0].payload["reason"] == "user_request"
    assert rig.plan(sid, "plan-0").status is PlanState.INTERRUPTED  # untouched (§17.4)
    assert rig.executor.calls[-1].cmd == "run r1"


async def given_shutdown_requested_when_session_running_then_session_interrupted_and_ready(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", hang_until_cancelled=True)
    rig.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1")]))
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(rig.executor.wait_spawned("t1"), 2.0)

    await asyncio.wait_for(rig.manager.shutdown(), 3.0)

    assert rig.session(sid).status is SessionState.READY
    assert rig.conversation("conv-0001").status is ConversationState.INTERRUPTED
    assert rig.task(sid, "t1").status is TaskState.INTERRUPTED
    assert rig.executor.cancellations == [("t1", "user_interrupt")]  # the runner's drain reason
    assert rig.events(EventType.INTERRUPTION_REQUESTED)[0].payload["reason"] == "shutdown"
    assert rig.events(EventType.CONVERSATION_STATE_CHANGED)[-1].payload["reason"] == "shutdown"
    assert rig.posted_types() == ["user_request"]
    assert (await rig.wait(sid)).status is SessionState.READY
    # idempotent: a second shutdown has nothing to do
    await asyncio.wait_for(rig.manager.shutdown(), 1.0)
    assert rig.session(sid).status is SessionState.READY


# ================================================================================================
# 4. session budget (§2.8, ADR-012 ; criterion 12)
# ================================================================================================
async def given_budget_max_plans_reached_when_plan_received_then_session_failed_with_budget_exceeded(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run(
        budget={"max_cycles": 20, "max_plans": 1, "max_total_duration_ms": 300_000}
    )
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert session.consumed_plans == 2 and session.consumed_cycles == 2
    assert session.ended_at == rig.clock.now()
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.BUDGET_EXCEEDED, "BUDGET_MAX_PLANS")
    ]
    assert failures[0].details == {"limit": "max_plans", "limit_value": 1, "consumed": 2}
    assert failures[0].plan_id == "plan-1" and failures[0].conversation_id == "conv-0001"
    assert session.last_failure_id == failures[0].failure_id
    plan = rig.plan(sid, "plan-1")
    assert (plan.status, plan.stop_reason) == (PlanState.FAILED, "budget_exceeded:max_plans")
    assert {(t.status, t.reason) for t in rig.tasks(sid, "plan-1")} == {
        (TaskState.SKIPPED, "budget_exceeded")
    }
    assert rig.executor.calls[-1].cmd == CMD_BUILD  # plan-1 never started
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert rig.cycles("conv-0001")[1].status is CycleState.FAILED
    assert rig.posted_types() == ["user_request", "execution_result"]  # nothing more sent
    assert rig.transport.closed == [REMOTE_1]
    exceeded = rig.events(EventType.BUDGET_EXCEEDED)
    assert len(exceeded) == 1 and exceeded[0].plan_id == "plan-1"
    assert exceeded[0].payload == {
        "limit": "max_plans",
        "limit_value": 1,
        "consumed": 2,
        "stage": "before_plan",
    }
    assert rig.event_kinds()[-8:] == [
        ("task.state_changed", "SKIPPED"),
        ("task.state_changed", "SKIPPED"),
        ("plan.state_changed", "FAILED"),
        ("failure.recorded", None),
        ("budget.exceeded", None),
        ("cycle.ended", None),
        ("conversation.state_changed", "FAILED"),
        ("session.state_changed", "FAILED"),
    ]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "budget_exceeded"
    snapshot = rig.manager.snapshot(sid)
    assert snapshot.session.session_budget.consumed_plans == 2
    assert snapshot.session.session_budget.max_plans == 1
    assert snapshot.session.status is SessionState.FAILED
    assert rig.app.audit.verify(sid).valid is True


async def given_budget_max_cycles_reached_when_next_cycle_needed_then_session_failed_before_post(
    rig: Rig,
) -> None:
    rig.script_java_scenario()

    session = await rig.run(
        budget={"max_cycles": 2, "max_plans": 10, "max_total_duration_ms": 300_000}
    )
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert session.consumed_cycles == 2 and session.consumed_plans == 2
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.BUDGET_EXCEEDED, "BUDGET_MAX_CYCLES")
    ]
    assert failures[0].details == {"limit": "max_cycles", "limit_value": 2, "consumed": 2}
    assert (
        rig.plan(sid, "plan-1").status is PlanState.COMPLETED
    )  # the plan ran; its result stays local
    assert rig.posted_types() == ["user_request", "execution_result"]
    assert [c.status for c in rig.cycles("conv-0001")] == [
        CycleState.COMPLETED,
        CycleState.COMPLETED,
    ]
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    exceeded = rig.events(EventType.BUDGET_EXCEEDED)[0]
    assert exceeded.payload == {
        "limit": "max_cycles",
        "limit_value": 2,
        "consumed": 2,
        "stage": "before_cycle",
    }
    outbound = rig.store.list_messages("conv-0001", direction=MessageDirection.OUTBOUND)
    assert [m.message_id for m in outbound] == ["msg-0001", "msg-0002"]


async def given_deadline_passed_between_tasks_when_next_task_due_then_plan_failed_and_session_failed(
    rig: Rig,
) -> None:
    rig.executor.script(task_id="t1", stdout=b"slow", duration_ms=1_500)
    rig.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1"), cmd_task("t2")]))

    session = await rig.run(
        budget={"max_cycles": 20, "max_plans": 10, "max_total_duration_ms": 1_000}
    )
    sid = session.session_id

    assert session.status is SessionState.FAILED
    plan = rig.plan(sid, "plan-0")
    assert (plan.status, plan.stop_reason) == (
        PlanState.FAILED,
        "budget_exceeded:max_total_duration_ms",
    )
    assert rig.task(sid, "t1").status is TaskState.COMPLETED
    assert (rig.task(sid, "t2").status, rig.task(sid, "t2").reason) == (
        TaskState.SKIPPED,
        "budget_exceeded",
    )
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.BUDGET_EXCEEDED, "BUDGET_MAX_TOTAL_DURATION_MS")
    ]
    assert failures[0].details["limit"] == "max_total_duration_ms"
    assert failures[0].details["consumed"] >= 1_500
    assert rig.posted_types() == ["user_request"]  # the result is persisted locally, never sent
    exceeded = rig.events(EventType.BUDGET_EXCEEDED)[0]
    assert exceeded.payload["stage"] == "between_tasks"
    assert exceeded.payload["limit"] == "max_total_duration_ms"
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED


# ================================================================================================
# 5. context rotation mid-session (§2.6, §10 ; ADR-013, ADR-014, ADR-019 ; criteria 6 and 7)
# ================================================================================================
def _tiny_rotation_rig() -> Rig:
    """Budgets small enough for a rotation after three 1.4 KB results, honouring ADR-019 §4."""
    config = make_config(
        context={"budget_bytes": 6_000, "summary_budget_bytes": 3_000},
        payload={
            "default_max_output_bytes": 2_000,
            "hard_max_output_bytes": 2_000,
            "max_message_bytes": 3_000,
        },
    )
    return make_rig(config, instructions=TINY_INSTRUCTIONS)


async def given_context_saturated_by_projection_when_result_ready_then_rotation_then_final_answer_in_child() -> (
    None
):
    rig = _tiny_rotation_rig()
    big = b"x" * 1_400
    for task_id in ("t1", "t2", "t3"):
        rig.executor.script(task_id=task_id, stdout=big)
    rig.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1", continue_on_error=True)]),
        execution_plan(
            message_id="model-msg-0002",
            plan_id="plan-1",
            tasks=[cmd_task("t2", continue_on_error=True)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
        execution_plan(
            message_id="model-msg-0003",
            plan_id="plan-2",
            tasks=[cmd_task("t3", continue_on_error=True)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
    )
    rig.reply(
        REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0004", evidence=False)
    )

    session = await rig.run()
    sid = session.session_id

    # ---- session: completed in the child, one rotation, budget carried over (ADR-012) --------------
    assert session.status is SessionState.COMPLETED
    assert session.rotations_count == 1
    assert (
        session.consumed_cycles == 5
    )  # user_request, 3 results (the 3rd never sent as such), resume
    assert session.consumed_plans == 3
    assert session.current_conversation_id == "conv-0002"
    assert session.final_answer is not None and session.final_answer["diagnosis"] == FINAL_DIAGNOSIS

    # ---- parent CLOSED (rotated), child HEALTHY and reusable ----------------------------------------
    parent, child = rig.conversations(sid)
    assert parent.conversation_id == "conv-0001"
    assert parent.status is ConversationState.CLOSED
    assert parent.closure_reason == "rotated"
    assert parent.context_window_state is ContextWindowState.SATURATED
    assert child.conversation_id == "conv-0002"
    assert child.parent_conversation_id == "conv-0001"
    assert child.remote_conversation_id == REMOTE_2
    assert child.status is ConversationState.WAITING_USER
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert child.final_answer_received is True
    assert child.context_bytes < rig.app.monitor.thresholds().warning_bytes

    # ---- transport: the third result was never sent in the parent, retransmitted in the child ------
    assert rig.posted_types() == [
        "user_request",
        "execution_result",
        "execution_result",
        "context_resume_request",
        "execution_result",
    ]
    assert [remote for remote, _ in rig.transport.posted] == [
        REMOTE_1,
        REMOTE_1,
        REMOTE_1,
        REMOTE_2,
        REMOTE_2,
    ]
    resume_request = rig.posted(3)
    assert resume_request["content"]["original_conversation_id"] == REMOTE_1
    assert resume_request["content"]["pending_message_type"] == "execution_result"
    assert resume_request["content"]["goal"] == GOAL
    summary = resume_request["content"]["context_summary"]
    assert [p["plan_id"] for p in summary["plan_ledger"]] == ["plan-0", "plan-1", "plan-2"]
    assert summary["budget"]["consumed_plans"] == 3
    retransmitted = rig.posted(4)
    assert retransmitted["message_id"] == "msg-0006"
    assert retransmitted["content"]["plan_id"] == "plan-2"
    original = rig.store.get_message("msg-0004")
    assert original is not None
    assert original.post_confirmed is False and original.conversation_id == "conv-0001"
    assert retransmitted["content"] == original.payload["content"]
    copy = rig.store.get_message("msg-0006")
    assert copy is not None
    assert copy.retransmission_of == "msg-0004"
    assert copy.cycle_id == "cyc-0004" and copy.post_confirmed is True
    assert rig.transport.closed == [REMOTE_1]
    assert len(rig.transport.inits) == 2

    # ---- cycles: M's cycle continues in the child (ADR-019 §5), the resume cycle is the child's -----
    parent_cycles = rig.cycles("conv-0001")
    assert [c.cycle_id for c in parent_cycles] == ["cyc-0001", "cyc-0002", "cyc-0003", "cyc-0004"]
    assert [c.status for c in parent_cycles] == [CycleState.COMPLETED] * 4
    assert parent_cycles[3].inbound_message_id == "model-msg-0004"
    child_cycles = rig.cycles("conv-0002")
    assert [(c.cycle_id, c.cycle_type, c.status) for c in child_cycles] == [
        ("cyc-0005", CycleType.RESUME, CycleState.COMPLETED)
    ]
    assert child.current_cycle_id == "cyc-0004"

    # ---- window and rotation events, in order ---------------------------------------------------------
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.conversation_id, e.payload["from"], e.payload["to"]) for e in windows] == [
        ("conv-0001", "HEALTHY", "WARNING"),
        ("conv-0001", "WARNING", "SATURATED"),
        ("conv-0002", "SATURATED", "HEALTHY"),
    ]
    assert windows[1].payload["reason"] == "projection"
    started = rig.events(EventType.ROTATION_STARTED)
    assert len(started) == 1 and started[0].conversation_id == "conv-0001"
    assert started[0].payload["pending_message_type"] == "execution_result"
    completed = rig.events(EventType.ROTATION_COMPLETED)
    assert len(completed) == 1 and completed[0].conversation_id == "conv-0002"
    assert rig.events(EventType.ROTATION_FAILED) == []
    retransmit = rig.events(EventType.MESSAGE_RETRANSMITTED)
    assert len(retransmit) == 1 and retransmit[0].payload["retransmission_of"] == "msg-0004"
    kinds = rig.event_kinds()
    assert kinds.index(("rotation.started", None)) < kinds.index(("rotation.completed", None))
    assert kinds.index(("rotation.completed", None)) < kinds.index(("final_answer.received", None))
    assert rig.store.get_context_summary_for_target("conv-0002") is not None
    assert rig.app.audit.verify(sid).valid is True
    snapshot = rig.manager.snapshot(sid)
    assert [(c.conversation_id, c.status) for c in snapshot.conversations] == [
        ("conv-0001", ConversationState.CLOSED),
        ("conv-0002", ConversationState.WAITING_USER),
    ]
    assert snapshot.session.rotations_count == 1


async def given_context_window_error_on_get_when_decided_rotate_then_user_request_retransmitted_in_child(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error(
        "get",
        _transport_error(
            ErrorType.MODEL_CONTEXT_WINDOW_ERROR,
            "HTTP_413",
            operation="GET",
            retryable=False,
            http_status=413,
        ),
    )
    rig.executor.script(task_id="t1", stdout=b"ok")
    rig.reply(
        REMOTE_2,
        resume_ack(),
        discovery_plan(REMOTE_2, message_id="model-msg-0010", tasks=[cmd_task("t1")]),
        final_answer(REMOTE_2, message_id="model-msg-0011"),
    )

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert session.rotations_count == 1
    assert session.consumed_cycles == 3  # user_request, resume, execution_result
    assert rig.posted_types() == [
        "user_request",
        "context_resume_request",
        "user_request",
        "execution_result",
    ]
    assert [remote for remote, _ in rig.transport.posted] == [
        REMOTE_1,
        REMOTE_2,
        REMOTE_2,
        REMOTE_2,
    ]
    assert rig.posted(2)["content"] == rig.posted(0)["content"]
    assert rig.posted(2)["message_id"] != rig.posted(0)["message_id"]
    parent, child = rig.conversations(sid)
    assert parent.status is ConversationState.CLOSED and parent.closure_reason == "rotated"
    assert parent.context_window_state is ContextWindowState.SATURATED
    assert child.status is ConversationState.WAITING_USER
    assert child.context_window_state is ContextWindowState.HEALTHY
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413")
    ]
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision) for d in decisions] == [("GET", "rotate")]
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.conversation_id, e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("conv-0001", "SATURATED", "context_window_error"),
        ("conv-0002", "HEALTHY", "resume_acknowledged"),
    ]
    assert rig.plan(sid, "plan-0").conversation_id == "conv-0002"


async def given_rotation_limit_reached_when_rotation_needed_then_session_failed_with_rotation_failed() -> (
    None
):
    rig = make_rig(make_config(context={"max_rotations_per_session": 0}))
    rig.transport.enqueue_error(
        "get",
        _transport_error(
            ErrorType.MODEL_CONTEXT_WINDOW_ERROR,
            "CONTEXT_WINDOW_EXCEEDED",
            operation="GET",
            retryable=False,
            http_status=400,
        ),
    )

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert session.rotations_count == 0
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "CONTEXT_WINDOW_EXCEEDED"),
        (ErrorType.ROTATION_FAILED, "ROTATION_LIMIT_REACHED"),
    ]
    assert session.last_failure_id == failures[1].failure_id
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.conversations(sid) == [rig.conversation("conv-0001")]
    assert (
        rig.events(EventType.ROTATION_FAILED)[0].payload["error_code"] == "ROTATION_LIMIT_REACHED"
    )
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "rotation_failed"
    assert rig.posted_types() == ["user_request"]
    assert len(rig.transport.inits) == 1


# ================================================================================================
# 6. protocol errors (§3.5, §7.2 ; ADR-007, ADR-019 §2 ; criterion 4)
# ================================================================================================
async def given_unexpected_message_type_in_healthy_window_when_received_then_session_failed_with_protocol_error(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, final_answer())  # a final_answer cannot answer the first user_request

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, "UNEXPECTED_MESSAGE_TYPE")
    ]
    assert failures[0].details["received"] == "final_answer"
    # the initial row of ADR-007 amended by ADR-022 (protocol.allow_direct_response defaults on)
    assert failures[0].details["expected"] == ["discovery_plan", "user_response"]
    assert failures[0].conversation_id == "conv-0001"
    assert [(d.operation, d.decision) for d in rig.store.list_retry_decisions(sid)] == [
        ("GET", "fail")
    ]
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert conversation.protocol_error_count == 1
    assert conversation.get_cursor == "model-msg-0003"  # the cursor moves past the rejected message
    assert conversation.last_model_response_state == "received_invalid"
    messages = rig.store.list_messages("conv-0001")
    assert [(m.direction, m.validation_status) for m in messages] == [
        (MessageDirection.OUTBOUND, None),
        (MessageDirection.INBOUND, "invalid"),
    ]
    assert messages[1].message_id == "model-msg-0003"
    assert messages[1].payload == final_answer()
    assert conversation.context_bytes == (
        len(render_instructions(rig.config).encode("utf-8")) + sum(m.size_bytes for m in messages)
    )
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].cycle_id == "cyc-0001"
    assert rejected[0].payload == {
        "message_type": "final_answer",
        "message_id": "model-msg-0003",
        "get_status": 200,
        "validation_status": "invalid",
        "error_code": "UNEXPECTED_MESSAGE_TYPE",
        "size_bytes": size_bytes(final_answer()),
        "details": failures[0].details,
    }
    assert rig.events(EventType.MESSAGE_INBOUND) == []
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    assert rig.events(EventType.CYCLE_ENDED)[0].payload["status"] == "FAILED"
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "failure"
    assert rig.transport.closed == [REMOTE_1]
    assert rig.events(EventType.ROTATION_STARTED) == []
    snapshot = rig.manager.snapshot(sid)
    assert snapshot.model_interaction.last_protocol_validation_status == "invalid"
    assert snapshot.model_interaction.last_inbound_message_type == "final_answer"


async def given_unexpected_message_in_warning_window_when_received_then_rotation_and_follow_up_retransmitted(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    assert session.status is SessionState.COMPLETED
    # preload the reusable conversation just above the warning threshold (ADR-019 §2 precondition)
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation("conv-0001", context_bytes=warning_bytes + 1_000)
    rig.recorder.clear()
    rig.reply(REMOTE_1, resume_ack(REMOTE_1, "remote-0000", message_id="model-msg-0009"))
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0010"))

    resumed = await rig.manager.continue_session(sid, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(sid)

    assert resumed.status is SessionState.RUNNING
    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    assert ended.consumed_cycles == 5  # 3 + follow-up + resume
    assert ended.final_answer == final_answer(REMOTE_2, message_id="model-msg-0010")["content"]
    parent, child = rig.conversations(sid)
    assert parent.status is ConversationState.CLOSED and parent.closure_reason == "rotated"
    assert parent.protocol_error_count == 1
    assert parent.context_window_state is ContextWindowState.SATURATED
    assert child.status is ConversationState.WAITING_USER
    assert child.final_answer_received is True
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert rig.posted_types() == [
        "user_request",
        "execution_result",
        "execution_result",
        "user_request",
        "context_resume_request",
        "user_request",
    ]
    follow_up, retransmitted = rig.posted(3), rig.posted(5)
    assert follow_up["conversation_id"] == REMOTE_1
    assert follow_up["content"]["user_message"] == "Is JAVA_HOME pointing at the JDK 17?"
    assert retransmitted["conversation_id"] == REMOTE_2
    assert retransmitted["content"] == follow_up["content"]
    assert rig.posted(4)["content"]["pending_message_type"] == "user_request"
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, "UNEXPECTED_MESSAGE_TYPE")
    ]
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.conversation_id, e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("conv-0001", "WARNING", "threshold"),
        ("conv-0001", "SATURATED", "unusable_reply"),
        ("conv-0002", "HEALTHY", "resume_acknowledged"),
    ]
    assert rig.event_kinds()[:2] == [
        ("session.state_changed", "RUNNING"),
        ("cycle.started", None),
    ]
    assert (
        rig.events(EventType.MESSAGE_REJECTED)[0].payload["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    )
    assert rig.app.audit.verify(sid).valid is True


async def given_get_timeout_exhausted_in_warning_window_when_decided_fail_then_rotation_instead() -> (
    None
):
    rig = make_rig(
        make_config(retry={"max_attempts": 2, "base_delay_ms": 100}), reply_timeout_ms=1_000
    )
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation("conv-0001", context_bytes=warning_bytes + 1_000)
    rig.recorder.clear()
    # nothing queued for remote-0001: every GET times out; the child answers at once
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0010"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.TIMEOUT_ERROR, "MODEL_GET_TIMEOUT"),
        (ErrorType.TIMEOUT_ERROR, "MODEL_GET_TIMEOUT"),
    ]
    assert [d.decision for d in rig.store.list_retry_decisions(sid)] == ["retry", "fail"]
    assert rig.cycles("conv-0001")[3].retry_count == 1
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("WARNING", "threshold"),
        ("SATURATED", "unusable_reply"),
        ("HEALTHY", "resume_acknowledged"),
    ]
    assert rig.posted_types()[-3:] == ["user_request", "context_resume_request", "user_request"]


# ================================================================================================
# 7. transport failures (§7 ; ADR-004, ADR-017, ADR-019 §6 ; criteria 4 and 5)
# ================================================================================================
async def given_network_error_on_post_when_retried_then_same_message_reposted_after_backoff(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    rig.transport.enqueue_error(
        "post",
        _transport_error(
            ErrorType.NETWORK_ERROR, "HTTP_503", operation="POST", retryable=True, http_status=503
        ),
    )
    before = rig.clock.monotonic_ms()

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert rig.posted(0)["message_id"] == "msg-0001"  # the same message_id (idempotent POST)
    assert rig.clock.monotonic_ms() - before == 500  # one backoff of base_delay_ms (ADR-017)
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code, f.attempt, f.max_attempts) for f in failures] == [
        (ErrorType.NETWORK_ERROR, "HTTP_503", 1, 4)
    ]
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision, d.delay_ms, d.attempt, d.cycle_id) for d in decisions] == [
        ("POST", "retry", 500, 1, "cyc-0001")
    ]
    assert rig.cycles("conv-0001")[0].retry_count == 1
    scheduled = rig.events(EventType.RETRY_SCHEDULED)
    assert len(scheduled) == 1 and scheduled[0].cycle_id == "cyc-0001"
    assert scheduled[0].payload["delay_ms"] == 500 and scheduled[0].payload["operation"] == "POST"
    outbound = rig.events(EventType.MESSAGE_OUTBOUND)[0]
    assert outbound.payload["attempts"] == 2
    assert rig.app.breaker.state.value == "CLOSED"
    assert rig.app.breaker.consecutive_failures == 0  # note_success after the successful call
    kinds = rig.event_kinds()
    assert kinds.index(("failure.recorded", None)) < kinds.index(("retry.scheduled", None))
    assert kinds.index(("retry.scheduled", None)) < kinds.index(("message.outbound", None))


async def given_authentication_error_on_post_when_decided_then_no_retry_and_session_failed(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error(
        "post",
        _transport_error(
            ErrorType.AUTHN_ERROR, "HTTP_401", operation="POST", retryable=False, http_status=401
        ),
    )

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert rig.transport.posted == []
    assert rig.transport.get_calls == []
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [(ErrorType.AUTHN_ERROR, "HTTP_401")]
    assert session.last_failure_id == failures[0].failure_id
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision, d.delay_ms) for d in decisions] == [("POST", "fail", None)]
    assert rig.events(EventType.RETRY_SCHEDULED) == []
    message = rig.store.get_message("msg-0001")
    assert message is not None and message.post_confirmed is False
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert rig.transport.closed == [REMOTE_1]
    assert rig.event_kinds()[-3:] == [
        ("cycle.ended", None),
        ("conversation.state_changed", "FAILED"),
        ("session.state_changed", "FAILED"),
    ]
    assert rig.events(EventType.CONVERSATION_STATE_CHANGED)[-1].payload["reason"] == "failure"


async def given_get_timeouts_in_healthy_window_when_attempts_exhausted_then_session_failed() -> (
    None
):
    rig = make_rig(reply_timeout_ms=1_000)
    before = rig.clock.monotonic_ms()

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert len(rig.transport.get_calls) == 4  # max_attempts
    failures = rig.store.list_failures(sid)
    assert [(f.error_code, f.attempt) for f in failures] == [
        ("MODEL_GET_TIMEOUT", 1),
        ("MODEL_GET_TIMEOUT", 2),
        ("MODEL_GET_TIMEOUT", 3),
        ("MODEL_GET_TIMEOUT", 4),
    ]
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.decision, d.delay_ms) for d in decisions] == [
        ("retry", 500),
        ("retry", 1_000),
        ("retry", 2_000),
        ("fail", None),
    ]
    assert rig.clock.monotonic_ms() - before == 4 * 1_000 + 500 + 1_000 + 2_000
    assert rig.cycles("conv-0001")[0].retry_count == 3
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.events(EventType.ROTATION_STARTED) == []
    assert rig.app.breaker.consecutive_failures == 4


async def given_open_circuit_breaker_when_remote_call_due_then_circuit_open_failure_without_call() -> (
    None
):
    rig = make_rig(
        make_config(breaker={"failure_threshold": 1, "open_duration_ms": 30_000}),
    )
    rig.app.breaker.record_failure()  # opens the breaker before the session starts
    assert rig.app.breaker.allow() is False
    before = rig.clock.monotonic_ms()

    session = await rig.run(
        budget={"max_cycles": 20, "max_plans": 10, "max_total_duration_ms": 1_000}
    )
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert rig.transport.inits == [] and rig.transport.posted == []
    # waited once, bounded by the duration budget (ADR-019 §6), then gave up
    assert rig.clock.monotonic_ms() - before == 1_000
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.NETWORK_ERROR, "CIRCUIT_OPEN")
    ]
    assert failures[0].details["operation"] == "INIT"
    assert failures[0].retryable is False
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision) for d in decisions] == [("INIT", "fail")]
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert conversation.remote_conversation_id is None
    assert rig.transport.closed == []  # nothing remote to close
    assert rig.app.breaker.degraded is True


async def given_network_errors_on_init_when_attempts_exhausted_then_conversation_failed_from_active(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error(
        "init",
        _transport_error(
            ErrorType.NETWORK_ERROR, "CONNECTION_ERROR", operation="INIT", retryable=True
        ),
        times=4,
    )

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert rig.transport.inits == []
    assert [d.decision for d in rig.store.list_retry_decisions(sid)] == [
        "retry",
        "retry",
        "retry",
        "fail",
    ]
    assert all(
        d.operation == "INIT" and d.cycle_id is None for d in rig.store.list_retry_decisions(sid)
    )
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.cycles("conv-0001") == []
    assert rig.events(EventType.CONVERSATION_STATE_CHANGED)[-1].payload == {
        "from": "ACTIVE",
        "to": "FAILED",
        "reason": "failure",
    }


# ================================================================================================
# 8. follow-up message (§11) and unexpected exceptions
# ================================================================================================
async def given_completed_reusable_session_when_follow_up_sent_then_same_conversation_continues(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    rig.executor.script(task_id="t8", stdout=b"/usr/lib/jvm/java-17")
    rig.reply(
        REMOTE_1,
        execution_plan(
            message_id="model-msg-0004",
            plan_id="plan-2",
            tasks=[cmd_task("t8", CMD_JAVA_HOME)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
        final_answer(message_id="model-msg-0005"),
    )
    rig.recorder.clear()

    resumed = await rig.manager.continue_session(sid, "Also print JAVA_HOME.")
    ended = await rig.wait(sid)

    assert resumed.status is SessionState.RUNNING and resumed.session_id == sid
    assert resumed.current_conversation_id == "conv-0001"
    assert ended.status is SessionState.COMPLETED
    assert ended.consumed_cycles == 5 and ended.consumed_plans == 3
    assert ended.final_answer == final_answer(message_id="model-msg-0005")["content"]
    assert (
        ended.user_message == USER_MESSAGE
    )  # the initial request is kept on the session (ADR-005)
    assert len(rig.transport.inits) == 1  # the remote conversation is reused
    assert rig.posted_types() == [
        "user_request",
        "execution_result",
        "execution_result",
        "user_request",
        "execution_result",
    ]
    follow_up = rig.posted(3)
    assert follow_up["conversation_id"] == REMOTE_1
    assert follow_up["message_id"] == "msg-0004"
    assert follow_up["content"]["user_message"] == "Also print JAVA_HOME."
    assert follow_up["content"]["goal"] == GOAL
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.final_answer_received is True
    assert conversation.last_completed_plan_id == "plan-2"
    cycles = rig.cycles("conv-0001")
    assert [c.cycle_type for c in cycles][3:] == [CycleType.EXECUTION, CycleType.EXECUTION]
    assert [c.status for c in cycles] == [CycleState.COMPLETED] * 5
    assert rig.conversations(sid) == [conversation]
    assert _non_execution_kinds(rig)[:4] == [
        ("session.state_changed", "RUNNING"),
        ("cycle.started", None),
        ("budget.updated", None),
        ("conversation.state_changed", "WAITING_MODEL_RESPONSE"),
    ]
    transitions = rig.events(EventType.CONVERSATION_STATE_CHANGED)
    assert transitions[0].payload == {
        "from": "WAITING_USER",
        "to": "WAITING_MODEL_RESPONSE",
        "reason": "user_request",
    }
    assert rig.events(EventType.SESSION_STATE_CHANGED)[0].payload == {
        "from": "COMPLETED",
        "to": "RUNNING",
        "reason": "user_request",
    }
    assert rig.app.audit.verify(sid).valid is True


async def given_follow_up_answered_by_final_answer_when_received_then_completed_again(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    rig.reply(REMOTE_1, final_answer(message_id="model-msg-0004", evidence=False))

    await rig.manager.continue_session(sid, "Thanks, summarise.")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.consumed_cycles == 4 and ended.consumed_plans == 2
    assert (
        ended.final_answer == final_answer(message_id="model-msg-0004", evidence=False)["content"]
    )
    assert rig.posted_types()[-1] == "user_request"


async def given_failed_or_unknown_session_when_follow_up_sent_then_refused(rig: Rig) -> None:
    rig.reply(REMOTE_1, final_answer())  # protocol error -> FAILED
    session = await rig.run()
    assert session.status is SessionState.FAILED

    with pytest.raises(ValueError):
        await rig.manager.continue_session(session.session_id, "again")
    with pytest.raises(KeyError):
        await rig.manager.continue_session("sess-unknown", "again")
    with pytest.raises(KeyError):
        await rig.manager.interrupt("sess-unknown")


async def given_unexpected_exception_in_loop_when_raised_then_system_error_recorded_session_failed_and_reraised(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig.reply(REMOTE_1, discovery_plan(tasks=[cmd_task("t1")]))

    async def explode(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("runner exploded")

    monkeypatch.setattr(rig.app.plan_runner, "run", explode)
    session = await rig.start()
    sid = session.session_id

    with pytest.raises(RuntimeError, match="runner exploded"):
        await rig.wait(sid)

    assert rig.session(sid).status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.SYSTEM_ERROR, "UNHANDLED_EXCEPTION")
    ]
    assert failures[0].details["type"] == "RuntimeError"
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "failure"
    assert rig.app.interruption.token_for(sid).is_cancelled is False


# ================================================================================================
# 9. hygiene
# ================================================================================================
def given_orchestration_package_when_imported_then_phase9_components_exported() -> None:
    import agentic_local_app.orchestration as orchestration

    for name in (
        "Application",
        "ConversationManager",
        "ProtocolOrchestrator",
        "RecoveryAction",
        "RecoveryCoordinator",
        "RecoveryReport",
        "build_application",
    ):
        assert name in orchestration.__all__
        assert getattr(orchestration, name) is not None


async def given_application_when_closed_then_store_closed_and_manager_idle(rig: Rig) -> None:
    rig.script_java_scenario()
    await rig.run()

    await rig.app.aclose()

    assert getattr(rig.store, "closed", True) is True


def given_build_application_when_production_defaults_used_then_sqlite_store_and_http_transport(
    tmp_path: Any,
) -> None:
    from agentic_local_app.execution.executor import SubprocessCommandExecutor
    from agentic_local_app.persistence.sqlite_store import SqliteConversationStore
    from agentic_local_app.transport.gateway import HttpTransportGateway

    config = make_config(str(tmp_path / "data"))
    app = build_application(config)
    try:
        assert isinstance(app.store, SqliteConversationStore)
        assert isinstance(app.transport, HttpTransportGateway)
        assert isinstance(app.executor, SubprocessCommandExecutor)
        assert app.manager.recovery_report is not None  # recovery ran on the empty store
        assert app.manager.recovery_report.actions == []
        assert app.instructions == render_instructions(config)
        assert (tmp_path / "data" / "agentic.db").exists()
        assert app.bus.subscriber_names == ["audit_log", "execution_tracker", "telemetry"]
    finally:
        app.close()


async def given_loop_running_when_settled_then_no_stray_tasks_after_completion(rig: Rig) -> None:
    rig.script_java_scenario()
    await rig.run()
    await settle()
    stray = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    assert stray == []


# ================================================================================================
# 10. interruption while waiting on the transport or on a backoff (§2.9, 07 §3)
# ================================================================================================
async def given_get_in_flight_when_user_interrupts_then_call_abandoned_and_session_ready(
    rig: Rig,
) -> None:
    rig.transport.hang_next("get")
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(rig.transport.wait_until_hanging(), 2.0)
    assert rig.conversation("conv-0001").status is ConversationState.WAITING_MODEL_RESPONSE

    report = await asyncio.wait_for(rig.manager.interrupt(sid), 3.0)
    ended = await rig.wait(sid)

    assert report.loop_drained is True and report.session_status is SessionState.READY
    assert ended.status is SessionState.READY
    assert rig.conversation("conv-0001").status is ConversationState.INTERRUPTED
    assert rig.cycles("conv-0001")[0].status is CycleState.INTERRUPTED
    assert rig.transport.in_flight == 0
    assert rig.store.list_failures(sid) == []  # an abandoned call is not a failure
    assert rig.events(EventType.MESSAGE_INBOUND) == []
    assert rig.events(EventType.CONVERSATION_STATE_CHANGED)[-1].payload == {
        "from": "WAITING_MODEL_RESPONSE",
        "to": "INTERRUPTED",
        "reason": "user_interrupt",
    }


async def given_backoff_in_progress_when_user_interrupts_then_wait_abandoned_at_once() -> None:
    blocked = asyncio.Event()
    entered = asyncio.Event()

    async def blocking_sleep(seconds: float) -> None:
        entered.set()
        await blocked.wait()  # only a cancellation gets out of here

    rig = make_rig(sleep=blocking_sleep)
    rig.transport.enqueue_error(
        "post",
        _transport_error(
            ErrorType.NETWORK_ERROR, "HTTP_503", operation="POST", retryable=True, http_status=503
        ),
    )
    session = await rig.start()
    sid = session.session_id
    await asyncio.wait_for(entered.wait(), 2.0)

    report = await asyncio.wait_for(rig.manager.interrupt(sid), 3.0)
    ended = await rig.wait(sid)

    assert report.loop_drained is True
    assert ended.status is SessionState.READY
    assert rig.transport.posted == []  # the retry never happened
    message = rig.store.get_message("msg-0001")
    assert message is not None and message.post_confirmed is False
    assert rig.conversation("conv-0001").status is ConversationState.INTERRUPTED
    assert [d.decision for d in rig.store.list_retry_decisions(sid)] == ["retry"]
    assert rig.cycles("conv-0001")[0].retry_count == 1
    assert rig.cycles("conv-0001")[0].status is CycleState.INTERRUPTED


# ================================================================================================
# 11. rotation edge cases (06 points ouverts n°4, ADR-014 "ack absent")
# ================================================================================================
async def given_first_request_projected_over_budget_when_sent_then_rotation_not_allowed_and_failed() -> (
    None
):
    config = make_config(
        context={"budget_bytes": 6_000, "summary_budget_bytes": 3_000},
        payload={
            "default_max_output_bytes": 2_000,
            "hard_max_output_bytes": 2_000,
            "max_message_bytes": 3_000,
        },
    )
    rig = make_rig(config, instructions=TINY_INSTRUCTIONS)

    session = await rig.run(user_message="x" * 6_500)
    sid = session.session_id

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.ROTATION_FAILED, "ROTATION_NOT_ALLOWED")
    ]
    assert failures[0].details["conversation_state"] == "ACTIVE"
    assert (
        rig.transport.posted == []
    )  # never sent (ADR-013: the message is not sent when saturated)
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert conversation.context_window_state is ContextWindowState.SATURATED
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    assert session.consumed_cycles == 1
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "rotation_failed"


async def given_rotation_ack_refused_when_rotating_then_parent_and_child_failed(rig: Rig) -> None:
    rig.transport.enqueue_error(
        "get",
        _transport_error(
            ErrorType.MODEL_CONTEXT_WINDOW_ERROR,
            "HTTP_413",
            operation="GET",
            retryable=False,
            http_status=413,
        ),
    )
    rig.reply(REMOTE_2, resume_ack(acknowledged=False))

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    parent, child = rig.conversations(sid)
    assert parent.status is ConversationState.FAILED
    assert child.status is ConversationState.FAILED
    assert child.protocol_error_count == 1
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413"),
        (ErrorType.MODEL_PROTOCOL_ERROR, "ACK_NOT_ACKNOWLEDGED"),
    ]
    assert rig.events(EventType.MESSAGE_REJECTED)[0].conversation_id == "conv-0002"
    assert rig.events(EventType.ROTATION_COMPLETED) == []
    assert sorted(rig.transport.closed) == [REMOTE_1, REMOTE_2]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "rotation_failed"
    assert rig.session(sid).rotations_count == 1  # the child was created before the ack


async def given_saturation_by_ratio_after_post_when_next_message_ready_then_rotation_before_it() -> (
    None
):
    """ADR-013 §3: the ratio saturates the window right after a POST; the rotation waits for the
    next outbound message (never during a plan, ADR-013 §4) and happens before it is sent."""
    config = make_config(
        context={"budget_bytes": 6_000, "summary_budget_bytes": 3_000, "saturation_ratio": 0.5},
        payload={
            "default_max_output_bytes": 2_000,
            "hard_max_output_bytes": 2_000,
            "max_message_bytes": 3_000,
        },
    )
    rig = make_rig(config, instructions=TINY_INSTRUCTIONS)
    rig.executor.script(task_id="t1", stdout=b"y" * 1_900)
    rig.executor.script(task_id="t2", stdout=b"z" * 100)
    rig.reply(
        REMOTE_1,
        discovery_plan(tasks=[cmd_task("t1", continue_on_error=True)]),
        execution_plan(
            tasks=[cmd_task("t2", continue_on_error=True)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
    )
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0004"))

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.COMPLETED and session.rotations_count == 1
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.conversation_id, e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("conv-0001", "SATURATED", "threshold"),
        ("conv-0002", "HEALTHY", "resume_acknowledged"),
    ]
    assert rig.posted_types() == [
        "user_request",
        "execution_result",
        "context_resume_request",
        "execution_result",
    ]
    assert rig.posted(1)["content"]["plan_id"] == "plan-0"  # sent in the parent (fitted)
    assert rig.posted(3)["content"]["plan_id"] == "plan-1"  # retransmitted in the child
    assert rig.plan(sid, "plan-1").conversation_id == "conv-0001"  # executed in the parent
    kinds = rig.event_kinds()
    assert kinds.index(("plan.state_changed", "COMPLETED")) < kinds.index(
        ("rotation.started", None)
    )


# ================================================================================================
# 12. wiring options
# ================================================================================================
def given_telemetry_disabled_when_application_built_then_telemetry_not_subscribed() -> None:
    from agentic_local_app.config import TelemetrySection

    config = make_config().model_copy(update={"telemetry": TelemetrySection(enabled=False)})
    rig = make_rig(config)
    assert "telemetry" not in rig.app.bus.subscriber_names
    assert rig.app.bus.subscriber_names.index("audit_log") < rig.app.bus.subscriber_names.index(
        "execution_tracker"
    )


async def given_session_without_loop_when_waited_then_record_returned_at_once(rig: Rig) -> None:
    budget = SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1)
    session = rig.app.lifecycle.create_session(GOAL, USER_MESSAGE, "u", budget, False)
    assert (await rig.manager.wait(session.session_id)).status is SessionState.READY
    assert rig.manager.loop_task(session.session_id) is None


async def given_interrupt_before_first_loop_tick_when_loop_starts_then_nothing_sent(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    session = await rig.start()
    sid = session.session_id
    # no yield to the event loop between the start and the interrupt: the loop never ran
    report = await asyncio.wait_for(rig.manager.interrupt(sid), 3.0)
    ended = await rig.wait(sid)

    assert report.session_status is SessionState.READY
    assert ended.status is SessionState.READY
    assert rig.transport.inits == [] and rig.transport.posted == []
    assert rig.conversation("conv-0001").status is ConversationState.INTERRUPTED
    assert rig.store.list_failures(sid) == []
