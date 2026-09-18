"""Phase 9 — the model answers the user directly with a ``user_response`` (ADR-022 ; spec §11,
§14 amended ; ADR-007, ADR-015).

Integration tests of ``ProtocolOrchestrator`` and ``ConversationManager`` on the doubles of §18.3
(``phase9_rig``): an analysis request answered without a single command, a ``user_response`` after
an ``execution_result``, a question (``expects_reply``) kept open even under auto-close and
answered through ``continue_session``, the strict grammar when ``protocol.allow_direct_response``
is off, the body size bound, and the reads of the façade (``user_responses``, ``last_reply``).

Sections: direct answer · after a plan · question and reply · strict flag · oversized body ·
façade reads.
"""

from __future__ import annotations

import pytest

from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    CycleType,
    MessageDirection,
    MessageType,
    SessionState,
)
from agentic_local_app.protocol.adapter import render_instructions
from integration.phase9_rig import (
    ANALYSIS_BODY,
    QUESTION_BODY,
    REMOTE_1,
    Rig,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    make_rig,
    user_response,
)

pytestmark = pytest.mark.phase9

ANALYSIS_GOAL = "Explain a Java build error"
ANALYSIS_REQUEST = (
    "Maven says 'invalid target release: 21'. What does it mean? Do not run anything yet."
)


def _kinds(rig: Rig) -> list[tuple[str, str | None]]:
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


@pytest.fixture
def rig() -> Rig:
    return make_rig()


# ================================================================================================
# 1. an analysis request answered directly, without any command
# ================================================================================================
async def given_analysis_request_when_model_answers_with_user_response_then_session_completed(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response())

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert session.final_answer is None  # a user_response is never a final answer
    assert session.consumed_cycles == 1 and session.consumed_plans == 0
    assert session.ended_at == rig.clock.now() and session.last_failure_id is None
    assert rig.posted_types() == ["user_request"]
    assert rig.executor.calls == []  # not a single command
    assert rig.store.list_plans(sid) == [] and rig.tasks(sid) == []

    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.final_answer_received is True
    assert conversation.current_plan_id is None and conversation.last_completed_plan_id is None
    assert conversation.last_inbound_message_id == "model-msg-0001"
    assert conversation.get_cursor == "model-msg-0001"
    assert conversation.last_model_response_state == "received_valid"
    assert conversation.protocol_error_count == 0 and conversation.closure_reason is None
    messages = rig.store.list_messages("conv-0001")
    assert [(m.direction, m.message_type, m.validation_status) for m in messages] == [
        (MessageDirection.OUTBOUND, MessageType.USER_REQUEST, None),
        (MessageDirection.INBOUND, MessageType.USER_RESPONSE, "valid"),
    ]
    assert messages[1].payload == user_response()
    assert messages[1].cycle_id == "cyc-0001" and messages[1].received_at == rig.clock.now()
    instructions = len(render_instructions(rig.config).encode("utf-8"))
    assert conversation.context_bytes == instructions + sum(m.size_bytes for m in messages)

    cycles = rig.cycles("conv-0001")
    assert [(c.cycle_type, c.status, c.inbound_message_id, c.plan_id) for c in cycles] == [
        (CycleType.DISCOVERY, CycleState.COMPLETED, "model-msg-0001", None)
    ]
    assert rig.store.list_failures(sid) == [] and rig.transport.closed == []


async def given_analysis_request_when_answered_directly_then_event_published_and_audit_valid(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response())

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id

    assert _kinds(rig) == [
        ("session.created", None),
        ("session.state_changed", "RUNNING"),
        ("conversation.created", None),
        ("conversation.state_changed", "ACTIVE"),
        ("cycle.started", None),
        ("budget.updated", None),
        ("conversation.state_changed", "WAITING_MODEL_RESPONSE"),
        ("message.outbound", None),
        ("message.inbound", None),
        ("conversation.state_changed", "COMPLETED"),
        ("user_response.received", None),
        ("cycle.ended", None),
        ("conversation.state_changed", "WAITING_USER"),
        ("session.state_changed", "COMPLETED"),
    ]
    assert rig.events(EventType.FINAL_ANSWER_RECEIVED) == []
    received = rig.events(EventType.USER_RESPONSE_RECEIVED)
    assert len(received) == 1
    event = received[0]
    assert (event.session_id, event.conversation_id, event.cycle_id) == (
        sid,
        "conv-0001",
        "cyc-0001",
    )
    assert event.plan_id is None and event.task_id is None and event.audited is True
    assert event.payload == {
        "message_id": "model-msg-0001",
        "format": "markdown",
        "status": "completed",
        "expects_reply": False,
        "body_bytes": len(ANALYSIS_BODY.encode("utf-8")),
        "auto_close_on_final_answer": False,
        "auto_close_skipped": False,
        "consumed_cycles": 1,
        "consumed_plans": 0,
        "session_duration_ms": 0,
    }
    inbound = rig.events(EventType.MESSAGE_INBOUND)[0]
    assert inbound.payload == {
        "message_type": "user_response",
        "message_id": "model-msg-0001",
        "get_status": 200,
        "validation_status": "valid",
        "size_bytes": size_bytes(user_response()),
    }
    ended = rig.events(EventType.CYCLE_ENDED)[0]
    assert ended.payload["inbound_message_type"] == "user_response"
    assert ended.payload["status"] == "COMPLETED"
    transitions = rig.events(EventType.CONVERSATION_STATE_CHANGED)
    assert [e.payload.get("reason") for e in transitions] == [
        "user_request",
        "user_request",
        "user_response",
        "reusable",
    ]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload == {
        "from": "RUNNING",
        "to": "COMPLETED",
        "reason": "user_response",
    }
    verification = rig.app.audit.verify(sid)
    assert verification.valid is True
    assert verification.checked == len([e for e in rig.events() if e.audited])
    snapshot = rig.manager.snapshot(sid)
    assert snapshot.session.status is SessionState.COMPLETED
    assert snapshot.conversation is not None
    assert snapshot.conversation.status is ConversationState.WAITING_USER
    assert snapshot.model_interaction.last_inbound_message_type == "user_response"
    assert snapshot.plan is None and snapshot.tasks == []
    assert snapshot == rig.app.tracker.rebuild(sid)


async def given_analysis_answered_directly_when_facade_read_then_last_reply_and_responses_returned(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response())

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id

    assert rig.manager.final_answer(sid) is None
    responses = rig.manager.user_responses(sid)
    assert responses == [
        {
            "message_id": "model-msg-0001",
            "conversation_id": "conv-0001",
            "cycle_id": "cyc-0001",
            "received_at": rig.store.list_messages("conv-0001")[1].model_dump(mode="json")[
                "received_at"
            ],
            "format": "markdown",
            "body": ANALYSIS_BODY,
            "status": "completed",
            "expects_reply": False,
        }
    ]
    reply = rig.manager.last_reply(sid)
    assert reply == {
        "type": "user_response",
        "message_id": "model-msg-0001",
        "conversation_id": "conv-0001",
        "cycle_id": "cyc-0001",
        "received_at": responses[0]["received_at"],
        "content": user_response()["content"],
    }
    assert rig.manager.user_responses("sess-unknown") == []
    assert rig.manager.last_reply("sess-unknown") is None


# ================================================================================================
# 2. a user_response after an execution_result (the model concludes without a diagnosis)
# ================================================================================================
async def given_execution_result_when_model_answers_with_user_response_then_loop_ends_completed(
    rig: Rig,
) -> None:
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        discovery_plan(),
        user_response(message_id="model-msg-0002", format="text", status="partial"),
    )

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert session.consumed_cycles == 2 and session.consumed_plans == 1
    assert session.final_answer is None
    assert rig.posted_types() == ["user_request", "execution_result"]
    cycles = rig.cycles("conv-0001")
    assert [(c.cycle_type, c.status, c.plan_id) for c in cycles] == [
        (CycleType.DISCOVERY, CycleState.COMPLETED, "plan-0"),
        (CycleType.EXECUTION, CycleState.COMPLETED, None),
    ]
    assert cycles[1].inbound_message_id == "model-msg-0002"
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.last_completed_plan_id == "plan-0"
    received = rig.events(EventType.USER_RESPONSE_RECEIVED)
    assert len(received) == 1 and received[0].cycle_id == "cyc-0002"
    assert received[0].payload["status"] == "partial"
    assert received[0].payload["format"] == "text"
    assert received[0].payload["consumed_plans"] == 1
    reply = rig.manager.last_reply(sid)
    assert reply is not None and reply["type"] == "user_response"
    assert reply["message_id"] == "model-msg-0002" and reply["cycle_id"] == "cyc-0002"
    assert rig.app.audit.verify(sid).valid is True


async def given_user_response_then_follow_up_when_model_sends_final_answer_then_last_reply_is_final(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response())
    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        execution_plan(message_id="model-msg-0002", execution_policy="sequential"),
        final_answer(message_id="model-msg-0003"),
    )

    await rig.manager.continue_session(sid, "Please confirm on this machine.")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.final_answer == final_answer(message_id="model-msg-0003")["content"]
    assert ended.consumed_cycles == 3 and ended.consumed_plans == 1
    # the follow-up user_request was answered by an execution_plan (follow-up row of ADR-007)
    assert rig.posted_types() == ["user_request", "user_request", "execution_result"]
    assert rig.cycles("conv-0001")[1].cycle_type is CycleType.EXECUTION
    reply = rig.manager.last_reply(sid)
    assert reply is not None
    assert (reply["type"], reply["message_id"]) == ("final_answer", "model-msg-0003")
    assert reply["content"] == ended.final_answer
    assert [r["message_id"] for r in rig.manager.user_responses(sid)] == ["model-msg-0001"]
    assert rig.app.audit.verify(sid).valid is True


# ================================================================================================
# 3. a question: expects_reply keeps the conversation open, even under auto-close
# ================================================================================================
async def given_question_with_auto_close_when_received_then_conversation_waits_for_user(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response(body=QUESTION_BODY, format="text", expects_reply=True))

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST, auto_close=True)

    assert session.status is SessionState.COMPLETED
    assert session.auto_close_on_final_answer is True
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER  # not CLOSED: a question
    assert conversation.closure_reason is None
    assert conversation.final_answer_received is True
    assert rig.transport.closed == []
    event = rig.events(EventType.USER_RESPONSE_RECEIVED)[0]
    assert event.payload["expects_reply"] is True
    assert event.payload["auto_close_on_final_answer"] is True
    assert event.payload["auto_close_skipped"] is True
    assert _kinds(rig)[-4:] == [
        ("user_response.received", None),
        ("cycle.ended", None),
        ("conversation.state_changed", "WAITING_USER"),
        ("session.state_changed", "COMPLETED"),
    ]


async def given_question_when_user_answers_then_follow_up_sent_and_final_answer_closes(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response(body=QUESTION_BODY, format="text", expects_reply=True))
    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST, auto_close=True)
    sid = session.session_id
    rig.reply(REMOTE_1, final_answer(message_id="model-msg-0002", evidence=False))
    rig.recorder.clear()

    resumed = await rig.manager.continue_session(sid, "Only service-api fails.")
    ended = await rig.wait(sid)

    assert resumed.status is SessionState.RUNNING
    assert ended.status is SessionState.COMPLETED
    assert ended.consumed_cycles == 2 and ended.consumed_plans == 0
    assert (
        ended.final_answer == final_answer(message_id="model-msg-0002", evidence=False)["content"]
    )
    assert len(rig.transport.inits) == 1  # the same remote conversation carried the answer
    assert rig.posted_types() == ["user_request", "user_request"]
    answer = rig.posted(1)
    assert answer["message_id"] == "msg-0002"
    assert answer["content"]["user_message"] == "Only service-api fails."
    assert answer["content"]["goal"] == ANALYSIS_GOAL
    # the follow-up row applied: a final_answer was accepted right after the user_request
    assert rig.store.list_failures(sid) == []
    # the final_answer, not the question, closes the conversation under auto-close
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.CLOSED
    assert conversation.closure_reason == "auto_close"
    assert rig.transport.closed == [REMOTE_1]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[0].payload == {
        "from": "COMPLETED",
        "to": "RUNNING",
        "reason": "user_request",
    }
    assert rig.events(EventType.CONVERSATION_STATE_CHANGED)[0].payload == {
        "from": "WAITING_USER",
        "to": "WAITING_MODEL_RESPONSE",
        "reason": "user_request",
    }
    reply = rig.manager.last_reply(sid)
    assert reply is not None and reply["type"] == "final_answer"
    assert [r["body"] for r in rig.manager.user_responses(sid)] == [QUESTION_BODY]
    with pytest.raises(ValueError):  # closed for good now
        await rig.manager.continue_session(sid, "one more")
    assert rig.app.audit.verify(sid).valid is True


async def given_question_without_auto_close_when_user_answers_then_model_may_answer_again(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response(body=QUESTION_BODY, format="text", expects_reply=True))
    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id
    rig.reply(REMOTE_1, user_response(message_id="model-msg-0002"))

    await rig.manager.continue_session(sid, "The whole project.")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED and ended.final_answer is None
    assert rig.conversation("conv-0001").status is ConversationState.WAITING_USER
    assert [r["message_id"] for r in rig.manager.user_responses(sid)] == [
        "model-msg-0001",
        "model-msg-0002",
    ]
    assert [r["expects_reply"] for r in rig.manager.user_responses(sid)] == [True, False]
    reply = rig.manager.last_reply(sid)
    assert reply is not None and reply["message_id"] == "model-msg-0002"
    assert len(rig.events(EventType.USER_RESPONSE_RECEIVED)) == 2


async def given_statement_with_auto_close_when_received_then_conversation_closed(
    rig: Rig,
) -> None:
    rig.reply(REMOTE_1, user_response())

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST, auto_close=True)

    assert session.status is SessionState.COMPLETED
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.CLOSED
    assert conversation.closure_reason == "auto_close"
    assert rig.transport.closed == [REMOTE_1]
    event = rig.events(EventType.USER_RESPONSE_RECEIVED)[0]
    assert event.payload["auto_close_skipped"] is False
    with pytest.raises(ValueError):
        await rig.manager.continue_session(session.session_id, "again")


# ================================================================================================
# 4. the strict grammar: protocol.allow_direct_response = false
# ================================================================================================
async def given_direct_response_disabled_when_initial_request_answered_directly_then_rejected() -> (
    None
):
    rig = make_rig(make_config(protocol={"allow_direct_response": False}))
    rig.reply(REMOTE_1, user_response())

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, "UNEXPECTED_MESSAGE_TYPE")
    ]
    assert failures[0].details["received"] == "user_response"
    assert failures[0].details["expected"] == ["discovery_plan"]
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    assert rejected[0].payload["message_type"] == "user_response"
    assert rig.events(EventType.USER_RESPONSE_RECEIVED) == []
    assert rig.conversation("conv-0001").status is ConversationState.FAILED
    assert rig.conversation("conv-0001").protocol_error_count == 1
    messages = rig.store.list_messages("conv-0001")
    assert [(m.message_type, m.validation_status) for m in messages] == [
        (MessageType.USER_REQUEST, None),
        (MessageType.USER_RESPONSE, "invalid"),
    ]
    assert rig.manager.user_responses(sid) == []  # a rejected response is not a response
    assert rig.manager.last_reply(sid) is None
    assert "**always** a `discovery_plan`" in rig.transport.inits[0]["instructions"]


async def given_direct_response_disabled_when_user_response_follows_execution_result_then_accepted() -> (
    None
):
    rig = make_rig(make_config(protocol={"allow_direct_response": False}))
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, discovery_plan(), user_response(message_id="model-msg-0002"))

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert rig.store.list_failures(session.session_id) == []
    assert len(rig.events(EventType.USER_RESPONSE_RECEIVED)) == 1


# ================================================================================================
# 5. the body size bound
# ================================================================================================
async def given_oversized_body_when_user_response_received_then_too_large_and_session_failed() -> (
    None
):
    rig = make_rig()
    limit = rig.config.payload.max_message_bytes
    rig.reply(REMOTE_1, user_response(body="x" * (limit + 1)))

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)
    sid = session.session_id

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, "USER_RESPONSE_TOO_LARGE")
    ]
    assert failures[0].details == {
        "size_bytes": limit + 1,
        "max_bytes": limit,
        "message_id": "model-msg-0001",
    }
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == "USER_RESPONSE_TOO_LARGE"
    assert rig.events(EventType.USER_RESPONSE_RECEIVED) == []
    assert rig.manager.last_reply(sid) is None
    assert rig.app.audit.verify(sid).valid is True


async def given_body_at_the_bound_when_user_response_received_then_accepted(rig: Rig) -> None:
    limit = rig.config.payload.max_message_bytes
    rig.reply(REMOTE_1, user_response(body="x" * limit))

    session = await rig.run(goal=ANALYSIS_GOAL, user_message=ANALYSIS_REQUEST)

    assert session.status is SessionState.COMPLETED
    event = rig.events(EventType.USER_RESPONSE_RECEIVED)[0]
    assert event.payload["body_bytes"] == limit
