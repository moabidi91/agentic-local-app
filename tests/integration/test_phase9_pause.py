"""Phase 9 — ADR-025: a 401 puts the session on hold instead of killing it.

An ``AUTHN_ERROR`` used to be a non-retryable failure like any other: the session ended ``FAILED``
and everything the user was doing — the cycles consumed, the commands already executed, the context
already paid for — was lost, for the one reason a user can fix in ten seconds. The failure policy
now answers ``pause``: the loop stops where it is, **writing nothing else**, and the session goes
``RUNNING -> PAUSED`` with ``session.paused``. The pending outbound message stays pending exactly as
the recovery path of ADR-016 expects it, so ``resume_session`` replays the refused POST, or reads
the reply that was awaited, and the session finishes as though nothing had happened.

A 403 is **not** paused (ADR-025): the credentials were accepted and the operation was refused, so
another token of the same identity changes nothing — it fails on the spot, as before.

Everything here runs on the real orchestrator, failure policy, lifecycle, persistence and audit of
``phase9_rig``; only the network, the shell, the clock and the identifiers are doubles (§18.3).
"""

from __future__ import annotations

import asyncio

import pytest

from agentic_local_app.domain.errors import ErrorType, TransportError
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    MessageDirection,
    MessageType,
    SessionState,
)
from agentic_local_app.orchestration.protocol_orchestrator import pending_outbound_of
from integration.phase9_rig import (
    FINAL_DIAGNOSIS,
    REMOTE_1,
    Rig,
    make_rig,
)

pytestmark = pytest.mark.phase9

CONV = "conv-0001"
CYCLE = "cyc-0001"
FIRST_MESSAGE = "msg-0001"


# ================================================================================================
# helpers
# ================================================================================================
def unauthorized(operation: str) -> TransportError:
    """What a provider raises on a 401 (``transport/http_base.py``): never retryable."""
    return TransportError(
        ErrorType.AUTHN_ERROR,
        "HTTP_401",
        retryable=False,
        operation=operation.upper(),
        http_status=401,
        url=f"fake://{operation}",
    )


def forbidden(operation: str) -> TransportError:
    """The same shape for a 403 — the error ADR-025 deliberately does not pause."""
    return TransportError(
        ErrorType.AUTHZ_ERROR,
        "HTTP_403",
        retryable=False,
        operation=operation.upper(),
        http_status=403,
        url=f"fake://{operation}",
    )


@pytest.fixture
def rig() -> Rig:
    return make_rig()


def audited_types(rig: Rig, session_id: str) -> list[str]:
    return [event.event_type for event in rig.store.list_audit_events(session_id)]


def paused_event(rig: Rig, index: int = 0) -> dict[str, object]:
    events = rig.events(EventType.SESSION_PAUSED)
    return dict(events[index].payload)


def pending_message(rig: Rig, session_id: str) -> object:
    conversation = rig.current_conversation(session_id)
    return pending_outbound_of(rig.store, conversation)


# ================================================================================================
# 1. pausing on a POST
# ================================================================================================
async def given_running_session_when_post_refused_with_401_then_session_paused_and_nothing_lost(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("post", unauthorized("post"))

    session = await rig.run()  # wait returns the paused session instead of raising
    sid = session.session_id

    assert session.status is SessionState.PAUSED
    assert session.ended_at is None and session.interrupted_at is None
    assert session.final_answer is None
    assert session.consumed_cycles == 1  # the cycle that was opened, counted once
    # the conversation did not move: it still waits for the model's answer
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.WAITING_MODEL_RESPONSE
    assert conversation.remote_conversation_id == REMOTE_1
    assert conversation.get_cursor is None
    assert conversation.protocol_error_count == 0
    assert conversation.last_outbound_message_id == FIRST_MESSAGE
    assert conversation.last_inbound_message_id is None
    # the message is still pending, exactly as the recovery path of ADR-016 reads it
    pending = pending_message(rig, sid)
    assert pending is not None
    assert pending.message_id == FIRST_MESSAGE
    assert pending.message_type is MessageType.USER_REQUEST
    assert pending.post_confirmed is False and pending.posted_at is None
    # the cycle is still open and the remote conversation is still open
    assert rig.cycles(CONV)[0].status is CycleState.RUNNING
    assert rig.transport.closed == []
    assert rig.transport.posted == []


async def given_post_refused_with_401_when_paused_then_failure_recorded_and_decision_persisted(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("post", unauthorized("post"))

    sid = (await rig.run()).session_id

    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [(ErrorType.AUTHN_ERROR, "HTTP_401")]
    assert failures[0].conversation_id == CONV and failures[0].retryable is False
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision, d.delay_ms) for d in decisions] == [("POST", "pause", None)]
    assert rig.events(EventType.RETRY_SCHEDULED) == []
    # the pause is not a transport outage: the breaker is left alone (§7.4)
    assert rig.app.breaker.consecutive_failures == 0
    # the session record still points at no failure: nothing ended
    assert rig.session(sid).last_failure_id is None


async def given_post_refused_with_401_when_paused_then_session_paused_event_published_and_audited(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("post", unauthorized("post"))

    sid = (await rig.run()).session_id

    assert paused_event(rig) == {
        "reason": "credentials_required",
        "error_code": "HTTP_401",
        "error_type": "AUTHN_ERROR",
        "operation": "POST",
        "message_id": FIRST_MESSAGE,
    }
    event = rig.events(EventType.SESSION_PAUSED)[0]
    assert (event.session_id, event.conversation_id, event.cycle_id) == (sid, CONV, CYCLE)
    # persisted before published (ADR-015): the state change comes first, the explanation after
    assert rig.event_kinds()[-3:] == [
        ("failure.recorded", None),
        ("session.state_changed", "PAUSED"),
        ("session.paused", None),
    ]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload == {
        "from": "RUNNING",
        "to": "PAUSED",
        "reason": "credentials_required",
    }
    assert "session.paused" in audited_types(rig, sid)
    assert rig.app.audit.verify(sid).valid is True


async def given_post_refused_with_401_when_paused_then_loop_ends_without_an_exception(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("post", unauthorized("post"))
    session = await rig.start()
    sid = session.session_id

    paused = await rig.wait(sid)

    task = rig.manager.loop_task(sid)
    assert task is not None and task.done() and not task.cancelled()
    assert task.exception() is None
    assert task.result().status is SessionState.PAUSED
    assert paused.status is SessionState.PAUSED
    assert rig.manager.get_session(sid) == paused
    assert rig.manager.snapshot(sid).session.status is SessionState.PAUSED


# ================================================================================================
# 2. pausing on a GET
# ================================================================================================
async def given_running_session_when_get_refused_with_401_then_paused_with_message_confirmed(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("get", unauthorized("get"))

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.PAUSED
    assert rig.posted_types() == ["user_request"]  # the POST went through, only the GET was refused
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.WAITING_MODEL_RESPONSE
    assert conversation.last_model_response_state == "awaiting"
    pending = pending_message(rig, sid)
    assert pending is not None
    assert pending.message_id == FIRST_MESSAGE and pending.post_confirmed is True
    assert rig.cycles(CONV)[0].status is CycleState.RUNNING
    assert rig.transport.closed == []
    assert paused_event(rig) == {
        "reason": "credentials_required",
        "error_code": "HTTP_401",
        "error_type": "AUTHN_ERROR",
        "operation": "GET",
        "message_id": FIRST_MESSAGE,
    }
    assert rig.app.audit.verify(sid).valid is True


# ================================================================================================
# 3. resuming — the point of the whole thing: the user loses nothing
# ================================================================================================
async def given_session_paused_on_post_when_token_provided_and_resumed_then_session_completes(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    rig.transport.enqueue_error("post", unauthorized("post"))
    paused = await rig.run()
    sid = paused.session_id
    assert paused.status is SessionState.PAUSED

    # the user loaded a new token and clicked send: the very same session goes on
    resumed = await rig.manager.resume_session(sid)
    assert resumed.status is SessionState.RUNNING
    session = await rig.wait(sid)

    assert session.status is SessionState.COMPLETED
    assert session.final_answer is not None
    assert session.final_answer["diagnosis"] == FINAL_DIAGNOSIS
    assert rig.manager.final_answer(sid) == session.final_answer
    # the refused message was re-POSTed under its own identifier (ADR-004), not a new one
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert rig.posted(0)["message_id"] == FIRST_MESSAGE
    assert rig.conversations(sid) == [rig.conversation(CONV)]  # no new conversation was opened
    assert rig.conversation(CONV).status is ConversationState.WAITING_USER  # reusable (§11)
    assert [c.status for c in rig.cycles(CONV)] == [CycleState.COMPLETED] * 3
    assert session.consumed_cycles == 3 and session.consumed_plans == 2
    assert [t.task_id for t in rig.tasks(sid)] == ["t1", "t2", "t3", "t4", "t5", "t6", "t7"]
    # the pause left its trace and the chain still holds across it
    assert [e.payload["to"] for e in rig.events(EventType.SESSION_STATE_CHANGED)] == [
        "RUNNING",
        "PAUSED",
        "RUNNING",
        "COMPLETED",
    ]
    assert rig.events(EventType.SESSION_STATE_CHANGED)[2].payload["reason"] == (
        "credentials_provided"
    )
    assert rig.app.audit.verify(sid).valid is True
    assert rig.manager.paused_reason(sid) is None


async def given_session_paused_on_get_when_resumed_then_awaited_reply_is_read_without_reposting(
    rig: Rig,
) -> None:
    rig.script_java_scenario()
    rig.transport.enqueue_error("get", unauthorized("get"))
    sid = (await rig.run()).session_id
    assert rig.session(sid).status is SessionState.PAUSED
    posted_before = len(rig.transport.posted)

    await rig.manager.resume_session(sid)
    session = await rig.wait(sid)

    assert session.status is SessionState.COMPLETED
    assert posted_before == 1  # the user_request was already confirmed before the pause
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert [remote for remote, _ in rig.transport.posted].count(REMOTE_1) == 3
    assert session.consumed_cycles == 3
    assert rig.app.audit.verify(sid).valid is True


async def given_session_paused_on_init_when_resumed_then_the_run_starts_again(rig: Rig) -> None:
    """Nothing had been sent yet: there is no message to replay, so the run starts from the top."""
    rig.script_java_scenario()
    rig.transport.enqueue_error("init", unauthorized("init"))

    paused = await rig.run()
    sid = paused.session_id

    assert paused.status is SessionState.PAUSED
    assert paused.consumed_cycles == 0
    assert rig.transport.inits == []
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.ACTIVE
    assert conversation.remote_conversation_id is None
    assert pending_message(rig, sid) is None
    assert paused_event(rig) == {
        "reason": "credentials_required",
        "error_code": "HTTP_401",
        "error_type": "AUTHN_ERROR",
        "operation": "INIT",
        "message_id": None,
    }

    await rig.manager.resume_session(sid)
    session = await rig.wait(sid)

    assert session.status is SessionState.COMPLETED
    assert len(rig.transport.inits) == 1
    assert rig.conversation(CONV).remote_conversation_id == REMOTE_1
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert rig.app.audit.verify(sid).valid is True


async def given_session_paused_when_resumed_without_a_valid_token_then_it_pauses_again(
    rig: Rig,
) -> None:
    """Repeated pauses are deliberately unbounded: each one costs a user gesture and nothing else.

    Nothing accumulates between them — the same cycle, the same message, the same conversation —
    so the only thing bounded is what the session budget already bounds (ADR-012).
    """
    rig.script_java_scenario()
    rig.transport.enqueue_error("post", unauthorized("post"), times=2)
    sid = (await rig.run()).session_id

    await rig.manager.resume_session(sid)
    again = await rig.wait(sid)

    assert again.status is SessionState.PAUSED
    assert len(rig.events(EventType.SESSION_PAUSED)) == 2
    assert [e.payload["to"] for e in rig.events(EventType.SESSION_STATE_CHANGED)] == [
        "RUNNING",
        "PAUSED",
        "RUNNING",
        "PAUSED",
    ]
    assert len(rig.store.list_failures(sid)) == 2
    assert [d.decision for d in rig.store.list_retry_decisions(sid)] == ["pause", "pause"]
    # nothing was duplicated by the second attempt
    assert again.consumed_cycles == 1
    assert len(rig.cycles(CONV)) == 1
    assert len(rig.store.list_messages(CONV, direction=MessageDirection.OUTBOUND)) == 1
    assert rig.transport.posted == []
    assert rig.app.audit.verify(sid).valid is True

    # the third attempt, with a token that works, still finds everything where it was
    await rig.manager.resume_session(sid)
    assert (await rig.wait(sid)).status is SessionState.COMPLETED
    assert rig.posted(0)["message_id"] == FIRST_MESSAGE


# ================================================================================================
# 4. interrupting a paused session
# ================================================================================================
async def given_paused_session_when_user_interrupts_then_ready_with_nothing_left_running(
    rig: Rig,
) -> None:
    """A paused session has no loop, no call in flight and no process: there is nothing to drain,
    so the interruption lands straight on ``READY`` without passing by ``INTERRUPTING``."""
    rig.transport.enqueue_error("get", unauthorized("get"))
    sid = (await rig.run()).session_id
    assert rig.session(sid).status is SessionState.PAUSED

    report = await asyncio.wait_for(rig.manager.interrupt(sid), 3.0)
    session = rig.session(sid)

    assert report.session_status is SessionState.READY
    assert report.nothing_to_interrupt is False
    assert report.loop_drained is True and report.within_timeout is True
    assert report.interrupted_task_ids == []
    assert (report.conversation_id, report.cycle_id) == (CONV, CYCLE)
    assert session.status is SessionState.READY
    assert rig.conversation(CONV).status is ConversationState.INTERRUPTED
    assert rig.cycles(CONV)[0].status is CycleState.INTERRUPTED
    assert rig.transport.closed == [REMOTE_1]
    assert [e.payload["to"] for e in rig.events(EventType.SESSION_STATE_CHANGED)] == [
        "RUNNING",
        "PAUSED",
        "READY",
    ]
    assert rig.manager.paused_reason(sid) is None
    assert rig.app.audit.verify(sid).valid is True
    # a READY session is usable again: the user asks something else (ADR-006 §3)
    with pytest.raises(ValueError):
        await rig.manager.resume_session(sid)


# ================================================================================================
# 5. what does not pause
# ================================================================================================
@pytest.mark.parametrize("operation", ["post", "get"])
async def given_running_session_when_refused_with_403_then_session_fails_as_before(
    rig: Rig, operation: str
) -> None:
    """ADR-025: a 403 is a permission problem, and a new token of the same identity will not fix
    it — pausing would ask the user for something that cannot help."""
    rig.transport.enqueue_error(operation, forbidden(operation))

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    assert rig.events(EventType.SESSION_PAUSED) == []
    assert rig.conversation(CONV).status is ConversationState.FAILED
    assert rig.cycles(CONV)[0].status is CycleState.FAILED
    decisions = rig.store.list_retry_decisions(sid)
    assert [d.decision for d in decisions] == ["fail"]
    assert session.last_failure_id == rig.store.list_failures(sid)[-1].failure_id
    assert rig.transport.closed == [REMOTE_1]
    assert rig.manager.paused_reason(sid) is None


# ================================================================================================
# 6. what the interface reads back
# ================================================================================================
async def given_paused_session_when_paused_reason_read_then_it_explains_the_pause(
    rig: Rig,
) -> None:
    rig.transport.enqueue_error("get", unauthorized("get"))
    sid = (await rig.run()).session_id
    paused_at = rig.session(sid).updated_at

    reason = rig.manager.paused_reason(sid)

    assert reason == {
        "reason": "credentials_required",
        "error_code": "HTTP_401",
        "error_type": "AUTHN_ERROR",
        "operation": "GET",
        "since": paused_at.isoformat().replace("+00:00", "Z"),
    }
    # nothing about a token beyond the fact that one is required
    assert "token" not in str(reason).lower()


async def given_session_that_is_not_paused_when_paused_reason_read_then_none(rig: Rig) -> None:
    rig.script_java_scenario()
    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert rig.manager.paused_reason(session.session_id) is None
    assert rig.manager.paused_reason("sess-unknown") is None


async def given_paused_session_when_a_follow_up_is_sent_then_refused_until_it_resumes(
    rig: Rig,
) -> None:
    """A pause is not a conversation waiting for the user: only ``resume_session`` restarts it."""
    rig.transport.enqueue_error("post", unauthorized("post"))
    sid = (await rig.run()).session_id

    with pytest.raises(ValueError, match="not reusable"):
        await rig.manager.continue_session(sid, "any news?")
    assert rig.session(sid).status is SessionState.PAUSED
