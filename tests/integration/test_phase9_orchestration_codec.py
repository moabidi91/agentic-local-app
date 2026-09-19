"""Phase 9 — the protocol loop through a message codec (ADR-021).

The harness of ``phase9_rig`` is assembled with ``transport.codec = "json_text"``: the wiring wraps
the injected ``FakeTransportGateway`` in a ``CodecTransport``. The scripted model replies are
queued **as raw text** (JSON surrounded by prose and Markdown fences, or chat-completion objects)
and the outbound messages reach the fake transport in the form the codec produces (``outbound``).
The orchestrator, the rotation and the failure policy are exercised unchanged: the decorator is
transparent, and a reply the codec cannot decode is a ``MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY``
transport failure recorded like any other (never retried, rotation once in WARNING).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from agentic_local_app.config import AppConfig, TransportSection
from agentic_local_app.domain.canonical import canonical_json, size_bytes
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.domain.states import (
    ConversationState,
    CycleState,
    MessageDirection,
    MessageType,
    SessionState,
)
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import render_instructions
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.codecs import UNPARSEABLE_REPLY, CodecTransport, JsonTextCodec
from agentic_local_app.transport.fake import FakeTransportGateway
from integration.phase9_rig import (
    REMOTE_1,
    REMOTE_2,
    Rig,
    advancing_sleep,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    resume_ack,
)

pytestmark = pytest.mark.phase9

NO_JSON = "I am unable to produce a plan right now, sorry."


# ================================================================================================
# harness
# ================================================================================================
def make_codec_rig(
    codec_options: dict[str, Any] | None = None,
    *,
    config: AppConfig | None = None,
    reply_timeout_ms: int = 120_000,
) -> Rig:
    """The phase 9 rig with ``transport.codec = "json_text"`` applied by the wiring around the
    injected fake transport (``rig.transport`` stays the fake, ``rig.app.transport`` the decorator)."""
    base = config or make_config()
    cfg = base.model_copy(
        update={"transport": TransportSection(codec="json_text", codec_options=codec_options or {})}
    )
    clk = FakeClock()
    ids = SequentialIdGenerator()
    store = InMemoryConversationStore()
    fake = FakeTransportGateway(clk, reply_timeout_ms=reply_timeout_ms)
    executor = FakeCommandExecutor(clk)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="phase9-recorder")
    app = build_application(
        cfg,
        store=store,
        transport=fake,
        executor=executor,
        clock=clk,
        ids=ids,
        bus=bus,
        run_recovery=False,
        sleep=advancing_sleep(clk),
        translator=ShellTranslator(ShellDialect.POSIX),  # scripted machine, ADR-030
    )
    return Rig(
        app=app,
        config=cfg,
        clock=clk,
        ids=ids,
        store=store,
        transport=fake,
        executor=executor,
        recorder=recorder,
    )


def fenced(
    message: dict[str, Any], *, lang: str = "json", before: str = "", after: str = ""
) -> str:
    """The message as a model would render it: prose, then a Markdown fence, then prose."""
    return f"{before}```{lang}\n{json.dumps(message, indent=2)}\n```{after}"


def reply_raw(rig: Rig, remote: str, *texts: Any) -> None:
    """Queue one raw reply (a string, or a chat-completion object) per GET, in order."""
    for text in texts:
        rig.transport.enqueue_messages(remote, [text])


def script_raw_java_scenario(rig: Rig, remote: str = REMOTE_1) -> None:
    """The §12 loop rendered the way a chatty model would: fences, prose, bare JSON."""
    rig.script_spec_outputs()
    reply_raw(
        rig,
        remote,
        fenced(discovery_plan(remote), before="Sure! Here is my discovery plan:\n\n", after="\n"),
        fenced(execution_plan(remote), lang="", after="\nLet me know how it goes."),
        "Final diagnosis below.\n" + json.dumps(final_answer(remote)) + "\nThat is all.",
    )


def posted_payloads(rig: Rig) -> list[Any]:
    return [payload for _, payload in rig.transport.posted]


def posted_envelopes(rig: Rig) -> list[dict[str, Any]]:
    return [json.loads(p) if isinstance(p, str) else p for p in posted_payloads(rig)]


def chat_completion(message: dict[str, Any] | str, *, completion_id: str) -> dict[str, Any]:
    """A raw GET item shaped like a chat-completion response carrying the model's text."""
    text = message if isinstance(message, str) else fenced(message)
    return {
        "id": completion_id,
        "object": "chat.completion",
        "choices": [{"index": 0, "message": {"role": "model", "content": text}}],
    }


# ================================================================================================
# 1. the nominal loop through the codec (§2.2 ; ADR-021)
# ================================================================================================
async def given_model_replying_in_fenced_markdown_when_session_runs_then_loop_completes_through_the_codec() -> (
    None
):
    rig = make_codec_rig({"outbound": "text"})
    script_raw_java_scenario(rig)

    session = await rig.run()
    sid = session.session_id

    # the wiring decorated the injected fake with the configured codec
    assert isinstance(rig.app.transport, CodecTransport)
    assert rig.app.transport.inner is rig.transport
    assert isinstance(rig.app.transport.codec, JsonTextCodec)

    assert session.status is SessionState.COMPLETED
    assert session.consumed_cycles == 3 and session.consumed_plans == 2
    assert session.final_answer == final_answer()["content"]
    assert rig.store.list_failures(sid) == [] and rig.events(EventType.MESSAGE_REJECTED) == []

    # outbound: the fake transport saw canonical JSON text, one per protocol message
    payloads = posted_payloads(rig)
    assert all(isinstance(payload, str) for payload in payloads)
    envelopes = posted_envelopes(rig)
    assert [e["type"] for e in envelopes] == [
        "user_request",
        "execution_result",
        "execution_result",
    ]
    assert [e["message_id"] for e in envelopes] == ["msg-0001", "msg-0002", "msg-0003"]
    assert payloads == [canonical_json(e) for e in envelopes]
    assert [remote for remote, _ in rig.transport.posted] == [REMOTE_1] * 3

    # persisted messages: outbound = protocol envelopes (not the text), inbound = decoded envelopes
    messages = rig.store.list_messages("conv-0001")
    assert [(m.direction, m.message_type) for m in messages] == [
        (MessageDirection.OUTBOUND, MessageType.USER_REQUEST),
        (MessageDirection.INBOUND, MessageType.DISCOVERY_PLAN),
        (MessageDirection.OUTBOUND, MessageType.EXECUTION_RESULT),
        (MessageDirection.INBOUND, MessageType.EXECUTION_PLAN),
        (MessageDirection.OUTBOUND, MessageType.EXECUTION_RESULT),
        (MessageDirection.INBOUND, MessageType.FINAL_ANSWER),
    ]
    assert [m.payload for m in messages if m.direction is MessageDirection.OUTBOUND] == envelopes
    assert [m.payload for m in messages if m.direction is MessageDirection.INBOUND] == [
        discovery_plan(),
        execution_plan(),
        final_answer(),
    ]
    assert all(m.post_confirmed for m in messages if m.direction is MessageDirection.OUTBOUND)
    assert all(
        m.validation_status == "valid" for m in messages if m.direction is MessageDirection.INBOUND
    )
    assert all(m.size_bytes == size_bytes(m.payload) for m in messages)

    # the cursor follows the decoded envelopes; the window counts the decoded bytes (ADR-021)
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.get_cursor == "model-msg-0003"
    assert conversation.last_inbound_message_id == "model-msg-0003"
    assert conversation.protocol_error_count == 0
    instructions = len(render_instructions(rig.config).encode("utf-8"))
    assert conversation.context_bytes == instructions + sum(m.size_bytes for m in messages)
    assert all(c.status is CycleState.COMPLETED for c in rig.cycles("conv-0001"))


async def given_outbound_object_when_session_runs_then_transport_receives_the_envelope_objects() -> (
    None
):
    rig = make_codec_rig()
    script_raw_java_scenario(rig)

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert all(isinstance(payload, dict) for payload in posted_payloads(rig))
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    assert rig.posted(0)["message_id"] == "msg-0001"
    assert rig.posted(2)["content"]["plan_id"] == "plan-1"


async def given_chat_completion_items_when_content_and_id_paths_configured_then_loop_completes() -> (
    None
):
    rig = make_codec_rig({"content_path": "choices[0].message.content", "id_path": "id"})
    rig.script_spec_outputs()
    unidentified = final_answer()
    del unidentified["message_id"]  # the model forgot it: synthesised from the completion id
    reply_raw(
        rig,
        REMOTE_1,
        chat_completion(discovery_plan(), completion_id="chatcmpl-1"),
        chat_completion(execution_plan(), completion_id="chatcmpl-2"),
        chat_completion(unidentified, completion_id="chatcmpl-3"),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    inbound = [
        m for m in rig.store.list_messages("conv-0001") if m.direction is MessageDirection.INBOUND
    ]
    assert [m.message_id for m in inbound] == ["model-msg-0001", "model-msg-0002", "chatcmpl-3"]
    assert inbound[2].payload == {**unidentified, "message_id": "chatcmpl-3"}
    conversation = rig.conversation("conv-0001")
    assert conversation.get_cursor == "chatcmpl-3"
    assert conversation.last_inbound_message_id == "chatcmpl-3"
    assert all(c.status is CycleState.COMPLETED for c in rig.cycles("conv-0001"))


# ================================================================================================
# 2. an undecodable reply: MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY, never retried (§7.2)
# ================================================================================================
async def given_reply_without_json_when_received_then_session_failed_with_unparseable_reply_recorded() -> (
    None
):
    """The classification of an undecodable reply, with the correction policy of ADR-023 off
    (``max_correction_attempts = 0``): the loop applies the policy that predates it."""
    rig = make_codec_rig(config=make_config(protocol={"max_correction_attempts": 0}))
    reply_raw(rig, REMOTE_1, NO_JSON)

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, UNPARSEABLE_REPLY)
    ]
    failure = failures[0]
    assert failure.retryable is False and failure.attempt == 1
    assert failure.origin == "TransportGateway"
    assert failure.conversation_id == "conv-0001"
    assert failure.details["codec"] == "json_text"
    assert failure.details["index"] == 0
    assert failure.details["reason"] == "no_json_found"
    assert failure.details["excerpt"] == NO_JSON  # the raw reply, for the correction policy to come
    assert failure.details["operation"] == "GET" and failure.details["http_status"] == 200
    assert session.last_failure_id == failure.failure_id
    decisions = rig.store.list_retry_decisions(sid)
    assert [(d.operation, d.decision, d.delay_ms) for d in decisions] == [("GET", "fail", None)]
    assert rig.events(EventType.RETRY_SCHEDULED) == []
    assert rig.app.breaker.consecutive_failures == 0  # a protocol error never feeds the breaker

    # no envelope was decoded, but the reply is still persisted and counted: the raw excerpt
    # under the internal system_error type (ADR-021 §2 amended by ADR-023)
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == UNPARSEABLE_REPLY
    assert rejected[0].payload["message_type"] is None
    messages = rig.store.list_messages("conv-0001")
    assert [m.direction for m in messages] == [MessageDirection.OUTBOUND, MessageDirection.INBOUND]
    assert messages[1].message_type is MessageType.SYSTEM_ERROR
    assert messages[1].validation_status == "invalid"
    assert messages[1].payload == {"raw": NO_JSON, "reason": "no_json_found"}
    conversation = rig.conversation("conv-0001")
    assert conversation.status is ConversationState.FAILED
    assert conversation.protocol_error_count == 1
    assert conversation.get_cursor is None  # an unreadable reply carries no identifier
    assert conversation.last_model_response_state == "received_invalid"
    assert rig.cycles("conv-0001")[0].status is CycleState.FAILED
    assert rig.transport.closed == [REMOTE_1]
    recorded = rig.events(EventType.FAILURE_RECORDED)
    assert len(recorded) == 1 and recorded[0].payload["error_code"] == UNPARSEABLE_REPLY
    assert recorded[0].payload["details"]["excerpt"] == NO_JSON
    assert rig.event_kinds()[-3:] == [
        ("cycle.ended", None),
        ("conversation.state_changed", "FAILED"),
        ("session.state_changed", "FAILED"),
    ]


async def given_unparseable_reply_in_warning_window_when_received_then_rotation_then_completion() -> (
    None
):
    rig = make_codec_rig(
        {"outbound": "text"}, config=make_config(protocol={"max_correction_attempts": 0})
    )
    script_raw_java_scenario(rig)
    session = await rig.run()
    sid = session.session_id
    assert session.status is SessionState.COMPLETED
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation("conv-0001", context_bytes=warning_bytes + 1_000)
    rig.recorder.clear()
    reply_raw(rig, REMOTE_1, NO_JSON)  # the reply to the follow-up: undecodable, in WARNING
    reply_raw(
        rig,
        REMOTE_2,
        fenced(resume_ack(), before="Acknowledged.\n"),
        fenced(final_answer(REMOTE_2, message_id="model-msg-0010")),
    )

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    failures = rig.store.list_failures(sid)
    assert [(f.error_type, f.error_code) for f in failures] == [
        (ErrorType.MODEL_PROTOCOL_ERROR, UNPARSEABLE_REPLY)
    ]
    assert [d.decision for d in rig.store.list_retry_decisions(sid)] == ["fail"]
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("WARNING", "threshold"),
        ("SATURATED", "unusable_reply"),
        ("HEALTHY", "resume_acknowledged"),
    ]
    # every message of the rotation went through the codec too (text outbound, decoded inbound)
    assert all(isinstance(payload, str) for payload in posted_payloads(rig))
    assert [e["type"] for e in posted_envelopes(rig)][-3:] == [
        "user_request",
        "context_resume_request",
        "user_request",
    ]
    child = rig.conversation("conv-0002")
    assert child.status is ConversationState.WAITING_USER
    assert child.get_cursor == "model-msg-0010"
    assert rig.conversation("conv-0001").status is ConversationState.CLOSED
    # the undecodable reply left its raw excerpt in the parent (ADR-021 §2 amended by ADR-023)
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].conversation_id == "conv-0001"
    assert rejected[0].payload["error_code"] == UNPARSEABLE_REPLY
