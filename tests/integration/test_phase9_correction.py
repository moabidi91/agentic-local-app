"""Phase 9 — the correction policy of ADR-023, end to end.

The first unusable reply used to end the session. It now opens a **correction exchange**: the
rejection is persisted and published exactly as before, then the application POSTs a
``protocol_correction_request`` (``correction.requested``) and reads again against the *same*
expectation — the ADR-007 row that was pending — at most ``protocol.max_correction_attempts``
replies in a row. A correction consumes no cycle and no plan; only the duration budget keeps
running. Once the budget of corrections is spent, the policy that predates ADR-023 applies:
rotate when the window is not ``HEALTHY`` (ADR-019 §2), fail otherwise.

Everything here runs on the real orchestrator, adapter, failure policy, persistence and audit of
``phase9_rig``; only the network, the shell, the clock and the identifiers are doubles (§18.3).
"""

from __future__ import annotations

from typing import Any

import pytest

from agentic_local_app.config import AppConfig, TransportSection
from agentic_local_app.context.window import ContextWindowMonitor
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import MessageRecord
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    MessageDirection,
    MessageType,
    SessionState,
)
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import build_application
from agentic_local_app.orchestration.protocol_orchestrator import consecutive_unusable_replies
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import CORRECTION_EXAMPLE_MESSAGE_ID, render_instructions
from agentic_local_app.protocol.messages import ProtocolCorrectionRequestContent
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.codecs import UNPARSEABLE_REPLY
from agentic_local_app.transport.fake import FakeTransportGateway
from integration.phase9_rig import (
    CMD_JAVA,
    CMD_UNAME,
    REMOTE_1,
    REMOTE_2,
    Rig,
    advancing_sleep,
    cmd_task,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    make_rig,
    resume_ack,
)

pytestmark = pytest.mark.phase9

CONV = "conv-0001"
NO_JSON = "I am unable to produce a plan right now, sorry."


# ================================================================================================
# helpers
# ================================================================================================
def rig_with(attempts: int, **overrides: Any) -> Rig:
    """A phase 9 rig whose correction budget is ``attempts`` (``0`` disables ADR-023)."""
    protocol = {"max_correction_attempts": attempts, **overrides.pop("protocol", {})}
    return make_rig(make_config(protocol=protocol, **overrides))


def codec_rig(attempts: int) -> Rig:
    """The same rig behind the ``json_text`` codec: a reply the codec cannot read is a
    ``MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY`` raised by the transport decorator (ADR-021)."""
    cfg = make_config(protocol={"max_correction_attempts": attempts}).model_copy(
        update={"transport": TransportSection(codec="json_text")}
    )
    clock = FakeClock()
    ids = SequentialIdGenerator()
    store = InMemoryConversationStore()
    transport = FakeTransportGateway(clock, reply_timeout_ms=120_000)
    executor = FakeCommandExecutor(clock)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="phase9-recorder")
    app = build_application(
        cfg,
        store=store,
        transport=transport,
        executor=executor,
        clock=clock,
        ids=ids,
        bus=bus,
        run_recovery=False,
        sleep=advancing_sleep(clock),
    )
    return Rig(
        app=app,
        config=cfg,
        clock=clock,
        ids=ids,
        store=store,
        transport=transport,
        executor=executor,
        recorder=recorder,
    )


def out_of_grammar(index: int) -> dict[str, Any]:
    """A ``final_answer`` where a ``discovery_plan`` is expected: valid content, wrong type."""
    return final_answer(message_id=f"bad-{index:04d}")


def stray_ack(index: int) -> dict[str, Any]:
    """A ``context_resume_ack`` outside any rotation: the one type no row of the table accepts
    outside a resume, so it is a fault even after a follow-up ``user_request``."""
    return resume_ack(REMOTE_1, REMOTE_1, message_id=f"stray-{index:04d}")


def messages(rig: Rig, conversation_id: str = CONV) -> list[MessageRecord]:
    return rig.store.list_messages(conversation_id)


def correction_records(rig: Rig, conversation_id: str = CONV) -> list[MessageRecord]:
    return [
        m
        for m in messages(rig, conversation_id)
        if m.message_type is MessageType.PROTOCOL_CORRECTION_REQUEST
    ]


def correction_content(record: MessageRecord) -> ProtocolCorrectionRequestContent:
    return ProtocolCorrectionRequestContent.model_validate(record.payload["content"])


def trail(rig: Rig, conversation_id: str = CONV) -> list[tuple[str, str, str | None]]:
    """``(direction, type, validation_status)`` of every message, in order."""
    return [
        (m.direction.value, m.message_type.value, m.validation_status)
        for m in messages(rig, conversation_id)
    ]


# ================================================================================================
# 1. One fault, one correction, the session goes on
# ================================================================================================
async def given_one_unusable_reply_then_a_valid_plan_when_session_runs_then_it_completes() -> None:
    """The nominal case of ADR-023: a model that is wrong once is not a lost session."""
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert session.final_answer == final_answer()["content"]
    # the correction is POSTed between the user_request and the first execution_result
    assert rig.posted_types() == [
        "user_request",
        "protocol_correction_request",
        "execution_result",
        "execution_result",
    ]
    # both the refused reply and the correction are on the persisted trail
    assert trail(rig) == [
        ("outbound", "user_request", None),
        ("inbound", "final_answer", "invalid"),
        ("outbound", "protocol_correction_request", None),
        ("inbound", "discovery_plan", "valid"),
        ("outbound", "execution_result", None),
        ("inbound", "execution_plan", "valid"),
        ("outbound", "execution_result", None),
        ("inbound", "final_answer", "valid"),
    ]
    conversation = rig.conversation(CONV)
    assert conversation.protocol_error_count == 1
    instructions = ContextWindowMonitor.instructions_bytes(render_instructions(rig.config))
    assert conversation.context_bytes == instructions + sum(m.size_bytes for m in messages(rig))
    assert rig.app.audit.verify(sid).valid is True


async def given_a_correction_when_published_then_one_audited_correction_requested_event() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())

    session = await rig.run()

    (event,) = rig.events(EventType.CORRECTION_REQUESTED)
    (record,) = correction_records(rig)
    assert event.conversation_id == CONV and event.cycle_id == record.cycle_id
    assert event.payload == {
        "message_id": record.message_id,
        "error_code": "UNEXPECTED_MESSAGE_TYPE",
        "attempt": 1,
        "max_attempts": 5,
        "expected_types": ["discovery_plan", "user_response"],
        "rejected_message_id": "bad-0001",
    }
    # the rejection is still published, exactly as before ADR-023
    (rejected,) = rig.events(EventType.MESSAGE_REJECTED)
    assert rejected.payload["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    assert rejected.payload["validation_status"] == "invalid"
    assert rig.app.audit.verify(session.session_id).valid is True


async def given_a_correction_when_built_then_it_quotes_the_fault_and_the_pending_expectation() -> (
    None
):
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())

    await rig.run()

    content = correction_content(correction_records(rig)[0])
    assert content.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert content.rejected_message_id == "bad-0001"
    assert content.attempt == 1 and content.max_attempts == 5
    # the expectation is the row that was pending, not a new one
    assert content.expected_types == ["discovery_plan", "user_response"]
    # the errors quote the adapter's own details, unchanged, so the model reads exactly what the
    # application refused and why (the counters the failure policy adds travel with them)
    (error,) = content.errors
    assert error["received"] == "final_answer"
    assert error["expected"] == ["discovery_plan", "user_response"]
    assert error["message_id"] == "bad-0001" and error["inbound"] is True
    assert content.example["type"] in content.expected_types
    assert content.example["conversation_id"] == REMOTE_1
    assert content.example["message_id"] == CORRECTION_EXAMPLE_MESSAGE_ID
    assert "discovery_plan" in content.reminder and content.raw_excerpt is None


async def given_a_correction_when_sent_then_no_cycle_and_no_plan_of_the_budget_are_spent() -> None:
    """A correction is the same turn asked again, not a new one (ADR-023 §Décision)."""
    clean = rig_with(5)
    clean.script_java_scenario()
    reference = await clean.run()

    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())
    session = await rig.run()

    assert session.status is reference.status is SessionState.COMPLETED
    assert session.consumed_cycles == reference.consumed_cycles == 3
    assert session.consumed_plans == reference.consumed_plans == 2
    assert session.rotations_count == reference.rotations_count == 0
    # the correction rides in the cycle of the message it corrects, which stays open then completes
    cycles = rig.cycles(CONV)
    assert [c.status for c in cycles] == [CycleState.COMPLETED] * 3
    assert correction_records(rig)[0].cycle_id == cycles[0].cycle_id


async def given_a_valid_reply_after_a_fault_when_a_later_reply_is_unusable_then_counting_restarts() -> (
    None
):
    """The counter is **consecutive**: any valid inbound message resets it (ADR-023)."""
    rig = rig_with(1)
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        out_of_grammar(1),  # fault 1 -> correction 1
        discovery_plan(),  # valid: the budget is back to zero
        # a discovery_plan is out of grammar after an execution_result: fault 1 again, hence a
        # second correction rather than the exhaustion of a budget of one
        discovery_plan(message_id="late-0001", plan_id="plan-late"),
        execution_plan(),
        final_answer(),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    attempts = [correction_content(r).attempt for r in correction_records(rig)]
    assert attempts == [1, 1]
    assert rig.conversation(CONV).protocol_error_count == 2


# ================================================================================================
# 2. The bound: max_correction_attempts, and the failure past it
# ================================================================================================
@pytest.mark.parametrize("attempts", [1, 2, 5])
async def given_exactly_max_correction_attempts_faults_then_a_valid_plan_when_run_then_completes(
    attempts: int,
) -> None:
    rig = rig_with(attempts)
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        *(out_of_grammar(i) for i in range(1, attempts + 1)),
        discovery_plan(),
        execution_plan(),
        final_answer(),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    records = correction_records(rig)
    assert [correction_content(r).attempt for r in records] == list(range(1, attempts + 1))
    assert {correction_content(r).max_attempts for r in records} == {attempts}
    assert rig.conversation(CONV).protocol_error_count == attempts
    assert len(rig.events(EventType.CORRECTION_REQUESTED)) == attempts


@pytest.mark.parametrize("attempts", [1, 2, 5])
async def given_one_fault_more_than_the_budget_when_run_then_session_failed_with_the_last_error(
    attempts: int,
) -> None:
    rig = rig_with(attempts)
    rig.reply(REMOTE_1, *(out_of_grammar(i) for i in range(1, attempts + 2)))

    session = await rig.run()
    sid = session.session_id

    assert session.status is SessionState.FAILED
    # exactly ``attempts`` corrections were sent, and the reply after the last one ends it
    assert len(correction_records(rig)) == attempts
    assert rig.conversation(CONV).protocol_error_count == attempts + 1
    failures = rig.store.list_failures(sid)
    last = failures[-1]
    assert last.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert last.error_code == "UNEXPECTED_MESSAGE_TYPE"
    # the details say how many corrections were attempted before giving up
    assert last.details["corrections_attempted"] == attempts
    assert last.details["max_correction_attempts"] == attempts
    assert last.details["unusable_replies"] == attempts + 1
    assert session.last_failure_id == last.failure_id
    assert rig.app.audit.verify(sid).valid is True


async def given_the_policy_disabled_when_the_first_reply_is_unusable_then_the_session_fails_at_once() -> (
    None
):
    """``max_correction_attempts = 0`` restores the behaviour that predates ADR-023."""
    rig = rig_with(0)
    rig.reply(REMOTE_1, out_of_grammar(1))

    session = await rig.run()

    assert session.status is SessionState.FAILED
    assert correction_records(rig) == []
    assert rig.events(EventType.CORRECTION_REQUESTED) == []
    assert rig.posted_types() == ["user_request"]
    (failure,) = rig.store.list_failures(session.session_id)
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    # with the policy off the details are exactly those of the fault, as before ADR-023
    assert "corrections_attempted" not in failure.details
    assert rig.conversation(CONV).protocol_error_count == 1


# ================================================================================================
# 3. A reply the codec could not read (ADR-021 §2 amended by ADR-023)
# ================================================================================================
async def given_an_unparseable_reply_when_corrected_on_the_second_try_then_session_completes() -> (
    None
):
    rig = codec_rig(5)
    rig.script_spec_outputs()
    # behind the json_text codec the model answers with TEXT: prose the codec cannot read, then
    # the same protocol messages rendered as JSON text
    rig.transport.enqueue_messages(REMOTE_1, [NO_JSON])
    for message in (discovery_plan(), execution_plan(), final_answer()):
        rig.transport.enqueue_messages(REMOTE_1, [canonical_json(message)])

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert rig.posted_types() == [
        "user_request",
        "protocol_correction_request",
        "execution_result",
        "execution_result",
    ]
    # there is no envelope, so the raw excerpt is what is persisted (ADR-021 §2 amended)
    rejected = messages(rig)[1]
    assert rejected.direction is MessageDirection.INBOUND
    assert rejected.message_type is MessageType.SYSTEM_ERROR
    assert rejected.validation_status == "invalid"
    assert rejected.payload == {"raw": NO_JSON, "reason": "no_json_found"}
    content = correction_content(correction_records(rig)[0])
    assert content.error_code == UNPARSEABLE_REPLY
    assert content.rejected_message_id is None
    assert content.raw_excerpt == NO_JSON
    assert content.expected_types == ["discovery_plan", "user_response"]
    assert rig.conversation(CONV).protocol_error_count == 1
    assert rig.app.audit.verify(session.session_id).valid is True


async def given_an_unparseable_reply_when_never_corrected_then_session_failed_after_the_budget() -> (
    None
):
    rig = codec_rig(1)
    for _ in range(2):
        rig.transport.enqueue_messages(REMOTE_1, [NO_JSON])

    session = await rig.run()

    assert session.status is SessionState.FAILED
    assert len(correction_records(rig)) == 1
    assert rig.conversation(CONV).protocol_error_count == 2
    last = rig.store.list_failures(session.session_id)[-1]
    assert last.error_code == UNPARSEABLE_REPLY
    assert last.details["corrections_attempted"] == 1


# ================================================================================================
# 4. Ordering with the rotation (ADR-023 §Décision, ADR-019 §2)
# ================================================================================================
async def given_a_warning_window_when_a_reply_is_unusable_then_it_is_corrected_before_any_rotation() -> (
    None
):
    """A ``WARNING`` window still has room: the correction goes first, the rotation is the fallback
    once the correction budget is spent."""
    rig = rig_with(1)
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    assert session.status is SessionState.COMPLETED
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation(CONV, context_bytes=warning_bytes + 1_000)
    rig.recorder.clear()
    # two faults for a budget of one: correction, then rotation. A context_resume_ack is the type
    # a follow-up user_request never accepts (a final_answer would be perfectly legal there).
    rig.reply(REMOTE_1, stray_ack(1), stray_ack(2))
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0010"))

    await rig.manager.continue_session(sid, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    assert len(correction_records(rig)) == 1
    assert rig.posted_types()[3:] == [
        "user_request",  # the follow-up
        "protocol_correction_request",  # the correction comes first
        "context_resume_request",  # only then the rotation
        "user_request",  # and the retransmission of the pending message
    ]
    parent, child = rig.conversations(sid)
    assert parent.protocol_error_count == 2
    assert parent.context_window_state is ContextWindowState.SATURATED
    assert child.status is ConversationState.WAITING_USER
    windows = rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    assert [(e.payload["to"], e.payload.get("reason")) for e in windows] == [
        ("WARNING", "threshold"),
        ("SATURATED", "unusable_reply"),
        ("HEALTHY", "resume_acknowledged"),
    ]


async def given_a_saturated_window_when_a_reply_is_unusable_then_it_rotates_without_correcting() -> (
    None
):
    """Correcting inside a full context is pointless: the correction itself would not fit."""
    rig = rig_with(5)
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    saturation_bytes = rig.app.monitor.thresholds().saturation_bytes
    rig.app.lifecycle.update_conversation(CONV, context_bytes=saturation_bytes + 1_000)
    rig.recorder.clear()
    rig.reply(REMOTE_1, stray_ack(1))
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0010"))

    await rig.manager.continue_session(sid, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    assert correction_records(rig) == []
    assert rig.events(EventType.CORRECTION_REQUESTED) == []
    # a saturated window is caught before the follow-up is even posted: the rotation comes first
    # and the pending message is retransmitted in the child, with no correction anywhere
    assert rig.posted_types()[3:] == ["context_resume_request", "user_request"]


async def given_a_rotation_after_a_fault_when_the_child_starts_then_the_correction_budget_is_fresh() -> (
    None
):
    """The child re-read the instructions in a fresh context: it is a new exchange, so the counter
    starts at zero there (ADR-023 §Décision, "portée du compteur")."""
    rig = rig_with(1)
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation(CONV, context_bytes=warning_bytes + 1_000)
    rig.reply(REMOTE_1, stray_ack(1), stray_ack(2))
    # the child gets its own budget: one fault is corrected there too
    rig.reply(
        REMOTE_2,
        resume_ack(),
        resume_ack(REMOTE_2, REMOTE_2, message_id="stray-0003"),  # a fault in the child
        # fresh plan and task identifiers: they are unique for the whole session (ADR-019 §1)
        discovery_plan(
            REMOTE_2,
            message_id="model-msg-0011",
            plan_id="plan-10",
            tasks=[cmd_task("t90", CMD_UNAME)],
        ),
    )
    rig.reply(
        REMOTE_2,
        execution_plan(
            REMOTE_2,
            message_id="model-msg-0012",
            plan_id="plan-11",
            tasks=[cmd_task("t91", CMD_JAVA)],
        ),
    )
    rig.reply(REMOTE_2, final_answer(REMOTE_2, message_id="model-msg-0013"))

    await rig.manager.continue_session(sid, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    parent, child = rig.conversations(sid)
    assert [correction_content(r).attempt for r in correction_records(rig, parent.conversation_id)]
    assert [
        correction_content(r).attempt for r in correction_records(rig, child.conversation_id)
    ] == [1]
    assert child.protocol_error_count == 1


# ================================================================================================
# 5. The counter is derived from what is persisted (no new column, schema v1)
# ================================================================================================
async def given_a_trail_of_faults_and_corrections_when_counted_then_the_stored_trail_is_enough() -> (
    None
):
    rig = rig_with(2)
    rig.reply(REMOTE_1, out_of_grammar(1), out_of_grammar(2), out_of_grammar(3))

    session = await rig.run()
    conversation = rig.conversation(CONV)

    assert session.status is SessionState.FAILED
    # the three refusals and the two corrections that answer them, still on the trail afterwards
    assert consecutive_unusable_replies(rig.store, conversation) == 3
    assert trail(rig) == [
        ("outbound", "user_request", None),
        ("inbound", "final_answer", "invalid"),
        ("outbound", "protocol_correction_request", None),
        ("inbound", "final_answer", "invalid"),
        ("outbound", "protocol_correction_request", None),
        ("inbound", "final_answer", "invalid"),
    ]


async def given_a_valid_reply_at_the_end_of_the_trail_when_counted_then_zero() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())

    session = await rig.run()

    assert consecutive_unusable_replies(rig.store, rig.conversation(CONV)) == 0
    assert session.status is SessionState.COMPLETED


# ================================================================================================
# 6. A correction stays under payload.max_message_bytes (ADR-010)
# ================================================================================================
async def given_a_fault_whose_correction_would_overflow_when_sent_then_it_is_truncated() -> None:
    """The correction is fitted like every outbound message: shrunk, never over the bound."""
    limit = 4_096
    rig = rig_with(
        1,
        payload={
            "max_message_bytes": limit,
            "hard_max_output_bytes": limit,  # the cross-section rule: hard_max <= max_message
            "default_max_output_bytes": 1_024,
            "max_state_summary_bytes": 1_024,
        },
        # the default context budget is kept: the protocol instructions alone are ~30 kB, so a
        # small budget would saturate the window and rotate instead of correcting
        context={"summary_budget_bytes": 1_024},  # summary_budget <= max_message
    )
    # a schema failure on a plan with many bad tasks: the error list alone is far over the bound
    bad_plan = discovery_plan(
        message_id="bad-0001",
        tasks=[{"task_id": f"t{i}", "type": "cmd"} for i in range(60)],
    )
    rig.reply(REMOTE_1, bad_plan)

    session = await rig.run()

    (record,) = correction_records(rig)
    assert record.size_bytes <= limit
    content = correction_content(record)
    # what identifies the fault always survives the shrinking
    assert content.error_code == "SCHEMA_INVALID"
    assert content.expected_types == ["discovery_plan", "user_response"]
    assert content.attempt == 1 and content.max_attempts == 1
    assert session.status is SessionState.FAILED  # nothing valid ever came back


# ================================================================================================
# 7. What the observers see (ADR-018, ADR-023 §4)
# ================================================================================================
async def given_corrections_when_read_through_the_manager_then_they_are_listed_oldest_first() -> (
    None
):
    rig = rig_with(2)
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        out_of_grammar(1),
        out_of_grammar(2),
        discovery_plan(),
        execution_plan(),
        final_answer(),
    )

    session = await rig.run()
    listed = rig.manager.corrections(session.session_id)

    assert [item["attempt"] for item in listed] == [1, 2]
    assert [item["error_code"] for item in listed] == ["UNEXPECTED_MESSAGE_TYPE"] * 2
    assert [item["message_id"] for item in listed] == [
        r.message_id for r in correction_records(rig)
    ]
    first = listed[0]
    assert first["conversation_id"] == CONV and first["posted_at"] is not None
    assert first["size_bytes"] == correction_records(rig)[0].size_bytes
    assert set(first) >= {"errors", "expected_types", "reminder", "example", "max_attempts"}
    assert rig.manager.corrections("sess-unknown") == []


async def given_a_correction_in_flight_when_the_snapshot_is_read_then_it_shows_the_attempt() -> (
    None
):
    """The CLI must show that the loop is waiting for a corrected reply, not that it is stuck."""
    rig = rig_with(3)
    rig.reply(REMOTE_1, out_of_grammar(1), out_of_grammar(2), out_of_grammar(3), out_of_grammar(4))

    session = await rig.run()
    interaction = rig.manager.snapshot(session.session_id).model_interaction

    assert session.status is SessionState.FAILED
    # the run ended on a refused reply that followed the last correction: it is still in flight
    assert interaction.correction_attempt == 3
    assert interaction.correction_max_attempts == 3
    assert interaction.last_protocol_validation_status == "invalid"


async def given_corrections_when_telemetry_is_read_then_they_are_counted_by_error_code() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, out_of_grammar(1), discovery_plan(), execution_plan(), final_answer())

    await rig.run()
    metrics = rig.app.telemetry.render_text()

    assert 'corrections_total{error_code="UNEXPECTED_MESSAGE_TYPE"} 1' in metrics


def given_a_correction_request_when_sent_by_the_model_then_it_is_refused_like_a_user_request(
    config: AppConfig,
) -> None:
    """``protocol_correction_request`` is outbound only: the model may never send one (ADR-023)."""
    from agentic_local_app.domain.states import INBOUND_MESSAGE_TYPES, OUTBOUND_MESSAGE_TYPES

    assert MessageType.PROTOCOL_CORRECTION_REQUEST in OUTBOUND_MESSAGE_TYPES
    assert MessageType.PROTOCOL_CORRECTION_REQUEST not in INBOUND_MESSAGE_TYPES
