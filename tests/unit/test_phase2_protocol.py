"""Phase 2 — protocol layer (spec §2.2, §2.3, §2.5, §3.5, §12, §18.2 ; ADR-004, ADR-005, ADR-007,
ADR-008, ADR-009, ADR-010, ADR-011, ADR-014, ADR-017, ADR-022).

What is pinned here:

1. **building** of every outbound type (``user_request``, ``execution_result``,
   ``context_resume_request``) compared byte-for-byte, in canonical JSON, with the examples of §12;
2. **parsing** of every inbound type from the very examples of the specification (loaded from
   ``docs/spec/SPEC-v1.1.md`` so that the schemas can never drift from the text), plus the
   ``user_response`` of ADR-022 (schema, opaque body, size bound);
3. **rejection** of every malformed message: one test per ``ProtocolError`` code, details checked;
4. the **table of expected messages** of ADR-007 (four rows + "nothing outstanding"), the
   ``protocol.allow_direct_response`` flag of ADR-022 on its initial row, and the full cartesian
   product *message type × protocol state*;
5. the **projection of a plan onto records** (``PlanRecord`` / ``TaskRecord``): ADR-009 flags and the
   effective stop rule for the eight flag combinations, ADR-010 output budgets, ADR-008 timeouts,
   ADR-011 chunk fields;
6. the **protocol instructions** sent to the model at init (ADR-004): configuration values injected,
   every rule named, every embedded JSON example valid against the schemas, the first-message
   rule rendered from the ADR-022 flag.

Only the doubles of ``tests/conftest.py`` are used (no shell, network or real database, §18.3).
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app.config import (
    AppConfig,
    ExecutionSection,
    PayloadSection,
    ProtocolSection,
)
from agentic_local_app.domain.canonical import canonical_json, size_bytes
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType, ProtocolError
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    MessageRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    INBOUND_MESSAGE_TYPES,
    OUTBOUND_MESSAGE_TYPES,
    PLAN_MESSAGE_TYPES,
    ConversationState,
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
from agentic_local_app.protocol import adapter as adapter_module
from agentic_local_app.protocol.adapter import (
    EXPECTED_INBOUND,
    InboundMessage,
    OutboundMessage,
    OutboundSituation,
    ProtocolAdapter,
    expected_inbound_for,
    render_instructions,
)
from agentic_local_app.protocol.messages import (
    ContextResumeAckContent,
    Envelope,
    ExecutionResultContent,
    FinalAnswerContent,
    PlanContent,
    TaskRef,
    TaskResult,
    UserResponseContent,
    content_model_for,
)

pytestmark = pytest.mark.phase2

# =============================================================================================
# Spec examples (§12) — loaded from the specification itself
# =============================================================================================

SPEC_PATH = Path(__file__).resolve().parents[2] / "docs" / "spec" / "SPEC-v1.1.md"
_SECTION_RE = re.compile(r"^### 12\.(\d+) (\w+)\s*\n+```json\s*\n(.*?)```", re.M | re.S)


def _load_spec_examples() -> dict[str, dict[str, Any]]:
    text = SPEC_PATH.read_text(encoding="utf-8")
    examples: dict[str, dict[str, Any]] = {}
    for match in _SECTION_RE.finditer(text):
        examples[f"12.{match.group(1)}"] = json.loads(match.group(3))
    return examples


#: section number -> example message, e.g. SPEC["12.2"] is the discovery_plan of §12.2.
SPEC: dict[str, dict[str, Any]] = _load_spec_examples()

#: inbound examples: section -> expected message type
INBOUND_EXAMPLES: dict[str, MessageType] = {
    "12.2": MessageType.DISCOVERY_PLAN,
    "12.3": MessageType.EXECUTION_PLAN,
    "12.4": MessageType.PRIORITY_CLARIFICATION,
    "12.6": MessageType.EXECUTION_PLAN,
    "12.7": MessageType.FINAL_ANSWER,
    "12.9": MessageType.CONTEXT_RESUME_ACK,
}

REMOTE_ID = "conv-1001"  # the remote conversation id used by the examples of §12.1-§12.7
RESUME_REMOTE_ID = "conv-2001"  # the child conversation of §12.8 / §12.9
T0 = datetime(2026, 1, 1, tzinfo=UTC)

#: The ``user_response`` of ADR-022 (not in §12): a markdown analysis, no command.
USER_RESPONSE_EXAMPLE: dict[str, Any] = {
    "type": "user_response",
    "conversation_id": REMOTE_ID,
    "message_id": "msg-010",
    "content": {
        "format": "markdown",
        "body": "## Why the build fails\n\nThe project targets Java 21 but Maven runs on Java 17.",
        "status": "completed",
        "expects_reply": False,
    },
}


def _section_number(section: str) -> int:
    return int(section.split(".")[1])


def _type_value(message_type: MessageType) -> str:
    return message_type.value


SPEC_SECTIONS: list[str] = sorted(SPEC, key=_section_number)

AFTER_INITIAL_REQUEST = EXPECTED_INBOUND[OutboundSituation.INITIAL_USER_REQUEST]
AFTER_FOLLOW_UP_REQUEST = EXPECTED_INBOUND[OutboundSituation.FOLLOW_UP_USER_REQUEST]
AFTER_EXECUTION_RESULT = EXPECTED_INBOUND[OutboundSituation.EXECUTION_RESULT]
AFTER_RESUME_REQUEST = EXPECTED_INBOUND[OutboundSituation.CONTEXT_RESUME_REQUEST]

# =============================================================================================
# Helpers
# =============================================================================================


def _conversation(
    remote: str | None = REMOTE_ID,
    *,
    conversation_id: str = "conv-0001",
    final_answer_received: bool = False,
) -> ConversationRecord:
    return ConversationRecord(
        conversation_id=conversation_id,
        session_id="sess-0001",
        remote_conversation_id=remote,
        status=ConversationState.ACTIVE,
        auto_close_on_final_answer=False,
        final_answer_received=final_answer_received,
        created_at=T0,
        updated_at=T0,
    )


def _session() -> SessionRecord:
    return SessionRecord(
        session_id="sess-0001",
        status=SessionState.RUNNING,
        goal="g",
        user_message="m",
        user_id="u",
        auto_close_on_final_answer=False,
        budget=SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=300_000),
        created_at=T0,
        updated_at=T0,
    )


def _outbound_record(message_type: MessageType) -> MessageRecord:
    return MessageRecord(
        message_id="msg-0001",
        session_id="sess-0001",
        conversation_id="conv-0001",
        direction=MessageDirection.OUTBOUND,
        message_type=message_type,
        payload={},
        size_bytes=0,
        created_at=T0,
    )


def _message(
    message_type: MessageType | str,
    content: Mapping[str, Any],
    *,
    conversation_id: str = REMOTE_ID,
    message_id: str = "msg-100",
) -> dict[str, Any]:
    value = message_type.value if isinstance(message_type, MessageType) else message_type
    return {
        "type": value,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "content": dict(content),
    }


def _task(task_id: str, **fields: Any) -> dict[str, Any]:
    task: dict[str, Any] = {"task_id": task_id, "type": "cmd", "cmd": f"echo {task_id}"}
    task.update(fields)
    return task


def _plan(
    tasks: Iterable[Mapping[str, Any]],
    *,
    plan_id: str = "plan-x",
    policy: str = "sequential",
    message_type: MessageType = MessageType.EXECUTION_PLAN,
    message_id: str = "msg-100",
    **extra: Any,
) -> dict[str, Any]:
    content: dict[str, Any] = {
        "plan_id": plan_id,
        "objective": "objective",
        "execution_policy": policy,
        "tasks": [dict(t) for t in tasks],
    }
    content.update(extra)
    return _message(message_type, content, message_id=message_id)


def _parse(
    adapter: ProtocolAdapter,
    *raw: Mapping[str, Any],
    expected: frozenset[MessageType] = AFTER_EXECUTION_RESULT,
    conversation: ConversationRecord | None = None,
    known_message_ids: set[str] | None = None,
    known_plan_ids: set[str] | None = None,
    known_task_ids: set[str] | None = None,
    stored_output_task_ids: set[str] | None = None,
    expected_original_conversation_id: str | None = None,
) -> InboundMessage:
    return adapter.parse_inbound(
        [dict(r) for r in raw],
        expected=expected,
        conversation=conversation or _conversation(),
        known_message_ids=known_message_ids or set(),
        known_plan_ids=known_plan_ids or set(),
        known_task_ids=known_task_ids or set(),
        stored_output_task_ids={"t4"} if stored_output_task_ids is None else stored_output_task_ids,
        expected_original_conversation_id=expected_original_conversation_id,
    )


def _protocol_error(action: Callable[[], object], code: str) -> dict[str, Any]:
    """Run ``action``, assert a ``ProtocolError`` with ``code`` and return its details."""
    with pytest.raises(ProtocolError) as exc:
        action()
    error = exc.value.error
    assert error.error_code == code, f"expected {code}, got {error.error_code}: {error.details}"
    assert error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.origin == "ProtocolAdapter"
    assert error.retryable is False and error.recoverable is False
    return dict(error.details)


def _records(
    adapter: ProtocolAdapter, inbound: InboundMessage, clock: FakeClock
) -> tuple[PlanRecord, list[TaskRecord]]:
    return adapter.plan_to_records(
        inbound,
        session=_session(),
        conversation=_conversation(),
        cycle_id="cyc-0001",
        clock=clock,
    )


def _sample_message(message_type: MessageType, conversation_id: str) -> dict[str, Any]:
    """A structurally valid message of ``message_type`` addressed to ``conversation_id``."""
    by_type = {
        MessageType.USER_REQUEST: SPEC["12.1"],
        MessageType.DISCOVERY_PLAN: SPEC["12.2"],
        MessageType.EXECUTION_PLAN: SPEC["12.3"],
        MessageType.PRIORITY_CLARIFICATION: SPEC["12.4"],
        MessageType.EXECUTION_RESULT: SPEC["12.5"],
        MessageType.FINAL_ANSWER: SPEC["12.7"],
        MessageType.USER_RESPONSE: USER_RESPONSE_EXAMPLE,
        MessageType.CONTEXT_RESUME_REQUEST: SPEC["12.8"],
        MessageType.CONTEXT_RESUME_ACK: SPEC["12.9"],
        MessageType.SYSTEM_ERROR: SPEC["12.10"],
    }
    if message_type is MessageType.CHUNK_REQUEST:
        raw = _message(
            MessageType.CHUNK_REQUEST,
            {"task_id": "t-chunk-1", "ref_task_id": "t4", "byte_offset": 0, "max_bytes": 1024},
        )
    else:
        raw = copy.deepcopy(by_type[message_type])
    raw["conversation_id"] = conversation_id
    return raw


@pytest.fixture
def adapter(config: AppConfig) -> ProtocolAdapter:
    return ProtocolAdapter(config)


# =============================================================================================
# 0. The spec examples still validate (messages.py may only be extended additively)
# =============================================================================================


@pytest.mark.parametrize("section", SPEC_SECTIONS)
def given_spec_example_when_validated_against_schemas_then_accepted(section: str) -> None:
    envelope = Envelope.model_validate(SPEC[section])
    content_model_for(envelope.type).model_validate(envelope.content)


def given_spec_when_examples_loaded_then_all_ten_sections_of_paragraph_12_present() -> None:
    assert SPEC_SECTIONS == [f"12.{i}" for i in range(1, 11)]


# =============================================================================================
# 1. Outbound messages (§12.1, §12.5, §12.8 ; ADR-014, ADR-017)
# =============================================================================================


def given_conversation_with_remote_id_when_user_request_built_then_canonical_json_equals_spec_12_1(
    adapter: ProtocolAdapter,
) -> None:
    example = SPEC["12.1"]
    built = adapter.build_user_request(
        _conversation(REMOTE_ID),
        "msg-001",
        goal=example["content"]["goal"],
        user_message=example["content"]["user_message"],
        budget=SessionBudget(**example["content"]["session_budget"]),
    )
    assert isinstance(built, OutboundMessage)
    assert built.payload == example
    assert built.canonical == canonical_json(example)
    assert built.size_bytes == len(built.canonical.encode("utf-8")) == size_bytes(example)
    assert built.message_type is MessageType.USER_REQUEST
    assert built.envelope.type is MessageType.USER_REQUEST
    assert built.envelope.conversation_id == REMOTE_ID
    assert built.envelope.message_id == "msg-001"


def given_conversation_without_remote_id_when_user_request_built_then_local_id_used_as_fallback(
    adapter: ProtocolAdapter,
) -> None:
    built = adapter.build_user_request(
        _conversation(None, conversation_id="conv-0007"),
        "msg-0001",
        goal="g",
        user_message="m",
        budget=SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1),
    )
    assert built.payload["conversation_id"] == "conv-0007"
    assert built.envelope.conversation_id == "conv-0007"


def given_sequential_ids_when_user_request_built_then_message_id_reproducible(
    adapter: ProtocolAdapter, ids: SequentialIdGenerator
) -> None:
    budget = SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1)
    first = adapter.build_user_request(
        _conversation(), ids.message_id(), goal="g", user_message="m", budget=budget
    )
    second = adapter.build_user_request(
        _conversation(), ids.message_id(), goal="g", user_message="m", budget=budget
    )
    assert first.payload["message_id"] == "msg-0001"
    assert second.payload["message_id"] == "msg-0002"
    assert first.canonical == (
        '{"content":{"goal":"g","session_budget":{"max_cycles":1,"max_plans":1,'
        '"max_total_duration_ms":1},"user_message":"m"},"conversation_id":"conv-1001",'
        '"message_id":"msg-0001","type":"user_request"}'
    )


def given_unicode_goal_when_user_request_built_then_utf8_preserved_and_size_counts_bytes(
    adapter: ProtocolAdapter,
) -> None:
    built = adapter.build_user_request(
        _conversation(),
        "msg-001",
        goal="Réparer le build — été",
        user_message="m",
        budget=SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1),
    )
    assert "Réparer le build — été" in built.canonical
    assert "\\u" not in built.canonical
    assert built.size_bytes == len(built.canonical.encode("utf-8")) > len(built.canonical)


def given_execution_result_content_when_built_then_canonical_json_equals_spec_12_5_modulo_adr_fields(
    adapter: ProtocolAdapter,
) -> None:
    example = SPEC["12.5"]
    content = ExecutionResultContent.model_validate(example["content"])
    built = adapter.build_execution_result(_conversation(REMOTE_ID), "msg-005", content)
    expected = copy.deepcopy(example)
    # Two documented differences with the text of §12.5:
    # - exclude_none=True drops ``stop_reason: null`` (absence means "no stop reason");
    # - ADR-008 adds the boolean ``timed_out`` to every task result (false unless it timed out).
    del expected["content"]["stop_reason"]
    for result in expected["content"]["results"]:
        result["timed_out"] = False
    assert built.payload == expected
    assert built.canonical == canonical_json(expected)
    assert "stop_reason" not in built.payload["content"]
    assert built.size_bytes == size_bytes(expected)
    assert built.message_type is MessageType.EXECUTION_RESULT
    assert built.envelope.message_id == "msg-005"


def given_execution_result_with_stop_reason_when_built_then_stop_reason_serialised(
    adapter: ProtocolAdapter,
) -> None:
    content = ExecutionResultContent(
        plan_id="plan-1",
        status=PlanState.STOPPED_ON_FAILURE.protocol_value,
        results=[TaskResult(task_id="t7", status="failed", exit_code=1, stderr="boom")],
        skipped_tasks=[TaskRef(task_id="t8", reason="stop_plan_on_failure:t7")],
        stop_reason="stop_plan_on_failure:t7",
    )
    built = adapter.build_execution_result(_conversation(), "msg-006", content)
    assert built.payload["content"]["status"] == "stopped_on_failure"
    assert built.payload["content"]["stop_reason"] == "stop_plan_on_failure:t7"
    assert built.payload["content"]["skipped_tasks"] == [
        {"task_id": "t8", "reason": "stop_plan_on_failure:t7"}
    ]
    assert built.payload["content"]["results"][0] == {
        "task_id": "t7",
        "status": "failed",
        "exit_code": 1,
        "stdout": "",
        "stderr": "boom",
        "truncated": False,
        "timed_out": False,
    }


def given_truncated_result_with_ranges_when_built_then_adr011_fields_serialised_as_lists(
    adapter: ProtocolAdapter,
) -> None:
    result = TaskResult(
        task_id="t4",
        status="completed",
        exit_code=0,
        stdout="tail",
        truncated=True,
        original_size_bytes=48211,
        stdout_total=48211,
        stderr_total=0,
        stdout_range=(31827, 48211),
        stderr_range=(0, 0),
        max_output_bytes_applied=16384,
    )
    content = ExecutionResultContent(plan_id="plan-0", status="completed", results=[result])
    built = adapter.build_execution_result(_conversation(), "msg-005", content)
    serialised = built.payload["content"]["results"][0]
    assert serialised["stdout_range"] == [31827, 48211]
    assert serialised["stderr_range"] == [0, 0]
    assert serialised["max_output_bytes_applied"] == 16384
    assert json.loads(built.canonical) == built.payload


def given_child_conversation_when_context_resume_request_built_then_equals_spec_12_8_plus_pending(
    adapter: ProtocolAdapter,
) -> None:
    example = SPEC["12.8"]
    built = adapter.build_context_resume_request(
        _conversation(RESUME_REMOTE_ID, conversation_id="conv-0002"),
        "msg-001",
        original_conversation_id=example["content"]["original_conversation_id"],
        goal=example["content"]["goal"],
        context_summary=example["content"]["context_summary"],
        pending_message_type=MessageType.EXECUTION_RESULT,
    )
    expected = copy.deepcopy(example)
    expected["content"]["pending_message_type"] = "execution_result"
    assert built.payload == expected
    assert built.canonical == canonical_json(expected)
    assert built.size_bytes == size_bytes(expected)
    assert built.message_type is MessageType.CONTEXT_RESUME_REQUEST
    assert built.envelope.conversation_id == RESUME_REMOTE_ID


@pytest.mark.parametrize(
    "pending", [MessageType.USER_REQUEST, MessageType.EXECUTION_RESULT], ids=lambda m: m.value
)
def given_pending_message_type_when_context_resume_request_built_then_type_value_carried(
    adapter: ProtocolAdapter, pending: MessageType
) -> None:
    built = adapter.build_context_resume_request(
        _conversation(RESUME_REMOTE_ID),
        "msg-001",
        original_conversation_id=REMOTE_ID,
        goal="g",
        context_summary={"environment": {}, "findings": []},
        pending_message_type=pending,
    )
    assert built.payload["content"]["pending_message_type"] == pending.value


def given_same_inputs_when_outbound_built_twice_then_byte_identical(
    adapter: ProtocolAdapter,
) -> None:
    content = ExecutionResultContent.model_validate(SPEC["12.5"]["content"])
    first = adapter.build_execution_result(_conversation(), "msg-005", content)
    second = adapter.build_execution_result(_conversation(), "msg-005", content)
    assert first == second
    assert first.canonical == second.canonical


def given_outbound_message_when_canonical_parsed_then_round_trips_to_payload_and_envelope(
    adapter: ProtocolAdapter,
) -> None:
    built = adapter.build_user_request(
        _conversation(),
        "msg-001",
        goal="g",
        user_message="m",
        budget=SessionBudget(max_cycles=2, max_plans=3, max_total_duration_ms=4),
    )
    assert json.loads(built.canonical) == built.payload
    assert built.envelope.model_dump(mode="json") == built.payload


def given_empty_message_id_when_outbound_built_then_value_error(adapter: ProtocolAdapter) -> None:
    with pytest.raises(ValueError):
        adapter.build_user_request(
            _conversation(),
            "",
            goal="g",
            user_message="m",
            budget=SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1),
        )


# =============================================================================================
# 2. Inbound parsing of every type from the spec examples (§12.2, 12.3, 12.4, 12.6, 12.7, 12.9)
# =============================================================================================


def given_spec_12_2_discovery_plan_when_parsed_after_initial_request_then_plan_content_typed(
    adapter: ProtocolAdapter,
) -> None:
    raw = SPEC["12.2"]
    inbound = _parse(adapter, raw, expected=AFTER_INITIAL_REQUEST)
    assert isinstance(inbound, InboundMessage)
    assert inbound.message_type is MessageType.DISCOVERY_PLAN
    assert inbound.plan_type is PlanType.DISCOVERY_PLAN
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.plan_id == "plan-0"
    assert inbound.content.execution_policy is ExecutionPolicy.SEQUENTIAL
    assert [t.task_id for t in inbound.content.tasks] == ["t1", "t2", "t3", "t4", "t5"]
    assert inbound.content.tasks[4].depends_on == ["t4"]
    assert inbound.warnings == []
    assert inbound.size_bytes == size_bytes(raw) == len(canonical_json(raw).encode("utf-8"))
    assert inbound.envelope.message_id == "msg-002"
    assert inbound.envelope.conversation_id == REMOTE_ID
    assert inbound.payload == raw


def given_spec_12_3_execution_plan_when_parsed_after_result_then_parallel_workers_kept(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, SPEC["12.3"], expected=AFTER_EXECUTION_RESULT)
    assert inbound.message_type is MessageType.EXECUTION_PLAN
    assert inbound.plan_type is PlanType.EXECUTION_PLAN
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.execution_policy is ExecutionPolicy.PARALLEL
    assert inbound.content.max_parallel_workers == 2
    assert inbound.warnings == []


def given_spec_12_4_priority_clarification_when_parsed_after_result_then_plan_type_clarification(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, SPEC["12.4"], expected=AFTER_EXECUTION_RESULT)
    assert inbound.message_type is MessageType.PRIORITY_CLARIFICATION
    assert inbound.plan_type is PlanType.PRIORITY_CLARIFICATION
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.plan_id == "plan-1a"
    assert inbound.content.tasks[0].critical is True
    assert inbound.warnings == []


def given_spec_12_6_chunk_request_plan_when_parsed_with_stored_output_then_chunk_task_typed(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(
        adapter, SPEC["12.6"], expected=AFTER_EXECUTION_RESULT, stored_output_task_ids={"t4"}
    )
    assert inbound.message_type is MessageType.EXECUTION_PLAN
    assert isinstance(inbound.content, PlanContent)
    chunk = inbound.content.tasks[0]
    assert chunk.type is TaskType.CHUNK_REQUEST
    assert chunk.cmd is None
    assert chunk.ref_task_id == "t4"
    assert chunk.byte_offset == 16384 and chunk.max_bytes == 16384
    assert chunk.stream is None and chunk.effective_stream is OutputStream.STDOUT
    assert inbound.warnings == []


def given_spec_12_7_final_answer_when_parsed_after_result_then_final_answer_content_typed(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, SPEC["12.7"], expected=AFTER_EXECUTION_RESULT)
    assert inbound.message_type is MessageType.FINAL_ANSWER
    assert inbound.plan_type is None
    assert isinstance(inbound.content, FinalAnswerContent)
    assert inbound.content.status == "completed"
    assert len(inbound.content.evidence) == 5
    assert inbound.content.recommended_next_step is not None
    assert inbound.warnings == []


def given_final_answer_with_extra_fields_when_parsed_then_accepted_and_extras_kept(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.7"])
    raw["content"]["confidence"] = 0.9
    inbound = _parse(adapter, raw, expected=AFTER_EXECUTION_RESULT)
    assert isinstance(inbound.content, FinalAnswerContent)
    assert inbound.content.model_dump()["confidence"] == 0.9


def given_user_response_when_parsed_after_result_then_user_response_content_typed(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, USER_RESPONSE_EXAMPLE, expected=AFTER_EXECUTION_RESULT)
    assert inbound.message_type is MessageType.USER_RESPONSE
    assert inbound.plan_type is None
    assert isinstance(inbound.content, UserResponseContent)
    assert inbound.content.format == "markdown"
    assert inbound.content.body.startswith("## Why the build fails")
    assert inbound.content.status == "completed"
    assert inbound.content.expects_reply is False
    assert inbound.warnings == []
    assert inbound.payload == USER_RESPONSE_EXAMPLE
    assert inbound.size_bytes == size_bytes(USER_RESPONSE_EXAMPLE)
    assert content_model_for(MessageType.USER_RESPONSE) is UserResponseContent


def given_user_response_with_only_a_body_when_parsed_then_defaults_applied() -> None:
    content = UserResponseContent.model_validate({"body": "Which module fails?"})
    assert content.format == "text"
    assert content.status == "completed"
    assert content.expects_reply is False
    assert content.model_dump() == {
        "format": "text",
        "body": "Which module fails?",
        "status": "completed",
        "expects_reply": False,
    }


def given_user_response_with_json_format_when_parsed_then_body_kept_opaque_never_parsed(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(USER_RESPONSE_EXAMPLE)
    raw["content"]["format"] = "json"
    raw["content"]["body"] = "{not json at all"  # opaque: the body is a string, nothing more
    inbound = _parse(adapter, raw, expected=AFTER_EXECUTION_RESULT)
    assert isinstance(inbound.content, UserResponseContent)
    assert inbound.content.body == "{not json at all"


@pytest.mark.parametrize(
    "status", ["completed", "partial", "failed"], ids=["completed", "partial", "failed"]
)
def given_user_response_with_each_status_when_parsed_then_accepted(
    adapter: ProtocolAdapter, status: str
) -> None:
    raw = copy.deepcopy(USER_RESPONSE_EXAMPLE)
    raw["content"]["status"] = status
    inbound = _parse(adapter, raw, expected=AFTER_EXECUTION_RESULT)
    assert isinstance(inbound.content, UserResponseContent)
    assert inbound.content.status == status


def given_user_response_with_question_when_parsed_then_expects_reply_true(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(USER_RESPONSE_EXAMPLE)
    raw["content"] = {"body": "Which module fails to build?", "expects_reply": True}
    inbound = _parse(adapter, raw, expected=AFTER_EXECUTION_RESULT)
    assert isinstance(inbound.content, UserResponseContent)
    assert inbound.content.expects_reply is True and inbound.content.format == "text"


def given_spec_12_9_ack_when_parsed_after_resume_request_then_ack_content_typed(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(
        adapter,
        SPEC["12.9"],
        expected=AFTER_RESUME_REQUEST,
        conversation=_conversation(RESUME_REMOTE_ID, conversation_id="conv-0002"),
        expected_original_conversation_id=REMOTE_ID,
    )
    assert inbound.message_type is MessageType.CONTEXT_RESUME_ACK
    assert inbound.plan_type is None
    assert isinstance(inbound.content, ContextResumeAckContent)
    assert inbound.content.acknowledged is True
    assert inbound.content.original_conversation_id == REMOTE_ID
    assert inbound.warnings == []


def given_ack_when_parsed_without_expected_original_then_original_not_checked(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(
        adapter,
        SPEC["12.9"],
        expected=AFTER_RESUME_REQUEST,
        conversation=_conversation(RESUME_REMOTE_ID),
        expected_original_conversation_id=None,
    )
    assert isinstance(inbound.content, ContextResumeAckContent)


@pytest.mark.parametrize(("section", "message_type"), sorted(INBOUND_EXAMPLES.items()))
def given_each_inbound_spec_example_when_parsed_then_envelope_and_type_match(
    adapter: ProtocolAdapter, section: str, message_type: MessageType
) -> None:
    raw = SPEC[section]
    conversation = _conversation(raw["conversation_id"])
    inbound = _parse(adapter, raw, expected=frozenset({message_type}), conversation=conversation)
    assert inbound.message_type is message_type
    assert inbound.envelope.message_id == raw["message_id"]
    assert inbound.envelope.content == raw["content"]
    assert (inbound.plan_type is not None) == (message_type in PLAN_MESSAGE_TYPES)


def given_plan_without_optional_flags_when_parsed_then_flags_none_and_no_warning(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, _plan([_task("t1")]))
    assert isinstance(inbound.content, PlanContent)
    task = inbound.content.tasks[0]
    assert task.critical is None and task.continue_on_error is None
    assert task.stop_plan_on_failure is None and task.stop_plan_on_success is None
    assert inbound.warnings == []


def given_plan_with_adr_extensions_when_parsed_then_timeout_stream_default_and_summary_kept(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan(
        [
            _task("t1", timeout_ms=5000, max_output_bytes=1024),
            {
                "task_id": "t2",
                "type": "chunk_request",
                "ref_task_id": "t4",
                "stream": "stderr",
                "byte_offset": 10,
                "max_bytes": 20,
            },
        ],
        policy="parallel",
        max_parallel_workers=3,
        default_max_output_bytes=2048,
        state_summary={
            "environment": {"os": "Linux"},
            "findings": ["f1"],
            "current_state": "s",
            "next_expected_step": "n",
            "custom": True,
        },
    )
    inbound = _parse(adapter, raw)
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.tasks[0].timeout_ms == 5000
    assert inbound.content.tasks[1].effective_stream is OutputStream.STDERR
    assert inbound.content.default_max_output_bytes == 2048
    assert inbound.content.state_summary is not None
    assert inbound.content.state_summary.findings == ["f1"]
    assert inbound.content.state_summary.model_dump()["custom"] is True
    assert inbound.warnings == []


# =============================================================================================
# 3. Rejections — one test per ProtocolError code, details checked (ADR-007, §18.2)
# =============================================================================================


def given_no_message_when_parsed_then_value_error_not_protocol_error(
    adapter: ProtocolAdapter,
) -> None:
    with pytest.raises(ValueError):
        _parse(adapter)


def given_two_messages_in_one_turn_when_parsed_then_unexpected_extra_message(
    adapter: ProtocolAdapter,
) -> None:
    second = copy.deepcopy(SPEC["12.7"])
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.3"], second, expected=AFTER_EXECUTION_RESULT),
        "UNEXPECTED_EXTRA_MESSAGE",
    )
    assert details["received"] == 2
    assert details["expected"] == 1
    assert details["message_ids"] == ["msg-003", "msg-007"]
    assert details["types"] == ["execution_plan", "final_answer"]


def given_envelope_missing_message_id_when_parsed_then_schema_invalid_envelope_stage(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.3"])
    del raw["message_id"]
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "envelope"
    assert details["errors"], "pydantic errors must be reported"
    assert any(err["loc"] == "message_id" for err in details["errors"])
    assert all(set(err) == {"loc", "type", "msg"} for err in details["errors"])
    json.dumps(details)  # details must stay JSON-serialisable (FailureRecord, audit)


def given_unknown_type_string_when_parsed_then_schema_invalid_envelope_stage(
    adapter: ProtocolAdapter,
) -> None:
    raw = _message("plan_of_attack", {"plan_id": "p"})
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "envelope"
    assert any(err["loc"] == "type" for err in details["errors"])


def given_non_object_message_when_parsed_then_schema_invalid(adapter: ProtocolAdapter) -> None:
    details = _protocol_error(
        lambda: adapter.parse_inbound(
            ["not-a-message"],  # type: ignore[list-item]
            expected=AFTER_EXECUTION_RESULT,
            conversation=_conversation(),
            known_message_ids=set(),
            known_plan_ids=set(),
            known_task_ids=set(),
            stored_output_task_ids=set(),
        ),
        "SCHEMA_INVALID",
    )
    assert details["stage"] == "envelope"


def given_envelope_with_unknown_field_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.3"])
    raw["priority"] = "high"
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "envelope"
    assert any(err["loc"] == "priority" for err in details["errors"])


def given_cmd_task_without_cmd_when_parsed_then_schema_invalid_content_stage(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([{"task_id": "t1", "type": "cmd"}])
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "content"
    assert details["message_type"] == "execution_plan"
    assert any(err["loc"].startswith("content.tasks.0") for err in details["errors"])
    assert any("cmd" in err["msg"] for err in details["errors"])


def given_plan_without_tasks_when_parsed_then_schema_invalid_content_stage(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(lambda: _parse(adapter, _plan([])), "SCHEMA_INVALID")
    assert details["stage"] == "content"
    assert any(err["loc"] == "content.tasks" for err in details["errors"])


@pytest.mark.parametrize(
    "field", ["max_output_bytes", "timeout_ms"], ids=["max_output_bytes", "timeout_ms"]
)
def given_non_positive_task_limit_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter, field: str
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, _plan([_task("t1", **{field: 0})])), "SCHEMA_INVALID"
    )
    assert details["stage"] == "content"
    assert any(err["loc"] == f"content.tasks.0.{field}" for err in details["errors"])


def given_chunk_task_with_non_positive_max_bytes_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter,
) -> None:
    chunk = {"task_id": "c", "type": "chunk_request", "ref_task_id": "t4", "byte_offset": 0}
    details = _protocol_error(
        lambda: _parse(adapter, _plan([{**chunk, "max_bytes": 0}])), "SCHEMA_INVALID"
    )
    assert any(err["loc"] == "content.tasks.0.max_bytes" for err in details["errors"])


def given_parallel_plan_with_zero_workers_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1")], policy="parallel", max_parallel_workers=0)
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert any(err["loc"] == "content.max_parallel_workers" for err in details["errors"])


def given_content_with_unknown_field_when_parsed_then_schema_invalid_content_stage(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1")], notes="extra")
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "content"
    assert any(err["loc"] == "content.notes" for err in details["errors"])


def given_ack_content_missing_field_when_parsed_then_schema_invalid_content_stage(
    adapter: ProtocolAdapter,
) -> None:
    raw = _message(
        MessageType.CONTEXT_RESUME_ACK, {"acknowledged": True}, conversation_id=RESUME_REMOTE_ID
    )
    details = _protocol_error(
        lambda: _parse(
            adapter,
            raw,
            expected=AFTER_RESUME_REQUEST,
            conversation=_conversation(RESUME_REMOTE_ID),
        ),
        "SCHEMA_INVALID",
    )
    assert details["stage"] == "content"
    assert any(err["loc"] == "content.original_conversation_id" for err in details["errors"])


def given_final_answer_missing_diagnosis_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter,
) -> None:
    raw = _message(MessageType.FINAL_ANSWER, {"status": "completed"})
    details = _protocol_error(lambda: _parse(adapter, raw), "SCHEMA_INVALID")
    assert details["stage"] == "content"
    assert any(err["loc"] == "content.diagnosis" for err in details["errors"])


def given_final_answer_after_initial_request_when_parsed_then_unexpected_message_type(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.7"], expected=AFTER_INITIAL_REQUEST),
        "UNEXPECTED_MESSAGE_TYPE",
    )
    assert details["received"] == "final_answer"
    assert details["expected"] == ["discovery_plan"]
    assert details["inbound"] is True


def given_nothing_expected_when_any_message_parsed_then_unexpected_message_type(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.3"], expected=frozenset()), "UNEXPECTED_MESSAGE_TYPE"
    )
    assert details["received"] == "execution_plan"
    assert details["expected"] == []


def _user_response(content: Mapping[str, Any]) -> dict[str, Any]:
    return _message(MessageType.USER_RESPONSE, content, message_id="msg-010")


@pytest.mark.parametrize(
    ("content", "loc"),
    [
        ({"format": "markdown"}, "content.body"),
        ({"body": ""}, "content.body"),
        ({"body": "x", "format": "html"}, "content.format"),
        ({"body": "x", "status": "done"}, "content.status"),
        ({"body": "x", "expects_reply": "maybe"}, "content.expects_reply"),
        ({"body": "x", "diagnosis": "not a final_answer"}, "content.diagnosis"),
        ({"body": ["not", "a", "string"]}, "content.body"),
    ],
    ids=[
        "missing_body",
        "empty_body",
        "unknown_format",
        "unknown_status",
        "non_boolean_expects_reply",
        "extra_field",
        "body_not_a_string",
    ],
)
def given_malformed_user_response_when_parsed_then_schema_invalid(
    adapter: ProtocolAdapter, content: dict[str, Any], loc: str
) -> None:
    details = _protocol_error(lambda: _parse(adapter, _user_response(content)), "SCHEMA_INVALID")
    assert details["stage"] == "content"
    assert details["message_type"] == "user_response"
    assert details["message_id"] == "msg-010"
    assert any(err["loc"] == loc for err in details["errors"]), details["errors"]


def given_user_response_body_over_max_message_bytes_when_parsed_then_too_large(
    config: AppConfig,
) -> None:
    small = ProtocolAdapter(
        config.model_copy(
            update={
                "payload": PayloadSection(
                    max_message_bytes=64, hard_max_output_bytes=32, max_state_summary_bytes=32
                )
            }
        )
    )
    body = "é" * 40  # 40 characters, 80 UTF-8 bytes: the bound counts bytes, not characters
    details = _protocol_error(
        lambda: _parse(small, _user_response({"body": body})), "USER_RESPONSE_TOO_LARGE"
    )
    assert details == {"size_bytes": 80, "max_bytes": 64, "message_id": "msg-010"}
    # exactly at the bound: accepted (the schema check runs first, the bound is semantic)
    inbound = _parse(small, _user_response({"body": "é" * 32}))
    assert isinstance(inbound.content, UserResponseContent)


def given_user_response_body_over_bound_and_bad_format_when_parsed_then_schema_checked_first(
    config: AppConfig,
) -> None:
    small = ProtocolAdapter(
        config.model_copy(
            update={
                "payload": PayloadSection(
                    max_message_bytes=64, hard_max_output_bytes=32, max_state_summary_bytes=32
                )
            }
        )
    )
    raw = _user_response({"body": "x" * 100, "format": "pdf"})
    details = _protocol_error(lambda: _parse(small, raw), "SCHEMA_INVALID")
    assert details["stage"] == "content"


def given_message_for_other_conversation_when_parsed_then_conversation_mismatch(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.3"])
    raw["conversation_id"] = "conv-9999"
    details = _protocol_error(lambda: _parse(adapter, raw), "CONVERSATION_MISMATCH")
    assert details["received"] == "conv-9999"
    assert details["expected"] == REMOTE_ID


def given_conversation_without_remote_id_when_message_addressed_to_local_id_then_accepted(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.3"])
    raw["conversation_id"] = "conv-0001"
    inbound = _parse(adapter, raw, conversation=_conversation(None, conversation_id="conv-0001"))
    assert inbound.envelope.conversation_id == "conv-0001"


def given_already_seen_message_id_when_parsed_then_duplicate_message_id(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.3"], known_message_ids={"msg-003", "msg-001"}),
        "DUPLICATE_MESSAGE_ID",
    )
    assert details["message_id"] == "msg-003"


def given_already_seen_plan_id_when_parsed_then_duplicate_plan_id(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.3"], known_plan_ids={"plan-1"}), "DUPLICATE_PLAN_ID"
    )
    assert details["plan_id"] == "plan-1"


def given_task_id_repeated_inside_plan_when_parsed_then_duplicate_task_id_scope_plan(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1"), _task("t2"), _task("t1")])
    details = _protocol_error(lambda: _parse(adapter, raw), "DUPLICATE_TASK_ID")
    assert details["task_id"] == "t1"
    assert details["scope"] == "plan"


def given_task_id_known_in_session_when_parsed_then_duplicate_task_id_scope_session(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.3"], known_task_ids={"t7"}), "DUPLICATE_TASK_ID"
    )
    assert details["task_id"] == "t7"
    assert details["scope"] == "session"
    assert details["plan_id"] == "plan-1"


def given_task_depending_on_itself_when_parsed_then_self_dependency(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1"), _task("t2", depends_on=["t2"])])
    details = _protocol_error(lambda: _parse(adapter, raw), "SELF_DEPENDENCY")
    assert details["task_id"] == "t2"


def given_dependency_outside_plan_when_parsed_then_unknown_dependency(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1"), _task("t2", depends_on=["t1", "t99"])])
    details = _protocol_error(lambda: _parse(adapter, raw), "UNKNOWN_DEPENDENCY")
    assert details["task_id"] == "t2"
    assert details["dependency"] == "t99"


def given_dependency_on_task_of_previous_plan_when_parsed_then_unknown_dependency(
    adapter: ProtocolAdapter,
) -> None:
    # depends_on only references tasks of the current plan (ADR-007), even if the id is known
    raw = _plan([_task("t9", depends_on=["t1"])])
    details = _protocol_error(
        lambda: _parse(adapter, raw, known_task_ids={"t1"}), "UNKNOWN_DEPENDENCY"
    )
    assert details["dependency"] == "t1"


def given_cyclic_dependencies_in_parallel_plan_when_parsed_then_dependency_cycle(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan(
        [
            _task("t1", depends_on=["t3"]),
            _task("t2", depends_on=["t1"]),
            _task("t3", depends_on=["t2"]),
        ],
        policy="parallel",
        max_parallel_workers=2,
    )
    details = _protocol_error(lambda: _parse(adapter, raw), "DEPENDENCY_CYCLE")
    assert details["cycle"] == ["t1", "t3", "t2", "t1"]


def given_cycle_in_sequential_plan_when_parsed_then_dependency_cycle_reported_first(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1", depends_on=["t2"]), _task("t2", depends_on=["t1"])])
    details = _protocol_error(lambda: _parse(adapter, raw), "DEPENDENCY_CYCLE")
    assert details["cycle"] == ["t1", "t2", "t1"]


def given_forward_dependency_in_sequential_plan_when_parsed_then_forward_dependency_error(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1", depends_on=["t2"]), _task("t2")])
    details = _protocol_error(lambda: _parse(adapter, raw), "FORWARD_DEPENDENCY_IN_SEQUENTIAL")
    assert details["task_id"] == "t1"
    assert details["dependency"] == "t2"
    assert details["execution_policy"] == "sequential"


def given_forward_dependency_in_parallel_plan_when_parsed_then_accepted(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan(
        [_task("t1", depends_on=["t2"]), _task("t2")], policy="parallel", max_parallel_workers=2
    )
    inbound = _parse(adapter, raw)
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.tasks[0].depends_on == ["t2"]


def given_backward_dependencies_in_sequential_plan_when_parsed_then_accepted(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([_task("t1"), _task("t2", depends_on=["t1"]), _task("t3", depends_on=["t1", "t2"])])
    inbound = _parse(adapter, raw)
    assert inbound.warnings == []


def given_chunk_request_on_unstored_output_when_parsed_then_chunk_ref_unknown(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.6"], stored_output_task_ids=set()), "CHUNK_REF_UNKNOWN"
    )
    assert details["task_id"] == "t-chunk-1"
    assert details["ref_task_id"] == "t4"


def given_chunk_request_referencing_task_of_same_plan_when_parsed_then_chunk_ref_unknown(
    adapter: ProtocolAdapter,
) -> None:
    # an output must already exist: a task of the current plan has not run yet
    raw = _plan(
        [
            _task("t1"),
            {
                "task_id": "c1",
                "type": "chunk_request",
                "ref_task_id": "t1",
                "byte_offset": 0,
                "max_bytes": 10,
            },
        ]
    )
    details = _protocol_error(
        lambda: _parse(adapter, raw, stored_output_task_ids=set()), "CHUNK_REF_UNKNOWN"
    )
    assert details["ref_task_id"] == "t1"


def given_state_summary_over_bound_when_parsed_then_state_summary_too_large(
    adapter: ProtocolAdapter, config: AppConfig
) -> None:
    limit = config.payload.max_state_summary_bytes
    raw = _plan([_task("t1")], state_summary={"findings": ["x" * (limit + 1)]})
    details = _protocol_error(lambda: _parse(adapter, raw), "STATE_SUMMARY_TOO_LARGE")
    assert details["max_bytes"] == limit
    assert details["size_bytes"] > limit
    assert details["plan_id"] == "plan-x"


def given_state_summary_within_bound_when_parsed_then_accepted(
    adapter: ProtocolAdapter, config: AppConfig
) -> None:
    limit = config.payload.max_state_summary_bytes
    raw = _plan([_task("t1")], state_summary={"findings": ["x" * (limit // 2)]})
    inbound = _parse(adapter, raw)
    assert isinstance(inbound.content, PlanContent)
    assert inbound.content.state_summary is not None


def given_small_configured_bound_when_summary_parsed_then_bound_taken_from_config() -> None:
    small = ProtocolAdapter(AppConfig(payload=PayloadSection(max_state_summary_bytes=64)))
    raw = _plan([_task("t1")], state_summary={"findings": ["y" * 100]})
    details = _protocol_error(lambda: _parse(small, raw), "STATE_SUMMARY_TOO_LARGE")
    assert details["max_bytes"] == 64


def given_ack_not_acknowledged_when_parsed_then_ack_not_acknowledged(
    adapter: ProtocolAdapter,
) -> None:
    raw = copy.deepcopy(SPEC["12.9"])
    raw["content"]["acknowledged"] = False
    details = _protocol_error(
        lambda: _parse(
            adapter,
            raw,
            expected=AFTER_RESUME_REQUEST,
            conversation=_conversation(RESUME_REMOTE_ID),
            expected_original_conversation_id=REMOTE_ID,
        ),
        "ACK_NOT_ACKNOWLEDGED",
    )
    assert details["original_conversation_id"] == REMOTE_ID


def given_ack_for_other_original_conversation_when_parsed_then_ack_wrong_original(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(
            adapter,
            SPEC["12.9"],
            expected=AFTER_RESUME_REQUEST,
            conversation=_conversation(RESUME_REMOTE_ID),
            expected_original_conversation_id="conv-0000",
        ),
        "ACK_WRONG_ORIGINAL",
    )
    assert details["received"] == REMOTE_ID
    assert details["expected"] == "conv-0000"


def given_system_error_message_from_model_when_parsed_then_system_error_not_allowed_inbound(
    adapter: ProtocolAdapter,
) -> None:
    details = _protocol_error(
        lambda: _parse(adapter, SPEC["12.10"]), "SYSTEM_ERROR_NOT_ALLOWED_INBOUND"
    )
    assert details["received"] == "system_error"
    assert details["inbound"] is False


@pytest.mark.parametrize(
    "message_type",
    sorted(OUTBOUND_MESSAGE_TYPES | {MessageType.CHUNK_REQUEST}),
    ids=_type_value,
)
def given_non_inbound_type_as_message_when_parsed_then_unexpected_message_type_not_inbound(
    adapter: ProtocolAdapter, message_type: MessageType
) -> None:
    raw = _sample_message(message_type, REMOTE_ID)
    expected = frozenset(MessageType)  # even when "everything" is expected, direction wins
    details = _protocol_error(
        lambda: _parse(adapter, raw, expected=expected), "UNEXPECTED_MESSAGE_TYPE"
    )
    assert details["received"] == message_type.value
    assert details["inbound"] is False


def given_protocol_error_when_raised_then_normalized_error_is_model_protocol_not_retryable(
    adapter: ProtocolAdapter,
) -> None:
    with pytest.raises(ProtocolError) as exc:
        _parse(adapter, SPEC["12.7"], expected=AFTER_INITIAL_REQUEST)
    error = exc.value.error
    assert error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert error.origin == "ProtocolAdapter"
    assert error.retryable is False and error.recoverable is False
    assert "UNEXPECTED_MESSAGE_TYPE" in str(exc.value)


def given_message_with_several_defects_when_parsed_then_envelope_checked_before_content(
    adapter: ProtocolAdapter,
) -> None:
    raw = _plan([{"task_id": "t1", "type": "cmd"}])  # invalid content...
    raw["conversation_id"] = "conv-9999"  # ...and wrong conversation: the envelope wins
    _protocol_error(lambda: _parse(adapter, raw), "CONVERSATION_MISMATCH")


# =============================================================================================
# 4. Table of expected inbound messages (ADR-007)
# =============================================================================================


def given_no_outbound_message_when_expected_inbound_asked_then_nothing_expected(
    adapter: ProtocolAdapter,
) -> None:
    assert adapter.expected_inbound(None, _conversation()) == frozenset()


def given_initial_user_request_when_expected_inbound_asked_then_discovery_plan_or_direct_response(
    adapter: ProtocolAdapter,
) -> None:
    # the default configuration allows a direct user_response (ADR-022 flag on)
    expected = adapter.expected_inbound(
        _outbound_record(MessageType.USER_REQUEST), _conversation(final_answer_received=False)
    )
    assert expected == frozenset({MessageType.DISCOVERY_PLAN, MessageType.USER_RESPONSE})
    assert expected == AFTER_INITIAL_REQUEST | {MessageType.USER_RESPONSE}
    assert expected == expected_inbound_for(OutboundSituation.INITIAL_USER_REQUEST)


def given_direct_response_disabled_when_initial_expected_inbound_asked_then_only_discovery_plan(
    config: AppConfig,
) -> None:
    strict = ProtocolAdapter(
        config.model_copy(update={"protocol": ProtocolSection(allow_direct_response=False)})
    )
    expected = strict.expected_inbound(
        _outbound_record(MessageType.USER_REQUEST), _conversation(final_answer_received=False)
    )
    assert expected == frozenset({MessageType.DISCOVERY_PLAN}) == AFTER_INITIAL_REQUEST
    assert expected == expected_inbound_for(
        OutboundSituation.INITIAL_USER_REQUEST, allow_direct_response=False
    )
    # the flag only touches the initial row
    for situation in OutboundSituation:
        if situation is not OutboundSituation.INITIAL_USER_REQUEST:
            assert expected_inbound_for(situation, allow_direct_response=False) == (
                expected_inbound_for(situation, allow_direct_response=True)
            )
            assert expected_inbound_for(situation) == EXPECTED_INBOUND[situation]


def given_follow_up_user_request_when_expected_inbound_asked_then_any_plan_final_or_response(
    adapter: ProtocolAdapter,
) -> None:
    expected = adapter.expected_inbound(
        _outbound_record(MessageType.USER_REQUEST), _conversation(final_answer_received=True)
    )
    assert expected == frozenset(
        {
            MessageType.DISCOVERY_PLAN,
            MessageType.EXECUTION_PLAN,
            MessageType.PRIORITY_CLARIFICATION,
            MessageType.FINAL_ANSWER,
            MessageType.USER_RESPONSE,
        }
    )
    assert expected == AFTER_FOLLOW_UP_REQUEST


def given_execution_result_when_expected_inbound_asked_then_plan_clarification_final_or_response(
    adapter: ProtocolAdapter,
) -> None:
    expected = adapter.expected_inbound(
        _outbound_record(MessageType.EXECUTION_RESULT), _conversation()
    )
    assert expected == frozenset(
        {
            MessageType.EXECUTION_PLAN,
            MessageType.PRIORITY_CLARIFICATION,
            MessageType.FINAL_ANSWER,
            MessageType.USER_RESPONSE,
        }
    )
    assert expected == AFTER_EXECUTION_RESULT
    assert MessageType.DISCOVERY_PLAN not in expected


@pytest.mark.parametrize("allow_direct_response", [True, False], ids=["direct", "strict"])
def given_execution_result_or_follow_up_when_user_response_received_then_accepted_whatever_flag(
    config: AppConfig, allow_direct_response: bool
) -> None:
    adapter = ProtocolAdapter(
        config.model_copy(
            update={"protocol": ProtocolSection(allow_direct_response=allow_direct_response)}
        )
    )
    for record, conversation in (
        (_outbound_record(MessageType.EXECUTION_RESULT), _conversation()),
        (_outbound_record(MessageType.USER_REQUEST), _conversation(final_answer_received=True)),
    ):
        expected = adapter.expected_inbound(record, conversation)
        assert MessageType.USER_RESPONSE in expected
        inbound = _parse(adapter, USER_RESPONSE_EXAMPLE, expected=expected)
        assert inbound.message_type is MessageType.USER_RESPONSE
        assert isinstance(inbound.content, UserResponseContent)
        assert inbound.plan_type is None and inbound.warnings == []


def given_context_resume_request_when_user_response_received_then_unexpected_message_type(
    adapter: ProtocolAdapter,
) -> None:
    expected = adapter.expected_inbound(
        _outbound_record(MessageType.CONTEXT_RESUME_REQUEST), _conversation(RESUME_REMOTE_ID)
    )
    assert MessageType.USER_RESPONSE not in expected
    raw = dict(USER_RESPONSE_EXAMPLE, conversation_id=RESUME_REMOTE_ID)
    details = _protocol_error(
        lambda: _parse(
            adapter, raw, expected=expected, conversation=_conversation(RESUME_REMOTE_ID)
        ),
        "UNEXPECTED_MESSAGE_TYPE",
    )
    assert details["received"] == "user_response"
    assert details["expected"] == ["context_resume_ack"]
    assert details["inbound"] is True


def given_direct_response_disabled_when_user_response_answers_initial_request_then_rejected(
    config: AppConfig,
) -> None:
    strict = ProtocolAdapter(
        config.model_copy(update={"protocol": ProtocolSection(allow_direct_response=False)})
    )
    expected = strict.expected_inbound(_outbound_record(MessageType.USER_REQUEST), _conversation())
    details = _protocol_error(
        lambda: _parse(strict, USER_RESPONSE_EXAMPLE, expected=expected), "UNEXPECTED_MESSAGE_TYPE"
    )
    assert details["received"] == "user_response"
    assert details["expected"] == ["discovery_plan"]
    # the very same message is accepted by the default (direct) adapter
    default = ProtocolAdapter(config)
    assert (
        _parse(
            default,
            USER_RESPONSE_EXAMPLE,
            expected=default.expected_inbound(
                _outbound_record(MessageType.USER_REQUEST), _conversation()
            ),
        ).message_type
        is MessageType.USER_RESPONSE
    )


def given_execution_result_after_final_answer_when_expected_inbound_asked_then_same_row(
    adapter: ProtocolAdapter,
) -> None:
    # final_answer_received only distinguishes the two user_request rows
    assert (
        adapter.expected_inbound(
            _outbound_record(MessageType.EXECUTION_RESULT),
            _conversation(final_answer_received=True),
        )
        == AFTER_EXECUTION_RESULT
    )


def given_context_resume_request_when_expected_inbound_asked_then_only_ack(
    adapter: ProtocolAdapter,
) -> None:
    expected = adapter.expected_inbound(
        _outbound_record(MessageType.CONTEXT_RESUME_REQUEST), _conversation()
    )
    assert expected == frozenset({MessageType.CONTEXT_RESUME_ACK}) == AFTER_RESUME_REQUEST


@pytest.mark.parametrize("message_type", sorted(INBOUND_MESSAGE_TYPES), ids=_type_value)
def given_inbound_record_as_last_outbound_when_expected_inbound_asked_then_value_error(
    adapter: ProtocolAdapter, message_type: MessageType
) -> None:
    record = _outbound_record(message_type).model_copy(
        update={"direction": MessageDirection.INBOUND}
    )
    with pytest.raises(ValueError):
        adapter.expected_inbound(record, _conversation())


def given_expected_inbound_table_when_inspected_then_four_rows_only_inbound_types_and_immutable() -> (
    None
):
    assert set(EXPECTED_INBOUND) == set(OutboundSituation)
    assert len(EXPECTED_INBOUND) == 4
    for allowed in EXPECTED_INBOUND.values():
        assert isinstance(allowed, frozenset)
        assert allowed <= INBOUND_MESSAGE_TYPES
    with pytest.raises(TypeError):
        EXPECTED_INBOUND[OutboundSituation.EXECUTION_RESULT] = frozenset()  # type: ignore[index]


# =============================================================================================
# 5. Cartesian product: every message type × every protocol state (§18.2 "per protocol state")
# =============================================================================================

_SITUATIONS: list[OutboundSituation | None] = [*OutboundSituation, None]


def _conversation_for(situation: OutboundSituation | None) -> ConversationRecord:
    if situation is OutboundSituation.CONTEXT_RESUME_REQUEST:
        return _conversation(RESUME_REMOTE_ID, conversation_id="conv-0002")
    return _conversation(
        REMOTE_ID, final_answer_received=situation is OutboundSituation.FOLLOW_UP_USER_REQUEST
    )


def _expected_for(situation: OutboundSituation | None) -> frozenset[MessageType]:
    return frozenset() if situation is None else EXPECTED_INBOUND[situation]


@pytest.mark.parametrize(
    ("situation", "message_type"),
    list(product(_SITUATIONS, MessageType)),
    ids=lambda v: v.value if isinstance(v, MessageType) else (v.value if v else "nothing_pending"),
)
def given_protocol_state_when_each_message_type_received_then_accepted_iff_in_expected_table(
    adapter: ProtocolAdapter, situation: OutboundSituation | None, message_type: MessageType
) -> None:
    conversation = _conversation_for(situation)
    expected = _expected_for(situation)
    raw = _sample_message(message_type, conversation.remote_conversation_id or "")

    def parse() -> InboundMessage:
        return _parse(
            adapter,
            raw,
            expected=expected,
            conversation=conversation,
            expected_original_conversation_id=REMOTE_ID
            if message_type is MessageType.CONTEXT_RESUME_ACK
            else None,
        )

    if message_type in expected:
        assert parse().message_type is message_type
    elif message_type is MessageType.SYSTEM_ERROR:
        details = _protocol_error(parse, "SYSTEM_ERROR_NOT_ALLOWED_INBOUND")
        assert details["inbound"] is False
    else:
        details = _protocol_error(parse, "UNEXPECTED_MESSAGE_TYPE")
        assert details["received"] == message_type.value
        assert details["inbound"] is (message_type in INBOUND_MESSAGE_TYPES)
        if message_type in INBOUND_MESSAGE_TYPES:
            assert details["expected"] == sorted(m.value for m in expected)


# =============================================================================================
# 6. Plan -> records (ADR-008, ADR-009, ADR-010, ADR-011, ADR-017)
# =============================================================================================


def given_spec_12_2_plan_when_projected_then_plan_record_pending_with_counters_and_timestamps(
    adapter: ProtocolAdapter, clock: FakeClock, config: AppConfig
) -> None:
    clock.advance(1234)
    inbound = _parse(adapter, SPEC["12.2"], expected=AFTER_INITIAL_REQUEST)
    plan, tasks = _records(adapter, inbound, clock)
    assert plan == PlanRecord(
        plan_id="plan-0",
        session_id="sess-0001",
        conversation_id="conv-0001",
        cycle_id="cyc-0001",
        plan_type=PlanType.DISCOVERY_PLAN,
        objective="Discover execution environment and build context",
        execution_policy=ExecutionPolicy.SEQUENTIAL,
        max_parallel_workers=1,
        status=PlanState.PENDING,
        task_count=5,
        default_max_output_bytes=None,
        state_summary=None,
        created_at=clock.now(),
        updated_at=clock.now(),
    )
    assert [t.task_id for t in tasks] == ["t1", "t2", "t3", "t4", "t5"]
    assert [t.order_index for t in tasks] == [0, 1, 2, 3, 4]
    assert all(t.status is TaskState.PENDING for t in tasks)
    assert all(t.plan_id == "plan-0" for t in tasks)
    assert all(t.session_id == "sess-0001" and t.conversation_id == "conv-0001" for t in tasks)
    assert all(t.created_at == t.updated_at == clock.now() for t in tasks)
    assert tasks[4].depends_on == ("t4",)
    assert tasks[0].depends_on == ()
    assert tasks[3].cmd == "test -f pom.xml && sed -n '1,220p' pom.xml"
    assert [t.max_output_bytes for t in tasks] == [2048, 1024, 1024, 16384, 32768]
    assert [t.max_output_bytes_applied for t in tasks] == [2048, 1024, 1024, 16384, 32768]
    assert all(t.timeout_ms is None for t in tasks)
    assert all(t.timeout_ms_applied == config.execution.default_task_timeout_ms for t in tasks)


def given_spec_12_2_plan_when_projected_then_flags_and_effective_stop_rule_follow_adr009(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, SPEC["12.2"], expected=AFTER_INITIAL_REQUEST)
    _, tasks = _records(adapter, inbound, clock)
    t1, t4 = tasks[0], tasks[3]
    assert (t1.critical, t1.continue_on_error, t1.stop_plan_on_failure) == (False, True, False)
    assert t1.stop_plan_on_success is False
    assert t1.stops_plan_on_failure is False
    assert (t4.critical, t4.continue_on_error, t4.stop_plan_on_failure) == (True, False, True)
    assert t4.stops_plan_on_failure is True


def given_task_without_any_declaration_when_projected_then_config_defaults_applied(
    adapter: ProtocolAdapter, clock: FakeClock, config: AppConfig
) -> None:
    inbound = _parse(adapter, _plan([_task("t1")]))
    plan, (task,) = _records(adapter, inbound, clock)
    assert task.max_output_bytes is None
    assert task.max_output_bytes_applied == config.payload.default_max_output_bytes == 8192
    assert task.timeout_ms is None
    assert task.timeout_ms_applied == config.execution.default_task_timeout_ms == 60_000
    assert (task.critical, task.continue_on_error) == (False, False)
    assert (task.stop_plan_on_failure, task.stop_plan_on_success) == (False, False)
    assert (
        task.stops_plan_on_failure is True
    )  # absent continue_on_error => a failure stops the plan
    assert task.depends_on == () and task.resource_lock is None
    assert task.type is TaskType.CMD and task.cmd == "echo t1"
    assert (task.ref_task_id, task.stream, task.byte_offset, task.max_bytes) == (None,) * 4
    assert plan.default_max_output_bytes is None and plan.max_parallel_workers == 1


def given_limits_declared_under_caps_when_projected_then_declared_values_applied(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, _plan([_task("t1", max_output_bytes=2048, timeout_ms=5_000)]))
    _, (task,) = _records(adapter, inbound, clock)
    assert (task.max_output_bytes, task.max_output_bytes_applied) == (2048, 2048)
    assert (task.timeout_ms, task.timeout_ms_applied) == (5_000, 5_000)


def given_limits_declared_above_caps_when_projected_then_caps_applied_and_declared_kept(
    adapter: ProtocolAdapter, clock: FakeClock, config: AppConfig
) -> None:
    inbound = _parse(
        adapter, _plan([_task("t1", max_output_bytes=10_000_000, timeout_ms=10_000_000)])
    )
    _, (task,) = _records(adapter, inbound, clock)
    assert task.max_output_bytes == 10_000_000
    assert task.max_output_bytes_applied == config.payload.hard_max_output_bytes == 131_072
    assert task.timeout_ms == 10_000_000
    assert task.timeout_ms_applied == config.execution.max_task_timeout_ms == 900_000


def given_plan_default_output_budget_when_projected_then_used_for_silent_tasks_only(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(
        adapter,
        _plan([_task("t1"), _task("t2", max_output_bytes=4096)], default_max_output_bytes=1024),
    )
    plan, (t1, t2) = _records(adapter, inbound, clock)
    assert plan.default_max_output_bytes == 1024
    assert (t1.max_output_bytes, t1.max_output_bytes_applied) == (None, 1024)
    assert (t2.max_output_bytes, t2.max_output_bytes_applied) == (4096, 4096)


def given_plan_default_above_hard_cap_when_projected_then_cap_applied(
    adapter: ProtocolAdapter, clock: FakeClock, config: AppConfig
) -> None:
    inbound = _parse(adapter, _plan([_task("t1")], default_max_output_bytes=5_000_000))
    plan, (task,) = _records(adapter, inbound, clock)
    assert plan.default_max_output_bytes == 5_000_000
    assert task.max_output_bytes_applied == config.payload.hard_max_output_bytes


def given_custom_config_when_projected_then_defaults_and_caps_come_from_config(
    clock: FakeClock,
) -> None:
    custom = AppConfig(
        payload=PayloadSection(default_max_output_bytes=100, hard_max_output_bytes=200),
        execution=ExecutionSection(default_task_timeout_ms=1_000, max_task_timeout_ms=2_000),
    )
    adapter = ProtocolAdapter(custom)
    inbound = _parse(
        adapter, _plan([_task("t1"), _task("t2", max_output_bytes=500, timeout_ms=5_000)])
    )
    _, (t1, t2) = _records(adapter, inbound, clock)
    assert (t1.max_output_bytes_applied, t1.timeout_ms_applied) == (100, 1_000)
    assert (t2.max_output_bytes_applied, t2.timeout_ms_applied) == (200, 2_000)


@pytest.mark.parametrize(
    ("critical", "continue_on_error", "stop_plan_on_failure"),
    list(product([False, True], repeat=3)),
    ids=lambda v: str(v).lower(),
)
def given_flag_combination_when_projected_then_stops_plan_on_failure_is_or_of_adr009_rule(
    adapter: ProtocolAdapter,
    clock: FakeClock,
    critical: bool,
    continue_on_error: bool,
    stop_plan_on_failure: bool,
) -> None:
    inbound = _parse(
        adapter,
        _plan(
            [
                _task(
                    "t1",
                    critical=critical,
                    continue_on_error=continue_on_error,
                    stop_plan_on_failure=stop_plan_on_failure,
                )
            ]
        ),
    )
    _, (task,) = _records(adapter, inbound, clock)
    assert task.stops_plan_on_failure is (critical or stop_plan_on_failure or not continue_on_error)
    assert (task.critical, task.continue_on_error, task.stop_plan_on_failure) == (
        critical,
        continue_on_error,
        stop_plan_on_failure,
    )
    contradictory = critical and continue_on_error
    assert ("CONTRADICTORY_FLAGS:t1" in inbound.warnings) is contradictory


def given_stop_plan_on_success_when_projected_then_flag_copied(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, _plan([_task("t1", stop_plan_on_success=True)]))
    _, (task,) = _records(adapter, inbound, clock)
    assert task.stop_plan_on_success is True


def given_contradictory_flags_when_parsed_then_warning_not_error_and_plan_stops_on_failure(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    raw = _plan(
        [
            _task("t1", critical=True, continue_on_error=True),
            _task("t2", critical=True, continue_on_error=True),
        ]
    )
    inbound = _parse(adapter, raw)
    assert inbound.warnings == ["CONTRADICTORY_FLAGS:t1", "CONTRADICTORY_FLAGS:t2"]
    _, tasks = _records(adapter, inbound, clock)
    assert all(t.stops_plan_on_failure for t in tasks)


def given_parallel_plan_without_workers_when_parsed_and_projected_then_default_one_and_warning(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, _plan([_task("t1"), _task("t2")], policy="parallel"))
    assert inbound.warnings == ["DEFAULT_WORKERS_APPLIED"]
    plan, _ = _records(adapter, inbound, clock)
    assert plan.execution_policy is ExecutionPolicy.PARALLEL
    assert plan.max_parallel_workers == 1


def given_parallel_plan_with_workers_when_projected_then_declared_value_kept(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, SPEC["12.3"])
    plan, _ = _records(adapter, inbound, clock)
    assert plan.max_parallel_workers == 2
    assert inbound.warnings == []


def given_sequential_plan_with_workers_declared_when_projected_then_one_worker_and_warning(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, _plan([_task("t1")], max_parallel_workers=4))
    assert inbound.warnings == ["WORKERS_IGNORED_IN_SEQUENTIAL"]
    plan, _ = _records(adapter, inbound, clock)
    assert plan.max_parallel_workers == 1


def given_spec_12_6_chunk_plan_when_projected_then_chunk_fields_and_no_timeout(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, SPEC["12.6"], stored_output_task_ids={"t4"})
    plan, (chunk,) = _records(adapter, inbound, clock)
    assert plan.plan_id == "plan-2" and plan.task_count == 1
    assert chunk.type is TaskType.CHUNK_REQUEST
    assert chunk.cmd is None
    assert chunk.ref_task_id == "t4"
    assert chunk.stream is OutputStream.STDOUT  # ADR-011 default
    assert chunk.byte_offset == 16384
    assert chunk.max_bytes == 16384
    assert chunk.max_output_bytes_applied == 16384  # the chunk budget is its output budget
    assert chunk.timeout_ms is None and chunk.timeout_ms_applied is None  # ADR-008 §5
    assert chunk.stops_plan_on_failure is True  # no continue_on_error declared


def given_chunk_task_with_stream_and_huge_max_bytes_when_projected_then_stream_kept_and_capped(
    adapter: ProtocolAdapter, clock: FakeClock, config: AppConfig
) -> None:
    raw = _plan(
        [
            {
                "task_id": "c1",
                "type": "chunk_request",
                "ref_task_id": "t4",
                "stream": "stderr",
                "byte_offset": 5,
                "max_bytes": 10_000_000,
                "continue_on_error": True,
            }
        ]
    )
    inbound = _parse(adapter, raw)
    _, (chunk,) = _records(adapter, inbound, clock)
    assert chunk.stream is OutputStream.STDERR
    assert chunk.byte_offset == 5
    assert chunk.max_bytes == config.payload.hard_max_output_bytes
    assert chunk.max_output_bytes_applied == config.payload.hard_max_output_bytes
    assert chunk.stops_plan_on_failure is False


def given_chunk_task_declaring_output_budget_when_projected_then_max_bytes_capped_by_it(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    raw = _plan(
        [
            {
                "task_id": "c1",
                "type": "chunk_request",
                "ref_task_id": "t4",
                "byte_offset": 0,
                "max_bytes": 16384,
                "max_output_bytes": 4096,
            }
        ]
    )
    inbound = _parse(adapter, raw)
    _, (chunk,) = _records(adapter, inbound, clock)
    assert chunk.max_output_bytes == 4096
    assert chunk.max_bytes == 4096 and chunk.max_output_bytes_applied == 4096


def given_plan_with_state_summary_when_projected_then_dump_persisted_on_plan_record(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    summary = {
        "environment": {"os": "Linux"},
        "findings": ["a", "b"],
        "current_state": "s",
        "next_expected_step": "n",
        "extra_key": [1, 2],
    }
    inbound = _parse(adapter, _plan([_task("t1")], state_summary=summary))
    plan, _ = _records(adapter, inbound, clock)
    assert plan.state_summary == summary
    canonical_json(plan.state_summary)  # persisted form is serialisable


def given_plan_with_partial_state_summary_when_projected_then_defaults_filled_in_dump(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, _plan([_task("t1")], state_summary={"findings": ["only"]}))
    plan, _ = _records(adapter, inbound, clock)
    assert plan.state_summary == {
        "environment": {},
        "findings": ["only"],
        "current_state": None,
        "next_expected_step": None,
    }


def given_plan_with_resource_locks_and_dependencies_when_projected_then_copied_as_tuples(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    raw = _plan(
        [
            _task("t1", resource_lock="pom.xml"),
            _task("t2", resource_lock="pom.xml", depends_on=["t1"]),
            _task("t3", depends_on=["t1", "t2"]),
        ],
        policy="parallel",
        max_parallel_workers=2,
    )
    inbound = _parse(adapter, raw)
    _, tasks = _records(adapter, inbound, clock)
    assert [t.resource_lock for t in tasks] == ["pom.xml", "pom.xml", None]
    assert [t.depends_on for t in tasks] == [(), ("t1",), ("t1", "t2")]


def given_advanced_clock_when_projected_then_timestamps_follow_injected_clock(
    adapter: ProtocolAdapter,
) -> None:
    clock = FakeClock(start=datetime(2030, 6, 1, 12, 0, tzinfo=UTC))
    clock.advance(60_000)
    inbound = _parse(adapter, _plan([_task("t1")]))
    plan, (task,) = _records(adapter, inbound, clock)
    expected = datetime(2030, 6, 1, 12, 1, tzinfo=UTC)
    assert plan.created_at == plan.updated_at == expected
    assert task.created_at == task.updated_at == expected
    assert plan.started_at is None and task.started_at is None


def given_final_answer_inbound_when_projected_then_value_error(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, SPEC["12.7"])
    with pytest.raises(ValueError):
        _records(adapter, inbound, clock)


def given_ack_inbound_when_projected_then_value_error(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(
        adapter,
        SPEC["12.9"],
        expected=AFTER_RESUME_REQUEST,
        conversation=_conversation(RESUME_REMOTE_ID),
    )
    with pytest.raises(ValueError):
        _records(adapter, inbound, clock)


def given_same_plan_when_projected_twice_then_records_identical(
    adapter: ProtocolAdapter, clock: FakeClock
) -> None:
    inbound = _parse(adapter, SPEC["12.3"])
    assert _records(adapter, inbound, clock) == _records(adapter, inbound, clock)


# =============================================================================================
# 7. Protocol instructions (ADR-004 bootstrap)
# =============================================================================================


def given_default_config_when_instructions_rendered_then_config_values_injected(
    config: AppConfig,
) -> None:
    text = render_instructions(config)
    assert ProtocolAdapter.render_instructions(config) == text
    for value in (
        config.payload.default_max_output_bytes,
        config.payload.hard_max_output_bytes,
        config.payload.max_message_bytes,
        config.payload.max_state_summary_bytes,
        config.execution.default_task_timeout_ms,
        config.execution.max_task_timeout_ms,
    ):
        assert str(value) in text, f"{value} missing from instructions"


def given_custom_config_when_instructions_rendered_then_custom_values_replace_defaults() -> None:
    custom = AppConfig(
        payload=PayloadSection(
            default_max_output_bytes=1111,
            hard_max_output_bytes=22222,
            max_message_bytes=133333,
            max_state_summary_bytes=4444,
        ),
        execution=ExecutionSection(default_task_timeout_ms=55555, max_task_timeout_ms=666666),
    )
    text = render_instructions(custom)
    for value in ("1111", "22222", "133333", "4444", "55555", "666666"):
        assert value in text
    assert "8192" not in text and "131072" not in text and "900000" not in text


def given_instructions_when_rendered_then_no_placeholder_left_unresolved(
    config: AppConfig,
) -> None:
    text = render_instructions(config)
    assert re.findall(r"\{[a-z_]+\}", text) == []


def given_instructions_when_rendered_twice_then_identical(config: AppConfig) -> None:
    assert render_instructions(config) == render_instructions(config)


@pytest.mark.parametrize(
    "keyword",
    [
        # role and turn discipline
        "trusted planner",
        "exactly one message",
        "as-is",
        # grammar §2.2 / §2.3
        "discovery_plan",
        "execution_plan",
        "priority_clarification",
        "execution_result",
        "final_answer",
        "user_request",
        "context_resume_request",
        "context_resume_ack",
        "pending_message_type",
        # envelope and uniqueness
        "message_id",
        "conversation_id",
        "plan_id",
        "task_id",
        "unique",
        # tasks, dependencies, parallelism (§2.4)
        "depends_on",
        "resource_lock",
        "max_parallel_workers",
        "sequential",
        "parallel",
        # flags ADR-009
        "critical",
        "continue_on_error",
        "stop_plan_on_failure",
        "stop_plan_on_success",
        "default",
        # payload ADR-010 / ADR-011
        "max_output_bytes",
        "default_max_output_bytes",
        "truncated",
        "original_size_bytes",
        "stdout_range",
        "stderr_range",
        "chunk_request",
        "ref_task_id",
        "byte_offset",
        "max_bytes",
        "stream",
        "stderr",
        "eof",
        # timeouts ADR-008
        "timeout_ms",
        "timed_out",
        # state summary ADR-005
        "state_summary",
        "findings",
        "next_expected_step",
        # results ADR-009 reasons
        "skipped_tasks",
        "cancelled_tasks",
        "interrupted_tasks",
        "reason",
        "stop_reason",
        # session budget ADR-012
        "session_budget",
        "max_cycles",
        # final answer §12.7
        "diagnosis",
        "evidence",
        "recommended_next_step",
        # direct answer to the user ADR-022
        "user_response",
        "expects_reply",
        "opaque",
    ],
)
def given_instructions_when_rendered_then_rule_keyword_present(
    config: AppConfig, keyword: str
) -> None:
    assert keyword in render_instructions(config)


def _table_row(text: str, label: str) -> str:
    """The row of the "You received / You may send" table whose first cell starts with ``label``."""
    rows = [line for line in text.splitlines() if line.startswith(f"| `{label}")]
    assert rows, f"no table row for {label}"
    return rows[0]


def given_direct_response_allowed_when_instructions_rendered_then_first_message_row_offers_it(
    config: AppConfig,
) -> None:
    assert config.protocol.allow_direct_response is True
    text = render_instructions(config)
    first_row = _table_row(text, "user_request` (first message")
    assert "`discovery_plan`, `user_response`" in first_row
    assert "unless the request needs no command at all" in text
    assert "discovery_plan | user_response" in text
    assert "**always**" not in text.split("## 1.")[1].split("```")[0]
    assert "## 9. Answering the user directly: user_response" in text
    assert re.findall(r"\{[a-z_]+\}", text) == []


def given_direct_response_disabled_when_instructions_rendered_then_first_message_is_always_a_plan() -> (
    None
):
    strict = AppConfig(protocol=ProtocolSection(allow_direct_response=False))
    text = render_instructions(strict)
    first_row = _table_row(text, "user_request` (first message")
    assert first_row.rstrip().endswith("| `discovery_plan` |")
    assert "`user_response`" not in first_row
    assert "is **always** a" in text
    assert "only accepted after an `execution_result` or a follow-up `user_request`" in text
    assert "└─> discovery_plan\n" in text
    assert "discovery_plan | user_response" not in text
    # the other rows and the user_response section are unchanged
    assert "`user_response`" in _table_row(text, "execution_result`")
    assert "`user_response`" in _table_row(text, "user_request` (follow-up")
    assert "## 9. Answering the user directly: user_response" in text
    assert re.findall(r"\{[a-z_]+\}", text) == []
    assert text != render_instructions(AppConfig())


def given_instructions_when_user_response_examples_extracted_then_both_validate_and_one_asks(
    config: AppConfig,
) -> None:
    text = render_instructions(config)
    blocks = [json.loads(b) for b in re.findall(r"```json[ \t]*\n(.*?)```", text, re.S)]
    responses = [b for b in blocks if isinstance(b, dict) and b.get("type") == "user_response"]
    assert len(responses) == 2
    contents = [UserResponseContent.model_validate(r["content"]) for r in responses]
    assert [c.expects_reply for c in contents] == [False, True]
    assert contents[0].format == "markdown" and contents[1].format == "text"
    assert str(config.payload.max_message_bytes) in text.split("## 9.")[1]


def given_instructions_when_json_examples_extracted_then_every_message_validates_against_schemas(
    config: AppConfig,
) -> None:
    text = render_instructions(config)
    blocks = re.findall(r"```json[ \t]*\n(.*?)```", text, re.S)
    assert len(blocks) >= 8, "the instructions must embed the message schemas as JSON examples"
    messages = 0
    for block in blocks:
        data = json.loads(block)
        if isinstance(data, dict) and "type" in data and "content" in data:
            envelope = Envelope.model_validate(data)
            content_model_for(envelope.type).model_validate(envelope.content)
            messages += 1
    assert messages >= 8


def given_instructions_when_rendered_then_every_protocol_message_type_exemplified(
    config: AppConfig,
) -> None:
    text = render_instructions(config)
    blocks = [json.loads(b) for b in re.findall(r"```json[ \t]*\n(.*?)```", text, re.S)]
    exemplified = {b["type"] for b in blocks if isinstance(b, dict) and "type" in b}
    assert exemplified >= {m.value for m in OUTBOUND_MESSAGE_TYPES | INBOUND_MESSAGE_TYPES}
    assert "system_error" not in exemplified  # internal only, never exchanged (ADR-007)


def given_instructions_plan_examples_when_parsed_by_adapter_then_accepted(
    adapter: ProtocolAdapter, config: AppConfig
) -> None:
    """The examples we give to the model must pass our own validation (with their outputs stored)."""
    text = render_instructions(config)
    blocks = [json.loads(b) for b in re.findall(r"```json[ \t]*\n(.*?)```", text, re.S)]
    plans = [b for b in blocks if isinstance(b, dict) and b.get("type") in PLAN_MESSAGE_TYPES]
    assert plans
    for raw in plans:
        conversation = _conversation(raw["conversation_id"])
        refs = {
            t["ref_task_id"] for t in raw["content"]["tasks"] if t.get("type") == "chunk_request"
        }
        inbound = _parse(
            adapter,
            raw,
            expected=frozenset({MessageType(raw["type"])}),
            conversation=conversation,
            stored_output_task_ids=refs,
        )
        assert isinstance(inbound.content, PlanContent)


# =============================================================================================
# 8. Determinism and hygiene (ADR-017)
# =============================================================================================


def given_adapter_source_when_inspected_then_no_wall_clock_or_randomness_used() -> None:
    source = Path(adapter_module.__file__ or "").read_text(encoding="utf-8")
    for forbidden in ("datetime.now", "time.time", "time.monotonic", "uuid", "random"):
        assert forbidden not in source, f"{forbidden} must not be used in the adapter"


def given_adapter_when_constructed_then_config_exposed_and_reusable(config: AppConfig) -> None:
    adapter = ProtocolAdapter(config)
    assert adapter.config is config
    first = _parse(adapter, SPEC["12.3"])
    second = _parse(adapter, SPEC["12.3"])
    assert first.envelope == second.envelope and first.content == second.content


def given_inbound_message_when_payload_read_then_equals_raw_message(
    adapter: ProtocolAdapter,
) -> None:
    inbound = _parse(adapter, SPEC["12.3"])
    assert inbound.payload == SPEC["12.3"]
    assert canonical_json(inbound.payload) == canonical_json(SPEC["12.3"])
