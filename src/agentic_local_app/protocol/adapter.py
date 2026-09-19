"""ProtocolAdapter — build outbound messages, parse and validate inbound ones (spec §3.5).

The adapter is **pure**: no I/O, no clock of its own (``plan_to_records`` receives the ``Clock``),
no identifier generation (``message_id`` is passed in by the caller, ADR-017). It owns:

- the construction of the four outbound types (§12.1, §12.5, §12.8 + ADR-014, and the
  ``protocol_correction_request`` of ADR-023) as canonical JSON;
- the **table of expected inbound messages** of ADR-007 (:data:`EXPECTED_INBOUND`), amended by
  ADR-022: ``user_response`` after an ``execution_result`` or a follow-up ``user_request``, and
  after the initial ``user_request`` only when ``protocol.allow_direct_response`` is set
  (:func:`expected_inbound_for`); a correction request is **transparent** for that table
  (:func:`last_substantive_outbound`) — it asks again for the reply that is still pending;
- the composition of a correction request (:meth:`ProtocolAdapter.build_protocol_correction_request`):
  the catalogue of minimal valid examples, the per-code hints and a reminder generated from the
  content models, fitted under ``payload.max_message_bytes`` (ADR-023);
- the structural validation of every inbound message: envelope, direction, expectation, content
  schema, then the semantic rules of ADR-007 (uniqueness, dependencies, chunk references,
  ``state_summary`` bound of ADR-005) and ADR-022 (``user_response`` body bound), each failure
  being a :class:`ProtocolError` with an explicit code and JSON-serialisable ``details``;
- the projection of an accepted plan onto ``PlanRecord`` / ``TaskRecord`` with the effective
  values of ADR-008 (timeouts), ADR-009 (flags), ADR-010 (output budgets) and ADR-011 (chunks);
- the rendering of ``PROTOCOL_INSTRUCTIONS.md`` sent to the model at init (ADR-004).

Outbound payloads are the pydantic dump of the content model with ``exclude_none=True``: the
optional extensions of the ADRs never appear unless set, so the messages stay identical to the
examples of §12. The single visible consequence is that a null ``stop_reason`` is omitted rather
than serialised as ``null`` (the model reads absence as "no stop reason").
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum, unique
from functools import lru_cache
from importlib import resources
from types import MappingProxyType
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.canonical import canonical_json, size_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import NormalizedError, ProtocolError
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
    PLAN_MESSAGE_TYPES,
    SUBSTANTIVE_OUTBOUND_MESSAGE_TYPES,
    ExecutionPolicy,
    MessageDirection,
    MessageType,
    PlanState,
    PlanType,
    TaskState,
    TaskType,
    plan_type_for_message,
)
from agentic_local_app.protocol.messages import (
    ContextResumeAckContent,
    ContextResumeRequestContent,
    Envelope,
    ExecutionResultContent,
    FinalAnswerContent,
    PlanContent,
    ProtocolCorrectionRequestContent,
    SessionBudgetContent,
    TaskMessage,
    UserRequestContent,
    UserResponseContent,
)

__all__ = [
    "CORRECTION_EXAMPLE_MESSAGE_ID",
    "EXPECTED_INBOUND",
    "INSTRUCTIONS_FILENAME",
    "InboundContent",
    "InboundMessage",
    "OutboundMessage",
    "OutboundSituation",
    "ProtocolAdapter",
    "example_envelope_for",
    "expected_inbound_for",
    "last_substantive_outbound",
    "peek_field",
    "rejected_payload",
    "render_instructions",
    "situation_for",
]

INSTRUCTIONS_FILENAME = "PROTOCOL_INSTRUCTIONS.md"

ContentT = TypeVar("ContentT", bound=BaseModel)

# ------------------------------------------------------------------------------------------------
# Value objects
# ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class OutboundMessage:
    """A message ready to be POSTed: typed envelope, JSON payload, canonical form and size."""

    envelope: Envelope
    payload: dict[str, Any]
    canonical: str
    size_bytes: int
    message_type: MessageType


InboundContent = PlanContent | FinalAnswerContent | UserResponseContent | ContextResumeAckContent


@dataclass(frozen=True)
class InboundMessage:
    """A validated model message. ``warnings`` carries the audit notices of ADR-009 (never errors)."""

    envelope: Envelope
    content: InboundContent
    message_type: MessageType
    plan_type: PlanType | None
    warnings: list[str] = field(default_factory=list)
    size_bytes: int = 0

    @property
    def payload(self) -> dict[str, Any]:
        """The message as received (for the ``MessageRecord`` and the context byte count)."""
        return self.envelope.model_dump(mode="json")


# ------------------------------------------------------------------------------------------------
# Table of expected inbound messages (ADR-007)
# ------------------------------------------------------------------------------------------------


@unique
class OutboundSituation(StrEnum):
    """The rows of the ADR-007 table: what the application last sent, and in which situation."""

    INITIAL_USER_REQUEST = "initial_user_request"
    FOLLOW_UP_USER_REQUEST = "follow_up_user_request"
    EXECUTION_RESULT = "execution_result"
    CONTEXT_RESUME_REQUEST = "context_resume_request"


#: The base table (ADR-007 amended by ADR-022). The initial row is the strict one of spec §14;
#: :func:`expected_inbound_for` adds ``user_response`` to it under ``protocol.allow_direct_response``.
EXPECTED_INBOUND: Mapping[OutboundSituation, frozenset[MessageType]] = MappingProxyType(
    {
        OutboundSituation.INITIAL_USER_REQUEST: frozenset({MessageType.DISCOVERY_PLAN}),
        OutboundSituation.FOLLOW_UP_USER_REQUEST: frozenset(
            {
                MessageType.DISCOVERY_PLAN,
                MessageType.EXECUTION_PLAN,
                MessageType.PRIORITY_CLARIFICATION,
                MessageType.FINAL_ANSWER,
                MessageType.USER_RESPONSE,
            }
        ),
        OutboundSituation.EXECUTION_RESULT: frozenset(
            {
                MessageType.EXECUTION_PLAN,
                MessageType.PRIORITY_CLARIFICATION,
                MessageType.FINAL_ANSWER,
                MessageType.USER_RESPONSE,
            }
        ),
        OutboundSituation.CONTEXT_RESUME_REQUEST: frozenset({MessageType.CONTEXT_RESUME_ACK}),
    }
)


def expected_inbound_for(
    situation: OutboundSituation, *, allow_direct_response: bool = True
) -> frozenset[MessageType]:
    """The row of :data:`EXPECTED_INBOUND` for ``situation``, with the ADR-022 flag applied: the
    initial ``user_request`` also accepts a ``user_response`` when ``allow_direct_response`` is
    set (the default); the other rows never depend on it."""
    expected = EXPECTED_INBOUND[situation]
    if situation is OutboundSituation.INITIAL_USER_REQUEST and allow_direct_response:
        return expected | {MessageType.USER_RESPONSE}
    return expected


def last_substantive_outbound(messages: Sequence[MessageRecord]) -> MessageRecord | None:
    """The last outbound message that **sets** the expectation, or ``None`` (ADR-007, ADR-023).

    A ``protocol_correction_request`` is transparent here: it never opens a new row of
    :data:`EXPECTED_INBOUND` — it asks again for the reply the last ``user_request`` /
    ``execution_result`` / ``context_resume_request`` is still waiting for.
    """
    for message in reversed(messages):
        if (
            message.direction is MessageDirection.OUTBOUND
            and message.message_type in SUBSTANTIVE_OUTBOUND_MESSAGE_TYPES
        ):
            return message
    return None


def situation_for(
    last_outbound: MessageRecord, conversation: ConversationRecord
) -> OutboundSituation:
    """Classify the last **substantive** outbound message into a row of :data:`EXPECTED_INBOUND`.

    ``conversation.final_answer_received`` means "the model concluded a turn in this conversation
    with a ``final_answer`` or a ``user_response``" (ADR-022): the next ``user_request`` is then a
    follow-up, whatever the type of that concluding message.

    A ``protocol_correction_request`` is not a row of the table (ADR-023): it leaves the pending
    expectation untouched, so the caller passes the message it corrects, never the correction
    itself (:func:`last_substantive_outbound` finds it).
    """
    if last_outbound.direction is not MessageDirection.OUTBOUND:
        raise ValueError(f"{last_outbound.message_id} is not an outbound message")
    match last_outbound.message_type:
        case MessageType.USER_REQUEST:
            if conversation.final_answer_received:
                return OutboundSituation.FOLLOW_UP_USER_REQUEST
            return OutboundSituation.INITIAL_USER_REQUEST
        case MessageType.EXECUTION_RESULT:
            return OutboundSituation.EXECUTION_RESULT
        case MessageType.CONTEXT_RESUME_REQUEST:
            return OutboundSituation.CONTEXT_RESUME_REQUEST
        case MessageType.PROTOCOL_CORRECTION_REQUEST:
            raise ValueError(
                "protocol_correction_request sets no expectation (ADR-023): pass the substantive "
                "outbound message it corrects"
            )
        case other:
            raise ValueError(f"{other.value} is not an outbound message type")


# ------------------------------------------------------------------------------------------------
# Instructions (ADR-004)
# ------------------------------------------------------------------------------------------------

_PLACEHOLDER_RE = re.compile(r"\{([a-z_]+)\}")


@lru_cache(maxsize=1)
def _instructions_template() -> str:
    return (
        resources.files("agentic_local_app.protocol")
        .joinpath(INSTRUCTIONS_FILENAME)
        .read_text(encoding="utf-8")
    )


#: Text of the first-message rule rendered into the instructions (ADR-022), by flag value.
_INITIAL_REPLY_RULE_DIRECT = (
    "Your first response to a `user_request` in a new conversation is a `discovery_plan` "
    "(that is how you learn the operating system, the shell, the working directory, the installed "
    "tools and their versions), unless the request needs no command at all — an explanation, an "
    "analysis of the text you were given, or a question back to the user: then it is a "
    "`user_response` (section 9). A request about the machine always starts with a "
    "`discovery_plan`."
)
_INITIAL_REPLY_RULE_STRICT = (
    "Your first response to a `user_request` in a new conversation is **always** a "
    "`discovery_plan`: that is how you learn the operating system, the shell, the working "
    "directory, the installed tools and their versions. A `user_response` (section 9) is only "
    "accepted after an `execution_result` or a follow-up `user_request`."
)

#: What a refused message means for the model (ADR-023), rendered from
#: ``protocol.max_correction_attempts``: with the policy off the instructions must not promise a
#: correction round that will never come.
_REJECTION_RULE_CORRECTED = (
    "A rejected message is **not** the end of the exchange: the application answers it with a "
    "`protocol_correction_request` (section 10) telling you exactly what was wrong and what it "
    "expects, and reads your next message against the same expectation. After "
    "{max_correction_attempts} refused replies in a row the session stops, so read that message "
    "carefully rather than resending."
)
_REJECTION_RULE_STRICT = (
    "A rejected message ends the conversation: this deployment asks for no correction, so "
    "re-read this table when in doubt — there is no second chance."
)
_CORRECTION_BUDGET_RULE_ON = (
    "After **{max_correction_attempts}** refused replies in a row, the application stops asking "
    "and the session ends in failure. `attempt` and `max_attempts` tell you where you stand. A "
    "reply that is accepted resets the count to zero."
)
_CORRECTION_BUDGET_RULE_OFF = (
    "The correction policy is **disabled** in this deployment "
    "(`protocol.max_correction_attempts = 0`): you will never receive a "
    "`protocol_correction_request`, and the first refused reply ends the session. This section is "
    "kept so that the protocol text reads the same everywhere."
)


def render_instructions(config: AppConfig) -> str:
    """The protocol text sent to the model at init, with the configured limits injected, the
    first-message rule of ADR-022 rendered from ``protocol.allow_direct_response`` and the
    correction policy of ADR-023 rendered from ``protocol.max_correction_attempts``."""
    direct = config.protocol.allow_direct_response
    attempts = config.protocol.max_correction_attempts
    corrects = attempts > 0
    values = {
        "default_max_output_bytes": config.payload.default_max_output_bytes,
        "hard_max_output_bytes": config.payload.hard_max_output_bytes,
        "max_message_bytes": config.payload.max_message_bytes,
        "max_state_summary_bytes": config.payload.max_state_summary_bytes,
        "default_task_timeout_ms": config.execution.default_task_timeout_ms,
        "max_task_timeout_ms": config.execution.max_task_timeout_ms,
        "initial_reply_types": (
            "`discovery_plan`, `user_response`" if direct else "`discovery_plan`"
        ),
        "initial_reply_grammar": "discovery_plan | user_response" if direct else "discovery_plan",
        "initial_reply_rule": _INITIAL_REPLY_RULE_DIRECT if direct else _INITIAL_REPLY_RULE_STRICT,
        "max_correction_attempts": attempts,
        "rejection_policy_rule": (
            _REJECTION_RULE_CORRECTED.format(max_correction_attempts=attempts)
            if corrects
            else _REJECTION_RULE_STRICT
        ),
        "correction_budget_rule": (
            _CORRECTION_BUDGET_RULE_ON.format(max_correction_attempts=attempts)
            if corrects
            else _CORRECTION_BUDGET_RULE_OFF
        ),
    }

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise ValueError(f"unknown placeholder in {INSTRUCTIONS_FILENAME}: {{{name}}}")
        return str(values[name])

    return _PLACEHOLDER_RE.sub(substitute, _instructions_template())


# ------------------------------------------------------------------------------------------------
# Correction requests (ADR-023): minimal examples, per-code hints, generated reminder
# ------------------------------------------------------------------------------------------------

#: The ``message_id`` carried by the example of a correction request: a placeholder, never an id to
#: reuse — the model must choose a new one, unique in the session (§3.5 of the instructions).
CORRECTION_EXAMPLE_MESSAGE_ID = "<new-unique-message-id>"

#: Minimal **valid** content per inbound message type, built from the content models themselves so
#: that an example can never drift from the schema it illustrates (ADR-023).
_EXAMPLE_CONTENT: Mapping[MessageType, BaseModel] = MappingProxyType(
    {
        MessageType.DISCOVERY_PLAN: PlanContent(
            plan_id="<new-unique-plan-id>",
            objective="Discover the execution environment",
            execution_policy=ExecutionPolicy.SEQUENTIAL,
            tasks=[
                TaskMessage(
                    task_id="<new-unique-task-id>",
                    type=TaskType.CMD,
                    cmd="uname -a",
                    continue_on_error=True,
                )
            ],
        ),
        MessageType.EXECUTION_PLAN: PlanContent(
            plan_id="<new-unique-plan-id>",
            objective="Confirm the hypothesis with one command",
            execution_policy=ExecutionPolicy.SEQUENTIAL,
            tasks=[
                TaskMessage(
                    task_id="<new-unique-task-id>",
                    type=TaskType.CMD,
                    cmd="echo $JAVA_HOME",
                    continue_on_error=True,
                )
            ],
        ),
        MessageType.PRIORITY_CLARIFICATION: PlanContent(
            plan_id="<new-unique-plan-id>",
            objective="Immediately confirm one fact before anything else",
            execution_policy=ExecutionPolicy.SEQUENTIAL,
            tasks=[
                TaskMessage(
                    task_id="<new-unique-task-id>",
                    type=TaskType.CMD,
                    cmd="mvn -version",
                    critical=True,
                )
            ],
        ),
        MessageType.FINAL_ANSWER: FinalAnswerContent(
            status="completed",
            diagnosis="What you concluded, in plain language, for the user.",
            evidence=["The task result that supports it"],
            recommended_next_step="What the user should do now.",
        ),
        MessageType.USER_RESPONSE: UserResponseContent(
            format="markdown",
            body="What you want to tell the user, as text.",
            status="completed",
            expects_reply=False,
        ),
        MessageType.CONTEXT_RESUME_ACK: ContextResumeAckContent(
            original_conversation_id="<the original_conversation_id you received>",
            acknowledged=True,
        ),
    }
)

#: Order in which an expected type is picked to illustrate the reply, when several are valid.
_EXAMPLE_PREFERENCE: tuple[MessageType, ...] = (
    MessageType.CONTEXT_RESUME_ACK,
    MessageType.DISCOVERY_PLAN,
    MessageType.EXECUTION_PLAN,
    MessageType.PRIORITY_CLARIFICATION,
    MessageType.FINAL_ANSWER,
    MessageType.USER_RESPONSE,
)

#: One short, targeted sentence per refusal code — what went wrong, in the model's terms.
_CORRECTION_HINTS: Mapping[str, str] = MappingProxyType(
    {
        "SCHEMA_INVALID": "the message does not match the schema of its type",
        "UNEXPECTED_MESSAGE_TYPE": "that type is not one of the types expected at this point",
        "SYSTEM_ERROR_NOT_ALLOWED_INBOUND": (
            "system_error is internal to the application and is never sent by you"
        ),
        "UNEXPECTED_EXTRA_MESSAGE": "one turn carries exactly one message, never two",
        "EMPTY_REPLY": "the reply carried no message at all",
        "CONVERSATION_MISMATCH": (
            "conversation_id must repeat the id of the conversation you are in"
        ),
        "DUPLICATE_MESSAGE_ID": "message_id must be new: it is already taken in this session",
        "DUPLICATE_PLAN_ID": "plan_id must be new: it is already taken in this session",
        "DUPLICATE_TASK_ID": "task_id must be new: it is already taken in this session",
        "SELF_DEPENDENCY": "a task cannot depend on itself",
        "UNKNOWN_DEPENDENCY": "depends_on may only name tasks declared in the same plan",
        "DEPENDENCY_CYCLE": "the dependencies of the plan form a cycle",
        "FORWARD_DEPENDENCY_IN_SEQUENTIAL": (
            "in sequential mode a task may only depend on tasks declared before it"
        ),
        "CHUNK_REF_UNKNOWN": (
            "a chunk_request must name a task of this session whose output is stored"
        ),
        "STATE_SUMMARY_TOO_LARGE": "the state_summary is over its byte budget: make it denser",
        "USER_RESPONSE_TOO_LARGE": "the body is over its byte budget: make it shorter",
        "ACK_WRONG_ORIGINAL": (
            "original_conversation_id must repeat the value of the resume request"
        ),
        "ACK_NOT_ACKNOWLEDGED": "the resume request must be acknowledged with acknowledged = true",
        "UNPARSEABLE_REPLY": (
            "no JSON message envelope could be read in the reply: answer with the envelope alone"
        ),
    }
)

_DEFAULT_HINT = "the reply could not be used as a protocol message"

#: Details of a refusal that belong to the envelope of the correction request, not to ``errors``.
_DETAIL_NOT_AN_ERROR: frozenset[str] = frozenset({"errors", "excerpt", "operation", "http_status"})

#: Reductions applied in order until the correction request fits ``payload.max_message_bytes``.
_SHRINK_EXCERPT_CHARS = 200
_SHRINK_REMINDER_CHARS = 400


def example_envelope_for(message_type: MessageType, conversation_id: str) -> dict[str, Any]:
    """A minimal **valid** envelope of ``message_type`` for ``conversation_id`` (ADR-023).

    The ``message_id`` is :data:`CORRECTION_EXAMPLE_MESSAGE_ID`, a placeholder: the example shows
    the shape to copy, never an identifier to reuse. ``ValueError`` for a type with no example.
    """
    try:
        content = _EXAMPLE_CONTENT[message_type]
    except KeyError as exc:
        raise ValueError(f"no minimal example for {message_type.value}") from exc
    return {
        "type": message_type.value,
        "conversation_id": conversation_id,
        "message_id": CORRECTION_EXAMPLE_MESSAGE_ID,
        "content": content.model_dump(mode="json", exclude_none=True),
    }


def _resolve_schema(node: Mapping[str, Any], defs: Mapping[str, Any]) -> Mapping[str, Any]:
    """The schema node with a single ``$ref`` (or ``allOf`` of one) replaced by its definition."""
    ref = node.get("$ref")
    if ref is None and isinstance(node.get("allOf"), list) and len(node["allOf"]) == 1:
        inner = node["allOf"][0]
        ref = inner.get("$ref") if isinstance(inner, Mapping) else None
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        target = defs.get(ref.removeprefix("#/$defs/"))
        if isinstance(target, Mapping):
            return target
    return node


def _value_domain(node: Mapping[str, Any], defs: Mapping[str, Any]) -> str:
    """The value domain of one JSON-schema node, as a short phrase the model can act on."""
    node = _resolve_schema(node, defs)
    enum = node.get("enum")
    if isinstance(enum, list) and enum:
        return "one of " + " | ".join(str(value) for value in enum)
    options = node.get("anyOf")
    if isinstance(options, list) and options:
        rendered = {
            _value_domain(option, defs): None for option in options if isinstance(option, Mapping)
        }
        return " or ".join(rendered)
    kind = node.get("type")
    if kind == "array":
        items = node.get("items")
        inner = _value_domain(items, defs) if isinstance(items, Mapping) else "value"
        minimum = node.get("minItems")
        bound = f", at least {minimum}" if isinstance(minimum, int) and minimum else ""
        return f"array of {inner}{bound}"
    if kind in ("integer", "number"):
        if "exclusiveMinimum" in node:
            return f"{kind} > {node['exclusiveMinimum']}"
        if "minimum" in node:
            return f"{kind} >= {node['minimum']}"
        return str(kind)
    if kind == "string":
        return "non-empty string" if node.get("minLength") else "string"
    if kind == "object" or "properties" in node:
        return "object"
    if kind is None:
        return "value"
    return str(kind)


def _content_digest(model: type[BaseModel]) -> str:
    """``field: domain (required)`` for every field of a content model, in declaration order."""
    schema = model.model_json_schema()
    defs = schema.get("$defs", {})
    required = set(schema.get("required", ()))
    fields = []
    for name, node in schema.get("properties", {}).items():
        if not isinstance(node, Mapping):
            continue
        suffix = " (required)" if name in required else ""
        fields.append(f"{name}: {_value_domain(node, defs)}{suffix}")
    return ", ".join(fields)


def correction_reminder(
    error_code: str, expected: frozenset[MessageType], conversation_id: str
) -> str:
    """The targeted reminder of a correction request: what was wrong, what is expected now, the
    shape of each expected message and the rule to follow (ADR-023).

    Everything but the per-code hint is **generated** from the content models, so the reminder
    cannot describe a schema the application does not enforce.
    """
    hint = _CORRECTION_HINTS.get(error_code, _DEFAULT_HINT)
    lines = [f"Your last reply was refused ({error_code}): {hint}."]
    if not expected:
        lines.append("Nothing is expected from you in this conversation.")
        return "\n".join(lines)
    names = ", ".join(f"`{message_type.value}`" for message_type in _ordered(expected))
    lines.append(f"Send exactly one message, of one of these types: {names}.")
    for message_type in _ordered(expected):
        lines.append(
            f"- {message_type.value}.content — {_content_digest(_content_model(message_type))}"
        )
    lines.append(
        "The envelope is type, conversation_id, message_id, content. Use conversation_id "
        f'"{conversation_id}" and a NEW message_id, unique in the session. Fix exactly what '
        "`errors` lists; do not resend the refused message unchanged."
    )
    return "\n".join(lines)


def _correction_errors(details: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The validation details of a refusal, **unchanged**, as a list of JSON objects (ADR-023).

    A schema failure already carries a list of ``{loc, type, msg}``; every other code carries flat
    details (``expected`` / ``received``, ``task_id`` / ``dependency``, ``size_bytes`` / ``max_bytes``…)
    which become one entry, minus what the envelope of the correction already says.
    """
    listed = details.get("errors")
    if isinstance(listed, list) and all(isinstance(item, Mapping) for item in listed):
        return [dict(item) for item in listed]
    entry = {
        key: value
        for key, value in details.items()
        if key not in _DETAIL_NOT_AN_ERROR and value is not None
    }
    return [entry] if entry else []


def _shrink_excerpt(fields: dict[str, Any]) -> None:
    excerpt = fields.get("raw_excerpt")
    fields["raw_excerpt"] = excerpt[:_SHRINK_EXCERPT_CHARS] if isinstance(excerpt, str) else None


def _drop_excerpt(fields: dict[str, Any]) -> None:
    fields["raw_excerpt"] = None


def _shrink_errors(fields: dict[str, Any]) -> None:
    fields["errors"] = list(fields.get("errors") or [])[:1]


def _drop_errors(fields: dict[str, Any]) -> None:
    fields["errors"] = []


def _shrink_reminder(fields: dict[str, Any]) -> None:
    reminder = fields.get("reminder")
    fields["reminder"] = reminder[:_SHRINK_REMINDER_CHARS] if isinstance(reminder, str) else ""


def _drop_example(fields: dict[str, Any]) -> None:
    fields["example"] = {}


def _drop_reminder(fields: dict[str, Any]) -> None:
    fields["reminder"] = ""


#: Applied in order until the correction request fits ``payload.max_message_bytes`` (ADR-010):
#: the excerpt goes first (the model wrote it), the example last (it is what makes the correction
#: actionable), and what is left — code, expected types, attempt — is always tiny.
_CORRECTION_SHRINK_STEPS: tuple[Callable[[dict[str, Any]], None], ...] = (
    _shrink_excerpt,
    _drop_excerpt,
    _shrink_errors,
    _drop_errors,
    _shrink_reminder,
    _drop_example,
    _drop_reminder,
)


def _ordered(expected: frozenset[MessageType]) -> list[MessageType]:
    """The expected types in the stable order of :data:`_EXAMPLE_PREFERENCE`, then by value."""
    order = {message_type: index for index, message_type in enumerate(_EXAMPLE_PREFERENCE)}
    return sorted(expected, key=lambda m: (order.get(m, len(order)), m.value))


def _content_model(message_type: MessageType) -> type[BaseModel]:
    if message_type in PLAN_MESSAGE_TYPES:
        return PlanContent
    if message_type is MessageType.FINAL_ANSWER:
        return FinalAnswerContent
    if message_type is MessageType.USER_RESPONSE:
        return UserResponseContent
    return ContextResumeAckContent


# ------------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------------


def peek_field(raw: Any, key: str) -> Any:
    """Best-effort read of an envelope field from an **unvalidated** reply item.

    A model that answers with something other than a JSON object (a bare string, a number, a
    list) is exactly the case the callers of this helper have to survive, so anything that is not
    a mapping simply yields ``None``, and a non-scalar value is rendered as text (the result goes
    into event payloads and error details, which must stay JSON-serialisable).
    """
    if not isinstance(raw, Mapping):
        return None
    value = raw.get(key)
    return value if isinstance(value, str | int | float | bool) or value is None else str(value)


def rejected_payload(raw_messages: Sequence[Any]) -> dict[str, Any]:
    """What is persisted as the payload of a rejected reply (``validation_status = "invalid"``).

    The stored record must keep what the model actually sent — it is the only trace of the fault
    and what a correction policy would quote back. A single JSON object is stored as is; anything
    else (no message at all, several messages, a list, a bare string, a number) is wrapped so that
    the record stays an object without losing the raw form.
    """
    if len(raw_messages) == 1:
        single = raw_messages[0]
        if isinstance(single, Mapping):
            return dict(single)
        return {"raw": single}
    return {"messages": list(raw_messages)}


def _remote_id(conversation: ConversationRecord) -> str:
    """The conversation id known to the model (falls back to the local id before init)."""
    return conversation.remote_conversation_id or conversation.conversation_id


def _pydantic_errors(exc: ValidationError, prefix: str = "") -> list[dict[str, str]]:
    """Compact, JSON-serialisable rendering of pydantic errors (``FailureRecord.details``, audit)."""
    rendered = []
    for err in exc.errors(include_url=False):
        loc = ".".join(str(part) for part in err["loc"])
        if prefix:
            loc = f"{prefix}.{loc}" if loc else prefix
        rendered.append({"loc": loc, "type": str(err["type"]), "msg": str(err["msg"])})
    return rendered


def _find_cycle(order: Sequence[str], deps: Mapping[str, Sequence[str]]) -> list[str] | None:
    """First dependency cycle in declaration order (iterative DFS), as a closed path, or ``None``."""
    white, grey, black = 0, 1, 2
    color = dict.fromkeys(order, white)
    for start in order:
        if color[start] != white:
            continue
        color[start] = grey
        path = [start]
        stack: list[tuple[str, Iterator[str]]] = [(start, iter(deps[start]))]
        while stack:
            node, pending = stack[-1]
            for nxt in pending:
                if color[nxt] == grey:
                    return [*path[path.index(nxt) :], nxt]
                if color[nxt] == white:
                    color[nxt] = grey
                    path.append(nxt)
                    stack.append((nxt, iter(deps[nxt])))
                    break
            else:
                color[node] = black
                path.pop()
                stack.pop()
    return None


# ------------------------------------------------------------------------------------------------
# The adapter
# ------------------------------------------------------------------------------------------------


class ProtocolAdapter:
    """Build / parse / validate protocol messages (§3.5). Stateless apart from the configuration."""

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    # -------------------------------------------------------------------------- outbound ----
    def _outbound(
        self,
        message_type: MessageType,
        conversation: ConversationRecord,
        message_id: str,
        content: BaseModel,
    ) -> OutboundMessage:
        envelope = Envelope(
            type=message_type,
            conversation_id=_remote_id(conversation),
            message_id=message_id,
            content=content.model_dump(mode="json", exclude_none=True),
        )
        payload = envelope.model_dump(mode="json")
        canonical = canonical_json(payload)
        return OutboundMessage(
            envelope=envelope,
            payload=payload,
            canonical=canonical,
            size_bytes=len(canonical.encode("utf-8")),
            message_type=message_type,
        )

    def build_user_request(
        self,
        conversation: ConversationRecord,
        message_id: str,
        goal: str,
        user_message: str,
        budget: SessionBudget,
    ) -> OutboundMessage:
        """§12.1 — the first message of a conversation, or a follow-up after ``final_answer``."""
        content = UserRequestContent(
            goal=goal,
            user_message=user_message,
            session_budget=SessionBudgetContent(
                max_cycles=budget.max_cycles,
                max_plans=budget.max_plans,
                max_total_duration_ms=budget.max_total_duration_ms,
            ),
        )
        return self._outbound(MessageType.USER_REQUEST, conversation, message_id, content)

    def build_execution_result(
        self,
        conversation: ConversationRecord,
        message_id: str,
        content: ExecutionResultContent,
    ) -> OutboundMessage:
        """§12.5 — exactly one per plan (§19.9); the content comes from the ``ResultCollector``."""
        return self._outbound(MessageType.EXECUTION_RESULT, conversation, message_id, content)

    def build_context_resume_request(
        self,
        conversation: ConversationRecord,
        message_id: str,
        *,
        original_conversation_id: str,
        goal: str,
        context_summary: Mapping[str, Any],
        pending_message_type: MessageType,
    ) -> OutboundMessage:
        """§12.8 + ADR-014 — sent in the **child** conversation with the pending message type."""
        content = ContextResumeRequestContent(
            original_conversation_id=original_conversation_id,
            goal=goal,
            context_summary=dict(context_summary),
            pending_message_type=pending_message_type.value,
        )
        return self._outbound(MessageType.CONTEXT_RESUME_REQUEST, conversation, message_id, content)

    def build_protocol_correction_request(
        self,
        conversation: ConversationRecord,
        message_id: str,
        *,
        error: NormalizedError,
        expected: frozenset[MessageType],
        rejected_message_id: str | None,
        attempt: int,
        max_attempts: int,
    ) -> OutboundMessage:
        """ADR-023 — ask the model to fix an unusable reply instead of ending the session.

        The adapter owns the grammar, the schemas and the expectation table, so it is where the
        correction is composed: the refusal code and its details **unchanged** (``errors``), the
        types valid right now (``expected``, the ADR-007 row that was pending — a correction never
        opens a new row), a ``reminder`` generated from the content models, a minimal valid
        ``example`` of one of those types with the right ``conversation_id`` and a placeholder
        ``message_id``, and the position in the correction budget. ``raw_excerpt`` carries the
        excerpt of a reply the codec could not read (``UNPARSEABLE_REPLY``, ADR-021 §2).

        The whole message is fitted under ``payload.max_message_bytes`` (ADR-010) by shrinking, in
        order, the excerpt, the error list, the reminder and the example — never by exceeding it.
        """
        if attempt < 1 or max_attempts < 1:
            raise ValueError("a correction request needs attempt >= 1 and max_attempts >= 1")
        details = error.details
        remote = _remote_id(conversation)
        excerpt = details.get("excerpt")
        fields: dict[str, Any] = {
            "rejected_message_id": rejected_message_id,
            "error_code": error.error_code,
            "errors": _correction_errors(details),
            "expected_types": sorted(message_type.value for message_type in expected),
            "reminder": correction_reminder(error.error_code, expected, remote),
            "example": self._correction_example(expected, details, remote),
            "raw_excerpt": excerpt if isinstance(excerpt, str) else None,
            "attempt": attempt,
            "max_attempts": max_attempts,
        }
        limit = self.config.payload.max_message_bytes
        message = self._correction_message(conversation, message_id, fields)
        for shrink in _CORRECTION_SHRINK_STEPS:
            if message.size_bytes <= limit:
                return message
            shrink(fields)
            message = self._correction_message(conversation, message_id, fields)
        return message

    def _correction_message(
        self, conversation: ConversationRecord, message_id: str, fields: Mapping[str, Any]
    ) -> OutboundMessage:
        content = ProtocolCorrectionRequestContent.model_validate(dict(fields))
        return self._outbound(
            MessageType.PROTOCOL_CORRECTION_REQUEST, conversation, message_id, content
        )

    @staticmethod
    def _correction_example(
        expected: frozenset[MessageType], details: Mapping[str, Any], remote: str
    ) -> dict[str, Any]:
        """A minimal valid example of one expected type: the type the model attempted when it is
        one of them (the shape it got wrong), otherwise the first of :data:`_EXAMPLE_PREFERENCE`."""
        candidates = [
            message_type for message_type in _ordered(expected) if message_type in _EXAMPLE_CONTENT
        ]
        if not candidates:
            return {}
        received = details.get("received")
        if isinstance(received, str):
            try:
                attempted = MessageType(received)
            except ValueError:
                attempted = None
            if attempted is not None and attempted in candidates:
                return example_envelope_for(attempted, remote)
        return example_envelope_for(candidates[0], remote)

    # -------------------------------------------------------------------------- expectation -
    def expected_inbound(
        self, last_outbound: MessageRecord | None, conversation: ConversationRecord
    ) -> frozenset[MessageType]:
        """ADR-007 table with the ADR-022 flag applied (``protocol.allow_direct_response``).
        Nothing outstanding (``None``) means nothing is expected."""
        if last_outbound is None:
            return frozenset()
        return expected_inbound_for(
            situation_for(last_outbound, conversation),
            allow_direct_response=self.config.protocol.allow_direct_response,
        )

    # -------------------------------------------------------------------------- inbound -----
    def parse_inbound(
        self,
        raw_messages: Sequence[Mapping[str, Any]],
        *,
        expected: frozenset[MessageType],
        conversation: ConversationRecord,
        known_message_ids: set[str],
        known_plan_ids: set[str],
        known_task_ids: set[str],
        stored_output_task_ids: set[str],
        expected_original_conversation_id: str | None = None,
    ) -> InboundMessage:
        """Validate the messages read by one GET. Exactly one is allowed per turn (ADR-007).

        Every violation is a :class:`ProtocolError`, an **empty** list included: ``wait_for_reply``
        promises at least one message, so a reply carrying none breaks the transport contract (a
        provider whose long poll returns an empty page, ADR-020) and is handled like any other
        unusable reply (``EMPTY_REPLY``) instead of crashing the loop. Checks run in a fixed order:
        count, envelope schema, conversation, message id, direction, expectation, content schema,
        then the semantic rules of the message type (plan rules of ADR-007, ack rules of ADR-014,
        body bound of ADR-022).
        """
        if not raw_messages:
            raise ProtocolError("EMPTY_REPLY", expected=1, received=0)
        if len(raw_messages) > 1:
            raise ProtocolError(
                "UNEXPECTED_EXTRA_MESSAGE",
                expected=1,
                received=len(raw_messages),
                message_ids=[self._peek(m, "message_id") for m in raw_messages],
                types=[self._peek(m, "type") for m in raw_messages],
            )
        raw = raw_messages[0]

        try:
            envelope = Envelope.model_validate(raw)
        except ValidationError as exc:
            raise ProtocolError(
                "SCHEMA_INVALID",
                stage="envelope",
                message_type=self._peek(raw, "type"),
                errors=_pydantic_errors(exc),
            ) from exc

        expected_conversation = _remote_id(conversation)
        if envelope.conversation_id != expected_conversation:
            raise ProtocolError(
                "CONVERSATION_MISMATCH",
                received=envelope.conversation_id,
                expected=expected_conversation,
                message_id=envelope.message_id,
            )
        if envelope.message_id in known_message_ids:
            raise ProtocolError("DUPLICATE_MESSAGE_ID", message_id=envelope.message_id)

        message_type = envelope.type
        if message_type not in INBOUND_MESSAGE_TYPES:
            code = (
                "SYSTEM_ERROR_NOT_ALLOWED_INBOUND"
                if message_type is MessageType.SYSTEM_ERROR
                else "UNEXPECTED_MESSAGE_TYPE"
            )
            raise ProtocolError(
                code,
                received=message_type.value,
                expected=sorted(m.value for m in expected),
                inbound=False,
                message_id=envelope.message_id,
            )
        if message_type not in expected:
            raise ProtocolError(
                "UNEXPECTED_MESSAGE_TYPE",
                received=message_type.value,
                expected=sorted(m.value for m in expected),
                inbound=True,
                message_id=envelope.message_id,
            )

        warnings: list[str] = []
        content: InboundContent
        plan_type: PlanType | None = None
        if message_type in PLAN_MESSAGE_TYPES:
            plan = self._validate_content(envelope, PlanContent)
            self._validate_plan(
                plan, known_plan_ids, known_task_ids, stored_output_task_ids, warnings
            )
            content = plan
            plan_type = plan_type_for_message(message_type)
        elif message_type is MessageType.FINAL_ANSWER:
            content = self._validate_content(envelope, FinalAnswerContent)
        elif message_type is MessageType.USER_RESPONSE:
            response = self._validate_content(envelope, UserResponseContent)
            self._validate_user_response(response, envelope.message_id)
            content = response
        else:
            ack = self._validate_content(envelope, ContextResumeAckContent)
            self._validate_ack(ack, expected_original_conversation_id)
            content = ack

        return InboundMessage(
            envelope=envelope,
            content=content,
            message_type=message_type,
            plan_type=plan_type,
            warnings=warnings,
            size_bytes=size_bytes(raw),
        )

    @staticmethod
    def _peek(raw: Any, key: str) -> Any:
        """Best-effort read of an envelope field from an unvalidated message (for error details)."""
        return peek_field(raw, key)

    @staticmethod
    def _validate_content(envelope: Envelope, model: type[ContentT]) -> ContentT:
        try:
            return model.model_validate(envelope.content)
        except ValidationError as exc:
            raise ProtocolError(
                "SCHEMA_INVALID",
                stage="content",
                message_type=envelope.type.value,
                message_id=envelope.message_id,
                errors=_pydantic_errors(exc, prefix="content"),
            ) from exc

    def _validate_plan(
        self,
        plan: PlanContent,
        known_plan_ids: set[str],
        known_task_ids: set[str],
        stored_output_task_ids: set[str],
        warnings: list[str],
    ) -> None:
        """Structural rules of ADR-007 beyond the JSON schema, plus the ADR-005 bound."""
        if plan.plan_id in known_plan_ids:
            raise ProtocolError("DUPLICATE_PLAN_ID", plan_id=plan.plan_id)

        order: list[str] = []
        for task in plan.tasks:
            if task.task_id in order:
                raise ProtocolError(
                    "DUPLICATE_TASK_ID", task_id=task.task_id, plan_id=plan.plan_id, scope="plan"
                )
            if task.task_id in known_task_ids:
                raise ProtocolError(
                    "DUPLICATE_TASK_ID", task_id=task.task_id, plan_id=plan.plan_id, scope="session"
                )
            order.append(task.task_id)
        ids = set(order)

        deps: dict[str, list[str]] = {}
        for task in plan.tasks:
            for dependency in task.depends_on:
                if dependency == task.task_id:
                    raise ProtocolError(
                        "SELF_DEPENDENCY", task_id=task.task_id, plan_id=plan.plan_id
                    )
                if dependency not in ids:
                    raise ProtocolError(
                        "UNKNOWN_DEPENDENCY",
                        task_id=task.task_id,
                        dependency=dependency,
                        plan_id=plan.plan_id,
                    )
            deps[task.task_id] = list(task.depends_on)
            if (
                task.type is TaskType.CHUNK_REQUEST
                and task.ref_task_id not in stored_output_task_ids
            ):
                raise ProtocolError(
                    "CHUNK_REF_UNKNOWN",
                    task_id=task.task_id,
                    ref_task_id=task.ref_task_id,
                    plan_id=plan.plan_id,
                )

        cycle = _find_cycle(order, deps)
        if cycle is not None:
            raise ProtocolError("DEPENDENCY_CYCLE", cycle=cycle, plan_id=plan.plan_id)

        if plan.execution_policy is ExecutionPolicy.SEQUENTIAL:
            position = {task_id: index for index, task_id in enumerate(order)}
            for task in plan.tasks:
                for dependency in task.depends_on:
                    if position[dependency] > position[task.task_id]:
                        raise ProtocolError(
                            "FORWARD_DEPENDENCY_IN_SEQUENTIAL",
                            task_id=task.task_id,
                            dependency=dependency,
                            plan_id=plan.plan_id,
                            execution_policy=plan.execution_policy.value,
                        )

        if plan.state_summary is not None:
            summary_size = size_bytes(plan.state_summary.model_dump(mode="json"))
            limit = self.config.payload.max_state_summary_bytes
            if summary_size > limit:
                raise ProtocolError(
                    "STATE_SUMMARY_TOO_LARGE",
                    size_bytes=summary_size,
                    max_bytes=limit,
                    plan_id=plan.plan_id,
                )

        if plan.execution_policy is ExecutionPolicy.PARALLEL and plan.max_parallel_workers is None:
            warnings.append("DEFAULT_WORKERS_APPLIED")
        if (
            plan.execution_policy is ExecutionPolicy.SEQUENTIAL
            and plan.max_parallel_workers is not None
            and plan.max_parallel_workers != 1
        ):
            warnings.append("WORKERS_IGNORED_IN_SEQUENTIAL")
        for task in plan.tasks:
            if task.critical and task.continue_on_error:
                warnings.append(f"CONTRADICTORY_FLAGS:{task.task_id}")

    def _validate_user_response(self, response: UserResponseContent, message_id: str) -> None:
        """ADR-022: the opaque body is never parsed; its only semantic rule is the size bound."""
        body_bytes = len(response.body.encode("utf-8"))
        limit = self.config.payload.max_message_bytes
        if body_bytes > limit:
            raise ProtocolError(
                "USER_RESPONSE_TOO_LARGE",
                size_bytes=body_bytes,
                max_bytes=limit,
                message_id=message_id,
            )

    @staticmethod
    def _validate_ack(ack: ContextResumeAckContent, expected_original: str | None) -> None:
        if expected_original is not None and ack.original_conversation_id != expected_original:
            raise ProtocolError(
                "ACK_WRONG_ORIGINAL",
                received=ack.original_conversation_id,
                expected=expected_original,
            )
        if not ack.acknowledged:
            raise ProtocolError(
                "ACK_NOT_ACKNOWLEDGED", original_conversation_id=ack.original_conversation_id
            )

    # -------------------------------------------------------------------------- records -----
    def plan_to_records(
        self,
        inbound: InboundMessage,
        *,
        session: SessionRecord,
        conversation: ConversationRecord,
        cycle_id: str,
        clock: Clock,
    ) -> tuple[PlanRecord, list[TaskRecord]]:
        """Project an accepted plan onto a PENDING ``PlanRecord`` and its PENDING ``TaskRecord``s."""
        plan = inbound.content
        if inbound.plan_type is None or not isinstance(plan, PlanContent):
            raise ValueError(f"{inbound.message_type.value} carries no plan")
        now = clock.now()
        workers = 1
        if plan.execution_policy is ExecutionPolicy.PARALLEL and plan.max_parallel_workers:
            workers = plan.max_parallel_workers
        plan_record = PlanRecord(
            plan_id=plan.plan_id,
            session_id=session.session_id,
            conversation_id=conversation.conversation_id,
            cycle_id=cycle_id,
            plan_type=inbound.plan_type,
            objective=plan.objective,
            execution_policy=plan.execution_policy,
            max_parallel_workers=workers,
            status=PlanState.PENDING,
            task_count=len(plan.tasks),
            default_max_output_bytes=plan.default_max_output_bytes,
            state_summary=(
                plan.state_summary.model_dump(mode="json") if plan.state_summary else None
            ),
            created_at=now,
            updated_at=now,
        )
        tasks = [
            self._task_record(task, index, plan_record, now)
            for index, task in enumerate(plan.tasks)
        ]
        return plan_record, tasks

    def _task_record(
        self, task: TaskMessage, index: int, plan: PlanRecord, now: datetime
    ) -> TaskRecord:
        payload_cfg, exec_cfg = self.config.payload, self.config.execution
        critical = bool(task.critical)
        continue_on_error = bool(task.continue_on_error)
        stop_plan_on_failure = bool(task.stop_plan_on_failure)

        declared_budget = task.max_output_bytes
        if declared_budget is None:
            declared_budget = plan.default_max_output_bytes
        if declared_budget is None:
            declared_budget = payload_cfg.default_max_output_bytes
        output_applied = min(declared_budget, payload_cfg.hard_max_output_bytes)

        timeout_applied: int | None
        max_bytes: int | None = None
        stream = None
        if task.type is TaskType.CHUNK_REQUEST:
            # ADR-011: max_bytes capped by the hard limit, then by the task's own declared budget;
            # ADR-008 §5: a local read has no timeout. The chunk budget is the task's output budget.
            declared_max_bytes = (
                task.max_bytes if task.max_bytes is not None else payload_cfg.hard_max_output_bytes
            )
            max_bytes = min(declared_max_bytes, payload_cfg.hard_max_output_bytes)
            if task.max_output_bytes is not None:
                max_bytes = min(max_bytes, task.max_output_bytes)
            output_applied = max_bytes
            stream = task.effective_stream
            timeout_applied = None
        else:
            timeout_applied = min(
                task.timeout_ms
                if task.timeout_ms is not None
                else exec_cfg.default_task_timeout_ms,
                exec_cfg.max_task_timeout_ms,
            )

        return TaskRecord(
            task_id=task.task_id,
            plan_id=plan.plan_id,
            session_id=plan.session_id,
            conversation_id=plan.conversation_id,
            order_index=index,
            type=task.type,
            cmd=task.cmd,
            status=TaskState.PENDING,
            critical=critical,
            continue_on_error=continue_on_error,
            stop_plan_on_failure=stop_plan_on_failure,
            stop_plan_on_success=bool(task.stop_plan_on_success),
            stops_plan_on_failure=critical or stop_plan_on_failure or not continue_on_error,
            depends_on=tuple(task.depends_on),
            resource_lock=task.resource_lock,
            max_output_bytes=task.max_output_bytes,
            max_output_bytes_applied=output_applied,
            timeout_ms=task.timeout_ms,
            timeout_ms_applied=timeout_applied,
            ref_task_id=task.ref_task_id,
            stream=stream,
            byte_offset=task.byte_offset,
            max_bytes=max_bytes,
            created_at=now,
            updated_at=now,
        )

    # -------------------------------------------------------------------------- instructions
    @staticmethod
    def render_instructions(config: AppConfig) -> str:
        """See :func:`render_instructions`."""
        return render_instructions(config)
