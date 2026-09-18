"""ProtocolAdapter — build outbound messages, parse and validate inbound ones (spec §3.5).

The adapter is **pure**: no I/O, no clock of its own (``plan_to_records`` receives the ``Clock``),
no identifier generation (``message_id`` is passed in by the caller, ADR-017). It owns:

- the construction of the three outbound types (§12.1, §12.5, §12.8 + ADR-014) as canonical JSON;
- the **table of expected inbound messages** of ADR-007 (:data:`EXPECTED_INBOUND`), amended by
  ADR-022: ``user_response`` after an ``execution_result`` or a follow-up ``user_request``, and
  after the initial ``user_request`` only when ``protocol.allow_direct_response`` is set
  (:func:`expected_inbound_for`);
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
from collections.abc import Iterator, Mapping, Sequence
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
from agentic_local_app.domain.errors import ProtocolError
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
    SessionBudgetContent,
    TaskMessage,
    UserRequestContent,
    UserResponseContent,
)

__all__ = [
    "EXPECTED_INBOUND",
    "INSTRUCTIONS_FILENAME",
    "InboundContent",
    "InboundMessage",
    "OutboundMessage",
    "OutboundSituation",
    "ProtocolAdapter",
    "expected_inbound_for",
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


def situation_for(
    last_outbound: MessageRecord, conversation: ConversationRecord
) -> OutboundSituation:
    """Classify the last outbound message into a row of :data:`EXPECTED_INBOUND`.

    ``conversation.final_answer_received`` means "the model concluded a turn in this conversation
    with a ``final_answer`` or a ``user_response``" (ADR-022): the next ``user_request`` is then a
    follow-up, whatever the type of that concluding message.
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


def render_instructions(config: AppConfig) -> str:
    """The protocol text sent to the model at init, with the configured limits injected and the
    first-message rule of ADR-022 rendered from ``protocol.allow_direct_response``."""
    direct = config.protocol.allow_direct_response
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
    }

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            raise ValueError(f"unknown placeholder in {INSTRUCTIONS_FILENAME}: {{{name}}}")
        return str(values[name])

    return _PLACEHOLDER_RE.sub(substitute, _instructions_template())


# ------------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------------


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

        Raises ``ValueError`` on an empty list (the caller must not call), :class:`ProtocolError`
        for every protocol violation. Checks run in a fixed order: count, envelope schema,
        conversation, message id, direction, expectation, content schema, then the semantic rules
        of the message type (plan rules of ADR-007, ack rules of ADR-014, body bound of ADR-022).
        """
        if not raw_messages:
            raise ValueError("parse_inbound requires at least one message")
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
        if isinstance(raw, Mapping):
            value = raw.get(key)
            return (
                value if isinstance(value, (str, int, float, bool)) or value is None else str(value)
            )
        return None

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
