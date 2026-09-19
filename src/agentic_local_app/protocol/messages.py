"""Protocol message schemas — spec §12 verbatim, plus the additive extensions of the ADRs.

Every extension is **optional** so that the examples of the specification validate unchanged:

- ``Task.timeout_ms`` (ADR-008), ``Task.stream`` (ADR-011)
- ``PlanContent.default_max_output_bytes`` (ADR-010), ``PlanContent.state_summary`` (ADR-005)
- ``TaskResult.*_total``, ``*_range``, ``max_output_bytes_applied``, ``timed_out``, chunk fields (ADR-011)
- ``ExecutionResultContent.skipped_tasks`` etc. carry ``{task_id, reason}`` objects (ADR-009)
- ``ContextResumeRequestContent.pending_message_type`` (ADR-014)
- ``UserResponseContent``, a new inbound type: the model's direct answer to the user (ADR-022)
- ``ProtocolCorrectionRequestContent``, a new outbound type: the application asks the model to fix
  an unusable reply instead of ending the session on the first fault (ADR-023)

The envelope (``type``, ``conversation_id``, ``message_id``, ``content``) is common to every message.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agentic_local_app.domain.states import (
    ExecutionPolicy,
    MessageType,
    OutputStream,
    TaskType,
)


class ProtocolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# ------------------------------------------------------------------------------------------------
# 12.1 user_request
# ------------------------------------------------------------------------------------------------
class SessionBudgetContent(ProtocolModel):
    max_cycles: int = Field(gt=0)
    max_plans: int = Field(gt=0)
    max_total_duration_ms: int = Field(gt=0)


class UserRequestContent(ProtocolModel):
    goal: str
    user_message: str
    session_budget: SessionBudgetContent


# ------------------------------------------------------------------------------------------------
# 12.2 / 12.3 / 12.4 plans (identical structure, ADR-005 state_summary, ADR-010 default budget)
# ------------------------------------------------------------------------------------------------
class StateSummary(BaseModel):
    """The model's own running notes (ADR-005). Extra keys are allowed; the size is bounded."""

    model_config = ConfigDict(extra="allow", frozen=True)

    environment: dict[str, Any] = Field(default_factory=dict)
    findings: list[str] = Field(default_factory=list)
    current_state: str | None = None
    next_expected_step: str | None = None


class TaskMessage(ProtocolModel):
    """A task as sent by the model (§12.2). Absent flags mean ``False`` (ADR-009)."""

    task_id: str = Field(min_length=1)
    type: TaskType = TaskType.CMD
    cmd: str | None = None
    critical: bool | None = None
    continue_on_error: bool | None = None
    stop_plan_on_failure: bool | None = None
    stop_plan_on_success: bool | None = None
    depends_on: list[str] = Field(default_factory=list)
    resource_lock: str | None = None
    max_output_bytes: int | None = Field(default=None, gt=0)
    timeout_ms: int | None = Field(default=None, gt=0)
    # chunk_request (§12.6, ADR-011)
    ref_task_id: str | None = None
    stream: OutputStream | None = None
    byte_offset: int | None = Field(default=None, ge=0)
    max_bytes: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _shape_matches_type(self) -> TaskMessage:
        if self.type is TaskType.CMD:
            if not self.cmd or not self.cmd.strip():
                raise ValueError("a cmd task requires a non-empty cmd")
            if (
                self.ref_task_id is not None
                or self.byte_offset is not None
                or self.max_bytes is not None
            ):
                raise ValueError("chunk fields are not allowed on a cmd task")
        else:
            if self.cmd is not None:
                raise ValueError("a chunk_request task must not carry a cmd")
            if not self.ref_task_id:
                raise ValueError("a chunk_request task requires ref_task_id")
            if self.byte_offset is None or self.max_bytes is None:
                raise ValueError("a chunk_request task requires byte_offset and max_bytes")
        return self

    @property
    def effective_stream(self) -> OutputStream:
        return self.stream or OutputStream.STDOUT


class PlanContent(ProtocolModel):
    plan_id: str = Field(min_length=1)
    objective: str
    execution_policy: ExecutionPolicy
    max_parallel_workers: int | None = Field(default=None, ge=1)
    default_max_output_bytes: int | None = Field(default=None, gt=0)
    state_summary: StateSummary | None = None
    tasks: list[TaskMessage] = Field(min_length=1)


# ------------------------------------------------------------------------------------------------
# 12.5 execution_result (+ ADR-009 reasons, ADR-011 ranges and chunk results)
# ------------------------------------------------------------------------------------------------
class TaskResult(ProtocolModel):
    task_id: str
    status: str  # lower-case TaskState (completed | failed | timed_out)
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    truncated: bool = False
    original_size_bytes: int | None = None
    stdout_total: int | None = None
    stderr_total: int | None = None
    stdout_range: tuple[int, int] | None = None
    stderr_range: tuple[int, int] | None = None
    max_output_bytes_applied: int | None = None
    timed_out: bool = False
    timeout_ms_applied: int | None = None
    duration_ms: int | None = None
    reason: str | None = None
    # chunk_request result (ADR-011)
    ref_task_id: str | None = None
    stream: OutputStream | None = None
    range: tuple[int, int] | None = None
    total: int | None = None
    eof: bool | None = None
    data: str | None = None


class TaskRef(ProtocolModel):
    task_id: str
    reason: str


class ExecutionResultContent(ProtocolModel):
    plan_id: str
    status: str  # lower-case PlanState (completed | stopped_on_failure | short_circuited_on_success | failed)
    results: list[TaskResult] = Field(default_factory=list)
    skipped_tasks: list[TaskRef] = Field(default_factory=list)
    cancelled_tasks: list[TaskRef] = Field(default_factory=list)
    interrupted_tasks: list[TaskRef] = Field(default_factory=list)
    stop_reason: str | None = None


# ------------------------------------------------------------------------------------------------
# 12.7 final_answer — the model may add fields (extra allowed)
# ------------------------------------------------------------------------------------------------
class FinalAnswerContent(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    status: str
    diagnosis: str
    evidence: list[str] = Field(default_factory=list)
    recommended_next_step: str | None = None


# ------------------------------------------------------------------------------------------------
# user_response — the model answers the user directly (ADR-022, not in §12)
# ------------------------------------------------------------------------------------------------
class UserResponseContent(ProtocolModel):
    """A text answer, an analysis or a question for the user, without any command.

    ``body`` is **opaque**: the application never parses it, whatever ``format`` says (``json``
    only tells the user's interface how to render it). Its only bounds are "non-empty" here and
    ``payload.max_message_bytes`` in the adapter (``USER_RESPONSE_TOO_LARGE``). ``expects_reply``
    marks a question: the conversation then stays reusable even under auto-close (§11, ADR-022).
    """

    format: Literal["text", "markdown", "json"] = "text"
    body: str = Field(min_length=1)
    status: Literal["completed", "partial", "failed"] = "completed"
    expects_reply: bool = False


# ------------------------------------------------------------------------------------------------
# protocol_correction_request — the application asks for a fix (ADR-023, not in §12)
# ------------------------------------------------------------------------------------------------
class ProtocolCorrectionRequestContent(ProtocolModel):
    """What the application sends back when the model's reply is unusable (ADR-023).

    It is **outbound only** (application → model) and carries everything the model needs to send a
    correct message without guessing: the code of the refusal, the validation details exactly as
    the adapter produced them, the message types valid *right now* (the ADR-007 row that was
    pending, unchanged by the fault), a reminder of their shape generated from the content models,
    a minimal valid ``example`` to copy, and where the model stands in the correction budget.

    ``rejected_message_id`` is absent when the reply carried no readable identifier;
    ``raw_excerpt`` is present for ``UNPARSEABLE_REPLY``, where there is no envelope to quote.
    """

    rejected_message_id: str | None = None
    error_code: str = Field(min_length=1)
    errors: list[dict[str, Any]] = Field(default_factory=list)
    expected_types: list[str] = Field(default_factory=list)
    reminder: str = ""
    example: dict[str, Any] = Field(default_factory=dict)
    raw_excerpt: str | None = None
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1)


# ------------------------------------------------------------------------------------------------
# 12.8 / 12.9 context resume (+ ADR-014 pending_message_type)
# ------------------------------------------------------------------------------------------------
class ContextResumeRequestContent(ProtocolModel):
    original_conversation_id: str
    goal: str
    context_summary: dict[str, Any]
    # ADR-014: optional so that the §12.8 example validates; the application always sets it
    pending_message_type: str | None = None


class ContextResumeAckContent(ProtocolModel):
    original_conversation_id: str
    acknowledged: bool


# ------------------------------------------------------------------------------------------------
# 12.10 system_error — internal only (ADR-007)
# ------------------------------------------------------------------------------------------------
class SystemErrorContent(ProtocolModel):
    error_type: str
    error_code: str
    severity: str
    origin: str
    retryable: bool
    recoverable: bool
    attempt: int
    max_attempts: int
    details: dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------------------------------------
# Envelope
# ------------------------------------------------------------------------------------------------
class Envelope(ProtocolModel):
    """The common shape of every message (§12): ``type``, ``conversation_id``, ``message_id``, ``content``."""

    type: MessageType
    conversation_id: str = Field(min_length=1)
    message_id: str = Field(min_length=1)
    content: dict[str, Any]


CONTENT_MODELS: dict[MessageType, type[BaseModel]] = {
    MessageType.USER_REQUEST: UserRequestContent,
    MessageType.DISCOVERY_PLAN: PlanContent,
    MessageType.EXECUTION_PLAN: PlanContent,
    MessageType.PRIORITY_CLARIFICATION: PlanContent,
    MessageType.EXECUTION_RESULT: ExecutionResultContent,
    MessageType.FINAL_ANSWER: FinalAnswerContent,
    MessageType.USER_RESPONSE: UserResponseContent,
    MessageType.PROTOCOL_CORRECTION_REQUEST: ProtocolCorrectionRequestContent,
    MessageType.CONTEXT_RESUME_REQUEST: ContextResumeRequestContent,
    MessageType.CONTEXT_RESUME_ACK: ContextResumeAckContent,
    MessageType.SYSTEM_ERROR: SystemErrorContent,
}


def content_model_for(message_type: MessageType) -> type[BaseModel]:
    """The pydantic model validating ``content`` for a message type (``chunk_request`` has none:
    it is a task type carried inside an ``execution_plan``)."""
    try:
        return CONTENT_MODELS[message_type]
    except KeyError as exc:
        raise ValueError(f"{message_type.value} is not a standalone message type") from exc
