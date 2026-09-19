"""Enumerations of every state and type used by the system.

State names follow the specification (§5) verbatim, in upper case. Protocol-level values that
appear inside JSON messages (message types, execution policy, plan status in execution_result)
use the lower-case spelling of the specification (§12).

Amendments to the specification are documented in docs/adr/ADR-006 and ADR-007:
- ``SessionState`` is new: READY lives at session level, INTERRUPTED is terminal for a conversation.
- ``CycleState`` is new (the spec lists a cycle ``status`` field without defining its values).
- ``CircuitState`` supports the CircuitBreaker (§7.4, phase 7 tests: open / half-open / close).
"""

from __future__ import annotations

from enum import StrEnum, unique

__all__ = [
    "StrEnum",
    "SessionState",
    "ConversationState",
    "CycleType",
    "CycleState",
    "PlanType",
    "ExecutionPolicy",
    "PlanState",
    "TaskType",
    "TaskState",
    "OutputStream",
    "ContextWindowState",
    "CircuitState",
    "MessageType",
    "MessageDirection",
    "PLAN_MESSAGE_TYPES",
    "OUTBOUND_MESSAGE_TYPES",
    "SUBSTANTIVE_OUTBOUND_MESSAGE_TYPES",
    "INBOUND_MESSAGE_TYPES",
    "CONCLUDING_MESSAGE_TYPES",
    "plan_type_for_message",
    "cycle_type_for_plan",
]


@unique
class SessionState(StrEnum):
    READY = "READY"
    RUNNING = "RUNNING"
    INTERRUPTING = "INTERRUPTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@unique
class ConversationState(StrEnum):
    NEW = "NEW"
    ACTIVE = "ACTIVE"
    WAITING_MODEL_RESPONSE = "WAITING_MODEL_RESPONSE"
    RUNNING_PLAN = "RUNNING_PLAN"
    WAITING_USER = "WAITING_USER"
    ROTATING = "ROTATING"
    INTERRUPTED = "INTERRUPTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


@unique
class CycleType(StrEnum):
    DISCOVERY = "discovery"
    EXECUTION = "execution"
    CLARIFICATION = "clarification"
    RESUME = "resume"


@unique
class CycleState(StrEnum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


@unique
class PlanType(StrEnum):
    DISCOVERY_PLAN = "discovery_plan"
    EXECUTION_PLAN = "execution_plan"
    PRIORITY_CLARIFICATION = "priority_clarification"


@unique
class ExecutionPolicy(StrEnum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


@unique
class PlanState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    STOPPED_ON_FAILURE = "STOPPED_ON_FAILURE"
    SHORT_CIRCUITED_ON_SUCCESS = "SHORT_CIRCUITED_ON_SUCCESS"
    INTERRUPTED = "INTERRUPTED"
    FAILED = "FAILED"

    @property
    def protocol_value(self) -> str:
        """Spelling used in ``execution_result.content.status`` (§12.5, ADR-009)."""
        return self.value.lower()


@unique
class TaskType(StrEnum):
    CMD = "cmd"
    CHUNK_REQUEST = "chunk_request"


@unique
class TaskState(StrEnum):
    PENDING = "PENDING"
    WAITING_DEPENDENCY = "WAITING_DEPENDENCY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"
    INTERRUPTED = "INTERRUPTED"

    @property
    def protocol_value(self) -> str:
        return self.value.lower()


@unique
class OutputStream(StrEnum):
    STDOUT = "stdout"
    STDERR = "stderr"


@unique
class ContextWindowState(StrEnum):
    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    SATURATED = "SATURATED"


@unique
class CircuitState(StrEnum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@unique
class MessageType(StrEnum):
    """Protocol message types (§3.5). ``chunk_request`` is kept for completeness but is a task type
    in practice (§12.6, ADR-007); ``system_error`` is internal and never sent to the model;
    ``user_response`` is the model's direct, opaque answer to the user (ADR-022);
    ``protocol_correction_request`` is the application asking the model to fix an unusable reply
    (ADR-023), outbound only like ``user_request``."""

    USER_REQUEST = "user_request"
    DISCOVERY_PLAN = "discovery_plan"
    EXECUTION_PLAN = "execution_plan"
    PRIORITY_CLARIFICATION = "priority_clarification"
    EXECUTION_RESULT = "execution_result"
    FINAL_ANSWER = "final_answer"
    USER_RESPONSE = "user_response"
    CONTEXT_RESUME_REQUEST = "context_resume_request"
    CONTEXT_RESUME_ACK = "context_resume_ack"
    PROTOCOL_CORRECTION_REQUEST = "protocol_correction_request"
    CHUNK_REQUEST = "chunk_request"
    SYSTEM_ERROR = "system_error"


PLAN_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.DISCOVERY_PLAN, MessageType.EXECUTION_PLAN, MessageType.PRIORITY_CLARIFICATION}
)

#: The outbound types that set the expectation of the next inbound message (ADR-007 rows).
#: ``protocol_correction_request`` is deliberately **not** one of them: it is transparent for the
#: expectation table (ADR-023) — it asks again for the reply the last substantive message expects.
SUBSTANTIVE_OUTBOUND_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.USER_REQUEST, MessageType.EXECUTION_RESULT, MessageType.CONTEXT_RESUME_REQUEST}
)

OUTBOUND_MESSAGE_TYPES: frozenset[MessageType] = SUBSTANTIVE_OUTBOUND_MESSAGE_TYPES | {
    MessageType.PROTOCOL_CORRECTION_REQUEST
}

INBOUND_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    PLAN_MESSAGE_TYPES
    | {MessageType.FINAL_ANSWER, MessageType.USER_RESPONSE, MessageType.CONTEXT_RESUME_ACK}
)

#: The two inbound types that conclude the model's turn without a plan (§11, ADR-022).
CONCLUDING_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.FINAL_ANSWER, MessageType.USER_RESPONSE}
)


def plan_type_for_message(message_type: MessageType) -> PlanType:
    """Map a plan-bearing message type onto its ``PlanType``."""
    mapping = {
        MessageType.DISCOVERY_PLAN: PlanType.DISCOVERY_PLAN,
        MessageType.EXECUTION_PLAN: PlanType.EXECUTION_PLAN,
        MessageType.PRIORITY_CLARIFICATION: PlanType.PRIORITY_CLARIFICATION,
    }
    return mapping[message_type]


def cycle_type_for_plan(plan_type: PlanType) -> CycleType:
    mapping = {
        PlanType.DISCOVERY_PLAN: CycleType.DISCOVERY,
        PlanType.EXECUTION_PLAN: CycleType.EXECUTION,
        PlanType.PRIORITY_CLARIFICATION: CycleType.CLARIFICATION,
    }
    return mapping[plan_type]


@unique
class MessageDirection(StrEnum):
    OUTBOUND = "outbound"
    INBOUND = "inbound"
