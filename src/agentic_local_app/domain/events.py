"""Events published on the EventBus (ADR-015, ADR-018).

Every event is small, structured and self-describing: the observability components (AuditLog,
ExecutionTracker, TelemetryService, SSE stream) consume nothing else. ``task.output`` events carry
live output chunks and are the only events **not** written to the audit log (ADR-018).

``session.paused`` (ADR-025) follows the ``session.state_changed`` that wrote ``PAUSED``: it says
why the loop stopped (``reason``, ``error_code``, ``error_type``, ``operation``) and which message
was in flight (``message_id``), so an interface can explain the pause without reading the failures.
"""

from __future__ import annotations

from datetime import datetime
from enum import unique
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.domain.states import StrEnum


@unique
class EventType(StrEnum):
    SESSION_CREATED = "session.created"
    SESSION_STATE_CHANGED = "session.state_changed"
    SESSION_PAUSED = "session.paused"
    CONVERSATION_CREATED = "conversation.created"
    CONVERSATION_STATE_CHANGED = "conversation.state_changed"
    CYCLE_STARTED = "cycle.started"
    CYCLE_ENDED = "cycle.ended"
    MESSAGE_OUTBOUND = "message.outbound"
    MESSAGE_INBOUND = "message.inbound"
    MESSAGE_REJECTED = "message.rejected"
    MESSAGE_RETRANSMITTED = "message.retransmitted"
    CORRECTION_REQUESTED = "correction.requested"
    PLAN_RECEIVED = "plan.received"
    PLAN_STATE_CHANGED = "plan.state_changed"
    TASK_STATE_CHANGED = "task.state_changed"
    TASK_OUTPUT = "task.output"
    FINAL_ANSWER_RECEIVED = "final_answer.received"
    USER_RESPONSE_RECEIVED = "user_response.received"
    FAILURE_RECORDED = "failure.recorded"
    RETRY_SCHEDULED = "retry.scheduled"
    BREAKER_STATE_CHANGED = "breaker.state_changed"
    CONTEXT_WINDOW_STATE_CHANGED = "context.window_state_changed"
    ROTATION_STARTED = "rotation.started"
    ROTATION_COMPLETED = "rotation.completed"
    ROTATION_FAILED = "rotation.failed"
    BUDGET_UPDATED = "budget.updated"
    BUDGET_EXCEEDED = "budget.exceeded"
    INTERRUPTION_REQUESTED = "interruption.requested"
    INTERRUPTION_COMPLETED = "interruption.completed"
    RECOVERY_STARTED = "recovery.started"
    RECOVERY_ACTION = "recovery.action"
    RECOVERY_COMPLETED = "recovery.completed"
    AUDIT_WARNING = "audit.warning"


#: Events excluded from the audit chain (volume) - ADR-018.
NON_AUDITED_EVENT_TYPES: frozenset[EventType] = frozenset({EventType.TASK_OUTPUT})


class Event(BaseModel):
    """An event on the bus. ``payload`` is JSON-serialisable and never contains raw bytes."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: EventType
    timestamp: datetime
    session_id: str
    conversation_id: str | None = None
    cycle_id: str | None = None
    plan_id: str | None = None
    task_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @property
    def audited(self) -> bool:
        return self.event_type not in NON_AUDITED_EVENT_TYPES


def state_change_payload(
    previous: str | None, current: str, reason: str | None = None
) -> dict[str, Any]:
    """Uniform payload for every ``*.state_changed`` event."""
    payload: dict[str, Any] = {"from": previous, "to": current}
    if reason is not None:
        payload["reason"] = reason
    return payload
