"""Persisted records (spec §16, extended by the ADRs) and a few value objects.

Records are immutable pydantic models: a state change produces a new record with
``record.model_copy(update={...})`` which is persisted **before** any event is published or any
action taken (ADR-015). Identifiers are plain strings; all timestamps are timezone-aware UTC.

Uniqueness scopes (ADR-007): ``plan_id`` and ``task_id`` come from the model and are unique within a
**session** (so that ``chunk_request.ref_task_id`` stays unambiguous across rotations). Stores key
plans by ``(session_id, plan_id)`` and tasks by ``(session_id, task_id)``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.domain.errors import ErrorType, Severity
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
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


class Record(BaseModel):
    """Base of every persisted record: frozen, strict about unknown fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")


# ------------------------------------------------------------------------------------------------
# Session (ADR-006, ADR-012)
# ------------------------------------------------------------------------------------------------
class SessionBudget(Record):
    """§2.8 / §12.1 ``session_budget``."""

    max_cycles: int = Field(gt=0)
    max_plans: int = Field(gt=0)
    max_total_duration_ms: int = Field(gt=0)


class SessionRecord(Record):
    session_id: str
    status: SessionState
    goal: str
    user_message: str
    user_id: str
    auto_close_on_final_answer: bool
    budget: SessionBudget
    consumed_cycles: int = 0
    consumed_plans: int = 0
    rotations_count: int = 0
    current_conversation_id: str | None = None
    final_answer: dict[str, Any] | None = None
    last_failure_id: str | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    interrupted_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


# ------------------------------------------------------------------------------------------------
# Conversation (§16 ConversationRecord + ADR-004/007/013/014)
# ------------------------------------------------------------------------------------------------
class ConversationRecord(Record):
    conversation_id: str
    session_id: str
    parent_conversation_id: str | None = None
    remote_conversation_id: str | None = None
    status: ConversationState
    auto_close_on_final_answer: bool
    context_window_state: ContextWindowState = ContextWindowState.HEALTHY
    context_bytes: int = 0
    protocol_error_count: int = 0
    last_model_response_state: str = "none"
    current_cycle_id: str | None = None
    current_plan_id: str | None = None
    last_completed_plan_id: str | None = None
    final_answer_received: bool = False
    last_outbound_message_id: str | None = None
    last_inbound_message_id: str | None = None
    get_cursor: str | None = None
    session_budget_json: dict[str, Any] = Field(default_factory=dict)
    closure_reason: str | None = None
    interrupted_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


# ------------------------------------------------------------------------------------------------
# Cycle (§16 CycleRecord + ADR-007)
# ------------------------------------------------------------------------------------------------
class CycleRecord(Record):
    cycle_id: str
    conversation_id: str
    session_id: str
    cycle_type: CycleType
    status: CycleState
    retry_count: int = 0
    outbound_message_id: str | None = None
    inbound_message_id: str | None = None
    plan_id: str | None = None
    started_at: datetime
    ended_at: datetime | None = None


# ------------------------------------------------------------------------------------------------
# Plan (§16 PlanRecord + §4.1 counters + ADR-005/010)
# ------------------------------------------------------------------------------------------------
class PlanRecord(Record):
    plan_id: str
    session_id: str
    conversation_id: str
    cycle_id: str
    plan_type: PlanType
    objective: str
    execution_policy: ExecutionPolicy
    max_parallel_workers: int = 1
    status: PlanState
    stop_reason: str | None = None
    task_count: int = 0
    completed_task_count: int = 0
    failed_task_count: int = 0
    skipped_task_count: int = 0
    cancelled_task_count: int = 0
    interrupted_task_count: int = 0
    default_max_output_bytes: int | None = None
    state_summary: dict[str, Any] | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


# ------------------------------------------------------------------------------------------------
# Task (§16 TaskRecord + §4.1 + ADR-008/009/011/016)
# ------------------------------------------------------------------------------------------------
class TaskRecord(Record):
    task_id: str
    plan_id: str
    session_id: str
    conversation_id: str
    order_index: int
    type: TaskType
    cmd: str | None = None
    status: TaskState
    # flags as declared by the model (None = absent in the message)
    critical: bool = False
    continue_on_error: bool = False
    stop_plan_on_failure: bool = False
    stop_plan_on_success: bool = False
    stops_plan_on_failure: bool = True  # effective rule of ADR-009
    depends_on: tuple[str, ...] = ()
    resource_lock: str | None = None
    max_output_bytes: int | None = None
    max_output_bytes_applied: int | None = None
    timeout_ms: int | None = None
    timeout_ms_applied: int | None = None
    # chunk_request fields (ADR-011)
    ref_task_id: str | None = None
    stream: OutputStream | None = None
    byte_offset: int | None = None
    max_bytes: int | None = None
    # outcome
    attempt_count: int = 0
    exit_code: int | None = None
    timed_out: bool = False
    stdout_ref: str | None = None
    stderr_ref: str | None = None
    truncated: bool = False
    original_size_bytes: int | None = None
    stdout_total: int | None = None
    stderr_total: int | None = None
    stdout_range: tuple[int, int] | None = None
    stderr_range: tuple[int, int] | None = None
    reason: str | None = None  # skip / cancel / interrupt / failure code
    pid: int | None = None
    process_group_id: int | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    duration_ms: int | None = None
    created_at: datetime
    updated_at: datetime


# ------------------------------------------------------------------------------------------------
# Protocol messages exchanged (needed for recovery §7.5, debugging API ADR-018, context bytes)
# ------------------------------------------------------------------------------------------------
class MessageRecord(Record):
    message_id: str
    session_id: str
    conversation_id: str
    direction: MessageDirection
    message_type: MessageType
    payload: dict[str, Any]
    size_bytes: int
    cycle_id: str | None = None
    post_confirmed: bool = False
    posted_at: datetime | None = None
    received_at: datetime | None = None
    validation_status: str | None = None  # valid | invalid | None (outbound)
    retransmission_of: str | None = None  # ADR-014
    created_at: datetime


# ------------------------------------------------------------------------------------------------
# Audit (§16 AuditEvent + ADR-017 hash chain)
# ------------------------------------------------------------------------------------------------
class AuditEvent(Record):
    event_id: str
    sequence: int
    previous_event_hash: str
    event_hash: str
    session_id: str
    conversation_id: str | None = None
    cycle_id: str | None = None
    plan_id: str | None = None
    task_id: str | None = None
    event_type: str
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)


# ------------------------------------------------------------------------------------------------
# Failure (§16 FailureRecord)
# ------------------------------------------------------------------------------------------------
class FailureRecord(Record):
    failure_id: str
    session_id: str
    conversation_id: str | None = None
    plan_id: str | None = None
    task_id: str | None = None
    error_type: ErrorType
    error_code: str
    severity: Severity
    origin: str
    retryable: bool
    recoverable: bool
    attempt: int = 1
    max_attempts: int = 1
    details: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime


# ------------------------------------------------------------------------------------------------
# Context summary (§16 ContextSummaryRecord + ADR-005 reduction step)
# ------------------------------------------------------------------------------------------------
class ContextSummaryRecord(Record):
    summary_id: str
    session_id: str
    source_conversation_id: str
    target_conversation_id: str
    summary_payload: dict[str, Any]
    summary_size_bytes: int
    reduction_step: int = 0
    created_at: datetime


# ------------------------------------------------------------------------------------------------
# Blob (§16 BlobRecord) - raw stdout / stderr, bytes, never truncated
# ------------------------------------------------------------------------------------------------
class BlobRecord(Record):
    blob_id: str
    session_id: str
    task_id: str
    blob_type: OutputStream
    content: bytes
    size_bytes: int
    created_at: datetime


# ------------------------------------------------------------------------------------------------
# Retry decisions (§7.3 "retry decisions persisted in state")
# ------------------------------------------------------------------------------------------------
class RetryDecisionRecord(Record):
    decision_id: str
    session_id: str
    conversation_id: str | None
    cycle_id: str | None
    operation: str  # "POST" | "GET" | "INIT"
    error_type: ErrorType
    error_code: str
    attempt: int
    max_attempts: int
    decision: str  # retry | abort | rotate | fail
    delay_ms: int | None
    created_at: datetime
