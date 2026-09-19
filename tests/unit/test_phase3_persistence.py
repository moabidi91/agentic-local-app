"""Phase 3 — persistence (spec §3.6, §7.5, §16, §17.1, §17.4, §18.2 ; ADR-001, ADR-011, ADR-012,
ADR-015, ADR-016, ADR-017).

Two layers are pinned here:

1. the **contract suite**, parametrised over the three store flavours (``InMemoryConversationStore``,
   ``SqliteConversationStore`` on a temporary file, ``SqliteConversationStore(":memory:")``): create /
   read / update of every record type with every optional field set (exact round trip), listing
   orders, checkpoint retrieval (``find_*_in_states``), transactions, blobs and range reads, the
   append-only audit chain, behaviour after ``close()``;
2. the **SQLite specifics**: durability across reopen (WAL), idempotent schema creation, schema
   version, pragmas, error mapping (``sqlite3.Error`` -> ``PersistenceError``), datetime
   normalisation, savepoint semantics, the ``open_store(config)`` factory and a throughput bound on
   the audit chain.

This phase is the sanctioned exception to §18.3: the real SQLite layer *is* what is under test, so
these tests open real databases on ``tmp_path`` and in memory. No shell, no network.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple, TypeVar, get_args

import pytest

from agentic_local_app.config import AppConfig, AppSection
from agentic_local_app.domain.errors import ErrorType, PersistenceError, Severity
from agentic_local_app.domain.models import (
    AuditEvent,
    BlobRecord,
    ContextSummaryRecord,
    ConversationRecord,
    CycleRecord,
    FailureRecord,
    MessageRecord,
    PlanRecord,
    Record,
    RetryDecisionRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
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
from agentic_local_app.persistence import (
    ConversationStore,
    InMemoryConversationStore,
    SqliteConversationStore,
    open_store,
)
from agentic_local_app.persistence import factory as factory_module
from agentic_local_app.persistence import sqlite_store as sqlite_module
from agentic_local_app.persistence.factory import DB_FILENAME
from agentic_local_app.persistence.sqlite_store import (
    SCHEMA_VERSION,
    persistence_error_from_sqlite,
)

pytestmark = pytest.mark.phase3

R = TypeVar("R", bound=Record)

# ================================================================================================
# Fixtures
# ================================================================================================
STORE_FLAVOURS = ["memory", "sqlite_file", "sqlite_memory"]


@pytest.fixture(params=STORE_FLAVOURS)
def store_impl(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[ConversationStore]:
    """The contract suite runs against every implementation with the same expectations."""
    store: ConversationStore
    if request.param == "memory":
        store = InMemoryConversationStore()
    elif request.param == "sqlite_file":
        store = SqliteConversationStore(tmp_path / "agentic.db")
    else:
        store = SqliteConversationStore(":memory:")
    yield store
    store.close()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "agentic.db"


@pytest.fixture
def sqlite_store(db_path: Path) -> Iterator[SqliteConversationStore]:
    store = SqliteConversationStore(db_path)
    yield store
    store.close()


# ================================================================================================
# Record factories — every optional field set (the round-trip tests must lose nothing)
# ================================================================================================
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def at(ms: int) -> datetime:
    return T0 + timedelta(milliseconds=ms)


NESTED: dict[str, Any] = {
    "summary": "ok é€ — unicode",
    "nested": {"list": [1, 2.5, {"deep": None, "flag": True}], "empty": {}, "text": "x"},
    "numbers": [0, -1, 10**12],
}
BUDGET = SessionBudget(max_cycles=20, max_plans=10, max_total_duration_ms=300_000)
BLOB_CONTENT = bytes(range(256))


def make_session(**overrides: Any) -> SessionRecord:
    data: dict[str, Any] = {
        "session_id": "sess-0001",
        "status": SessionState.RUNNING,
        "goal": "objectif",
        "user_message": "message utilisateur é€",
        "user_id": "local-user",
        "auto_close_on_final_answer": True,
        "budget": BUDGET,
        "consumed_cycles": 3,
        "consumed_plans": 2,
        "rotations_count": 1,
        "current_conversation_id": "conv-0002",
        "final_answer": NESTED,
        "last_failure_id": "fail-0001",
        "started_at": at(1),
        "ended_at": at(900),
        "interrupted_at": at(500),
        "created_at": at(0),
        "updated_at": at(900),
    }
    data.update(overrides)
    return SessionRecord(**data)


def make_conversation(**overrides: Any) -> ConversationRecord:
    data: dict[str, Any] = {
        "conversation_id": "conv-0001",
        "session_id": "sess-0001",
        "parent_conversation_id": "conv-0000",
        "remote_conversation_id": "remote-1",
        "status": ConversationState.WAITING_MODEL_RESPONSE,
        "auto_close_on_final_answer": False,
        "context_window_state": ContextWindowState.WARNING,
        "context_bytes": 12_345,
        "protocol_error_count": 1,
        "last_model_response_state": "valid",
        "current_cycle_id": "cyc-0001",
        "current_plan_id": "plan-1",
        "last_completed_plan_id": "plan-0",
        "final_answer_received": True,
        "last_outbound_message_id": "msg-0001",
        "last_inbound_message_id": "msg-0002",
        "get_cursor": "cursor-42",
        "session_budget_json": BUDGET.model_dump(),
        "closure_reason": "rotated",
        "interrupted_at": at(10),
        "created_at": at(0),
        "updated_at": at(10),
    }
    data.update(overrides)
    return ConversationRecord(**data)


def make_cycle(**overrides: Any) -> CycleRecord:
    data: dict[str, Any] = {
        "cycle_id": "cyc-0001",
        "conversation_id": "conv-0001",
        "session_id": "sess-0001",
        "cycle_type": CycleType.EXECUTION,
        "status": CycleState.COMPLETED,
        "retry_count": 2,
        "outbound_message_id": "msg-0001",
        "inbound_message_id": "msg-0002",
        "plan_id": "plan-1",
        "started_at": at(0),
        "ended_at": at(100),
    }
    data.update(overrides)
    return CycleRecord(**data)


def make_plan(**overrides: Any) -> PlanRecord:
    data: dict[str, Any] = {
        "plan_id": "plan-1",
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "cycle_id": "cyc-0001",
        "plan_type": PlanType.EXECUTION_PLAN,
        "objective": "faire quelque chose",
        "execution_policy": ExecutionPolicy.PARALLEL,
        "max_parallel_workers": 4,
        "status": PlanState.RUNNING,
        "stop_reason": "budget_exceeded:max_total_duration_ms",
        "task_count": 5,
        "completed_task_count": 2,
        "failed_task_count": 1,
        "skipped_task_count": 1,
        "cancelled_task_count": 1,
        "interrupted_task_count": 0,
        "default_max_output_bytes": 8_192,
        "state_summary": {"progress": 0.4, "notes": ["a", "b"]},
        "started_at": at(5),
        "ended_at": at(50),
        "created_at": at(0),
        "updated_at": at(50),
    }
    data.update(overrides)
    return PlanRecord(**data)


def make_task(**overrides: Any) -> TaskRecord:
    data: dict[str, Any] = {
        "task_id": "t1",
        "plan_id": "plan-1",
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "order_index": 0,
        "type": TaskType.CHUNK_REQUEST,
        "cmd": "echo 'héllo' && ls",
        "status": TaskState.COMPLETED,
        "critical": True,
        "continue_on_error": True,
        "stop_plan_on_failure": True,
        "stop_plan_on_success": True,
        "stops_plan_on_failure": False,
        "depends_on": ("t0", "t-1"),
        "resource_lock": "git",
        "max_output_bytes": 16_384,
        "max_output_bytes_applied": 8_192,
        "timeout_ms": 60_000,
        "timeout_ms_applied": 30_000,
        "ref_task_id": "t4",
        "stream": OutputStream.STDERR,
        "byte_offset": 16_384,
        "max_bytes": 4_096,
        "attempt_count": 2,
        "exit_code": 1,
        "timed_out": True,
        "stdout_ref": "blob-0001",
        "stderr_ref": "blob-0002",
        "truncated": True,
        "original_size_bytes": 48_211,
        "stdout_total": 48_211,
        "stderr_total": 12,
        "stdout_range": (31_827, 48_211),
        "stderr_range": (0, 12),
        "reason": "restart",
        "pid": 4242,
        "process_group_id": 4242,
        "started_at": at(1),
        "ended_at": at(30),
        "duration_ms": 29,
        "created_at": at(0),
        "updated_at": at(30),
    }
    data.update(overrides)
    return TaskRecord(**data)


def make_message(**overrides: Any) -> MessageRecord:
    data: dict[str, Any] = {
        "message_id": "msg-0001",
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "direction": MessageDirection.OUTBOUND,
        "message_type": MessageType.EXECUTION_RESULT,
        "payload": {"type": "execution_result", "content": NESTED},
        "size_bytes": 321,
        "cycle_id": "cyc-0001",
        "post_confirmed": True,
        "posted_at": at(1),
        "received_at": at(2),
        "validation_status": "valid",
        "retransmission_of": "msg-0000",
        "created_at": at(0),
    }
    data.update(overrides)
    return MessageRecord(**data)


def make_failure(**overrides: Any) -> FailureRecord:
    data: dict[str, Any] = {
        "failure_id": "fail-0001",
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "plan_id": "plan-1",
        "task_id": "t1",
        "error_type": ErrorType.NETWORK_ERROR,
        "error_code": "CONNECTION_RESET",
        "severity": Severity.MEDIUM,
        "origin": "TransportGateway",
        "retryable": True,
        "recoverable": True,
        "attempt": 2,
        "max_attempts": 4,
        "details": {"status": 503, "nested": NESTED},
        "timestamp": at(7),
    }
    data.update(overrides)
    return FailureRecord(**data)


def make_retry_decision(**overrides: Any) -> RetryDecisionRecord:
    data: dict[str, Any] = {
        "decision_id": "dec-0001",
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "cycle_id": "cyc-0001",
        "operation": "POST",
        "error_type": ErrorType.TIMEOUT_ERROR,
        "error_code": "REQUEST_TIMEOUT",
        "attempt": 1,
        "max_attempts": 4,
        "decision": "retry",
        "delay_ms": 500,
        "created_at": at(3),
    }
    data.update(overrides)
    return RetryDecisionRecord(**data)


def make_summary(**overrides: Any) -> ContextSummaryRecord:
    data: dict[str, Any] = {
        "summary_id": "sum-0001",
        "session_id": "sess-0001",
        "source_conversation_id": "conv-0001",
        "target_conversation_id": "conv-0002",
        "summary_payload": {"goal": "objectif", "state": NESTED},
        "summary_size_bytes": 1_234,
        "reduction_step": 2,
        "created_at": at(4),
    }
    data.update(overrides)
    return ContextSummaryRecord(**data)


def make_blob(**overrides: Any) -> BlobRecord:
    data: dict[str, Any] = {
        "blob_id": "blob-0001",
        "session_id": "sess-0001",
        "task_id": "t1",
        "blob_type": OutputStream.STDOUT,
        "content": BLOB_CONTENT,
        "size_bytes": len(BLOB_CONTENT),
        "created_at": at(6),
    }
    data.update(overrides)
    return BlobRecord(**data)


def make_audit_event(**overrides: Any) -> AuditEvent:
    data: dict[str, Any] = {
        "event_id": "evt-0001",
        "sequence": 1,
        "previous_event_hash": "0" * 64,
        "event_hash": "f" * 64,
        "session_id": "sess-0001",
        "conversation_id": "conv-0001",
        "cycle_id": "cyc-0001",
        "plan_id": "plan-1",
        "task_id": "t1",
        "event_type": "task.state_changed",
        "timestamp": at(8),
        "payload": {"from": "PENDING", "to": "RUNNING", "extra": NESTED},
    }
    data.update(overrides)
    return AuditEvent(**data)


def audit_chain(session_id: str, count: int, *, start: int = 1) -> list[AuditEvent]:
    """``count`` consecutive events of one session, sequences ``start .. start + count - 1``."""
    return [
        make_audit_event(
            event_id=f"evt-{session_id}-{seq:04d}",
            sequence=seq,
            session_id=session_id,
            timestamp=at(seq),
            payload={"seq": seq},
        )
        for seq in range(start, start + count)
    ]


def apply(record: R, changes: dict[str, Any]) -> R:
    """A validated copy of ``record`` with ``changes`` (the discipline of phase 1)."""
    return type(record).model_validate({**record.model_dump(), **changes})


def allows_none(annotation: Any) -> bool:
    return type(None) in get_args(annotation)


def minimal(record: R) -> R:
    """Only the required fields kept (nullable ones set to ``None``): defaults and NULLs round trip."""
    model = type(record)
    data = {
        name: (None if allows_none(field.annotation) else getattr(record, name))
        for name, field in model.model_fields.items()
        if field.is_required()
    }
    return model.model_validate(data)


class Case(NamedTuple):
    """How to save / read / list / change one record type through the ``ConversationStore`` ABC."""

    name: str
    make: Callable[..., Any]
    save: Callable[[ConversationStore, Any], None]
    get: Callable[[ConversationStore, Any], Any]
    listing: Callable[[ConversationStore, Any], list[Any]]
    changes: dict[str, Any]


def _first(rows: list[Any], key: str, value: str) -> Any:
    return next((row for row in rows if getattr(row, key) == value), None)


CASES: list[Case] = [
    Case(
        "session",
        make_session,
        lambda s, r: s.save_session(r),
        lambda s, r: s.get_session(r.session_id),
        lambda s, r: s.list_sessions(),
        {
            "status": SessionState.COMPLETED,
            "consumed_cycles": 4,
            "budget": SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1),
            "final_answer": None,
        },
    ),
    Case(
        "conversation",
        make_conversation,
        lambda s, r: s.save_conversation(r),
        lambda s, r: s.get_conversation(r.conversation_id),
        lambda s, r: s.list_conversations(r.session_id),
        {"status": ConversationState.RUNNING_PLAN, "context_bytes": 1, "get_cursor": None},
    ),
    Case(
        "cycle",
        make_cycle,
        lambda s, r: s.save_cycle(r),
        lambda s, r: s.get_cycle(r.cycle_id),
        lambda s, r: s.list_cycles(r.conversation_id),
        {"status": CycleState.FAILED, "retry_count": 3, "ended_at": None},
    ),
    Case(
        "plan",
        make_plan,
        lambda s, r: s.save_plan(r),
        lambda s, r: s.get_plan(r.session_id, r.plan_id),
        lambda s, r: s.list_plans(r.session_id),
        {"status": PlanState.COMPLETED, "completed_task_count": 5, "state_summary": None},
    ),
    Case(
        "task",
        make_task,
        lambda s, r: s.save_task(r),
        lambda s, r: s.get_task(r.session_id, r.task_id),
        lambda s, r: s.list_tasks(r.session_id),
        {"status": TaskState.INTERRUPTED, "depends_on": (), "stdout_range": None, "pid": None},
    ),
    Case(
        "message",
        make_message,
        lambda s, r: s.save_message(r),
        lambda s, r: s.get_message(r.message_id),
        lambda s, r: s.list_messages(r.conversation_id),
        {"post_confirmed": False, "payload": {"changed": True}, "received_at": None},
    ),
    Case(
        "failure",
        make_failure,
        lambda s, r: s.save_failure(r),
        lambda s, r: _first(s.list_failures(r.session_id), "failure_id", r.failure_id),
        lambda s, r: s.list_failures(r.session_id),
        {"attempt": 3, "details": {}, "task_id": None},
    ),
    Case(
        "retry_decision",
        make_retry_decision,
        lambda s, r: s.save_retry_decision(r),
        lambda s, r: _first(s.list_retry_decisions(r.session_id), "decision_id", r.decision_id),
        lambda s, r: s.list_retry_decisions(r.session_id),
        {"decision": "abort", "delay_ms": None},
    ),
    Case(
        "context_summary",
        make_summary,
        lambda s, r: s.save_context_summary(r),
        lambda s, r: s.get_context_summary_for_target(r.target_conversation_id),
        lambda s, r: s.list_context_summaries(r.session_id),
        {"reduction_step": 3, "summary_payload": {"reduced": True}},
    ),
    Case(
        "blob",
        make_blob,
        lambda s, r: s.save_blob(r),
        lambda s, r: s.get_blob(r.blob_id),
        lambda s, r: [
            b for b in [s.get_blob_for_task(r.session_id, r.task_id, r.blob_type)] if b is not None
        ],
        {"content": b"new content", "size_bytes": len(b"new content")},
    ),
    Case(
        "audit_event",
        make_audit_event,
        lambda s, r: s.append_audit_event(r),
        lambda s, r: _first(s.list_audit_events(r.session_id), "event_id", r.event_id),
        lambda s, r: s.list_audit_events(r.session_id),
        {},
    ),
]
UPSERT_CASES = [case for case in CASES if case.name != "audit_event"]
CASE_IDS = [case.name for case in CASES]
UPSERT_IDS = [case.name for case in UPSERT_CASES]


def error_of(exc: pytest.ExceptionInfo[PersistenceError]) -> Any:
    return exc.value.error


# ================================================================================================
# 1. Contract — create / read / update of every record type (§18.2 phase 3, §16)
# ================================================================================================
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def given_full_record_factory_when_built_then_every_field_is_set(case: Case) -> None:
    record = case.make()
    dump = record.model_dump()
    assert set(dump) == set(type(record).model_fields)
    assert all(value is not None for value in dump.values()), (
        "a None field would hide a lost column"
    )


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def given_empty_store_when_full_record_saved_then_read_back_equal(
    store_impl: ConversationStore, case: Case
) -> None:
    record = case.make()
    case.save(store_impl, record)
    assert case.get(store_impl, record) == record
    assert case.listing(store_impl, record) == [record]


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def given_empty_store_when_minimal_record_saved_then_defaults_and_nulls_read_back_equal(
    store_impl: ConversationStore, case: Case
) -> None:
    record = minimal(case.make())
    case.save(store_impl, record)
    assert case.get(store_impl, record) == record


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
def given_empty_store_when_unknown_key_read_then_none(
    store_impl: ConversationStore, case: Case
) -> None:
    assert case.get(store_impl, case.make()) is None
    assert case.listing(store_impl, case.make()) == []


@pytest.mark.parametrize("case", UPSERT_CASES, ids=UPSERT_IDS)
def given_saved_record_when_saved_again_with_changes_then_new_version_read_and_single_entry(
    store_impl: ConversationStore, case: Case
) -> None:
    record = case.make()
    case.save(store_impl, record)
    updated = apply(record, case.changes)
    case.save(store_impl, updated)
    assert case.get(store_impl, record) == updated
    assert case.listing(store_impl, record) == [updated]


def given_task_read_back_when_types_inspected_then_tuples_bytes_enums_and_utc_datetimes_preserved(
    store_impl: ConversationStore,
) -> None:
    store_impl.save_task(make_task())
    store_impl.save_blob(make_blob())
    store_impl.save_session(make_session())
    task = store_impl.get_task("sess-0001", "t1")
    blob = store_impl.get_blob("blob-0001")
    session = store_impl.get_session("sess-0001")
    assert task is not None and blob is not None and session is not None
    assert isinstance(task.depends_on, tuple) and task.depends_on == ("t0", "t-1")
    assert isinstance(task.stdout_range, tuple) and task.stdout_range == (31_827, 48_211)
    assert isinstance(task.type, TaskType) and isinstance(task.stream, OutputStream)
    assert isinstance(blob.content, bytes) and blob.content == BLOB_CONTENT
    assert isinstance(session.budget, SessionBudget) and session.budget == BUDGET
    for stamp in (task.created_at, task.started_at, session.interrupted_at, blob.created_at):
        assert stamp is not None and stamp.utcoffset() == timedelta(0)
    assert session.final_answer == NESTED


# ================================================================================================
# 2. Contract — sessions: newest first, filters, pagination
# ================================================================================================
def given_sessions_saved_in_scrambled_order_when_listed_then_newest_first_by_created_at_then_id(
    store_impl: ConversationStore,
) -> None:
    oldest = make_session(session_id="sess-0001", created_at=at(0))
    tie_low = make_session(session_id="sess-0002", created_at=at(100))
    tie_high = make_session(session_id="sess-0003", created_at=at(100))
    for record in (tie_low, oldest, tie_high):
        store_impl.save_session(record)
    assert store_impl.list_sessions() == [tie_high, tie_low, oldest]


def given_five_sessions_when_listed_with_limit_and_offset_then_page_of_newest_first_returned(
    store_impl: ConversationStore,
) -> None:
    sessions = [make_session(session_id=f"sess-{i:04d}", created_at=at(i * 10)) for i in range(5)]
    for record in sessions:
        store_impl.save_session(record)
    newest_first = list(reversed(sessions))
    assert store_impl.list_sessions(limit=2) == newest_first[:2]
    assert store_impl.list_sessions(limit=2, offset=1) == newest_first[1:3]
    assert store_impl.list_sessions(limit=10, offset=4) == newest_first[4:]
    assert store_impl.list_sessions(offset=5) == []


def given_sessions_in_mixed_states_when_filtered_by_statuses_then_only_matching_newest_first(
    store_impl: ConversationStore,
) -> None:
    ready = make_session(session_id="sess-0001", status=SessionState.READY, created_at=at(0))
    running = make_session(session_id="sess-0002", status=SessionState.RUNNING, created_at=at(1))
    failed = make_session(session_id="sess-0003", status=SessionState.FAILED, created_at=at(2))
    for record in (ready, running, failed):
        store_impl.save_session(record)
    assert store_impl.list_sessions(statuses=[SessionState.READY, SessionState.FAILED]) == [
        failed,
        ready,
    ]
    assert store_impl.list_sessions(statuses=iter([SessionState.RUNNING])) == [running]
    assert store_impl.list_sessions(statuses=[]) == []
    assert store_impl.list_sessions(statuses=[SessionState.COMPLETED]) == []


# ================================================================================================
# 3. Contract — insertion-order listings (oldest first) and scoping
# ================================================================================================
def given_conversations_saved_out_of_timestamp_order_when_listed_then_insertion_order_kept(
    store_impl: ConversationStore,
) -> None:
    later = make_conversation(conversation_id="conv-0002", created_at=at(100))
    earlier = make_conversation(conversation_id="conv-0001", created_at=at(0))
    other = make_conversation(conversation_id="conv-0009", session_id="sess-0002")
    for record in (later, other, earlier):
        store_impl.save_conversation(record)
    assert store_impl.list_conversations("sess-0001") == [later, earlier]
    assert store_impl.list_conversations("sess-0002") == [other]
    assert store_impl.list_conversations("sess-none") == []


def given_conversation_updated_when_listed_then_original_position_kept(
    store_impl: ConversationStore,
) -> None:
    first = make_conversation(conversation_id="conv-0001")
    second = make_conversation(conversation_id="conv-0002")
    store_impl.save_conversation(first)
    store_impl.save_conversation(second)
    updated = apply(first, {"status": ConversationState.CLOSED, "closure_reason": "rotated"})
    store_impl.save_conversation(updated)
    assert store_impl.list_conversations("sess-0001") == [updated, second]


def given_cycles_of_two_conversations_when_listed_then_only_matching_in_insertion_order(
    store_impl: ConversationStore,
) -> None:
    c2 = make_cycle(cycle_id="cyc-0002", started_at=at(50))
    c1 = make_cycle(cycle_id="cyc-0001", started_at=at(0))
    foreign = make_cycle(cycle_id="cyc-0003", conversation_id="conv-0002")
    for record in (c2, foreign, c1):
        store_impl.save_cycle(record)
    assert store_impl.list_cycles("conv-0001") == [c2, c1]
    assert store_impl.list_cycles("conv-0002") == [foreign]
    assert store_impl.list_cycles("conv-none") == []


def given_plans_of_two_conversations_when_listed_then_insertion_order_and_conversation_filter(
    store_impl: ConversationStore,
) -> None:
    p_b = make_plan(plan_id="plan-b", conversation_id="conv-0002", created_at=at(50))
    p_a = make_plan(plan_id="plan-a", conversation_id="conv-0001", created_at=at(0))
    p_c = make_plan(plan_id="plan-c", conversation_id="conv-0001", created_at=at(100))
    foreign = make_plan(plan_id="plan-a", session_id="sess-0002")
    for record in (p_b, foreign, p_a, p_c):
        store_impl.save_plan(record)
    assert store_impl.list_plans("sess-0001") == [p_b, p_a, p_c]
    assert store_impl.list_plans("sess-0001", conversation_id="conv-0001") == [p_a, p_c]
    assert store_impl.list_plans("sess-0002") == [foreign]
    assert store_impl.get_plan("sess-0002", "plan-a") == foreign
    assert store_impl.get_plan("sess-0003", "plan-a") is None


def given_messages_of_both_directions_when_listed_then_insertion_order_and_direction_filter(
    store_impl: ConversationStore,
) -> None:
    out1 = make_message(message_id="msg-0001", direction=MessageDirection.OUTBOUND)
    in1 = make_message(message_id="msg-0002", direction=MessageDirection.INBOUND)
    out2 = make_message(message_id="msg-0003", direction=MessageDirection.OUTBOUND)
    foreign = make_message(message_id="msg-0004", conversation_id="conv-0002")
    for record in (out1, in1, foreign, out2):
        store_impl.save_message(record)
    assert store_impl.list_messages("conv-0001") == [out1, in1, out2]
    assert store_impl.list_messages("conv-0001", direction=MessageDirection.OUTBOUND) == [
        out1,
        out2,
    ]
    assert store_impl.list_messages("conv-0001", direction=MessageDirection.INBOUND) == [in1]
    assert store_impl.list_messages("conv-0002") == [foreign]


def given_failures_and_retry_decisions_when_listed_then_insertion_order_per_session(
    store_impl: ConversationStore,
) -> None:
    f2 = make_failure(failure_id="fail-0002", timestamp=at(50))
    f1 = make_failure(failure_id="fail-0001", timestamp=at(0))
    d2 = make_retry_decision(decision_id="dec-0002", created_at=at(50))
    d1 = make_retry_decision(decision_id="dec-0001", created_at=at(0))
    for failure in (f2, make_failure(failure_id="fail-0003", session_id="sess-0002"), f1):
        store_impl.save_failure(failure)
    for decision in (d2, make_retry_decision(decision_id="dec-0003", session_id="sess-0002"), d1):
        store_impl.save_retry_decision(decision)
    assert store_impl.list_failures("sess-0001") == [f2, f1]
    assert store_impl.list_retry_decisions("sess-0001") == [d2, d1]
    assert len(store_impl.list_failures("sess-0002")) == 1
    assert len(store_impl.list_retry_decisions("sess-0002")) == 1
    assert store_impl.list_failures("sess-none") == []


def given_summaries_for_two_targets_when_listed_and_looked_up_then_insertion_order_and_first_match(
    store_impl: ConversationStore,
) -> None:
    s2 = make_summary(summary_id="sum-0002", target_conversation_id="conv-0003", created_at=at(50))
    s1 = make_summary(summary_id="sum-0001", target_conversation_id="conv-0002", created_at=at(0))
    s3 = make_summary(summary_id="sum-0003", target_conversation_id="conv-0002", created_at=at(99))
    foreign = make_summary(
        summary_id="sum-0004", session_id="sess-0002", target_conversation_id="conv-0099"
    )
    for record in (s2, foreign, s1, s3):
        store_impl.save_context_summary(record)
    assert store_impl.list_context_summaries("sess-0001") == [s2, s1, s3]
    assert store_impl.get_context_summary_for_target("conv-0003") == s2
    assert store_impl.get_context_summary_for_target("conv-0002") == s1
    assert store_impl.get_context_summary_for_target("conv-none") is None


# ================================================================================================
# 4. Contract — tasks: by plan insertion order then order_index (ADR-017), filters, bulk save
# ================================================================================================
def _two_plans_with_tasks(store: ConversationStore) -> dict[str, TaskRecord]:
    """plan-2 saved before plan-1; tasks saved in scrambled order."""
    store.save_plan(make_plan(plan_id="plan-2", created_at=at(100)))
    store.save_plan(make_plan(plan_id="plan-1", created_at=at(0)))
    tasks = {
        "p1-0": make_task(task_id="p1-0", plan_id="plan-1", order_index=0),
        "p1-1": make_task(
            task_id="p1-1", plan_id="plan-1", order_index=1, status=TaskState.RUNNING
        ),
        "p2-0": make_task(
            task_id="p2-0", plan_id="plan-2", order_index=0, status=TaskState.RUNNING
        ),
        "p2-1": make_task(task_id="p2-1", plan_id="plan-2", order_index=1),
    }
    for key in ("p1-1", "p2-1", "p1-0", "p2-0"):
        store.save_task(tasks[key])
    return tasks


def given_two_plans_when_tasks_listed_then_ordered_by_plan_insertion_then_order_index(
    store_impl: ConversationStore,
) -> None:
    tasks = _two_plans_with_tasks(store_impl)
    assert store_impl.list_tasks("sess-0001") == [
        tasks["p2-0"],
        tasks["p2-1"],
        tasks["p1-0"],
        tasks["p1-1"],
    ]


def given_two_plans_when_tasks_listed_with_plan_and_status_filters_then_subset_in_same_order(
    store_impl: ConversationStore,
) -> None:
    tasks = _two_plans_with_tasks(store_impl)
    assert store_impl.list_tasks("sess-0001", plan_id="plan-1") == [tasks["p1-0"], tasks["p1-1"]]
    assert store_impl.list_tasks("sess-0001", statuses=[TaskState.RUNNING]) == [
        tasks["p2-0"],
        tasks["p1-1"],
    ]
    assert store_impl.list_tasks("sess-0001", plan_id="plan-2", statuses=[TaskState.COMPLETED]) == [
        tasks["p2-1"]
    ]
    assert store_impl.list_tasks("sess-0001", statuses=[]) == []
    assert store_impl.list_tasks("sess-0002") == []


def given_task_whose_plan_is_unknown_when_listed_then_sorted_before_known_plans(
    store_impl: ConversationStore,
) -> None:
    tasks = _two_plans_with_tasks(store_impl)
    orphan = make_task(task_id="orphan", plan_id="plan-missing", order_index=5)
    store_impl.save_task(orphan)
    assert store_impl.list_tasks("sess-0001")[0] == orphan
    assert store_impl.list_tasks("sess-0001")[1:] == [
        tasks["p2-0"],
        tasks["p2-1"],
        tasks["p1-0"],
        tasks["p1-1"],
    ]


def given_task_updated_when_listed_then_position_follows_plan_and_order_index_not_update_time(
    store_impl: ConversationStore,
) -> None:
    tasks = _two_plans_with_tasks(store_impl)
    updated = apply(tasks["p2-0"], {"status": TaskState.FAILED, "exit_code": 2})
    store_impl.save_task(updated)
    listed = store_impl.list_tasks("sess-0001")
    assert listed[0] == updated and len(listed) == 4
    assert store_impl.list_tasks("sess-0001", statuses=[TaskState.FAILED]) == [updated]


def given_tasks_when_saved_in_bulk_then_all_present_in_declaration_order(
    store_impl: ConversationStore,
) -> None:
    store_impl.save_plan(make_plan(plan_id="plan-1"))
    tasks = [make_task(task_id=f"t{i}", order_index=i) for i in range(4)]
    store_impl.save_tasks(reversed(tasks))
    assert store_impl.list_tasks("sess-0001", plan_id="plan-1") == tasks
    assert store_impl.get_task("sess-0001", "t3") == tasks[3]


def given_bulk_save_whose_iterator_fails_midway_when_called_then_no_task_persisted(
    store_impl: ConversationStore,
) -> None:
    def records() -> Iterator[TaskRecord]:
        yield make_task(task_id="t0", order_index=0)
        yield make_task(task_id="t1", order_index=1)
        raise RuntimeError("plan reception aborted")

    with pytest.raises(RuntimeError):
        store_impl.save_tasks(records())
    assert store_impl.list_tasks("sess-0001") == []
    assert store_impl.get_task("sess-0001", "t0") is None


# ================================================================================================
# 5. Contract — checkpoint retrieval for recovery (§7.5, §17.4, ADR-016)
# ================================================================================================
def given_crashed_session_when_running_tasks_searched_then_only_running_in_plan_then_index_order(
    store_impl: ConversationStore,
) -> None:
    tasks = _two_plans_with_tasks(store_impl)
    store_impl.save_task(
        make_task(
            task_id="other", session_id="sess-0002", plan_id="plan-x", status=TaskState.RUNNING
        )
    )
    running = store_impl.find_tasks_in_states([TaskState.RUNNING])
    assert [t.task_id for t in running] == ["other", "p2-0", "p1-1"]
    assert running[1] == tasks["p2-0"]
    assert store_impl.find_tasks_in_states([TaskState.TIMED_OUT]) == []
    assert store_impl.find_tasks_in_states([]) == []


def given_tasks_in_pending_and_waiting_states_when_searched_together_then_both_returned(
    store_impl: ConversationStore,
) -> None:
    store_impl.save_plan(make_plan(plan_id="plan-1"))
    pending = make_task(task_id="t0", order_index=0, status=TaskState.PENDING)
    waiting = make_task(task_id="t1", order_index=1, status=TaskState.WAITING_DEPENDENCY)
    done = make_task(task_id="t2", order_index=2, status=TaskState.COMPLETED)
    store_impl.save_tasks([done, waiting, pending])
    found = store_impl.find_tasks_in_states({TaskState.PENDING, TaskState.WAITING_DEPENDENCY})
    assert found == [pending, waiting]


def given_plans_in_various_states_when_running_or_pending_searched_then_insertion_order(
    store_impl: ConversationStore,
) -> None:
    running = make_plan(plan_id="plan-r", status=PlanState.RUNNING)
    done = make_plan(plan_id="plan-d", status=PlanState.COMPLETED)
    pending_other = make_plan(plan_id="plan-p", session_id="sess-0002", status=PlanState.PENDING)
    for record in (running, done, pending_other):
        store_impl.save_plan(record)
    assert store_impl.find_plans_in_states([PlanState.RUNNING]) == [running]
    assert store_impl.find_plans_in_states([PlanState.PENDING, PlanState.RUNNING]) == [
        running,
        pending_other,
    ]
    assert store_impl.find_plans_in_states([PlanState.FAILED]) == []


def given_conversations_when_waiting_model_response_searched_then_matches_across_sessions_in_order(
    store_impl: ConversationStore,
) -> None:
    waiting_b = make_conversation(
        conversation_id="conv-b",
        session_id="sess-0002",
        status=ConversationState.WAITING_MODEL_RESPONSE,
    )
    closed = make_conversation(conversation_id="conv-c", status=ConversationState.CLOSED)
    waiting_a = make_conversation(
        conversation_id="conv-a", status=ConversationState.WAITING_MODEL_RESPONSE
    )
    running = make_conversation(conversation_id="conv-d", status=ConversationState.RUNNING_PLAN)
    for record in (waiting_b, closed, waiting_a, running):
        store_impl.save_conversation(record)
    found = store_impl.find_conversations_in_states([ConversationState.WAITING_MODEL_RESPONSE])
    assert found == [waiting_b, waiting_a]
    active = store_impl.find_conversations_in_states(
        [ConversationState.RUNNING_PLAN, ConversationState.WAITING_MODEL_RESPONSE]
    )
    assert active == [waiting_b, waiting_a, running]
    assert store_impl.find_conversations_in_states([ConversationState.NEW]) == []


def given_checkpoint_of_interrupted_plan_when_reloaded_then_plan_and_tasks_consistent(
    store_impl: ConversationStore,
) -> None:
    """A stable checkpoint (ADR-015) is simply the state after a full transition: reload it."""
    plan = make_plan(plan_id="plan-1", status=PlanState.RUNNING, task_count=2)
    tasks = [
        make_task(task_id="t0", order_index=0, status=TaskState.COMPLETED),
        make_task(task_id="t1", order_index=1, status=TaskState.RUNNING, pid=777),
    ]
    with store_impl.transaction():
        store_impl.save_plan(plan)
        store_impl.save_tasks(tasks)
    running_plan = store_impl.find_plans_in_states([PlanState.RUNNING])
    assert running_plan == [plan]
    reloaded = store_impl.list_tasks("sess-0001", plan_id="plan-1")
    assert reloaded == tasks
    assert [t.status for t in reloaded] == [TaskState.COMPLETED, TaskState.RUNNING]
    assert reloaded[1].pid == 777


# ================================================================================================
# 6. Contract — transactions (ADR-015)
# ================================================================================================
def given_committed_data_when_transaction_raises_then_writes_inside_rolled_back_and_store_usable(
    store_impl: ConversationStore,
) -> None:
    first = make_session(session_id="sess-0001")
    store_impl.save_session(first)
    with pytest.raises(RuntimeError), store_impl.transaction():
        store_impl.save_session(make_session(session_id="sess-0002"))
        store_impl.save_session(apply(first, {"status": SessionState.FAILED}))
        raise RuntimeError("abort")
    assert store_impl.list_sessions() == [first]
    store_impl.save_session(make_session(session_id="sess-0003"))
    assert len(store_impl.list_sessions()) == 2


def given_transaction_when_several_record_types_written_then_exception_then_nothing_visible(
    store_impl: ConversationStore,
) -> None:
    with pytest.raises(ValueError), store_impl.transaction():
        store_impl.save_session(make_session())
        store_impl.save_conversation(make_conversation())
        store_impl.save_plan(make_plan())
        store_impl.save_task(make_task())
        store_impl.save_blob(make_blob())
        store_impl.append_audit_event(make_audit_event())
        raise ValueError("partial write must not survive")
    assert store_impl.get_session("sess-0001") is None
    assert store_impl.list_conversations("sess-0001") == []
    assert store_impl.list_plans("sess-0001") == []
    assert store_impl.list_tasks("sess-0001") == []
    assert store_impl.get_blob("blob-0001") is None
    assert store_impl.count_audit_events("sess-0001") == 0


def given_transaction_when_writes_read_back_inside_then_own_writes_visible_before_commit(
    store_impl: ConversationStore,
) -> None:
    with store_impl.transaction():
        store_impl.save_conversation(make_conversation())
        assert store_impl.get_conversation("conv-0001") == make_conversation()
        assert store_impl.list_conversations("sess-0001") == [make_conversation()]
    assert store_impl.get_conversation("conv-0001") == make_conversation()


def given_nested_transactions_when_both_succeed_then_all_writes_visible(
    store_impl: ConversationStore,
) -> None:
    with store_impl.transaction():
        store_impl.save_session(make_session(session_id="sess-0001"))
        with store_impl.transaction():
            store_impl.save_conversation(make_conversation())
            with store_impl.transaction():
                store_impl.save_cycle(make_cycle())
        store_impl.save_plan(make_plan())
    assert store_impl.get_session("sess-0001") is not None
    assert store_impl.get_conversation("conv-0001") is not None
    assert store_impl.get_cycle("cyc-0001") is not None
    assert store_impl.get_plan("sess-0001", "plan-1") is not None


def given_nested_transactions_when_inner_raises_through_outer_then_everything_rolled_back(
    store_impl: ConversationStore,
) -> None:
    with pytest.raises(RuntimeError), store_impl.transaction():
        store_impl.save_session(make_session(session_id="sess-0001"))
        with store_impl.transaction():
            store_impl.save_conversation(make_conversation())
            raise RuntimeError("inner failure")
    assert store_impl.get_session("sess-0001") is None
    assert store_impl.get_conversation("conv-0001") is None


def given_transaction_when_persistence_error_raised_inside_then_rolled_back_and_store_usable(
    store_impl: ConversationStore,
) -> None:
    with pytest.raises(PersistenceError) as exc, store_impl.transaction():
        store_impl.save_task(make_task())
        store_impl.save_blob(make_blob(size_bytes=1))  # BLOB_SIZE_MISMATCH
    assert error_of(exc).error_code == "BLOB_SIZE_MISMATCH"
    assert store_impl.get_task("sess-0001", "t1") is None
    store_impl.save_task(make_task())
    assert store_impl.get_task("sess-0001", "t1") == make_task()


def given_audit_events_in_failed_transaction_when_rolled_back_then_count_restored_and_sequence_reusable(
    store_impl: ConversationStore,
) -> None:
    chain = audit_chain("sess-0001", 4)
    for event in chain[:2]:
        store_impl.append_audit_event(event)
    with pytest.raises(RuntimeError), store_impl.transaction():
        store_impl.append_audit_event(chain[2])
        store_impl.append_audit_event(chain[3])
        raise RuntimeError("abort")
    assert store_impl.count_audit_events("sess-0001") == 2
    assert store_impl.get_last_audit_event("sess-0001") == chain[1]
    store_impl.append_audit_event(chain[2])
    assert store_impl.list_audit_events("sess-0001") == chain[:3]


# ================================================================================================
# 7. Contract — blobs and range reads (§3.6, §18.2, ADR-011)
# ================================================================================================
def given_blob_with_wrong_size_when_saved_then_blob_size_mismatch_and_nothing_stored(
    store_impl: ConversationStore,
) -> None:
    with pytest.raises(PersistenceError) as exc:
        store_impl.save_blob(make_blob(size_bytes=len(BLOB_CONTENT) + 1))
    error = error_of(exc)
    assert error.error_type is ErrorType.PERSISTENCE_ERROR
    assert error.error_code == "BLOB_SIZE_MISMATCH"
    assert error.details["blob_id"] == "blob-0001"
    assert error.retryable is False
    assert store_impl.get_blob("blob-0001") is None


def given_stdout_and_stderr_blobs_when_looked_up_for_task_then_stream_specific_record(
    store_impl: ConversationStore,
) -> None:
    stdout = make_blob(blob_id="blob-0001", blob_type=OutputStream.STDOUT)
    stderr = make_blob(
        blob_id="blob-0002", blob_type=OutputStream.STDERR, content=b"err", size_bytes=3
    )
    other_task = make_blob(blob_id="blob-0003", task_id="t2", content=b"", size_bytes=0)
    for record in (stdout, stderr, other_task):
        store_impl.save_blob(record)
    assert store_impl.get_blob_for_task("sess-0001", "t1", OutputStream.STDOUT) == stdout
    assert store_impl.get_blob_for_task("sess-0001", "t1", OutputStream.STDERR) == stderr
    assert store_impl.get_blob_for_task("sess-0001", "t2", OutputStream.STDOUT) == other_task
    assert store_impl.get_blob_for_task("sess-0001", "t2", OutputStream.STDERR) is None
    assert store_impl.get_blob_for_task("sess-0002", "t1", OutputStream.STDOUT) is None


@pytest.mark.parametrize(
    ("offset", "max_bytes"),
    [
        (0, 16),  # start
        (100, 50),  # middle
        (240, 16),  # exact end
        (250, 100),  # clipped at the end
        (0, 10_000),  # whole blob
        (256, 10),  # offset == size -> empty
        (300, 10),  # offset > size -> empty
        (10, 0),  # zero length
        (0, 0),
        (255, 1),  # last byte
    ],
    ids=[
        "start",
        "middle",
        "end",
        "clipped",
        "whole",
        "at_size",
        "past_size",
        "zero_len",
        "zero_zero",
        "last",
    ],
)
def given_blob_when_range_read_then_slice_clipped_to_blob_size(
    store_impl: ConversationStore, offset: int, max_bytes: int
) -> None:
    store_impl.save_blob(make_blob())
    assert (
        store_impl.read_blob_range("blob-0001", offset, max_bytes)
        == (BLOB_CONTENT[offset : offset + max_bytes])
    )


def given_empty_blob_when_range_read_then_empty_bytes(store_impl: ConversationStore) -> None:
    store_impl.save_blob(make_blob(content=b"", size_bytes=0))
    assert store_impl.read_blob_range("blob-0001", 0, 10) == b""
    assert store_impl.read_blob_range("blob-0001", 5, 10) == b""
    assert store_impl.get_blob("blob-0001") == make_blob(content=b"", size_bytes=0)


def given_unknown_blob_when_range_read_then_blob_not_found(store_impl: ConversationStore) -> None:
    with pytest.raises(PersistenceError) as exc:
        store_impl.read_blob_range("blob-none", 0, 10)
    assert error_of(exc).error_code == "BLOB_NOT_FOUND"
    assert error_of(exc).details["blob_id"] == "blob-none"
    with pytest.raises(PersistenceError) as exc2:
        store_impl.read_blob_range("blob-none", -1, 10)
    assert error_of(exc2).error_code == "BLOB_NOT_FOUND", "existence is checked before the range"


@pytest.mark.parametrize(("offset", "max_bytes"), [(-1, 10), (0, -1), (-5, -5)])
def given_blob_when_range_read_with_negative_bounds_then_blob_range_invalid(
    store_impl: ConversationStore, offset: int, max_bytes: int
) -> None:
    store_impl.save_blob(make_blob())
    with pytest.raises(PersistenceError) as exc:
        store_impl.read_blob_range("blob-0001", offset, max_bytes)
    error = error_of(exc)
    assert error.error_code == "BLOB_RANGE_INVALID"
    assert error.details["blob_id"] == "blob-0001"
    assert error.details["offset"] == offset and error.details["max_bytes"] == max_bytes


def given_large_blob_when_saved_then_round_trip_and_ranges_exact(
    store_impl: ConversationStore,
) -> None:
    content = bytes((i * 7_919 + i // 251) % 256 for i in range(300_000))
    store_impl.save_blob(make_blob(content=content, size_bytes=len(content)))
    blob = store_impl.get_blob("blob-0001")
    assert blob is not None and blob.content == content and blob.size_bytes == 300_000
    assert store_impl.read_blob_range("blob-0001", 0, 16_384) == content[:16_384]
    assert store_impl.read_blob_range("blob-0001", 299_000, 5_000) == content[299_000:]
    assert store_impl.read_blob_range("blob-0001", 150_000, 1) == content[150_000:150_001]


def given_blob_when_saved_again_then_content_replaced_and_ranges_follow(
    store_impl: ConversationStore,
) -> None:
    store_impl.save_blob(make_blob())
    replaced = make_blob(content=b"replaced", size_bytes=8)
    store_impl.save_blob(replaced)
    assert store_impl.get_blob("blob-0001") == replaced
    assert store_impl.read_blob_range("blob-0001", 2, 3) == b"pla"
    assert store_impl.read_blob_range("blob-0001", 0, 100) == b"replaced"


# ================================================================================================
# 7b. Contract — reset: every record goes, the store stays open and usable
# ================================================================================================
def given_a_full_store_when_reset_then_every_table_is_empty_and_the_store_still_works(
    store_impl: ConversationStore,
) -> None:
    """The one destructive operation (``POST /admin/reset-database``, gated by configuration)."""
    store_impl.save_session(make_session())
    store_impl.save_conversation(make_conversation())
    store_impl.save_cycle(make_cycle())
    store_impl.save_plan(make_plan())
    store_impl.save_task(make_task())
    store_impl.save_message(make_message())
    store_impl.save_failure(make_failure())
    store_impl.save_retry_decision(make_retry_decision())
    store_impl.save_context_summary(make_summary())
    store_impl.save_blob(make_blob())
    for event in audit_chain("sess-0001", 3):
        store_impl.append_audit_event(event)

    store_impl.reset()

    assert store_impl.list_sessions() == []
    assert store_impl.get_session("sess-0001") is None
    assert store_impl.list_conversations("sess-0001") == []
    assert store_impl.list_cycles("conv-0001") == []
    assert store_impl.list_plans("sess-0001") == []
    assert store_impl.list_tasks("sess-0001") == []
    assert store_impl.list_messages("conv-0001") == []
    assert store_impl.list_failures("sess-0001") == []
    assert store_impl.list_retry_decisions("sess-0001") == []
    assert store_impl.list_context_summaries("sess-0001") == []
    assert store_impl.get_blob(make_blob().blob_id) is None
    assert store_impl.count_audit_events("sess-0001") == 0
    assert store_impl.get_last_audit_event("sess-0001") is None
    # a reset database is exactly a fresh one: the chain starts again at sequence 1
    store_impl.save_session(make_session())
    store_impl.append_audit_event(make_audit_event(event_id="evt-new", sequence=1))
    assert [e.event_id for e in store_impl.list_audit_events("sess-0001")] == ["evt-new"]
    assert [s.session_id for s in store_impl.list_sessions()] == ["sess-0001"]


def given_a_closed_store_when_reset_then_store_closed(store_impl: ConversationStore) -> None:
    store_impl.close()
    with pytest.raises(PersistenceError) as exc:
        store_impl.reset()
    assert error_of(exc).error_code == "STORE_CLOSED"


# ================================================================================================
# 8. Contract — append-only audit chain (§3.16, §17.3, ADR-017)
# ================================================================================================
def given_empty_session_when_events_appended_then_last_count_and_ascending_list(
    store_impl: ConversationStore,
) -> None:
    chain = audit_chain("sess-0001", 5)
    assert store_impl.get_last_audit_event("sess-0001") is None
    assert store_impl.count_audit_events("sess-0001") == 0
    assert store_impl.list_audit_events("sess-0001") == []
    for event in chain:
        store_impl.append_audit_event(event)
    assert store_impl.count_audit_events("sess-0001") == 5
    assert store_impl.get_last_audit_event("sess-0001") == chain[-1]
    assert store_impl.list_audit_events("sess-0001") == chain
    assert [e.sequence for e in store_impl.list_audit_events("sess-0001")] == [1, 2, 3, 4, 5]


def given_chain_when_listed_after_sequence_with_limit_then_window_returned(
    store_impl: ConversationStore,
) -> None:
    chain = audit_chain("sess-0001", 6)
    for event in chain:
        store_impl.append_audit_event(event)
    assert store_impl.list_audit_events("sess-0001", after_sequence=3) == chain[3:]
    assert store_impl.list_audit_events("sess-0001", after_sequence=3, limit=2) == chain[3:5]
    assert store_impl.list_audit_events("sess-0001", limit=1) == chain[:1]
    assert store_impl.list_audit_events("sess-0001", after_sequence=6) == []
    assert store_impl.list_audit_events("sess-0001", after_sequence=0) == chain
    assert store_impl.list_audit_events("sess-0001", limit=0) == []


def given_chain_when_event_with_sequence_gap_appended_then_audit_sequence_gap_and_nothing_appended(
    store_impl: ConversationStore,
) -> None:
    chain = audit_chain("sess-0001", 2)
    for event in chain:
        store_impl.append_audit_event(event)
    with pytest.raises(PersistenceError) as exc:
        store_impl.append_audit_event(make_audit_event(event_id="evt-gap", sequence=4))
    error = error_of(exc)
    assert error.error_code == "AUDIT_SEQUENCE_GAP"
    assert error.details["expected"] == 3 and error.details["got"] == 4
    assert error.error_type is ErrorType.PERSISTENCE_ERROR and error.retryable is False
    assert store_impl.count_audit_events("sess-0001") == 2
    assert store_impl.get_last_audit_event("sess-0001") == chain[-1]


def given_chain_when_lower_sequence_appended_then_append_only_violation(
    store_impl: ConversationStore,
) -> None:
    for event in audit_chain("sess-0001", 3):
        store_impl.append_audit_event(event)
    with pytest.raises(PersistenceError) as exc:
        store_impl.append_audit_event(make_audit_event(event_id="evt-rewrite", sequence=2))
    error = error_of(exc)
    assert error.error_code == "AUDIT_APPEND_ONLY_VIOLATION"
    assert error.details["event_id"] == "evt-rewrite" and error.details["sequence"] == 2
    assert store_impl.count_audit_events("sess-0001") == 3


def given_chain_when_existing_event_id_appended_with_next_sequence_then_append_only_violation(
    store_impl: ConversationStore,
) -> None:
    chain = audit_chain("sess-0001", 3)
    for event in chain:
        store_impl.append_audit_event(event)
    duplicate = apply(chain[0], {"sequence": 4})
    with pytest.raises(PersistenceError) as exc:
        store_impl.append_audit_event(duplicate)
    assert error_of(exc).error_code == "AUDIT_APPEND_ONLY_VIOLATION"
    assert error_of(exc).details["event_id"] == chain[0].event_id
    assert store_impl.list_audit_events("sess-0001") == chain


def given_chain_when_same_event_appended_twice_then_append_only_violation_and_chain_unchanged(
    store_impl: ConversationStore,
) -> None:
    event = make_audit_event()
    store_impl.append_audit_event(event)
    with pytest.raises(PersistenceError) as exc:
        store_impl.append_audit_event(event)
    assert error_of(exc).error_code == "AUDIT_APPEND_ONLY_VIOLATION"
    assert store_impl.list_audit_events("sess-0001") == [event]


def given_two_sessions_when_events_appended_then_chains_independent(
    store_impl: ConversationStore,
) -> None:
    chain_a = audit_chain("sess-0001", 3)
    chain_b = audit_chain("sess-0002", 2)
    for event in (chain_a[0], chain_b[0], chain_a[1], chain_b[1], chain_a[2]):
        store_impl.append_audit_event(event)
    assert store_impl.list_audit_events("sess-0001") == chain_a
    assert store_impl.list_audit_events("sess-0002") == chain_b
    assert store_impl.count_audit_events("sess-0001") == 3
    assert store_impl.count_audit_events("sess-0002") == 2
    assert store_impl.get_last_audit_event("sess-0002") == chain_b[-1]
    assert store_impl.count_audit_events("sess-none") == 0


def given_empty_session_when_first_event_has_arbitrary_sequence_then_accepted_and_chain_continues(
    store_impl: ConversationStore,
) -> None:
    """The store does not impose the first sequence (the AuditLog does): only continuity."""
    chain = audit_chain("sess-0001", 2, start=7)
    store_impl.append_audit_event(chain[0])
    store_impl.append_audit_event(chain[1])
    with pytest.raises(PersistenceError):
        store_impl.append_audit_event(make_audit_event(event_id="evt-x", sequence=1))
    assert [e.sequence for e in store_impl.list_audit_events("sess-0001")] == [7, 8]


# ================================================================================================
# 9. Contract — after close()
# ================================================================================================
WRITE_OPERATIONS: list[tuple[str, Callable[[ConversationStore], None]]] = [
    ("save_session", lambda s: s.save_session(make_session())),
    ("save_conversation", lambda s: s.save_conversation(make_conversation())),
    ("save_cycle", lambda s: s.save_cycle(make_cycle())),
    ("save_plan", lambda s: s.save_plan(make_plan())),
    ("save_task", lambda s: s.save_task(make_task())),
    ("save_tasks", lambda s: s.save_tasks([make_task()])),
    ("save_message", lambda s: s.save_message(make_message())),
    ("save_failure", lambda s: s.save_failure(make_failure())),
    ("save_retry_decision", lambda s: s.save_retry_decision(make_retry_decision())),
    ("save_context_summary", lambda s: s.save_context_summary(make_summary())),
    ("save_blob", lambda s: s.save_blob(make_blob())),
    ("append_audit_event", lambda s: s.append_audit_event(make_audit_event())),
]
READ_OPERATIONS: list[tuple[str, Callable[[ConversationStore], Any]]] = [
    ("get_session", lambda s: s.get_session("sess-0001")),
    ("list_sessions", lambda s: s.list_sessions()),
    ("get_conversation", lambda s: s.get_conversation("conv-0001")),
    ("list_conversations", lambda s: s.list_conversations("sess-0001")),
    (
        "find_conversations_in_states",
        lambda s: s.find_conversations_in_states([ConversationState.NEW]),
    ),
    ("get_cycle", lambda s: s.get_cycle("cyc-0001")),
    ("list_cycles", lambda s: s.list_cycles("conv-0001")),
    ("get_plan", lambda s: s.get_plan("sess-0001", "plan-1")),
    ("list_plans", lambda s: s.list_plans("sess-0001")),
    ("find_plans_in_states", lambda s: s.find_plans_in_states([PlanState.RUNNING])),
    ("get_task", lambda s: s.get_task("sess-0001", "t1")),
    ("list_tasks", lambda s: s.list_tasks("sess-0001")),
    ("find_tasks_in_states", lambda s: s.find_tasks_in_states([TaskState.RUNNING])),
    ("get_message", lambda s: s.get_message("msg-0001")),
    ("list_messages", lambda s: s.list_messages("conv-0001")),
    ("list_failures", lambda s: s.list_failures("sess-0001")),
    ("list_retry_decisions", lambda s: s.list_retry_decisions("sess-0001")),
    ("get_context_summary_for_target", lambda s: s.get_context_summary_for_target("conv-0002")),
    ("list_context_summaries", lambda s: s.list_context_summaries("sess-0001")),
    ("get_blob", lambda s: s.get_blob("blob-0001")),
    ("get_blob_for_task", lambda s: s.get_blob_for_task("sess-0001", "t1", OutputStream.STDOUT)),
    ("read_blob_range", lambda s: s.read_blob_range("blob-0001", 0, 1)),
    ("get_last_audit_event", lambda s: s.get_last_audit_event("sess-0001")),
    ("list_audit_events", lambda s: s.list_audit_events("sess-0001")),
    ("count_audit_events", lambda s: s.count_audit_events("sess-0001")),
]


@pytest.mark.parametrize(
    ("name", "operation"), WRITE_OPERATIONS, ids=[n for n, _ in WRITE_OPERATIONS]
)
def given_closed_store_when_write_attempted_then_store_closed_error(
    store_impl: ConversationStore, name: str, operation: Callable[[ConversationStore], None]
) -> None:
    store_impl.close()
    with pytest.raises(PersistenceError) as exc:
        operation(store_impl)
    assert error_of(exc).error_code == "STORE_CLOSED", name


def given_store_when_closed_twice_then_no_error(store_impl: ConversationStore) -> None:
    store_impl.close()
    store_impl.close()


# ================================================================================================
# 10. Contract — scoping by session (rotation keeps blobs readable, ADR-011)
# ================================================================================================
def given_records_of_two_sessions_when_session_scoped_listings_used_then_other_session_invisible(
    store_impl: ConversationStore,
) -> None:
    for sid in ("sess-0001", "sess-0002"):
        store_impl.save_session(make_session(session_id=sid))
        store_impl.save_conversation(
            make_conversation(conversation_id=f"conv-{sid}", session_id=sid)
        )
        store_impl.save_plan(make_plan(session_id=sid))
        store_impl.save_task(make_task(session_id=sid))
        store_impl.save_failure(make_failure(failure_id=f"fail-{sid}", session_id=sid))
        store_impl.save_retry_decision(
            make_retry_decision(decision_id=f"dec-{sid}", session_id=sid)
        )
        store_impl.save_context_summary(make_summary(summary_id=f"sum-{sid}", session_id=sid))
        store_impl.save_blob(make_blob(blob_id=f"blob-{sid}", session_id=sid))
        for event in audit_chain(sid, 2):
            store_impl.append_audit_event(event)
    for sid in ("sess-0001", "sess-0002"):
        assert [c.session_id for c in store_impl.list_conversations(sid)] == [sid]
        assert [p.session_id for p in store_impl.list_plans(sid)] == [sid]
        assert [t.session_id for t in store_impl.list_tasks(sid)] == [sid]
        assert [f.session_id for f in store_impl.list_failures(sid)] == [sid]
        assert [d.session_id for d in store_impl.list_retry_decisions(sid)] == [sid]
        assert [c.session_id for c in store_impl.list_context_summaries(sid)] == [sid]
        blob = store_impl.get_blob_for_task(sid, "t1", OutputStream.STDOUT)
        assert blob is not None and blob.blob_id == f"blob-{sid}"
        assert store_impl.count_audit_events(sid) == 2
    assert store_impl.get_plan("sess-0001", "plan-1") != store_impl.get_plan("sess-0002", "plan-1")
    assert len(store_impl.list_sessions()) == 2


# ================================================================================================
# 11. SQLite — reopen, WAL, idempotent schema, schema version, pragmas
# ================================================================================================
def given_file_store_with_data_when_closed_and_reopened_then_data_present(db_path: Path) -> None:
    store = SqliteConversationStore(db_path)
    store.save_session(make_session())
    store.save_conversation(make_conversation())
    store.save_plan(make_plan())
    store.save_tasks(
        [make_task(task_id="t0", order_index=0), make_task(task_id="t1", order_index=1)]
    )
    store.save_blob(make_blob())
    for event in audit_chain("sess-0001", 3):
        store.append_audit_event(event)
    store.close()

    reopened = SqliteConversationStore(db_path)
    try:
        assert reopened.get_session("sess-0001") == make_session()
        assert reopened.list_conversations("sess-0001") == [make_conversation()]
        assert reopened.list_tasks("sess-0001") == [
            make_task(task_id="t0", order_index=0),
            make_task(task_id="t1", order_index=1),
        ]
        assert reopened.read_blob_range("blob-0001", 250, 10) == BLOB_CONTENT[250:]
        assert reopened.count_audit_events("sess-0001") == 3
        assert reopened.get_last_audit_event("sess-0001") == audit_chain("sess-0001", 3)[-1]
        reopened.append_audit_event(audit_chain("sess-0001", 4)[3])
        assert reopened.count_audit_events("sess-0001") == 4
    finally:
        reopened.close()


def given_file_store_when_opened_then_wal_journal_and_pragmas_applied(db_path: Path) -> None:
    store = SqliteConversationStore(db_path)
    try:
        assert store.journal_mode == "wal"
        assert store.pragma("foreign_keys") == 1
        assert store.pragma("synchronous") == 2  # FULL (ADR-019)
        assert store.path == str(db_path) and store.in_memory is False
    finally:
        store.close()
    raw = sqlite3.connect(str(db_path))
    try:
        assert raw.execute("PRAGMA journal_mode").fetchone()[0] == "wal", (
            "WAL is persisted in the file"
        )
    finally:
        raw.close()


def given_memory_store_when_opened_then_memory_journal_and_instances_independent() -> None:
    first = SqliteConversationStore(":memory:")
    second = SqliteConversationStore(":memory:")
    try:
        assert first.journal_mode == "memory" and first.in_memory is True
        assert first.pragma("foreign_keys") == 1
        first.save_session(make_session())
        assert second.get_session("sess-0001") is None
        assert first.get_session("sess-0001") == make_session()
    finally:
        first.close()
        second.close()


def given_existing_database_when_opened_twice_then_schema_idempotent_and_writes_visible_across(
    db_path: Path,
) -> None:
    first = SqliteConversationStore(db_path)
    second = SqliteConversationStore(db_path)
    try:
        first.save_session(make_session())
        assert second.get_session("sess-0001") == make_session()
        second.save_conversation(make_conversation())
        assert first.list_conversations("sess-0001") == [make_conversation()]
        for event in audit_chain("sess-0001", 2):
            first.append_audit_event(event)
        assert second.count_audit_events("sess-0001") == 2
    finally:
        first.close()
        second.close()
    third = SqliteConversationStore(str(db_path))
    try:
        assert third.get_session("sess-0001") == make_session()
    finally:
        third.close()


EXPECTED_TABLES: dict[str, type[Record]] = {
    "sessions": SessionRecord,
    "conversations": ConversationRecord,
    "cycles": CycleRecord,
    "plans": PlanRecord,
    "tasks": TaskRecord,
    "messages": MessageRecord,
    "failures": FailureRecord,
    "retry_decisions": RetryDecisionRecord,
    "context_summaries": ContextSummaryRecord,
    "blobs": BlobRecord,
    "audit_events": AuditEvent,
}
EXPECTED_PRIMARY_KEYS: dict[str, set[str]] = {
    "sessions": {"session_id"},
    "conversations": {"conversation_id"},
    "cycles": {"cycle_id"},
    "plans": {"session_id", "plan_id"},
    "tasks": {"session_id", "task_id"},
    "messages": {"message_id"},
    "failures": {"failure_id"},
    "retry_decisions": {"decision_id"},
    "context_summaries": {"summary_id"},
    "blobs": {"blob_id"},
    "audit_events": {"session_id", "sequence"},
}
SQL_TYPE_FOR_KIND: dict[str, set[str]] = {
    "str": {"TEXT"},
    "int": {"INTEGER"},
    "bool": {"INTEGER"},
    "bytes": {"BLOB"},
    "datetime": {"TEXT"},
    "enum": {"TEXT"},
    "json": {"TEXT"},
}


def _expected_kind(annotation: Any) -> str:
    inner = next((a for a in get_args(annotation) if a is not type(None)), annotation)
    origin = getattr(inner, "__origin__", inner)
    if isinstance(origin, type):
        if issubclass(origin, bool):
            return "bool"
        if issubclass(origin, (SessionState, ErrorType)) or hasattr(origin, "__members__"):
            return "enum"
        if issubclass(origin, int):
            return "int"
        if issubclass(origin, str):
            return "str"
        if issubclass(origin, bytes):
            return "bytes"
        if issubclass(origin, datetime):
            return "datetime"
    return "json"


def given_new_database_when_created_then_every_model_field_has_a_typed_column_and_keys_match(
    sqlite_store: SqliteConversationStore, db_path: Path
) -> None:
    raw = sqlite3.connect(str(db_path))
    try:
        tables = {
            row[0] for row in raw.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert set(EXPECTED_TABLES) | {"schema_version"} <= tables
        assert raw.execute("SELECT version FROM schema_version").fetchall() == [(SCHEMA_VERSION,)]
        assert SCHEMA_VERSION == 1
        for table, model in EXPECTED_TABLES.items():
            info = raw.execute(f"PRAGMA table_info({table})").fetchall()
            columns = {row[1]: row for row in info}  # name -> (cid, name, type, notnull, dflt, pk)
            assert set(columns) == set(model.model_fields), table
            assert {name for name, row in columns.items() if row[5] > 0} == EXPECTED_PRIMARY_KEYS[
                table
            ]
            for name, field in model.model_fields.items():
                _, _, sql_type, notnull, _, _ = columns[name]
                assert sql_type in SQL_TYPE_FOR_KIND[_expected_kind(field.annotation)], (
                    table,
                    name,
                )
                assert bool(notnull) is (not allows_none(field.annotation)), (table, name)
        indexes = {
            row[0] for row in raw.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
        }
        indexed_columns = set()
        for index in indexes:
            indexed_columns |= {row[2] for row in raw.execute(f"PRAGMA index_info({index})")}
        assert {"session_id", "conversation_id", "status", "plan_id", "event_id"} <= indexed_columns
    finally:
        raw.close()


def given_database_with_unsupported_schema_version_when_opened_then_persistence_error(
    db_path: Path,
) -> None:
    raw = sqlite3.connect(str(db_path))
    raw.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
    raw.execute("INSERT INTO schema_version (version) VALUES (99)")
    raw.commit()
    raw.close()
    with pytest.raises(PersistenceError) as exc:
        SqliteConversationStore(db_path)
    error = error_of(exc)
    assert error.error_code == "SCHEMA_VERSION_UNSUPPORTED"
    assert error.details["found"] == 99 and error.details["supported"] == SCHEMA_VERSION


def given_unreachable_directory_when_store_created_then_sqlite_error_mapped(tmp_path: Path) -> None:
    with pytest.raises(PersistenceError) as exc:
        SqliteConversationStore(tmp_path / "missing" / "dir" / "agentic.db")
    error = error_of(exc)
    assert error.error_code == "SQLITE_ERROR"
    assert error.error_type is ErrorType.PERSISTENCE_ERROR
    assert "sqlite" in error.details


def given_store_when_used_as_context_manager_then_closed_on_exit(db_path: Path) -> None:
    with SqliteConversationStore(db_path) as store:
        store.save_session(make_session())
        assert store.closed is False
    assert store.closed is True
    with pytest.raises(PersistenceError) as exc:
        store.get_session("sess-0001")
    assert error_of(exc).error_code == "STORE_CLOSED"


# ================================================================================================
# 12. SQLite — errors: closed store, sqlite3.Error mapping, corrupted rows, datetimes
# ================================================================================================
@pytest.mark.parametrize(
    ("name", "operation"), READ_OPERATIONS, ids=[n for n, _ in READ_OPERATIONS]
)
def given_closed_sqlite_store_when_read_attempted_then_store_closed_error(
    sqlite_store: SqliteConversationStore, name: str, operation: Callable[[ConversationStore], Any]
) -> None:
    sqlite_store.close()
    with pytest.raises(PersistenceError) as exc:
        operation(sqlite_store)
    assert error_of(exc).error_code == "STORE_CLOSED", name


def given_closed_sqlite_store_when_transaction_opened_then_store_closed_error(
    sqlite_store: SqliteConversationStore,
) -> None:
    sqlite_store.close()
    with pytest.raises(PersistenceError) as exc, sqlite_store.transaction():
        pass
    assert error_of(exc).error_code == "STORE_CLOSED"


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (sqlite3.OperationalError("database is locked"), True),
        (sqlite3.OperationalError("database table is locked: sessions"), True),
        (sqlite3.OperationalError("database is busy"), True),
        (sqlite3.OperationalError("no such table: nope"), False),
        (sqlite3.IntegrityError("UNIQUE constraint failed: audit_events.event_id"), False),
        (sqlite3.DatabaseError("database disk image is malformed"), False),
        (sqlite3.ProgrammingError("Cannot operate on a closed database."), False),
    ],
    ids=["locked", "table_locked", "busy", "no_table", "integrity", "malformed", "programming"],
)
def given_sqlite_error_when_mapped_then_persistence_error_with_details_and_transient_flag(
    exc: sqlite3.Error, transient: bool
) -> None:
    mapped = persistence_error_from_sqlite(exc)
    assert isinstance(mapped, PersistenceError)
    assert mapped.error.error_type is ErrorType.PERSISTENCE_ERROR
    assert mapped.error.error_code == "SQLITE_ERROR"
    assert mapped.error.severity is Severity.CRITICAL
    assert mapped.error.details["sqlite"] == str(exc)
    assert mapped.error.details["transient"] is transient
    assert mapped.error.retryable is transient and mapped.error.recoverable is transient


def given_locked_database_when_write_attempted_then_transient_sqlite_error_then_recovers(
    db_path: Path,
) -> None:
    store = SqliteConversationStore(db_path, busy_timeout_ms=20)
    other = sqlite3.connect(str(db_path), isolation_level=None, timeout=0.05)
    try:
        other.execute("BEGIN IMMEDIATE")
        with pytest.raises(PersistenceError) as exc:
            store.save_session(make_session())
        error = error_of(exc)
        assert error.error_code == "SQLITE_ERROR"
        assert error.details["transient"] is True and error.retryable is True
        assert "locked" in error.details["sqlite"]
        other.execute("ROLLBACK")
        store.save_session(make_session())
        assert store.get_session("sess-0001") == make_session()
    finally:
        other.close()
        store.close()


def given_locked_database_when_transaction_begins_then_transient_error_and_depth_reset(
    db_path: Path,
) -> None:
    store = SqliteConversationStore(db_path, busy_timeout_ms=20)
    other = sqlite3.connect(str(db_path), isolation_level=None, timeout=0.05)
    try:
        other.execute("BEGIN IMMEDIATE")
        with pytest.raises(PersistenceError) as exc, store.transaction():
            store.save_session(make_session())
        assert error_of(exc).details["transient"] is True
        other.execute("ROLLBACK")
        with store.transaction():
            store.save_session(make_session())
        assert store.get_session("sess-0001") == make_session()
    finally:
        other.close()
        store.close()


def given_corrupted_row_when_read_then_record_invalid_error(
    sqlite_store: SqliteConversationStore, db_path: Path
) -> None:
    sqlite_store.save_session(make_session())
    raw = sqlite3.connect(str(db_path))
    raw.execute("UPDATE sessions SET status = 'BOGUS'")
    raw.commit()
    raw.close()
    with pytest.raises(PersistenceError) as exc:
        sqlite_store.get_session("sess-0001")
    error = error_of(exc)
    assert error.error_code == "RECORD_INVALID"
    assert error.details["table"] == "sessions"
    assert "BOGUS" in error.details["error"]


def given_naive_datetime_when_saved_then_datetime_not_tz_aware_error_and_nothing_written(
    sqlite_store: SqliteConversationStore,
) -> None:
    naive = make_session(created_at=datetime(2026, 1, 1, 12, 0, 0))
    with pytest.raises(PersistenceError) as exc:
        sqlite_store.save_session(naive)
    error = error_of(exc)
    assert error.error_code == "DATETIME_NOT_TZ_AWARE"
    assert error.details["field"] == "created_at"
    assert sqlite_store.get_session("sess-0001") is None


def given_non_utc_aware_datetime_when_saved_then_stored_as_utc_text_and_read_back_equal(
    sqlite_store: SqliteConversationStore, db_path: Path
) -> None:
    paris = timezone(timedelta(hours=2))
    record = make_session(created_at=datetime(2026, 1, 1, 14, 0, 0, tzinfo=paris))
    sqlite_store.save_session(record)
    read = sqlite_store.get_session("sess-0001")
    assert read == record
    assert read is not None and read.created_at.utcoffset() == timedelta(0)
    assert read.created_at == datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    raw = sqlite3.connect(str(db_path))
    try:
        stored = raw.execute("SELECT created_at FROM sessions").fetchone()[0]
    finally:
        raw.close()
    assert stored == "2026-01-01T12:00:00.000000+00:00"
    assert datetime.fromisoformat(stored) == record.created_at


def given_timestamps_with_and_without_microseconds_when_sessions_listed_then_chronological(
    sqlite_store: SqliteConversationStore,
) -> None:
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    records = [
        make_session(session_id="sess-a", created_at=base),
        make_session(session_id="sess-b", created_at=base + timedelta(microseconds=1)),
        make_session(session_id="sess-c", created_at=base + timedelta(seconds=1)),
        make_session(session_id="sess-d", created_at=base + timedelta(seconds=1, microseconds=5)),
    ]
    for record in (records[2], records[0], records[3], records[1]):
        sqlite_store.save_session(record)
    assert sqlite_store.list_sessions() == list(reversed(records))


# ================================================================================================
# 13. SQLite — savepoint semantics and throughput
# ================================================================================================
def given_inner_transaction_failure_caught_by_outer_when_outer_commits_then_only_outer_writes_kept(
    sqlite_store: SqliteConversationStore,
) -> None:
    with sqlite_store.transaction():
        sqlite_store.save_session(make_session(session_id="sess-0001"))
        try:
            with sqlite_store.transaction():
                sqlite_store.save_session(make_session(session_id="sess-0002"))
                sqlite_store.save_conversation(make_conversation())
                raise RuntimeError("inner failure handled by the caller")
        except RuntimeError:
            pass
        sqlite_store.save_session(make_session(session_id="sess-0003"))
        assert sqlite_store.get_session("sess-0002") is None, (
            "savepoint rolled back inside the outer"
        )
    assert {s.session_id for s in sqlite_store.list_sessions()} == {"sess-0001", "sess-0003"}
    assert sqlite_store.get_conversation("conv-0001") is None


def given_inner_savepoint_rolled_back_when_sibling_inner_transaction_follows_then_it_commits(
    sqlite_store: SqliteConversationStore,
) -> None:
    with sqlite_store.transaction():
        try:
            with sqlite_store.transaction():
                sqlite_store.save_cycle(make_cycle(cycle_id="cyc-lost"))
                raise ValueError("lost")
        except ValueError:
            pass
        with sqlite_store.transaction():
            sqlite_store.save_cycle(make_cycle(cycle_id="cyc-kept"))
    assert [c.cycle_id for c in sqlite_store.list_cycles("conv-0001")] == ["cyc-kept"]


# Sanity bound, not a benchmark: with ``synchronous=FULL`` (ADR-019 §9) every append fsyncs, which
# costs ~0.1 ms on Linux tmpfs but ~4 ms on the Windows CI runners; the bound only guards against
# pathological slowness (per-event cost is negligible next to the model round-trips).
AUDIT_APPEND_SANITY_BOUND_S = 15.0


def given_sqlite_file_store_when_1000_audit_events_appended_then_within_sanity_bound(
    sqlite_store: SqliteConversationStore,
) -> None:
    chain = audit_chain("sess-perf", 1000)
    started = time.perf_counter()
    for event in chain:
        sqlite_store.append_audit_event(event)
    elapsed = time.perf_counter() - started
    assert elapsed < AUDIT_APPEND_SANITY_BOUND_S, f"1000 appends took {elapsed:.2f}s"
    assert sqlite_store.count_audit_events("sess-perf") == 1000
    assert sqlite_store.get_last_audit_event("sess-perf") == chain[-1]
    assert sqlite_store.list_audit_events("sess-perf", after_sequence=998) == chain[998:]


# ================================================================================================
# 14. Factory — open_store(config)
# ================================================================================================
def given_config_with_missing_nested_data_dir_when_open_store_then_directory_and_database_created(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "var" / "agentic" / "data"
    config = AppConfig(app=AppSection(data_dir=str(data_dir)))
    store = open_store(config)
    try:
        assert isinstance(store, SqliteConversationStore)
        assert data_dir.is_dir() and (data_dir / DB_FILENAME).is_file()
        assert DB_FILENAME == "agentic.db"
        assert store.path == str(data_dir / DB_FILENAME)
        store.save_session(make_session())
        assert store.get_session("sess-0001") == make_session()
    finally:
        store.close()


def given_config_fixture_when_open_store_twice_then_same_database_reused(config: AppConfig) -> None:
    first = open_store(config)
    first.save_session(make_session())
    first.close()
    second = open_store(config)
    try:
        assert second.get_session("sess-0001") == make_session()
        assert Path(config.app.data_dir).is_dir()
    finally:
        second.close()


def given_data_dir_path_occupied_by_a_file_when_open_store_then_persistence_error(
    tmp_path: Path,
) -> None:
    blocker = tmp_path / "data"
    blocker.write_text("not a directory")
    with pytest.raises(PersistenceError) as exc:
        open_store(AppConfig(app=AppSection(data_dir=str(blocker))))
    error = error_of(exc)
    assert error.error_code == "DATA_DIR_UNAVAILABLE"
    assert error.details["path"] == str(blocker)


# ================================================================================================
# 15. Structure — the ABC is fully implemented, no wall clock or randomness in production code
# ================================================================================================
def given_sqlite_store_class_when_inspected_then_every_abstract_method_implemented() -> None:
    assert issubclass(SqliteConversationStore, ConversationStore)
    assert SqliteConversationStore.__abstractmethods__ == frozenset()
    for name in ConversationStore.__abstractmethods__:
        assert getattr(SqliteConversationStore, name) is not getattr(ConversationStore, name), name


def given_persistence_sources_when_inspected_then_no_wall_clock_or_randomness_used() -> None:
    for module in (sqlite_module, factory_module):
        source = Path(module.__file__ or "").read_text(encoding="utf-8")
        for forbidden in ("datetime.now", "time.time", "time.monotonic", "uuid", "random"):
            assert forbidden not in source, (module.__name__, forbidden)
