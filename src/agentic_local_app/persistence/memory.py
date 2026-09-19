"""In-memory ``ConversationStore`` for unit tests (§18.3).

Semantics are identical to the SQLite store (same contract test-suite in phase 3), including the
append-only audit rule, range reads on blobs, and a transaction that rolls back every write made
inside it when an exception escapes (copy-on-enter snapshot, cheap for test-sized data).
"""

from __future__ import annotations

import copy
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from agentic_local_app.domain.errors import PersistenceError
from agentic_local_app.domain.models import (
    AuditEvent,
    BlobRecord,
    ContextSummaryRecord,
    ConversationRecord,
    CycleRecord,
    FailureRecord,
    MessageRecord,
    PlanRecord,
    RetryDecisionRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ConversationState,
    MessageDirection,
    OutputStream,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.persistence.interface import ConversationStore


class InMemoryConversationStore(ConversationStore):
    def __init__(self) -> None:
        self._sessions: dict[str, SessionRecord] = {}
        self._conversations: dict[str, ConversationRecord] = {}
        self._cycles: dict[str, CycleRecord] = {}
        self._plans: dict[tuple[str, str], PlanRecord] = {}
        self._tasks: dict[tuple[str, str], TaskRecord] = {}
        self._messages: dict[str, MessageRecord] = {}
        self._failures: dict[str, FailureRecord] = {}
        self._retry_decisions: dict[str, RetryDecisionRecord] = {}
        self._summaries: dict[str, ContextSummaryRecord] = {}
        self._blobs: dict[str, BlobRecord] = {}
        self._audit: dict[str, list[AuditEvent]] = {}
        self._insertion_counter = 0
        self._order: dict[str, int] = {}  # insertion order of keyed records (stable listing)
        self._snapshots: list[dict[str, Any]] = []
        self.fail_next_write: bool = False  # test hook: next write raises PersistenceError
        self.closed = False

    # ---- helpers --------------------------------------------------------------------------
    def _write_guard(self) -> None:
        if self.closed:
            raise PersistenceError("STORE_CLOSED")
        if self.fail_next_write:
            self.fail_next_write = False
            raise PersistenceError("SIMULATED_WRITE_FAILURE", transient=False)

    def _touch(self, key: str) -> None:
        if key not in self._order:
            self._insertion_counter += 1
            self._order[key] = self._insertion_counter

    def _state(self) -> dict[str, Any]:
        return {
            name: copy.deepcopy(getattr(self, name))
            for name in (
                "_sessions",
                "_conversations",
                "_cycles",
                "_plans",
                "_tasks",
                "_messages",
                "_failures",
                "_retry_decisions",
                "_summaries",
                "_blobs",
                "_audit",
                "_order",
                "_insertion_counter",
            )
        }

    def _restore(self, snapshot: dict[str, Any]) -> None:
        for name, value in snapshot.items():
            setattr(self, name, value)

    # ---- transactions ---------------------------------------------------------------------
    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Savepoint semantics (ADR-019): every level snapshots on entry; an exception escaping
        a level restores that level's snapshot only, so an inner failure caught by the outer
        block loses the inner writes and keeps the outer ones - exactly like SQLite SAVEPOINTs."""
        if self.closed:
            raise PersistenceError("STORE_CLOSED")
        self._snapshots.append(self._state())
        try:
            yield
        except BaseException:
            self._restore(self._snapshots.pop())
            raise
        else:
            self._snapshots.pop()

    def transaction(self) -> AbstractContextManager[None]:
        return self._transaction()

    # ---- sessions -------------------------------------------------------------------------
    def save_session(self, record: SessionRecord) -> None:
        self._write_guard()
        self._sessions[record.session_id] = record
        self._touch(f"session:{record.session_id}")

    def get_session(self, session_id: str) -> SessionRecord | None:
        return self._sessions.get(session_id)

    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]:
        wanted = set(statuses) if statuses is not None else None
        rows = [s for s in self._sessions.values() if wanted is None or s.status in wanted]
        rows.sort(key=lambda s: (s.created_at, s.session_id), reverse=True)
        return rows[offset : offset + limit]

    # ---- conversations --------------------------------------------------------------------
    def save_conversation(self, record: ConversationRecord) -> None:
        self._write_guard()
        self._conversations[record.conversation_id] = record
        self._touch(f"conversation:{record.conversation_id}")

    def get_conversation(self, conversation_id: str) -> ConversationRecord | None:
        return self._conversations.get(conversation_id)

    def list_conversations(self, session_id: str) -> list[ConversationRecord]:
        rows = [c for c in self._conversations.values() if c.session_id == session_id]
        rows.sort(key=lambda c: self._order[f"conversation:{c.conversation_id}"])
        return rows

    def find_conversations_in_states(
        self, states: Iterable[ConversationState]
    ) -> list[ConversationRecord]:
        wanted = set(states)
        rows = [c for c in self._conversations.values() if c.status in wanted]
        rows.sort(key=lambda c: self._order[f"conversation:{c.conversation_id}"])
        return rows

    # ---- cycles ---------------------------------------------------------------------------
    def save_cycle(self, record: CycleRecord) -> None:
        self._write_guard()
        self._cycles[record.cycle_id] = record
        self._touch(f"cycle:{record.cycle_id}")

    def get_cycle(self, cycle_id: str) -> CycleRecord | None:
        return self._cycles.get(cycle_id)

    def list_cycles(self, conversation_id: str) -> list[CycleRecord]:
        rows = [c for c in self._cycles.values() if c.conversation_id == conversation_id]
        rows.sort(key=lambda c: self._order[f"cycle:{c.cycle_id}"])
        return rows

    # ---- plans ----------------------------------------------------------------------------
    def save_plan(self, record: PlanRecord) -> None:
        self._write_guard()
        self._plans[(record.session_id, record.plan_id)] = record
        self._touch(f"plan:{record.session_id}:{record.plan_id}")

    def get_plan(self, session_id: str, plan_id: str) -> PlanRecord | None:
        return self._plans.get((session_id, plan_id))

    def list_plans(
        self, session_id: str, *, conversation_id: str | None = None
    ) -> list[PlanRecord]:
        rows = [
            p
            for p in self._plans.values()
            if p.session_id == session_id
            and (conversation_id is None or p.conversation_id == conversation_id)
        ]
        rows.sort(key=lambda p: self._order[f"plan:{p.session_id}:{p.plan_id}"])
        return rows

    def find_plans_in_states(self, states: Iterable[PlanState]) -> list[PlanRecord]:
        wanted = set(states)
        rows = [p for p in self._plans.values() if p.status in wanted]
        rows.sort(key=lambda p: self._order[f"plan:{p.session_id}:{p.plan_id}"])
        return rows

    # ---- tasks ----------------------------------------------------------------------------
    def save_task(self, record: TaskRecord) -> None:
        self._write_guard()
        self._tasks[(record.session_id, record.task_id)] = record
        self._touch(f"task:{record.session_id}:{record.task_id}")

    def save_tasks(self, records: Iterable[TaskRecord]) -> None:
        with self.transaction():
            for record in records:
                self.save_task(record)

    def get_task(self, session_id: str, task_id: str) -> TaskRecord | None:
        return self._tasks.get((session_id, task_id))

    def _plan_order(self, task: TaskRecord) -> int:
        return self._order.get(f"plan:{task.session_id}:{task.plan_id}", 0)

    def list_tasks(
        self,
        session_id: str,
        *,
        plan_id: str | None = None,
        statuses: Iterable[TaskState] | None = None,
    ) -> list[TaskRecord]:
        wanted = set(statuses) if statuses is not None else None
        rows = [
            t
            for t in self._tasks.values()
            if t.session_id == session_id
            and (plan_id is None or t.plan_id == plan_id)
            and (wanted is None or t.status in wanted)
        ]
        rows.sort(key=lambda t: (self._plan_order(t), t.order_index))
        return rows

    def find_tasks_in_states(self, states: Iterable[TaskState]) -> list[TaskRecord]:
        wanted = set(states)
        rows = [t for t in self._tasks.values() if t.status in wanted]
        rows.sort(key=lambda t: (self._plan_order(t), t.order_index))
        return rows

    # ---- messages -------------------------------------------------------------------------
    def save_message(self, record: MessageRecord) -> None:
        self._write_guard()
        self._messages[record.message_id] = record
        self._touch(f"message:{record.message_id}")

    def get_message(self, message_id: str) -> MessageRecord | None:
        return self._messages.get(message_id)

    def list_messages(
        self,
        conversation_id: str,
        *,
        direction: MessageDirection | None = None,
    ) -> list[MessageRecord]:
        rows = [
            m
            for m in self._messages.values()
            if m.conversation_id == conversation_id
            and (direction is None or m.direction == direction)
        ]
        rows.sort(key=lambda m: self._order[f"message:{m.message_id}"])
        return rows

    # ---- failures / retry decisions -------------------------------------------------------
    def save_failure(self, record: FailureRecord) -> None:
        self._write_guard()
        self._failures[record.failure_id] = record
        self._touch(f"failure:{record.failure_id}")

    def list_failures(self, session_id: str) -> list[FailureRecord]:
        rows = [f for f in self._failures.values() if f.session_id == session_id]
        rows.sort(key=lambda f: self._order[f"failure:{f.failure_id}"])
        return rows

    def save_retry_decision(self, record: RetryDecisionRecord) -> None:
        self._write_guard()
        self._retry_decisions[record.decision_id] = record
        self._touch(f"retry:{record.decision_id}")

    def list_retry_decisions(self, session_id: str) -> list[RetryDecisionRecord]:
        rows = [r for r in self._retry_decisions.values() if r.session_id == session_id]
        rows.sort(key=lambda r: self._order[f"retry:{r.decision_id}"])
        return rows

    # ---- context summaries ----------------------------------------------------------------
    def save_context_summary(self, record: ContextSummaryRecord) -> None:
        self._write_guard()
        self._summaries[record.summary_id] = record
        self._touch(f"summary:{record.summary_id}")

    def get_context_summary_for_target(
        self, target_conversation_id: str
    ) -> ContextSummaryRecord | None:
        for s in self._summaries.values():
            if s.target_conversation_id == target_conversation_id:
                return s
        return None

    def list_context_summaries(self, session_id: str) -> list[ContextSummaryRecord]:
        rows = [s for s in self._summaries.values() if s.session_id == session_id]
        rows.sort(key=lambda s: self._order[f"summary:{s.summary_id}"])
        return rows

    # ---- blobs ----------------------------------------------------------------------------
    def save_blob(self, record: BlobRecord) -> None:
        self._write_guard()
        if record.size_bytes != len(record.content):
            raise PersistenceError("BLOB_SIZE_MISMATCH", blob_id=record.blob_id)
        self._blobs[record.blob_id] = record
        self._touch(f"blob:{record.blob_id}")

    def get_blob(self, blob_id: str) -> BlobRecord | None:
        return self._blobs.get(blob_id)

    def get_blob_for_task(
        self, session_id: str, task_id: str, blob_type: OutputStream
    ) -> BlobRecord | None:
        for b in self._blobs.values():
            if b.session_id == session_id and b.task_id == task_id and b.blob_type == blob_type:
                return b
        return None

    def read_blob_range(self, blob_id: str, offset: int, max_bytes: int) -> bytes:
        blob = self._blobs.get(blob_id)
        if blob is None:
            raise PersistenceError("BLOB_NOT_FOUND", blob_id=blob_id)
        if offset < 0 or max_bytes < 0:
            raise PersistenceError(
                "BLOB_RANGE_INVALID", blob_id=blob_id, offset=offset, max_bytes=max_bytes
            )
        return blob.content[offset : offset + max_bytes]

    # ---- audit ----------------------------------------------------------------------------
    def append_audit_event(self, event: AuditEvent) -> None:
        self._write_guard()
        chain = self._audit.setdefault(event.session_id, [])
        if any(e.sequence == event.sequence for e in chain) or any(
            e.event_id == event.event_id for events in self._audit.values() for e in events
        ):
            raise PersistenceError(
                "AUDIT_APPEND_ONLY_VIOLATION", event_id=event.event_id, sequence=event.sequence
            )
        if chain and event.sequence != chain[-1].sequence + 1:
            raise PersistenceError(
                "AUDIT_SEQUENCE_GAP", expected=chain[-1].sequence + 1, got=event.sequence
            )
        chain.append(event)

    def get_last_audit_event(self, session_id: str) -> AuditEvent | None:
        chain = self._audit.get(session_id)
        return chain[-1] if chain else None

    def list_audit_events(
        self,
        session_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 1000,
    ) -> list[AuditEvent]:
        chain = self._audit.get(session_id, [])
        rows = [e for e in chain if after_sequence is None or e.sequence > after_sequence]
        return rows[:limit]

    def count_audit_events(self, session_id: str) -> int:
        return len(self._audit.get(session_id, []))

    # ---- maintenance ----------------------------------------------------------------------
    def reset(self) -> None:
        """Empty every collection, insertion order included (the audit chain restarts at genesis)."""
        self._write_guard()
        self._sessions.clear()
        self._conversations.clear()
        self._cycles.clear()
        self._plans.clear()
        self._tasks.clear()
        self._messages.clear()
        self._failures.clear()
        self._retry_decisions.clear()
        self._summaries.clear()
        self._blobs.clear()
        self._audit.clear()
        self._order.clear()
        self._insertion_counter = 0

    def close(self) -> None:
        self.closed = True
