"""``ConversationStore`` — the persistence interface (§3.6, §16), synchronous (ADR-001).

Every method is an upsert or a read. Implementations must be transactional per call and support
``transaction()`` for multi-record atomic writes (a whole state transition with its counters).
Any failure surfaces as :class:`~agentic_local_app.domain.errors.PersistenceError`.

Two implementations: :class:`~agentic_local_app.persistence.memory.InMemoryConversationStore`
(unit tests) and :class:`~agentic_local_app.persistence.sqlite_store.SqliteConversationStore`
(runtime, phase 3). Phase 3 tests run the same contract suite against both.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager

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


class ConversationStore(ABC):
    # ---- transactions --------------------------------------------------------------------
    @abstractmethod
    def transaction(self) -> AbstractContextManager[None]:
        """Group several writes atomically. Nested use is allowed (joins the outer transaction)."""

    # ---- sessions -------------------------------------------------------------------------
    @abstractmethod
    def save_session(self, record: SessionRecord) -> None: ...

    @abstractmethod
    def get_session(self, session_id: str) -> SessionRecord | None: ...

    @abstractmethod
    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]:
        """Newest first (by ``created_at`` then ``session_id``)."""

    # ---- conversations --------------------------------------------------------------------
    @abstractmethod
    def save_conversation(self, record: ConversationRecord) -> None: ...

    @abstractmethod
    def get_conversation(self, conversation_id: str) -> ConversationRecord | None: ...

    @abstractmethod
    def list_conversations(self, session_id: str) -> list[ConversationRecord]:
        """Oldest first (creation order = rotation chain order)."""

    @abstractmethod
    def find_conversations_in_states(
        self, states: Iterable[ConversationState]
    ) -> list[ConversationRecord]: ...

    # ---- cycles ---------------------------------------------------------------------------
    @abstractmethod
    def save_cycle(self, record: CycleRecord) -> None: ...

    @abstractmethod
    def get_cycle(self, cycle_id: str) -> CycleRecord | None: ...

    @abstractmethod
    def list_cycles(self, conversation_id: str) -> list[CycleRecord]:
        """Oldest first."""

    # ---- plans ----------------------------------------------------------------------------
    @abstractmethod
    def save_plan(self, record: PlanRecord) -> None: ...

    @abstractmethod
    def get_plan(self, session_id: str, plan_id: str) -> PlanRecord | None: ...

    @abstractmethod
    def list_plans(
        self, session_id: str, *, conversation_id: str | None = None
    ) -> list[PlanRecord]:
        """Oldest first."""

    @abstractmethod
    def find_plans_in_states(self, states: Iterable[PlanState]) -> list[PlanRecord]: ...

    # ---- tasks ----------------------------------------------------------------------------
    @abstractmethod
    def save_task(self, record: TaskRecord) -> None: ...

    @abstractmethod
    def save_tasks(self, records: Iterable[TaskRecord]) -> None:
        """Atomic bulk upsert (plan reception)."""

    @abstractmethod
    def get_task(self, session_id: str, task_id: str) -> TaskRecord | None: ...

    @abstractmethod
    def list_tasks(
        self,
        session_id: str,
        *,
        plan_id: str | None = None,
        statuses: Iterable[TaskState] | None = None,
    ) -> list[TaskRecord]:
        """Ordered by plan creation then ``order_index`` (ADR-017)."""

    @abstractmethod
    def find_tasks_in_states(self, states: Iterable[TaskState]) -> list[TaskRecord]: ...

    # ---- messages -------------------------------------------------------------------------
    @abstractmethod
    def save_message(self, record: MessageRecord) -> None: ...

    @abstractmethod
    def get_message(self, message_id: str) -> MessageRecord | None: ...

    @abstractmethod
    def list_messages(
        self,
        conversation_id: str,
        *,
        direction: MessageDirection | None = None,
    ) -> list[MessageRecord]:
        """Oldest first."""

    # ---- failures / retry decisions -------------------------------------------------------
    @abstractmethod
    def save_failure(self, record: FailureRecord) -> None: ...

    @abstractmethod
    def list_failures(self, session_id: str) -> list[FailureRecord]: ...

    @abstractmethod
    def save_retry_decision(self, record: RetryDecisionRecord) -> None: ...

    @abstractmethod
    def list_retry_decisions(self, session_id: str) -> list[RetryDecisionRecord]: ...

    # ---- context summaries ----------------------------------------------------------------
    @abstractmethod
    def save_context_summary(self, record: ContextSummaryRecord) -> None: ...

    @abstractmethod
    def get_context_summary_for_target(
        self, target_conversation_id: str
    ) -> ContextSummaryRecord | None: ...

    @abstractmethod
    def list_context_summaries(self, session_id: str) -> list[ContextSummaryRecord]: ...

    # ---- blobs ----------------------------------------------------------------------------
    @abstractmethod
    def save_blob(self, record: BlobRecord) -> None: ...

    @abstractmethod
    def get_blob(self, blob_id: str) -> BlobRecord | None: ...

    @abstractmethod
    def get_blob_for_task(
        self, session_id: str, task_id: str, blob_type: OutputStream
    ) -> BlobRecord | None: ...

    @abstractmethod
    def read_blob_range(self, blob_id: str, offset: int, max_bytes: int) -> bytes:
        """Bytes ``[offset, offset + max_bytes)`` clipped to the blob size. Unknown blob -> PersistenceError."""

    # ---- audit ----------------------------------------------------------------------------
    @abstractmethod
    def append_audit_event(self, event: AuditEvent) -> None:
        """Append-only: rewriting an existing ``event_id`` or ``sequence`` is a PersistenceError."""

    @abstractmethod
    def get_last_audit_event(self, session_id: str) -> AuditEvent | None: ...

    @abstractmethod
    def list_audit_events(
        self,
        session_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 1000,
    ) -> list[AuditEvent]:
        """By ``sequence`` ascending."""

    @abstractmethod
    def count_audit_events(self, session_id: str) -> int: ...

    # ---- maintenance ----------------------------------------------------------------------
    @abstractmethod
    def reset(self) -> None:
        """Drop **every** record, atomically; the store stays open and its schema unchanged.

        Destructive and irreversible: sessions, conversations, cycles, plans, tasks, messages,
        blobs, failures, retry decisions, context summaries and the audit chain all go. Only the
        one gated administration route (``POST /admin/reset-database``, ``api
        .allow_destructive_admin``) and the tests call it; nothing in the protocol loop ever does.
        """

    @abstractmethod
    def close(self) -> None: ...
