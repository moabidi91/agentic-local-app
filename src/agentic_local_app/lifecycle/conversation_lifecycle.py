"""``ConversationLifecycleManager`` — owner of every session and conversation transition (§3.3).

Contract (spec §17.1, ADR-006, ADR-007, ADR-015, ADR-017):

- a transition is **valid only if listed** in :mod:`agentic_local_app.domain.transitions`; nothing
  here hard-codes a state change, ``assert_transition`` decides;
- the order is always ``validate -> persist (store) -> publish (bus) -> return``: when the store
  raises :class:`~agentic_local_app.domain.errors.PersistenceError` the returned/in-memory state is
  untouched and no event is published;
- every timestamp comes from the injected ``Clock`` and every identifier from the injected
  ``IdGenerator``;
- records are frozen pydantic models: a change produces a **validated** new record
  (``model_validate`` over the dumped record plus the changes), so an unknown field or a value of
  the wrong type is rejected with a ``ValueError`` (pydantic ``ValidationError``) before any write.

Unknown identifiers raise :class:`KeyError` (``get_session`` / ``get_conversation`` return ``None``
instead). Fields owned by the manager (``status``, ``context_window_state``, identifiers and the
``created_at`` / ``updated_at`` timestamps) cannot be set through ``**updates``: ``ValueError``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypeVar

from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import InvalidTransitionError
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    Record,
    SessionBudget,
    SessionRecord,
)
from agentic_local_app.domain.states import ContextWindowState, ConversationState, SessionState
from agentic_local_app.domain.transitions import (
    ACTIVE_CONVERSATION_STATES,
    CONTEXT_WINDOW_TRANSITIONS,
    CONVERSATION_TRANSITIONS,
    SESSION_TRANSITIONS,
    assert_transition,
)
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "CONTEXT_WINDOW_ENTITY",
    "CONVERSATION_ENTITY",
    "SESSION_ENTITY",
    "ConversationLifecycleManager",
]

#: ``entity`` values carried by :class:`InvalidTransitionError` (and its normalized error details).
SESSION_ENTITY = "session"
CONVERSATION_ENTITY = "conversation"
CONTEXT_WINDOW_ENTITY = "context_window"

#: Fields a caller may never set through ``**updates``: they are owned by the manager.
_SESSION_MANAGED_FIELDS: frozenset[str] = frozenset(
    {"status", "session_id", "created_at", "updated_at"}
)
_CONVERSATION_MANAGED_FIELDS: frozenset[str] = frozenset(
    {"status", "context_window_state", "conversation_id", "session_id", "created_at", "updated_at"}
)

R = TypeVar("R", bound=Record)


def _apply(record: R, changes: dict[str, Any]) -> R:
    """A new, fully validated record: ``record`` with ``changes`` applied.

    ``model_copy(update=...)`` is deliberately avoided because it validates nothing (unknown keys
    and wrong types would be persisted silently).
    """
    data = record.model_dump()
    data.update(changes)
    return type(record).model_validate(data)


def _reject_managed_fields(updates: dict[str, Any], managed: frozenset[str], entity: str) -> None:
    forbidden = sorted(managed.intersection(updates))
    if forbidden:
        raise ValueError(
            f"{entity} field(s) {forbidden} are owned by ConversationLifecycleManager "
            "and cannot be set through updates"
        )


class ConversationLifecycleManager:
    """Sessions (ADR-006 / ADR-012) and conversations (§5.1) — create, transition, update, read."""

    def __init__(
        self, store: ConversationStore, bus: EventBus, clock: Clock, ids: IdGenerator
    ) -> None:
        self._store = store
        self._bus = bus
        self._clock = clock
        self._ids = ids

    # ------------------------------------------------------------------ sessions -----------
    def create_session(
        self,
        goal: str,
        user_message: str,
        user_id: str,
        budget: SessionBudget,
        auto_close: bool,
    ) -> SessionRecord:
        """A new ``READY`` session. Persists it, then publishes ``session.created``."""
        now = self._clock.now()
        session = SessionRecord(
            session_id=self._ids.session_id(),
            status=SessionState.READY,
            goal=goal,
            user_message=user_message,
            user_id=user_id,
            auto_close_on_final_answer=auto_close,
            budget=budget,
            created_at=now,
            updated_at=now,
        )
        self._store.save_session(session)
        self._publish(
            EventType.SESSION_CREATED,
            now,
            session_id=session.session_id,
            payload={"goal": goal, "budget": budget.model_dump()},
        )
        return session

    def transition_session(
        self,
        session_id: str,
        to: SessionState,
        *,
        reason: str | None = None,
        **updates: Any,
    ) -> SessionRecord:
        """``current.status -> to`` per ``SESSION_TRANSITIONS``, with ``updates`` in the same write.

        ``COMPLETED -> RUNNING`` (follow-up message) is refused when the session has
        ``auto_close_on_final_answer`` set: ``COMPLETED`` is then terminal for it (ADR-007) —
        unless its current conversation was deliberately left ``WAITING_USER`` because the model
        concluded with a question (``user_response`` with ``expects_reply``, ADR-022): the user
        must be able to answer it. ``started_at`` is set on the first ``RUNNING``, ``ended_at`` on
        ``COMPLETED`` / ``FAILED`` (and cleared when the session runs again), ``interrupted_at``
        on ``INTERRUPTING``.
        """
        current = self._require_session(session_id)
        _reject_managed_fields(updates, _SESSION_MANAGED_FIELDS, SESSION_ENTITY)
        assert_transition(SESSION_TRANSITIONS, current.status, to, entity=SESSION_ENTITY)
        if (
            current.status is SessionState.COMPLETED
            and to is SessionState.RUNNING
            and current.auto_close_on_final_answer
            and not self._conversation_waits_for_user(current)
        ):
            raise InvalidTransitionError(
                entity=SESSION_ENTITY, current=current.status.value, target=to.value
            )

        now = self._clock.now()
        changes: dict[str, Any] = dict(updates)
        changes["status"] = to
        changes["updated_at"] = now
        if to is SessionState.RUNNING:
            if current.started_at is None:
                changes.setdefault("started_at", now)
            changes.setdefault("ended_at", None)
        elif to in (SessionState.COMPLETED, SessionState.FAILED):
            changes.setdefault("ended_at", now)
        elif to is SessionState.INTERRUPTING:
            changes.setdefault("interrupted_at", now)
        session = _apply(current, changes)

        self._store.save_session(session)
        self._publish(
            EventType.SESSION_STATE_CHANGED,
            now,
            session_id=session_id,
            payload=state_change_payload(current.status.value, to.value, reason),
        )
        return session

    def _conversation_waits_for_user(self, session: SessionRecord) -> bool:
        """ADR-022: the current conversation of an auto-close session is ``WAITING_USER`` only
        when the model asked a question, which the user is entitled to answer."""
        if session.current_conversation_id is None:
            return False
        conversation = self._store.get_conversation(session.current_conversation_id)
        return conversation is not None and conversation.status is ConversationState.WAITING_USER

    def update_session(self, session_id: str, **updates: Any) -> SessionRecord:
        """Change plain fields (counters, pointers...) without a state change; no event."""
        current = self._require_session(session_id)
        _reject_managed_fields(updates, _SESSION_MANAGED_FIELDS, SESSION_ENTITY)
        session = _apply(current, {**updates, "updated_at": self._clock.now()})
        self._store.save_session(session)
        return session

    def get_session(self, session_id: str) -> SessionRecord | None:
        return self._store.get_session(session_id)

    # ------------------------------------------------------------------ conversations ------
    def create_conversation(
        self,
        session_id: str,
        *,
        parent_conversation_id: str | None = None,
        context_window_state: ContextWindowState = ContextWindowState.HEALTHY,
    ) -> ConversationRecord:
        """A new ``NEW`` conversation of ``session_id``; the session now points to it.

        ``auto_close_on_final_answer`` and the budget snapshot (``session_budget_json``) are copied
        from the session (ADR-012). The conversation and the session update are written in one
        store transaction, then ``conversation.created`` is published. A parent (interruption,
        ADR-006; rotation, ADR-014) must exist and belong to the same session.
        """
        session = self._require_session(session_id)
        if parent_conversation_id is not None:
            parent = self._require_conversation(parent_conversation_id)
            if parent.session_id != session_id:
                raise ValueError(
                    f"parent conversation {parent_conversation_id} belongs to session "
                    f"{parent.session_id}, not {session_id}"
                )

        now = self._clock.now()
        conversation = ConversationRecord(
            conversation_id=self._ids.conversation_id(),
            session_id=session_id,
            parent_conversation_id=parent_conversation_id,
            status=ConversationState.NEW,
            auto_close_on_final_answer=session.auto_close_on_final_answer,
            context_window_state=context_window_state,
            session_budget_json=session.budget.model_dump(),
            created_at=now,
            updated_at=now,
        )
        updated_session = _apply(
            session,
            {"current_conversation_id": conversation.conversation_id, "updated_at": now},
        )
        with self._store.transaction():
            self._store.save_conversation(conversation)
            self._store.save_session(updated_session)
        self._publish(
            EventType.CONVERSATION_CREATED,
            now,
            session_id=session_id,
            conversation_id=conversation.conversation_id,
            payload={
                "parent_conversation_id": parent_conversation_id,
                "context_window_state": context_window_state.value,
            },
        )
        return conversation

    def transition_conversation(
        self,
        conversation_id: str,
        to: ConversationState,
        *,
        reason: str | None = None,
        **updates: Any,
    ) -> ConversationRecord:
        """``current.status -> to`` per ``CONVERSATION_TRANSITIONS``, ``updates`` in the same write.

        ``interrupted_at`` is set when entering ``INTERRUPTED`` (unless given explicitly).
        """
        current = self._require_conversation(conversation_id)
        _reject_managed_fields(updates, _CONVERSATION_MANAGED_FIELDS, CONVERSATION_ENTITY)
        assert_transition(CONVERSATION_TRANSITIONS, current.status, to, entity=CONVERSATION_ENTITY)
        return self._commit_conversation_transition(current, to, reason=reason, updates=updates)

    def update_conversation(self, conversation_id: str, **updates: Any) -> ConversationRecord:
        """Change plain fields (counters, cursors, remote id...) without a state change; no event."""
        current = self._require_conversation(conversation_id)
        _reject_managed_fields(updates, _CONVERSATION_MANAGED_FIELDS, CONVERSATION_ENTITY)
        conversation = _apply(current, {**updates, "updated_at": self._clock.now()})
        self._store.save_conversation(conversation)
        return conversation

    def interrupt_conversation(self, conversation_id: str, *, reason: str) -> ConversationRecord:
        """User interrupt (§9): ``ANY_ACTIVE_STATE -> INTERRUPTED``, terminal (ADR-006).

        From any other state the interrupt is refused with :class:`InvalidTransitionError`.
        """
        current = self._require_conversation(conversation_id)
        target = ConversationState.INTERRUPTED
        if current.status not in ACTIVE_CONVERSATION_STATES:
            raise InvalidTransitionError(
                entity=CONVERSATION_ENTITY, current=current.status.value, target=target.value
            )
        assert_transition(
            CONVERSATION_TRANSITIONS, current.status, target, entity=CONVERSATION_ENTITY
        )
        return self._commit_conversation_transition(current, target, reason=reason, updates={})

    def transition_context_window(
        self,
        conversation_id: str,
        to: ContextWindowState,
        *,
        reason: str | None = None,
    ) -> ConversationRecord:
        """``context_window_state -> to`` per ``CONTEXT_WINDOW_TRANSITIONS`` (§5.4, ADR-013).

        Publishes ``context.window_state_changed`` with the from / to / reason payload plus the
        persisted ``context_bytes``.
        """
        current = self._require_conversation(conversation_id)
        assert_transition(
            CONTEXT_WINDOW_TRANSITIONS,
            current.context_window_state,
            to,
            entity=CONTEXT_WINDOW_ENTITY,
        )
        now = self._clock.now()
        conversation = _apply(current, {"context_window_state": to, "updated_at": now})

        self._store.save_conversation(conversation)
        payload = state_change_payload(current.context_window_state.value, to.value, reason)
        payload["context_bytes"] = conversation.context_bytes
        self._publish(
            EventType.CONTEXT_WINDOW_STATE_CHANGED,
            now,
            session_id=conversation.session_id,
            conversation_id=conversation_id,
            payload=payload,
        )
        return conversation

    def get_conversation(self, conversation_id: str) -> ConversationRecord | None:
        return self._store.get_conversation(conversation_id)

    # ------------------------------------------------------------------ internals ----------
    def _commit_conversation_transition(
        self,
        current: ConversationRecord,
        to: ConversationState,
        *,
        reason: str | None,
        updates: dict[str, Any],
    ) -> ConversationRecord:
        """Persist then publish an already validated ``current.status -> to`` (ADR-015)."""
        now = self._clock.now()
        changes: dict[str, Any] = dict(updates)
        changes["status"] = to
        changes["updated_at"] = now
        if to is ConversationState.INTERRUPTED:
            changes.setdefault("interrupted_at", now)
        conversation = _apply(current, changes)

        self._store.save_conversation(conversation)
        self._publish(
            EventType.CONVERSATION_STATE_CHANGED,
            now,
            session_id=conversation.session_id,
            conversation_id=conversation.conversation_id,
            payload=state_change_payload(current.status.value, to.value, reason),
        )
        return conversation

    def _require_session(self, session_id: str) -> SessionRecord:
        session = self._store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session

    def _require_conversation(self, conversation_id: str) -> ConversationRecord:
        conversation = self._store.get_conversation(conversation_id)
        if conversation is None:
            raise KeyError(f"unknown conversation: {conversation_id}")
        return conversation

    def _publish(
        self,
        event_type: EventType,
        timestamp: datetime,
        *,
        session_id: str,
        conversation_id: str | None = None,
        payload: dict[str, Any],
    ) -> None:
        """Publish after the store write; ``timestamp`` is the one persisted on the record."""
        self._bus.publish(
            Event(
                event_type=event_type,
                timestamp=timestamp,
                session_id=session_id,
                conversation_id=conversation_id,
                payload=payload,
            )
        )
