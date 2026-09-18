"""``AuditLog`` — the hash-chained, append-only audit trail (§3.16, §16, §17.3 ; ADR-015, ADR-017).

The AuditLog is the **critical** subscriber of the EventBus (ADR-015): registered first, it turns
every audited event (``Event.audited``, i.e. everything but ``task.output``, ADR-018) into an
:class:`~agentic_local_app.domain.models.AuditEvent` chained **per session**:

- ``sequence`` is ``1..n`` per session, resumed from ``store.get_last_audit_event`` after a restart;
- ``previous_event_hash`` is the hash of the previous event of the session, or ``GENESIS_HASH``;
- ``event_hash = chain_hash(previous_event_hash, audit_hash_input(event))`` where the hash input is
  **exactly** the dictionary documented by :func:`audit_hash_input` (canonical JSON, ADR-017).

A :class:`~agentic_local_app.domain.errors.PersistenceError` raised by the store propagates through
``bus.publish`` (critical subscriber) and leaves the in-memory chain state untouched, so the next
event takes the same sequence number: the store never holds a partial or forked chain.

Every timestamp written here is the **event's** timestamp (the value persisted on the record by the
publisher, ADR-015), so the audit trail mirrors the bus stream byte for byte (ADR-018: the SSE
stream is this trail). The injected ``Clock`` only dates verifications; no wall clock is read.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from agentic_local_app.domain.canonical import GENESIS_HASH, chain_hash
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import AuditEvent
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "AUDIT_SUBSCRIBER_NAME",
    "REASON_HASH_MISMATCH",
    "REASON_PREVIOUS_HASH_MISMATCH",
    "REASON_SEQUENCE_GAP",
    "AuditLog",
    "AuditVerification",
    "audit_hash_input",
]

#: Name under which the AuditLog registers on the bus (ADR-015 order: audit_log first).
AUDIT_SUBSCRIBER_NAME = "audit_log"

#: ``AuditVerification.reason`` values.
REASON_HASH_MISMATCH = "HASH_MISMATCH"  # recomputed hash differs from the stored event_hash
REASON_PREVIOUS_HASH_MISMATCH = "PREVIOUS_HASH_MISMATCH"  # link to the previous event broken
REASON_SEQUENCE_GAP = "SEQUENCE_GAP"  # sequences are not 1, 2, 3, ... (missing or extra event)


class AuditVerification(BaseModel):
    """Result of :meth:`AuditLog.verify`.

    ``checked`` is the number of events found intact **before** the first break (``first_broken_
    sequence``); when ``valid`` both ``first_broken_sequence`` and ``reason`` are ``None``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    valid: bool
    checked: int
    first_broken_sequence: int | None
    reason: str | None
    verified_at: datetime


def audit_hash_input(event: AuditEvent) -> dict[str, Any]:
    """The dictionary hashed for ``event`` — the contract of ADR-017, verifiable from any language.

    Exactly these keys (``canonical_json`` sorts them): ``event_id``, ``sequence``,
    ``previous_event_hash``, ``session_id``, ``conversation_id``, ``cycle_id``, ``plan_id``,
    ``task_id``, ``event_type`` (the string value), ``timestamp`` (ISO 8601 with offset) and
    ``payload`` (the event payload, JSON-serialisable). ``event_hash`` itself is never part of it.
    """
    return _hash_input(
        event_id=event.event_id,
        sequence=event.sequence,
        previous_event_hash=event.previous_event_hash,
        session_id=event.session_id,
        conversation_id=event.conversation_id,
        cycle_id=event.cycle_id,
        plan_id=event.plan_id,
        task_id=event.task_id,
        event_type=event.event_type,
        timestamp=event.timestamp,
        payload=event.payload,
    )


def _hash_input(
    *,
    event_id: str,
    sequence: int,
    previous_event_hash: str,
    session_id: str,
    conversation_id: str | None,
    cycle_id: str | None,
    plan_id: str | None,
    task_id: str | None,
    event_type: str,
    timestamp: datetime,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "sequence": sequence,
        "previous_event_hash": previous_event_hash,
        "session_id": session_id,
        "conversation_id": conversation_id,
        "cycle_id": cycle_id,
        "plan_id": plan_id,
        "task_id": task_id,
        "event_type": event_type,
        "timestamp": timestamp.isoformat(),
        "payload": payload,
    }


class AuditLog:
    """Append-only, hash-chained audit trail fed by the EventBus (§3.16)."""

    def __init__(self, store: ConversationStore, clock: Clock, ids: IdGenerator) -> None:
        self._store = store
        self._clock = clock
        self._ids = ids
        #: per session: the last chained event (``None`` when the session has no event yet).
        #: Filled lazily from the store, which is the truth after a restart.
        self._last: dict[str, AuditEvent | None] = {}

    # ------------------------------------------------------------------ bus ----------------
    def subscribe(self, bus: EventBus) -> None:
        """Register as the **critical** subscriber ``audit_log`` (ADR-015)."""
        bus.subscribe(self._on_event, name=AUDIT_SUBSCRIBER_NAME, critical=True)

    def _on_event(self, event: Event) -> None:
        self.handle(event)

    # ------------------------------------------------------------------ append -------------
    def handle(self, event: Event) -> AuditEvent | None:
        """Chain and persist ``event``; ``None`` for a non-audited event (``task.output``).

        Any :class:`~agentic_local_app.domain.errors.PersistenceError` propagates; the chain state
        is only advanced once the store has accepted the event.
        """
        if not event.audited:
            return None
        previous = self._last_of(event.session_id)
        sequence = previous.sequence + 1 if previous is not None else 1
        previous_hash = previous.event_hash if previous is not None else GENESIS_HASH
        event_id = self._ids.event_id()
        hashed = _hash_input(
            event_id=event_id,
            sequence=sequence,
            previous_event_hash=previous_hash,
            session_id=event.session_id,
            conversation_id=event.conversation_id,
            cycle_id=event.cycle_id,
            plan_id=event.plan_id,
            task_id=event.task_id,
            event_type=event.event_type.value,
            timestamp=event.timestamp,
            payload=event.payload,
        )
        audited = AuditEvent(
            event_id=event_id,
            sequence=sequence,
            previous_event_hash=previous_hash,
            event_hash=chain_hash(previous_hash, hashed),
            session_id=event.session_id,
            conversation_id=event.conversation_id,
            cycle_id=event.cycle_id,
            plan_id=event.plan_id,
            task_id=event.task_id,
            event_type=event.event_type.value,
            timestamp=event.timestamp,
            payload=event.payload,
        )
        self._store.append_audit_event(audited)
        self._last[event.session_id] = audited
        return audited

    def last(self, session_id: str) -> AuditEvent | None:
        """The last chained event of ``session_id`` (from the store when not cached), or ``None``."""
        return self._last_of(session_id)

    def _last_of(self, session_id: str) -> AuditEvent | None:
        if session_id not in self._last:
            self._last[session_id] = self._store.get_last_audit_event(session_id)
        return self._last[session_id]

    # ------------------------------------------------------------------ verify --------------
    @staticmethod
    def recompute_hash(event: AuditEvent) -> str:
        """``chain_hash(event.previous_event_hash, audit_hash_input(event))`` — must equal ``event_hash``."""
        return chain_hash(event.previous_event_hash, audit_hash_input(event))

    def verify(self, session_id: str, *, page_size: int = 1000) -> AuditVerification:
        """Re-read the whole chain of ``session_id`` by pages and recompute every link.

        For each event, in this order: the sequence must be the previous one plus one
        (``SEQUENCE_GAP``), ``previous_event_hash`` must equal the previous event's hash — or
        ``GENESIS_HASH`` for the first (``PREVIOUS_HASH_MISMATCH``), and the recomputed hash must
        equal ``event_hash`` (``HASH_MISMATCH``). The first failure stops the walk.
        """
        expected_sequence = 1
        previous_hash = GENESIS_HASH
        checked = 0
        after_sequence: int | None = None
        while True:
            page = self._store.list_audit_events(
                session_id, after_sequence=after_sequence, limit=page_size
            )
            for event in page:
                reason = self._check_link(event, expected_sequence, previous_hash)
                if reason is not None:
                    return AuditVerification(
                        valid=False,
                        checked=checked,
                        first_broken_sequence=event.sequence,
                        reason=reason,
                        verified_at=self._clock.now(),
                    )
                checked += 1
                expected_sequence += 1
                previous_hash = event.event_hash
            if len(page) < page_size:
                break
            after_sequence = page[-1].sequence
        return AuditVerification(
            valid=True,
            checked=checked,
            first_broken_sequence=None,
            reason=None,
            verified_at=self._clock.now(),
        )

    @classmethod
    def _check_link(
        cls, event: AuditEvent, expected_sequence: int, previous_hash: str
    ) -> str | None:
        if event.sequence != expected_sequence:
            return REASON_SEQUENCE_GAP
        if event.previous_event_hash != previous_hash:
            return REASON_PREVIOUS_HASH_MISMATCH
        if cls.recompute_hash(event) != event.event_hash:
            return REASON_HASH_MISMATCH
        return None
