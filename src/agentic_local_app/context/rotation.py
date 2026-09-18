"""``RotationCoordinator`` — the rotation sequence of ADR-014 (spec §2.6, §10 ; ADR-005, ADR-007,
ADR-012, ADR-013, ADR-015, ADR-019 §5).

A rotation always happens with an outbound message ``M`` pending (ADR-014): the caller hands it
over as a :class:`PendingOutbound` — its type, the identifier of the original ``M`` and a ``build``
closure that rebuilds the **same content** for the child conversation with a new ``message_id``.

``rotate`` performs, in this order (every state change persisted before the event that reports
it, ADR-015; every remote call awaited after a consistent persisted state; the session and the
source conversation are re-read from the store first, the caller's copies may be stale):

1. guards — ``max_rotations_per_session`` (``RotationFailedError / ROTATION_LIMIT_REACHED``),
   ``max_cycles`` (``BudgetExceededError``, a rotation costs one cycle, ADR-012 / ADR-019 §5) and
   the transition table (``InvalidTransitionError`` unless ``source.status -> ROTATING`` is listed);
   nothing changes when a guard fails;
2. parent window forced ``SATURATED`` if it is not yet, parent ``-> ROTATING``, ``rotation.started``;
3. summary composed by the ``ContextReducer`` (pure); failure → parent ``-> FAILED``,
   ``rotation.failed``, exception re-raised;
4. remote ``init`` → child ``NEW`` (``parent_conversation_id``, window ``SATURATED``) →
   ``ContextSummaryRecord`` → remote id and instructions bytes → child ``ACTIVE`` → in one store
   transaction: ``rotations_count + 1``, ``consumed_cycles + 1``, ``CycleRecord`` (``resume``,
   ``RUNNING``), ``MessageRecord`` of the ``context_resume_request`` → ``cycle.started``;
5. child ``-> WAITING_MODEL_RESPONSE`` → POST → post confirmed, ``context_bytes`` → ``message.outbound``;
6. GET (``wait_for_reply``) → ack validated by the ``ProtocolAdapter`` (only ``context_resume_ack``
   of the right ``original_conversation_id`` with ``acknowledged = true``) → ``MessageRecord``,
   ``context_bytes``, cursor → ``message.inbound`` (a rejected reply is counted, the cursor moves
   past it, ``protocol_error_count + 1``, ``message.rejected``, ``ProtocolError`` re-raised);
7. child window ``SATURATED -> HEALTHY``, parent ``ROTATING -> CLOSED`` (``rotated``), cycle
   ``COMPLETED`` + ``cycle.ended``, ``rotation.completed``;
8. retransmission: ``M'`` built by ``pending.build`` → ``MessageRecord`` (``retransmission_of``,
   the cycle of ``M``) → POST → confirmed, ``context_bytes`` → ``message.retransmitted`` (the
   outbound event of ``M'``; ``message.outbound`` is **not** published as well — phase 10 counts
   both event types as outbound, a second event would count the POST twice); the child stays
   ``WAITING_MODEL_RESPONSE`` for the reply to ``M'``;
9. best-effort close of the remote parent, last because it is outside the critical path: a plain
   transport failure is ignored, an ``INTERRUPTED`` error (``abandon()``) propagates.

State left at each failure point (the orchestrator applies the failure policy, phase 9):

| failure | parent | child | notes |
|---|---|---|---|
| guard | unchanged | none | ``rotation.failed`` only for the rotation limit |
| summary over budget | ``FAILED`` | none | ``rotation.failed`` |
| ``init`` error | ``ROTATING`` | none | no summary record, counters unchanged |
| summary / cycle write error | ``ROTATING`` | ``NEW`` / ``ACTIVE`` | the failed group is rolled back |
| POST of the request error | ``ROTATING`` | ``WAITING_MODEL_RESPONSE``, request unconfirmed | counters and cycle already written |
| GET error / timeout | ``ROTATING`` | ``WAITING_MODEL_RESPONSE`` | cursor untouched, cycle ``RUNNING`` |
| ack rejected | ``ROTATING`` | ``WAITING_MODEL_RESPONSE`` | cursor moved, ``protocol_error_count + 1`` |
| POST of ``M'`` error | ``CLOSED`` | ``WAITING_MODEL_RESPONSE``, ``M'`` unconfirmed | rotation already ``completed`` |
| close of the remote parent interrupted | ``CLOSED`` | ``WAITING_MODEL_RESPONSE``, ``M'`` confirmed | the rotation is complete |

``session.current_conversation_id`` points to the child as soon as it exists and
``child.current_cycle_id`` to the resume cycle until the retransmission, so the orchestrator can
find both without the :class:`RotationResult`. A cancellation (``asyncio.CancelledError``, or the
transport's ``abandon()`` surfacing as ``TransportError(INTERRUPTED, ABANDONED)``) propagates
unchanged: it can only interrupt an awaited transport call, and the persisted state is then one
of the rows above.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from agentic_local_app.config import AppConfig
from agentic_local_app.context.reducer import ContextReducer, SummaryDraft, known_remote_id
from agentic_local_app.context.window import ContextWindowMonitor
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import (
    BudgetExceededError,
    ErrorType,
    InvalidTransitionError,
    ProtocolError,
    RotationFailedError,
    TransportError,
)
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ContextSummaryRecord,
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    SessionRecord,
)
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
    MessageDirection,
    MessageType,
)
from agentic_local_app.domain.transitions import CONVERSATION_TRANSITIONS, can_transition
from agentic_local_app.lifecycle.conversation_lifecycle import (
    CONVERSATION_ENTITY,
    ConversationLifecycleManager,
)
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.adapter import (
    InboundMessage,
    OutboundMessage,
    ProtocolAdapter,
    peek_field,
    rejected_payload,
)
from agentic_local_app.transport.gateway import GetResult, TransportGateway

__all__ = [
    "REASON_RESUME_ACKNOWLEDGED",
    "REASON_ROTATED",
    "REASON_ROTATION_FAILED",
    "REASON_SATURATED",
    "RETRANSMITTABLE_MESSAGE_TYPES",
    "ROTATION_LIMIT_REACHED",
    "PendingOutbound",
    "RotationCoordinator",
    "RotationResult",
]

#: ``RotationFailedError.error_code`` when ``max_rotations_per_session`` is reached (ADR-013 §5).
ROTATION_LIMIT_REACHED = "ROTATION_LIMIT_REACHED"

#: The message types a rotation can be pending on (ADR-014): never a resume request itself.
RETRANSMITTABLE_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.USER_REQUEST, MessageType.EXECUTION_RESULT}
)

#: ``reason`` values carried by the state-change events of a rotation.
REASON_SATURATED = "context_saturated"
REASON_ROTATION_REQUESTED = "rotation_requested"
REASON_ROTATION_FAILED = "rotation_failed"
REASON_RESUME_ACKNOWLEDGED = "resume_acknowledged"
REASON_ROTATED = "rotated"
REASON_ROTATION = "rotation"

#: ``ConversationRecord.last_model_response_state`` values written here (08 §4 proposal).
_AWAITING = "awaiting"
_RECEIVED_VALID = "received_valid"
_RECEIVED_INVALID = "received_invalid"

_ONE_MS = timedelta(milliseconds=1)
_ACK_ONLY: frozenset[MessageType] = frozenset({MessageType.CONTEXT_RESUME_ACK})


@dataclass(frozen=True)
class PendingOutbound:
    """The message ``M`` the rotation is pending on (ADR-014).

    ``build(child, message_id)`` must return the same content as ``M`` for the child conversation
    (the caller keeps the content in the closure); ``cycle_id`` is the cycle ``M`` belongs to, which
    the retransmission continues (ADR-019 §5) — ``None`` when ``M`` opened no cycle yet.
    """

    message_type: MessageType
    original_message_id: str
    build: Callable[[ConversationRecord, str], OutboundMessage]
    cycle_id: str | None = None

    def __post_init__(self) -> None:
        if self.message_type not in RETRANSMITTABLE_MESSAGE_TYPES:
            raise ValueError(
                f"{self.message_type.value} cannot be pending on a rotation; expected one of "
                f"{sorted(t.value for t in RETRANSMITTABLE_MESSAGE_TYPES)}"
            )


@dataclass(frozen=True)
class RotationResult:
    """What a completed rotation leaves: the two conversations, the summary, the resume cycle and
    the identifiers of the ack and of the retransmitted copy of ``M``."""

    child: ConversationRecord
    source: ConversationRecord
    summary: ContextSummaryRecord
    resume_cycle: CycleRecord
    retransmitted_message_id: str
    ack_message_id: str


class RotationCoordinator:
    """Run the rotation of ADR-014 end to end with the injected collaborators."""

    def __init__(
        self,
        config: AppConfig,
        store: ConversationStore,
        bus: EventBus,
        clock: Clock,
        ids: IdGenerator,
        lifecycle: ConversationLifecycleManager,
        adapter: ProtocolAdapter,
        transport: TransportGateway,
        reducer: ContextReducer,
        monitor: ContextWindowMonitor,
        instructions: str,
    ) -> None:
        self._config = config
        self._store = store
        self._bus = bus
        self._clock = clock
        self._ids = ids
        self._lifecycle = lifecycle
        self._adapter = adapter
        self._transport = transport
        self._reducer = reducer
        self._monitor = monitor
        self._instructions = instructions

    # ------------------------------------------------------------------ public -------------
    async def rotate(
        self, session: SessionRecord, source: ConversationRecord, pending: PendingOutbound
    ) -> RotationResult:
        """Rotate ``source`` into a child conversation and retransmit ``pending`` there."""
        # the store is the truth: the caller's records may be behind it
        session = self._require_session(session.session_id)
        source = self._require_conversation(source.conversation_id)
        session_id = session.session_id
        self._check_guards(session, source)

        # 2. parent: SATURATED (if needed) -> ROTATING, rotation.started
        source = self._mark_rotating(source)
        self._publish(
            EventType.ROTATION_STARTED,
            session_id=session_id,
            conversation_id=source.conversation_id,
            payload={
                "source_conversation_id": source.conversation_id,
                "context_bytes": source.context_bytes,
                "pending_message_type": pending.message_type.value,
                "rotations_count": session.rotations_count,
            },
        )

        # 3. the summary, before anything is created (ADR-014 step 1)
        draft = self._compose_summary(session, source, pending)

        # 4. remote init, child, summary record, activation, counters and resume cycle
        parent_remote = known_remote_id(source)
        remote = await self._transport.init_conversation(
            self._instructions,
            {
                "session_id": session_id,
                "parent_conversation_id": source.conversation_id,
                "rotation_index": session.rotations_count + 1,
            },
        )
        child = self._lifecycle.create_conversation(
            session_id,
            parent_conversation_id=source.conversation_id,
            context_window_state=ContextWindowState.SATURATED,
        )
        summary = self._reducer.persist(
            draft,
            session_id=session_id,
            source_conversation_id=source.conversation_id,
            target_conversation_id=child.conversation_id,
        )
        child = self._lifecycle.update_conversation(
            child.conversation_id,
            remote_conversation_id=remote,
            context_bytes=self._monitor.instructions_bytes(self._instructions),
        )
        child = self._lifecycle.transition_conversation(
            child.conversation_id, ConversationState.ACTIVE, reason=REASON_ROTATION
        )
        request = self._adapter.build_context_resume_request(
            child,
            self._ids.message_id(),
            original_conversation_id=parent_remote,
            goal=session.goal,
            context_summary=summary.summary_payload,
            pending_message_type=pending.message_type,
        )
        cycle, request_record, child = self._open_resume_cycle(child, request)

        # 5. POST the context_resume_request
        child = self._lifecycle.transition_conversation(
            child.conversation_id,
            ConversationState.WAITING_MODEL_RESPONSE,
            reason=request.message_type.value,
        )
        child = await self._post(child, request, request_record)

        # 6. GET until the ack
        reply = await self._transport.wait_for_reply(remote, after=None)
        ack, child = self._accept_ack(reply, child, cycle, parent_remote)

        # 7. close the rotation: child HEALTHY, parent CLOSED, cycle COMPLETED
        child = self._lifecycle.transition_context_window(
            child.conversation_id, ContextWindowState.HEALTHY, reason=REASON_RESUME_ACKNOWLEDGED
        )
        source = self._lifecycle.transition_conversation(
            source.conversation_id,
            ConversationState.CLOSED,
            reason=REASON_ROTATED,
            closure_reason=REASON_ROTATED,
        )
        cycle = self._complete_resume_cycle(cycle, ack)
        self._publish(
            EventType.ROTATION_COMPLETED,
            session_id=session_id,
            conversation_id=child.conversation_id,
            payload={
                "source_conversation_id": source.conversation_id,
                "target_conversation_id": child.conversation_id,
                "remote_conversation_id": remote,
                "summary_id": summary.summary_id,
                "summary_size_bytes": summary.summary_size_bytes,
                "reduction_step": summary.reduction_step,
            },
        )

        # 8. retransmit M in the child, with a new message_id (ADR-014 step 4)
        retransmitted = pending.build(child, self._ids.message_id())
        if retransmitted.message_type is not pending.message_type:
            raise ValueError(
                f"pending.build returned a {retransmitted.message_type.value}, expected a "
                f"{pending.message_type.value}"
            )
        retransmitted_record = self._outbound_record(
            child,
            retransmitted,
            cycle_id=pending.cycle_id,
            now=self._clock.now(),
            retransmission_of=pending.original_message_id,
        )
        with self._store.transaction():
            self._store.save_message(retransmitted_record)
            child = self._lifecycle.update_conversation(
                child.conversation_id,
                current_cycle_id=pending.cycle_id,
                last_outbound_message_id=retransmitted_record.message_id,
            )
        child = await self._post(child, retransmitted, retransmitted_record)

        # 9. best effort, outside the critical path
        await self._close_remote_parent(source)
        return RotationResult(
            child=child,
            source=source,
            summary=summary,
            resume_cycle=cycle,
            retransmitted_message_id=retransmitted.envelope.message_id,
            ack_message_id=ack.envelope.message_id,
        )

    # ------------------------------------------------------------------ steps --------------
    def _check_guards(self, session: SessionRecord, source: ConversationRecord) -> None:
        """Step 1: refuse before any change when the rotation cannot legally happen."""
        limit = self._config.context.max_rotations_per_session
        if session.rotations_count >= limit:
            details = {
                "rotations_count": session.rotations_count,
                "max_rotations_per_session": limit,
            }
            self._publish(
                EventType.ROTATION_FAILED,
                session_id=session.session_id,
                conversation_id=source.conversation_id,
                payload={
                    "source_conversation_id": source.conversation_id,
                    "error_code": ROTATION_LIMIT_REACHED,
                    **details,
                },
            )
            raise RotationFailedError(ROTATION_LIMIT_REACHED, **details)
        if session.consumed_cycles >= session.budget.max_cycles:
            raise BudgetExceededError(
                "max_cycles", session.budget.max_cycles, session.consumed_cycles
            )
        if not can_transition(CONVERSATION_TRANSITIONS, source.status, ConversationState.ROTATING):
            raise InvalidTransitionError(
                entity=CONVERSATION_ENTITY,
                current=source.status.value,
                target=ConversationState.ROTATING.value,
            )

    def _mark_rotating(self, source: ConversationRecord) -> ConversationRecord:
        """Step 2: a rotating conversation is saturated by definition, then ``ROTATING``."""
        if source.context_window_state is not ContextWindowState.SATURATED:
            source = self._lifecycle.transition_context_window(
                source.conversation_id,
                ContextWindowState.SATURATED,
                reason=REASON_ROTATION_REQUESTED,
            )
        return self._lifecycle.transition_conversation(
            source.conversation_id, ConversationState.ROTATING, reason=REASON_SATURATED
        )

    def _compose_summary(
        self, session: SessionRecord, source: ConversationRecord, pending: PendingOutbound
    ) -> SummaryDraft:
        """Step 3: the summary within budget, or the explicit failure of §2.6."""
        try:
            return self._reducer.compose(session, source, pending_message_type=pending.message_type)
        except RotationFailedError as exc:
            failed = self._lifecycle.transition_conversation(
                source.conversation_id, ConversationState.FAILED, reason=REASON_ROTATION_FAILED
            )
            details = exc.error.details
            self._publish(
                EventType.ROTATION_FAILED,
                session_id=session.session_id,
                conversation_id=failed.conversation_id,
                payload={
                    "source_conversation_id": failed.conversation_id,
                    "error_code": exc.error.error_code,
                    "summary_size_bytes": details.get("size_bytes"),
                    "summary_budget_bytes": details.get("budget_bytes"),
                    "reduction_step": details.get("step"),
                },
            )
            raise

    def _open_resume_cycle(
        self, child: ConversationRecord, request: OutboundMessage
    ) -> tuple[CycleRecord, MessageRecord, ConversationRecord]:
        """Step 4 (end): counters, ``CycleRecord`` and the request's ``MessageRecord`` in one write,
        then ``cycle.started`` (the resume cycle starts when its outbound message is persisted)."""
        session_id = child.session_id
        now = self._clock.now()
        cycle = CycleRecord(
            cycle_id=self._ids.cycle_id(),
            conversation_id=child.conversation_id,
            session_id=session_id,
            cycle_type=CycleType.RESUME,
            status=CycleState.RUNNING,
            outbound_message_id=request.envelope.message_id,
            started_at=now,
        )
        record = self._outbound_record(child, request, cycle_id=cycle.cycle_id, now=now)
        with self._store.transaction():
            current = self._require_session(session_id)
            session = self._lifecycle.update_session(
                session_id,
                rotations_count=current.rotations_count + 1,
                consumed_cycles=current.consumed_cycles + 1,
            )
            self._store.save_cycle(cycle)
            self._store.save_message(record)
            child = self._lifecycle.update_conversation(
                child.conversation_id,
                current_cycle_id=cycle.cycle_id,
                last_outbound_message_id=record.message_id,
            )
        self._publish(
            EventType.CYCLE_STARTED,
            session_id=session_id,
            conversation_id=child.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "cycle_type": cycle.cycle_type.value,
                "outbound_message_type": request.message_type.value,
                "consumed_cycles": session.consumed_cycles,
            },
        )
        return cycle, record, child

    async def _post(
        self, child: ConversationRecord, message: OutboundMessage, record: MessageRecord
    ) -> ConversationRecord:
        """Steps 5 and 8: POST an already persisted outbound message → confirm it and count its
        bytes → ``message.outbound`` (request) or ``message.retransmitted`` (``M'``)."""
        ack = await self._transport.post_message(known_remote_id(child), message.payload)
        posted_at = self._clock.now()
        with self._store.transaction():
            self._store.save_message(
                record.model_copy(update={"post_confirmed": True, "posted_at": posted_at})
            )
            child = self._lifecycle.update_conversation(
                child.conversation_id,
                context_bytes=self._monitor.account(child.context_bytes, message.size_bytes),
                last_model_response_state=_AWAITING,
            )
        payload: dict[str, Any] = {
            "message_type": message.message_type.value,
            "message_id": record.message_id,
            "post_status": ack.http_status,
            "size_bytes": message.size_bytes,
        }
        event_type = EventType.MESSAGE_OUTBOUND
        if record.retransmission_of is not None:
            event_type = EventType.MESSAGE_RETRANSMITTED
            payload.update(
                {
                    "retransmission_of": record.retransmission_of,
                    "original_message_id": record.retransmission_of,
                    "new_message_id": record.message_id,
                    "reason": REASON_ROTATION,
                }
            )
        self._publish(
            event_type,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            cycle_id=record.cycle_id,
            payload=payload,
        )
        return child

    def _accept_ack(
        self,
        reply: GetResult,
        child: ConversationRecord,
        cycle: CycleRecord,
        parent_remote: str,
    ) -> tuple[InboundMessage, ConversationRecord]:
        """Step 6: validate the reply as the ack of ``parent_remote``; persist and publish either way."""
        known_message_ids, known_plan_ids, known_task_ids = self._known_ids(child.session_id)
        received_bytes = sum(size_bytes(message) for message in reply.messages)
        try:
            ack = self._adapter.parse_inbound(
                reply.messages,
                expected=_ACK_ONLY,
                conversation=child,
                known_message_ids=known_message_ids,
                known_plan_ids=known_plan_ids,
                known_task_ids=known_task_ids,
                stored_output_task_ids=known_task_ids,
                expected_original_conversation_id=parent_remote,
            )
        except ProtocolError as exc:
            self._reject_ack(reply, child, cycle, exc, received_bytes)
            raise
        now = self._clock.now()
        record = MessageRecord(
            message_id=ack.envelope.message_id,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            direction=MessageDirection.INBOUND,
            message_type=ack.message_type,
            payload=ack.payload,
            size_bytes=ack.size_bytes,
            cycle_id=cycle.cycle_id,
            received_at=now,
            validation_status="valid",
            created_at=now,
        )
        with self._store.transaction():
            self._store.save_message(record)
            child = self._lifecycle.update_conversation(
                child.conversation_id,
                context_bytes=self._monitor.account(child.context_bytes, ack.size_bytes),
                get_cursor=reply.cursor,
                last_inbound_message_id=record.message_id,
                last_model_response_state=_RECEIVED_VALID,
            )
        self._publish(
            EventType.MESSAGE_INBOUND,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "message_type": ack.message_type.value,
                "message_id": record.message_id,
                "get_status": reply.http_status,
                "validation_status": "valid",
                "size_bytes": ack.size_bytes,
            },
        )
        return ack, child

    def _reject_ack(
        self,
        reply: GetResult,
        child: ConversationRecord,
        cycle: CycleRecord,
        exc: ProtocolError,
        received_bytes: int,
    ) -> None:
        """A reply that is not the expected ack: persisted, counted, published, and the resume
        cycle closed as ``FAILED``.

        The trail is the one of every other protocol rejection (orchestrator ``_persist_rejected``):
        the faulty reply is stored with ``validation_status = "invalid"`` — it is the only trace of
        what the model answered, and a correction policy will quote it — and the cycle opened for
        the resume never stays ``RUNNING`` behind a session that stops here.
        """
        first = reply.messages[0] if reply.messages else None
        raw_type = peek_field(first, "type")
        raw_id = peek_field(first, "message_id")
        type_str = raw_type if isinstance(raw_type, str) else None
        id_str = raw_id if isinstance(raw_id, str) and raw_id else None
        try:
            message_type = (
                MessageType(type_str) if type_str is not None else MessageType.SYSTEM_ERROR
            )
        except ValueError:
            message_type = MessageType.SYSTEM_ERROR
        message_id = (
            id_str if id_str is not None and self._store.get_message(id_str) is None else None
        )
        if message_id is None:
            message_id = self._ids.message_id()
        now = self._clock.now()
        record = MessageRecord(
            message_id=message_id,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            direction=MessageDirection.INBOUND,
            message_type=message_type,
            payload=rejected_payload(reply.messages),
            size_bytes=received_bytes,
            cycle_id=cycle.cycle_id,
            received_at=now,
            validation_status="invalid",
            created_at=now,
        )
        failed = cycle.model_copy(
            update={
                "status": CycleState.FAILED,
                "ended_at": now,
                "inbound_message_id": record.message_id,
            }
        )
        with self._store.transaction():
            self._store.save_message(record)
            self._store.save_cycle(failed)
            self._lifecycle.update_conversation(
                child.conversation_id,
                context_bytes=self._monitor.account(child.context_bytes, received_bytes),
                protocol_error_count=child.protocol_error_count + 1,
                get_cursor=reply.cursor,
                last_inbound_message_id=record.message_id,
                last_model_response_state=_RECEIVED_INVALID,
            )
        self._publish(
            EventType.MESSAGE_REJECTED,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "message_type": type_str,
                "message_id": id_str,
                "get_status": reply.http_status,
                "validation_status": "invalid",
                "error_code": exc.error.error_code,
                "size_bytes": received_bytes,
            },
        )
        self._publish(
            EventType.CYCLE_ENDED,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "status": failed.status.value,
                "duration_ms": max(0, (now - cycle.started_at) // _ONE_MS),
                "retry_count": failed.retry_count,
                "error_code": exc.error.error_code,
            },
        )

    def _complete_resume_cycle(self, cycle: CycleRecord, ack: InboundMessage) -> CycleRecord:
        """Step 7: the resume cycle ends when the ack is processed (ADR-007)."""
        ended_at = self._clock.now()
        completed = cycle.model_copy(
            update={
                "status": CycleState.COMPLETED,
                "ended_at": ended_at,
                "inbound_message_id": ack.envelope.message_id,
            }
        )
        self._store.save_cycle(completed)
        self._publish(
            EventType.CYCLE_ENDED,
            session_id=cycle.session_id,
            conversation_id=cycle.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "status": completed.status.value,
                "duration_ms": max(0, (ended_at - cycle.started_at) // _ONE_MS),
                "retry_count": completed.retry_count,
                "inbound_message_type": ack.message_type.value,
            },
        )
        return completed

    async def _close_remote_parent(self, source: ConversationRecord) -> None:
        """Step 9, best effort and without retry (ADR-006): a transport failure is ignored, an
        interruption (``abandon()`` → ``INTERRUPTED``) and a cancellation are not."""
        if source.remote_conversation_id is None:
            return
        try:
            await self._transport.close_conversation(source.remote_conversation_id)
        except TransportError as exc:
            if exc.error_type is ErrorType.INTERRUPTED:
                raise

    # ------------------------------------------------------------------ helpers ------------
    def _outbound_record(
        self,
        child: ConversationRecord,
        message: OutboundMessage,
        *,
        cycle_id: str | None,
        now: datetime,
        retransmission_of: str | None = None,
    ) -> MessageRecord:
        return MessageRecord(
            message_id=message.envelope.message_id,
            session_id=child.session_id,
            conversation_id=child.conversation_id,
            direction=MessageDirection.OUTBOUND,
            message_type=message.message_type,
            payload=message.payload,
            size_bytes=message.size_bytes,
            cycle_id=cycle_id,
            retransmission_of=retransmission_of,
            created_at=now,
        )

    def _known_ids(self, session_id: str) -> tuple[set[str], set[str], set[str]]:
        """Every message, plan and task identifier of the session (ADR-007 uniqueness scopes)."""
        message_ids = {
            message.message_id
            for conversation in self._store.list_conversations(session_id)
            for message in self._store.list_messages(conversation.conversation_id)
        }
        plan_ids = {plan.plan_id for plan in self._store.list_plans(session_id)}
        task_ids = {task.task_id for task in self._store.list_tasks(session_id)}
        return message_ids, plan_ids, task_ids

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
        *,
        session_id: str,
        conversation_id: str | None,
        payload: dict[str, Any],
        cycle_id: str | None = None,
    ) -> None:
        """Publish after the store writes (ADR-015); the timestamp is the injected clock's."""
        self._bus.publish(
            Event(
                event_type=event_type,
                timestamp=self._clock.now(),
                session_id=session_id,
                conversation_id=conversation_id,
                cycle_id=cycle_id,
                payload=payload,
            )
        )
