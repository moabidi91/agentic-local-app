"""``ProtocolOrchestrator`` — the protocol loop (spec §2.2, §3.2, §7, §8.4, §10, §11, §14, §15 ;
ADR-004, ADR-006, ADR-007, ADR-010, ADR-012, ADR-013, ADR-014, ADR-015, ADR-017, ADR-019,
ADR-022).

One ``run`` drives one session from an entry point to a terminal state of the loop, with every
state change owned by its component (``ConversationLifecycleManager`` for sessions and
conversations, ``PlanRunner`` for plans and tasks, ``RotationCoordinator`` for a rotation, the
``FailureManager`` for failures and retry decisions) and every record persisted **before** the
event that reports it (ADR-015). Three entry points:

- :meth:`ProtocolOrchestrator.run_session` — a conversation ``ACTIVE`` (or ``NEW``) without remote
  identifier: remote ``init``, then the initial ``user_request`` (``discovery`` cycle);
- :meth:`ProtocolOrchestrator.continue_session` — a reusable conversation (``WAITING_USER``, §11):
  a follow-up ``user_request`` (``execution`` cycle, follow-up row of the ADR-007 table);
- :meth:`ProtocolOrchestrator.resume_session` — after a restart (ADR-016): a conversation
  ``WAITING_MODEL_RESPONSE`` whose last outbound message is persisted; the POST is replayed with the
  same ``message_id`` when it was not confirmed, then the loop **receives first**.

The loop itself (§14 amended by the ADRs — see ``docs/phases/phase-09-orchestration.md``):

1. **send** ``M``: ``max_cycles`` checked before a cycle opens (ADR-012); the ``CycleRecord`` and the
   outbound ``MessageRecord`` are persisted together with the counters (``cycle.started``,
   ``budget.updated``); the window is evaluated with the projected size of ``M`` (ADR-013) and, when
   ``SATURATED``, the rotation of ADR-014 runs with ``M`` pending — its cycle continues in the
   child (ADR-019 §5) and ``M`` is retransmitted there; otherwise the conversation goes
   ``WAITING_MODEL_RESPONSE`` and ``M`` is POSTed with the retry policy of §7;
2. **receive**: GET (polling in the gateway) with the retry policy; ``MODEL_CONTEXT_WINDOW_ERROR``
   rotates; an exhausted ``MODEL_GET_TIMEOUT`` or a protocol error rotates once when the window is
   ``WARNING`` (ADR-019 §2) and fails otherwise; a valid message is persisted (``message.inbound``),
   a rejected one too (``message.rejected``, ``protocol_error_count``);
3. **process**: a plan is persisted with its tasks (``plan.received``), ``max_plans`` and the
   duration are checked before it starts (plan ``PENDING → FAILED`` on excess), the ``PlanRunner``
   executes it, the ``execution_result`` is fitted under ``max_message_bytes`` (ADR-010) and becomes
   the next ``M``; a ``final_answer`` completes the conversation and the session (§11), and so
   does a ``user_response`` (ADR-022) — the model's direct answer to the user — except that it is
   never written to ``SessionRecord.final_answer`` and that a question (``expects_reply``) keeps
   the conversation ``WAITING_USER`` even under ``auto_close_on_final_answer``.

Interruption (§2.9, ADR-006): the token is checked before every step and every transport call is
abandoned by the ``InterruptionHandler``; the loop then stops **silently** — nothing is sent, no
state is written, the handler owns the cleanup — and signals its end through the registered loop
event. Every remote call is preceded by ``breaker.allow()`` (ADR-019 §6). A remote call is never
retried by anything else than the ``FailureManager`` decision, and a retried POST / GET reuses the
same ``message_id`` / cursor (ADR-004). Time comes from the injected ``Clock`` and waiting from the
injected ``sleep`` (ADR-017).
"""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, TypeVar

from agentic_local_app.config import AppConfig
from agentic_local_app.context.rotation import PendingOutbound, RotationCoordinator
from agentic_local_app.context.window import ContextWindowMonitor
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import (
    AppError,
    BudgetExceededError,
    ErrorType,
    NormalizedError,
    ProtocolError,
    RotationFailedError,
    SessionInterruptedError,
    TransportError,
)
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    PlanRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    CONCLUDING_MESSAGE_TYPES,
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
    MessageDirection,
    MessageType,
    PlanState,
    SessionState,
    TaskState,
    cycle_type_for_plan,
)
from agentic_local_app.domain.transitions import (
    CONVERSATION_TRANSITIONS,
    CYCLE_TRANSITIONS,
    PLAN_TRANSITIONS,
    SESSION_TRANSITIONS,
    TASK_TRANSITIONS,
    assert_transition,
    can_transition,
)
from agentic_local_app.execution.payload_guard import PayloadGuard
from agentic_local_app.execution.plan_runner import (
    BUDGET_EXCEEDED_REASON,
    PLAN_ENTITY,
    TASK_ENTITY,
    PlanRunner,
)
from agentic_local_app.interruption.handler import CYCLE_ENTITY, InterruptionHandler
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.adapter import InboundMessage, OutboundMessage, ProtocolAdapter
from agentic_local_app.protocol.messages import (
    ExecutionResultContent,
    UserRequestContent,
    UserResponseContent,
)
from agentic_local_app.resilience.circuit_breaker import CircuitBreaker
from agentic_local_app.resilience.failure_manager import FailureManager
from agentic_local_app.transport.gateway import (
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    TransportGateway,
)

__all__ = [
    "CIRCUIT_OPEN_CODE",
    "REASON_AUTO_CLOSE",
    "REASON_BUDGET_EXCEEDED",
    "REASON_FAILURE",
    "REASON_FINAL_ANSWER",
    "REASON_PLAN_RECEIVED",
    "REASON_REUSABLE",
    "REASON_ROTATION_FAILED",
    "REASON_USER_REQUEST",
    "REASON_USER_RESPONSE",
    "ROTATION_NOT_ALLOWED_CODE",
    "STAGE_BEFORE_CYCLE",
    "STAGE_BEFORE_PLAN",
    "STAGE_BETWEEN_TASKS",
    "WINDOW_REASON_CONTEXT_ERROR",
    "WINDOW_REASON_PROJECTION",
    "WINDOW_REASON_SATURATION_RATIO",
    "WINDOW_REASON_THRESHOLD",
    "WINDOW_REASON_UNUSABLE_REPLY",
    "ProtocolOrchestrator",
    "SleepFn",
    "pending_outbound_of",
]

log = logging.getLogger(__name__)

SleepFn = Callable[[float], Awaitable[None]]
T = TypeVar("T")

#: ``reason`` values of the session / conversation transitions driven by the loop (08 §2).
REASON_USER_REQUEST = "user_request"
REASON_PLAN_RECEIVED = "plan_received"
REASON_FINAL_ANSWER = "final_answer"
REASON_USER_RESPONSE = "user_response"
REASON_REUSABLE = "reusable"
REASON_AUTO_CLOSE = "auto_close"
REASON_FAILURE = "failure"
REASON_BUDGET_EXCEEDED = "budget_exceeded"
REASON_ROTATION_FAILED = "rotation_failed"

#: ``reason`` values of ``context.window_state_changed`` (06 §1.1).
WINDOW_REASON_THRESHOLD = "threshold"
WINDOW_REASON_PROJECTION = "projection"
WINDOW_REASON_SATURATION_RATIO = "saturation_ratio"
WINDOW_REASON_CONTEXT_ERROR = "context_window_error"
WINDOW_REASON_UNUSABLE_REPLY = "unusable_reply"

#: ``stage`` values of ``budget.exceeded`` (08 §2, ADR-012 §3).
STAGE_BEFORE_CYCLE = "before_cycle"
STAGE_BEFORE_PLAN = "before_plan"
STAGE_BETWEEN_TASKS = "between_tasks"

#: ``error_code`` of the ``NETWORK_ERROR`` raised when the breaker keeps refusing (ADR-019 §6).
CIRCUIT_OPEN_CODE = "CIRCUIT_OPEN"
#: ``error_code`` of the ``ROTATION_FAILED`` raised when a saturated conversation cannot rotate
#: from its state (a first ``user_request`` projected over the budget: 06 points ouverts n°4).
ROTATION_NOT_ALLOWED_CODE = "ROTATION_NOT_ALLOWED"

#: ``ConversationRecord.last_model_response_state`` values written by the loop (08 §4 proposal).
_AWAITING = "awaiting"
_RECEIVED_VALID = "received_valid"
_RECEIVED_INVALID = "received_invalid"

_RESUMABLE_MESSAGE_TYPES: frozenset[MessageType] = frozenset(
    {MessageType.USER_REQUEST, MessageType.EXECUTION_RESULT}
)
_ONE_MS = timedelta(milliseconds=1)


def pending_outbound_of(
    store: ConversationStore, conversation: ConversationRecord
) -> MessageRecord | None:
    """The unanswered ``user_request`` / ``execution_result`` a ``WAITING_MODEL_RESPONSE``
    conversation waits on (its last message), or ``None`` (ADR-016 "POST sent, no GET")."""
    if conversation.status is not ConversationState.WAITING_MODEL_RESPONSE:
        return None
    messages = store.list_messages(conversation.conversation_id)
    if not messages:
        return None
    last = messages[-1]
    if last.direction is not MessageDirection.OUTBOUND:
        return None
    if last.message_type not in _RESUMABLE_MESSAGE_TYPES:
        return None
    return last


# ------------------------------------------------------------------------------------------------
# Internal control flow
# ------------------------------------------------------------------------------------------------
class _Entry(enum.Enum):
    START = "start"
    FOLLOW_UP = "follow_up"
    RESUME = "resume"


class _InterruptedError(Exception):
    """The loop must stop silently: the ``InterruptionHandler`` owns the cleanup."""


class _RotateRequestedError(Exception):
    """The failure policy decided ``rotate`` (``MODEL_CONTEXT_WINDOW_ERROR``)."""

    def __init__(self, error: NormalizedError) -> None:
        super().__init__(error.error_code)
        self.error = error


class _FailedError(Exception):
    """The loop must end in ``FAILED``: the error, the transition reason, whether a
    ``FailureRecord`` was already written, and the budget stage when it is a budget failure."""

    def __init__(
        self,
        error: NormalizedError,
        *,
        reason: str,
        recorded: bool,
        stage: str | None = None,
        plan_id: str | None = None,
    ) -> None:
        super().__init__(f"{error.error_type.value}/{error.error_code}")
        self.error = error
        self.reason = reason
        self.recorded = recorded
        self.stage = stage
        self.plan_id = plan_id


@dataclass
class _Pending:
    """The outbound message ``M`` the loop is about to send, or waits a reply to (ADR-014)."""

    message_type: MessageType
    cycle_type: CycleType
    follow_up: bool = False
    user_message: str | None = None
    content: ExecutionResultContent | None = None
    fitted: bool = False


# ------------------------------------------------------------------------------------------------
# The orchestrator
# ------------------------------------------------------------------------------------------------
class ProtocolOrchestrator:
    """Drive the protocol loop of one session (§3.2). Stateless between runs."""

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
        plan_runner: PlanRunner,
        payload_guard: PayloadGuard,
        failure_manager: FailureManager,
        breaker: CircuitBreaker,
        monitor: ContextWindowMonitor,
        rotation: RotationCoordinator,
        interruption: InterruptionHandler,
        instructions: str,
        *,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        self.config = config
        self.store = store
        self.bus = bus
        self.clock = clock
        self.ids = ids
        self.lifecycle = lifecycle
        self.adapter = adapter
        self.transport = transport
        self.plan_runner = plan_runner
        self.payload_guard = payload_guard
        self.failure_manager = failure_manager
        self.breaker = breaker
        self.monitor = monitor
        self.rotation = rotation
        self.interruption = interruption
        self.instructions = instructions
        self.sleep = sleep

    async def run_session(
        self, session_id: str, *, user_message: str | None = None
    ) -> SessionRecord:
        """Run a ``RUNNING`` session whose current conversation is ``ACTIVE`` (or ``NEW``): remote
        init, initial ``user_request``, then the loop. ``user_message`` defaults to the session's."""
        return await _SessionRun(self, session_id, _Entry.START, user_message).run()

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord:
        """§11: a follow-up ``user_request`` in the reusable conversation of a ``RUNNING`` session."""
        return await _SessionRun(self, session_id, _Entry.FOLLOW_UP, user_message).run()

    async def resume_session(self, session_id: str) -> SessionRecord:
        """ADR-016: replay the unconfirmed POST if any, then GET first, then the loop."""
        return await _SessionRun(self, session_id, _Entry.RESUME, None).run()


class _SessionRun:
    """The mutable state of one run: current records and the pending message."""

    def __init__(
        self,
        orchestrator: ProtocolOrchestrator,
        session_id: str,
        entry: _Entry,
        user_message: str | None,
    ) -> None:
        self._o = orchestrator
        self._sid = session_id
        self._entry = entry
        self._user_message = user_message
        self._conv: ConversationRecord | None = None
        self._cycle: CycleRecord | None = None
        self._last_outbound: MessageRecord | None = None
        self._pending: _Pending | None = None

    # ---- entry ---------------------------------------------------------------------------------
    async def run(self) -> SessionRecord:
        o = self._o
        done = o.interruption.register_loop(self._sid)
        try:
            await self._body()
        except (_InterruptedError, SessionInterruptedError):
            pass  # nothing is sent, nothing is written: the InterruptionHandler cleans up
        except asyncio.CancelledError:
            raise
        except _FailedError as failed:
            await self._terminate_failed(failed)
        except AppError as exc:
            await self._terminate_failed(
                _FailedError(
                    exc.error, reason=_reason_for(exc), recorded=False, plan_id=self._plan_id()
                )
            )
        except Exception as exc:
            error = o.failure_manager.classify(exc)
            try:
                await self._terminate_failed(
                    _FailedError(
                        error, reason=REASON_FAILURE, recorded=False, plan_id=self._plan_id()
                    )
                )
            except Exception:  # the original defect matters more than the cleanup's
                log.exception("session %s: cleanup after an unexpected error failed", self._sid)
            raise
        finally:
            done.set()  # the handler drains on this event (it may already have taken it)
            o.interruption.loop_finished(self._sid)
        return self._session()

    async def _body(self) -> None:
        session = self._session()
        if session.status is not SessionState.RUNNING:
            # interrupted (or failed) before the loop got its first tick: nothing to drive
            log.info("session %s is %s: the loop does not start", self._sid, session.status.value)
            return
        if self._entry is _Entry.START:
            await self._start()
        elif self._entry is _Entry.FOLLOW_UP:
            await self._follow_up()
        else:
            await self._resume()
        while True:
            inbound = await self._receive()
            if inbound.message_type in CONCLUDING_MESSAGE_TYPES:
                await self._finish(inbound)
                return
            content = await self._execute_plan(inbound)
            self._pending = _Pending(
                message_type=MessageType.EXECUTION_RESULT,
                cycle_type=CycleType.EXECUTION,
                content=content,
            )
            await self._send_pending()

    # ---- entry points --------------------------------------------------------------------------
    async def _start(self) -> None:
        o = self._o
        conv = self._conversation()
        if conv.status is ConversationState.NEW:
            conv = self._transition(ConversationState.ACTIVE, reason=REASON_USER_REQUEST)
        if conv.remote_conversation_id is None:
            metadata = {
                "session_id": self._sid,
                "parent_conversation_id": conv.parent_conversation_id,
            }
            remote, _ = await self._call(
                OP_INIT, lambda: o.transport.init_conversation(o.instructions, metadata)
            )
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id,
                remote_conversation_id=remote,
                context_bytes=o.monitor.instructions_bytes(o.instructions),
            )
        session = self._session()
        self._pending = _Pending(
            message_type=MessageType.USER_REQUEST,
            cycle_type=CycleType.DISCOVERY,
            user_message=(
                self._user_message if self._user_message is not None else session.user_message
            ),
        )
        await self._send_pending()

    async def _follow_up(self) -> None:
        conv = self._conversation()
        if conv.status is ConversationState.COMPLETED:
            self._transition(ConversationState.WAITING_USER, reason=REASON_REUSABLE)
        self._pending = _Pending(
            message_type=MessageType.USER_REQUEST,
            cycle_type=CycleType.EXECUTION,
            follow_up=True,
            user_message=self._user_message,
        )
        await self._send_pending()

    async def _resume(self) -> None:
        o = self._o
        conv = self._conversation()
        record = pending_outbound_of(o.store, conv)
        if record is None:
            raise ValueError(f"conversation {conv.conversation_id} has nothing to resume")
        cycle = o.store.get_cycle(record.cycle_id) if record.cycle_id is not None else None
        if cycle is None and conv.current_cycle_id is not None:
            cycle = o.store.get_cycle(conv.current_cycle_id)
        self._cycle = cycle
        self._last_outbound = record
        self._pending = self._pending_from_record(record, conv)
        if not record.post_confirmed:
            message = self._build(self._pending, conv, record.message_id)
            await self._post(message, record)

    def _pending_from_record(self, record: MessageRecord, conv: ConversationRecord) -> _Pending:
        content = record.payload.get("content", {})
        if record.message_type is MessageType.USER_REQUEST:
            request = UserRequestContent.model_validate(content)
            return _Pending(
                message_type=MessageType.USER_REQUEST,
                cycle_type=CycleType.EXECUTION
                if conv.final_answer_received
                else CycleType.DISCOVERY,
                follow_up=conv.final_answer_received,
                user_message=request.user_message,
            )
        return _Pending(
            message_type=MessageType.EXECUTION_RESULT,
            cycle_type=CycleType.EXECUTION,
            content=ExecutionResultContent.model_validate(content),
            fitted=True,
        )

    # ---- 1. send -------------------------------------------------------------------------------
    async def _send_pending(self) -> None:
        o = self._o
        pending = self._require_pending()
        self._check_interrupted()
        session = self._session()
        conv = self._conversation()
        message_id = o.ids.message_id()
        if pending.message_type is MessageType.EXECUTION_RESULT and not pending.fitted:
            assert pending.content is not None
            overhead = self._envelope_overhead(conv, message_id, pending.message_type)
            pending.content = o.payload_guard.fit_message(
                pending.content, max(0, o.config.payload.max_message_bytes - overhead)
            )
            pending.fitted = True
        message = self._build(pending, conv, message_id)
        self._open_cycle(message, session, pending.cycle_type)
        conv = self._conversation()
        state = o.monitor.evaluate(conv, projected_outbound_bytes=message.size_bytes)
        if state is ContextWindowState.SATURATED:
            reason = (
                WINDOW_REASON_SATURATION_RATIO
                if o.monitor.evaluate(conv) is ContextWindowState.SATURATED
                else WINDOW_REASON_PROJECTION
            )
        else:
            reason = WINDOW_REASON_THRESHOLD
        self._apply_window(state, reason)
        if state is ContextWindowState.SATURATED:
            if not can_transition(
                CONVERSATION_TRANSITIONS, conv.status, ConversationState.ROTATING
            ):
                error = RotationFailedError(
                    ROTATION_NOT_ALLOWED_CODE,
                    conversation_state=conv.status.value,
                    context_bytes=conv.context_bytes,
                    projected_outbound_bytes=message.size_bytes,
                    budget_bytes=o.monitor.thresholds().budget_bytes,
                ).error
                raise _FailedError(error, reason=REASON_ROTATION_FAILED, recorded=False)
            await self._rotate()
            return
        self._transition(
            ConversationState.WAITING_MODEL_RESPONSE, reason=pending.message_type.value
        )
        assert self._last_outbound is not None
        await self._post(message, self._last_outbound)

    def _open_cycle(
        self, message: OutboundMessage, session: SessionRecord, cycle_type: CycleType
    ) -> None:
        """ADR-007: a cycle starts when its outbound message is persisted; ADR-012: ``max_cycles``
        is checked before, the counter incremented in the same write."""
        o = self._o
        limit = session.budget.max_cycles
        if session.consumed_cycles >= limit:
            error = BudgetExceededError("max_cycles", limit, session.consumed_cycles).error
            raise _FailedError(
                error, reason=REASON_BUDGET_EXCEEDED, recorded=False, stage=STAGE_BEFORE_CYCLE
            )
        conv = self._conversation()
        now = o.clock.now()
        cycle = CycleRecord(
            cycle_id=o.ids.cycle_id(),
            conversation_id=conv.conversation_id,
            session_id=self._sid,
            cycle_type=cycle_type,
            status=CycleState.RUNNING,
            outbound_message_id=message.envelope.message_id,
            started_at=now,
        )
        record = MessageRecord(
            message_id=message.envelope.message_id,
            session_id=self._sid,
            conversation_id=conv.conversation_id,
            direction=MessageDirection.OUTBOUND,
            message_type=message.message_type,
            payload=message.payload,
            size_bytes=message.size_bytes,
            cycle_id=cycle.cycle_id,
            created_at=now,
        )
        with o.store.transaction():
            updated = o.lifecycle.update_session(
                self._sid, consumed_cycles=session.consumed_cycles + 1
            )
            o.store.save_cycle(cycle)
            o.store.save_message(record)
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id,
                current_cycle_id=cycle.cycle_id,
                last_outbound_message_id=record.message_id,
            )
        self._cycle = cycle
        self._last_outbound = record
        self._publish(
            EventType.CYCLE_STARTED,
            now,
            cycle_id=cycle.cycle_id,
            payload={
                "cycle_type": cycle.cycle_type.value,
                "outbound_message_id": record.message_id,
                "outbound_message_type": record.message_type.value,
                "consumed_cycles": updated.consumed_cycles,
            },
        )
        self._publish_budget(updated)

    async def _post(self, message: OutboundMessage, record: MessageRecord) -> None:
        """POST an already persisted outbound message (retries reuse the same ``message_id``),
        confirm it, count its bytes, ``message.outbound``, re-evaluate the window."""
        o = self._o
        remote = self._remote()
        ack, attempts = await self._call(
            OP_POST, lambda: o.transport.post_message(remote, message.payload)
        )
        conv = self._conversation()
        now = o.clock.now()
        confirmed = record.model_copy(update={"post_confirmed": True, "posted_at": now})
        with o.store.transaction():
            o.store.save_message(confirmed)
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id,
                context_bytes=o.monitor.account(conv.context_bytes, message.size_bytes),
                last_model_response_state=_AWAITING,
            )
        self._last_outbound = confirmed
        self._publish(
            EventType.MESSAGE_OUTBOUND,
            now,
            cycle_id=record.cycle_id,
            payload={
                "message_type": message.message_type.value,
                "message_id": record.message_id,
                "post_status": ack.http_status,
                "size_bytes": message.size_bytes,
                "attempts": attempts,
            },
        )
        self._apply_window(o.monitor.evaluate(self._conversation()), WINDOW_REASON_THRESHOLD)

    # ---- 2. receive ----------------------------------------------------------------------------
    async def _receive(self) -> InboundMessage:
        o = self._o
        while True:
            self._check_interrupted()
            get = self._get_call(self._remote(), self._conversation().get_cursor)
            try:
                reply, _ = await self._call(OP_GET, get)
            except _RotateRequestedError as requested:
                self._apply_window(
                    o.monitor.evaluate(self._conversation(), error=requested.error),
                    WINDOW_REASON_CONTEXT_ERROR,
                )
                await self._rotate()
                continue
            except _FailedError as failed:
                if o.monitor.should_rotate_on_unusable_reply(self._conversation(), failed.error):
                    self._apply_window(ContextWindowState.SATURATED, WINDOW_REASON_UNUSABLE_REPLY)
                    await self._rotate()
                    continue
                raise
            inbound = await self._accept(reply)
            if inbound is not None:
                return inbound

    def _get_call(self, remote: str, cursor: str | None) -> Callable[[], Awaitable[GetResult]]:
        """The GET closure of one attempt series: the same cursor on every retry (ADR-004)."""
        transport = self._o.transport

        async def get() -> GetResult:
            return await transport.wait_for_reply(remote, after=cursor)

        return get

    async def _accept(self, reply: GetResult) -> InboundMessage | None:
        """Validate the reply (ADR-007 table); persist and publish it either way. ``None`` when a
        rejected reply led to a rotation (ADR-019 §2): the caller receives again in the child."""
        o = self._o
        conv = self._conversation()
        assert self._last_outbound is not None
        expected = o.adapter.expected_inbound(self._last_outbound, conv)
        message_ids, plan_ids, task_ids = self._known_ids()
        received_bytes = sum(size_bytes(message) for message in reply.messages)
        try:
            inbound = o.adapter.parse_inbound(
                reply.messages,
                expected=expected,
                conversation=conv,
                known_message_ids=message_ids,
                known_plan_ids=plan_ids,
                known_task_ids=task_ids,
                stored_output_task_ids=task_ids,
            )
        except ProtocolError as exc:
            self._persist_rejected(reply, exc, received_bytes)
            error, decision = o.failure_manager.handle(
                exc,
                1,
                operation=OP_GET,
                session_id=self._sid,
                conversation_id=conv.conversation_id,
                cycle_id=self._cycle_id(),
            )
            if o.monitor.should_rotate_on_unusable_reply(self._conversation(), error):
                self._apply_window(ContextWindowState.SATURATED, WINDOW_REASON_UNUSABLE_REPLY)
                await self._rotate()
                return None
            raise _FailedError(error, reason=REASON_FAILURE, recorded=True) from exc
        now = o.clock.now()
        record = MessageRecord(
            message_id=inbound.envelope.message_id,
            session_id=self._sid,
            conversation_id=conv.conversation_id,
            direction=MessageDirection.INBOUND,
            message_type=inbound.message_type,
            payload=inbound.payload,
            size_bytes=inbound.size_bytes,
            cycle_id=self._cycle_id(),
            received_at=now,
            validation_status="valid",
            created_at=now,
        )
        with o.store.transaction():
            o.store.save_message(record)
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id,
                context_bytes=o.monitor.account(conv.context_bytes, inbound.size_bytes),
                get_cursor=reply.cursor,
                last_inbound_message_id=record.message_id,
                last_model_response_state=_RECEIVED_VALID,
            )
        self._publish(
            EventType.MESSAGE_INBOUND,
            now,
            cycle_id=record.cycle_id,
            payload={
                "message_type": inbound.message_type.value,
                "message_id": record.message_id,
                "get_status": reply.http_status,
                "validation_status": "valid",
                "size_bytes": inbound.size_bytes,
            },
        )
        self._apply_window(o.monitor.evaluate(self._conversation()), WINDOW_REASON_THRESHOLD)
        return inbound

    def _persist_rejected(self, reply: GetResult, exc: ProtocolError, received_bytes: int) -> None:
        """A rejected reply is persisted (``validation_status = invalid``) and counted in the
        context — it is in the model's context (ADR-013) — then ``message.rejected``."""
        o = self._o
        conv = self._conversation()
        first: dict[str, Any] = dict(reply.messages[0]) if reply.messages else {}
        raw_type = first.get("type")
        raw_id = first.get("message_id")
        type_str = raw_type if isinstance(raw_type, str) else None
        id_str = raw_id if isinstance(raw_id, str) and raw_id else None
        try:
            message_type = (
                MessageType(type_str) if type_str is not None else MessageType.SYSTEM_ERROR
            )
        except ValueError:
            message_type = MessageType.SYSTEM_ERROR
        message_id = id_str if id_str is not None and o.store.get_message(id_str) is None else None
        if message_id is None:
            message_id = o.ids.message_id()
        payload = first if len(reply.messages) == 1 else {"messages": list(reply.messages)}
        now = o.clock.now()
        record = MessageRecord(
            message_id=message_id,
            session_id=self._sid,
            conversation_id=conv.conversation_id,
            direction=MessageDirection.INBOUND,
            message_type=message_type,
            payload=payload,
            size_bytes=received_bytes,
            cycle_id=self._cycle_id(),
            received_at=now,
            validation_status="invalid",
            created_at=now,
        )
        with o.store.transaction():
            o.store.save_message(record)
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id,
                context_bytes=o.monitor.account(conv.context_bytes, received_bytes),
                protocol_error_count=conv.protocol_error_count + 1,
                get_cursor=reply.cursor if reply.cursor is not None else conv.get_cursor,
                last_inbound_message_id=record.message_id,
                last_model_response_state=_RECEIVED_INVALID,
            )
        self._publish(
            EventType.MESSAGE_REJECTED,
            now,
            cycle_id=record.cycle_id,
            payload={
                "message_type": type_str,
                "message_id": id_str,
                "get_status": reply.http_status,
                "validation_status": "invalid",
                "error_code": exc.error.error_code,
                "size_bytes": received_bytes,
                "details": dict(exc.error.details),
            },
        )

    # ---- 3. process ----------------------------------------------------------------------------
    async def _execute_plan(self, inbound: InboundMessage) -> ExecutionResultContent:
        o = self._o
        self._check_interrupted()
        session = self._session()
        conv = self._conversation()
        cycle = self._require_cycle()
        plan, tasks = o.adapter.plan_to_records(
            inbound, session=session, conversation=conv, cycle_id=cycle.cycle_id, clock=o.clock
        )
        now = o.clock.now()
        refined = cycle.model_copy(
            update={"cycle_type": cycle_type_for_plan(plan.plan_type), "plan_id": plan.plan_id}
        )
        with o.store.transaction():
            o.store.save_plan(plan)
            o.store.save_tasks(tasks)
            session = o.lifecycle.update_session(
                self._sid, consumed_plans=session.consumed_plans + 1
            )
            o.store.save_cycle(refined)
            self._conv = o.lifecycle.update_conversation(
                conv.conversation_id, current_plan_id=plan.plan_id
            )
        self._cycle = refined
        contradictory = [
            warning.split(":", 1)[1]
            for warning in inbound.warnings
            if warning.startswith("CONTRADICTORY_FLAGS:")
        ]
        self._publish(
            EventType.PLAN_RECEIVED,
            now,
            cycle_id=cycle.cycle_id,
            plan_id=plan.plan_id,
            payload={
                "plan_type": plan.plan_type.value,
                "objective": plan.objective,
                "execution_policy": plan.execution_policy.value,
                "max_parallel_workers": plan.max_parallel_workers,
                "task_count": plan.task_count,
                "consumed_plans": session.consumed_plans,
                "contradictory_flags": contradictory,
            },
        )
        self._publish_budget(session)
        for warning in inbound.warnings:
            code, _, argument = warning.partition(":")
            details = {"task_id": argument} if code == "CONTRADICTORY_FLAGS" else {}
            self._publish(
                EventType.AUDIT_WARNING,
                now,
                cycle_id=cycle.cycle_id,
                plan_id=plan.plan_id,
                payload={"code": warning, "entity": "plan", "id": plan.plan_id, "details": details},
            )

        # budget checkpoints before the plan starts (ADR-012 §3, ADR-007 PENDING -> FAILED)
        limit_plans = session.budget.max_plans
        if session.consumed_plans > limit_plans:
            self._fail_plan_before_start(
                plan, tasks, "max_plans", limit_plans, session.consumed_plans
            )
        elapsed = self._consumed_duration_ms(session)
        limit_duration = session.budget.max_total_duration_ms
        if elapsed >= limit_duration:
            self._fail_plan_before_start(
                plan, tasks, "max_total_duration_ms", limit_duration, elapsed
            )

        self._transition(ConversationState.RUNNING_PLAN, reason=REASON_PLAN_RECEIVED)
        outcome = await o.plan_runner.run(
            plan, tasks, session, interrupt=o.interruption.token_for(self._sid)
        )
        if outcome.interrupted:
            raise _InterruptedError()
        if outcome.budget_exceeded:
            consumed = self._consumed_duration_ms(self._session())
            error = BudgetExceededError("max_total_duration_ms", limit_duration, consumed).error
            raise _FailedError(
                error,
                reason=REASON_BUDGET_EXCEEDED,
                recorded=False,
                stage=STAGE_BETWEEN_TASKS,
                plan_id=plan.plan_id,
            )
        assert outcome.execution_result is not None
        self._complete_cycle(inbound.envelope.message_id, inbound.message_type)
        self._conv = o.lifecycle.update_conversation(
            self._conversation().conversation_id, last_completed_plan_id=plan.plan_id
        )
        return outcome.execution_result

    def _fail_plan_before_start(
        self, plan: PlanRecord, tasks: list[TaskRecord], limit: str, limit_value: int, consumed: int
    ) -> None:
        """ADR-007 / ADR-012: the persisted plan fails before its first task, its tasks are
        skipped (``budget_exceeded``), then the session fails."""
        o = self._o
        now = o.clock.now()
        assert_transition(PLAN_TRANSITIONS, plan.status, PlanState.FAILED, entity=PLAN_ENTITY)
        skipped: list[TaskRecord] = []
        for task in tasks:
            assert_transition(TASK_TRANSITIONS, task.status, TaskState.SKIPPED, entity=TASK_ENTITY)
            skipped.append(
                task.model_copy(
                    update={
                        "status": TaskState.SKIPPED,
                        "reason": BUDGET_EXCEEDED_REASON,
                        "ended_at": now,
                        "updated_at": now,
                    }
                )
            )
        stop_reason = f"{BUDGET_EXCEEDED_REASON}:{limit}"
        failed = plan.model_copy(
            update={
                "status": PlanState.FAILED,
                "stop_reason": stop_reason,
                "ended_at": now,
                "updated_at": now,
                "skipped_task_count": len(skipped),
            }
        )
        with o.store.transaction():
            o.store.save_tasks(skipped)
            o.store.save_plan(failed)
        for before, after in zip(tasks, skipped, strict=True):
            self._publish(
                EventType.TASK_STATE_CHANGED,
                now,
                cycle_id=plan.cycle_id,
                plan_id=plan.plan_id,
                task_id=after.task_id,
                payload=state_change_payload(
                    before.status.value, after.status.value, BUDGET_EXCEEDED_REASON
                ),
            )
        payload = state_change_payload(
            plan.status.value, failed.status.value, BUDGET_EXCEEDED_REASON
        )
        payload["stop_reason"] = stop_reason
        self._publish(
            EventType.PLAN_STATE_CHANGED,
            now,
            cycle_id=plan.cycle_id,
            plan_id=plan.plan_id,
            payload=payload,
        )
        error = BudgetExceededError(limit, limit_value, consumed).error
        raise _FailedError(
            error,
            reason=REASON_BUDGET_EXCEEDED,
            recorded=False,
            stage=STAGE_BEFORE_PLAN,
            plan_id=plan.plan_id,
        )

    async def _finish(self, inbound: InboundMessage) -> None:
        """§11 / ADR-022: a ``final_answer`` or a ``user_response`` → conversation ``COMPLETED``
        then ``CLOSED`` or ``WAITING_USER``, session ``COMPLETED``.

        Only a ``final_answer`` is written to ``SessionRecord.final_answer``; a ``user_response``
        is read back from the messages table. Under ``auto_close_on_final_answer`` the conversation
        is closed, unless the ``user_response`` is a question (``expects_reply``): the user must be
        able to answer it, so the conversation stays ``WAITING_USER`` (``auto_close_skipped``).
        """
        o = self._o
        message_id = inbound.envelope.message_id
        if isinstance(inbound.content, UserResponseContent):
            reason = REASON_USER_RESPONSE
            session = self._session()
            auto_close_skipped = (
                session.auto_close_on_final_answer and inbound.content.expects_reply
            )
            close = session.auto_close_on_final_answer and not auto_close_skipped
            event_type = EventType.USER_RESPONSE_RECEIVED
            payload: dict[str, Any] = {
                "message_id": message_id,
                "format": inbound.content.format,
                "status": inbound.content.status,
                "expects_reply": inbound.content.expects_reply,
                "body_bytes": len(inbound.content.body.encode("utf-8")),
                "auto_close_on_final_answer": session.auto_close_on_final_answer,
                "auto_close_skipped": auto_close_skipped,
            }
        else:
            reason = REASON_FINAL_ANSWER
            content = dict(inbound.envelope.content)
            session = o.lifecycle.update_session(self._sid, final_answer=content)
            close = session.auto_close_on_final_answer
            event_type = EventType.FINAL_ANSWER_RECEIVED
            payload = {
                "message_id": message_id,
                "status": str(content.get("status")),
                "auto_close_on_final_answer": session.auto_close_on_final_answer,
            }
        payload.update(
            consumed_cycles=session.consumed_cycles,
            consumed_plans=session.consumed_plans,
            session_duration_ms=self._consumed_duration_ms(session),
        )
        conv = self._transition(
            ConversationState.COMPLETED, reason=reason, final_answer_received=True
        )
        self._publish(event_type, o.clock.now(), cycle_id=self._cycle_id(), payload=payload)
        self._complete_cycle(message_id, inbound.message_type)
        if close:
            conv = self._transition(
                ConversationState.CLOSED, reason=REASON_AUTO_CLOSE, closure_reason=REASON_AUTO_CLOSE
            )
        else:
            conv = self._transition(ConversationState.WAITING_USER, reason=REASON_REUSABLE)
        current = self._session()
        if current.status is SessionState.RUNNING:
            o.lifecycle.transition_session(self._sid, SessionState.COMPLETED, reason=reason)
        if conv.status is ConversationState.CLOSED:
            await self._close_remote([conv])

    # ---- rotation (ADR-014) --------------------------------------------------------------------
    async def _rotate(self) -> None:
        """Rotate the current conversation with ``M`` pending; on return the current conversation
        is the child, ``M`` has been retransmitted there and the loop receives next."""
        o = self._o
        pending = self._require_pending()
        cycle = self._require_cycle()
        assert self._last_outbound is not None
        outbound = PendingOutbound(
            message_type=pending.message_type,
            original_message_id=self._last_outbound.message_id,
            build=lambda child, message_id: self._build(pending, child, message_id),
            cycle_id=cycle.cycle_id,
        )
        session = self._session()
        conv = self._conversation()
        try:
            result = await o.rotation.rotate(session, conv, outbound)
        except RotationFailedError as exc:
            raise _FailedError(exc.error, reason=REASON_ROTATION_FAILED, recorded=False) from exc
        except BudgetExceededError as exc:
            raise _FailedError(
                exc.error, reason=REASON_BUDGET_EXCEEDED, recorded=False, stage=STAGE_BEFORE_CYCLE
            ) from exc
        except TransportError as exc:
            if exc.error_type is ErrorType.INTERRUPTED:
                raise _InterruptedError() from exc
            self._record_failure(exc.error)
            raise _FailedError(exc.error, reason=REASON_ROTATION_FAILED, recorded=True) from exc
        except ProtocolError as exc:
            self._record_failure(exc.error)
            raise _FailedError(exc.error, reason=REASON_ROTATION_FAILED, recorded=True) from exc
        child = self._fresh_conversation(result.child.conversation_id)
        if pending.message_type is MessageType.USER_REQUEST and pending.follow_up:
            child = o.lifecycle.update_conversation(
                child.conversation_id, final_answer_received=True
            )
        self._conv = child
        self._last_outbound = o.store.get_message(result.retransmitted_message_id)
        self._apply_window(o.monitor.evaluate(child), WINDOW_REASON_THRESHOLD)

    # ---- transport calls with the §7 policy -----------------------------------------------------
    async def _call(self, operation: str, fn: Callable[[], Awaitable[T]]) -> tuple[T, int]:
        """Run one remote operation under the failure policy: ``breaker.allow()`` first (ADR-019
        §6), then the ``FailureManager`` decision on every ``TransportError`` — ``retry`` after the
        backoff (same message / cursor), ``rotate`` and ``fail`` surfaced to the caller, an
        abandoned call stops the loop. Returns the result and the number of attempts."""
        o = self._o
        attempt = 0
        while True:
            attempt += 1
            self._check_interrupted()
            try:
                await self._ensure_breaker_allows(operation)
                result = await fn()
            except TransportError as exc:
                if exc.error_type is ErrorType.INTERRUPTED:
                    raise _InterruptedError() from exc
                error, decision = o.failure_manager.handle(
                    exc,
                    attempt,
                    operation=operation,
                    session_id=self._sid,
                    conversation_id=self._conversation_id(),
                    cycle_id=self._cycle_id(),
                )
                if decision.kind == "retry":
                    self._bump_retry_count()
                    await self._interruptible_sleep(decision.delay_ms or 0)
                    continue
                if decision.kind == "rotate":
                    raise _RotateRequestedError(error) from exc
                if decision.kind == "abort":
                    raise _InterruptedError() from exc
                raise _FailedError(error, reason=REASON_FAILURE, recorded=True) from exc
            o.failure_manager.note_success()
            return result, attempt

    async def _ensure_breaker_allows(self, operation: str) -> None:
        """ADR-019 §6: an open breaker is waited on once (``open_duration_ms``, bounded by the
        remaining duration budget), then a persistent refusal is a ``NETWORK_ERROR / CIRCUIT_OPEN``."""
        o = self._o
        if o.breaker.allow():
            return
        wait_ms = min(o.config.circuit_breaker.open_duration_ms, self._remaining_budget_ms())
        if wait_ms > 0:
            await self._interruptible_sleep(wait_ms)
        if o.breaker.allow():
            return
        raise TransportError(
            ErrorType.NETWORK_ERROR,
            CIRCUIT_OPEN_CODE,
            retryable=False,
            operation=operation,
            http_status=None,
            breaker_state=o.breaker.state.value,
            waited_ms=wait_ms,
        )

    async def _interruptible_sleep(self, delay_ms: int) -> None:
        """Wait ``delay_ms`` on the injected ``sleep`` unless the session is interrupted first."""
        o = self._o
        self._check_interrupted()
        if delay_ms <= 0:
            return
        token = o.interruption.token_for(self._sid)
        sleeper: asyncio.Future[Any] = asyncio.ensure_future(o.sleep(delay_ms / 1000))
        waiter: asyncio.Future[Any] = asyncio.ensure_future(token.wait())
        try:
            await asyncio.wait({sleeper, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for future in (sleeper, waiter):
                if not future.done():
                    future.cancel()
            await asyncio.gather(sleeper, waiter, return_exceptions=True)
        self._check_interrupted()

    def _bump_retry_count(self) -> None:
        o = self._o
        cycle = self._cycle
        if cycle is None:
            return
        self._cycle = cycle.model_copy(update={"retry_count": cycle.retry_count + 1})
        o.store.save_cycle(self._cycle)

    # ---- failure termination -------------------------------------------------------------------
    async def _terminate_failed(self, failed: _FailedError) -> None:
        """Cycle ``FAILED``, conversation(s) ``FAILED``, session ``FAILED`` — valid transitions only
        — after the ``FailureRecord`` and the ``budget.exceeded`` event; remote close best effort."""
        o = self._o
        failure_id: str | None = None
        if failed.recorded:
            failures = o.store.list_failures(self._sid)
            failure_id = failures[-1].failure_id if failures else None
        else:
            failure_id = self._record_failure(failed.error, plan_id=failed.plan_id)
        if failed.stage is not None:
            details = failed.error.details
            self._publish(
                EventType.BUDGET_EXCEEDED,
                o.clock.now(),
                plan_id=failed.plan_id,
                payload={
                    "limit": details.get("limit"),
                    "limit_value": details.get("limit_value"),
                    "consumed": details.get("consumed"),
                    "stage": failed.stage,
                },
            )
        self._fail_cycle(failed.reason)
        failed_conversations = self._fail_conversations(failed.reason)
        session = self._session()
        if can_transition(SESSION_TRANSITIONS, session.status, SessionState.FAILED):
            o.lifecycle.transition_session(
                self._sid, SessionState.FAILED, reason=failed.reason, last_failure_id=failure_id
            )
        await self._close_remote(failed_conversations)

    def _record_failure(self, error: NormalizedError, *, plan_id: str | None = None) -> str:
        record = self._o.failure_manager.record(
            error,
            session_id=self._sid,
            conversation_id=self._conversation_id(),
            plan_id=plan_id,
        )
        return record.failure_id

    def _fail_cycle(self, reason: str) -> None:
        o = self._o
        cycle = self._cycle
        if cycle is None:
            return
        current = o.store.get_cycle(cycle.cycle_id) or cycle
        if current.status is not CycleState.RUNNING:
            self._cycle = current
            return
        assert_transition(CYCLE_TRANSITIONS, current.status, CycleState.FAILED, entity=CYCLE_ENTITY)
        ended_at = o.clock.now()
        failed = current.model_copy(update={"status": CycleState.FAILED, "ended_at": ended_at})
        o.store.save_cycle(failed)
        self._cycle = failed
        self._publish(
            EventType.CYCLE_ENDED,
            ended_at,
            cycle_id=failed.cycle_id,
            plan_id=failed.plan_id,
            payload={
                "status": failed.status.value,
                "reason": reason,
                "duration_ms": max(0, (ended_at - current.started_at) // _ONE_MS),
                "retry_count": failed.retry_count,
                "inbound_message_id": failed.inbound_message_id,
                "inbound_message_type": None,
            },
        )

    def _fail_conversations(self, reason: str) -> list[ConversationRecord]:
        """The current conversation and, during a rotation, its ``ROTATING`` parent."""
        o = self._o
        failed: list[ConversationRecord] = []
        session = self._session()
        current_id = session.current_conversation_id
        if current_id is None:
            return failed
        conv = o.store.get_conversation(current_id)
        if conv is None:
            return failed
        parent = (
            o.store.get_conversation(conv.parent_conversation_id)
            if conv.parent_conversation_id is not None
            else None
        )
        if can_transition(CONVERSATION_TRANSITIONS, conv.status, ConversationState.FAILED):
            failed.append(
                o.lifecycle.transition_conversation(
                    conv.conversation_id, ConversationState.FAILED, reason=reason
                )
            )
        if parent is not None and parent.status is ConversationState.ROTATING:
            failed.append(
                o.lifecycle.transition_conversation(
                    parent.conversation_id, ConversationState.FAILED, reason=reason
                )
            )
        self._conv = o.store.get_conversation(current_id)
        return failed

    async def _close_remote(self, conversations: Iterable[ConversationRecord]) -> None:
        """ADR-006: best effort, never retried, every failure ignored."""
        for conversation in conversations:
            remote = conversation.remote_conversation_id
            if remote is None:
                continue
            try:
                await self._o.transport.close_conversation(remote)
            except Exception:  # noqa: BLE001 - best effort by design
                log.debug("session %s: best-effort remote close of %s failed", self._sid, remote)

    # ---- small steps ---------------------------------------------------------------------------
    def _complete_cycle(self, inbound_message_id: str, inbound_type: MessageType) -> None:
        o = self._o
        cycle = self._cycle
        if cycle is None:
            return
        current = o.store.get_cycle(cycle.cycle_id) or cycle
        if current.status is not CycleState.RUNNING:
            self._cycle = current
            return
        assert_transition(
            CYCLE_TRANSITIONS, current.status, CycleState.COMPLETED, entity=CYCLE_ENTITY
        )
        ended_at = o.clock.now()
        completed = current.model_copy(
            update={
                "status": CycleState.COMPLETED,
                "ended_at": ended_at,
                "inbound_message_id": inbound_message_id,
            }
        )
        o.store.save_cycle(completed)
        self._cycle = completed
        self._publish(
            EventType.CYCLE_ENDED,
            ended_at,
            cycle_id=completed.cycle_id,
            plan_id=completed.plan_id,
            payload={
                "status": completed.status.value,
                "duration_ms": max(0, (ended_at - current.started_at) // _ONE_MS),
                "retry_count": completed.retry_count,
                "inbound_message_id": inbound_message_id,
                "inbound_message_type": inbound_type.value,
            },
        )

    def _apply_window(self, state: ContextWindowState, reason: str) -> None:
        conv = self._conversation()
        if state is conv.context_window_state:
            return
        self._conv = self._o.lifecycle.transition_context_window(
            conv.conversation_id, state, reason=reason
        )

    def _transition(
        self, to: ConversationState, *, reason: str, **updates: Any
    ) -> ConversationRecord:
        conv = self._conversation()
        self._conv = self._o.lifecycle.transition_conversation(
            conv.conversation_id, to, reason=reason, **updates
        )
        return self._conv

    def _build(
        self, pending: _Pending, conv: ConversationRecord, message_id: str
    ) -> OutboundMessage:
        o = self._o
        if pending.message_type is MessageType.USER_REQUEST:
            session = self._session()
            return o.adapter.build_user_request(
                conv,
                message_id,
                session.goal,
                pending.user_message if pending.user_message is not None else session.user_message,
                session.budget,
            )
        assert pending.content is not None
        return o.adapter.build_execution_result(conv, message_id, pending.content)

    def _envelope_overhead(
        self, conv: ConversationRecord, message_id: str, message_type: MessageType
    ) -> int:
        """Bytes of the envelope around ``content`` (ADR-010: the whole message must fit)."""
        probe: dict[str, Any] = {
            "type": message_type.value,
            "conversation_id": conv.remote_conversation_id or conv.conversation_id,
            "message_id": message_id,
            "content": {},
        }
        return size_bytes(probe) - 2

    def _known_ids(self) -> tuple[set[str], set[str], set[str]]:
        """Every message, plan and task identifier of the session (ADR-007, ADR-019 §1)."""
        store = self._o.store
        message_ids = {
            message.message_id
            for conversation in store.list_conversations(self._sid)
            for message in store.list_messages(conversation.conversation_id)
        }
        plan_ids = {plan.plan_id for plan in store.list_plans(self._sid)}
        task_ids = {task.task_id for task in store.list_tasks(self._sid)}
        return message_ids, plan_ids, task_ids

    def _publish_budget(self, session: SessionRecord) -> None:
        self._publish(
            EventType.BUDGET_UPDATED,
            self._o.clock.now(),
            payload={
                "consumed_cycles": session.consumed_cycles,
                "consumed_plans": session.consumed_plans,
                "consumed_duration_ms": self._consumed_duration_ms(session),
                "max_cycles": session.budget.max_cycles,
                "max_plans": session.budget.max_plans,
                "max_total_duration_ms": session.budget.max_total_duration_ms,
            },
            with_conversation=False,
        )

    def _consumed_duration_ms(self, session: SessionRecord) -> int:
        if session.started_at is None:
            return 0
        end = session.ended_at if session.ended_at is not None else self._o.clock.now()
        return max(0, (end - session.started_at) // _ONE_MS)

    def _remaining_budget_ms(self) -> int:
        session = self._session()
        return max(0, session.budget.max_total_duration_ms - self._consumed_duration_ms(session))

    def _check_interrupted(self) -> None:
        self._o.interruption.raise_if_interrupted(self._sid)

    # ---- records -------------------------------------------------------------------------------
    def _session(self) -> SessionRecord:
        session = self._o.store.get_session(self._sid)
        if session is None:
            raise KeyError(f"unknown session: {self._sid}")
        return session

    def _conversation(self) -> ConversationRecord:
        """The current conversation of the session, re-read from the store (the truth)."""
        if self._conv is not None:
            fresh = self._o.store.get_conversation(self._conv.conversation_id)
            if fresh is not None:
                self._conv = fresh
                return fresh
        session = self._session()
        if session.current_conversation_id is None:
            raise KeyError(f"session {self._sid} has no current conversation")
        return self._fresh_conversation(session.current_conversation_id)

    def _fresh_conversation(self, conversation_id: str) -> ConversationRecord:
        conversation = self._o.store.get_conversation(conversation_id)
        if conversation is None:
            raise KeyError(f"unknown conversation: {conversation_id}")
        self._conv = conversation
        return conversation

    def _conversation_id(self) -> str | None:
        return self._conv.conversation_id if self._conv is not None else None

    def _cycle_id(self) -> str | None:
        return self._cycle.cycle_id if self._cycle is not None else None

    def _plan_id(self) -> str | None:
        return self._conv.current_plan_id if self._conv is not None else None

    def _remote(self) -> str:
        conv = self._conversation()
        return conv.remote_conversation_id or conv.conversation_id

    def _require_pending(self) -> _Pending:
        if self._pending is None:
            raise RuntimeError("no pending outbound message")
        return self._pending

    def _require_cycle(self) -> CycleRecord:
        if self._cycle is None:
            raise RuntimeError("no open cycle")
        return self._cycle

    def _publish(
        self,
        event_type: EventType,
        timestamp: datetime,
        *,
        payload: dict[str, Any],
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
        with_conversation: bool = True,
    ) -> None:
        self._o.bus.publish(
            Event(
                event_type=event_type,
                timestamp=timestamp,
                session_id=self._sid,
                conversation_id=self._conversation_id() if with_conversation else None,
                cycle_id=cycle_id,
                plan_id=plan_id,
                task_id=task_id,
                payload=payload,
            )
        )


def _reason_for(exc: AppError) -> str:
    if exc.error_type is ErrorType.BUDGET_EXCEEDED:
        return REASON_BUDGET_EXCEEDED
    if exc.error_type is ErrorType.ROTATION_FAILED:
        return REASON_ROTATION_FAILED
    return REASON_FAILURE
