"""``ConversationManager`` — the façade of the interfaces (spec §3.1 ; ADR-002, ADR-006, ADR-016,
ADR-018, ADR-022).

No protocol or execution logic lives here (§3.1): the manager creates the session and its first
conversation through the ``ConversationLifecycleManager``, hands the loop to the
``ProtocolOrchestrator`` as a background ``asyncio.Task``, forwards interruptions to the
``InterruptionHandler`` at once, and exposes the reads the API and the CLI need (records, the
``ExecutionTracker`` snapshot, the final answer, the model's direct responses and the last reply
of either kind (ADR-022, read back from the messages table), the running tasks, the recovery
report).

Loops are tracked per session (``loop_task``); their exceptions are never lost: the orchestrator
reflects them in the persisted state (session ``FAILED``), the manager logs them, and ``wait``
re-raises an unexpected one to its caller.

Entry points and the session states they accept (ADR-006 / ADR-007 session machine):

| call | accepted state | effect |
|---|---|---|
| ``start_session`` | — | session ``READY → RUNNING``, conversation ``NEW → ACTIVE``, loop started |
| ``continue_session`` | ``COMPLETED`` (reusable, §11 — also under auto-close when the model asked a question, ADR-022) | ``COMPLETED → RUNNING``, follow-up in the same conversation |
| ``continue_session`` | ``READY`` (after an interruption or a restart) | ``READY → RUNNING``, **new** child conversation (ADR-006 §3) |
| ``resume_session`` | ``RUNNING`` left resumable by the recovery (ADR-016) | the loop resumes with a GET first |
| ``interrupt`` | any | delegated to the ``InterruptionHandler`` (idle sessions: nothing to do) |
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Coroutine, Iterable
from typing import Any

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import MessageRecord, SessionBudget, SessionRecord
from agentic_local_app.domain.states import (
    CONCLUDING_MESSAGE_TYPES,
    ConversationState,
    MessageDirection,
    MessageType,
    SessionState,
    TaskState,
)
from agentic_local_app.interruption.handler import InterruptionHandler, InterruptionReport
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker, RuntimeSnapshot
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.orchestration.protocol_orchestrator import (
    REASON_USER_REQUEST,
    ProtocolOrchestrator,
    pending_outbound_of,
)
from agentic_local_app.orchestration.recovery import RecoveryReport
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.messages import UserResponseContent

__all__ = ["SHUTDOWN_REASON", "ConversationManager"]

log = logging.getLogger(__name__)

#: ``reason`` of the interruption performed by :meth:`ConversationManager.shutdown`.
SHUTDOWN_REASON = "shutdown"

_REUSABLE_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {ConversationState.WAITING_USER, ConversationState.COMPLETED}
)


class ConversationManager:
    """Entry point of user requests and interruptions (§3.1)."""

    def __init__(
        self,
        *,
        config: AppConfig,
        store: ConversationStore,
        bus: EventBus,
        clock: Clock,
        ids: IdGenerator,
        lifecycle: ConversationLifecycleManager,
        orchestrator: ProtocolOrchestrator,
        interruption: InterruptionHandler,
        tracker: ExecutionTracker,
        audit: AuditLog,
        telemetry: TelemetryService,
        recovery_report: RecoveryReport | None = None,
    ) -> None:
        self._config = config
        self._store = store
        self._bus = bus
        self._clock = clock
        self._ids = ids
        self._lifecycle = lifecycle
        self._orchestrator = orchestrator
        self._interruption = interruption
        self._tracker = tracker
        self._audit = audit
        self._telemetry = telemetry
        self._recovery_report = recovery_report
        self._loops: dict[str, asyncio.Task[SessionRecord]] = {}

    # ------------------------------------------------------------------ properties ---------
    @property
    def config(self) -> AppConfig:
        return self._config

    @property
    def store(self) -> ConversationStore:
        return self._store

    @property
    def bus(self) -> EventBus:
        return self._bus

    @property
    def clock(self) -> Clock:
        return self._clock

    @property
    def tracker(self) -> ExecutionTracker:
        return self._tracker

    @property
    def audit(self) -> AuditLog:
        return self._audit

    @property
    def telemetry(self) -> TelemetryService:
        return self._telemetry

    @property
    def recovery_report(self) -> RecoveryReport | None:
        return self._recovery_report

    @property
    def interruption(self) -> InterruptionHandler:
        return self._interruption

    # ------------------------------------------------------------------ requests -----------
    async def start_session(
        self,
        *,
        goal: str,
        user_message: str,
        budget: SessionBudget | None = None,
        auto_close: bool | None = None,
    ) -> SessionRecord:
        """Create the session (``READY`` then ``RUNNING``) and its first conversation (``NEW`` then
        ``ACTIVE``), start the loop in the background and return at once."""
        effective_budget = budget or SessionBudget(
            max_cycles=self._config.budget.default_max_cycles,
            max_plans=self._config.budget.default_max_plans,
            max_total_duration_ms=self._config.budget.default_max_total_duration_ms,
        )
        effective_auto_close = (
            self._config.budget.auto_close_on_final_answer if auto_close is None else auto_close
        )
        session = self._lifecycle.create_session(
            goal,
            user_message,
            self._config.transport.user_id,
            effective_budget,
            effective_auto_close,
        )
        sid = session.session_id
        self._lifecycle.transition_session(sid, SessionState.RUNNING, reason=REASON_USER_REQUEST)
        conversation = self._lifecycle.create_conversation(sid)
        self._lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.ACTIVE, reason=REASON_USER_REQUEST
        )
        self._launch(sid, self._orchestrator.run_session(sid))
        return self._require_session(sid)

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord:
        """§11 follow-up on a reusable ``COMPLETED`` session, or a new request on a ``READY`` one
        (new child conversation, ADR-006 §3). ``ValueError`` otherwise, ``KeyError`` if unknown."""
        session = self._require_session(session_id)
        if self._loop_running(session_id):
            raise ValueError(f"session {session_id} is still running")
        if session.status is SessionState.COMPLETED:
            conversation = (
                self._store.get_conversation(session.current_conversation_id)
                if session.current_conversation_id is not None
                else None
            )
            if conversation is None or conversation.status not in _REUSABLE_CONVERSATION_STATES:
                # an auto-close session keeps its conversation open only for a question (ADR-022)
                if session.auto_close_on_final_answer:
                    raise ValueError(f"session {session_id} was closed after its final answer")
                raise ValueError(f"session {session_id} has no reusable conversation")
            self._lifecycle.transition_session(
                session_id, SessionState.RUNNING, reason=REASON_USER_REQUEST, final_answer=None
            )
            self._launch(session_id, self._orchestrator.continue_session(session_id, user_message))
            return self._require_session(session_id)
        if session.status is SessionState.READY:
            self._lifecycle.transition_session(
                session_id,
                SessionState.RUNNING,
                reason=REASON_USER_REQUEST,
                user_message=user_message,
                final_answer=None,
            )
            conversation = self._lifecycle.create_conversation(
                session_id, parent_conversation_id=session.current_conversation_id
            )
            self._lifecycle.transition_conversation(
                conversation.conversation_id, ConversationState.ACTIVE, reason=REASON_USER_REQUEST
            )
            self._launch(
                session_id, self._orchestrator.run_session(session_id, user_message=user_message)
            )
            return self._require_session(session_id)
        raise ValueError(f"session {session_id} is not reusable ({session.status.value})")

    async def resume_session(self, session_id: str) -> SessionRecord:
        """ADR-016: continue a ``RUNNING`` session the recovery left resumable (GET first)."""
        session = self._require_session(session_id)
        if self._loop_running(session_id):
            raise ValueError(f"session {session_id} is already running")
        if session.status is not SessionState.RUNNING or session.current_conversation_id is None:
            raise ValueError(f"session {session_id} is not resumable ({session.status.value})")
        conversation = self._store.get_conversation(session.current_conversation_id)
        if conversation is None or pending_outbound_of(self._store, conversation) is None:
            raise ValueError(f"session {session_id} has no pending outbound message to resume")
        self._launch(session_id, self._orchestrator.resume_session(session_id))
        return self._require_session(session_id)

    async def interrupt(self, session_id: str) -> InterruptionReport:
        """Forward the interrupt signal at once (§3.1); returns when the session is ``READY``."""
        return await self._interruption.interrupt(session_id)

    async def wait(self, session_id: str, *, timeout_ms: int | None = None) -> SessionRecord:
        """Wait for the loop of ``session_id`` to end; ``TimeoutError`` after ``timeout_ms``.

        An unexpected exception of the loop (already reflected as ``FAILED`` in the store) is
        re-raised here so that it is never lost silently.
        """
        self._require_session(session_id)
        task = self._loops.get(session_id)
        if task is None:
            return self._require_session(session_id)
        timeout = None if timeout_ms is None else timeout_ms / 1000
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout)
        except TimeoutError:
            raise TimeoutError(
                f"session {session_id} still running after {timeout_ms} ms"
            ) from None
        except asyncio.CancelledError:
            if task.cancelled():
                return self._require_session(session_id)
            raise
        return self._require_session(session_id)

    # ------------------------------------------------------------------ reads --------------
    def get_session(self, session_id: str) -> SessionRecord | None:
        return self._store.get_session(session_id)

    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]:
        return self._store.list_sessions(statuses=statuses, limit=limit, offset=offset)

    def snapshot(self, session_id: str) -> RuntimeSnapshot:
        """The §4 snapshot (``KeyError`` for an unknown session)."""
        return self._tracker.snapshot(session_id)

    def final_answer(self, session_id: str) -> dict[str, Any] | None:
        session = self._store.get_session(session_id)
        return None if session is None else session.final_answer

    def user_responses(self, session_id: str) -> list[dict[str, Any]]:
        """ADR-022: every valid ``user_response`` of the session, oldest first, across all its
        conversations, read back from the messages table (no dedicated column). Each item:
        ``message_id``, ``conversation_id``, ``cycle_id``, ``received_at`` and the content fields
        ``format``, ``body``, ``status``, ``expects_reply``. Empty for an unknown session."""
        return [
            {
                **self._reply_head(message),
                **UserResponseContent.model_validate(message.payload["content"]).model_dump(),
            }
            for message in self._concluding_messages(session_id)
            if message.message_type is MessageType.USER_RESPONSE
        ]

    def last_reply(self, session_id: str) -> dict[str, Any] | None:
        """ADR-022: the newest concluding reply of the model — a ``final_answer`` or a
        ``user_response`` — as ``{type, message_id, conversation_id, cycle_id, received_at,
        content}``, or ``None`` when the model has not concluded a turn yet."""
        messages = self._concluding_messages(session_id)
        if not messages:
            return None
        last = messages[-1]
        return {
            "type": last.message_type.value,
            **self._reply_head(last),
            "content": dict(last.payload.get("content", {})),
        }

    def _concluding_messages(self, session_id: str) -> list[MessageRecord]:
        """Valid inbound ``final_answer`` / ``user_response`` records, oldest first."""
        return [
            message
            for conversation in self._store.list_conversations(session_id)
            for message in self._store.list_messages(
                conversation.conversation_id, direction=MessageDirection.INBOUND
            )
            if message.message_type in CONCLUDING_MESSAGE_TYPES
            and message.validation_status == "valid"
        ]

    @staticmethod
    def _reply_head(message: MessageRecord) -> dict[str, Any]:
        """The identifiers and the timestamp of a reply, rendered as the API renders records."""
        dumped = message.model_dump(mode="json")
        return {
            "message_id": message.message_id,
            "conversation_id": message.conversation_id,
            "cycle_id": message.cycle_id,
            "received_at": dumped["received_at"],
        }

    def running_task_ids(self, session_id: str) -> list[str]:
        return [
            task.task_id
            for task in self._store.list_tasks(session_id, statuses=[TaskState.RUNNING])
        ]

    def loop_task(self, session_id: str) -> asyncio.Task[SessionRecord] | None:
        """The background loop of ``session_id`` (finished or not), ``None`` if none was started."""
        return self._loops.get(session_id)

    # ------------------------------------------------------------------ shutdown -----------
    async def shutdown(self) -> None:
        """Interrupt every running session properly (reason ``shutdown``) and wait for its loop."""
        drain_s = self._config.execution.interrupt_drain_timeout_ms / 1000 + 1.0
        for session_id, task in list(self._loops.items()):
            if task.done():
                continue
            session = self._store.get_session(session_id)
            if session is not None and session.status in (
                SessionState.RUNNING,
                SessionState.INTERRUPTING,
            ):
                with contextlib.suppress(Exception):
                    await self._interruption.interrupt(session_id, reason=SHUTDOWN_REASON)
            if task.done():
                continue
            _, pending = await asyncio.wait({task}, timeout=drain_s)
            if pending:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    # ------------------------------------------------------------------ internals ----------
    def _launch(self, session_id: str, coroutine: Coroutine[Any, Any, SessionRecord]) -> None:
        task = asyncio.create_task(coroutine, name=f"protocol-loop:{session_id}")
        self._loops[session_id] = task
        task.add_done_callback(self._loop_done)

    def _loop_running(self, session_id: str) -> bool:
        task = self._loops.get(session_id)
        return task is not None and not task.done()

    @staticmethod
    def _loop_done(task: asyncio.Task[SessionRecord]) -> None:
        if task.cancelled():
            log.warning("%s was cancelled", task.get_name())
            return
        exc = task.exception()
        if exc is not None:
            log.error("%s ended with an unexpected error: %r", task.get_name(), exc)

    def _require_session(self, session_id: str) -> SessionRecord:
        session = self._store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session
