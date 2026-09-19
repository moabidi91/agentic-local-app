"""``ConversationManager`` — the façade of the interfaces (spec §3.1 ; ADR-002, ADR-006, ADR-016,
ADR-018, ADR-022).

No protocol or execution logic lives here (§3.1): the manager creates the session and its first
conversation through the ``ConversationLifecycleManager``, hands the loop to the
``ProtocolOrchestrator`` as a background ``asyncio.Task``, forwards interruptions to the
``InterruptionHandler`` at once, and exposes the reads the API and the CLI need (records, the
``ExecutionTracker`` snapshot, the final answer, the model's direct responses and the last reply
of either kind (ADR-022, read back from the messages table), the correction requests sent to the
model (ADR-023, same source), why a session is paused (ADR-025), the running tasks, the recovery
report).

Loops are tracked per session (``loop_task``); their exceptions are never lost: the orchestrator
reflects them in the persisted state (session ``FAILED``), the manager logs them, and ``wait``
re-raises an unexpected one to its caller.

ADR-026 (open point 1) is closed here rather than in the orchestrator: the working space of a
session is **bound** by :meth:`ConversationManager.start_session` (the ``working_space`` the user
typed in the desktop front) and **released** when the loop of that session ends on a terminal state.
Both ends therefore live in the one class that owns the session's lifecycle from the outside, and
the release has a single call site — the loop's completion callback — instead of one per terminal
transition inside the orchestrator (``final_answer``, failure, budget), where the next one added
would be the one forgotten. It runs after the loop, hence after everything that matters is
persisted (ADR-026 §5), and a cleanup problem is logged, never raised: a session that finished its
work is not broken afterwards by a locked file.

ADR-028 adds one shape to :meth:`ConversationManager.start_session`: the **opening message is
optional**. Called without ``goal`` and ``user_message``, it creates the session and stops there —
``READY``, no conversation, no cycle, nothing posted to the model, no ``user_request`` persisted.
The session is then exactly a session an interruption left behind, and the first
``continue_session`` opens its first conversation the way a follow-up already does. Giving one of
the two without the other is a programming error (``ValueError``): the interfaces refuse it before
anything is created.

Entry points and the session states they accept (ADR-006 / ADR-007 session machine):

| call | accepted state | effect |
|---|---|---|
| ``start_session`` (with an opening message) | — | session ``READY → RUNNING``, conversation ``NEW → ACTIVE``, loop started |
| ``start_session`` (without one, ADR-028) | — | session stays ``READY``, no conversation, no loop |
| ``continue_session`` | ``COMPLETED`` (reusable, §11 — also under auto-close when the model asked a question, ADR-022) | ``COMPLETED → RUNNING``, follow-up in the same conversation |
| ``continue_session`` | ``READY`` (after an interruption, a restart, or an opening-less creation) | ``READY → RUNNING``, **new** child conversation (ADR-006 §3) |
| ``resume_session`` | ``RUNNING`` left resumable by the recovery (ADR-016) | the loop resumes with a GET first |
| ``resume_session`` | ``PAUSED`` on an authentication error, once a token was provided (ADR-025) | ``PAUSED → RUNNING``, the loop replays the refused POST or reads the reply it awaited |
| ``interrupt`` | any | delegated to the ``InterruptionHandler`` (idle sessions: nothing to do; a ``PAUSED`` session has nothing to drain and lands ``READY``) |
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
from agentic_local_app.execution.scratch import (
    REASON_BOUND_WORKING_SPACE,
    ScratchManager,
    validate_working_space,
)
from agentic_local_app.interruption.handler import InterruptionHandler, InterruptionReport
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker, RuntimeSnapshot
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.orchestration.protocol_orchestrator import (
    REASON_CREDENTIALS_PROVIDED,
    REASON_CREDENTIALS_REQUIRED,
    REASON_USER_REQUEST,
    ProtocolOrchestrator,
    pending_outbound_of,
)
from agentic_local_app.orchestration.recovery import RecoveryReport
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.messages import (
    ProtocolCorrectionRequestContent,
    UserResponseContent,
)

__all__ = ["SHUTDOWN_REASON", "ConversationManager"]

log = logging.getLogger(__name__)

#: ``reason`` of the interruption performed by :meth:`ConversationManager.shutdown`.
SHUTDOWN_REASON = "shutdown"

_REUSABLE_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {ConversationState.WAITING_USER, ConversationState.COMPLETED}
)

#: ADR-016 (a session the recovery left resumable) and ADR-025 (a session waiting for a token).
_RESUMABLE_SESSION_STATES: frozenset[SessionState] = frozenset(
    {SessionState.RUNNING, SessionState.PAUSED}
)

#: ADR-025: a conversation that has not sent anything yet — the session paused on the remote
#: ``init``, so there is no pending message to replay and the run starts again from the top.
_UNSENT_CONVERSATION_STATES: frozenset[ConversationState] = frozenset(
    {ConversationState.NEW, ConversationState.ACTIVE}
)

#: ADR-026 §5: the states on which the working space of a session is released.
_TERMINAL_SESSION_STATES: frozenset[SessionState] = frozenset(
    {SessionState.COMPLETED, SessionState.FAILED}
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
        scratch: ScratchManager | None = None,
        default_user_id: str | None = None,
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
        #: ADR-026: ``None`` means no working space management at all (nothing to bind or release).
        self._scratch = scratch
        #: ADR-028 §2: the ``user_id`` a session gets when the caller names none. The wiring passes
        #: the machine identity (``Application.identity``), which is what ``GET /whoami`` answers,
        #: so the two never contradict each other; ``transport.user_id`` is the last resort, the
        #: same one the identity resolution itself falls back on (ADR-024 §2).
        self._default_user_id = (default_user_id or "").strip() or config.transport.user_id
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

    @property
    def default_user_id(self) -> str:
        """ADR-028 §2: the ``user_id`` of a session created without one."""
        return self._default_user_id

    # ------------------------------------------------------------------ requests -----------
    async def start_session(
        self,
        *,
        goal: str | None = None,
        user_message: str | None = None,
        budget: SessionBudget | None = None,
        auto_close: bool | None = None,
        working_space: str | None = None,
        skills: list[str] | None = None,
        effort: str | None = None,
        user_id: str | None = None,
    ) -> SessionRecord:
        """Create the session, and open its first conversation when an opening message was given.

        With ``goal`` **and** ``user_message``: the session goes ``READY`` then ``RUNNING``, its
        first conversation ``NEW`` then ``ACTIVE``, the loop starts in the background and this
        returns at once — the behaviour of every release so far.

        With **neither** (ADR-028): the session is created and stays ``READY``, with no
        conversation, no cycle, nothing posted to the model and no ``user_request`` persisted. It
        is then in exactly the state an interruption leaves behind, so it is listable, resumable by
        the first :meth:`continue_session`, interruptible (nothing to drain) and untouched by the
        recovery, which only settles ``RUNNING`` / ``INTERRUPTING`` sessions (ADR-016 §2). Its
        ``goal`` is empty until that first message, which becomes it.

        With exactly **one** of the two: ``ValueError``. There is no such thing as a goal without a
        first message or a first message without a goal; the interfaces refuse the pair before
        anything is created (400 ``GOAL_REQUIRED`` / ``USER_MESSAGE_REQUIRED``).

        ``working_space`` (ADR-026 §3) is the folder the user designated — the *Working folder* of
        the desktop front. It is validated **before** anything is created, so a mistyped path costs
        no record, then bound to the session: its commands see it in their environment and no
        cleanup policy will ever touch it.

        ``skills`` and ``effort`` (ADR-027 §4) are the two other fields of that sign-in screen.
        They are **recorded and nothing more**: they reach the payload of ``session.created`` and
        stop there — no file is read, no instruction is derived, and the model receives exactly
        what it received before. Acting on them needs its own decision.

        ``user_id`` (ADR-028 §2) is who the session belongs to; blank or absent, it falls back to
        :attr:`default_user_id`, the machine identity ``GET /whoami`` reports.

        :raises ValueError: only one of ``goal`` / ``user_message`` was given.
        :raises ConfigError: ``WORKING_SPACE_INVALID`` when the path cannot be used as it is.
        """
        if (goal is None) != (user_message is None):
            missing = "user_message" if goal is not None else "goal"
            raise ValueError(f"an opening message needs both goal and user_message ({missing})")
        if working_space is not None:
            validate_working_space(working_space)
        effective_budget = budget or SessionBudget(
            max_cycles=self._config.budget.default_max_cycles,
            max_plans=self._config.budget.default_max_plans,
            max_total_duration_ms=self._config.budget.default_max_total_duration_ms,
        )
        effective_auto_close = (
            self._config.budget.auto_close_on_final_answer if auto_close is None else auto_close
        )
        session = self._lifecycle.create_session(
            goal or "",
            user_message or "",
            (user_id or "").strip() or self._default_user_id,
            effective_budget,
            effective_auto_close,
            skills=skills,
            effort=effort,
        )
        sid = session.session_id
        if working_space is not None and self._scratch is not None:
            self._scratch.bind(sid, working_space)
        if user_message is None:
            # ADR-028: nothing was said yet, so nothing is opened and nothing is sent. The working
            # space is already bound: the session owns it from the moment it exists.
            return self._require_session(sid)
        self._lifecycle.transition_session(sid, SessionState.RUNNING, reason=REASON_USER_REQUEST)
        conversation = self._lifecycle.create_conversation(sid)
        self._lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.ACTIVE, reason=REASON_USER_REQUEST
        )
        self._launch(sid, self._orchestrator.run_session(sid))
        return self._require_session(sid)

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord:
        """§11 follow-up on a reusable ``COMPLETED`` session, or a new request on a ``READY`` one
        (new child conversation, ADR-006 §3). ``ValueError`` otherwise, ``KeyError`` if unknown.

        ADR-028: this is also how a session created without an opening message starts. The
        ``READY`` branch already carried everything it needs — it opens a conversation whose parent
        is the session's current one, which is simply ``None`` here — so the first message of such a
        session takes exactly the path a message after an interruption takes. The one addition is
        that it **becomes the goal** of a session that has none yet: the goal is what the user
        wants, and until they have written a word there is nothing to record.
        """
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
            updates: dict[str, Any] = {"user_message": user_message, "final_answer": None}
            if not session.goal:
                updates["goal"] = user_message  # ADR-028: an opening-less session gets its goal
            self._lifecycle.transition_session(
                session_id,
                SessionState.RUNNING,
                reason=REASON_USER_REQUEST,
                **updates,
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
        """ADR-016: continue a ``RUNNING`` session the recovery left resumable (GET first).

        ADR-025: a ``PAUSED`` session resumes the same way once the user provided a token —
        ``PAUSED -> RUNNING`` (``credentials_provided``), then the loop replays the POST that was
        refused or reads the reply it was waiting for. A session paused on the remote ``init``, with
        nothing sent yet, starts its run again from the top instead: there is nothing to replay.
        """
        session = self._require_session(session_id)
        if self._loop_running(session_id):
            raise ValueError(f"session {session_id} is already running")
        if (
            session.status not in _RESUMABLE_SESSION_STATES
            or session.current_conversation_id is None
        ):
            raise ValueError(f"session {session_id} is not resumable ({session.status.value})")
        paused = session.status is SessionState.PAUSED
        conversation = self._store.get_conversation(session.current_conversation_id)
        pending = None if conversation is None else pending_outbound_of(self._store, conversation)
        if pending is None:
            if not paused or conversation is None:
                raise ValueError(f"session {session_id} has no pending outbound message to resume")
            if conversation.status not in _UNSENT_CONVERSATION_STATES:
                raise ValueError(f"session {session_id} has no pending outbound message to resume")
            self._resumed(session_id)
            self._launch(session_id, self._orchestrator.run_session(session_id))
            return self._require_session(session_id)
        if paused:
            self._resumed(session_id)
        self._launch(session_id, self._orchestrator.resume_session(session_id))
        return self._require_session(session_id)

    def paused_reason(self, session_id: str) -> dict[str, Any] | None:
        """ADR-025: why a ``PAUSED`` session is paused, for the interface that asks for a token.

        ``{reason, error_code, error_type, operation, since}`` read from the last ``FailureRecord``
        of the session — the one the pause was decided on — and from the session record itself;
        ``None`` for an unknown session and for a session that is not paused. The token is never
        part of it: nothing here ever names a secret.
        """
        session = self._store.get_session(session_id)
        if session is None or session.status is not SessionState.PAUSED:
            return None
        failures = self._store.list_failures(session_id)
        failure = failures[-1] if failures else None
        operation = failure.details.get("operation") if failure is not None else None
        return {
            "reason": REASON_CREDENTIALS_REQUIRED,
            "error_code": failure.error_code if failure is not None else None,
            "error_type": failure.error_type.value if failure is not None else None,
            "operation": operation if isinstance(operation, str) else None,
            "since": session.model_dump(mode="json")["updated_at"],
        }

    def _resumed(self, session_id: str) -> None:
        """ADR-025: ``PAUSED -> RUNNING`` before the loop is launched again."""
        self._lifecycle.transition_session(
            session_id, SessionState.RUNNING, reason=REASON_CREDENTIALS_PROVIDED
        )

    async def interrupt(self, session_id: str) -> InterruptionReport:
        """Forward the interrupt signal at once (§3.1); returns when the session is ``READY``."""
        return await self._interruption.interrupt(session_id)

    async def wait(self, session_id: str, *, timeout_ms: int | None = None) -> SessionRecord:
        """Wait for the loop of ``session_id`` to end; ``TimeoutError`` after ``timeout_ms``.

        An unexpected exception of the loop (already reflected as ``FAILED`` in the store) is
        re-raised here so that it is never lost silently. A loop that paused (ADR-025) ended
        cleanly: the ``PAUSED`` session is returned like any other.
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

    def corrections(self, session_id: str) -> list[dict[str, Any]]:
        """ADR-023: every ``protocol_correction_request`` of the session, oldest first, across all
        its conversations, read back from the messages table (no dedicated column, the schema is
        version 1). Each item: ``message_id``, ``conversation_id``, ``cycle_id``, ``created_at``,
        ``posted_at``, ``size_bytes`` and the content fields (``error_code``, ``errors``,
        ``expected_types``, ``reminder``, ``example``, ``attempt``, ``max_attempts``…). Empty for
        an unknown session, or for a session where the model never had to be corrected."""
        items: list[dict[str, Any]] = []
        for conversation in self._store.list_conversations(session_id):
            for message in self._store.list_messages(
                conversation.conversation_id, direction=MessageDirection.OUTBOUND
            ):
                if message.message_type is not MessageType.PROTOCOL_CORRECTION_REQUEST:
                    continue
                dumped = message.model_dump(mode="json")
                items.append(
                    {
                        "message_id": message.message_id,
                        "conversation_id": message.conversation_id,
                        "cycle_id": message.cycle_id,
                        "created_at": dumped["created_at"],
                        "posted_at": dumped["posted_at"],
                        "size_bytes": message.size_bytes,
                        **ProtocolCorrectionRequestContent.model_validate(
                            message.payload["content"]
                        ).model_dump(mode="json"),
                    }
                )
        return items

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
        task.add_done_callback(lambda done: self._loop_done(session_id, done))

    def _loop_running(self, session_id: str) -> bool:
        task = self._loops.get(session_id)
        return task is not None and not task.done()

    def _loop_done(self, session_id: str, task: asyncio.Task[SessionRecord]) -> None:
        if task.cancelled():
            log.warning("%s was cancelled", task.get_name())
            return
        exc = task.exception()
        if exc is not None:
            log.error("%s ended with an unexpected error: %r", task.get_name(), exc)
        self._release_working_space(session_id)

    def _release_working_space(self, session_id: str) -> None:
        """ADR-026 §5: apply the ``[scratch]`` policy once the session is done, never fail on it.

        A session that only paused (ADR-025) or was interrupted keeps its folder: it is not over.
        ``ScratchManager.release`` reports filesystem problems instead of raising, but the guard is
        kept anyway — a cleanup must never be the reason a finished session breaks.
        """
        if self._scratch is None:
            return
        session = self._store.get_session(session_id)
        if session is None or session.status not in _TERMINAL_SESSION_STATES:
            return
        try:
            outcome = self._scratch.release(
                session_id, failed=session.status is SessionState.FAILED
            )
            if outcome.reason == REASON_BOUND_WORKING_SPACE and outcome.path is not None:
                # A folder the user owns was not touched and is not the run's, it is the
                # session's: releasing it only stopped tracking it. Bind it again so that a
                # follow-up (§11) writes where the user asked instead of in a generated folder.
                self._scratch.bind(session_id, outcome.path)
        except Exception as exc:  # pragma: no cover - release is documented never to raise
            log.warning("working space of %s could not be released: %r", session_id, exc)
            return
        if outcome.error is not None:
            log.warning("working space of %s: %s (%s)", session_id, outcome.error, outcome.reason)

    def _require_session(self, session_id: str) -> SessionRecord:
        session = self._store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session
