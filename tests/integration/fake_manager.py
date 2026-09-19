"""``FakeConversationManager`` — the test double of the phase 9a façade (``ConversationManager``).

It implements the ``ConversationManagerLike`` protocol of ``interfaces/http_api.py`` **faithfully**
on top of real components: an ``InMemoryConversationStore``, an ``EventBus`` with the ADR-015
subscribers (``AuditLog`` critical → ``ExecutionTracker`` → ``TelemetryService``), the real
``ConversationLifecycleManager`` for every session / conversation transition and the real
``InterruptionHandler`` for ``interrupt``. There is no protocol loop: the tests drive the activity
through the helpers of §"simulation" (publish events, insert plans / tasks / blobs / messages /
failures, move states), so the interfaces are tested against the exact records and events the
orchestrator will produce, without the orchestrator.

Deterministic by construction: ``FakeClock`` and ``SequentialIdGenerator`` (ADR-017).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Iterable
from typing import Any

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType, Severity
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    BlobRecord,
    ConversationRecord,
    CycleRecord,
    FailureRecord,
    MessageRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
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
from agentic_local_app.execution.scratch import validate_working_space
from agentic_local_app.interruption.handler import InterruptionHandler, InterruptionReport
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker, RuntimeSnapshot
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.orchestration.protocol_orchestrator import REASON_CREDENTIALS_PROVIDED
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.messages import (
    ProtocolCorrectionRequestContent,
    UserResponseContent,
)
from agentic_local_app.resilience.failure_manager import CREDENTIALS_REQUIRED_REASON

__all__ = ["FakeConversationManager"]

_TERMINAL_SESSION_STATES = frozenset({SessionState.COMPLETED, SessionState.FAILED})
_REUSABLE_CONVERSATION_STATES = frozenset(
    {ConversationState.WAITING_USER, ConversationState.COMPLETED}
)
_CONCLUDING_MESSAGE_TYPES = frozenset({MessageType.FINAL_ANSWER, MessageType.USER_RESPONSE})


class FakeConversationManager:
    """The façade double. Public attributes mirror the protocol; ``calls`` records every call."""

    def __init__(
        self,
        config: AppConfig | None = None,
        *,
        clock: FakeClock | None = None,
        ids: SequentialIdGenerator | None = None,
        store: InMemoryConversationStore | None = None,
        bus: EventBus | None = None,
        telemetry_enabled: bool = True,
        default_user_id: str | None = None,
    ) -> None:
        self.config = config or AppConfig()
        self.clock = clock or FakeClock()
        self.ids = ids or SequentialIdGenerator()
        self.store = store or InMemoryConversationStore()
        self.bus = bus or EventBus()
        # ADR-015 order: audit (critical) -> tracker -> telemetry
        self.audit = AuditLog(self.store, self.clock, self.ids)
        self.audit.subscribe(self.bus)
        self.tracker = ExecutionTracker(self.store, self.clock)
        self.tracker.subscribe(self.bus)
        self.telemetry = TelemetryService(self.clock)
        if telemetry_enabled:
            self.telemetry.subscribe(self.bus)
        self.lifecycle = ConversationLifecycleManager(self.store, self.bus, self.clock, self.ids)
        self.interruption = InterruptionHandler(
            self.store, self.bus, self.lifecycle, self.clock, self.ids, self.config
        )
        self.recovery_report: Any | None = None
        #: ADR-028 §2: the ``user_id`` of a session created without one, as the façade keeps it.
        self.default_user_id = (default_user_id or "").strip() or self.config.transport.user_id
        #: ADR-026: the ``working_space`` bound to a session by ``start_session``, if any.
        self.working_spaces: dict[str, str] = {}
        #: ADR-027 §4: the ``skills`` / ``effort`` of ``start_session``, per session — recorded
        #: here exactly as the façade records them, in the ``session.created`` event and nowhere
        #: else; nothing in the double reads them back.
        self.session_extras: dict[str, tuple[list[str] | None, str | None]] = {}
        # ---- test knobs ------------------------------------------------------------------
        #: every façade call, in order: ``(method, *args)``.
        self.calls: list[tuple[Any, ...]] = []
        #: run right after ``start_session`` created and started the session (e.g. ``complete``).
        self.on_start: Callable[[FakeConversationManager, str], None] | None = None
        #: raised (once) by the next ``wait`` — the CLI tests inject a ``KeyboardInterrupt`` here.
        self.wait_raises: deque[BaseException] = deque()
        #: run by successive ``wait`` calls (one per call) to make the session progress.
        self.wait_script: deque[Callable[[FakeConversationManager, str], None]] = deque()
        #: façade contract: ``wait`` raises ``TimeoutError`` while the session is still RUNNING.
        self.wait_times_out: bool = True
        #: raised by the next ``start_session`` (uniform error handling tests).
        self.start_raises: BaseException | None = None
        #: raised by the next ``interrupt`` (a second Ctrl-C while the interruption runs).
        self.interrupt_raises: BaseException | None = None
        self.shutdown_called = False

    # ==================================================================== façade =========
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
        self.calls.append(("start_session", goal, user_message, budget, auto_close))
        if self.start_raises is not None:
            exc, self.start_raises = self.start_raises, None
            raise exc
        # ADR-028 §1, same rule as the façade: the opening message is a pair, all or nothing.
        if (goal is None) != (user_message is None):
            missing = "user_message" if goal is not None else "goal"
            raise ValueError(f"an opening message needs both goal and user_message ({missing})")
        if working_space is not None:
            # ADR-026 §3, same order as the façade: a path that cannot be used is refused before
            # anything is created, so a mistyped working space costs no record.
            validate_working_space(working_space)
        effective_budget = budget or SessionBudget(
            max_cycles=self.config.budget.default_max_cycles,
            max_plans=self.config.budget.default_max_plans,
            max_total_duration_ms=self.config.budget.default_max_total_duration_ms,
        )
        effective_auto_close = (
            self.config.budget.auto_close_on_final_answer if auto_close is None else auto_close
        )
        session = self.lifecycle.create_session(
            goal or "",
            user_message or "",
            (user_id or "").strip() or self.default_user_id,
            effective_budget,
            effective_auto_close,
            skills=skills,
            effort=effort,
        )
        sid = session.session_id
        if working_space is not None:
            self.working_spaces[sid] = working_space
        self.session_extras[sid] = (skills, effort)
        if user_message is None:
            # ADR-028: no conversation, no cycle, nothing posted — the session waits, READY.
            return self.require_session(sid)
        self.lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
        conversation = self.lifecycle.create_conversation(sid)
        self.lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.ACTIVE
        )
        self.lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
        )
        if self.on_start is not None:
            self.on_start(self, sid)
        return self.require_session(sid)

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord:
        self.calls.append(("continue_session", session_id, user_message))
        session = self.require_session(session_id)
        conversation = self.current_conversation(session_id)
        if session.status is SessionState.READY:
            # ADR-006 §3 / ADR-028, same rule as the façade: a new child conversation in the same
            # session — after an interruption, after a restart, or on the first message of a
            # session that was created without one (its parent is then simply None, and that first
            # message becomes the goal the creation had nothing to record).
            updates: dict[str, Any] = {"user_message": user_message, "final_answer": None}
            if not session.goal:
                updates["goal"] = user_message
            self.lifecycle.transition_session(
                session_id, SessionState.RUNNING, reason="user_request", **updates
            )
            child = self.lifecycle.create_conversation(
                session_id, parent_conversation_id=session.current_conversation_id
            )
            self.lifecycle.transition_conversation(
                child.conversation_id, ConversationState.ACTIVE, reason="user_request"
            )
            self.lifecycle.transition_conversation(
                child.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
            )
            return self.require_session(session_id)
        # the same rule as the façade: a COMPLETED session whose conversation is reusable — under
        # auto-close only when the model asked a question and left it WAITING_USER (ADR-022)
        if (
            session.status is not SessionState.COMPLETED
            or conversation is None
            or conversation.status not in _REUSABLE_CONVERSATION_STATES
            or (
                session.auto_close_on_final_answer
                and conversation.status is not ConversationState.WAITING_USER
            )
        ):
            raise ValueError(f"session {session_id} is not reusable ({session.status.value})")
        self.lifecycle.transition_session(session_id, SessionState.RUNNING, reason="user_request")
        self.lifecycle.update_session(session_id, user_message=user_message, final_answer=None)
        if conversation.status is ConversationState.COMPLETED:
            self.lifecycle.transition_conversation(
                conversation.conversation_id, ConversationState.WAITING_USER, reason="user_request"
            )
        self.lifecycle.transition_conversation(
            conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
        )
        return self.require_session(session_id)

    async def resume_session(self, session_id: str) -> SessionRecord:
        """ADR-025 §5, same contract as the façade: a ``PAUSED`` session goes back to ``RUNNING``
        (``credentials_provided``); anything else is a ``ValueError``."""
        self.calls.append(("resume_session", session_id))
        session = self.require_session(session_id)
        if session.status is not SessionState.PAUSED:
            raise ValueError(f"session {session_id} is not resumable ({session.status.value})")
        return self.lifecycle.transition_session(
            session_id, SessionState.RUNNING, reason=REASON_CREDENTIALS_PROVIDED
        )

    async def interrupt(self, session_id: str) -> InterruptionReport:
        self.calls.append(("interrupt", session_id))
        if self.interrupt_raises is not None:
            exc, self.interrupt_raises = self.interrupt_raises, None
            raise exc
        return await self.interruption.interrupt(session_id)

    async def wait(self, session_id: str, *, timeout_ms: int | None = None) -> SessionRecord:
        self.calls.append(("wait", session_id, timeout_ms))
        if self.wait_raises:
            raise self.wait_raises.popleft()
        if self.wait_script:
            self.wait_script.popleft()(self, session_id)
        await asyncio.sleep(0)
        session = self.require_session(session_id)
        if (
            timeout_ms is not None
            and session.status is SessionState.RUNNING
            and self.wait_times_out
        ):
            raise TimeoutError(f"session {session_id} still running after {timeout_ms} ms")
        return session

    def get_session(self, session_id: str) -> SessionRecord | None:
        return self.store.get_session(session_id)

    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]:
        return self.store.list_sessions(statuses=statuses, limit=limit, offset=offset)

    def snapshot(self, session_id: str) -> RuntimeSnapshot:
        return self.tracker.snapshot(session_id)

    def final_answer(self, session_id: str) -> dict[str, Any] | None:
        session = self.store.get_session(session_id)
        return None if session is None else session.final_answer

    def user_responses(self, session_id: str) -> list[dict[str, Any]]:
        """ADR-022, same contract as the façade: the valid ``user_response`` messages, oldest
        first, identifiers and timestamp plus the content fields (defaults filled)."""
        return [
            {
                **self._reply_head(m),
                **UserResponseContent.model_validate(m.payload["content"]).model_dump(),
            }
            for m in self._concluding_messages(session_id)
            if m.message_type is MessageType.USER_RESPONSE
        ]

    def last_reply(self, session_id: str) -> dict[str, Any] | None:
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
        """ADR-023, same contract as the façade: the ``protocol_correction_request`` messages sent
        to the model, oldest first, identifiers and sizes plus the content fields."""
        items: list[dict[str, Any]] = []
        for conversation in self.store.list_conversations(session_id):
            for message in self.store.list_messages(
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

    def paused_reason(self, session_id: str) -> dict[str, Any] | None:
        """ADR-025, same contract as the façade: why a ``PAUSED`` session waits for a token —
        ``{reason, error_code, error_type, operation, since}`` — or ``None``."""
        session = self.store.get_session(session_id)
        if session is None or session.status is not SessionState.PAUSED:
            return None
        failures = self.store.list_failures(session_id)
        failure = failures[-1] if failures else None
        operation = failure.details.get("operation") if failure is not None else None
        return {
            "reason": CREDENTIALS_REQUIRED_REASON,
            "error_code": failure.error_code if failure is not None else None,
            "error_type": failure.error_type.value if failure is not None else None,
            "operation": operation if isinstance(operation, str) else None,
            "since": session.model_dump(mode="json")["updated_at"],
        }

    def _concluding_messages(self, session_id: str) -> list[MessageRecord]:
        return [
            m
            for c in self.store.list_conversations(session_id)
            for m in self.store.list_messages(c.conversation_id, direction=MessageDirection.INBOUND)
            if m.message_type in _CONCLUDING_MESSAGE_TYPES and m.validation_status == "valid"
        ]

    @staticmethod
    def _reply_head(message: MessageRecord) -> dict[str, Any]:
        return {
            "message_id": message.message_id,
            "conversation_id": message.conversation_id,
            "cycle_id": message.cycle_id,
            "received_at": message.model_dump(mode="json")["received_at"],
        }

    def running_task_ids(self, session_id: str) -> list[str]:
        return [
            task.task_id for task in self.store.list_tasks(session_id, statuses=[TaskState.RUNNING])
        ]

    async def shutdown(self) -> None:
        self.calls.append(("shutdown",))
        self.shutdown_called = True

    # ==================================================================== reads ==========
    def require_session(self, session_id: str) -> SessionRecord:
        session = self.store.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session

    def current_conversation(self, session_id: str) -> ConversationRecord | None:
        session = self.require_session(session_id)
        if session.current_conversation_id is None:
            return None
        return self.store.get_conversation(session.current_conversation_id)

    def require_conversation(self, session_id: str) -> ConversationRecord:
        conversation = self.current_conversation(session_id)
        if conversation is None:
            raise KeyError(f"session {session_id} has no current conversation")
        return conversation

    # ==================================================================== simulation =====
    def publish(
        self,
        event_type: EventType,
        session_id: str,
        *,
        conversation_id: str | None = None,
        cycle_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Event:
        """Publish one event stamped with the fake clock (persist-before-publish is the caller's job)."""
        event = Event(
            event_type=event_type,
            timestamp=self.clock.now(),
            session_id=session_id,
            conversation_id=conversation_id,
            cycle_id=cycle_id,
            plan_id=plan_id,
            task_id=task_id,
            payload=payload or {},
        )
        self.bus.publish(event)
        return event

    def add_cycle(
        self, session_id: str, *, cycle_type: CycleType = CycleType.EXECUTION
    ) -> CycleRecord:
        """A RUNNING cycle of the current conversation, pointed to by ``current_cycle_id``."""
        conversation = self.require_conversation(session_id)
        cycle = CycleRecord(
            cycle_id=self.ids.cycle_id(),
            conversation_id=conversation.conversation_id,
            session_id=session_id,
            cycle_type=cycle_type,
            status=CycleState.RUNNING,
            started_at=self.clock.now(),
        )
        self.store.save_cycle(cycle)
        self.lifecycle.update_conversation(
            conversation.conversation_id, current_cycle_id=cycle.cycle_id
        )
        self.publish(
            EventType.CYCLE_STARTED,
            session_id,
            conversation_id=conversation.conversation_id,
            cycle_id=cycle.cycle_id,
            payload={
                "cycle_type": cycle_type.value,
                "outbound_message_type": "user_request",
                "consumed_cycles": 1,
            },
        )
        return cycle

    def add_plan(
        self,
        session_id: str,
        plan_id: str,
        tasks: list[dict[str, Any]],
        *,
        plan_type: PlanType = PlanType.EXECUTION_PLAN,
        policy: ExecutionPolicy = ExecutionPolicy.SEQUENTIAL,
        status: PlanState = PlanState.PENDING,
        objective: str = "objective",
    ) -> tuple[PlanRecord, list[TaskRecord]]:
        """A plan and its tasks persisted (PENDING by default), the conversation pointing to it,
        then ``plan.received`` published — the contract of the phase 10 guide.

        Each task dict: ``task_id`` (required) and optional ``cmd``, ``status``, ``critical``,
        ``continue_on_error``, ``stop_plan_on_failure``, ``stop_plan_on_success``, ``depends_on``,
        ``resource_lock``, ``max_output_bytes``.
        """
        conversation = self.require_conversation(session_id)
        cycle_id = conversation.current_cycle_id or self.add_cycle(session_id).cycle_id
        now = self.clock.now()
        plan = PlanRecord(
            plan_id=plan_id,
            session_id=session_id,
            conversation_id=conversation.conversation_id,
            cycle_id=cycle_id,
            plan_type=plan_type,
            objective=objective,
            execution_policy=policy,
            max_parallel_workers=1 if policy is ExecutionPolicy.SEQUENTIAL else 2,
            status=status,
            task_count=len(tasks),
            started_at=now if status is not PlanState.PENDING else None,
            created_at=now,
            updated_at=now,
        )
        records: list[TaskRecord] = []
        for index, spec in enumerate(tasks):
            fields = dict(spec)
            task_id = fields.pop("task_id")
            task_status = TaskState(fields.pop("status", TaskState.PENDING))
            depends_on = tuple(fields.pop("depends_on", ()))
            records.append(
                TaskRecord(
                    task_id=task_id,
                    plan_id=plan_id,
                    session_id=session_id,
                    conversation_id=conversation.conversation_id,
                    order_index=index,
                    type=TaskType.CMD,
                    cmd=fields.pop("cmd", f"run {task_id}"),
                    status=task_status,
                    depends_on=depends_on,
                    started_at=now if task_status is TaskState.RUNNING else None,
                    attempt_count=1 if task_status is TaskState.RUNNING else 0,
                    created_at=now,
                    updated_at=now,
                    **fields,
                )
            )
        with self.store.transaction():
            self.store.save_plan(plan)
            self.store.save_tasks(records)
        self.lifecycle.update_conversation(conversation.conversation_id, current_plan_id=plan_id)
        self.publish(
            EventType.PLAN_RECEIVED,
            session_id,
            conversation_id=conversation.conversation_id,
            cycle_id=cycle_id,
            plan_id=plan_id,
            payload={
                "plan_type": plan_type.value,
                "execution_policy": policy.value,
                "task_count": len(tasks),
                "max_parallel_workers": plan.max_parallel_workers,
                "consumed_plans": 1,
            },
        )
        return plan, records

    def set_task_state(
        self, session_id: str, task_id: str, state: TaskState, **fields: Any
    ) -> TaskRecord:
        """Persist the new task state (plus ``fields``) then publish ``task.state_changed``."""
        task = self.store.get_task(session_id, task_id)
        if task is None:
            raise KeyError(f"unknown task: {task_id}")
        now = self.clock.now()
        changes: dict[str, Any] = {"status": state, "updated_at": now, **fields}
        if state is TaskState.RUNNING:
            changes.setdefault("started_at", now)
            changes.setdefault("attempt_count", task.attempt_count + 1)
        record = task.model_copy(update=changes)
        self.store.save_task(record)
        payload = state_change_payload(task.status.value, state.value, fields.get("reason"))
        if record.duration_ms is not None:
            payload["duration_ms"] = record.duration_ms
        if record.exit_code is not None:
            payload["exit_code"] = record.exit_code
        self.publish(
            EventType.TASK_STATE_CHANGED,
            session_id,
            conversation_id=record.conversation_id,
            plan_id=record.plan_id,
            task_id=task_id,
            payload=payload,
        )
        return record

    def set_plan_state(self, session_id: str, plan_id: str, state: PlanState) -> PlanRecord:
        plan = self.store.get_plan(session_id, plan_id)
        if plan is None:
            raise KeyError(f"unknown plan: {plan_id}")
        now = self.clock.now()
        record = plan.model_copy(update={"status": state, "updated_at": now, "started_at": now})
        self.store.save_plan(record)
        self.publish(
            EventType.PLAN_STATE_CHANGED,
            session_id,
            conversation_id=plan.conversation_id,
            cycle_id=plan.cycle_id,
            plan_id=plan_id,
            payload=state_change_payload(plan.status.value, state.value),
        )
        return record

    def emit_output(
        self,
        session_id: str,
        task_id: str,
        data: str,
        *,
        stream: OutputStream = OutputStream.STDOUT,
        offset: int = 0,
    ) -> Event:
        """A live ``task.output`` chunk (not audited, ADR-018) for ``task_id``."""
        task = self.store.get_task(session_id, task_id)
        return self.publish(
            EventType.TASK_OUTPUT,
            session_id,
            conversation_id=task.conversation_id if task else None,
            plan_id=task.plan_id if task else None,
            task_id=task_id,
            payload={"stream": stream.value, "offset": offset, "size": len(data), "data": data},
        )

    def add_blob(
        self,
        session_id: str,
        task_id: str,
        content: bytes,
        *,
        stream: OutputStream = OutputStream.STDOUT,
    ) -> BlobRecord:
        blob = BlobRecord(
            blob_id=self.ids.blob_id(),
            session_id=session_id,
            task_id=task_id,
            blob_type=stream,
            content=content,
            size_bytes=len(content),
            created_at=self.clock.now(),
        )
        self.store.save_blob(blob)
        task = self.store.get_task(session_id, task_id)
        if task is not None:
            ref = "stdout_ref" if stream is OutputStream.STDOUT else "stderr_ref"
            self.store.save_task(task.model_copy(update={ref: blob.blob_id}))
        return blob

    def add_message(
        self,
        session_id: str,
        *,
        direction: MessageDirection,
        message_type: MessageType,
        payload: dict[str, Any] | None = None,
        conversation_id: str | None = None,
        message_id: str | None = None,
        cycle_id: str | None = None,
    ) -> MessageRecord:
        conversation_id = conversation_id or self.require_conversation(session_id).conversation_id
        now = self.clock.now()
        record = MessageRecord(
            message_id=message_id or self.ids.message_id(),
            session_id=session_id,
            conversation_id=conversation_id,
            direction=direction,
            message_type=message_type,
            payload=payload or {"type": message_type.value},
            size_bytes=64,
            cycle_id=cycle_id,
            posted_at=now if direction is MessageDirection.OUTBOUND else None,
            received_at=now if direction is MessageDirection.INBOUND else None,
            validation_status="valid" if direction is MessageDirection.INBOUND else None,
            created_at=now,
        )
        self.store.save_message(record)
        return record

    def add_failure(
        self,
        session_id: str,
        *,
        error_type: ErrorType = ErrorType.NETWORK_ERROR,
        error_code: str = "CONNECTION_RESET",
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> FailureRecord:
        conversation = self.current_conversation(session_id)
        record = FailureRecord(
            failure_id=self.ids.failure_id(),
            session_id=session_id,
            conversation_id=conversation.conversation_id if conversation else None,
            task_id=task_id,
            error_type=error_type,
            error_code=error_code,
            severity=Severity.HIGH,
            origin="TransportGateway",
            retryable=error_type is ErrorType.NETWORK_ERROR,
            recoverable=True,
            details=dict(details) if details is not None else {"attempt": 1},
            timestamp=self.clock.now(),
        )
        self.store.save_failure(record)
        self.publish(
            EventType.FAILURE_RECORDED,
            session_id,
            conversation_id=record.conversation_id,
            task_id=task_id,
            payload={
                "failure_id": record.failure_id,
                "error_type": error_type.value,
                "error_code": error_code,
                "severity": record.severity.value,
                "origin": record.origin,
                "retryable": record.retryable,
                "recoverable": record.recoverable,
                "attempt": 1,
                "max_attempts": 4,
            },
        )
        return record

    def complete(
        self, session_id: str, final_answer: dict[str, Any] | None = None
    ) -> SessionRecord:
        """A ``final_answer`` arrives: conversation COMPLETED, session COMPLETED (§11)."""
        conversation = self.require_conversation(session_id)
        answer = final_answer or {"status": "success", "summary": "done"}
        if conversation.status is not ConversationState.COMPLETED:
            self.lifecycle.transition_conversation(
                conversation.conversation_id,
                ConversationState.COMPLETED,
                reason="final_answer",
                final_answer_received=True,
            )
        self.publish(
            EventType.FINAL_ANSWER_RECEIVED,
            session_id,
            conversation_id=conversation.conversation_id,
            payload={"message_id": "msg-final", "status": str(answer.get("status", "success"))},
        )
        session = self.require_session(session_id)
        if session.status not in _TERMINAL_SESSION_STATES:
            self.lifecycle.transition_session(
                session_id, SessionState.COMPLETED, reason="final_answer", final_answer=answer
            )
        return self.require_session(session_id)

    def respond(
        self,
        session_id: str,
        body: str = "Here is my analysis.",
        *,
        format: str = "text",
        status: str = "completed",
        expects_reply: bool = False,
    ) -> SessionRecord:
        """A ``user_response`` arrives (ADR-022): the message is persisted like the orchestrator
        does, ``user_response.received`` is published, the conversation ends ``WAITING_USER`` (or
        ``CLOSED`` under auto-close when no reply is expected) and the session ``COMPLETED``."""
        conversation = self.require_conversation(session_id)
        session = self.require_session(session_id)
        content = {
            "format": format,
            "body": body,
            "status": status,
            "expects_reply": expects_reply,
        }
        message_id = f"model-{self.ids.message_id()}"
        record = self.add_message(
            session_id,
            direction=MessageDirection.INBOUND,
            message_type=MessageType.USER_RESPONSE,
            message_id=message_id,
            cycle_id=conversation.current_cycle_id,
            payload={
                "type": "user_response",
                "conversation_id": conversation.remote_conversation_id
                or conversation.conversation_id,
                "message_id": message_id,
                "content": content,
            },
        )
        if conversation.status is not ConversationState.COMPLETED:
            self.lifecycle.transition_conversation(
                conversation.conversation_id,
                ConversationState.COMPLETED,
                reason="user_response",
                final_answer_received=True,
            )
        auto_close_skipped = session.auto_close_on_final_answer and expects_reply
        self.publish(
            EventType.USER_RESPONSE_RECEIVED,
            session_id,
            conversation_id=conversation.conversation_id,
            cycle_id=conversation.current_cycle_id,
            payload={
                "message_id": record.message_id,
                "format": format,
                "status": status,
                "expects_reply": expects_reply,
                "body_bytes": len(body.encode("utf-8")),
                "auto_close_on_final_answer": session.auto_close_on_final_answer,
                "auto_close_skipped": auto_close_skipped,
                "consumed_cycles": session.consumed_cycles,
                "consumed_plans": session.consumed_plans,
                "session_duration_ms": 0,
            },
        )
        if session.auto_close_on_final_answer and not auto_close_skipped:
            self.lifecycle.transition_conversation(
                conversation.conversation_id,
                ConversationState.CLOSED,
                reason="auto_close",
                closure_reason="auto_close",
            )
        else:
            self.lifecycle.transition_conversation(
                conversation.conversation_id, ConversationState.WAITING_USER, reason="reusable"
            )
        if session.status not in _TERMINAL_SESSION_STATES:
            self.lifecycle.transition_session(
                session_id, SessionState.COMPLETED, reason="user_response"
            )
        return self.require_session(session_id)

    def pause(
        self, session_id: str, *, operation: str = "POST", message_id: str | None = None
    ) -> SessionRecord:
        """ADR-025: the loop stopped on a 401 — the failure, then ``PAUSED``, then ``session.paused``.

        Nothing else moves: the conversation keeps its state and its pending message, exactly like
        the orchestrator's pause.
        """
        failure = self.add_failure(
            session_id,
            error_type=ErrorType.AUTHN_ERROR,
            error_code="HTTP_401",
            details={"attempt": 1, "operation": operation, "http_status": 401},
        )
        session = self.lifecycle.transition_session(
            session_id, SessionState.PAUSED, reason=CREDENTIALS_REQUIRED_REASON
        )
        self.publish(
            EventType.SESSION_PAUSED,
            session_id,
            conversation_id=failure.conversation_id,
            payload={
                "reason": CREDENTIALS_REQUIRED_REASON,
                "error_code": failure.error_code,
                "error_type": failure.error_type.value,
                "operation": operation,
                "message_id": message_id,
            },
        )
        return session

    def fail(self, session_id: str, reason: str = "failure") -> SessionRecord:
        conversation = self.current_conversation(session_id)
        if conversation is not None and conversation.status not in (
            ConversationState.FAILED,
            ConversationState.CLOSED,
            ConversationState.INTERRUPTED,
        ):
            self.lifecycle.transition_conversation(
                conversation.conversation_id, ConversationState.FAILED, reason=reason
            )
        return self.lifecycle.transition_session(session_id, SessionState.FAILED, reason=reason)
