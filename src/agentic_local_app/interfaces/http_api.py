"""Local HTTP API (ADR-002, ADR-018): REST resources for the state, SSE for the live stream.

``create_app(manager)`` builds a FastAPI application under ``/api/v1`` on top of the
``ConversationManager`` façade (§3.1) — described here by the :class:`ConversationManagerLike`
protocol so that this module never imports the orchestration package. There is no state on the
server side other than the store (read through the façade) and the per-client SSE queues of the
:class:`~agentic_local_app.interfaces.sse.SseBroker`.

``POST /sessions`` takes its opening message as an optional **pair** (ADR-028): both fields open the
first conversation at once, neither leaves the session ``READY`` until its first
``POST /sessions/{sid}/messages``, and one without the other is a ``400``. Its optional ``user_id``
defaults to what ``GET /whoami`` answers, resolved once per process and shared by the two routes.

What a desktop front needs on top of the session resources (ADR-024, ADR-025, ADR-026, ADR-027):

- ``GET /whoami`` and ``GET /models`` — who the user is on this machine and the model catalogue,
  never a URL, a token or a provider option; each model carries the ``credential_fields`` an
  interface has to render (ADR-027 §2), never the environment variables behind them;
- ``GET /skills`` — the reusable notes of ``[skills] root``, for the sign-in screen (ADR-027 §4);
- ``POST /credentials`` — the credentials of the **active** profile after a 401; the body carries
  secrets, so no value is ever echoed, logged or put in an error detail;
- ``POST /sessions/{sid}/resume`` and ``GET /sessions/{sid}/pause`` — resume a paused session, and
  why it is paused (the pop-in of ADR-025 §7);
- ``GET /sessions/{sid}/chat`` — the protocol messages mapped onto chat turns;
- ``GET /admin/sessions`` · ``/admin/events`` · ``/admin/audit`` — the whole store, every session,
  for the "live database" screen, and ``POST /admin/reset-database``, which empties it and answers
  ``403 ADMIN_DISABLED`` unless ``api.allow_destructive_admin`` is set.

Conventions, identical on every route:

- records are pydantic models dumped in JSON mode (ISO 8601 timestamps, enum values, never bytes);
  task output is decoded UTF-8 with replacement characters, exactly like a ``chunk_request``;
- lists are paginated with ``limit`` (default ``api.page_size``) / ``offset`` or, for the audit
  chain, ``after`` / ``limit``;
- every error is ``{"error": <NormalizedError>}`` (§6): an ``AppError`` keeps its own normalized
  error and gets a status from :func:`status_for`; ``KeyError`` → 404 ``NOT_FOUND``; ``ValueError``
  → 409 ``CONFLICT``; request validation → 422 ``VALIDATION_ERROR``; other HTTP errors →
  ``HTTP_<status>``;
- CORS is restricted to ``api.cors_origins``.

Identifiers of plans and tasks are unique per session (ADR-007), hence the nesting
``/sessions/{sid}/plans/{pid}`` and ``/sessions/{sid}/tasks/{tid}``.
"""

from __future__ import annotations

import dataclasses
import json
import os
from collections.abc import AsyncIterator, Callable, Iterable, MutableMapping
from contextlib import asynccontextmanager
from enum import Enum
from typing import Annotated, Any, Protocol, TypeVar

from fastapi import APIRouter, FastAPI, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from starlette.exceptions import HTTPException as StarletteHTTPException

from agentic_local_app import __version__, credentials
from agentic_local_app.config import (
    DEFAULT_CREDENTIAL_KEY,
    AppConfig,
    declared_credential_fields,
)
from agentic_local_app.domain.clock import Clock, SystemClock
from agentic_local_app.domain.errors import (
    AppError,
    ConfigError,
    ErrorType,
    NormalizedError,
    Severity,
)
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.models import (
    AuditEvent,
    MessageRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    PLAN_MESSAGE_TYPES,
    MessageDirection,
    MessageType,
    OutputStream,
    SessionState,
    TaskState,
)
from agentic_local_app.execution.payload_guard import ChunkError, PayloadGuard, decode_output
from agentic_local_app.execution.scratch import WORKING_SPACE_INVALID
from agentic_local_app.identity import UserIdentity, current_user
from agentic_local_app.interfaces.sse import SseBroker
from agentic_local_app.interruption.handler import InterruptionReport
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker, RuntimeSnapshot
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.skills import list_skills

__all__ = [
    "API_PREFIX",
    "CREDENTIAL_FIELD_UNKNOWN",
    "DEFAULT_OUTPUT_MAX_BYTES",
    "DEFAULT_SSE_HEARTBEAT_S",
    "EFFORT_INVALID",
    "EFFORT_LEVELS",
    "GOAL_REQUIRED",
    "ROLE_ASSISTANT",
    "ROLE_SYSTEM",
    "ROLE_USER",
    "USER_MESSAGE_REQUIRED",
    "ConversationManagerLike",
    "chat_turn",
    "create_app",
    "status_for",
]

API_PREFIX = "/api/v1"
API_ORIGIN = "http_api"
DEFAULT_OUTPUT_MAX_BYTES = 65_536
DEFAULT_SSE_HEARTBEAT_S = 15.0
_UNBOUNDED = 1_000_000_000
#: ``error_code`` of a posted credential the active profile does not declare (ADR-027 §3).
CREDENTIAL_FIELD_UNKNOWN = "CREDENTIAL_FIELD_UNKNOWN"
#: ``error_code`` of an ``effort`` outside :data:`EFFORT_LEVELS` (ADR-027 §4).
EFFORT_INVALID = "EFFORT_INVALID"
#: The effort levels ``POST /sessions`` accepts — traced, not acted upon (ADR-027 §4).
EFFORT_LEVELS: tuple[str, ...] = ("low", "medium", "high")
#: ADR-028 §1: ``user_message`` was given without ``goal`` — half an opening message is not one.
GOAL_REQUIRED = "GOAL_REQUIRED"
#: ADR-028 §1: ``goal`` was given without ``user_message`` — the other half.
USER_MESSAGE_REQUIRED = "USER_MESSAGE_REQUIRED"


class ConversationManagerLike(Protocol):
    """The façade contract of phase 9a (``orchestration.conversation_manager``) this API relies on.

    The components are read-only members (properties or plain attributes both satisfy them)."""

    @property
    def config(self) -> AppConfig: ...

    @property
    def store(self) -> ConversationStore: ...

    @property
    def bus(self) -> EventBus: ...

    @property
    def tracker(self) -> ExecutionTracker: ...

    @property
    def audit(self) -> AuditLog: ...

    @property
    def telemetry(self) -> TelemetryService: ...

    @property
    def recovery_report(self) -> Any | None: ...

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
    ) -> SessionRecord: ...

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord: ...

    async def resume_session(self, session_id: str) -> SessionRecord: ...

    async def interrupt(self, session_id: str) -> InterruptionReport: ...

    async def wait(self, session_id: str, *, timeout_ms: int | None = None) -> SessionRecord: ...

    def get_session(self, session_id: str) -> SessionRecord | None: ...

    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]: ...

    def snapshot(self, session_id: str) -> RuntimeSnapshot: ...

    def final_answer(self, session_id: str) -> dict[str, Any] | None: ...

    def user_responses(self, session_id: str) -> list[dict[str, Any]]: ...

    def last_reply(self, session_id: str) -> dict[str, Any] | None: ...

    def corrections(self, session_id: str) -> list[dict[str, Any]]: ...

    def paused_reason(self, session_id: str) -> dict[str, Any] | None: ...

    def running_task_ids(self, session_id: str) -> list[str]: ...

    async def shutdown(self) -> None: ...


# ------------------------------------------------------------------------------------------------
# request bodies
# ------------------------------------------------------------------------------------------------
class CreateSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    #: ADR-028 §1: the opening message is optional, but it comes as a **pair**. Both given, the
    #: session starts at once as it always did; neither given, the session is created ``READY`` and
    #: waits for its first ``POST /sessions/{sid}/messages``; one without the other is refused
    #: (400 ``GOAL_REQUIRED`` / ``USER_MESSAGE_REQUIRED``). An explicitly empty string stays a
    #: ``422``: omitting a field and emptying it are not the same statement.
    goal: str | None = Field(default=None, min_length=1)
    user_message: str | None = Field(default=None, min_length=1)
    #: ADR-028 §2: who the session belongs to. Left out, the session carries the machine identity
    #: ``GET /whoami`` answers — the same value, resolved once per process.
    user_id: str | None = Field(default=None, min_length=1)
    session_budget: SessionBudget | None = None
    auto_close_on_final_answer: bool | None = None
    #: ADR-026 §3: the folder the user designated ("Working folder"), absolute and existing.
    working_space: str | None = None
    #: ADR-027 §4: the reusable notes the user attached to the session, by name or path.
    #: **Traced, not acted upon**: they travel to the ``session.created`` event and stop there —
    #: nothing is read from them and nothing reaches the model yet (that needs an ADR of its own).
    skills: list[str] | None = None
    #: ADR-027 §4: ``low`` | ``medium`` | ``high``, refused otherwise (400 ``EFFORT_INVALID``).
    #: Traced with the skills, and just as inert: no instruction is derived from it yet.
    effort: str | None = None


class FollowUpRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_message: str = Field(min_length=1)


# ------------------------------------------------------------------------------------------------
# chat view of the protocol messages
# ------------------------------------------------------------------------------------------------
ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
ROLE_SYSTEM = "system"

#: The states in which a session refuses a follow-up because its loop still owns it.
_BUSY_SESSION_STATES: frozenset[SessionState] = frozenset(
    {SessionState.RUNNING, SessionState.INTERRUPTING}
)


def _plural(count: int, singular: str) -> str:
    return f"{count} {singular}" + ("s" if count > 1 else "")


def _summary(message_type: MessageType, content: dict[str, Any]) -> str:
    """The one line a system turn shows — enough to follow the exchange, never a wall of JSON."""
    if message_type in PLAN_MESSAGE_TYPES:
        tasks = content.get("tasks")
        count = len(tasks) if isinstance(tasks, list) else 0
        return f"{content.get('plan_id', '?')} · {_plural(count, 'tâche')}"
    if message_type is MessageType.EXECUTION_RESULT:
        results = content.get("results")
        count = len(results) if isinstance(results, list) else 0
        status = content.get("status", "?")
        return f"{content.get('plan_id', '?')} · {status} · {_plural(count, 'résultat')}"
    if message_type is MessageType.PROTOCOL_CORRECTION_REQUEST:
        attempt = content.get("attempt", "?")
        return (
            f"correction {attempt}/{content.get('max_attempts', '?')} · "
            f"{content.get('error_code', '?')}"
        )
    return message_type.value


def chat_turn(message: MessageRecord) -> dict[str, Any]:
    """One chat turn from one protocol message (ADR-018: the front shows a conversation).

    ``role`` is the speaker: the ``user_request`` the user wrote, the ``user_response`` /
    ``final_answer`` the model concluded with, and everything else — plans, execution results,
    correction requests, rotation messages — a ``system`` line carrying a one-line summary. The
    text of a ``final_answer`` is its ``diagnosis`` alone: ``evidence`` and
    ``recommended_next_step`` stay in the record (``GET /sessions/{sid}/final-answer``) rather than
    being flattened into a chat bubble.

    ``created_at`` is when the turn appeared: ``received_at`` for an inbound message, ``posted_at``
    for one that was sent, and its creation time while it is still on its way.
    """
    content = message.payload.get("content")
    content = content if isinstance(content, dict) else {}
    message_type = message.message_type
    if message_type is MessageType.USER_REQUEST:
        role, text = ROLE_USER, str(content.get("user_message", ""))
    elif message_type is MessageType.USER_RESPONSE:
        role, text = ROLE_ASSISTANT, str(content.get("body", ""))
    elif message_type is MessageType.FINAL_ANSWER:
        role, text = ROLE_ASSISTANT, str(content.get("diagnosis", ""))
    else:
        role, text = ROLE_SYSTEM, _summary(message_type, content)
    dumped = message.model_dump(mode="json")
    turn: dict[str, Any] = {
        "id": message.message_id,
        "role": role,
        "text": text,
        "created_at": dumped["received_at"] or dumped["posted_at"] or dumped["created_at"],
        "message_type": message_type.value,
    }
    plan_id = content.get("plan_id")
    if isinstance(plan_id, str) and plan_id:
        turn["plan_id"] = plan_id
    return turn


# ------------------------------------------------------------------------------------------------
# errors
# ------------------------------------------------------------------------------------------------
_STATUS_BY_ERROR_TYPE: dict[ErrorType, int] = {
    ErrorType.AUTHN_ERROR: 502,
    ErrorType.AUTHZ_ERROR: 502,
    ErrorType.NETWORK_ERROR: 502,
    ErrorType.TIMEOUT_ERROR: 504,
    ErrorType.RATE_LIMIT_ERROR: 503,
    ErrorType.MODEL_PROTOCOL_ERROR: 502,
    ErrorType.MODEL_CONTEXT_WINDOW_ERROR: 502,
    ErrorType.TASK_EXECUTION_ERROR: 500,
    ErrorType.PERSISTENCE_ERROR: 500,
    ErrorType.BUDGET_EXCEEDED: 409,
    ErrorType.ROTATION_FAILED: 500,
    ErrorType.INTERRUPTED: 409,
    ErrorType.SYSTEM_ERROR: 500,
}


def status_for(exc: AppError) -> int:
    """HTTP status of an ``AppError``: by error type, except the state conflicts (409)."""
    if exc.error.error_code == "INVALID_TRANSITION":
        return 409
    return _STATUS_BY_ERROR_TYPE.get(exc.error.error_type, 500)


def _normalized(code: str, message: str, **details: Any) -> NormalizedError:
    return NormalizedError(
        error_type=ErrorType.SYSTEM_ERROR,
        error_code=code,
        severity=Severity.LOW,
        origin=API_ORIGIN,
        retryable=False,
        recoverable=True,
        details={"message": message, **details},
    )


def _error_response(status: int, error: NormalizedError) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error.model_dump(mode="json")})


def _validation_error(param: str, message: str, value: Any) -> RequestValidationError:
    return RequestValidationError(
        [{"type": "value_error", "loc": ("query", param), "msg": message, "input": value}]
    )


E = TypeVar("E", bound=Enum)


def _parse_enums(
    raw: str | None, enum: type[E], param: str, *, normalise: Callable[[str], str] = str.upper
) -> list[E] | None:
    """``"running,ready"`` -> ``[RUNNING, READY]``; ``None`` / blank -> ``None``; else 422."""
    if raw is None or not raw.strip():
        return None
    values: list[E] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            values.append(enum(normalise(token)))
        except ValueError:
            expected = ", ".join(str(member.value) for member in enum)
            raise _validation_error(
                param, f"unknown value {token!r}; expected: {expected}", raw
            ) from None
    return values or None


_DIRECTION_ALIASES = {"in": "inbound", "out": "outbound"}


def _direction(token: str) -> str:
    lowered = token.lower()
    return _DIRECTION_ALIASES.get(lowered, lowered)


# ------------------------------------------------------------------------------------------------
# serialisation
# ------------------------------------------------------------------------------------------------
_REPORT_ADAPTER: TypeAdapter[InterruptionReport] = TypeAdapter(InterruptionReport)


def _dump(record: BaseModel) -> dict[str, Any]:
    return record.model_dump(mode="json")


def _dump_all(records: Iterable[BaseModel]) -> list[dict[str, Any]]:
    return [_dump(record) for record in records]


def _jsonable(value: Any) -> Any:
    """JSON-ready form of a pydantic model, a dataclass or anything ``jsonable_encoder`` accepts."""
    if value is None:
        return None
    if isinstance(value, BaseModel):
        return _dump(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return TypeAdapter(type(value)).dump_python(value, mode="json")
    return jsonable_encoder(value)


# ------------------------------------------------------------------------------------------------
# application
# ------------------------------------------------------------------------------------------------
def create_app(
    manager: ConversationManagerLike,
    *,
    clock: Clock | None = None,
    sse_heartbeat_s: float | None = DEFAULT_SSE_HEARTBEAT_S,
    identity: UserIdentity | None = None,
    environ: MutableMapping[str, str] | None = None,
) -> FastAPI:
    """The API over ``manager``. ``clock`` dates the SSE ``dropped`` frames (defaults to the
    manager's own clock when it exposes one, else the system clock); ``sse_heartbeat_s`` is the
    keep-alive period of the streams (``None`` disables it, as the tests do).

    ``identity`` is what ``GET /whoami`` answers — ``Application.identity``, resolved once at wiring
    time (ADR-024 §6); left out, it is resolved on the first call and kept. ``environ`` is where
    ``POST /credentials`` writes the token and where ``GET /models`` reads whether a profile still
    needs one; it defaults to the real process environment, which is what the transport reads at
    call time (ADR-004).
    """
    config = manager.config
    machine: UserIdentity | None = identity
    variables: MutableMapping[str, str] = os.environ if environ is None else environ
    effective_clock: Clock = clock or getattr(manager, "clock", None) or SystemClock()
    broker = SseBroker(
        manager.bus,
        effective_clock,
        queue_size=config.api.sse_queue_size,
        audit=manager.audit,
        store=manager.store,
    )
    guard = PayloadGuard(config.payload)
    page_size = config.api.page_size

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            broker.close()
            await manager.shutdown()

    app = FastAPI(
        title="agentic-local-app API",
        version=__version__,
        lifespan=lifespan,
        docs_url=f"{API_PREFIX}/docs",
        openapi_url=f"{API_PREFIX}/openapi.json",
        redoc_url=None,
    )
    app.state.manager = manager
    app.state.sse_broker = broker
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(config.api.cors_origins),
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )
    _install_error_handlers(app)
    router = APIRouter(prefix=API_PREFIX)

    # ---- helpers -------------------------------------------------------------------------
    def require_session(session_id: str) -> SessionRecord:
        session = manager.get_session(session_id)
        if session is None:
            raise KeyError(f"unknown session: {session_id}")
        return session

    def require_task(session_id: str, task_id: str) -> TaskRecord:
        require_session(session_id)
        task = manager.store.get_task(session_id, task_id)
        if task is None:
            raise KeyError(f"unknown task: {task_id} (session {session_id})")
        return task

    def page_limit(limit: int | None) -> int:
        return page_size if limit is None else limit

    def sse(iterator: AsyncIterator[bytes]) -> StreamingResponse:
        return StreamingResponse(
            iterator,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    def resume_position(request: Request, last_event_id: str | None) -> str | None:
        return last_event_id or request.headers.get("last-event-id")

    def refuse_when_busy(session: SessionRecord) -> None:
        """A front blocks its send button while a session runs; this is the belt (ADR-018)."""
        if session.status in _BUSY_SESSION_STATES:
            raise _ConflictError(
                "SESSION_BUSY",
                f"session {session.session_id} is still running",
                {"session_id": session.session_id, "status": session.status.value},
            )

    def audit_events_of_every_session() -> list[AuditEvent]:
        """Every audited event of the store, newest first (``timestamp``, session, sequence).

        The store indexes the audit chain per session (§16), so the whole table is read session by
        session and merged here: the administration screen of a local application looks at a
        database that holds one user's sessions, not at a stream.
        """
        events: list[AuditEvent] = []
        for session in manager.list_sessions(limit=_UNBOUNDED):
            events.extend(manager.store.list_audit_events(session.session_id, limit=_UNBOUNDED))
        events.sort(key=lambda event: (event.timestamp, event.session_id, event.sequence))
        events.reverse()
        return events

    def machine_identity() -> UserIdentity:
        """ADR-024 §5 / ADR-028 §2: the machine identity, resolved once and kept.

        ``GET /whoami`` and the default ``user_id`` of ``POST /sessions`` read it here, and only
        here: whatever a deployment injects, the name on the screen and the name on the session
        rows are the same string.
        """
        nonlocal machine
        if machine is None:
            machine = current_user(fallback_user_id=config.transport.user_id)
        return machine

    def page(items: list[dict[str, Any]], limit: int, offset: int) -> JSONResponse:
        window = items[offset : offset + limit]
        return JSONResponse(
            content={
                "items": window,
                "limit": limit,
                "offset": offset,
                "next_offset": offset + limit if offset + limit < len(items) else None,
            }
        )

    # ---- sessions -------------------------------------------------------------------------
    @router.post("/sessions", status_code=201)
    async def create_session(body: CreateSessionRequest) -> JSONResponse:
        """ADR-018 §4, ADR-026 §3 (``working_space``), ADR-027 §4 (``skills`` / ``effort``) and
        ADR-028 (optional opening message, ``user_id``).

        The opening message is a **pair**. With ``goal`` and ``user_message`` the session answers
        ``RUNNING`` and the loop has already started; with neither it answers ``READY``, holds no
        conversation and has posted nothing to the model — the sign-in screen of a desktop front
        has no message to give yet, and inventing one would put a ``user_request`` the user never
        wrote in front of the model and in the audit trail. Half a pair is refused here, before
        anything is created.

        ``skills`` and ``effort`` are **recorded, not applied**: they end up in the payload of the
        ``session.created`` event and nowhere else — no file is read, no instruction is derived,
        nothing is added to what the model receives. Turning them into behaviour is a decision of
        its own (ADR-027, point ouvert).
        """
        if body.effort is not None and body.effort not in EFFORT_LEVELS:
            raise _BadRequestError(
                EFFORT_INVALID,
                "unknown effort level",
                {"effort": body.effort, "expected": list(EFFORT_LEVELS)},
            )
        if body.goal is None and body.user_message is not None:
            raise _BadRequestError(
                GOAL_REQUIRED,
                "user_message was given without a goal",
                {"field": "goal", "expected": ["goal", "user_message"]},
            )
        if body.user_message is None and body.goal is not None:
            raise _BadRequestError(
                USER_MESSAGE_REQUIRED,
                "goal was given without a user_message",
                {"field": "user_message", "expected": ["goal", "user_message"]},
            )
        try:
            session = await manager.start_session(
                goal=body.goal,
                user_message=body.user_message,
                budget=body.session_budget,
                auto_close=body.auto_close_on_final_answer,
                working_space=body.working_space,
                skills=body.skills,
                effort=body.effort,
                user_id=body.user_id or machine_identity().user_id,
            )
        except ConfigError as exc:
            if exc.error.error_code != WORKING_SPACE_INVALID:
                raise
            raise _BadRequestError(
                WORKING_SPACE_INVALID,
                "the working space cannot be used as it is",
                dict(exc.error.details),
            ) from None
        return JSONResponse(status_code=201, content=_dump(session))

    @router.get("/sessions")
    async def list_sessions(
        status: Annotated[
            str | None, Query(description="comma-separated SessionState values")
        ] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JSONResponse:
        statuses = _parse_enums(status, SessionState, "status")
        size = page_limit(limit)
        items = manager.list_sessions(statuses=statuses, limit=size, offset=offset)
        return JSONResponse(
            content={
                "items": _dump_all(items),
                "limit": size,
                "offset": offset,
                "next_offset": offset + size if len(items) == size else None,
            }
        )

    @router.get("/sessions/{sid}")
    async def get_session(sid: str) -> JSONResponse:
        session = require_session(sid)
        conversation = (
            manager.store.get_conversation(session.current_conversation_id)
            if session.current_conversation_id is not None
            else None
        )
        body = _dump(session)
        body["conversation"] = _dump(conversation) if conversation is not None else None
        return JSONResponse(content=body)

    @router.post("/sessions/{sid}/interrupt")
    async def interrupt_session(sid: str) -> JSONResponse:
        require_session(sid)
        report = await manager.interrupt(sid)
        return JSONResponse(content=_REPORT_ADAPTER.dump_python(report, mode="json"))

    @router.post("/sessions/{sid}/resume")
    async def resume_session(sid: str) -> JSONResponse:
        """ADR-025 §5: continue a session paused on a 401 once a token was provided (or one the
        recovery left resumable, ADR-016); 409 ``SESSION_NOT_RESUMABLE`` when the façade refuses."""
        require_session(sid)
        try:
            session = await manager.resume_session(sid)
        except ValueError as exc:
            raise _ConflictError("SESSION_NOT_RESUMABLE", str(exc), {"session_id": sid}) from None
        return JSONResponse(content=_dump(session))

    @router.get("/sessions/{sid}/pause")
    async def pause_reason(sid: str) -> JSONResponse:
        """ADR-025 §7: why the session is paused — ``{reason, error_code, error_type, operation,
        since}`` —, the pop-in that asks for a token; 404 ``NOT_PAUSED`` when it is not."""
        require_session(sid)
        reason = manager.paused_reason(sid)
        if reason is None:
            raise _NotFoundError("NOT_PAUSED", f"session {sid} is not paused")
        return JSONResponse(content=_jsonable(reason))

    @router.post("/sessions/{sid}/messages", status_code=202)
    async def follow_up(sid: str, body: FollowUpRequest) -> JSONResponse:
        refuse_when_busy(require_session(sid))
        try:
            session = await manager.continue_session(sid, body.user_message)
        except ValueError:
            # the record said the session was free; its loop was still draining (§3.1 race)
            refuse_when_busy(require_session(sid))
            raise
        return JSONResponse(status_code=202, content=_dump(session))

    @router.get("/sessions/{sid}/snapshot")
    async def snapshot(sid: str) -> JSONResponse:
        return JSONResponse(content=_dump(manager.snapshot(sid)))

    @router.get("/sessions/{sid}/final-answer")
    async def final_answer(sid: str) -> JSONResponse:
        require_session(sid)
        return JSONResponse(
            content={"session_id": sid, "final_answer": _jsonable(manager.final_answer(sid))}
        )

    @router.get("/sessions/{sid}/responses")
    async def user_responses(sid: str) -> JSONResponse:
        """ADR-022: every ``user_response`` of the session, oldest first (empty list if none)."""
        require_session(sid)
        return JSONResponse(content=_jsonable(manager.user_responses(sid)))

    @router.get("/sessions/{sid}/reply")
    async def last_reply(sid: str) -> JSONResponse:
        """ADR-022: the newest concluding reply (``final_answer`` or ``user_response``); 404
        (``REPLY_NOT_FOUND``) while the model has not concluded a turn."""
        require_session(sid)
        reply = manager.last_reply(sid)
        if reply is None:
            raise _NotFoundError("REPLY_NOT_FOUND", f"session {sid} has no reply yet")
        return JSONResponse(content=_jsonable(reply))

    # ---- conversations, plans, tasks -------------------------------------------------------
    @router.get("/sessions/{sid}/conversations")
    async def list_conversations(sid: str) -> JSONResponse:
        require_session(sid)
        return JSONResponse(content=_dump_all(manager.store.list_conversations(sid)))

    @router.get("/sessions/{sid}/conversations/{cid}")
    async def get_conversation(sid: str, cid: str) -> JSONResponse:
        require_session(sid)
        conversation = manager.store.get_conversation(cid)
        if conversation is None or conversation.session_id != sid:
            raise KeyError(f"unknown conversation: {cid} (session {sid})")
        return JSONResponse(content=_dump(conversation))

    def plan_body(sid: str, plan: PlanRecord, include: set[str]) -> dict[str, Any]:
        body = _dump(plan)
        if "tasks" in include:
            plan_id: str = plan.plan_id
            body["tasks"] = _dump_all(manager.store.list_tasks(sid, plan_id=plan_id))
        return body

    def parse_include(include: str | None) -> set[str]:
        tokens = {t.strip().lower() for t in (include or "").split(",") if t.strip()}
        unknown = tokens - {"tasks"}
        if unknown:
            raise _validation_error("include", f"unknown value(s) {sorted(unknown)}", include)
        return tokens

    @router.get("/sessions/{sid}/plans")
    async def list_plans(sid: str, include: Annotated[str | None, Query()] = None) -> JSONResponse:
        require_session(sid)
        wanted = parse_include(include)
        plans = manager.store.list_plans(sid)
        return JSONResponse(content=[plan_body(sid, plan, wanted) for plan in plans])

    @router.get("/sessions/{sid}/plans/{pid}")
    async def get_plan(
        sid: str, pid: str, include: Annotated[str | None, Query()] = None
    ) -> JSONResponse:
        require_session(sid)
        wanted = parse_include(include)
        plan = manager.store.get_plan(sid, pid)
        if plan is None:
            raise KeyError(f"unknown plan: {pid} (session {sid})")
        return JSONResponse(content=plan_body(sid, plan, wanted))

    @router.get("/sessions/{sid}/tasks")
    async def list_tasks(
        sid: str,
        status: Annotated[str | None, Query(description="comma-separated TaskState values")] = None,
        plan_id: Annotated[str | None, Query()] = None,
    ) -> JSONResponse:
        require_session(sid)
        statuses = _parse_enums(status, TaskState, "status")
        tasks = manager.store.list_tasks(sid, plan_id=plan_id, statuses=statuses)
        return JSONResponse(content=_dump_all(tasks))

    @router.get("/sessions/{sid}/tasks/{tid}")
    async def get_task(sid: str, tid: str) -> JSONResponse:
        return JSONResponse(content=_dump(require_task(sid, tid)))

    @router.get("/sessions/{sid}/tasks/{tid}/output")
    async def task_output(
        sid: str,
        tid: str,
        stream: Annotated[OutputStream, Query()] = OutputStream.STDOUT,
        offset: Annotated[int, Query(ge=0)] = 0,
        max_bytes: Annotated[int, Query(ge=1)] = DEFAULT_OUTPUT_MAX_BYTES,
    ) -> JSONResponse:
        """Bytes ``[offset, offset + max_bytes)`` of the stored stream, decoded — the engine of
        ``chunk_request`` (ADR-011): no blob → 404, offset past the end → 422; ``offset == total``
        (including an empty stream) is a valid, empty, ``eof`` read."""
        require_task(sid, tid)
        blob = manager.store.get_blob_for_task(sid, tid, stream)
        if blob is None:
            raise _NotFoundError("CHUNK_REF_NOT_FOUND", f"no {stream.value} stored for task {tid}")
        if offset == blob.size_bytes:
            data, span, total, eof = "", (offset, offset), blob.size_bytes, True
        else:
            served = guard.serve_chunk(manager.store, sid, tid, stream, offset, max_bytes)
            if isinstance(served, ChunkError):
                raise _UnprocessableError(served.code, "invalid output range", served.details)
            data, span, total, eof = (
                decode_output(served.data),
                served.range,
                served.total,
                served.eof,
            )
        return JSONResponse(
            content={
                "task_id": tid,
                "stream": stream.value,
                "offset": offset,
                "data": data,
                "range": list(span),
                "total": total,
                "eof": eof,
            }
        )

    # ---- messages, failures, audit ---------------------------------------------------------
    @router.get("/sessions/{sid}/messages")
    async def list_messages(
        sid: str,
        direction: Annotated[str | None, Query(description="inbound | outbound (in | out)")] = None,
        conversation_id: Annotated[str | None, Query()] = None,
    ) -> JSONResponse:
        require_session(sid)
        directions = _parse_enums(direction, MessageDirection, "direction", normalise=_direction)
        wanted = set(directions) if directions is not None else None
        conversations = manager.store.list_conversations(sid)
        if conversation_id is not None:
            conversations = [c for c in conversations if c.conversation_id == conversation_id]
        messages = [
            message
            for conversation in conversations
            for message in manager.store.list_messages(conversation.conversation_id)
            if wanted is None or message.direction in wanted
        ]
        return JSONResponse(content=_dump_all(messages))

    @router.get("/sessions/{sid}/chat")
    async def chat(
        sid: str,
        include_system: Annotated[
            bool, Query(description="keep the plan / result / correction lines")
        ] = True,
    ) -> JSONResponse:
        """The session as a conversation, oldest first — what a chat interface displays.

        One turn per protocol message (:func:`chat_turn`), across every conversation of the session
        (a rotation or an interruption opens a new one but continues the same exchange). An inbound
        message the protocol rejected is left out: it never became a turn, and the correction
        request that follows tells that part of the story. ``include_system=false`` keeps only what
        the user and the model said.
        """
        require_session(sid)
        turns = [
            chat_turn(message)
            for conversation in manager.store.list_conversations(sid)
            for message in manager.store.list_messages(conversation.conversation_id)
            if message.direction is MessageDirection.OUTBOUND
            or message.validation_status == "valid"
        ]
        if not include_system:
            turns = [turn for turn in turns if turn["role"] != ROLE_SYSTEM]
        return JSONResponse(content={"messages": turns})

    @router.get("/sessions/{sid}/failures")
    async def list_failures(sid: str) -> JSONResponse:
        require_session(sid)
        return JSONResponse(content=_dump_all(manager.store.list_failures(sid)))

    @router.get("/sessions/{sid}/corrections")
    async def list_corrections(sid: str) -> JSONResponse:
        """ADR-023: the ``protocol_correction_request`` messages sent to the model, oldest first."""
        require_session(sid)
        return JSONResponse(content=manager.corrections(sid))

    @router.get("/sessions/{sid}/audit")
    async def list_audit(
        sid: str,
        after: Annotated[
            int | None, Query(ge=0, description="sequence after which to read")
        ] = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> JSONResponse:
        require_session(sid)
        size = page_limit(limit)
        events = manager.store.list_audit_events(sid, after_sequence=after, limit=size)
        return JSONResponse(
            content={
                "items": _dump_all(events),
                "after": after,
                "limit": size,
                "next_after": events[-1].sequence if len(events) == size else None,
            }
        )

    @router.get("/sessions/{sid}/audit/verify")
    async def verify_audit(sid: str) -> JSONResponse:
        require_session(sid)
        return JSONResponse(content=_dump(manager.audit.verify(sid)))

    # ---- live streams (SSE) ---------------------------------------------------------------
    @router.get("/sessions/{sid}/events")
    async def session_events(
        sid: str,
        request: Request,
        last_event_id: Annotated[str | None, Query()] = None,
        event_types: Annotated[
            str | None, Query(description="comma-separated EventType values")
        ] = None,
    ) -> StreamingResponse:
        require_session(sid)
        types = _parse_enums(event_types, EventType, "event_types", normalise=str.lower)
        return sse(
            broker.stream(
                session_id=sid,
                event_types=types,
                last_event_id=resume_position(request, last_event_id),
                heartbeat_s=sse_heartbeat_s,
            )
        )

    @router.get("/events")
    async def all_events(
        event_types: Annotated[
            str | None, Query(description="comma-separated EventType values")
        ] = None,
    ) -> StreamingResponse:
        types = _parse_enums(event_types, EventType, "event_types", normalise=str.lower)
        return sse(broker.stream(event_types=types, heartbeat_s=sse_heartbeat_s))

    @router.get("/sessions/{sid}/tasks/{tid}/output/live")
    async def task_output_live(sid: str, tid: str) -> StreamingResponse:
        require_task(sid, tid)
        return sse(
            broker.stream(
                session_id=sid,
                task_id=tid,
                event_types=[EventType.TASK_OUTPUT],
                heartbeat_s=sse_heartbeat_s,
            )
        )

    # ---- metrics, health, config -----------------------------------------------------------
    @router.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(
            manager.telemetry.render_text(), media_type="text/plain; version=0.0.4; charset=utf-8"
        )

    @router.get("/health")
    async def health() -> JSONResponse:
        running = manager.list_sessions(statuses=[SessionState.RUNNING], limit=_UNBOUNDED)
        return JSONResponse(
            content={
                "status": "ok",
                "version": __version__,
                "sessions_running": len(running),
                "recovery_report": _jsonable(manager.recovery_report),
            }
        )

    @router.get("/config")
    async def show_config() -> JSONResponse:
        return JSONResponse(content=jsonable_encoder(config.masked()))

    # ---- machine, models, skills, credentials (ADR-024, ADR-025, ADR-027) ------------------
    @router.get("/whoami")
    async def whoami() -> JSONResponse:
        """ADR-024 §5: who the user is on this machine and how it was found (``source``)."""
        return JSONResponse(content=_dump(machine_identity()))

    @router.get("/models")
    async def list_models() -> JSONResponse:
        """ADR-024 §3 / ADR-027 §2: the catalogue, active profile first, and the active name.

        Computed at read time, like ``requires_credentials`` itself: right after credentials were
        handed over the front must see the profile stop asking for them. ``credential_fields`` is
        always present — possibly empty, in declaration order — and carries ``key``, ``label``,
        ``placeholder`` and ``secret`` only: the environment variable behind a field never travels
        through this route, a form has no use for it (``GET /config``, which renders the effective
        configuration, shows it like it already shows ``token_env``). No secret is in the payload
        either — no token, no URL, no provider option.
        """
        return JSONResponse(
            content={
                "active": config.models.active,
                "models": _dump_all(config.profile_views(variables)),
            }
        )

    @router.get("/skills")
    async def skills() -> JSONResponse:
        """ADR-027 §4: the markdown notes found under ``[skills] root``, sorted by name.

        This route **never fails**: an unset, missing or unreadable root answers an empty list, so
        the sign-in screen that lists them degrades into a free text field instead of an error.
        """
        return JSONResponse(content={"skills": _dump_all(list_skills(config.skills))})

    @router.post("/credentials", status_code=204)
    async def set_credentials(request: Request) -> Response:
        """ADR-025 §6 / ADR-027 §3: the credentials of the **active** profile, after a 401.

        The body is ``{"credentials": {"<key>": "<value>", …}}``, one entry per field the profile
        declares; ``{"token": "…"}`` stays accepted as an alias for the implicit ``access_token``
        field (the CLI and the older client post it). Each value is handed to
        ``credentials.set_token``, which writes it to that field's environment variable and does
        nothing else: **no value is ever echoed, logged, put in an error detail or in an event** —
        the refusals below name the key or the provider, never what was posted, and not even the
        variable behind a key (``GET /models`` does not expose it either). The transport reads
        those variables at call time (ADR-004), so the next call uses them without anything being
        rebuilt.

        Nothing is written until everything posted has been accepted: a refused entry leaves the
        environment exactly as it was. An entry for a field that is not declared is refused rather
        than ignored — silently dropping it would let a front believe it signed in.

        Credentials for an **inactive** profile (ADR-025, point ouvert 4) are deliberately not
        accepted: one model per process (ADR-024 §2), so the only credentials this process can use
        are the active one's; preparing another means restarting with another ``models.active``.
        """
        posted = await _posted_credentials(request)
        profile = config.transport
        declared = {field.key: field for field in declared_credential_fields(profile)}
        if not posted:
            raise _BadRequestError(credentials.CREDENTIALS_EMPTY, "no credential was posted")
        if not declared:
            raise _ConflictError(
                credentials.CREDENTIALS_NOT_CONFIGURED,
                "the active model profile declares no credential field",
                {"field": "credential_fields", "provider": profile.provider},
            )
        for key, value in posted.items():
            if key not in declared:
                raise _BadRequestError(
                    CREDENTIAL_FIELD_UNKNOWN,
                    "the active model profile does not declare this credential",
                    {"key": key, "expected": list(declared)},
                )
            if not value.strip():
                raise _BadRequestError(
                    credentials.CREDENTIALS_EMPTY, "the credential is empty", {"key": key}
                )
        try:
            for key, value in posted.items():
                credentials.set_token(profile, value, variables, variable=declared[key].env)
        except ConfigError as exc:
            raise _credentials_refused(exc) from None
        return Response(status_code=204)

    # ---- administration (the "live database" screen) ---------------------------------------
    @router.get("/admin/sessions")
    async def admin_sessions(
        limit: Annotated[int | None, Query(ge=1)] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JSONResponse:
        """Every session of the store, newest first, unfiltered."""
        size = page_limit(limit)
        items = manager.list_sessions(limit=size, offset=offset)
        return JSONResponse(
            content={
                "items": _dump_all(items),
                "limit": size,
                "offset": offset,
                "next_offset": offset + size if len(items) == size else None,
            }
        )

    @router.get("/admin/events")
    async def admin_events(
        limit: Annotated[int | None, Query(ge=1)] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JSONResponse:
        """The audited events of every session, newest first, without the chain columns.

        Same rows as ``/admin/audit`` read as events rather than as a chain: identifiers,
        ``event_type``, ``timestamp`` and ``payload``. The hashes belong to the audit view.
        """
        events = []
        for event in audit_events_of_every_session():
            dumped = _dump(event)
            dumped.pop("previous_event_hash", None)
            dumped.pop("event_hash", None)
            events.append(dumped)
        return page(events, page_limit(limit), offset)

    @router.get("/admin/audit")
    async def admin_audit(
        limit: Annotated[int | None, Query(ge=1)] = None,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> JSONResponse:
        """The audit chain of every session, newest first, hashes included (§3.6, ADR-017).

        Verifying a chain stays per session (``GET /sessions/{sid}/audit/verify``): the chain is
        built per session and only means something there.
        """
        return page(_dump_all(audit_events_of_every_session()), page_limit(limit), offset)

    @router.post("/admin/reset-database", status_code=204)
    async def reset_database() -> Response:
        """Empty the store — every session, every record, the audit chain included.

        Gated by ``api.allow_destructive_admin`` (``false`` by default): the route then answers
        ``403 ADMIN_DISABLED`` and touches **nothing**. The schema is not migrated, dropped or
        recreated — only the rows go, so a reset database is exactly a fresh one.
        """
        if not config.api.allow_destructive_admin:
            raise _ForbiddenError(
                "ADMIN_DISABLED",
                "destructive administration is disabled (api.allow_destructive_admin)",
                {"setting": "api.allow_destructive_admin"},
            )
        manager.store.reset()
        return Response(status_code=204)

    app.include_router(router)
    return app


# ------------------------------------------------------------------------------------------------
# credentials (ADR-025 §6, ADR-027 §3): read the posted values without ever handing one back
# ------------------------------------------------------------------------------------------------
async def _posted_credentials(request: Request) -> dict[str, str]:
    """The posted credentials as ``{key: value}``, or a 422 that never quotes a value.

    The body is read here rather than through a pydantic parameter on purpose: a validation error
    of FastAPI carries the rejected ``input`` back to the client, which for this one route would be
    the secrets themselves (a misspelled field name is enough for pydantic to report the whole
    body). Every refusal below is written by hand for the same reason.

    ``{"credentials": {…}}`` is the shape; ``{"token": "…"}`` is the historical one-field alias and
    means ``{"credentials": {"access_token": "…"}}``. Unknown keys of the body itself are ignored.
    """
    try:
        body = json.loads(await request.body())
    except ValueError:
        raise _UnprocessableError("VALIDATION_ERROR", "the body must be a JSON object") from None
    if not isinstance(body, dict):
        raise _UnprocessableError("VALIDATION_ERROR", "the body must be a JSON object")
    if "credentials" in body:
        posted = body["credentials"]
        if not isinstance(posted, dict):
            raise _UnprocessableError(
                "VALIDATION_ERROR", 'the "credentials" field must be an object'
            )
        values: dict[str, str] = {}
        for key, value in posted.items():
            if not isinstance(value, str):
                raise _UnprocessableError(
                    "VALIDATION_ERROR", "every credential must be a string", {"key": str(key)}
                )
            values[str(key)] = value
        return values
    if "token" in body:
        token = body["token"]
        if not isinstance(token, str):
            raise _UnprocessableError("VALIDATION_ERROR", 'the "token" field must be a string')
        return {DEFAULT_CREDENTIAL_KEY: token}
    raise _UnprocessableError("VALIDATION_ERROR", 'the body must carry a "credentials" object')


def _credentials_refused(exc: ConfigError) -> _ApiError:
    """``CREDENTIALS_EMPTY`` -> 400, ``CREDENTIALS_NOT_CONFIGURED`` -> 409; details name the
    variable and the provider, which are public (``config.toml``, ``ModelProfileView``).

    A safety net: the route validates every posted entry against the declared fields before
    writing any of them, so ``credentials.set_token`` is not expected to refuse anything.
    """
    code = exc.error.error_code
    details = dict(exc.error.details)
    if code == credentials.CREDENTIALS_EMPTY:
        return _BadRequestError(code, "the credential is empty", details)
    return _ConflictError(code, "the active model profile takes no credential", details)


# ------------------------------------------------------------------------------------------------
# error handlers
# ------------------------------------------------------------------------------------------------
class _ApiError(Exception):
    """An error raised by a route with its own status and code (uniform body)."""

    status = 500

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error = _normalized(code, message, **(details or {}))


class _BadRequestError(_ApiError):
    status = 400


class _ForbiddenError(_ApiError):
    status = 403


class _NotFoundError(_ApiError):
    status = 404


class _ConflictError(_ApiError):
    status = 409


class _UnprocessableError(_ApiError):
    status = 422


def _install_error_handlers(app: FastAPI) -> None:
    async def on_app_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, AppError)
        return _error_response(status_for(exc), exc.error)

    async def on_api_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, _ApiError)
        return _error_response(exc.status, exc.error)

    async def on_key_error(_: Request, exc: Exception) -> Response:
        message = exc.args[0] if exc.args else "not found"
        return _error_response(404, _normalized("NOT_FOUND", str(message)))

    async def on_value_error(_: Request, exc: Exception) -> Response:
        return _error_response(409, _normalized("CONFLICT", str(exc)))

    async def on_validation_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, RequestValidationError)
        errors = jsonable_encoder(exc.errors())
        return _error_response(
            422, _normalized("VALIDATION_ERROR", "request validation failed", errors=errors)
        )

    async def on_http_error(_: Request, exc: Exception) -> Response:
        assert isinstance(exc, StarletteHTTPException)
        error = _normalized(f"HTTP_{exc.status_code}", str(exc.detail))
        return _error_response(exc.status_code, error)

    app.add_exception_handler(AppError, on_app_error)
    app.add_exception_handler(_ApiError, on_api_error)
    app.add_exception_handler(KeyError, on_key_error)
    app.add_exception_handler(ValueError, on_value_error)
    app.add_exception_handler(RequestValidationError, on_validation_error)
    app.add_exception_handler(StarletteHTTPException, on_http_error)
