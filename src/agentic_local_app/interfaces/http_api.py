"""Local HTTP API (ADR-002, ADR-018): REST resources for the state, SSE for the live stream.

``create_app(manager)`` builds a FastAPI application under ``/api/v1`` on top of the
``ConversationManager`` façade (§3.1) — described here by the :class:`ConversationManagerLike`
protocol so that this module never imports the orchestration package. There is no state on the
server side other than the store (read through the façade) and the per-client SSE queues of the
:class:`~agentic_local_app.interfaces.sse.SseBroker`.

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
from collections.abc import AsyncIterator, Callable, Iterable
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

from agentic_local_app import __version__
from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock, SystemClock
from agentic_local_app.domain.errors import AppError, ErrorType, NormalizedError, Severity
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.models import PlanRecord, SessionBudget, SessionRecord, TaskRecord
from agentic_local_app.domain.states import (
    MessageDirection,
    OutputStream,
    SessionState,
    TaskState,
)
from agentic_local_app.execution.payload_guard import ChunkError, PayloadGuard, decode_output
from agentic_local_app.interfaces.sse import SseBroker
from agentic_local_app.interruption.handler import InterruptionReport
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker, RuntimeSnapshot
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "API_PREFIX",
    "DEFAULT_OUTPUT_MAX_BYTES",
    "DEFAULT_SSE_HEARTBEAT_S",
    "ConversationManagerLike",
    "create_app",
    "status_for",
]

API_PREFIX = "/api/v1"
API_ORIGIN = "http_api"
DEFAULT_OUTPUT_MAX_BYTES = 65_536
DEFAULT_SSE_HEARTBEAT_S = 15.0
_UNBOUNDED = 1_000_000_000


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
        goal: str,
        user_message: str,
        budget: SessionBudget | None = None,
        auto_close: bool | None = None,
    ) -> SessionRecord: ...

    async def continue_session(self, session_id: str, user_message: str) -> SessionRecord: ...

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

    def running_task_ids(self, session_id: str) -> list[str]: ...

    async def shutdown(self) -> None: ...


# ------------------------------------------------------------------------------------------------
# request bodies
# ------------------------------------------------------------------------------------------------
class CreateSessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    goal: str = Field(min_length=1)
    user_message: str = Field(min_length=1)
    session_budget: SessionBudget | None = None
    auto_close_on_final_answer: bool | None = None


class FollowUpRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_message: str = Field(min_length=1)


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
) -> FastAPI:
    """The API over ``manager``. ``clock`` dates the SSE ``dropped`` frames (defaults to the
    manager's own clock when it exposes one, else the system clock); ``sse_heartbeat_s`` is the
    keep-alive period of the streams (``None`` disables it, as the tests do)."""
    config = manager.config
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

    # ---- sessions -------------------------------------------------------------------------
    @router.post("/sessions", status_code=201)
    async def create_session(body: CreateSessionRequest) -> JSONResponse:
        session = await manager.start_session(
            goal=body.goal,
            user_message=body.user_message,
            budget=body.session_budget,
            auto_close=body.auto_close_on_final_answer,
        )
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

    @router.post("/sessions/{sid}/messages", status_code=202)
    async def follow_up(sid: str, body: FollowUpRequest) -> JSONResponse:
        require_session(sid)
        session = await manager.continue_session(sid, body.user_message)
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

    @router.get("/sessions/{sid}/failures")
    async def list_failures(sid: str) -> JSONResponse:
        require_session(sid)
        return JSONResponse(content=_dump_all(manager.store.list_failures(sid)))

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

    app.include_router(router)
    return app


# ------------------------------------------------------------------------------------------------
# error handlers
# ------------------------------------------------------------------------------------------------
class _ApiError(Exception):
    """An error raised by a route with its own status and code (uniform body)."""

    status = 500

    def __init__(self, code: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error = _normalized(code, message, **(details or {}))


class _NotFoundError(_ApiError):
    status = 404


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
