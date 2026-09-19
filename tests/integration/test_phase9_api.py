"""Phase 9b — local HTTP API (REST + SSE) of ADR-002 / ADR-018, tested against the
``FakeConversationManager`` double of the phase 9a façade (spec §3.1, §4, §17.3, §19 ; ADR-015,
ADR-017).

No server, no port, no network: every request goes through ``httpx.ASGITransport`` (REST) or a
small streaming ASGI transport (SSE, because ``httpx.ASGITransport`` buffers the whole body and
would never return on an endless stream). Time and identifiers are the injected ``FakeClock`` /
``SequentialIdGenerator`` of the double.

Sections: harness · sessions · snapshot · interruption and follow-up · conversations, plans, tasks
· task output by range · messages, failures, audit · metrics, health, config, CORS · uniform errors
· SSE broker · SSE routes.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from agentic_local_app import __version__
from agentic_local_app.config import (
    ApiSection,
    AppConfig,
    BudgetSection,
    CredentialField,
    ModelsSection,
    SkillsSection,
    TransportSection,
)
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import BudgetExceededError, ErrorType, PersistenceError
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.models import SessionBudget
from agentic_local_app.domain.states import (
    ConversationState,
    MessageDirection,
    MessageType,
    OutputStream,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.identity import UserIdentity
from agentic_local_app.interfaces.http_api import (
    API_PREFIX,
    ConversationManagerLike,
    create_app,
    status_for,
)
from agentic_local_app.interfaces.sse import (
    DROPPED_EVENT,
    SSE_SUBSCRIBER_NAME,
    SseBroker,
    SseFrame,
    parse_event_id,
)
from agentic_local_app.observability.event_bus import EventBus
from integration.fake_manager import FakeConversationManager

pytestmark = pytest.mark.phase9

BOUND_S = 2.0
TOKEN_ENV = "AGENTIC_TRANSPORT_TOKEN_PHASE9_TEST"
CHAT_ENV = "AGENTIC_CHAT_ID_PHASE9_TEST"
#: ADR-027 §1: a profile that needs more than a token — the shape the front renders field by field.
TWO_CREDENTIAL_FIELDS = [
    CredentialField(
        key="access_token", label="Access token", placeholder="Paste it", env=TOKEN_ENV
    ),
    CredentialField(key="chat_id", label="Chat id", secret=False, env=CHAT_ENV),
]


# ================================================================================================
# harness
# ================================================================================================
def _config(**api: Any) -> AppConfig:
    return AppConfig(
        api=ApiSection(**api),
        budget=BudgetSection(default_max_cycles=7, default_max_plans=3),
        transport=TransportSection(token_env=TOKEN_ENV),
    )


@pytest.fixture
def manager() -> FakeConversationManager:
    return FakeConversationManager(_config())


@pytest.fixture
def app(manager: FakeConversationManager) -> FastAPI:
    return create_app(manager, clock=manager.clock, sse_heartbeat_s=None)


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


@pytest.fixture
async def stream_client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=StreamingASGITransport(app), base_url="http://testserver"
    ) as client:
        yield client


def _url(path: str) -> str:
    return f"{API_PREFIX}{path}"


async def _start(
    manager: FakeConversationManager, goal: str = "goal", message: str = "message"
) -> str:
    session = await manager.start_session(goal=goal, user_message=message)
    return session.session_id


def _running_plan(manager: FakeConversationManager, sid: str, plan_id: str = "plan-1") -> None:
    """A RUNNING plan with one COMPLETED, one RUNNING and one PENDING task."""
    manager.add_plan(
        sid,
        plan_id,
        [
            {"task_id": "t1", "cmd": "echo one", "status": TaskState.COMPLETED, "critical": True},
            {"task_id": "t2", "cmd": "echo two", "status": TaskState.RUNNING},
            {"task_id": "t3", "cmd": "echo three", "depends_on": ["t2"], "resource_lock": "pom"},
        ],
        status=PlanState.RUNNING,
    )


def _error(response: httpx.Response) -> dict[str, Any]:
    body = response.json()
    assert set(body) == {"error"}, body
    error = body["error"]
    assert {
        "error_type",
        "error_code",
        "severity",
        "origin",
        "retryable",
        "recoverable",
        "attempt",
        "max_attempts",
        "details",
    } <= set(error), error
    return dict(error)


# ---- SSE helpers ---------------------------------------------------------------------------
class _ChunkStream(httpx.AsyncByteStream):
    def __init__(
        self,
        chunks: asyncio.Queue[bytes | None],
        task: asyncio.Task[None],
        disconnected: asyncio.Event,
    ) -> None:
        self._chunks = chunks
        self._task = task
        self._disconnected = disconnected

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self._chunks.get()
            if chunk is None:
                return
            yield chunk

    async def aclose(self) -> None:
        """Client gone: the app sees ``http.disconnect`` and must finish on its own."""
        self._disconnected.set()
        if not self._task.done():
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=BOUND_S)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        if self._task.done() and not self._task.cancelled():
            self._task.exception()  # retrieved


class StreamingASGITransport(httpx.AsyncBaseTransport):
    """``httpx.ASGITransport`` runs the app to completion before returning the response, which never
    happens for an endless SSE stream. This transport hands body chunks over as the application
    sends them and answers ``http.disconnect`` once the client closes the response."""

    def __init__(self, app: FastAPI) -> None:
        self._app = app

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)
        scope: dict[str, Any] = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for (k, v) in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port),
            "client": ("127.0.0.1", 123),
            "root_path": "",
        }
        body_chunks = request.stream.__aiter__()
        request_complete = False
        chunks: asyncio.Queue[bytes | None] = asyncio.Queue()
        started = asyncio.Event()
        disconnected = asyncio.Event()
        status: dict[str, Any] = {}

        async def receive() -> dict[str, Any]:
            nonlocal request_complete
            if not request_complete:
                try:
                    body = await body_chunks.__anext__()
                except StopAsyncIteration:
                    request_complete = True
                    return {"type": "http.request", "body": b"", "more_body": False}
                return {"type": "http.request", "body": body, "more_body": True}
            await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
                status["headers"] = message.get("headers", [])
                started.set()
            elif message["type"] == "http.response.body":
                body = message.get("body", b"")
                if body:
                    chunks.put_nowait(body)
                if not message.get("more_body", False):
                    chunks.put_nowait(None)

        task = asyncio.create_task(self._app(scope, receive, send))
        waiter = asyncio.ensure_future(started.wait())
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if not started.is_set():
            waiter.cancel()
            task.result()  # raises the application error
            raise RuntimeError("application finished without a response")
        return httpx.Response(
            status["code"],
            headers=status["headers"],
            stream=_ChunkStream(chunks, task, disconnected),
            request=request,
        )


@dataclass(frozen=True)
class Frame:
    id: str | None
    event: str | None
    data: str | None
    comment: str | None = None

    @property
    def json(self) -> dict[str, Any]:
        assert self.data is not None
        return dict(json.loads(self.data))


def parse_frames(text: str) -> list[Frame]:
    """Parse ``text`` (a full or partial SSE body) into frames, blank-line separated."""
    frames: list[Frame] = []
    for block in re.split(r"\n\n", text):
        if not block.strip():
            continue
        fid = event = comment = None
        data_lines: list[str] = []
        for line in block.split("\n"):
            if line.startswith(":"):
                comment = line[1:].strip()
            elif line.startswith("id:"):
                fid = line[3:].strip()
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
        frames.append(
            Frame(fid, event, "\n".join(data_lines) if data_lines else None, comment=comment)
        )
    return frames


async def read_frames(
    client: httpx.AsyncClient,
    url: str,
    count: int,
    *,
    headers: dict[str, str] | None = None,
    after_start: Callable[[], None] | None = None,
) -> tuple[httpx.Response, list[Frame]]:
    """Open the SSE stream, run ``after_start`` once the headers are in, read ``count`` frames
    (comments excluded) then close the response (the client disconnects)."""
    frames: list[Frame] = []
    async with client.stream("GET", url, headers=headers) as response:
        if response.status_code != 200:
            await response.aread()
            return response, frames
        if after_start is not None:
            await _settle()  # the generator has registered its client on the broker
            after_start()
        buffer = ""
        async for chunk in response.aiter_text():
            buffer += chunk
            while "\n\n" in buffer:
                block, buffer = buffer.split("\n\n", 1)
                for frame in parse_frames(block + "\n\n"):
                    if frame.comment is None:
                        frames.append(frame)
            if len(frames) >= count:
                break
    return response, frames[:count]


async def collect(iterator: AsyncIterator[SseFrame], count: int) -> list[SseFrame]:
    """Read up to ``count`` frames from a broker subscription (fewer when it ends), bounded in
    real time, then close it."""
    frames: list[SseFrame] = []
    try:
        while len(frames) < count:
            try:
                frame = await asyncio.wait_for(iterator.__anext__(), timeout=BOUND_S)
            except StopAsyncIteration:
                break
            frames.append(frame)
    finally:
        await iterator.aclose()
    return frames


async def _settle(rounds: int = 5) -> None:
    for _ in range(rounds):
        await asyncio.sleep(0)


# ================================================================================================
# sessions
# ================================================================================================
async def given_valid_request_when_session_created_then_201_with_record_and_default_budget(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    response = await client.post(
        _url("/sessions"), json={"goal": "fix the build", "user_message": "mvn fails"}
    )
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["session_id"] == "sess-0001"
    assert body["status"] == "RUNNING"
    assert body["goal"] == "fix the build"
    assert body["user_message"] == "mvn fails"
    assert body["auto_close_on_final_answer"] is False
    assert body["budget"] == {"max_cycles": 7, "max_plans": 3, "max_total_duration_ms": 300_000}
    assert body["created_at"] == "2026-01-01T00:00:00Z"
    assert manager.calls[0] == ("start_session", "fix the build", "mvn fails", None, None)
    assert manager.get_session("sess-0001") is not None


async def given_explicit_budget_and_auto_close_when_session_created_then_forwarded_to_manager(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    response = await client.post(
        _url("/sessions"),
        json={
            "goal": "g",
            "user_message": "m",
            "session_budget": {"max_cycles": 2, "max_plans": 1, "max_total_duration_ms": 1000},
            "auto_close_on_final_answer": True,
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["budget"] == {
        "max_cycles": 2,
        "max_plans": 1,
        "max_total_duration_ms": 1000,
    }
    assert response.json()["auto_close_on_final_answer"] is True
    budget = SessionBudget(max_cycles=2, max_plans=1, max_total_duration_ms=1000)
    assert manager.calls[0] == ("start_session", "g", "m", budget, True)


@pytest.mark.parametrize(
    "body",
    [
        {"goal": "", "user_message": "m"},
        {"goal": "g", "user_message": ""},
        {"goal": "g", "user_message": "m", "user_id": ""},
        {"goal": "g", "user_message": "m", "session_budget": {"max_cycles": 0}},
        {"goal": "g", "user_message": "m", "unknown": 1},
    ],
    ids=["empty_goal", "empty_message", "empty_user_id", "invalid_budget", "unknown_field"],
)
async def given_invalid_body_when_session_created_then_422_with_uniform_error(
    client: httpx.AsyncClient, manager: FakeConversationManager, body: dict[str, Any]
) -> None:
    response = await client.post(_url("/sessions"), json=body)
    assert response.status_code == 422, response.text
    error = _error(response)
    assert error["error_code"] == "VALIDATION_ERROR"
    assert error["error_type"] == "SYSTEM_ERROR"
    assert error["details"]["errors"]
    assert manager.calls == []


async def given_three_sessions_when_listed_with_status_filter_then_only_matching_newest_first(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    first = await _start(manager)
    second = await _start(manager)
    third = await _start(manager)
    manager.complete(second)

    response = await client.get(_url("/sessions"))
    assert response.status_code == 200
    body = response.json()
    assert [s["session_id"] for s in body["items"]] == [third, second, first]
    assert body["limit"] == 100 and body["offset"] == 0 and body["next_offset"] is None

    response = await client.get(_url("/sessions"), params={"status": "running"})
    assert [s["session_id"] for s in response.json()["items"]] == [third, first]

    response = await client.get(_url("/sessions"), params={"status": "Completed,ready"})
    assert [s["session_id"] for s in response.json()["items"]] == [second]


async def given_sessions_when_listed_with_limit_and_offset_then_page_with_next_offset(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    ids = [await _start(manager) for _ in range(5)]
    response = await client.get(_url("/sessions"), params={"limit": 2, "offset": 1})
    body = response.json()
    assert [s["session_id"] for s in body["items"]] == [ids[3], ids[2]]
    assert body == {**body, "limit": 2, "offset": 1, "next_offset": 3}
    response = await client.get(_url("/sessions"), params={"limit": 2, "offset": 3})
    assert [s["session_id"] for s in response.json()["items"]] == [ids[1], ids[0]]
    assert response.json()["next_offset"] == 5
    response = await client.get(_url("/sessions"), params={"limit": 2, "offset": 5})
    assert response.json()["items"] == [] and response.json()["next_offset"] is None


async def given_manager_with_small_page_size_when_listed_without_limit_then_page_size_applies() -> (
    None
):
    manager = FakeConversationManager(_config(page_size=2))
    for _ in range(3):
        await _start(manager)
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as client:
        body = (await client.get(_url("/sessions"))).json()
    assert len(body["items"]) == 2 and body["limit"] == 2 and body["next_offset"] == 2


@pytest.mark.parametrize("params", [{"status": "bogus"}, {"limit": 0}, {"offset": -1}])
async def given_invalid_list_parameters_when_sessions_listed_then_422(
    client: httpx.AsyncClient, params: dict[str, Any]
) -> None:
    response = await client.get(_url("/sessions"), params=params)
    assert response.status_code == 422, response.text
    assert _error(response)["error_code"] == "VALIDATION_ERROR"


async def given_session_when_fetched_then_record_with_current_conversation(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.get(_url(f"/sessions/{sid}"))
    assert response.status_code == 200
    body = response.json()
    record = manager.require_session(sid).model_dump(mode="json")
    assert {k: v for k, v in body.items() if k != "conversation"} == record
    assert body["conversation"]["conversation_id"] == "conv-0001"
    assert body["conversation"]["status"] == "WAITING_MODEL_RESPONSE"


async def given_unknown_session_when_fetched_then_404_with_normalized_error(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(_url("/sessions/nope"))
    assert response.status_code == 404
    error = _error(response)
    assert error["error_code"] == "NOT_FOUND"
    assert error["error_type"] == "SYSTEM_ERROR"
    assert "nope" in error["details"]["message"]


# ================================================================================================
# snapshot
# ================================================================================================
SNAPSHOT_KEYS = {
    "session",
    "conversation",
    "conversations",
    "cycle",
    "plan",
    "tasks",
    "running_task_ids",
    "model_interaction",
    "last_event_type",
    "last_event_sequence",
    "snapshot_at",
}
CONVERSATION_KEYS_4_1 = {
    "conversation_id",
    "parent_conversation_id",
    "status",
    "auto_close_on_final_answer",
    "context_window_state",
    "last_model_response_state",
    "current_cycle_id",
    "current_plan_id",
    "last_completed_plan_id",
    "final_answer_received",
    "interrupted_at",
    "session_budget",
    "created_at",
    "updated_at",
}
CYCLE_KEYS_4_1 = {
    "cycle_id",
    "cycle_type",
    "status",
    "started_at",
    "ended_at",
    "retry_count",
    "conversation_id",
}
PLAN_KEYS_4_1 = {
    "plan_id",
    "plan_type",
    "objective",
    "execution_policy",
    "max_parallel_workers",
    "status",
    "stop_reason",
    "task_count",
    "completed_task_count",
    "failed_task_count",
    "skipped_task_count",
    "cancelled_task_count",
    "interrupted_task_count",
    "started_at",
    "ended_at",
}
TASK_KEYS_4_1 = {
    "task_id",
    "plan_id",
    "type",
    "cmd",
    "status",
    "critical",
    "continue_on_error",
    "stop_plan_on_failure",
    "stop_plan_on_success",
    "depends_on",
    "resource_lock",
    "max_output_bytes",
    "attempt_count",
    "exit_code",
    "truncated",
    "original_size_bytes",
    "started_at",
    "ended_at",
    "duration_ms",
}
MODEL_KEYS_4_1 = {
    "last_outbound_message_type",
    "last_inbound_message_type",
    "last_post_status",
    "last_get_status",
    "last_protocol_validation_status",
    # ADR-023: where the correction budget stands, derived from the messages, never persisted
    "correction_attempt",
    "correction_max_attempts",
}


async def given_running_session_when_snapshot_requested_then_complete_4_1_snapshot_equal_to_tracker(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_cycle(sid)
    _running_plan(manager, sid)
    manager.publish(
        EventType.MESSAGE_OUTBOUND,
        sid,
        payload={
            "message_type": "user_request",
            "message_id": "msg-1",
            "post_status": 202,
            "size_bytes": 10,
        },
    )
    manager.clock.advance(1_500)

    response = await client.get(_url(f"/sessions/{sid}/snapshot"))
    assert response.status_code == 200
    body = response.json()
    assert set(body) == SNAPSHOT_KEYS
    assert body == manager.snapshot(sid).model_dump(mode="json")
    assert CONVERSATION_KEYS_4_1 <= set(body["conversation"])
    assert set(body["cycle"]) == CYCLE_KEYS_4_1
    assert set(body["plan"]) == PLAN_KEYS_4_1
    assert TASK_KEYS_4_1 <= set(body["tasks"][0])
    assert set(body["model_interaction"]) == MODEL_KEYS_4_1
    assert body["session"]["session_budget"] == {
        "max_cycles": 7,
        "max_plans": 3,
        "max_total_duration_ms": 300_000,
        "consumed_cycles": 0,
        "consumed_plans": 0,
        "consumed_duration_ms": 1_500,
    }
    assert body["plan"]["task_count"] == 3 and body["plan"]["completed_task_count"] == 1
    assert body["running_task_ids"] == ["t2"]
    assert [t["task_id"] for t in body["tasks"]] == ["t1", "t2", "t3"]
    assert body["model_interaction"]["last_post_status"] == 202
    assert body["last_event_type"] == "message.outbound"
    assert body["last_event_sequence"] == manager.audit.last(sid).sequence  # type: ignore[union-attr]
    assert body["snapshot_at"] == "2026-01-01T00:00:01.500000Z"


async def given_unknown_session_when_snapshot_requested_then_404(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(_url("/sessions/sess-9999/snapshot"))
    assert response.status_code == 404
    assert _error(response)["error_code"] == "NOT_FOUND"


# ================================================================================================
# interruption and follow-up messages
# ================================================================================================
async def given_running_session_when_interrupted_then_report_returned_and_session_ready(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    response = await client.post(_url(f"/sessions/{sid}/interrupt"))
    assert response.status_code == 200, response.text
    report = response.json()
    assert report["session_id"] == sid
    assert report["reason"] == "user_interrupt"
    assert report["session_status"] == "READY"
    assert report["nothing_to_interrupt"] is False
    assert report["interrupted_task_ids"] == ["t2", "t3"]
    assert report["plan_id"] == "plan-1"
    assert report["conversation_id"] == "conv-0001"
    assert report["requested_at"] == "2026-01-01T00:00:00Z"
    assert manager.require_session(sid).status is SessionState.READY
    assert manager.calls[-1] == ("interrupt", sid)


async def given_idle_session_when_interrupted_then_nothing_to_interrupt_report(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.complete(sid)
    response = await client.post(_url(f"/sessions/{sid}/interrupt"))
    assert response.status_code == 200
    assert response.json()["nothing_to_interrupt"] is True
    assert response.json()["session_status"] == "COMPLETED"


async def given_unknown_session_when_interrupted_then_404(client: httpx.AsyncClient) -> None:
    response = await client.post(_url("/sessions/sess-9999/interrupt"))
    assert response.status_code == 404


async def given_completed_reusable_session_when_follow_up_posted_then_202_and_session_running(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.complete(sid)
    response = await client.post(
        _url(f"/sessions/{sid}/messages"), json={"user_message": "and now the tests"}
    )
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "RUNNING"
    assert response.json()["user_message"] == "and now the tests"
    assert manager.calls[-1] == ("continue_session", sid, "and now the tests")
    conversation = manager.require_conversation(sid)
    assert conversation.status is ConversationState.WAITING_MODEL_RESPONSE


async def given_running_session_when_follow_up_posted_then_409_session_busy(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """The front blocks its send button while a session runs; this is the belt behind it."""
    sid = await _start(manager)
    response = await client.post(_url(f"/sessions/{sid}/messages"), json={"user_message": "m"})
    assert response.status_code == 409, response.text
    error = _error(response)
    assert error["error_code"] == "SESSION_BUSY"
    assert error["details"] == {
        "message": f"session {sid} is still running",
        "session_id": sid,
        "status": "RUNNING",
    }
    assert ("continue_session", sid, "m") not in manager.calls  # the façade was never called


async def given_failed_session_when_follow_up_posted_then_409_conflict(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """A session that is not busy but not reusable either keeps the generic conflict."""
    sid = await _start(manager)
    manager.fail(sid)
    response = await client.post(_url(f"/sessions/{sid}/messages"), json={"user_message": "m"})
    assert response.status_code == 409, response.text
    error = _error(response)
    assert error["error_code"] == "CONFLICT"
    assert "not reusable" in error["details"]["message"]


async def given_follow_up_without_message_when_posted_then_422(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.post(_url(f"/sessions/{sid}/messages"), json={})
    assert response.status_code == 422


async def given_unknown_session_when_follow_up_posted_then_404(
    client: httpx.AsyncClient,
) -> None:
    response = await client.post(_url("/sessions/nope/messages"), json={"user_message": "m"})
    assert response.status_code == 404


async def given_completed_session_when_final_answer_requested_then_answer_returned(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.get(_url(f"/sessions/{sid}/final-answer"))
    assert response.status_code == 200
    assert response.json() == {"session_id": sid, "final_answer": None}
    manager.complete(sid, {"status": "success", "summary": "Build fixed", "details": {"n": 1}})
    response = await client.get(_url(f"/sessions/{sid}/final-answer"))
    assert response.json() == {
        "session_id": sid,
        "final_answer": {"status": "success", "summary": "Build fixed", "details": {"n": 1}},
    }
    assert (await client.get(_url("/sessions/nope/final-answer"))).status_code == 404


# ---- ADR-022: the model's direct responses and the last reply of either kind ------------------
async def given_session_without_reply_when_responses_and_reply_requested_then_empty_and_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.get(_url(f"/sessions/{sid}/responses"))
    assert response.status_code == 200 and response.json() == []
    response = await client.get(_url(f"/sessions/{sid}/reply"))
    assert response.status_code == 404
    error = _error(response)
    assert error["error_code"] == "REPLY_NOT_FOUND" and sid in error["details"]["message"]
    assert (await client.get(_url("/sessions/nope/responses"))).status_code == 404
    assert (await client.get(_url("/sessions/nope/reply"))).status_code == 404


async def given_user_response_received_when_responses_requested_then_listed_with_content(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_cycle(sid)
    manager.respond(sid, "## Analysis\n\nThe target release is wrong.", format="markdown")
    response = await client.get(_url(f"/sessions/{sid}/responses"))
    assert response.status_code == 200, response.text
    items = response.json()
    assert len(items) == 1
    assert items[0] == {
        "message_id": "model-msg-0001",
        "conversation_id": "conv-0001",
        "cycle_id": "cyc-0001",
        "received_at": manager.store.list_messages("conv-0001")[0].model_dump(mode="json")[
            "received_at"
        ],
        "format": "markdown",
        "body": "## Analysis\n\nThe target release is wrong.",
        "status": "completed",
        "expects_reply": False,
    }
    assert items == manager.user_responses(sid)
    # the final-answer route stays what it was: a user_response is not a final answer
    response = await client.get(_url(f"/sessions/{sid}/final-answer"))
    assert response.json() == {"session_id": sid, "final_answer": None}


async def given_user_response_received_when_reply_requested_then_newest_reply_returned(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.respond(sid, "Which module fails?", expects_reply=True)
    response = await client.get(_url(f"/sessions/{sid}/reply"))
    assert response.status_code == 200, response.text
    reply = response.json()
    assert reply["type"] == "user_response"
    assert reply["message_id"] == "model-msg-0001"
    assert reply["conversation_id"] == "conv-0001"
    assert reply["content"] == {
        "format": "text",
        "body": "Which module fails?",
        "status": "completed",
        "expects_reply": True,
    }
    assert reply == manager.last_reply(sid)
    assert manager.require_conversation(sid).status is ConversationState.WAITING_USER
    # the user answers the question through the existing follow-up route
    follow_up = await client.post(
        _url(f"/sessions/{sid}/messages"), json={"user_message": "service-api only"}
    )
    assert follow_up.status_code == 202, follow_up.text
    assert manager.require_conversation(sid).status is ConversationState.WAITING_MODEL_RESPONSE
    # a final_answer afterwards becomes the newest reply; the responses list keeps the question
    manager.complete(sid, {"status": "success", "summary": "Fixed"})
    manager.add_message(
        sid,
        direction=MessageDirection.INBOUND,
        message_type=MessageType.FINAL_ANSWER,
        payload={
            "type": "final_answer",
            "conversation_id": "conv-0001",
            "message_id": "model-msg-0002",
            "content": {"status": "success", "summary": "Fixed"},
        },
        message_id="model-msg-0002",
    )
    reply = (await client.get(_url(f"/sessions/{sid}/reply"))).json()
    assert reply["type"] == "final_answer" and reply["message_id"] == "model-msg-0002"
    assert reply["content"] == {"status": "success", "summary": "Fixed"}
    assert [
        r["message_id"] for r in (await client.get(_url(f"/sessions/{sid}/responses"))).json()
    ] == ["model-msg-0001"]


async def given_question_under_auto_close_when_follow_up_posted_then_202_conversation_reused(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    session = await manager.start_session(goal="g", user_message="m", auto_close=True)
    sid = session.session_id
    manager.respond(sid, "Which module?", expects_reply=True)
    assert manager.require_conversation(sid).status is ConversationState.WAITING_USER
    response = await client.post(_url(f"/sessions/{sid}/messages"), json={"user_message": "all"})
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "RUNNING"


async def given_statement_under_auto_close_when_follow_up_posted_then_409(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    session = await manager.start_session(goal="g", user_message="m", auto_close=True)
    sid = session.session_id
    manager.respond(sid, "Done.", expects_reply=False)
    assert manager.require_conversation(sid).status is ConversationState.CLOSED
    response = await client.post(_url(f"/sessions/{sid}/messages"), json={"user_message": "x"})
    assert response.status_code == 409


# ================================================================================================
# conversations, plans, tasks
# ================================================================================================
async def given_interrupted_then_restarted_session_when_conversations_listed_then_chain_in_order(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    await manager.interrupt(sid)
    manager.lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
    child = manager.lifecycle.create_conversation(sid, parent_conversation_id="conv-0001")

    response = await client.get(_url(f"/sessions/{sid}/conversations"))
    assert response.status_code == 200
    items = response.json()
    assert [(c["conversation_id"], c["status"]) for c in items] == [
        ("conv-0001", "INTERRUPTED"),
        (child.conversation_id, "NEW"),
    ]
    assert items[1]["parent_conversation_id"] == "conv-0001"
    assert items[0] == manager.store.get_conversation("conv-0001").model_dump(mode="json")  # type: ignore[union-attr]

    one = await client.get(_url(f"/sessions/{sid}/conversations/{child.conversation_id}"))
    assert one.status_code == 200 and one.json()["conversation_id"] == child.conversation_id


async def given_conversation_of_another_session_when_fetched_then_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    first = await _start(manager)
    second = await _start(manager)
    other = manager.require_conversation(second).conversation_id
    assert (await client.get(_url(f"/sessions/{first}/conversations/{other}"))).status_code == 404
    assert (await client.get(_url(f"/sessions/{first}/conversations/nope"))).status_code == 404
    assert (await client.get(_url("/sessions/nope/conversations"))).status_code == 404


async def given_plans_when_listed_with_include_tasks_then_tasks_embedded_in_plan_order(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_plan(sid, "plan-0", [{"task_id": "d1", "status": TaskState.COMPLETED}])
    _running_plan(manager, sid, "plan-1")

    response = await client.get(_url(f"/sessions/{sid}/plans"))
    assert response.status_code == 200
    plans = response.json()
    assert [p["plan_id"] for p in plans] == ["plan-0", "plan-1"]
    assert "tasks" not in plans[0]
    assert plans[1] == manager.store.get_plan(sid, "plan-1").model_dump(mode="json")  # type: ignore[union-attr]

    response = await client.get(_url(f"/sessions/{sid}/plans"), params={"include": "tasks"})
    plans = response.json()
    assert [t["task_id"] for t in plans[0]["tasks"]] == ["d1"]
    assert [t["task_id"] for t in plans[1]["tasks"]] == ["t1", "t2", "t3"]
    assert plans[1]["tasks"][2]["depends_on"] == ["t2"]

    one = await client.get(_url(f"/sessions/{sid}/plans/plan-1"), params={"include": "tasks"})
    assert one.status_code == 200
    assert one.json()["status"] == "RUNNING"
    assert [t["task_id"] for t in one.json()["tasks"]] == ["t1", "t2", "t3"]
    assert (await client.get(_url(f"/sessions/{sid}/plans/plan-9"))).status_code == 404
    bad = await client.get(_url(f"/sessions/{sid}/plans"), params={"include": "bogus"})
    assert bad.status_code == 422


async def given_running_and_pending_tasks_when_filtered_by_status_running_then_only_running(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_plan(sid, "plan-2", [{"task_id": "u1", "status": TaskState.RUNNING}])

    response = await client.get(_url(f"/sessions/{sid}/tasks"), params={"status": "running"})
    assert response.status_code == 200
    assert [t["task_id"] for t in response.json()] == ["t2", "u1"]
    assert all(t["status"] == "RUNNING" for t in response.json())

    response = await client.get(
        _url(f"/sessions/{sid}/tasks"), params={"status": "RUNNING,pending"}
    )
    assert [t["task_id"] for t in response.json()] == ["t2", "t3", "u1"]

    response = await client.get(_url(f"/sessions/{sid}/tasks"))
    assert [t["task_id"] for t in response.json()] == ["t1", "t2", "t3", "u1"]


async def given_tasks_of_two_plans_when_filtered_by_plan_id_then_only_that_plan(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid, "plan-1")
    manager.add_plan(sid, "plan-2", [{"task_id": "u1"}])
    response = await client.get(_url(f"/sessions/{sid}/tasks"), params={"plan_id": "plan-2"})
    assert [t["task_id"] for t in response.json()] == ["u1"]
    response = await client.get(
        _url(f"/sessions/{sid}/tasks"), params={"plan_id": "plan-1", "status": "completed"}
    )
    assert [t["task_id"] for t in response.json()] == ["t1"]
    bad = await client.get(_url(f"/sessions/{sid}/tasks"), params={"status": "flying"})
    assert bad.status_code == 422


async def given_task_when_fetched_then_full_record_and_404_for_unknown(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    response = await client.get(_url(f"/sessions/{sid}/tasks/t3"))
    assert response.status_code == 200
    body = response.json()
    assert body == manager.store.get_task(sid, "t3").model_dump(mode="json")  # type: ignore[union-attr]
    assert body["resource_lock"] == "pom" and body["depends_on"] == ["t2"]
    assert body["timed_out"] is False and body["reason"] is None
    assert (await client.get(_url(f"/sessions/{sid}/tasks/t9"))).status_code == 404
    assert (await client.get(_url("/sessions/nope/tasks/t1"))).status_code == 404


# ================================================================================================
# task output by range (same engine as chunk_request, ADR-011)
# ================================================================================================
async def given_stdout_blob_when_output_read_by_range_then_exact_data_range_total_and_eof(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_blob(sid, "t1", b"0123456789abcdef")

    response = await client.get(
        _url(f"/sessions/{sid}/tasks/t1/output"),
        params={"stream": "stdout", "offset": 4, "max_bytes": 6},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {
        "task_id": "t1",
        "stream": "stdout",
        "offset": 4,
        "data": "456789",
        "range": [4, 10],
        "total": 16,
        "eof": False,
    }
    response = await client.get(
        _url(f"/sessions/{sid}/tasks/t1/output"), params={"offset": 10, "max_bytes": 100}
    )
    assert response.json()["data"] == "abcdef"
    assert response.json()["range"] == [10, 16] and response.json()["eof"] is True
    whole = await client.get(_url(f"/sessions/{sid}/tasks/t1/output"))
    assert whole.json()["data"] == "0123456789abcdef" and whole.json()["eof"] is True


async def given_stderr_blob_with_invalid_utf8_when_output_read_then_replacement_characters(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_blob(sid, "t1", b"err\xff\xfe!", stream=OutputStream.STDERR)
    response = await client.get(
        _url(f"/sessions/{sid}/tasks/t1/output"), params={"stream": "stderr"}
    )
    assert response.status_code == 200
    assert response.json()["data"] == "err��!"
    assert response.json()["total"] == 6


async def given_task_without_blob_when_output_read_then_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    response = await client.get(_url(f"/sessions/{sid}/tasks/t2/output"))
    assert response.status_code == 404
    assert _error(response)["error_code"] == "CHUNK_REF_NOT_FOUND"
    assert (await client.get(_url(f"/sessions/{sid}/tasks/t9/output"))).status_code == 404
    assert (await client.get(_url("/sessions/nope/tasks/t1/output"))).status_code == 404


@pytest.mark.parametrize(
    "params",
    [{"offset": -1}, {"max_bytes": 0}, {"stream": "both"}, {"offset": "x"}],
    ids=["negative_offset", "zero_max_bytes", "bad_stream", "non_int_offset"],
)
async def given_invalid_output_parameters_when_output_read_then_422(
    client: httpx.AsyncClient, manager: FakeConversationManager, params: dict[str, Any]
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_blob(sid, "t1", b"data")
    response = await client.get(_url(f"/sessions/{sid}/tasks/t1/output"), params=params)
    assert response.status_code == 422, response.text
    assert _error(response)["error_code"] == "VALIDATION_ERROR"


async def given_offset_beyond_total_when_output_read_then_422_range_invalid(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_blob(sid, "t1", b"data")
    response = await client.get(_url(f"/sessions/{sid}/tasks/t1/output"), params={"offset": 5})
    assert response.status_code == 422
    error = _error(response)
    assert error["error_code"] == "CHUNK_RANGE_INVALID"
    assert error["details"]["total"] == 4


async def given_offset_equal_to_total_when_output_read_then_empty_data_and_eof(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    manager.add_blob(sid, "t1", b"data")
    manager.add_blob(sid, "t2", b"")
    response = await client.get(_url(f"/sessions/{sid}/tasks/t1/output"), params={"offset": 4})
    assert response.status_code == 200
    assert response.json() == {
        "task_id": "t1",
        "stream": "stdout",
        "offset": 4,
        "data": "",
        "range": [4, 4],
        "total": 4,
        "eof": True,
    }
    empty = await client.get(_url(f"/sessions/{sid}/tasks/t2/output"))
    assert empty.status_code == 200 and empty.json()["total"] == 0 and empty.json()["eof"]


async def given_max_bytes_above_hard_limit_when_output_read_then_capped(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    hard = manager.config.payload.hard_max_output_bytes
    manager.add_blob(sid, "t1", b"x" * (hard + 10))
    response = await client.get(
        _url(f"/sessions/{sid}/tasks/t1/output"), params={"max_bytes": hard + 10}
    )
    assert response.status_code == 200
    assert len(response.json()["data"]) == hard and response.json()["eof"] is False


# ================================================================================================
# messages, failures, audit
# ================================================================================================
async def given_messages_in_two_conversations_when_listed_then_all_in_order_with_direction_filter(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_message(
        sid, direction=MessageDirection.OUTBOUND, message_type=MessageType.USER_REQUEST
    )
    manager.add_message(
        sid, direction=MessageDirection.INBOUND, message_type=MessageType.EXECUTION_PLAN
    )
    await manager.interrupt(sid)
    manager.lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
    child = manager.lifecycle.create_conversation(sid, parent_conversation_id="conv-0001")
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.USER_REQUEST,
        conversation_id=child.conversation_id,
    )

    response = await client.get(_url(f"/sessions/{sid}/messages"))
    assert response.status_code == 200
    messages = response.json()
    assert [(m["message_id"], m["conversation_id"]) for m in messages] == [
        ("msg-0001", "conv-0001"),
        ("msg-0002", "conv-0001"),
        ("msg-0003", child.conversation_id),
    ]
    assert messages[0] == manager.store.get_message("msg-0001").model_dump(mode="json")  # type: ignore[union-attr]

    response = await client.get(_url(f"/sessions/{sid}/messages"), params={"direction": "in"})
    assert [m["message_id"] for m in response.json()] == ["msg-0002"]
    response = await client.get(_url(f"/sessions/{sid}/messages"), params={"direction": "outbound"})
    assert [m["message_id"] for m in response.json()] == ["msg-0001", "msg-0003"]
    response = await client.get(
        _url(f"/sessions/{sid}/messages"), params={"conversation_id": child.conversation_id}
    )
    assert [m["message_id"] for m in response.json()] == ["msg-0003"]
    assert (
        await client.get(_url(f"/sessions/{sid}/messages"), params={"direction": "sideways"})
    ).status_code == 422
    assert (await client.get(_url("/sessions/nope/messages"))).status_code == 404


async def given_failures_when_listed_then_records_returned_in_order(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_failure(sid)
    manager.add_failure(sid, error_type=ErrorType.TIMEOUT_ERROR, error_code="MODEL_GET_TIMEOUT")
    response = await client.get(_url(f"/sessions/{sid}/failures"))
    assert response.status_code == 200
    failures = response.json()
    assert [f["failure_id"] for f in failures] == ["fail-0001", "fail-0002"]
    assert failures[1]["error_type"] == "TIMEOUT_ERROR"
    assert failures[0] == manager.store.list_failures(sid)[0].model_dump(mode="json")
    assert (await client.get(_url("/sessions/nope/failures"))).status_code == 404


async def given_audit_chain_when_paged_with_after_and_limit_then_contiguous_pages(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)  # 5 audited events
    _running_plan(manager, sid)  # + cycle.started + plan.received
    total = manager.store.count_audit_events(sid)
    assert total == 7

    response = await client.get(_url(f"/sessions/{sid}/audit"), params={"limit": 3})
    assert response.status_code == 200
    page = response.json()
    assert [e["sequence"] for e in page["items"]] == [1, 2, 3]
    assert page["after"] is None and page["limit"] == 3 and page["next_after"] == 3
    assert page["items"][0] == manager.store.list_audit_events(sid)[0].model_dump(mode="json")
    assert page["items"][0]["event_type"] == "session.created"

    response = await client.get(_url(f"/sessions/{sid}/audit"), params={"after": 3, "limit": 3})
    page = response.json()
    assert [e["sequence"] for e in page["items"]] == [4, 5, 6]
    assert page["after"] == 3 and page["next_after"] == 6

    response = await client.get(_url(f"/sessions/{sid}/audit"), params={"after": 6, "limit": 3})
    page = response.json()
    assert [e["sequence"] for e in page["items"]] == [7]
    assert page["next_after"] is None
    assert (await client.get(_url("/sessions/nope/audit"))).status_code == 404
    bad = await client.get(_url(f"/sessions/{sid}/audit"), params={"after": -1})
    assert bad.status_code == 422


async def given_audit_chain_when_verified_then_valid_and_tampered_chain_reported(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.get(_url(f"/sessions/{sid}/audit/verify"))
    assert response.status_code == 200
    assert response.json() == {
        "valid": True,
        "checked": 5,
        "first_broken_sequence": None,
        "reason": None,
        "verified_at": "2026-01-01T00:00:00Z",
    }
    chain = manager.store._audit[sid]
    chain[2] = chain[2].model_copy(update={"payload": {"tampered": True}})
    response = await client.get(_url(f"/sessions/{sid}/audit/verify"))
    assert response.json()["valid"] is False
    assert response.json()["first_broken_sequence"] == 3
    assert response.json()["reason"] == "HASH_MISMATCH"
    assert (await client.get(_url("/sessions/nope/audit/verify"))).status_code == 404


# ================================================================================================
# metrics, health, config, CORS
# ================================================================================================
async def given_events_when_metrics_requested_then_prometheus_text_from_telemetry(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_failure(sid)
    response = await client.get(_url("/metrics"))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert response.text == manager.telemetry.render_text()
    assert 'events_total{event_type="session.created"} 1' in response.text
    assert 'failures_total{error_type="NETWORK_ERROR"} 1' in response.text
    assert "# TYPE tasks_completed_per_minute gauge" in response.text


async def given_manager_when_health_requested_then_status_running_count_and_recovery_report(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    response = await client.get(_url("/health"))
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "version": __version__,
        "sessions_running": 0,
        "recovery_report": None,
    }
    sid = await _start(manager)
    await _start(manager)
    manager.complete(sid)
    manager.recovery_report = {"actions": 2, "sessions_found": 1}
    response = await client.get(_url("/health"))
    assert response.json()["sessions_running"] == 1
    assert response.json()["recovery_report"] == {"actions": 2, "sessions_found": 1}


async def given_token_in_environment_when_config_requested_then_masked_configuration(
    client: httpx.AsyncClient, manager: FakeConversationManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "s3cr3t")
    response = await client.get(_url("/config"))
    assert response.status_code == 200
    body = response.json()
    assert body == manager.config.masked()
    assert body["transport"]["token"] == "***"
    assert "s3cr3t" not in response.text
    assert body["api"]["port"] == 8765 and body["budget"]["default_max_cycles"] == 7
    monkeypatch.delenv(TOKEN_ENV)
    assert (await client.get(_url("/config"))).json()["transport"]["token"] is None


async def given_allowed_origin_when_preflight_and_request_then_cors_headers_present() -> None:
    manager = FakeConversationManager(_config(cors_origins=["http://front.local:5173"]))
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t"
    ) as client:
        preflight = await client.options(
            _url("/sessions"),
            headers={
                "Origin": "http://front.local:5173",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type",
            },
        )
        assert preflight.status_code == 200, preflight.text
        assert preflight.headers["access-control-allow-origin"] == "http://front.local:5173"
        assert "POST" in preflight.headers["access-control-allow-methods"]
        actual = await client.get(_url("/health"), headers={"Origin": "http://front.local:5173"})
        assert actual.headers["access-control-allow-origin"] == "http://front.local:5173"
        denied = await client.get(_url("/health"), headers={"Origin": "http://evil.example"})
        assert "access-control-allow-origin" not in denied.headers


# ================================================================================================
# uniform errors
# ================================================================================================
async def given_manager_raising_app_error_when_request_then_normalized_json_with_mapped_status(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    manager.start_raises = BudgetExceededError("max_cycles", 7, 7)
    response = await client.post(_url("/sessions"), json={"goal": "g", "user_message": "m"})
    assert response.status_code == 409, response.text
    error = _error(response)
    assert error == BudgetExceededError("max_cycles", 7, 7).error.model_dump(mode="json")
    assert error["error_type"] == "BUDGET_EXCEEDED" and error["error_code"] == "BUDGET_MAX_CYCLES"

    manager.start_raises = PersistenceError("DISK_FULL")
    response = await client.post(_url("/sessions"), json={"goal": "g", "user_message": "m"})
    assert response.status_code == 500
    assert _error(response)["error_type"] == "PERSISTENCE_ERROR"


@pytest.mark.parametrize(
    ("error_type", "expected"),
    [
        (ErrorType.AUTHN_ERROR, 502),
        (ErrorType.NETWORK_ERROR, 502),
        (ErrorType.TIMEOUT_ERROR, 504),
        (ErrorType.RATE_LIMIT_ERROR, 503),
        (ErrorType.MODEL_PROTOCOL_ERROR, 502),
        (ErrorType.PERSISTENCE_ERROR, 500),
        (ErrorType.BUDGET_EXCEEDED, 409),
        (ErrorType.INTERRUPTED, 409),
        (ErrorType.SYSTEM_ERROR, 500),
    ],
)
def given_error_type_when_status_mapped_then_documented_http_status(
    error_type: ErrorType, expected: int
) -> None:
    from agentic_local_app.domain.errors import AppError, NormalizedError

    error = AppError(NormalizedError(error_type=error_type, error_code="X", origin="test"))
    assert status_for(error) == expected


def given_invalid_transition_error_when_status_mapped_then_409() -> None:
    from agentic_local_app.domain.errors import InvalidTransitionError

    exc = InvalidTransitionError(entity="session", current="FAILED", target="RUNNING")
    assert status_for(exc) == 409


async def given_manager_raising_key_error_when_request_then_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    manager.start_raises = KeyError("unknown session: ghost")
    response = await client.post(_url("/sessions"), json={"goal": "g", "user_message": "m"})
    assert response.status_code == 404
    assert "ghost" in _error(response)["details"]["message"]


async def given_unknown_route_when_requested_then_uniform_404_error(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get(_url("/nothing-here"))
    assert response.status_code == 404
    assert _error(response)["error_code"] == "HTTP_404"


def given_fake_manager_when_checked_then_satisfies_the_facade_protocol(
    manager: FakeConversationManager,
) -> None:
    facade: ConversationManagerLike = manager
    assert facade.config is manager.config
    assert facade.bus.subscriber_names[:3] == ["audit_log", "execution_tracker", "telemetry"]


def given_the_real_facade_when_compared_to_the_protocol_then_every_member_exists() -> None:
    """The API is served over the real ``ConversationManager`` through the protocol, which type
    checking never sees (the wiring hands it over as an ``ApplicationLike``): a member declared
    here and missing there would only show as an ``AttributeError`` on a request."""
    from agentic_local_app.orchestration import ConversationManager

    declared = {name for name in dir(ConversationManagerLike) if not name.startswith("_")}
    assert "paused_reason" in declared and "resume_session" in declared
    assert [name for name in sorted(declared) if not hasattr(ConversationManager, name)] == []


# ================================================================================================
# SSE broker (interfaces/sse.py)
# ================================================================================================
@pytest.fixture
def broker(manager: FakeConversationManager) -> Iterator[SseBroker]:
    broker = SseBroker(
        manager.bus,
        manager.clock,
        queue_size=manager.config.api.sse_queue_size,
        audit=manager.audit,
        store=manager.store,
    )
    yield broker
    broker.close()


def given_broker_when_created_then_non_critical_subscriber_named_sse_after_adr015_order(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    assert manager.bus.subscriber_names == [
        "audit_log",
        "execution_tracker",
        "telemetry",
        SSE_SUBSCRIBER_NAME,
    ]
    broker.close()
    assert SSE_SUBSCRIBER_NAME not in manager.bus.subscriber_names


async def given_subscriber_when_events_published_then_frames_in_order_with_id_event_and_canonical_data(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)  # audit sequences 1..5 before the client
    iterator = broker.subscribe(session_id=sid)
    frames_task = asyncio.ensure_future(collect(iterator, 3))
    await _settle()
    manager.add_cycle(sid)  # cycle.started -> sequence 6
    _running_plan(manager, sid)  # plan.received -> 7
    manager.set_task_state(sid, "t2", TaskState.COMPLETED, exit_code=0, duration_ms=12)  # 8
    frames = await frames_task

    assert [f.event for f in frames] == ["cycle.started", "plan.received", "task.state_changed"]
    assert [f.id for f in frames] == ["6", "7", "8"]
    payload = frames[2].json
    assert payload["event_id"] == "evt-0008" and payload["sequence"] == 8
    assert payload["event_type"] == "task.state_changed"
    assert payload["session_id"] == sid and payload["task_id"] == "t2"
    assert payload["plan_id"] == "plan-1" and payload["conversation_id"] == "conv-0001"
    assert payload["timestamp"] == "2026-01-01T00:00:00Z"
    assert payload["payload"] == {
        "from": "RUNNING",
        "to": "COMPLETED",
        "duration_ms": 12,
        "exit_code": 0,
    }
    assert frames[2].data == json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert set(payload) == {
        "event_id",
        "sequence",
        "event_type",
        "timestamp",
        "session_id",
        "conversation_id",
        "cycle_id",
        "plan_id",
        "task_id",
        "payload",
    }
    encoded = frames[2].encode()
    assert encoded.startswith(b"id: 8\nevent: task.state_changed\ndata: {")
    assert encoded.endswith(b"}\n\n")


async def given_task_output_events_when_published_then_ids_are_sequence_dot_n_and_not_audited(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)  # last audited sequence: 7
    iterator = broker.subscribe(session_id=sid)
    frames_task = asyncio.ensure_future(collect(iterator, 4))
    await _settle()
    manager.emit_output(sid, "t2", "hello ", offset=0)
    manager.emit_output(sid, "t2", "world\n", offset=6)
    manager.set_task_state(sid, "t2", TaskState.COMPLETED, exit_code=0)  # sequence 8
    manager.emit_output(sid, "t3", "three", offset=0)
    frames = await frames_task

    assert [f.id for f in frames] == ["7.1", "7.2", "8", "8.1"]
    assert [f.event for f in frames] == [
        "task.output",
        "task.output",
        "task.state_changed",
        "task.output",
    ]
    assert frames[0].json["payload"] == {
        "stream": "stdout",
        "offset": 0,
        "size": 6,
        "data": "hello ",
    }
    assert "event_id" not in frames[0].json and "sequence" not in frames[0].json
    assert manager.store.count_audit_events(sid) == 8
    assert parse_event_id("7.2") == (7, 2)
    assert parse_event_id("8") == (8, 0)
    assert parse_event_id("garbage") is None


async def given_two_sessions_when_client_subscribed_to_one_then_only_its_events_delivered(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    first = await _start(manager)
    second = await _start(manager)
    only_first = broker.subscribe(session_id=first)
    everything = broker.subscribe()
    first_task = asyncio.ensure_future(collect(only_first, 2))
    all_task = asyncio.ensure_future(collect(everything, 3))
    await _settle()
    manager.add_cycle(second)
    manager.add_cycle(first)
    manager.complete(first)
    first_frames = await first_task
    all_frames = await all_task

    assert [(f.json["session_id"], f.event) for f in first_frames] == [
        (first, "cycle.started"),
        (first, "conversation.state_changed"),
    ]
    assert [(f.json["session_id"], f.event) for f in all_frames] == [
        (second, "cycle.started"),
        (first, "cycle.started"),
        (first, "conversation.state_changed"),
    ]


async def given_event_type_filter_when_events_published_then_only_matching_types(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)
    iterator = broker.subscribe(session_id=sid, event_types={EventType.SESSION_STATE_CHANGED})
    task = asyncio.ensure_future(collect(iterator, 1))
    await _settle()
    manager.add_cycle(sid)
    manager.complete(sid)
    frames = await task
    assert [f.event for f in frames] == ["session.state_changed"]
    assert frames[0].json["payload"]["to"] == "COMPLETED"


async def given_task_filter_when_output_of_several_tasks_published_then_only_that_task_output(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)
    iterator = broker.subscribe(session_id=sid, task_id="t2", event_types={EventType.TASK_OUTPUT})
    task = asyncio.ensure_future(collect(iterator, 2))
    await _settle()
    manager.emit_output(sid, "t3", "other task", offset=0)
    manager.emit_output(sid, "t2", "chunk 1", offset=0)
    manager.set_task_state(sid, "t2", TaskState.COMPLETED)  # not an output event
    manager.emit_output(sid, "t2", "chunk 2", offset=7, stream=OutputStream.STDERR)
    frames = await task
    assert [(f.json["task_id"], f.json["payload"]["data"]) for f in frames] == [
        ("t2", "chunk 1"),
        ("t2", "chunk 2"),
    ]
    assert frames[1].json["payload"]["stream"] == "stderr"


async def given_last_event_id_when_client_reconnects_then_missing_audited_events_replayed_once_then_live(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)  # sequences 1..5
    manager.add_cycle(sid)  # 6
    _running_plan(manager, sid)  # 7
    # the client saw up to "5" (and a task.output "5.3" after it), then dropped the connection
    iterator = broker.subscribe(session_id=sid, last_event_id="5.3")
    task = asyncio.ensure_future(collect(iterator, 4))
    await _settle()
    manager.set_task_state(sid, "t2", TaskState.COMPLETED)  # 8, live
    manager.emit_output(sid, "t3", "x", offset=0)  # 8.1, live
    frames = await task

    assert [f.id for f in frames] == ["6", "7", "8", "8.1"]
    assert [f.event for f in frames] == [
        "cycle.started",
        "plan.received",
        "task.state_changed",
        "task.output",
    ]
    replayed = frames[0].json
    stored = manager.store.list_audit_events(sid, after_sequence=5)[0]
    assert replayed["event_id"] == stored.event_id == "evt-0006"
    assert replayed["payload"] == stored.payload
    assert replayed["timestamp"] == "2026-01-01T00:00:00Z"
    assert set(replayed) == {
        "event_id",
        "sequence",
        "event_type",
        "timestamp",
        "session_id",
        "conversation_id",
        "cycle_id",
        "plan_id",
        "task_id",
        "payload",
    }


async def given_events_published_during_replay_when_reconnected_then_no_duplicate_and_order_kept(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)  # 1..5
    for _ in range(3):
        manager.add_cycle(sid)  # 6, 7, 8
    iterator = broker.subscribe(session_id=sid, last_event_id="4")
    first = await asyncio.wait_for(iterator.__anext__(), timeout=BOUND_S)
    assert first.id == "5"
    # the client is registered: events published now are queued behind the replay
    manager.add_cycle(sid)  # 9
    rest = await collect(iterator, 4)
    assert [f.id for f in rest] == ["6", "7", "8", "9"]


async def given_slow_client_with_queue_size_2_when_10_events_published_then_dropped_and_bus_never_blocked(
    manager: FakeConversationManager,
) -> None:
    broker = SseBroker(
        manager.bus, manager.clock, queue_size=2, audit=manager.audit, store=manager.store
    )
    try:
        sid = await _start(manager)  # 1..5
        iterator = broker.subscribe(session_id=sid)
        # the consumer registers itself then waits; the ten synchronous publications below give
        # it no chance to read: it is a slow client
        started = asyncio.ensure_future(collect(iterator, 3))
        await _settle()
        assert broker.client_count == 1
        for _ in range(10):
            manager.add_cycle(sid)  # synchronous publish: must return every time
        assert manager.bus.published_count >= 15
        assert manager.bus.subscriber_errors == 0
        assert broker.client_count == 0  # dropped as soon as the queue overflowed
        assert broker.dropped_count == 1
        frames = await started
        assert [f.id for f in frames] == ["6", "7", None]
        assert frames[2].event == DROPPED_EVENT
        dropped = frames[2].json
        assert dropped["reason"] == "queue_full"
        assert dropped["queue_size"] == 2 and dropped["session_id"] == sid
        assert dropped["timestamp"] == "2026-01-01T00:00:00Z"
        assert frames[2].encode() == (
            b"event: dropped\ndata: " + frames[2].data.encode("utf-8") + b"\n\n"
        )
        with pytest.raises(StopAsyncIteration):
            await iterator.__anext__()
    finally:
        broker.close()


async def given_client_when_stream_closed_then_unsubscribed_and_bus_keeps_publishing(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)
    iterator = broker.subscribe(session_id=sid)
    frames = await asyncio.wait_for(
        asyncio.ensure_future(_first_after(iterator, manager, sid)), timeout=BOUND_S
    )
    assert frames.event == "cycle.started"
    assert broker.client_count == 1
    await iterator.aclose()
    assert broker.client_count == 0
    manager.add_cycle(sid)  # nobody listens: no error, no growth
    assert manager.bus.subscriber_errors == 0
    assert broker.client_count == 0


async def _first_after(
    iterator: AsyncIterator[SseFrame], manager: FakeConversationManager, sid: str
) -> SseFrame:
    task = asyncio.ensure_future(iterator.__anext__())
    await _settle()
    manager.add_cycle(sid)
    return await task


async def given_broker_when_closed_then_every_open_subscription_ends(
    manager: FakeConversationManager,
) -> None:
    broker = SseBroker(manager.bus, manager.clock, queue_size=8, audit=manager.audit)
    sid = await _start(manager)
    iterator = broker.subscribe(session_id=sid)
    task = asyncio.ensure_future(collect(iterator, 5))
    await _settle()
    broker.close()
    assert await asyncio.wait_for(task, timeout=BOUND_S) == []
    assert SSE_SUBSCRIBER_NAME not in manager.bus.subscriber_names


async def given_heartbeat_enabled_when_stream_idle_then_keep_alive_comment_frames(
    manager: FakeConversationManager, broker: SseBroker
) -> None:
    sid = await _start(manager)
    iterator = broker.subscribe(session_id=sid, heartbeat_s=0.01)
    frames = await collect(iterator, 2)
    assert all(f.comment == "keep-alive" for f in frames)
    assert frames[0].encode() == b": keep-alive\n\n"
    assert frames[0].id is None and frames[0].event is None and frames[0].data is None


async def given_broker_without_audit_when_events_published_then_ids_count_per_session(
    manager: FakeConversationManager,
) -> None:
    bus = EventBus()
    broker = SseBroker(bus, FakeClock(), queue_size=4)
    try:
        iterator = broker.subscribe()
        task = asyncio.ensure_future(collect(iterator, 3))
        await _settle()
        for sid in ("a", "b", "a"):
            bus.publish(
                Event(
                    event_type=EventType.BUDGET_UPDATED,
                    timestamp=manager.clock.now(),
                    session_id=sid,
                )
            )
        frames = await task
        assert [(f.json["session_id"], f.id) for f in frames] == [
            ("a", "1"),
            ("b", "1"),
            ("a", "2"),
        ]
    finally:
        broker.close()


def given_frame_with_multiline_data_when_encoded_then_one_data_line_per_line() -> None:
    frame = SseFrame(id="3", event="x", data="line1\nline2")
    assert frame.encode() == b"id: 3\nevent: x\ndata: line1\ndata: line2\n\n"


# ================================================================================================
# SSE routes
# ================================================================================================
async def given_session_events_route_when_streamed_then_event_stream_headers_and_frames_in_order(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)

    def activity() -> None:
        manager.add_cycle(sid)
        _running_plan(manager, sid)
        manager.emit_output(sid, "t2", "live!", offset=0)

    response, frames = await read_frames(
        stream_client, _url(f"/sessions/{sid}/events"), 3, after_start=activity
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["cache-control"] == "no-cache"
    assert [(f.id, f.event) for f in frames] == [
        ("6", "cycle.started"),
        ("7", "plan.received"),
        ("7.1", "task.output"),
    ]
    assert frames[2].json["payload"]["data"] == "live!"
    await _settle()
    assert manager.bus.subscriber_errors == 0


async def given_client_gone_when_stream_closed_then_broker_client_removed(
    app: FastAPI, stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    broker: SseBroker = app.state.sse_broker
    _, frames = await read_frames(
        stream_client,
        _url(f"/sessions/{sid}/events"),
        1,
        after_start=lambda: manager.add_cycle(sid),
    )
    assert len(frames) == 1
    await _settle(20)
    assert broker.client_count == 0


async def given_global_events_route_when_streamed_then_events_of_every_session(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    first = await _start(manager)
    second = await _start(manager)

    def activity() -> None:
        manager.add_cycle(second)
        manager.add_cycle(first)

    _, frames = await read_frames(stream_client, _url("/events"), 2, after_start=activity)
    assert [f.json["session_id"] for f in frames] == [second, first]


async def given_output_live_route_when_streamed_then_only_that_task_output(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _running_plan(manager, sid)

    def activity() -> None:
        manager.emit_output(sid, "t3", "not mine", offset=0)
        manager.set_task_state(sid, "t2", TaskState.COMPLETED)
        manager.emit_output(sid, "t2", "mine 1", offset=0)
        manager.emit_output(sid, "t2", "mine 2", offset=6)

    _, frames = await read_frames(
        stream_client, _url(f"/sessions/{sid}/tasks/t2/output/live"), 2, after_start=activity
    )
    assert [(f.event, f.json["payload"]["data"]) for f in frames] == [
        ("task.output", "mine 1"),
        ("task.output", "mine 2"),
    ]
    assert [f.id for f in frames] == ["8.1", "8.2"]


async def given_last_event_id_header_when_events_route_streamed_then_replay_then_live(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)  # 1..5
    manager.add_cycle(sid)  # 6
    _, frames = await read_frames(
        stream_client,
        _url(f"/sessions/{sid}/events"),
        3,
        headers={"Last-Event-ID": "4"},
        after_start=lambda: manager.complete(sid),
    )
    assert [f.id for f in frames] == ["5", "6", "7"]
    assert frames[0].event == "conversation.state_changed"
    assert frames[2].event == "conversation.state_changed"
    assert frames[2].json["payload"]["to"] == "COMPLETED"


async def given_last_event_id_query_when_events_route_streamed_then_same_replay(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _, frames = await read_frames(stream_client, _url(f"/sessions/{sid}/events?last_event_id=3"), 2)
    assert [f.id for f in frames] == ["4", "5"]


async def given_event_types_query_when_events_route_streamed_then_filtered(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)

    def activity() -> None:
        manager.add_cycle(sid)
        manager.complete(sid)

    _, frames = await read_frames(
        stream_client,
        _url(f"/sessions/{sid}/events?event_types=session.state_changed,final_answer.received"),
        2,
        after_start=activity,
    )
    assert [f.event for f in frames] == ["final_answer.received", "session.state_changed"]
    bad = await stream_client.get(_url(f"/sessions/{sid}/events?event_types=nope"))
    assert bad.status_code == 422


async def given_user_response_received_when_events_route_streamed_then_frame_carries_payload(
    stream_client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)

    def activity() -> None:
        manager.add_cycle(sid)
        manager.respond(sid, "Which module fails?", expects_reply=True)

    _, frames = await read_frames(
        stream_client,
        _url(f"/sessions/{sid}/events?event_types=user_response.received,session.state_changed"),
        2,
        after_start=activity,
    )
    assert [f.event for f in frames] == ["user_response.received", "session.state_changed"]
    frame = frames[0].json
    assert frame["event_type"] == "user_response.received"
    assert frame["session_id"] == sid and frame["conversation_id"] == "conv-0001"
    assert frame["cycle_id"] == "cyc-0001"
    assert frame["payload"] == {
        "message_id": "model-msg-0001",
        "format": "text",
        "status": "completed",
        "expects_reply": True,
        "body_bytes": len("Which module fails?"),
        "auto_close_on_final_answer": False,
        "auto_close_skipped": False,
        "consumed_cycles": 0,
        "consumed_plans": 0,
        "session_duration_ms": 0,
    }
    assert "body" not in frame["payload"]  # the body is read from the messages table, not the bus
    assert frames[0].id is not None  # audited, hence replayable
    audited = manager.store.list_audit_events(sid, limit=100)
    assert [e.event_type for e in audited].count("user_response.received") == 1
    assert frames[1].json["payload"]["reason"] == "user_response"


async def given_unknown_session_or_task_when_sse_route_requested_then_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    assert (await client.get(_url("/sessions/nope/events"))).status_code == 404
    assert (await client.get(_url(f"/sessions/{sid}/tasks/t9/output/live"))).status_code == 404


# ================================================================================================
# machine identity and model catalogue (ADR-024)
# ================================================================================================
@asynccontextmanager
async def _client_for(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """A client on an application the test built itself (identity, environment, configuration)."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


def _models_config(**profiles: TransportSection) -> AppConfig:
    return AppConfig(
        api=ApiSection(),
        models=ModelsSection(active="claude", profiles=dict(profiles)),
    )


async def given_wired_identity_when_whoami_requested_then_user_source_and_host(
    manager: FakeConversationManager,
) -> None:
    identity = UserIdentity(user_id="alice", source="env:USER", host="workstation")
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, identity=identity)
    async with _client_for(app) as client:
        response = await client.get(_url("/whoami"))
    assert response.status_code == 200
    assert response.json() == {"user_id": "alice", "source": "env:USER", "host": "workstation"}


async def given_no_injected_identity_when_whoami_requested_then_resolved_from_the_environment(
    manager: FakeConversationManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("USER", "resolved-user")  # POSIX
    monkeypatch.setenv("USERNAME", "resolved-user")  # Windows (ADR-024 §5, step 1 either way)
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None)
    async with _client_for(app) as client:
        body = (await client.get(_url("/whoami"))).json()
        again = (await client.get(_url("/whoami"))).json()
    assert body["user_id"] == "resolved-user"
    assert body["source"].startswith("env:")
    assert set(body) == {"user_id", "source", "host"}
    assert again == body  # resolved once, kept


async def given_two_profiles_when_models_requested_then_catalogue_active_first_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _models_config(
        claude=TransportSection(
            provider="templated_http",
            codec="json_text",
            token_env=TOKEN_ENV,
            display_name="Claude (chat completions)",
            description="the real model",
            init_url="https://api.example.test/v1/conversations",
            options={"api_key": "never-shown"},
        ),
        mock=TransportSection(token_env="", display_name="Mock local"),
    )
    manager = FakeConversationManager(config)
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, environ={})
    async with _client_for(app) as client:
        response = await client.get(_url("/models"))
    assert response.status_code == 200
    body = response.json()
    assert body["active"] == "claude"
    assert body["models"] == [
        {
            "name": "claude",
            "display_name": "Claude (chat completions)",
            "description": "the real model",
            "provider": "templated_http",
            "codec": "json_text",
            "requires_credentials": True,  # token_env named, nothing in the environment
            # ADR-027 §1: no declaration, so the implicit access_token field of token_env
            "credential_fields": [
                {
                    "key": "access_token",
                    "label": "Access token",
                    "placeholder": "Paste an access token",
                    "secret": True,
                }
            ],
            "active": True,
        },
        {
            "name": "mock",
            "display_name": "Mock local",
            "description": None,
            "provider": "generic_http",
            "codec": "passthrough",
            "requires_credentials": False,  # takes no token at all
            "credential_fields": [],  # and nothing to render for it
            "active": False,
        },
    ]
    for leak in ("api.example.test", "never-shown", "api_key", TOKEN_ENV):
        assert leak not in response.text


async def given_token_in_the_environment_when_models_requested_then_profile_stops_asking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``requires_credentials`` is a read-time view (ADR-024 §4): the front must see it change."""
    environ: dict[str, str] = {}
    manager = FakeConversationManager(_models_config(claude=TransportSection(token_env=TOKEN_ENV)))
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, environ=environ)
    async with _client_for(app) as client:
        assert (await client.get(_url("/models"))).json()["models"][0]["requires_credentials"]
        environ[TOKEN_ENV] = "s3cr3t"
        assert not (await client.get(_url("/models"))).json()["models"][0]["requires_credentials"]


async def given_declared_credential_fields_when_models_requested_then_rendered_without_env() -> (
    None
):
    """ADR-027 §2: what the front draws travels; the variable behind a field never does."""
    manager = FakeConversationManager(
        _models_config(
            claude=TransportSection(token_env=TOKEN_ENV, credential_fields=TWO_CREDENTIAL_FIELDS)
        )
    )
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, environ={})
    async with _client_for(app) as client:
        response = await client.get(_url("/models"))
    entry = response.json()["models"][0]
    assert entry["credential_fields"] == [
        {
            "key": "access_token",
            "label": "Access token",
            "placeholder": "Paste it",
            "secret": True,
        },
        {"key": "chat_id", "label": "Chat id", "placeholder": None, "secret": False},
    ]
    assert entry["requires_credentials"] is True
    for leak in ("env", TOKEN_ENV, CHAT_ENV):
        assert leak not in response.text


async def given_declared_fields_when_only_one_is_provided_then_the_profile_still_asks() -> None:
    """``requires_credentials`` covers every declared field, not only the first (ADR-027 §1)."""
    environ: dict[str, str] = {}
    manager = FakeConversationManager(
        _models_config(
            claude=TransportSection(token_env=TOKEN_ENV, credential_fields=TWO_CREDENTIAL_FIELDS)
        )
    )
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, environ=environ)
    async with _client_for(app) as client:
        environ[TOKEN_ENV] = "s3cr3t"
        assert (await client.get(_url("/models"))).json()["models"][0]["requires_credentials"]
        environ[CHAT_ENV] = "chat-42"
        assert not (await client.get(_url("/models"))).json()["models"][0]["requires_credentials"]


# ================================================================================================
# credentials (ADR-025 §6): the token goes in, nothing comes out
# ================================================================================================
def _credentials_app(
    token_env: str = TOKEN_ENV,
    credential_fields: list[CredentialField] | None = None,
) -> tuple[FakeConversationManager, FastAPI, dict[str, str]]:
    manager = FakeConversationManager(
        AppConfig(
            api=ApiSection(),
            transport=TransportSection(token_env=token_env, credential_fields=credential_fields),
        )
    )
    environ: dict[str, str] = {}
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, environ=environ)
    return manager, app, environ


async def given_token_posted_when_credentials_set_then_204_and_variable_written(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _, app, environ = _credentials_app()
    with caplog.at_level(0):
        async with _client_for(app) as client:
            response = await client.post(_url("/credentials"), json={"token": "  s3cr3t-token  "})
    assert response.status_code == 204
    assert response.content == b""
    assert environ == {TOKEN_ENV: "s3cr3t-token"}  # stripped, written where the transport reads it
    assert "s3cr3t-token" not in caplog.text


async def given_a_token_when_anything_is_read_back_then_the_value_is_nowhere(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """ADR-025 §6: the value never reaches a response, a log line, an event or an error detail."""
    manager, app, environ = _credentials_app()
    secret = "s3cr3t-token"
    with caplog.at_level(0):
        async with _client_for(app) as client:
            assert (await client.post(_url("/credentials"), json={"token": secret})).content == b""
            # a misspelled field: pydantic would hand the whole body back in the validation error
            wrong = await client.post(_url("/credentials"), json={"tok": secret})
            nested = await client.post(_url("/credentials"), json={"token": {"value": secret}})
            extra = await client.post(_url("/credentials"), json={"token": secret, "x": 1})
            broken = await client.post(
                _url("/credentials"), content=secret.encode(), headers={"content-type": "app/json"}
            )
            seen = [
                (await client.get(_url("/config"))).text,
                (await client.get(_url("/models"))).text,
                (await client.get(_url("/sessions"))).text,
                (await client.get(_url("/admin/events"))).text,
                (await client.get(_url("/admin/audit"))).text,
                (await client.get(_url("/metrics"))).text,
            ]
    assert wrong.status_code == 422 and nested.status_code == 422 and broken.status_code == 422
    assert extra.status_code == 204  # an unknown key is ignored; the token is still taken
    for response in (wrong, nested, extra, broken):
        assert secret not in response.text
    assert all(secret not in text for text in seen)
    assert secret not in caplog.text
    assert environ[TOKEN_ENV] == secret
    assert secret not in json.dumps(
        [event.model_dump(mode="json") for event in manager.store.list_audit_events("sess-0001")],
        default=str,
    )


async def given_an_undeclared_profile_when_the_implicit_field_is_posted_then_it_is_written() -> (
    None
):
    """ADR-027 §1: with no declaration the profile still answers to ``access_token``."""
    _, app, environ = _credentials_app()
    async with _client_for(app) as client:
        written = await client.post(
            _url("/credentials"), json={"credentials": {"access_token": " tok "}}
        )
        unknown = await client.post(_url("/credentials"), json={"credentials": {"chat_id": "42"}})
    assert written.status_code == 204, written.text
    assert environ == {TOKEN_ENV: "tok"}
    assert unknown.status_code == 400
    assert _error(unknown)["details"] == {
        "message": "the active model profile does not declare this credential",
        "key": "chat_id",
        "expected": ["access_token"],
    }


async def given_several_credentials_when_anything_is_read_back_then_no_value_is_anywhere(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """ADR-027 §3: the same rule, field by field — the refusals name a key, never a value."""
    manager, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    token, chat = "s3cr3t-token", "s3cr3t-chat-id"
    with caplog.at_level(0):
        async with _client_for(app) as client:
            written = await client.post(
                _url("/credentials"),
                json={"credentials": {"access_token": token, "chat_id": chat}},
            )
            # a key the profile does not declare: named back, with the declared ones
            unknown = await client.post(
                _url("/credentials"), json={"credentials": {"chat": chat, "access_token": token}}
            )
            blank = await client.post(
                _url("/credentials"), json={"credentials": {"chat_id": "   "}}
            )
            nested = await client.post(
                _url("/credentials"), json={"credentials": {"chat_id": {"value": chat}}}
            )
            not_an_object = await client.post(_url("/credentials"), json={"credentials": chat})
            seen = [
                (await client.get(_url("/config"))).text,
                (await client.get(_url("/models"))).text,
                (await client.get(_url("/admin/events"))).text,
                (await client.get(_url("/admin/audit"))).text,
            ]
    assert written.status_code == 204 and written.content == b""
    assert environ == {TOKEN_ENV: token, CHAT_ENV: chat}
    assert unknown.status_code == 400 and blank.status_code == 400
    assert nested.status_code == 422 and not_an_object.status_code == 422
    # the wrong-key error names the key and what was expected, and nothing else
    assert _error(unknown)["error_code"] == "CREDENTIAL_FIELD_UNKNOWN"
    assert _error(unknown)["details"]["key"] == "chat"
    assert _error(unknown)["details"]["expected"] == ["access_token", "chat_id"]
    assert _error(blank)["details"]["key"] == "chat_id"
    for response in (unknown, blank, nested, not_an_object):
        assert token not in response.text and chat not in response.text
    assert all(token not in text and chat not in text for text in seen)
    assert token not in caplog.text and chat not in caplog.text
    assert token not in json.dumps(
        [event.model_dump(mode="json") for event in manager.store.list_audit_events("sess-0001")],
        default=str,
    )


async def given_declared_fields_when_credentials_posted_then_each_value_lands_in_its_variable() -> (
    None
):
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        response = await client.post(
            _url("/credentials"),
            json={"credentials": {"access_token": "  tok  ", "chat_id": "chat-42"}},
        )
        models = (await client.get(_url("/models"))).json()["models"]
    assert response.status_code == 204, response.text
    assert environ == {TOKEN_ENV: "tok", CHAT_ENV: "chat-42"}  # stripped, one variable each
    assert models[0]["requires_credentials"] is False  # both fields are now provided


async def given_a_subset_of_the_fields_when_posted_then_accepted_and_still_incomplete() -> None:
    """A partial form is not a protocol error: the catalogue keeps asking for the rest."""
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"credentials": {"chat_id": "42"}})
        models = (await client.get(_url("/models"))).json()["models"]
    assert response.status_code == 204, response.text
    assert environ == {CHAT_ENV: "42"}
    assert models[0]["requires_credentials"] is True


async def given_the_token_alias_when_posted_then_it_fills_the_access_token_field() -> None:
    """ADR-027 §3: the CLI and the older client keep posting ``{"token": …}``."""
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"token": "tok"})
    assert response.status_code == 204, response.text
    assert environ == {TOKEN_ENV: "tok"}


async def given_a_refused_entry_when_posted_then_nothing_at_all_is_written() -> None:
    """Validation runs on the whole form before the first write (ADR-027 §3)."""
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        unknown = await client.post(
            _url("/credentials"), json={"credentials": {"access_token": "tok", "nope": "x"}}
        )
        blank = await client.post(
            _url("/credentials"), json={"credentials": {"access_token": "tok", "chat_id": " "}}
        )
    assert unknown.status_code == 400 and blank.status_code == 400
    assert _error(unknown)["error_code"] == "CREDENTIAL_FIELD_UNKNOWN"
    assert _error(blank)["error_code"] == "CREDENTIALS_EMPTY"
    assert environ == {}


async def given_an_empty_credentials_object_when_posted_then_400_credentials_empty() -> None:
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"credentials": {}})
    assert response.status_code == 400, response.text
    error = _error(response)
    assert error["error_code"] == "CREDENTIALS_EMPTY"
    assert "key" not in error["details"]  # no entry at all: there is no key to name
    assert environ == {}


async def given_a_body_without_credentials_when_posted_then_422_naming_no_value() -> None:
    _, app, environ = _credentials_app(credential_fields=TWO_CREDENTIAL_FIELDS)
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"creds": {"chat_id": "s3cr3t"}})
    assert response.status_code == 422, response.text
    assert _error(response)["error_code"] == "VALIDATION_ERROR"
    assert "s3cr3t" not in response.text
    assert environ == {}


async def given_a_profile_declaring_nothing_when_credentials_posted_then_409() -> None:
    """An explicitly empty declaration is a profile that takes no credential at all."""
    _, app, environ = _credentials_app(credential_fields=[])
    async with _client_for(app) as client:
        response = await client.post(
            _url("/credentials"), json={"credentials": {"access_token": "tok"}}
        )
    assert response.status_code == 409, response.text
    assert _error(response)["error_code"] == "CREDENTIALS_NOT_CONFIGURED"
    assert environ == {}


async def given_blank_token_when_credentials_set_then_400_credentials_empty() -> None:
    _, app, environ = _credentials_app()
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"token": "   "})
    assert response.status_code == 400, response.text
    error = _error(response)
    assert error["error_code"] == "CREDENTIALS_EMPTY"
    # ADR-027 §3: the refusal names the field the front drew, never the value and never ``env``
    assert error["details"]["key"] == "access_token"
    assert TOKEN_ENV not in response.text
    assert environ == {}


async def given_profile_without_token_variable_when_credentials_set_then_409() -> None:
    _, app, environ = _credentials_app(token_env="")
    async with _client_for(app) as client:
        response = await client.post(_url("/credentials"), json={"token": "s3cr3t"})
    assert response.status_code == 409, response.text
    error = _error(response)
    assert error["error_code"] == "CREDENTIALS_NOT_CONFIGURED"
    assert error["details"] == {
        "message": "the active model profile declares no credential field",
        "field": "credential_fields",
        "provider": "generic_http",
    }
    assert environ == {}


# ================================================================================================
# pause and resume (ADR-025)
# ================================================================================================
async def given_paused_session_when_pause_read_then_reason_code_operation_and_since(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.pause(sid, operation="GET")
    response = await client.get(_url(f"/sessions/{sid}/pause"))
    assert response.status_code == 200, response.text
    assert response.json() == {
        "reason": "credentials_required",
        "error_code": "HTTP_401",
        "error_type": "AUTHN_ERROR",
        "operation": "GET",
        "since": manager.require_session(sid).model_dump(mode="json")["updated_at"],
    }


async def given_running_session_when_pause_read_then_404_not_paused(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.get(_url(f"/sessions/{sid}/pause"))
    assert response.status_code == 404, response.text
    assert _error(response)["error_code"] == "NOT_PAUSED"
    assert (await client.get(_url("/sessions/ghost/pause"))).status_code == 404


async def given_paused_session_when_resumed_then_200_with_the_running_record(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.pause(sid)
    response = await client.post(_url(f"/sessions/{sid}/resume"))
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["session_id"] == sid and body["status"] == "RUNNING"
    assert ("resume_session", sid) in manager.calls
    assert manager.require_session(sid).status is SessionState.RUNNING


async def given_session_that_is_not_paused_when_resumed_then_409_not_resumable(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.complete(sid)
    response = await client.post(_url(f"/sessions/{sid}/resume"))
    assert response.status_code == 409, response.text
    error = _error(response)
    assert error["error_code"] == "SESSION_NOT_RESUMABLE"
    assert error["details"]["session_id"] == sid
    assert "not resumable" in error["details"]["message"]
    assert (await client.post(_url("/sessions/ghost/resume"))).status_code == 404


# ================================================================================================
# chat view of a session
# ================================================================================================
def _exchange(manager: FakeConversationManager, sid: str) -> None:
    """One full exchange: request, plan, result, correction, answer — one message of each kind."""
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.USER_REQUEST,
        message_id="out-1",
        payload={"type": "user_request", "content": {"user_message": "debug my build"}},
    )
    manager.add_message(
        sid,
        direction=MessageDirection.INBOUND,
        message_type=MessageType.DISCOVERY_PLAN,
        message_id="in-1",
        payload={
            "type": "discovery_plan",
            "content": {"plan_id": "plan-1", "tasks": [{"task_id": f"t{i}"} for i in range(5)]},
        },
    )
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.EXECUTION_RESULT,
        message_id="out-2",
        payload={
            "type": "execution_result",
            "content": {
                "plan_id": "plan-1",
                "status": "completed",
                "results": [{"task_id": "t0"}, {"task_id": "t1"}],
            },
        },
    )
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.PROTOCOL_CORRECTION_REQUEST,
        message_id="out-3",
        payload={
            "type": "protocol_correction_request",
            "content": {"error_code": "UNPARSEABLE_REPLY", "attempt": 1, "max_attempts": 5},
        },
    )
    manager.add_message(
        sid,
        direction=MessageDirection.INBOUND,
        message_type=MessageType.FINAL_ANSWER,
        message_id="in-2",
        payload={
            "type": "final_answer",
            "content": {
                "status": "success",
                "diagnosis": "the target release is wrong",
                "evidence": ["pom.xml line 12"],
                "recommended_next_step": "set maven.compiler.source to 17",
            },
        },
    )


async def given_an_exchange_when_chat_read_then_every_message_type_is_mapped_to_a_turn(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _exchange(manager, sid)
    response = await client.get(_url(f"/sessions/{sid}/chat"))
    assert response.status_code == 200, response.text
    turns = response.json()["messages"]
    assert [(t["id"], t["role"], t["text"]) for t in turns] == [
        ("out-1", "user", "debug my build"),
        ("in-1", "system", "plan-1 · 5 tâches"),
        ("out-2", "system", "plan-1 · completed · 2 résultats"),
        ("out-3", "system", "correction 1/5 · UNPARSEABLE_REPLY"),
        ("in-2", "assistant", "the target release is wrong"),
    ]
    assert [t["message_type"] for t in turns] == [
        "user_request",
        "discovery_plan",
        "execution_result",
        "protocol_correction_request",
        "final_answer",
    ]
    assert [t.get("plan_id") for t in turns] == [None, "plan-1", "plan-1", None, None]
    assert all(t["created_at"] is not None for t in turns)
    # ADR-022 / §12.7: the evidence and the next step stay in the record, out of the bubble
    assert "pom.xml line 12" not in response.text
    assert "maven.compiler.source" not in response.text


async def given_a_user_response_when_chat_read_then_its_body_is_the_assistant_turn(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.add_cycle(sid)
    manager.respond(sid, "## Analysis\n\nthe module does not compile", format="markdown")
    turns = (await client.get(_url(f"/sessions/{sid}/chat"))).json()["messages"]
    assert [(t["role"], t["text"], t["message_type"]) for t in turns] == [
        ("assistant", "## Analysis\n\nthe module does not compile", "user_response")
    ]


async def given_include_system_false_when_chat_read_then_only_user_and_model_turns(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    _exchange(manager, sid)
    response = await client.get(_url(f"/sessions/{sid}/chat?include_system=false"))
    turns = response.json()["messages"]
    assert [(t["role"], t["id"]) for t in turns] == [("user", "out-1"), ("assistant", "in-2")]
    assert (await client.get(_url(f"/sessions/{sid}/chat?include_system=true"))).json()[
        "messages"
    ] == (await client.get(_url(f"/sessions/{sid}/chat"))).json()["messages"]


async def given_a_rejected_reply_when_chat_read_then_it_is_not_a_turn(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """An inbound message the protocol refused never became a turn; the correction tells the story."""
    sid = await _start(manager)
    rejected = manager.add_message(
        sid,
        direction=MessageDirection.INBOUND,
        message_type=MessageType.FINAL_ANSWER,
        message_id="in-bad",
        payload={"type": "final_answer", "content": {"diagnosis": "unusable"}},
    )
    manager.store.save_message(rejected.model_copy(update={"validation_status": "invalid"}))
    turns = (await client.get(_url(f"/sessions/{sid}/chat"))).json()["messages"]
    assert turns == []
    assert (await client.get(_url("/sessions/ghost/chat"))).status_code == 404


async def given_several_conversations_when_chat_read_then_turns_are_chronological(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """A rotation or an interruption opens a conversation; the exchange is still one thread."""
    sid = await _start(manager)
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.USER_REQUEST,
        message_id="out-1",
        payload={"type": "user_request", "content": {"user_message": "first"}},
    )
    second = manager.lifecycle.create_conversation(sid, parent_conversation_id="conv-0001")
    manager.add_message(
        sid,
        direction=MessageDirection.OUTBOUND,
        message_type=MessageType.USER_REQUEST,
        conversation_id=second.conversation_id,
        message_id="out-2",
        payload={"type": "user_request", "content": {"user_message": "second"}},
    )
    turns = (await client.get(_url(f"/sessions/{sid}/chat"))).json()["messages"]
    assert [t["text"] for t in turns] == ["first", "second"]


# ================================================================================================
# administration: the whole store, and the gate on the destructive route
# ================================================================================================
async def given_several_sessions_when_admin_sessions_read_then_paginated_newest_first(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    first = await _start(manager, goal="first")
    manager.clock.advance(1_000)
    second = await _start(manager, goal="second")
    page = (await client.get(_url("/admin/sessions"))).json()
    assert [item["session_id"] for item in page["items"]] == [second, first]
    assert (page["limit"], page["offset"], page["next_offset"]) == (100, 0, None)
    head = (await client.get(_url("/admin/sessions?limit=1"))).json()
    assert [item["session_id"] for item in head["items"]] == [second]
    assert (head["limit"], head["offset"], head["next_offset"]) == (1, 0, 1)
    tail = (await client.get(_url("/admin/sessions?limit=1&offset=1"))).json()
    assert [item["session_id"] for item in tail["items"]] == [first]


async def given_events_of_two_sessions_when_admin_events_read_then_flat_rows_without_hashes(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    first = await _start(manager)
    manager.clock.advance(1_000)
    second = await _start(manager)
    manager.complete(second)
    page = (await client.get(_url("/admin/events"))).json()
    rows = page["items"]
    assert {row["session_id"] for row in rows} == {first, second}
    assert rows[0]["session_id"] == second  # newest first
    assert set(rows[0]) == {
        "event_id",
        "sequence",
        "session_id",
        "conversation_id",
        "cycle_id",
        "plan_id",
        "task_id",
        "event_type",
        "timestamp",
        "payload",
    }
    assert "event_hash" not in json.dumps(page, default=str)
    assert page["limit"] == 100 and page["offset"] == 0 and page["next_offset"] is None
    first_page = (await client.get(_url("/admin/events?limit=2"))).json()
    assert [row["event_id"] for row in first_page["items"]] == [r["event_id"] for r in rows[:2]]
    assert first_page["next_offset"] == 2


async def given_events_of_two_sessions_when_admin_audit_read_then_the_chain_with_its_hashes(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    manager.complete(sid)
    rows = (await client.get(_url("/admin/audit"))).json()["items"]
    stored = manager.store.list_audit_events(sid, limit=100)
    assert len(rows) == len(stored)
    assert [row["sequence"] for row in rows] == sorted(
        (event.sequence for event in stored), reverse=True
    )
    assert all({"previous_event_hash", "event_hash"} <= set(row) for row in rows)
    assert rows[-1]["event_hash"] == stored[0].event_hash


async def given_destructive_admin_disabled_when_reset_requested_then_403_and_nothing_touched(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    response = await client.post(_url("/admin/reset-database"))
    assert response.status_code == 403, response.text
    error = _error(response)
    assert error["error_code"] == "ADMIN_DISABLED"
    assert error["details"]["setting"] == "api.allow_destructive_admin"
    assert manager.get_session(sid) is not None
    assert manager.store.list_audit_events(sid, limit=10)


async def given_destructive_admin_allowed_when_reset_requested_then_204_and_empty_store() -> None:
    manager = FakeConversationManager(_config(allow_destructive_admin=True))
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None)
    async with _client_for(app) as client:
        sid = await _start(manager)
        manager.add_plan(sid, "plan-1", [{"task_id": "t1"}])
        manager.add_blob(sid, "t1", b"output")
        manager.add_failure(sid)
        response = await client.post(_url("/admin/reset-database"))
        assert response.status_code == 204, response.text
        assert response.content == b""
        assert (await client.get(_url("/admin/sessions"))).json()["items"] == []
        assert (await client.get(_url("/admin/events"))).json()["items"] == []
        assert (await client.get(_url("/admin/audit"))).json()["items"] == []
        assert (await client.get(_url(f"/sessions/{sid}"))).status_code == 404
    assert manager.store.list_plans(sid) == []
    assert manager.store.list_failures(sid) == []
    assert manager.store.get_blob_for_task(sid, "t1", OutputStream.STDOUT) is None


# ================================================================================================
# working space (ADR-026) and the default CORS origins of the desktop front
# ================================================================================================
async def given_a_working_space_when_session_created_then_it_is_bound_to_the_session(
    client: httpx.AsyncClient, manager: FakeConversationManager, tmp_path: Path
) -> None:
    response = await client.post(
        _url("/sessions"),
        json={"goal": "g", "user_message": "m", "working_space": str(tmp_path)},
    )
    assert response.status_code == 201, response.text
    assert manager.working_spaces == {response.json()["session_id"]: str(tmp_path)}


async def given_an_unusable_working_space_when_session_created_then_400_and_no_session(
    client: httpx.AsyncClient, manager: FakeConversationManager, tmp_path: Path
) -> None:
    missing = tmp_path / "nowhere"
    response = await client.post(
        _url("/sessions"), json={"goal": "g", "user_message": "m", "working_space": str(missing)}
    )
    assert response.status_code == 400, response.text
    error = _error(response)
    assert error["error_code"] == "WORKING_SPACE_INVALID"
    assert error["details"]["reason"] == "does_not_exist"
    assert error["details"]["path"] == str(missing)
    relative = await client.post(
        _url("/sessions"), json={"goal": "g", "user_message": "m", "working_space": "./relative"}
    )
    assert _error(relative)["details"]["reason"] == "not_absolute"
    assert manager.list_sessions() == []  # refused before anything was created
    assert manager.working_spaces == {}


async def given_the_default_configuration_when_a_desktop_origin_preflights_then_it_is_allowed() -> (
    None
):
    manager = FakeConversationManager(AppConfig())
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None)
    async with _client_for(app) as client:
        for origin in (
            # ADR-028 §3: the desktop front pins its Vite dev server to 1420 (strictPort)
            "http://localhost:1420",
            "http://127.0.0.1:1420",
            "http://localhost:3000",
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "tauri://localhost",
        ):
            preflight = await client.options(
                _url("/sessions"),
                headers={
                    "Origin": origin,
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "content-type",
                },
            )
            assert preflight.status_code == 200, (origin, preflight.text)
            assert preflight.headers["access-control-allow-origin"] == origin
        denied = await client.get(_url("/health"), headers={"Origin": "http://evil.example"})
        assert "access-control-allow-origin" not in denied.headers


def given_the_code_defaults_when_read_then_the_front_dev_server_port_is_allowed() -> None:
    """ADR-028 §3: 1420 on both spellings of the loopback, and nothing removed."""
    origins = AppConfig().api.cors_origins
    assert "http://localhost:1420" in origins and "http://127.0.0.1:1420" in origins
    assert {
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "tauri://localhost",
    } <= set(origins)


# ================================================================================================
# opening a session without a message, and the user it belongs to (ADR-028)
# ================================================================================================
async def given_no_goal_and_no_message_when_session_created_then_ready_and_nothing_sent(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """ADR-028 §1: the sign-in screen has nothing to say yet, so nothing is said."""
    response = await client.post(_url("/sessions"), json={})

    assert response.status_code == 201, response.text
    body = response.json()
    sid = body["session_id"]
    assert body["status"] == "READY"
    assert body["goal"] == "" and body["user_message"] == ""
    assert body["current_conversation_id"] is None
    assert body["started_at"] is None
    # no conversation, hence no cycle and no message: nothing was posted to the model, and the
    # only thing that ever happened to this session is its own creation
    assert manager.store.list_conversations(sid) == []
    assert [event.event_type for event in manager.store.list_audit_events(sid)] == [
        EventType.SESSION_CREATED.value
    ]
    assert _created_payload(manager, sid)["goal"] == ""
    # the budget is the one of the configuration, exactly as for a session that starts at once
    assert body["budget"] == {"max_cycles": 7, "max_plans": 3, "max_total_duration_ms": 300_000}


async def given_an_empty_session_when_the_first_message_arrives_then_it_opens_the_first_cycle(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """ADR-028 §1: the first message takes the path of a message after an interruption."""
    sid = (await client.post(_url("/sessions"), json={})).json()["session_id"]

    response = await client.post(
        _url(f"/sessions/{sid}/messages"), json={"user_message": "debug my build"}
    )

    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == "RUNNING"
    assert body["user_message"] == "debug my build"
    assert body["goal"] == "debug my build"  # the first message becomes the goal
    conversations = manager.store.list_conversations(sid)
    assert len(conversations) == 1
    assert conversations[0].parent_conversation_id is None
    assert body["current_conversation_id"] == conversations[0].conversation_id


async def given_a_session_opened_with_a_goal_when_a_follow_up_arrives_then_the_goal_is_kept(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """The promotion of ADR-028 §1 only fills a goal that is missing; it never rewrites one."""
    sid = (
        await client.post(_url("/sessions"), json={"goal": "fix the build", "user_message": "m"})
    ).json()["session_id"]
    await manager.interrupt(sid)

    body = (
        await client.post(_url(f"/sessions/{sid}/messages"), json={"user_message": "and now this"})
    ).json()

    assert body["goal"] == "fix the build"
    assert body["user_message"] == "and now this"


async def given_an_empty_session_when_interrupted_and_listed_then_idle_and_visible(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """A session with no conversation is an ordinary idle session: nothing to interrupt, listed."""
    sid = (await client.post(_url("/sessions"), json={})).json()["session_id"]

    report = await client.post(_url(f"/sessions/{sid}/interrupt"))
    assert report.status_code == 200, report.text
    assert report.json()["nothing_to_interrupt"] is True
    assert report.json()["conversation_id"] is None
    assert report.json()["session_status"] == "READY"

    listed = (await client.get(_url("/sessions"), params={"status": "ready"})).json()
    assert [item["session_id"] for item in listed["items"]] == [sid]
    single = (await client.get(_url(f"/sessions/{sid}"))).json()
    assert single["conversation"] is None
    snapshot = (await client.get(_url(f"/sessions/{sid}/snapshot"))).json()
    assert snapshot["conversation"] is None and snapshot["conversations"] == []
    assert snapshot["session"]["status"] == "READY"


@pytest.mark.parametrize(
    ("body", "code", "field"),
    [
        ({"user_message": "m"}, "GOAL_REQUIRED", "goal"),
        ({"goal": "g"}, "USER_MESSAGE_REQUIRED", "user_message"),
    ],
    ids=["message_without_goal", "goal_without_message"],
)
async def given_half_an_opening_message_when_session_created_then_400_and_no_session(
    client: httpx.AsyncClient,
    manager: FakeConversationManager,
    body: dict[str, Any],
    code: str,
    field: str,
) -> None:
    """ADR-028 §1: the opening message is a pair; half of one is refused before anything exists."""
    response = await client.post(_url("/sessions"), json=body)

    assert response.status_code == 400, response.text
    error = _error(response)
    assert error["error_code"] == code
    assert error["details"]["field"] == field
    assert error["details"]["expected"] == ["goal", "user_message"]
    assert manager.list_sessions() == []
    assert manager.calls == []  # refused by the route, the façade never saw it


async def given_a_user_id_when_session_created_then_the_session_carries_it(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    response = await client.post(_url("/sessions"), json={"user_id": "alice"})
    assert response.status_code == 201, response.text
    assert response.json()["user_id"] == "alice"
    assert manager.require_session(response.json()["session_id"]).user_id == "alice"


async def given_no_user_id_when_session_created_then_the_one_whoami_reports(
    manager: FakeConversationManager,
) -> None:
    """ADR-028 §2: the two routes answer the same name, whatever the configuration says."""
    identity = UserIdentity(user_id="alice", source="env:USER", host="workstation")
    app = create_app(manager, clock=manager.clock, sse_heartbeat_s=None, identity=identity)
    async with _client_for(app) as client:
        whoami = (await client.get(_url("/whoami"))).json()
        created = (await client.post(_url("/sessions"), json={})).json()
        started = (
            await client.post(_url("/sessions"), json={"goal": "g", "user_message": "m"})
        ).json()

    assert whoami["user_id"] == "alice"
    assert created["user_id"] == "alice" == started["user_id"]
    assert manager.config.transport.user_id == "local-user"  # the constant it used to contradict


# ================================================================================================
# skills and the two sign-in extras of a session (ADR-027 §4)
# ================================================================================================
def _skills_app(**skills: Any) -> tuple[FakeConversationManager, FastAPI]:
    manager = FakeConversationManager(AppConfig(api=ApiSection(), skills=SkillsSection(**skills)))
    return manager, create_app(manager, clock=manager.clock, sse_heartbeat_s=None)


async def given_a_skills_root_when_skills_requested_then_the_notes_sorted_by_name(
    tmp_path: Path,
) -> None:
    (tmp_path / "review.md").write_text("review", encoding="utf-8")
    (tmp_path / "deploy.md").write_text("deploy", encoding="utf-8")
    (tmp_path / "notes.txt").write_text("not a skill", encoding="utf-8")
    (tmp_path / "java").mkdir()
    (tmp_path / "java" / "build.md").write_text("build", encoding="utf-8")
    _, app = _skills_app(root=str(tmp_path))
    async with _client_for(app) as client:
        response = await client.get(_url("/skills"))
    assert response.status_code == 200, response.text
    assert response.json() == {
        "skills": [
            {"name": "build", "path": str(tmp_path / "java" / "build.md")},
            {"name": "deploy", "path": str(tmp_path / "deploy.md")},
            {"name": "review", "path": str(tmp_path / "review.md")},
        ]
    }


@pytest.mark.parametrize(
    "section",
    [{}, {"root": "  "}, {"root": "/nowhere-at-all/agentic-skills"}, {"enabled": False}],
)
async def given_no_usable_root_when_skills_requested_then_200_and_an_empty_list(
    section: dict[str, Any], tmp_path: Path
) -> None:
    """The sign-in screen must degrade quietly: never an error, always a list."""
    if section.get("enabled") is False:
        (tmp_path / "deploy.md").write_text("deploy", encoding="utf-8")
        section = {**section, "root": str(tmp_path)}
    _, app = _skills_app(**section)
    async with _client_for(app) as client:
        response = await client.get(_url("/skills"))
    assert response.status_code == 200, response.text
    assert response.json() == {"skills": []}


def _created_payload(manager: FakeConversationManager, sid: str) -> dict[str, Any]:
    events = [
        event
        for event in manager.store.list_audit_events(sid)
        if event.event_type == EventType.SESSION_CREATED.value
    ]
    assert len(events) == 1
    return dict(events[0].payload)


async def given_skills_and_effort_when_session_created_then_traced_in_the_created_event(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    """ADR-027 §4: recorded in the event payload, and acted upon nowhere."""
    response = await client.post(
        _url("/sessions"),
        json={
            "goal": "g",
            "user_message": "m",
            "skills": ["deploy", "/home/user/skills/review.md"],
            "effort": "high",
        },
    )
    assert response.status_code == 201, response.text
    sid = response.json()["session_id"]
    payload = _created_payload(manager, sid)
    assert payload["skills"] == ["deploy", "/home/user/skills/review.md"]
    assert payload["effort"] == "high"
    assert manager.session_extras[sid] == (["deploy", "/home/user/skills/review.md"], "high")
    # nothing else carries them: the record is untouched (no column, ADR-027 §4)
    assert "skills" not in response.json() and "effort" not in response.json()


async def given_no_extras_when_session_created_then_the_payload_still_carries_both_keys(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    response = await client.post(_url("/sessions"), json={"goal": "g", "user_message": "m"})
    assert response.status_code == 201, response.text
    payload = _created_payload(manager, response.json()["session_id"])
    assert payload["skills"] == [] and payload["effort"] is None


@pytest.mark.parametrize("effort", ["low", "medium", "high"])
async def given_a_known_effort_level_when_session_created_then_accepted(
    client: httpx.AsyncClient, manager: FakeConversationManager, effort: str
) -> None:
    response = await client.post(
        _url("/sessions"), json={"goal": "g", "user_message": "m", "effort": effort}
    )
    assert response.status_code == 201, response.text
    assert _created_payload(manager, response.json()["session_id"])["effort"] == effort


@pytest.mark.parametrize("effort", ["HIGH", "extreme", "", "1"])
async def given_an_unknown_effort_level_when_session_created_then_400_and_no_session(
    client: httpx.AsyncClient, manager: FakeConversationManager, effort: str
) -> None:
    response = await client.post(
        _url("/sessions"), json={"goal": "g", "user_message": "m", "effort": effort}
    )
    assert response.status_code == 400, response.text
    error = _error(response)
    assert error["error_code"] == "EFFORT_INVALID"
    assert error["details"] == {
        "message": "unknown effort level",
        "effort": effort,
        "expected": ["low", "medium", "high"],
    }
    assert manager.list_sessions() == []  # refused before anything was created
