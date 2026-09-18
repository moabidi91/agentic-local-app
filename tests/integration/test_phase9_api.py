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
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from agentic_local_app import __version__
from agentic_local_app.config import ApiSection, AppConfig, BudgetSection, TransportSection
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
        {"user_message": "m"},
        {"goal": "g"},
        {"goal": "g", "user_message": "m", "session_budget": {"max_cycles": 0}},
        {"goal": "g", "user_message": "m", "unknown": 1},
    ],
    ids=["missing_goal", "missing_message", "invalid_budget", "unknown_field"],
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


async def given_running_session_when_follow_up_posted_then_409_conflict(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
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


async def given_unknown_session_or_task_when_sse_route_requested_then_404(
    client: httpx.AsyncClient, manager: FakeConversationManager
) -> None:
    sid = await _start(manager)
    assert (await client.get(_url("/sessions/nope/events"))).status_code == 404
    assert (await client.get(_url(f"/sessions/{sid}/tasks/t9/output/live"))).status_code == 404
