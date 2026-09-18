"""Mock model server — a FastAPI application playing the remote model (ADR-004 "Serveur mock").

It implements **exactly** the transport contract:

| Operation | Route | Reply |
|---|---|---|
| init  | ``POST /v1/conversations`` | ``201 {"conversation_id"}`` |
| post  | ``POST /v1/conversations/{cid}/messages`` | ``202 {"accepted": true, "message_id"}``, idempotent on ``message_id`` |
| get   | ``GET /v1/conversations/{cid}/messages?after=`` | ``200 {"messages": [...], "cursor"}`` (model messages after the cursor) |
| close | ``POST /v1/conversations/{cid}/close`` | ``200 {"closed": true, "conversation_id"}`` |

``X-User-Id`` is required on every request (``400 missing_user_id``); when the scenario defines a
``token`` the ``Authorization: Bearer <token>`` header is verified (``401 invalid_token``). Request
bodies may be gzip-encoded (``Content-Encoding: gzip``).

**Scenario engine.** A :class:`Scenario` is an ordered list of :class:`Step`; the steps are consumed
one per accepted POST, across conversations (so a rotation simply continues the script in the child
conversation). A step applies when its ``on`` is ``"*"`` or equals the posted message ``type``; a
mismatch is answered ``409 unexpected_message_type`` and the step is kept. When the steps are
exhausted, POSTs are still accepted but the model stays silent (the application then hits
``MODEL_GET_TIMEOUT``). The step's ``respond`` messages are published to the conversation outbox,
``{conversation_id}`` replaced in every string and ``"message_id": "auto"`` replaced by
``mock-msg-0001``, ``mock-msg-0002``...; they become visible to GET after ``delay_ms``.

A :class:`Fault` on a step applies ``times`` times, then normal behaviour resumes: ``on_operation``
``"post"`` faults the POST that would consume the step (the message is not recorded, so a retry is
processed normally); ``"init"`` faults the init performed while the step is pending; ``"get"`` /
``"close"`` are armed when the step is consumed and fault the following GETs / closes. ``status``
defaults to 200 and ``body`` to ``{"error": "injected_fault", ...}``, which lets a scenario produce
any HTTP status of the ADR-004 mapping table or an out-of-protocol body.

**Time.** Reply availability (``delay_ms``) is the only place where this test tool measures real
time; it goes through the injectable ``now_ms`` callable, ``SystemClock().monotonic_ms`` by default,
so that tests inject a ``FakeClock`` and never wait. Production code never imports this module.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from agentic_local_app.domain.clock import SystemClock

__all__ = [
    "Fault",
    "MockEngine",
    "Scenario",
    "Step",
    "create_mock_app",
    "default_java_debug_scenario",
    "load_scenario",
    "run_mock_server",
]

OperationName = Literal["init", "post", "get", "close"]
Reply = tuple[int, dict[str, Any]]

CONVERSATION_ID_PLACEHOLDER = "{conversation_id}"
AUTO_MESSAGE_ID = "auto"


# ------------------------------------------------------------------------------------------------
# scenario model
# ------------------------------------------------------------------------------------------------
class _ScenarioModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Fault(_ScenarioModel):
    """An injected failure. ``status`` None -> 200 (out-of-protocol body); ``body`` None -> default."""

    status: int | None = Field(default=None, ge=100, le=599)
    body: dict[str, Any] | None = None
    times: int = Field(default=1, ge=1)
    on_operation: OperationName = "post"


class Step(_ScenarioModel):
    """One model turn: expected inbound type (``"*"`` = any), replies, delay, optional fault."""

    on: str = "*"
    respond: list[dict[str, Any]] = Field(default_factory=list)
    delay_ms: int = Field(default=0, ge=0)
    fault: Fault | None = None


class Scenario(_ScenarioModel):
    steps: list[Step] = Field(default_factory=list)
    token: str | None = None


def load_scenario(path: str | Path) -> Scenario:
    """Load a scenario from a JSON file; ``ValueError`` when the content does not match the model."""
    text = Path(path).read_text(encoding="utf-8")
    try:
        return Scenario.model_validate_json(text)
    except ValidationError as exc:
        raise ValueError(f"invalid scenario {path}: {exc}") from exc


# ------------------------------------------------------------------------------------------------
# engine (pure Python, no HTTP)
# ------------------------------------------------------------------------------------------------
@dataclass
class _Conversation:
    conversation_id: str
    outbox: list[tuple[int, dict[str, Any]]] = field(default_factory=list)  # (available_at_ms, msg)
    acks: dict[str, dict[str, Any]] = field(default_factory=dict)  # message_id -> ack body
    last_step: int | None = None
    closed: bool = False


class MockEngine:
    """The scenario interpreter behind the routes. Exposed as ``app.state.engine`` for assertions."""

    def __init__(self, scenario: Scenario, now_ms: Callable[[], int] | None = None) -> None:
        self.scenario = scenario
        self._now_ms = now_ms if now_ms is not None else SystemClock().monotonic_ms
        self.conversations: dict[str, _Conversation] = {}
        self.inits: list[dict[str, Any]] = []
        self.received: list[tuple[str, dict[str, Any]]] = []
        self.closed: list[str] = []
        self._next_step = 0
        self._fault_remaining: dict[int, int] = {
            index: step.fault.times
            for index, step in enumerate(scenario.steps)
            if step.fault is not None
        }
        self._conversation_counter = 0
        self._message_counter = 0

    # ---- authentication -------------------------------------------------------------------
    def authorize(self, headers: Mapping[str, str]) -> Reply | None:
        """``None`` when the request may proceed, else the 400 / 401 reply."""
        if not headers.get("x-user-id", "").strip():
            return 400, {"error": "missing_user_id"}
        token = self.scenario.token
        if token is not None and headers.get("authorization", "") != f"Bearer {token}":
            return 401, {"error": "invalid_token"}
        return None

    # ---- operations -----------------------------------------------------------------------
    def init(self, body: Any) -> Reply:
        if not isinstance(body, dict):
            return 400, {"error": "invalid_json"}
        fault = self._take_fault(self._next_step, "init")
        if fault is not None:
            return self._fault_reply(fault, "init")
        self._conversation_counter += 1
        conversation_id = f"mock-conv-{self._conversation_counter:04d}"
        self.conversations[conversation_id] = _Conversation(conversation_id)
        self.inits.append(dict(body))
        return 201, {"conversation_id": conversation_id}

    def post(self, conversation_id: str, body: Any) -> Reply:
        conversation = self.conversations.get(conversation_id)
        if conversation is None:
            return 404, {"error": "unknown_conversation", "conversation_id": conversation_id}
        if conversation.closed:
            return 410, {"error": "conversation_closed", "conversation_id": conversation_id}
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("type"), str)
            or not isinstance(body.get("message_id"), str)
            or not body["message_id"]
        ):
            return 400, {"error": "invalid_message", "reason": "type and message_id are required"}
        message_id: str = body["message_id"]
        cached = conversation.acks.get(message_id)
        if cached is not None:
            return 202, dict(cached)  # idempotent re-POST: same ack, nothing republished
        fault = self._take_fault(self._next_step, "post")
        if fault is not None:
            return self._fault_reply(fault, "post")
        if self._next_step < len(self.scenario.steps):
            step = self.scenario.steps[self._next_step]
            if step.on != "*" and step.on != body["type"]:
                return 409, {
                    "error": "unexpected_message_type",
                    "expected": step.on,
                    "received": body["type"],
                }
            conversation.last_step = self._next_step
            self._next_step += 1
            available_at = self._now_ms() + step.delay_ms
            for template in step.respond:
                conversation.outbox.append(
                    (available_at, self._materialise(template, conversation_id))
                )
        ack = {"accepted": True, "message_id": message_id}
        conversation.acks[message_id] = ack
        self.received.append((conversation_id, dict(body)))
        return 202, dict(ack)

    def get(self, conversation_id: str, after: str | None) -> Reply:
        conversation = self.conversations.get(conversation_id)
        if conversation is None:
            return 404, {"error": "unknown_conversation", "conversation_id": conversation_id}
        if conversation.last_step is not None:
            fault = self._take_fault(conversation.last_step, "get")
            if fault is not None:
                return self._fault_reply(fault, "get")
        cursor = after or None
        start = 0
        if cursor is not None:
            for index, (_, message) in enumerate(conversation.outbox):
                if message.get("message_id") == cursor:
                    start = index + 1
                    break
        now = self._now_ms()
        messages: list[dict[str, Any]] = []
        for available_at, message in conversation.outbox[start:]:
            if available_at > now:
                break  # preserve ordering: never skip a not-yet-available message
            messages.append(message)
        if messages:
            cursor = str(messages[-1]["message_id"])
        return 200, {"messages": messages, "cursor": cursor}

    def close(self, conversation_id: str) -> Reply:
        conversation = self.conversations.get(conversation_id)
        if conversation is None:
            return 404, {"error": "unknown_conversation", "conversation_id": conversation_id}
        if conversation.last_step is not None:
            fault = self._take_fault(conversation.last_step, "close")
            if fault is not None:
                return self._fault_reply(fault, "close")
        conversation.closed = True
        self.closed.append(conversation_id)
        return 200, {"closed": True, "conversation_id": conversation_id}

    # ---- internals ------------------------------------------------------------------------
    def _take_fault(self, step_index: int, operation: OperationName) -> Fault | None:
        if not 0 <= step_index < len(self.scenario.steps):
            return None
        fault = self.scenario.steps[step_index].fault
        if fault is None or fault.on_operation != operation:
            return None
        if self._fault_remaining.get(step_index, 0) <= 0:
            return None
        self._fault_remaining[step_index] -= 1
        return fault

    @staticmethod
    def _fault_reply(fault: Fault, operation: OperationName) -> Reply:
        status = fault.status if fault.status is not None else 200
        body = (
            dict(fault.body)
            if fault.body is not None
            else {"error": "injected_fault", "operation": operation}
        )
        return status, body

    def _materialise(self, template: dict[str, Any], conversation_id: str) -> dict[str, Any]:
        message = _fill_placeholders(template, conversation_id)
        if not isinstance(message, dict):  # pragma: no cover - templates are dicts by schema
            raise TypeError("a reply template must be a JSON object")
        if message.get("message_id") == AUTO_MESSAGE_ID:
            self._message_counter += 1
            message["message_id"] = f"mock-msg-{self._message_counter:04d}"
        return message


def _fill_placeholders(value: Any, conversation_id: str) -> Any:
    if isinstance(value, str):
        return value.replace(CONVERSATION_ID_PLACEHOLDER, conversation_id)
    if isinstance(value, dict):
        return {key: _fill_placeholders(item, conversation_id) for key, item in value.items()}
    if isinstance(value, list):
        return [_fill_placeholders(item, conversation_id) for item in value]
    return value


# ------------------------------------------------------------------------------------------------
# FastAPI application
# ------------------------------------------------------------------------------------------------
async def _read_json(request: Request) -> Any | None:
    """The decoded JSON body (gzip-aware); ``None`` when it cannot be decoded."""
    raw = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            raw = gzip.decompress(raw)
        except (OSError, EOFError, ValueError):
            return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return None


def _json(reply: Reply) -> JSONResponse:
    status, body = reply
    return JSONResponse(status_code=status, content=body)


def create_mock_app(scenario: Scenario, *, now_ms: Callable[[], int] | None = None) -> FastAPI:
    """Build the FastAPI application for ``scenario`` (``now_ms`` injectable for deterministic delays)."""
    engine = MockEngine(scenario, now_ms=now_ms)
    app = FastAPI(title="agentic-local-app mock model server", docs_url=None, redoc_url=None)
    app.state.engine = engine

    @app.post("/v1/conversations")
    async def init_conversation(request: Request) -> JSONResponse:
        denied = engine.authorize(request.headers)
        if denied is not None:
            return _json(denied)
        body = await _read_json(request)
        if body is None:
            return _json((400, {"error": "invalid_json"}))
        return _json(engine.init(body))

    @app.post("/v1/conversations/{conversation_id}/messages")
    async def post_message(conversation_id: str, request: Request) -> JSONResponse:
        denied = engine.authorize(request.headers)
        if denied is not None:
            return _json(denied)
        body = await _read_json(request)
        if body is None:
            return _json((400, {"error": "invalid_json"}))
        return _json(engine.post(conversation_id, body))

    @app.get("/v1/conversations/{conversation_id}/messages")
    async def get_messages(conversation_id: str, request: Request, after: str = "") -> JSONResponse:
        denied = engine.authorize(request.headers)
        if denied is not None:
            return _json(denied)
        return _json(engine.get(conversation_id, after or None))

    @app.post("/v1/conversations/{conversation_id}/close")
    async def close_conversation(conversation_id: str, request: Request) -> JSONResponse:
        denied = engine.authorize(request.headers)
        if denied is not None:
            return _json(denied)
        return _json(engine.close(conversation_id))

    return app


def run_mock_server(
    host: str,
    port: int,
    scenario_path: str | Path | None,
    *,
    runner: Callable[..., Any] | None = None,
) -> None:
    """Serve a scenario with uvicorn (``agentic-app mock-server``). ``runner`` is injectable for tests."""
    scenario = (
        load_scenario(scenario_path) if scenario_path is not None else default_java_debug_scenario()
    )
    app = create_mock_app(scenario)
    if runner is None:
        import uvicorn

        runner = uvicorn.run
    runner(app, host=host, port=port, log_level="info")


# ------------------------------------------------------------------------------------------------
# default scenario: the §12 Java debugging loop (12.2 -> 12.3 -> 12.7)
# ------------------------------------------------------------------------------------------------
def default_java_debug_scenario() -> Scenario:
    """``user_request -> discovery_plan (§12.2) -> execution_result -> execution_plan (§12.3) ->
    execution_result -> final_answer (§12.7)``."""
    discovery_plan = {
        "type": "discovery_plan",
        "conversation_id": CONVERSATION_ID_PLACEHOLDER,
        "message_id": AUTO_MESSAGE_ID,
        "content": {
            "plan_id": "plan-0",
            "objective": "Discover execution environment and build context",
            "execution_policy": "sequential",
            "tasks": [
                {
                    "task_id": "t1",
                    "type": "cmd",
                    "cmd": "uname -a && echo $SHELL && echo $PWD",
                    "critical": False,
                    "continue_on_error": True,
                    "max_output_bytes": 2048,
                },
                {
                    "task_id": "t2",
                    "type": "cmd",
                    "cmd": "java -version",
                    "critical": False,
                    "continue_on_error": True,
                    "max_output_bytes": 1024,
                },
                {
                    "task_id": "t3",
                    "type": "cmd",
                    "cmd": "mvn -version",
                    "critical": False,
                    "continue_on_error": True,
                    "max_output_bytes": 1024,
                },
                {
                    "task_id": "t4",
                    "type": "cmd",
                    "cmd": "test -f pom.xml && sed -n '1,220p' pom.xml",
                    "critical": True,
                    "continue_on_error": False,
                    "stop_plan_on_failure": True,
                    "max_output_bytes": 16384,
                },
                {
                    "task_id": "t5",
                    "type": "cmd",
                    "cmd": "mvn clean install 2>&1 | tail -80",
                    "critical": True,
                    "continue_on_error": False,
                    "stop_plan_on_failure": True,
                    "depends_on": ["t4"],
                    "max_output_bytes": 32768,
                },
            ],
        },
    }
    execution_plan = {
        "type": "execution_plan",
        "conversation_id": CONVERSATION_ID_PLACEHOLDER,
        "message_id": AUTO_MESSAGE_ID,
        "content": {
            "plan_id": "plan-1",
            "objective": "Confirm Java version mismatch between Maven runtime and project target",
            "execution_policy": "parallel",
            "max_parallel_workers": 2,
            "tasks": [
                {
                    "task_id": "t6",
                    "type": "cmd",
                    "cmd": "echo $JAVA_HOME",
                    "critical": False,
                    "continue_on_error": True,
                    "max_output_bytes": 512,
                },
                {
                    "task_id": "t7",
                    "type": "cmd",
                    "cmd": 'grep -n "maven.compiler.source\\|maven.compiler.target" pom.xml',
                    "critical": True,
                    "continue_on_error": False,
                    "stop_plan_on_failure": True,
                    "max_output_bytes": 2048,
                },
            ],
        },
    }
    final_answer = {
        "type": "final_answer",
        "conversation_id": CONVERSATION_ID_PLACEHOLDER,
        "message_id": AUTO_MESSAGE_ID,
        "content": {
            "status": "completed",
            "diagnosis": (
                "The build fails because the project targets Java 21 while Maven runs with Java 17."
            ),
            "evidence": [
                "uname confirms Linux x86_64 environment, shell is bash",
                "java -version shows OpenJDK 17.0.12",
                "mvn -version confirms Maven uses Java 17",
                "pom.xml targets maven.compiler.source = 21",
                "build fails with: invalid target release: 21",
            ],
            "recommended_next_step": (
                "Run Maven with JDK 21 or align the project target version with Java 17."
            ),
        },
    }
    return Scenario(
        steps=[
            Step(on="user_request", respond=[discovery_plan]),
            Step(on="execution_result", respond=[execution_plan]),
            Step(on="execution_result", respond=[final_answer]),
        ]
    )
