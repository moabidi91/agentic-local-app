"""Phase 7 — the mock model server (ADR-004 contract) and its integration with HttpTransportGateway.

The FastAPI application runs in-process behind ``httpx.ASGITransport``: no port, no network. The
server's notion of time for ``delay_ms`` is injected (``now_ms``) so that delayed replies are tested
without waiting.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType, TransportError
from agentic_local_app.testing.mock_model_server import (
    BUILTIN_SCENARIOS,
    DEFAULT_SCENARIO_NAME,
    Fault,
    Scenario,
    Step,
    create_mock_app,
    default_analysis_scenario,
    default_java_debug_scenario,
    load_scenario,
    run_mock_server,
)
from agentic_local_app.transport.gateway import HttpTransportGateway

pytestmark = pytest.mark.phase7

TOKEN_ENV = "PHASE7_TEST_MOCK_TOKEN"
BASE = "http://mock/v1/conversations"
HEADERS = {"X-User-Id": "tester"}


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def _final_answer() -> dict[str, Any]:
    return {
        "type": "final_answer",
        "conversation_id": "{conversation_id}",
        "message_id": "auto",
        "content": {"status": "completed", "diagnosis": "d", "evidence": []},
    }


def _plan(plan_id: str = "plan-0", message_type: str = "discovery_plan") -> dict[str, Any]:
    return {
        "type": message_type,
        "conversation_id": "{conversation_id}",
        "message_id": "auto",
        "content": {
            "plan_id": plan_id,
            "objective": "o",
            "execution_policy": "sequential",
            "tasks": [{"task_id": "t1", "type": "cmd", "cmd": "echo hi"}],
        },
    }


def _app_message(
    message_type: str, message_id: str, conversation_id: str = "conv-0001"
) -> dict[str, Any]:
    return {
        "type": message_type,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "content": {"plan_id": "plan-0", "status": "completed", "results": []},
    }


def _client(app: Any, **headers: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://mock", headers=headers or HEADERS
    )


def _scenario(*steps: Step, token: str | None = None) -> Scenario:
    return Scenario(steps=list(steps), token=token)


def _section(**overrides: Any) -> TransportSection:
    values: dict[str, Any] = {
        "init_url": BASE,
        "post_url": BASE + "/{conversation_id}/messages",
        "get_url": BASE + "/{conversation_id}/messages?after={after}",
        "close_url": BASE + "/{conversation_id}/close",
        "token_env": TOKEN_ENV,
        "user_id": "tester",
        "request_timeout_ms": 1_000,
        "poll_interval_ms": 1_000,
        "reply_timeout_ms": 5_000,
        "gzip": True,
        "verify_tls": True,
    }
    values.update(overrides)
    return TransportSection(**values)


def _gateway(
    app: Any,
    clock: FakeClock | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    **overrides: Any,
) -> HttpTransportGateway:
    kwargs: dict[str, Any] = {"transport": httpx.ASGITransport(app=app)}
    if sleep is not None:
        kwargs["sleep"] = sleep
    return HttpTransportGateway(_section(**overrides), clock or FakeClock(), **kwargs)


async def _init(client: httpx.AsyncClient) -> str:
    response = await client.post(
        "/v1/conversations", json={"user_id": "tester", "instructions": "i", "metadata": {}}
    )
    assert response.status_code == 201, response.text
    return str(response.json()["conversation_id"])


async def _error_of(coro: Awaitable[Any]) -> TransportError:
    with pytest.raises(TransportError) as exc:
        await coro
    return exc.value


@pytest.fixture
async def default_client() -> AsyncIterator[httpx.AsyncClient]:
    async with _client(create_mock_app(default_java_debug_scenario())) as client:
        yield client


# ------------------------------------------------------------------------------------------------
# scenario model
# ------------------------------------------------------------------------------------------------
def given_default_scenario_when_inspected_then_three_steps_reproduce_spec_section_12() -> None:
    scenario = default_java_debug_scenario()
    assert [s.on for s in scenario.steps] == [
        "user_request",
        "execution_result",
        "execution_result",
    ]
    types = [[m["type"] for m in s.respond] for s in scenario.steps]
    assert types == [["discovery_plan"], ["execution_plan"], ["final_answer"]]
    discovery, execution, final = (s.respond[0] for s in scenario.steps)
    assert discovery["content"]["plan_id"] == "plan-0"
    assert [t["task_id"] for t in discovery["content"]["tasks"]] == ["t1", "t2", "t3", "t4", "t5"]
    assert execution["content"]["plan_id"] == "plan-1"
    assert execution["content"]["execution_policy"] == "parallel"
    assert final["content"]["status"] == "completed"
    assert all(
        m["conversation_id"] == "{conversation_id}" and m["message_id"] == "auto"
        for m in (discovery, execution, final)
    )
    assert scenario.token is None and all(
        s.fault is None and s.delay_ms == 0 for s in scenario.steps
    )


def given_json_file_when_load_scenario_then_scenario_equal_to_constructed_one(
    tmp_path: Path,
) -> None:
    expected = _scenario(
        Step(
            on="user_request",
            respond=[_plan()],
            delay_ms=250,
            fault=Fault(status=503, times=2, on_operation="post"),
        ),
        Step(on="*", respond=[_final_answer()]),
        token="t0k",
    )
    path = tmp_path / "scenario.json"
    path.write_text(
        json.dumps(
            {
                "token": "t0k",
                "steps": [
                    {
                        "on": "user_request",
                        "respond": [_plan()],
                        "delay_ms": 250,
                        "fault": {"status": 503, "times": 2, "on_operation": "post"},
                    },
                    {"respond": [_final_answer()]},
                ],
            }
        ),
        encoding="utf-8",
    )
    assert load_scenario(path) == expected


def given_scenario_json_with_unknown_field_when_loaded_then_value_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text(json.dumps({"steps": [{"respond": [], "surprise": 1}]}), encoding="utf-8")
    with pytest.raises(ValueError):
        load_scenario(path)


def given_fault_when_built_with_defaults_then_one_time_post_fault_with_no_body() -> None:
    fault = Fault(status=429)
    assert (fault.status, fault.body, fault.times, fault.on_operation) == (429, None, 1, "post")


def given_run_mock_server_when_called_with_injected_runner_then_app_host_port_forwarded(
    tmp_path: Path,
) -> None:
    captured: dict[str, Any] = {}

    def runner(app: Any, *, host: str, port: int, **kwargs: Any) -> None:
        captured.update(app=app, host=host, port=port)

    path = tmp_path / "s.json"
    path.write_text(default_java_debug_scenario().model_dump_json(), encoding="utf-8")
    run_mock_server("127.0.0.1", 9123, path, runner=runner)
    assert (captured["host"], captured["port"]) == ("127.0.0.1", 9123)
    assert len(captured["app"].state.engine.scenario.steps) == 3


def given_run_mock_server_without_scenario_path_when_called_then_default_scenario_used() -> None:
    captured: dict[str, Any] = {}

    def runner(app: Any, *, host: str, port: int, **kwargs: Any) -> None:
        captured["app"] = app

    run_mock_server("127.0.0.1", 9000, None, runner=runner)
    assert captured["app"].state.engine.scenario == default_java_debug_scenario()
    assert DEFAULT_SCENARIO_NAME == "java"
    assert BUILTIN_SCENARIOS[DEFAULT_SCENARIO_NAME]() == default_java_debug_scenario()


# ---- ADR-022: the analysis scenario (a user_response, no command) ------------------------------
def given_analysis_scenario_when_inspected_then_single_user_response_to_the_user_request() -> None:
    scenario = default_analysis_scenario()
    assert [s.on for s in scenario.steps] == ["user_request"]
    (reply,) = scenario.steps[0].respond
    assert reply["type"] == "user_response"
    assert reply["conversation_id"] == "{conversation_id}" and reply["message_id"] == "auto"
    content = reply["content"]
    assert content["format"] == "markdown" and content["status"] == "completed"
    assert content["expects_reply"] is False
    assert "invalid target release" in content["body"]
    assert scenario.token is None and scenario.steps[0].fault is None
    assert set(BUILTIN_SCENARIOS) == {"java", "analysis"}
    assert BUILTIN_SCENARIOS["analysis"]() == scenario


def given_run_mock_server_with_scenario_name_when_called_then_named_scenario_used() -> None:
    captured: dict[str, Any] = {}

    def runner(app: Any, *, host: str, port: int, **kwargs: Any) -> None:
        captured["app"] = app

    run_mock_server("127.0.0.1", 9000, None, scenario_name="analysis", runner=runner)
    assert captured["app"].state.engine.scenario == default_analysis_scenario()
    with pytest.raises(ValueError, match="unknown built-in scenario 'nope'"):
        run_mock_server("127.0.0.1", 9000, None, scenario_name="nope", runner=runner)


async def given_analysis_scenario_when_user_request_posted_then_user_response_served_by_get() -> (
    None
):
    app = create_mock_app(default_analysis_scenario())
    async with _client(app) as client:
        cid = await _init(client)
        posted = await client.post(
            f"/v1/conversations/{cid}/messages",
            json={
                "type": "user_request",
                "conversation_id": cid,
                "message_id": "msg-0001",
                "content": {
                    "goal": "Explain a Java build error",
                    "user_message": "What does invalid target release mean?",
                    "session_budget": {
                        "max_cycles": 20,
                        "max_plans": 10,
                        "max_total_duration_ms": 300000,
                    },
                },
            },
        )
        assert posted.status_code == 202
        got = await client.get(f"/v1/conversations/{cid}/messages")
    assert got.status_code == 200
    messages = got.json()["messages"]
    assert len(messages) == 1
    assert messages[0]["type"] == "user_response"
    assert messages[0]["conversation_id"] == cid
    assert messages[0]["message_id"] == "mock-msg-0001"
    assert messages[0]["content"]["format"] == "markdown"
    assert got.json()["cursor"] == "mock-msg-0001"


async def given_scenario_with_user_response_step_when_json_loaded_then_reply_kept_verbatim(
    tmp_path: Path,
) -> None:
    reply = {
        "type": "user_response",
        "conversation_id": "{conversation_id}",
        "message_id": "auto",
        "content": {"body": "Which module?", "expects_reply": True},
    }
    path = tmp_path / "question.json"
    path.write_text(
        json.dumps({"steps": [{"on": "user_request", "respond": [reply]}]}), encoding="utf-8"
    )
    scenario = load_scenario(path)
    assert scenario.steps[0].respond == [reply]
    app = create_mock_app(scenario)
    async with _client(app) as client:
        cid = await _init(client)
        await client.post(
            f"/v1/conversations/{cid}/messages", json=_app_message("user_request", "m1", cid)
        )
        got = await client.get(f"/v1/conversations/{cid}/messages")
    assert got.json()["messages"][0]["content"] == {"body": "Which module?", "expects_reply": True}


# ------------------------------------------------------------------------------------------------
# raw HTTP contract (ADR-004)
# ------------------------------------------------------------------------------------------------
async def given_mock_app_when_init_without_user_id_header_then_400() -> None:
    async with _client(create_mock_app(_scenario()), Accept="application/json") as client:
        response = await client.post(
            "/v1/conversations", json={"instructions": "i", "metadata": {}}
        )
    assert response.status_code == 400
    assert response.json()["error"] == "missing_user_id"


@pytest.mark.parametrize("operation", ["post", "get", "close"])
async def given_mock_app_when_operation_without_user_id_header_then_400(operation: str) -> None:
    app = create_mock_app(_scenario(Step(respond=[_final_answer()])))
    async with _client(app) as client:
        cid = await _init(client)
        calls = {
            "post": lambda: client.post(
                f"/v1/conversations/{cid}/messages",
                json=_app_message("user_request", "m1"),
                headers={"X-User-Id": ""},
            ),
            "get": lambda: client.get(
                f"/v1/conversations/{cid}/messages", headers={"X-User-Id": ""}
            ),
            "close": lambda: client.post(
                f"/v1/conversations/{cid}/close", headers={"X-User-Id": ""}
            ),
        }
        response = await calls[operation]()
    assert response.status_code == 400


@pytest.mark.parametrize("header", [None, "Bearer wrong", "Basic c2VjcmV0", "secret"])
async def given_scenario_with_token_when_request_without_matching_bearer_then_401(
    header: str | None,
) -> None:
    app = create_mock_app(_scenario(token="secret"))
    headers = dict(HEADERS)
    if header is not None:
        headers["Authorization"] = header
    async with _client(app, **headers) as client:
        response = await client.post(
            "/v1/conversations", json={"instructions": "i", "metadata": {}}
        )
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_token"


async def given_scenario_with_token_when_request_with_matching_bearer_then_201() -> None:
    app = create_mock_app(_scenario(token="secret"))
    async with _client(app, **HEADERS, Authorization="Bearer secret") as client:
        response = await client.post(
            "/v1/conversations", json={"instructions": "i", "metadata": {}}
        )
    assert response.status_code == 201


async def given_scenario_without_token_when_request_carries_a_bearer_then_ignored_and_accepted() -> (
    None
):
    app = create_mock_app(_scenario())
    async with _client(app, **HEADERS, Authorization="Bearer whatever") as client:
        response = await client.post(
            "/v1/conversations", json={"instructions": "i", "metadata": {}}
        )
    assert response.status_code == 201


async def given_mock_app_when_init_twice_then_201_with_sequential_conversation_ids() -> None:
    app = create_mock_app(_scenario())
    async with _client(app) as client:
        first, second = await _init(client), await _init(client)
    assert (first, second) == ("mock-conv-0001", "mock-conv-0002")
    assert app.state.engine.inits[0]["instructions"] == "i"


async def given_init_with_invalid_json_when_received_then_400() -> None:
    async with _client(create_mock_app(_scenario())) as client:
        response = await client.post(
            "/v1/conversations", content=b"not json", headers={"Content-Type": "application/json"}
        )
    assert response.status_code == 400 and response.json()["error"] == "invalid_json"


async def given_unknown_conversation_when_post_get_or_close_then_404() -> None:
    async with _client(create_mock_app(_scenario())) as client:
        post = await client.post(
            "/v1/conversations/nope/messages", json=_app_message("user_request", "m1")
        )
        get = await client.get("/v1/conversations/nope/messages")
        close = await client.post("/v1/conversations/nope/close")
    assert (post.status_code, get.status_code, close.status_code) == (404, 404, 404)


async def given_post_when_body_lacks_type_or_message_id_then_400() -> None:
    async with _client(create_mock_app(_scenario(Step(respond=[])))) as client:
        cid = await _init(client)
        response = await client.post(f"/v1/conversations/{cid}/messages", json={"content": {}})
    assert response.status_code == 400 and response.json()["error"] == "invalid_message"


async def given_post_when_same_message_id_twice_then_same_ack_and_single_message_recorded() -> None:
    app = create_mock_app(
        _scenario(Step(on="user_request", respond=[_plan()]), Step(respond=[_final_answer()]))
    )
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        first = await client.post(url, json=_app_message("user_request", "msg-0001"))
        second = await client.post(url, json=_app_message("user_request", "msg-0001"))
        listing = await client.get(url)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json() == {"accepted": True, "message_id": "msg-0001"}
    assert [m["message_id"] for _, m in app.state.engine.received] == ["msg-0001"]
    assert [m["type"] for m in listing.json()["messages"]] == ["discovery_plan"]  # published once


async def given_post_with_gzip_body_when_received_then_decoded_and_accepted() -> None:
    app = create_mock_app(_scenario(Step(respond=[_final_answer()])))
    async with _client(app) as client:
        cid = await _init(client)
        body = gzip.compress(json.dumps(_app_message("user_request", "m1")).encode())
        response = await client.post(
            f"/v1/conversations/{cid}/messages",
            content=body,
            headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
        )
    assert response.status_code == 202
    assert app.state.engine.received[0][1]["message_id"] == "m1"


async def given_post_with_corrupt_gzip_when_received_then_400() -> None:
    async with _client(create_mock_app(_scenario(Step(respond=[])))) as client:
        cid = await _init(client)
        response = await client.post(
            f"/v1/conversations/{cid}/messages",
            content=b"\x1f\x8bgarbage",
            headers={"Content-Type": "application/json", "Content-Encoding": "gzip"},
        )
    assert response.status_code == 400 and response.json()["error"] == "invalid_json"


async def given_step_on_mismatch_when_post_then_409_and_step_not_consumed() -> None:
    app = create_mock_app(_scenario(Step(on="user_request", respond=[_plan()])))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        wrong = await client.post(url, json=_app_message("execution_result", "m1"))
        right = await client.post(url, json=_app_message("user_request", "m2"))
        listing = await client.get(url)
    assert wrong.status_code == 409
    assert wrong.json() == {
        "error": "unexpected_message_type",
        "expected": "user_request",
        "received": "execution_result",
    }
    assert right.status_code == 202
    assert [m["type"] for m in listing.json()["messages"]] == ["discovery_plan"]


async def given_steps_exhausted_when_post_then_202_and_model_stays_silent() -> None:
    app = create_mock_app(_scenario(Step(respond=[_final_answer()])))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        await client.post(url, json=_app_message("user_request", "m1"))
        response = await client.post(url, json=_app_message("execution_result", "m2"))
        listing = await client.get(url, params={"after": "mock-msg-0001"})
    assert response.status_code == 202 and response.json()["accepted"] is True
    assert listing.json() == {"messages": [], "cursor": "mock-msg-0001"}


async def given_step_with_several_replies_when_get_then_all_delivered_in_order_with_cursor() -> (
    None
):
    app = create_mock_app(
        _scenario(Step(respond=[_plan("plan-0"), _plan("plan-1", "execution_plan")]))
    )
    async with _client(app) as client:
        cid = await _init(client)
        await client.post(
            f"/v1/conversations/{cid}/messages", json=_app_message("user_request", "m1")
        )
        listing = await client.get(f"/v1/conversations/{cid}/messages", params={"after": ""})
    body = listing.json()
    assert [m["message_id"] for m in body["messages"]] == ["mock-msg-0001", "mock-msg-0002"]
    assert [m["conversation_id"] for m in body["messages"]] == [cid, cid]
    assert body["cursor"] == "mock-msg-0002"


async def given_get_with_cursor_when_called_then_only_messages_after_cursor() -> None:
    app = create_mock_app(_scenario(Step(respond=[_plan()]), Step(respond=[_final_answer()])))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        await client.post(url, json=_app_message("user_request", "m1"))
        first = await client.get(url)
        await client.post(url, json=_app_message("execution_result", "m2"))
        second = await client.get(url, params={"after": first.json()["cursor"]})
        everything = await client.get(url)
    assert [m["type"] for m in first.json()["messages"]] == ["discovery_plan"]
    assert [m["type"] for m in second.json()["messages"]] == ["final_answer"]
    assert second.json()["cursor"] == "mock-msg-0002"
    assert [m["type"] for m in everything.json()["messages"]] == ["discovery_plan", "final_answer"]


async def given_get_without_available_messages_when_called_then_empty_and_cursor_echoes_after() -> (
    None
):
    async with _client(create_mock_app(_scenario())) as client:
        cid = await _init(client)
        no_cursor = await client.get(f"/v1/conversations/{cid}/messages")
        with_cursor = await client.get(f"/v1/conversations/{cid}/messages", params={"after": "x"})
    assert no_cursor.json() == {"messages": [], "cursor": None}
    assert with_cursor.json() == {"messages": [], "cursor": "x"}


async def given_delay_ms_when_get_before_delay_then_empty_then_visible_once_elapsed() -> None:
    clock = FakeClock()
    app = create_mock_app(
        _scenario(Step(respond=[_plan()], delay_ms=1_500)), now_ms=clock.monotonic_ms
    )
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        await client.post(url, json=_app_message("user_request", "m1"))
        early = await client.get(url)
        clock.advance(1_499)
        still_early = await client.get(url)
        clock.advance(1)
        ready = await client.get(url)
    assert early.json()["messages"] == [] and still_early.json()["messages"] == []
    assert [m["type"] for m in ready.json()["messages"]] == ["discovery_plan"]


async def given_post_fault_when_posting_then_fault_returned_times_then_message_processed() -> None:
    fault = Fault(status=503, body={"error": "unavailable"}, times=2, on_operation="post")
    app = create_mock_app(_scenario(Step(on="user_request", respond=[_plan()], fault=fault)))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        responses = [
            await client.post(url, json=_app_message("user_request", "m1")) for _ in range(3)
        ]
        listing = await client.get(url)
    assert [r.status_code for r in responses] == [503, 503, 202]
    assert responses[0].json() == {"error": "unavailable"}
    assert len(app.state.engine.received) == 1  # the faulted POSTs were not recorded
    assert [m["type"] for m in listing.json()["messages"]] == ["discovery_plan"]


async def given_get_fault_when_polling_then_fault_returned_then_replies_available() -> None:
    fault = Fault(status=429, body={"error": "slow down"}, times=1, on_operation="get")
    app = create_mock_app(_scenario(Step(respond=[_plan()], fault=fault)))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        before_post = await client.get(url)
        await client.post(url, json=_app_message("user_request", "m1"))
        faulted = await client.get(url)
        recovered = await client.get(url)
    assert before_post.status_code == 200  # the fault is armed by the step, not before
    assert faulted.status_code == 429 and faulted.json() == {"error": "slow down"}
    assert recovered.status_code == 200
    assert [m["type"] for m in recovered.json()["messages"]] == ["discovery_plan"]


async def given_init_fault_when_initialising_then_fault_then_normal_init() -> None:
    fault = Fault(status=500, times=1, on_operation="init")
    app = create_mock_app(_scenario(Step(respond=[_plan()], fault=fault)))
    async with _client(app) as client:
        first = await client.post("/v1/conversations", json={"instructions": "i", "metadata": {}})
        second = await client.post("/v1/conversations", json={"instructions": "i", "metadata": {}})
    assert first.status_code == 500 and first.json()["error"] == "injected_fault"
    assert second.status_code == 201


async def given_close_fault_when_closing_then_fault_then_closed() -> None:
    fault = Fault(status=502, times=1, on_operation="close")
    app = create_mock_app(_scenario(Step(respond=[_plan()], fault=fault)))
    async with _client(app) as client:
        cid = await _init(client)
        await client.post(
            f"/v1/conversations/{cid}/messages", json=_app_message("user_request", "m1")
        )
        first = await client.post(f"/v1/conversations/{cid}/close")
        second = await client.post(f"/v1/conversations/{cid}/close")
    assert first.status_code == 502
    assert second.status_code == 200 and second.json() == {"closed": True, "conversation_id": cid}
    assert app.state.engine.closed == [cid]


async def given_fault_without_status_when_applied_then_200_with_given_body() -> None:
    fault = Fault(body={"weird": True}, on_operation="post")
    app = create_mock_app(_scenario(Step(respond=[_plan()], fault=fault)))
    async with _client(app) as client:
        cid = await _init(client)
        response = await client.post(
            f"/v1/conversations/{cid}/messages", json=_app_message("user_request", "m1")
        )
    assert response.status_code == 200 and response.json() == {"weird": True}


async def given_close_when_called_then_200_and_further_posts_rejected_with_410() -> None:
    app = create_mock_app(_scenario(Step(respond=[_plan()])))
    async with _client(app) as client:
        cid = await _init(client)
        closed = await client.post(f"/v1/conversations/{cid}/close")
        after = await client.post(
            f"/v1/conversations/{cid}/messages", json=_app_message("user_request", "m1")
        )
    assert closed.status_code == 200 and after.status_code == 410


async def given_placeholders_when_step_responds_then_conversation_id_and_message_ids_filled() -> (
    None
):
    reply = _final_answer()
    reply["content"]["evidence"] = ["seen in {conversation_id}"]
    app = create_mock_app(_scenario(Step(respond=[reply, _plan()])))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        await client.post(url, json=_app_message("user_request", "m1"))
        listing = await client.get(url)
    messages = listing.json()["messages"]
    assert [m["message_id"] for m in messages] == ["mock-msg-0001", "mock-msg-0002"]
    assert messages[0]["conversation_id"] == cid == "mock-conv-0001"
    assert messages[0]["content"]["evidence"] == [f"seen in {cid}"]


async def given_explicit_message_id_in_reply_when_published_then_kept_verbatim() -> None:
    reply = _final_answer()
    reply["message_id"] = "fixed-id"
    app = create_mock_app(_scenario(Step(respond=[reply])))
    async with _client(app) as client:
        cid = await _init(client)
        url = f"/v1/conversations/{cid}/messages"
        await client.post(url, json=_app_message("user_request", "m1"))
        listing = await client.get(url)
    assert listing.json()["cursor"] == "fixed-id"


async def given_default_scenario_when_driven_over_raw_http_then_protocol_replies_in_order(
    default_client: httpx.AsyncClient,
) -> None:
    cid = await _init(default_client)
    url = f"/v1/conversations/{cid}/messages"
    await default_client.post(url, json=_app_message("user_request", "msg-0001"))
    discovery = (await default_client.get(url)).json()
    await default_client.post(url, json=_app_message("execution_result", "msg-0002"))
    execution = (await default_client.get(url, params={"after": discovery["cursor"]})).json()
    await default_client.post(url, json=_app_message("execution_result", "msg-0003"))
    final = (await default_client.get(url, params={"after": execution["cursor"]})).json()
    assert [m["type"] for m in discovery["messages"]] == ["discovery_plan"]
    assert [m["type"] for m in execution["messages"]] == ["execution_plan"]
    assert [m["type"] for m in final["messages"]] == ["final_answer"]
    assert (discovery["cursor"], execution["cursor"], final["cursor"]) == (
        "mock-msg-0001",
        "mock-msg-0002",
        "mock-msg-0003",
    )


# ------------------------------------------------------------------------------------------------
# HttpTransportGateway <-> mock server through ASGITransport
# ------------------------------------------------------------------------------------------------
async def given_default_scenario_when_full_loop_driven_through_http_gateway_then_types_and_cursors_follow_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    app = create_mock_app(default_java_debug_scenario())
    gateway = _gateway(app)

    remote = await gateway.init_conversation("PROTOCOL", {"session_id": "sess-0001"})
    assert remote == "mock-conv-0001"
    assert app.state.engine.inits[0] == {
        "user_id": "tester",
        "instructions": "PROTOCOL",
        "metadata": {"session_id": "sess-0001"},
    }

    ack = await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    assert ack.accepted and ack.message_id == "msg-0001" and ack.http_status == 202
    discovery = await gateway.wait_for_reply(remote, None)
    assert [m["type"] for m in discovery.messages] == ["discovery_plan"]
    assert discovery.messages[0]["conversation_id"] == remote
    assert discovery.cursor == "mock-msg-0001"

    await gateway.post_message(remote, _app_message("execution_result", "msg-0002"))
    execution = await gateway.wait_for_reply(remote, discovery.cursor)
    assert [m["type"] for m in execution.messages] == ["execution_plan"]
    assert execution.messages[0]["content"]["plan_id"] == "plan-1"

    await gateway.post_message(remote, _app_message("execution_result", "msg-0003"))
    final = await gateway.wait_for_reply(remote, execution.cursor)
    assert [m["type"] for m in final.messages] == ["final_answer"]
    assert final.cursor == "mock-msg-0003"

    await gateway.close_conversation(remote)
    assert app.state.engine.closed == [remote]
    assert [m["message_id"] for _, m in app.state.engine.received] == [
        "msg-0001",
        "msg-0002",
        "msg-0003",
    ]
    assert (await gateway.get_messages(remote, final.cursor)).messages == []


async def given_gzip_gateway_when_posting_to_mock_then_body_decoded_server_side() -> None:
    app = create_mock_app(_scenario(Step(respond=[_final_answer()])))
    gateway = _gateway(app, gzip=True)
    remote = await gateway.init_conversation("i", {})
    await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    assert app.state.engine.received[0][1] == _app_message("user_request", "msg-0001")


async def given_post_repeated_with_same_message_id_through_gateway_then_same_ack_and_no_duplicate() -> (
    None
):
    app = create_mock_app(default_java_debug_scenario())
    gateway = _gateway(app)
    remote = await gateway.init_conversation("i", {})
    first = await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    second = await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    assert first == second
    assert len(app.state.engine.received) == 1
    reply = await gateway.get_messages(remote, None)
    assert len(reply.messages) == 1


async def given_503_fault_on_post_when_posted_twice_through_gateway_then_network_error_then_success() -> (
    None
):
    fault = Fault(status=503, body={"error": "unavailable"}, times=1, on_operation="post")
    scenario = _scenario(Step(on="user_request", respond=[_plan()], fault=fault))
    gateway = _gateway(create_mock_app(scenario))
    remote = await gateway.init_conversation("i", {})

    error = await _error_of(gateway.post_message(remote, _app_message("user_request", "msg-0001")))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == "HTTP_503" and error.error.retryable is True
    assert error.error.details["operation"] == "POST" and error.error.details["http_status"] == 503

    ack = await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    assert ack.accepted
    assert [m["type"] for m in (await gateway.wait_for_reply(remote, None)).messages] == [
        "discovery_plan"
    ]


async def given_context_window_fault_when_posted_through_gateway_then_model_context_window_error() -> (
    None
):
    fault = Fault(status=400, body={"error": "context_window_exceeded"}, on_operation="post")
    gateway = _gateway(create_mock_app(_scenario(Step(respond=[_plan()], fault=fault))))
    remote = await gateway.init_conversation("i", {})
    error = await _error_of(
        gateway.post_message(remote, _app_message("execution_result", "msg-0002"))
    )
    assert error.error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR
    assert error.error.error_code == "CONTEXT_WINDOW_EXCEEDED"
    assert error.error.retryable is False and error.error.recoverable is True


async def given_413_fault_on_get_when_polling_through_gateway_then_model_context_window_error() -> (
    None
):
    fault = Fault(status=413, on_operation="get")
    gateway = _gateway(create_mock_app(_scenario(Step(respond=[_plan()], fault=fault))))
    remote = await gateway.init_conversation("i", {})
    await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    error = await _error_of(gateway.wait_for_reply(remote, None))
    assert error.error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR
    assert error.error.error_code == "HTTP_413" and error.error.details["operation"] == "GET"


async def given_empty_user_id_when_init_through_gateway_then_400_maps_to_system_error_not_retryable() -> (
    None
):
    gateway = _gateway(create_mock_app(_scenario()), user_id="")
    error = await _error_of(gateway.init_conversation("i", {}))
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.error_code == "HTTP_400" and error.error.retryable is False
    assert error.error.details["operation"] == "INIT"


async def given_wrong_token_when_init_through_gateway_then_authn_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "wrong")
    gateway = _gateway(create_mock_app(_scenario(token="secret")))
    error = await _error_of(gateway.init_conversation("i", {}))
    assert error.error.error_type is ErrorType.AUTHN_ERROR and error.error.error_code == "HTTP_401"


async def given_right_token_when_init_through_gateway_then_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "secret")
    gateway = _gateway(create_mock_app(_scenario(token="secret")))
    assert await gateway.init_conversation("i", {}) == "mock-conv-0001"


async def given_mismatching_message_type_when_posted_through_gateway_then_409_maps_to_system_error() -> (
    None
):
    gateway = _gateway(create_mock_app(_scenario(Step(on="user_request", respond=[_plan()]))))
    remote = await gateway.init_conversation("i", {})
    error = await _error_of(gateway.post_message(remote, _app_message("execution_result", "m1")))
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.error_code == "HTTP_409" and error.error.retryable is False


async def given_silent_model_when_wait_for_reply_through_gateway_then_model_get_timeout() -> None:
    clock = FakeClock()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(int(round(seconds * 1000)))

    gateway = _gateway(
        create_mock_app(_scenario()),
        clock=clock,
        sleep=fake_sleep,
        poll_interval_ms=1_000,
        reply_timeout_ms=3_000,
    )
    remote = await gateway.init_conversation("i", {})
    await gateway.post_message(remote, _app_message("user_request", "m1"))  # no step: silence
    error = await _error_of(gateway.wait_for_reply(remote, None))
    assert error.error.error_code == "MODEL_GET_TIMEOUT" and sleeps == [1.0, 1.0, 1.0]


async def given_delayed_reply_when_wait_for_reply_through_gateway_then_polls_until_available() -> (
    None
):
    clock = FakeClock()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(int(round(seconds * 1000)))

    app = create_mock_app(
        _scenario(Step(respond=[_plan()], delay_ms=1_500)), now_ms=clock.monotonic_ms
    )
    gateway = _gateway(
        app, clock=clock, sleep=fake_sleep, poll_interval_ms=1_000, reply_timeout_ms=10_000
    )
    remote = await gateway.init_conversation("i", {})
    await gateway.post_message(remote, _app_message("user_request", "m1"))
    reply = await gateway.wait_for_reply(remote, None)
    assert [m["type"] for m in reply.messages] == ["discovery_plan"]
    assert sleeps == [1.0, 1.0] and clock.monotonic_ms() == 2_000
