"""Phase 7 — TransportGateway: HTTP implementation over ``httpx.MockTransport`` and the fake.

No real network: every HTTP exchange goes through an in-process ``httpx.MockTransport`` handler,
time comes from ``FakeClock`` and the polling ``sleep`` is injected (it advances the fake clock
instead of waiting).
"""

from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import httpx
import pytest

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ErrorType, TransportError
from agentic_local_app.transport.fake import FakeTransportGateway
from agentic_local_app.transport.gateway import (
    OP_CLOSE,
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    HttpTransportGateway,
    PostAck,
    TransportGateway,
)

pytestmark = pytest.mark.phase7

RID = "remote-conv-1"
TOKEN_ENV = "PHASE7_TEST_TRANSPORT_TOKEN"
BASE = "http://model.test/v1/conversations"


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def _section(**overrides: Any) -> TransportSection:
    values: dict[str, Any] = {
        "init_url": BASE,
        "post_url": BASE + "/{conversation_id}/messages",
        "get_url": BASE + "/{conversation_id}/messages?after={after}",
        "close_url": "",
        "token_env": TOKEN_ENV,
        "user_id": "tester",
        "request_timeout_ms": 1_000,
        "poll_interval_ms": 1_000,
        "reply_timeout_ms": 3_000,
        "gzip": False,
        "verify_tls": True,
    }
    values.update(overrides)
    return TransportSection(**values)


Scripted = httpx.Response | Exception | Callable[[httpx.Request], httpx.Response]


class ScriptedServer:
    """A MockTransport handler replaying scripted responses in order (the last one repeats)."""

    def __init__(self, *responses: Scripted) -> None:
        self._responses = list(responses)
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self._responses) - 1)
        scripted = self._responses[index]
        if isinstance(scripted, Exception):
            raise scripted
        if isinstance(scripted, httpx.Response):
            return scripted
        return scripted(request)

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


def _gateway(
    server: Callable[[httpx.Request], Awaitable[httpx.Response]],
    clock: FakeClock | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    **overrides: Any,
) -> HttpTransportGateway:
    kwargs: dict[str, Any] = {"transport": httpx.MockTransport(server)}
    if sleep is not None:
        kwargs["sleep"] = sleep
    return HttpTransportGateway(_section(**overrides), clock or FakeClock(), **kwargs)


def _init_ok(conversation_id: str = "mock-conv-1", status: int = 201) -> httpx.Response:
    return httpx.Response(status, json={"conversation_id": conversation_id})


def _post_ok(message_id: str = "msg-0001", status: int = 202) -> httpx.Response:
    return httpx.Response(status, json={"accepted": True, "message_id": message_id})


def _get_ok(messages: list[dict[str, Any]], cursor: str | None = "auto") -> httpx.Response:
    if cursor == "auto":
        cursor = messages[-1]["message_id"] if messages else None
    return httpx.Response(200, json={"messages": messages, "cursor": cursor})


def _model_message(message_id: str = "mock-msg-0001") -> dict[str, Any]:
    return {
        "type": "final_answer",
        "conversation_id": RID,
        "message_id": message_id,
        "content": {"status": "completed", "diagnosis": "ok", "evidence": []},
    }


def _payload(message_id: str = "msg-0001") -> dict[str, Any]:
    return {
        "type": "user_request",
        "conversation_id": "conv-0001",
        "message_id": message_id,
        "content": {"goal": "g", "user_message": "m"},
    }


def _sleeper(clock: FakeClock) -> tuple[Callable[[float], Awaitable[None]], list[float]]:
    calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        calls.append(seconds)
        clock.advance(int(round(seconds * 1000)))

    return fake_sleep, calls


async def _error_of(coro: Awaitable[Any]) -> TransportError:
    with pytest.raises(TransportError) as exc:
        await coro
    return exc.value


# ------------------------------------------------------------------------------------------------
# value objects
# ------------------------------------------------------------------------------------------------
def given_post_ack_when_built_then_fields_exposed_and_frozen() -> None:
    ack = PostAck(message_id="m", accepted=True, http_status=202)
    assert (ack.message_id, ack.accepted, ack.http_status) == ("m", True, 202)
    with pytest.raises((AttributeError, TypeError)):
        ack.accepted = False  # type: ignore[misc]


def given_get_result_when_built_then_fields_exposed() -> None:
    result = GetResult(messages=[_model_message()], cursor="mock-msg-0001", http_status=200)
    assert result.messages[0]["type"] == "final_answer"
    assert result.cursor == "mock-msg-0001" and result.http_status == 200


def given_http_gateway_when_checked_then_is_a_transport_gateway() -> None:
    assert issubclass(HttpTransportGateway, TransportGateway)
    assert issubclass(FakeTransportGateway, TransportGateway)


# ------------------------------------------------------------------------------------------------
# init
# ------------------------------------------------------------------------------------------------
async def given_init_endpoint_when_init_conversation_then_body_headers_and_remote_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    server = ScriptedServer(_init_ok("mock-conv-7"))
    gateway = _gateway(server)

    remote = await gateway.init_conversation("PROTOCOL", {"session_id": "sess-0001"})

    assert remote == "mock-conv-7"
    request = server.last
    assert request.method == "POST" and str(request.url) == BASE
    assert json.loads(request.content) == {
        "user_id": "tester",
        "instructions": "PROTOCOL",
        "metadata": {"session_id": "sess-0001"},
    }
    assert request.headers["X-User-Id"] == "tester"
    assert request.headers["Accept"] == "application/json"
    assert request.headers["Content-Type"] == "application/json"
    assert "Authorization" not in request.headers
    assert "Content-Encoding" not in request.headers


async def given_token_in_environment_when_request_sent_then_bearer_header_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "secret-token")
    server = ScriptedServer(_init_ok())
    await _gateway(server).init_conversation("i", {})
    assert server.last.headers["Authorization"] == "Bearer secret-token"


async def given_init_returning_200_when_init_conversation_then_accepted() -> None:
    server = ScriptedServer(_init_ok("c", status=200))
    assert await _gateway(server).init_conversation("i", {}) == "c"


async def given_empty_user_id_when_request_sent_then_no_user_id_header() -> None:
    server = ScriptedServer(_init_ok())
    await _gateway(server, user_id="").init_conversation("i", {})
    assert "X-User-Id" not in server.last.headers


# ------------------------------------------------------------------------------------------------
# post
# ------------------------------------------------------------------------------------------------
async def given_gzip_enabled_when_post_message_then_body_gzip_encoded_and_decodable() -> None:
    server = ScriptedServer(_post_ok())
    await _gateway(server, gzip=True).post_message(RID, _payload())
    request = server.last
    assert request.headers["Content-Encoding"] == "gzip"
    assert request.headers["Content-Type"] == "application/json"
    assert gzip.decompress(request.content).decode() == canonical_json(_payload())


async def given_gzip_disabled_when_post_message_then_plain_canonical_json_body() -> None:
    server = ScriptedServer(_post_ok())
    await _gateway(server, gzip=False).post_message(RID, _payload())
    assert "Content-Encoding" not in server.last.headers
    assert server.last.content.decode() == canonical_json(_payload())


async def given_post_url_template_when_post_message_then_conversation_id_quoted_in_url() -> None:
    server = ScriptedServer(_post_ok())
    await _gateway(server).post_message("conv/with space", _payload())
    assert str(server.last.url) == BASE + "/conv%2Fwith%20space/messages"
    assert server.last.method == "POST"


async def given_post_accepted_when_post_message_then_ack_with_message_id_and_status() -> None:
    server = ScriptedServer(_post_ok("msg-0001", status=202))
    ack = await _gateway(server).post_message(RID, _payload("msg-0001"))
    assert ack == PostAck(message_id="msg-0001", accepted=True, http_status=202)


async def given_post_returning_200_when_post_message_then_accepted() -> None:
    server = ScriptedServer(_post_ok("msg-0001", status=200))
    ack = await _gateway(server).post_message(RID, _payload())
    assert ack.accepted and ack.http_status == 200


async def given_post_not_accepted_when_post_message_then_model_protocol_error() -> None:
    server = ScriptedServer(
        httpx.Response(202, json={"accepted": False, "message_id": "msg-0001", "reason": "dup"})
    )
    error = await _error_of(_gateway(server).post_message(RID, _payload()))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "POST_NOT_ACCEPTED"
    assert error.error.retryable is False
    assert error.error.details["operation"] == OP_POST
    assert error.error.details["http_status"] == 202


# ------------------------------------------------------------------------------------------------
# get
# ------------------------------------------------------------------------------------------------
async def given_get_url_template_when_get_messages_then_cursor_quoted_in_url() -> None:
    server = ScriptedServer(_get_ok([]))
    await _gateway(server).get_messages("conv/1", "msg 1/2")
    assert str(server.last.url) == BASE + "/conv%2F1/messages?after=msg%201%2F2"
    assert server.last.method == "GET"


async def given_no_cursor_when_get_messages_then_after_parameter_empty() -> None:
    server = ScriptedServer(_get_ok([]))
    await _gateway(server).get_messages(RID, None)
    assert str(server.last.url) == BASE + f"/{RID}/messages?after="


async def given_get_request_when_sent_then_accept_and_user_id_headers_without_body_headers() -> (
    None
):
    server = ScriptedServer(_get_ok([]))
    await _gateway(server).get_messages(RID, None)
    headers = server.last.headers
    assert headers["Accept"] == "application/json" and headers["X-User-Id"] == "tester"
    assert "Content-Type" not in headers and "Content-Encoding" not in headers


async def given_get_returning_messages_when_get_messages_then_result_with_cursor_and_status() -> (
    None
):
    messages = [_model_message("mock-msg-0001"), _model_message("mock-msg-0002")]
    server = ScriptedServer(_get_ok(messages))
    result = await _gateway(server).get_messages(RID, None)
    assert result == GetResult(messages=messages, cursor="mock-msg-0002", http_status=200)


async def given_get_body_without_cursor_when_get_messages_then_cursor_derived_from_last_message() -> (
    None
):
    server = ScriptedServer(httpx.Response(200, json={"messages": [_model_message("m-9")]}))
    result = await _gateway(server).get_messages(RID, None)
    assert result.cursor == "m-9"


async def given_empty_get_body_when_get_messages_then_empty_result_and_null_cursor() -> None:
    server = ScriptedServer(_get_ok([], cursor=None))
    result = await _gateway(server).get_messages(RID, "prev")
    assert result.messages == [] and result.cursor is None


# ------------------------------------------------------------------------------------------------
# close
# ------------------------------------------------------------------------------------------------
async def given_empty_close_url_when_close_conversation_then_no_request_sent() -> None:
    server = ScriptedServer(httpx.Response(200, json={}))
    await _gateway(server, close_url="").close_conversation(RID)
    assert server.requests == []


async def given_close_url_when_close_conversation_then_post_sent_to_formatted_url() -> None:
    server = ScriptedServer(httpx.Response(200, json={"closed": True}))
    gateway = _gateway(server, close_url=BASE + "/{conversation_id}/close")
    await gateway.close_conversation("c 1")
    assert server.last.method == "POST" and str(server.last.url) == BASE + "/c%201/close"


async def given_close_url_returning_error_when_close_conversation_then_mapped_with_close_operation() -> (
    None
):
    server = ScriptedServer(httpx.Response(500, text="boom"))
    gateway = _gateway(server, close_url=BASE + "/{conversation_id}/close")
    error = await _error_of(gateway.close_conversation(RID))
    assert error.error.details["operation"] == OP_CLOSE
    assert error.error.error_type is ErrorType.SYSTEM_ERROR


# ------------------------------------------------------------------------------------------------
# HTTP -> ErrorType mapping (ADR-004): one test per line
# ------------------------------------------------------------------------------------------------
async def given_401_when_request_sent_then_authn_error_not_retryable() -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(401))).init_conversation("i", {})
    )
    assert error.error.error_type is ErrorType.AUTHN_ERROR
    assert error.error.error_code == "HTTP_401"
    assert error.error.retryable is False
    assert error.error.details["http_status"] == 401


async def given_403_when_request_sent_then_authz_error_not_retryable() -> None:
    error = await _error_of(_gateway(ScriptedServer(httpx.Response(403))).post_message(RID, {}))
    assert error.error.error_type is ErrorType.AUTHZ_ERROR
    assert error.error.error_code == "HTTP_403"
    assert error.error.retryable is False


async def given_429_with_retry_after_seconds_when_request_sent_then_rate_limit_with_retry_after_ms() -> (
    None
):
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": "7"}))
    error = await _error_of(_gateway(server).post_message(RID, {}))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert error.error.error_code == "HTTP_429"
    assert error.error.retryable is True
    assert error.error.details["retry_after_ms"] == 7_000


async def given_429_without_retry_after_when_request_sent_then_no_retry_after_detail() -> None:
    error = await _error_of(_gateway(ScriptedServer(httpx.Response(429))).get_messages(RID, None))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert "retry_after_ms" not in error.error.details


async def given_429_with_http_date_retry_after_when_request_sent_then_delay_computed_from_clock() -> (
    None
):
    clock = FakeClock()
    retry_at = clock.now() + timedelta(seconds=30)
    http_date = retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": http_date}))
    error = await _error_of(_gateway(server, clock=clock).get_messages(RID, None))
    assert error.error.details["retry_after_ms"] == 30_000


async def given_429_with_unparseable_retry_after_when_request_sent_then_detail_omitted() -> None:
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": "soon"}))
    error = await _error_of(_gateway(server).get_messages(RID, None))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert "retry_after_ms" not in error.error.details


@pytest.mark.parametrize("status", [408, 504])
async def given_timeout_status_when_request_sent_then_timeout_error_retryable(status: int) -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(status))).get_messages(RID, None)
    )
    assert error.error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.error.error_code == f"HTTP_{status}"
    assert error.error.retryable is True


@pytest.mark.parametrize("exc_type", [httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout])
async def given_httpx_timeout_when_request_sent_then_timeout_error_retryable(
    exc_type: type[httpx.TimeoutException],
) -> None:
    error = await _error_of(_gateway(ScriptedServer(exc_type("slow"))).get_messages(RID, None))
    assert error.error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.error.error_code == "REQUEST_TIMEOUT"
    assert error.error.retryable is True
    assert error.error.details["http_status"] is None
    assert error.error.details["operation"] == OP_GET


async def given_413_when_request_sent_then_context_window_error_not_retryable_but_recoverable() -> (
    None
):
    error = await _error_of(_gateway(ScriptedServer(httpx.Response(413))).post_message(RID, {}))
    assert error.error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR
    assert error.error.error_code == "HTTP_413"
    assert error.error.retryable is False
    assert error.error.recoverable is True


@pytest.mark.parametrize("status", [400, 409, 422, 429, 403])
async def given_4xx_with_context_window_body_when_request_sent_then_context_window_error(
    status: int,
) -> None:
    server = ScriptedServer(httpx.Response(status, json={"error": "context_window_exceeded"}))
    error = await _error_of(_gateway(server).post_message(RID, {}))
    assert error.error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR
    assert error.error.error_code == "CONTEXT_WINDOW_EXCEEDED"
    assert error.error.retryable is False and error.error.recoverable is True
    assert error.error.details["http_status"] == status


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("refused"),
        httpx.ReadError("reset"),
        httpx.WriteError("broken pipe"),
        httpx.RemoteProtocolError("server disconnected"),
    ],
)
async def given_connection_failure_when_request_sent_then_network_error_retryable(
    exc: Exception,
) -> None:
    error = await _error_of(_gateway(ScriptedServer(exc)).post_message(RID, {}))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == "CONNECTION_ERROR"
    assert error.error.retryable is True
    assert error.error.details["http_status"] is None
    assert error.error.details["cause"] == type(exc).__name__


@pytest.mark.parametrize("status", [502, 503])
async def given_bad_gateway_status_when_request_sent_then_network_error_retryable(
    status: int,
) -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(status))).get_messages(RID, None)
    )
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == f"HTTP_{status}"
    assert error.error.retryable is True


@pytest.mark.parametrize("status", [500, 501, 505, 599])
async def given_other_5xx_when_request_sent_then_transient_system_error_retryable(
    status: int,
) -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(status))).init_conversation("i", {})
    )
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.error_code == f"HTTP_{status}"
    assert error.error.retryable is True
    assert error.error.details["transient"] is True


async def given_non_json_body_on_success_status_when_request_sent_then_invalid_response_body() -> (
    None
):
    server = ScriptedServer(httpx.Response(200, text="<html>not json</html>"))
    error = await _error_of(_gateway(server).init_conversation("i", {}))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.retryable is False
    assert error.error.details["http_status"] == 200


@pytest.mark.parametrize(
    ("operation", "body"),
    [
        ("init", {"id": "x"}),
        ("init", {"conversation_id": 42}),
        ("init", []),
        ("post", {"accepted": True}),
        ("post", {"accepted": "yes", "message_id": "m"}),
        ("get", {"messages": "nope", "cursor": None}),
        ("get", {"cursor": None}),
        ("get", {"messages": ["not-a-dict"], "cursor": None}),
        ("get", {"messages": [], "cursor": 12}),
    ],
)
async def given_unexpected_json_shape_when_request_sent_then_invalid_response_body(
    operation: str, body: Any
) -> None:
    gateway = _gateway(ScriptedServer(httpx.Response(200, json=body)))
    calls: dict[str, Callable[[], Awaitable[Any]]] = {
        "init": lambda: gateway.init_conversation("i", {}),
        "post": lambda: gateway.post_message(RID, _payload()),
        "get": lambda: gateway.get_messages(RID, None),
    }
    error = await _error_of(calls[operation]())
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert "reason" in error.error.details


@pytest.mark.parametrize("status", [400, 404, 409, 422])
async def given_other_4xx_when_request_sent_then_system_error_not_retryable(status: int) -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(status, json={"detail": "x"}))).post_message(RID, {})
    )
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.error_code == f"HTTP_{status}"
    assert error.error.retryable is False
    assert error.error.details.get("transient", False) is False


@pytest.mark.parametrize("status", [204, 302, 101])
async def given_unexpected_non_error_status_when_request_sent_then_model_protocol_error(
    status: int,
) -> None:
    error = await _error_of(
        _gateway(ScriptedServer(httpx.Response(status))).init_conversation("i", {})
    )
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "UNEXPECTED_STATUS"
    assert error.error.details["http_status"] == status


@pytest.mark.parametrize("operation", [OP_INIT, OP_POST, OP_GET, OP_CLOSE])
async def given_failing_operation_when_error_raised_then_details_carry_operation_status_and_url(
    operation: str,
) -> None:
    server = ScriptedServer(httpx.Response(500))
    gateway = _gateway(server, close_url=BASE + "/{conversation_id}/close")
    calls: dict[str, Callable[[], Awaitable[Any]]] = {
        OP_INIT: lambda: gateway.init_conversation("i", {}),
        OP_POST: lambda: gateway.post_message(RID, _payload()),
        OP_GET: lambda: gateway.get_messages(RID, "c"),
        OP_CLOSE: lambda: gateway.close_conversation(RID),
    }
    error = await _error_of(calls[operation]())
    details = error.error.details
    assert details["operation"] == operation
    assert details["http_status"] == 500
    assert details["url"] == str(server.last.url)
    assert error.error.origin == "TransportGateway"


async def given_token_appearing_in_url_when_error_raised_then_url_detail_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "sekrit")
    server = ScriptedServer(httpx.Response(500))
    gateway = _gateway(server, init_url=BASE + "?key=sekrit")
    error = await _error_of(gateway.init_conversation("i", {}))
    assert "sekrit" not in error.error.details["url"]
    assert "***" in error.error.details["url"]


# ------------------------------------------------------------------------------------------------
# wait_for_reply: polling
# ------------------------------------------------------------------------------------------------
async def given_empty_gets_when_wait_for_reply_then_polls_every_interval_until_a_message() -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(_get_ok([]), _get_ok([]), _get_ok([_model_message()]))
    gateway = _gateway(
        server, clock=clock, sleep=sleep, poll_interval_ms=1_000, reply_timeout_ms=10_000
    )

    result = await gateway.wait_for_reply(RID, "msg-0001")

    assert [m["message_id"] for m in result.messages] == ["mock-msg-0001"]
    assert result.cursor == "mock-msg-0001"
    assert len(server.requests) == 3
    assert all(str(r.url).endswith("after=msg-0001") for r in server.requests)
    assert sleeps == [1.0, 1.0]
    assert clock.monotonic_ms() == 2_000


async def given_first_get_has_messages_when_wait_for_reply_then_no_sleep() -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(_get_ok([_model_message()]))
    result = await _gateway(server, clock=clock, sleep=sleep).wait_for_reply(RID, None)
    assert len(result.messages) == 1 and sleeps == [] and len(server.requests) == 1


async def given_no_reply_when_wait_for_reply_then_model_get_timeout_after_reply_timeout() -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(_get_ok([]))
    gateway = _gateway(
        server, clock=clock, sleep=sleep, poll_interval_ms=1_000, reply_timeout_ms=2_500
    )

    error = await _error_of(gateway.wait_for_reply(RID, None))

    assert error.error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.error.error_code == "MODEL_GET_TIMEOUT"
    assert error.error.retryable is True
    assert error.error.details["operation"] == OP_GET
    assert error.error.details["timeout_ms"] == 2_500
    # GET at t=0, 1000, 2000 and a last one exactly at the deadline (2500)
    assert sleeps == [1.0, 1.0, 0.5]
    assert len(server.requests) == 4
    assert clock.monotonic_ms() == 2_500


async def given_get_error_during_polling_when_wait_for_reply_then_error_propagates_immediately() -> (
    None
):
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(_get_ok([]), httpx.Response(503))
    error = await _error_of(_gateway(server, clock=clock, sleep=sleep).wait_for_reply(RID, None))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == "HTTP_503"
    assert sleeps == [1.0] and len(server.requests) == 2


async def given_reply_timeout_shorter_than_poll_interval_when_wait_for_reply_then_single_short_sleep() -> (
    None
):
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(_get_ok([]))
    gateway = _gateway(
        server, clock=clock, sleep=sleep, poll_interval_ms=5_000, reply_timeout_ms=1_200
    )
    await _error_of(gateway.wait_for_reply(RID, None))
    assert sleeps == [1.2] and len(server.requests) == 2


# ------------------------------------------------------------------------------------------------
# abandon()
# ------------------------------------------------------------------------------------------------
class _Hanging:
    """A handler that blocks until released; ``entered`` is set once ``expected`` requests are in."""

    def __init__(self, expected: int = 1) -> None:
        self.expected = expected
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.requests: list[httpx.Request] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if len(self.requests) >= self.expected:
            self.entered.set()
        await self.release.wait()
        return _get_ok([_model_message()])


async def given_in_flight_get_when_abandon_then_call_raises_interrupted_abandoned() -> None:
    hanging = _Hanging()
    gateway = _gateway(hanging)
    task = asyncio.create_task(gateway.get_messages(RID, None))
    await hanging.entered.wait()

    gateway.abandon()

    error = await _error_of(task)
    assert error.error.error_type is ErrorType.INTERRUPTED
    assert error.error.error_code == "ABANDONED"
    assert error.error.retryable is False
    assert error.error.details["operation"] == OP_GET
    assert gateway.in_flight == 0


async def given_abandoned_gateway_when_called_again_then_works_normally() -> None:
    hanging = _Hanging()
    gateway = _gateway(hanging)
    task = asyncio.create_task(gateway.post_message(RID, _payload()))
    await hanging.entered.wait()
    gateway.abandon()
    await _error_of(task)

    hanging.release.set()
    result = await gateway.get_messages(RID, None)
    assert len(result.messages) == 1
    assert len(hanging.requests) == 2


async def given_nothing_in_flight_when_abandon_then_no_error_and_next_call_works() -> None:
    server = ScriptedServer(_init_ok())
    gateway = _gateway(server)
    gateway.abandon()
    gateway.abandon()
    assert await gateway.init_conversation("i", {}) == "mock-conv-1"


async def given_wait_for_reply_sleeping_when_abandon_then_abandoned() -> None:
    clock = FakeClock()
    sleeping = asyncio.Event()
    never = asyncio.Event()

    async def blocking_sleep(seconds: float) -> None:
        sleeping.set()
        await never.wait()

    server = ScriptedServer(_get_ok([]))
    gateway = _gateway(server, clock=clock, sleep=blocking_sleep)
    task = asyncio.create_task(gateway.wait_for_reply(RID, None))
    await sleeping.wait()

    gateway.abandon()

    error = await _error_of(task)
    assert error.error.error_code == "ABANDONED" and error.error.details["operation"] == OP_GET


async def given_two_in_flight_calls_when_abandon_then_both_abandoned() -> None:
    hanging = _Hanging(expected=2)
    gateway = _gateway(hanging)
    first = asyncio.create_task(gateway.get_messages(RID, None))
    second = asyncio.create_task(gateway.post_message(RID, _payload()))
    await hanging.entered.wait()
    assert len(hanging.requests) == 2 and gateway.in_flight == 2

    gateway.abandon()

    errors = [await _error_of(first), await _error_of(second)]
    assert {e.error.details["operation"] for e in errors} == {OP_GET, OP_POST}
    assert all(e.error.error_code == "ABANDONED" for e in errors)


async def given_outer_task_cancelled_when_awaiting_gateway_then_cancelled_error_not_transport_error() -> (
    None
):
    hanging = _Hanging()
    gateway = _gateway(hanging)
    task = asyncio.create_task(gateway.get_messages(RID, None))
    await hanging.entered.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert gateway.in_flight == 0


async def given_completed_call_when_abandon_then_result_unaffected() -> None:
    server = ScriptedServer(_get_ok([_model_message()]))
    gateway = _gateway(server)
    result = await gateway.get_messages(RID, None)
    gateway.abandon()
    assert len(result.messages) == 1


async def given_gateway_when_aclose_then_client_closed_and_reusable_gateway_not_required() -> None:
    server = ScriptedServer(_get_ok([]))
    gateway = _gateway(server)
    await gateway.get_messages(RID, None)
    await gateway.aclose()
    assert gateway.closed is True


# ------------------------------------------------------------------------------------------------
# FakeTransportGateway
# ------------------------------------------------------------------------------------------------
async def given_fake_gateway_when_init_twice_then_sequential_remote_ids_and_inits_recorded() -> (
    None
):
    fake = FakeTransportGateway()
    first = await fake.init_conversation("INSTRUCTIONS", {"session_id": "sess-0001"})
    second = await fake.init_conversation("INSTRUCTIONS", {"session_id": "sess-0001", "p": "c"})
    assert (first, second) == ("remote-0001", "remote-0002")
    assert fake.inits == [
        {"instructions": "INSTRUCTIONS", "metadata": {"session_id": "sess-0001"}},
        {"instructions": "INSTRUCTIONS", "metadata": {"session_id": "sess-0001", "p": "c"}},
    ]


async def given_fake_gateway_when_post_then_ack_with_payload_message_id_and_posted_recorded() -> (
    None
):
    fake = FakeTransportGateway()
    ack = await fake.post_message("remote-0001", _payload("msg-0007"))
    assert ack == PostAck(message_id="msg-0007", accepted=True, http_status=202)
    assert fake.posted == [("remote-0001", _payload("msg-0007"))]


async def given_enqueued_messages_when_get_messages_then_one_entry_consumed_and_cursor_is_last_id() -> (
    None
):
    fake = FakeTransportGateway()
    batch = [_model_message("mock-msg-0001"), _model_message("mock-msg-0002")]
    fake.enqueue_messages("remote-0001", batch)
    result = await fake.get_messages("remote-0001", None)
    assert result == GetResult(messages=batch, cursor="mock-msg-0002", http_status=200)
    assert fake.get_calls == [("remote-0001", None)]
    assert fake.pending("remote-0001") == 0


async def given_two_enqueued_entries_when_get_twice_then_consumed_in_order() -> None:
    fake = FakeTransportGateway()
    fake.enqueue_messages("remote-0001", [_model_message("a")])
    fake.enqueue_messages("remote-0001", [_model_message("b")])
    first = await fake.get_messages("remote-0001", None)
    second = await fake.wait_for_reply("remote-0001", first.cursor)
    assert [m["message_id"] for m in first.messages] == ["a"]
    assert [m["message_id"] for m in second.messages] == ["b"]
    assert fake.get_calls == [("remote-0001", None), ("remote-0001", "a")]


async def given_messages_for_other_conversation_when_get_then_not_returned() -> None:
    fake = FakeTransportGateway()
    fake.enqueue_messages("remote-0002", [_model_message("x")])
    result = await fake.get_messages("remote-0001", None)
    assert result.messages == [] and fake.pending("remote-0002") == 1


async def given_empty_queue_when_get_messages_then_empty_result_with_after_as_cursor() -> None:
    fake = FakeTransportGateway()
    result = await fake.get_messages("remote-0001", "msg-0003")
    assert result == GetResult(messages=[], cursor="msg-0003", http_status=200)


async def given_empty_queue_when_wait_for_reply_then_model_get_timeout() -> None:
    fake = FakeTransportGateway()
    error = await _error_of(fake.wait_for_reply("remote-0001", None))
    assert error.error.error_type is ErrorType.TIMEOUT_ERROR
    assert error.error.error_code == "MODEL_GET_TIMEOUT"
    assert error.error.retryable is True
    assert error.error.details["operation"] == OP_GET
    assert error.error.details["timeout_ms"] == fake.reply_timeout_ms
    assert fake.get_calls == [("remote-0001", None)]


async def given_fake_clock_when_wait_for_reply_times_out_then_clock_advanced_by_reply_timeout() -> (
    None
):
    clock = FakeClock()
    fake = FakeTransportGateway(clock=clock, reply_timeout_ms=4_000)
    await _error_of(fake.wait_for_reply("remote-0001", None))
    assert clock.monotonic_ms() == 4_000


@pytest.mark.parametrize("operation", ["init", "post", "get", "close"])
async def given_enqueued_error_when_operation_called_then_error_raised_then_normal_behavior_resumes(
    operation: str,
) -> None:
    fake = FakeTransportGateway()
    injected = TransportError(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True)
    fake.enqueue_error(operation, injected)
    calls: dict[str, Callable[[], Awaitable[Any]]] = {
        "init": lambda: fake.init_conversation("i", {}),
        "post": lambda: fake.post_message("remote-0001", _payload()),
        "get": lambda: fake.get_messages("remote-0001", None),
        "close": lambda: fake.close_conversation("remote-0001"),
    }
    with pytest.raises(TransportError) as exc:
        await calls[operation]()
    assert exc.value is injected
    await calls[operation]()  # the error queue is drained: normal behaviour again


async def given_enqueued_error_times_two_when_called_three_times_then_two_failures_then_success() -> (
    None
):
    fake = FakeTransportGateway()
    fake.enqueue_error(
        "get", TransportError(ErrorType.TIMEOUT_ERROR, "HTTP_504", retryable=True), times=2
    )
    fake.enqueue_messages("remote-0001", [_model_message()])
    await _error_of(fake.get_messages("remote-0001", None))
    await _error_of(fake.get_messages("remote-0001", None))
    result = await fake.get_messages("remote-0001", None)
    assert len(result.messages) == 1
    assert len(fake.get_calls) == 3


async def given_enqueued_get_error_when_wait_for_reply_then_same_error_raised() -> None:
    fake = FakeTransportGateway()
    injected = TransportError(ErrorType.MODEL_CONTEXT_WINDOW_ERROR, "HTTP_413", retryable=False)
    fake.enqueue_error("get", injected)
    with pytest.raises(TransportError) as exc:
        await fake.wait_for_reply("remote-0001", None)
    assert exc.value is injected


def given_unknown_operation_when_enqueue_error_then_value_error() -> None:
    fake = FakeTransportGateway()
    with pytest.raises(ValueError):
        fake.enqueue_error("fetch", TransportError(ErrorType.NETWORK_ERROR, "X", retryable=True))


async def given_fake_gateway_when_close_then_closed_recorded() -> None:
    fake = FakeTransportGateway()
    await fake.close_conversation("remote-0001")
    assert fake.closed == ["remote-0001"]


async def given_hanging_operation_when_abandon_then_abandoned_and_next_call_works() -> None:
    fake = FakeTransportGateway()
    fake.hang_next("get")
    fake.enqueue_messages("remote-0001", [_model_message()])
    task = asyncio.create_task(fake.wait_for_reply("remote-0001", None))
    await fake.wait_until_hanging()
    assert fake.is_hanging and fake.in_flight == 1

    fake.abandon()

    error = await _error_of(task)
    assert error.error.error_type is ErrorType.INTERRUPTED
    assert error.error.error_code == "ABANDONED"
    assert error.error.retryable is False
    result = await fake.wait_for_reply("remote-0001", None)
    assert len(result.messages) == 1


async def given_hanging_post_when_abandon_then_message_not_recorded_as_posted() -> None:
    fake = FakeTransportGateway()
    fake.hang_next("post")
    task = asyncio.create_task(fake.post_message("remote-0001", _payload()))
    await fake.wait_until_hanging()
    fake.abandon()
    await _error_of(task)
    assert fake.posted == [] and fake.is_hanging is False
