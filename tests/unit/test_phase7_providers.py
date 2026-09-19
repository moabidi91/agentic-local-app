"""Phase 7 — pluggable transport providers (ADR-020).

One abstract contract (``TransportGateway``), several implementations, the choice made by
configuration only: ``HttpProviderBase`` (template method over httpx), ``GenericHttpProvider``
(the ADR-004 contract, formerly ``HttpTransportGateway``), ``TemplatedHttpProvider`` (driven by
``transport.options``), ``TransportRegistry`` (names, entry points, import paths) and the wiring.

No real network: every HTTP exchange goes through ``httpx.MockTransport`` or
``httpx.ASGITransport(create_mock_app(...))``; time is a ``FakeClock`` and the polling ``sleep`` is
injected.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, ClassVar

import httpx
import pytest
from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.config import AppConfig, AppSection, TransportSection, load_config
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import Clock, FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import ConfigError, ErrorType, TransportError
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.orchestration import Application, build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.testing.mock_model_server import (
    Fault,
    Scenario,
    Step,
    create_mock_app,
    default_java_debug_scenario,
)
from agentic_local_app.transport import gateway as gateway_module
from agentic_local_app.transport.base import (
    OP_CLOSE,
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    PostAck,
    TransportGateway,
)
from agentic_local_app.transport.fake import FakeTransportGateway, FakeTransportProvider
from agentic_local_app.transport.http_base import HttpCall, HttpProviderBase, InvalidResponseError
from agentic_local_app.transport.providers.generic_http import (
    GenericHttpProvider,
    HttpTransportGateway,
)
from agentic_local_app.transport.providers.templated_http import (
    TemplatedHttpProvider,
    extract_path,
    parse_path,
)
from agentic_local_app.transport.registry import (
    ENTRY_POINT_GROUP,
    ORIGIN_BUILTIN,
    ORIGIN_ENTRY_POINT,
    ORIGIN_IMPORT_PATH,
    TransportRegistry,
)

pytestmark = pytest.mark.phase7

RID = "remote-conv-1"
TOKEN_ENV = "PHASE7_PROVIDERS_TOKEN"
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


def _generic(
    server: Callable[[httpx.Request], Awaitable[httpx.Response]],
    clock: FakeClock | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    **overrides: Any,
) -> GenericHttpProvider:
    kwargs: dict[str, Any] = {"transport": httpx.MockTransport(server)}
    if sleep is not None:
        kwargs["sleep"] = sleep
    return GenericHttpProvider(_section(**overrides), clock or FakeClock(), **kwargs)


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


def _config_error_of(call: Callable[[], Any]) -> ConfigError:
    with pytest.raises(ConfigError) as exc:
        call()
    return exc.value


# ================================================================================================
# 1. configuration (additive keys of ADR-020)
# ================================================================================================
def given_default_transport_section_when_built_then_provider_generic_http_and_no_options() -> None:
    section = TransportSection()
    assert section.provider == "generic_http"
    assert section.close_method == "POST"
    assert section.options == {}


def given_close_method_outside_post_delete_when_config_loaded_then_config_invalid() -> None:
    error = _config_error_of(
        lambda: load_config(
            None, environ={"AGENTIC__TRANSPORT__CLOSE_METHOD": "PUT"}, load_env_file=False
        )
    )
    assert error.error.error_code == "CONFIG_INVALID"
    assert any("close_method" in str(p.get("loc")) for p in error.error.details["errors"])


def given_provider_and_options_in_toml_when_config_loaded_then_kept_verbatim(tmp_path: Any) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        "\n".join(
            [
                "[transport]",
                'provider = "templated_http"',
                "[transport.options.init]",
                'url = "https://api.example.com/v1/threads"',
                'headers = { "X-Api-Key" = "${env:MY_MODEL_KEY}" }',
                'body = { instructions = "{instructions}" }',
                'conversation_id_path = "data.id"',
                "expected_statuses = [200, 201]",
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_config(path, environ={}, load_env_file=False)
    assert config.transport.provider == "templated_http"
    assert config.transport.options["init"]["conversation_id_path"] == "data.id"
    assert config.transport.options["init"]["expected_statuses"] == [200, 201]
    assert config.transport.options["init"]["headers"] == {"X-Api-Key": "${env:MY_MODEL_KEY}"}


def given_options_as_json_object_in_environment_when_config_loaded_then_parsed() -> None:
    config = load_config(
        None,
        environ={
            "AGENTIC__TRANSPORT__PROVIDER": "fake",
            "AGENTIC__TRANSPORT__OPTIONS": '{"init": {"url": "http://x"}}',
        },
        load_env_file=False,
    )
    assert config.transport.provider == "fake"
    assert config.transport.options == {"init": {"url": "http://x"}}


def given_blank_provider_when_config_loaded_then_config_invalid() -> None:
    error = _config_error_of(
        lambda: load_config(
            None, environ={"AGENTIC__TRANSPORT__PROVIDER": "  "}, load_env_file=False
        )
    )
    assert error.error.error_code == "CONFIG_INVALID"


def given_options_with_secrets_when_masked_then_env_references_and_secret_keys_hidden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MY_MODEL_KEY", "k-secret")
    config = AppConfig(
        transport=TransportSection(
            provider="templated_http",
            options={
                "headers": {"X-Api-Key": "${env:MY_MODEL_KEY}", "Accept": "application/json"},
                "init": {
                    "url": "https://api.example.com/v1/threads?api_token=abc",
                    "headers": {"Authorization": "Bearer {token}"},
                    "body": {"user": "{user_id}", "client_secret": "s3", "nested": {"key": "v"}},
                    "conversation_id_path": "id",
                    "expected_statuses": [200, 201],
                    "tags": ["${env:MY_MODEL_KEY}", "plain"],
                },
            },
        )
    )
    masked = config.masked()["transport"]
    options = masked["options"]
    assert options["headers"] == {"X-Api-Key": "***", "Accept": "application/json"}
    assert options["init"]["headers"] == {"Authorization": "***"}
    assert options["init"]["url"] == "https://api.example.com/v1/threads?api_token=abc"
    assert options["init"]["body"] == {
        "user": "{user_id}",
        "client_secret": "***",
        "nested": {"key": "***"},
    }
    assert options["init"]["conversation_id_path"] == "id"
    assert options["init"]["expected_statuses"] == [200, 201]
    assert options["init"]["tags"] == ["***", "plain"]
    assert masked["provider"] == "templated_http"
    assert masked["token"] is None
    assert "k-secret" not in json.dumps(masked)


def given_no_options_when_masked_then_options_empty_and_token_key_still_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "tok")
    masked = AppConfig(transport=_section()).masked()["transport"]
    assert masked["options"] == {} and masked["token"] == "***"


# ================================================================================================
# 2. HttpCall and the template method base
# ================================================================================================
def given_http_call_when_built_then_fields_exposed_frozen_and_parse_json_defaults_true() -> None:
    call = HttpCall("POST", "http://x", {"A": "b"}, {"k": 1}, frozenset({200}))
    assert (call.method, call.url, call.headers, call.json) == (
        "POST",
        "http://x",
        {"A": "b"},
        {"k": 1},
    )
    assert call.expected_statuses == frozenset({200}) and call.parse_json is True
    with pytest.raises((AttributeError, TypeError)):
        call.method = "GET"  # type: ignore[misc]


def given_http_provider_base_when_checked_then_abstract_transport_gateway() -> None:
    assert issubclass(HttpProviderBase, TransportGateway)
    assert HttpProviderBase.options_model is None
    with pytest.raises(TypeError):
        HttpProviderBase(_section(), FakeClock())  # type: ignore[abstract]


class _TeapotProvider(HttpProviderBase):
    """A minimal provider: custom calls, custom parsing, custom classification for 418."""

    def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
        return HttpCall(
            "PUT",
            "http://teapot.test/threads",
            {**self.headers(OP_INIT), "X-Custom": "1"},
            {"i": instructions, "m": metadata},
            frozenset({200}),
        )

    def parse_init(self, status: int, body: Any) -> str:
        if not isinstance(body, dict) or "thread" not in body:
            raise InvalidResponseError(reason="missing:thread", path="thread")
        return str(body["thread"])

    def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
        return HttpCall(
            "POST",
            f"http://teapot.test/threads/{remote_conversation_id}",
            self.headers(OP_POST),
            payload,
            frozenset({200}),
        )

    def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
        if isinstance(body, dict) and body.get("ok") is False:
            raise InvalidResponseError("POST_NOT_ACCEPTED", reason=body.get("why"))
        return PostAck(message_id=str(payload["message_id"]), accepted=True, http_status=status)

    def build_get(self, remote_conversation_id: str, after: str | None) -> HttpCall:
        return HttpCall(
            "GET",
            f"http://teapot.test/threads/{remote_conversation_id}?since={after or ''}",
            self.headers(OP_GET),
            None,
            frozenset({200}),
        )

    def parse_get(self, status: int, body: Any) -> GetResult:
        return GetResult(messages=list(body), cursor=None, http_status=status)

    def build_close(self, remote_conversation_id: str) -> HttpCall | None:
        return None

    def classify_error(
        self, operation: str, status: int, body: str, headers: Any
    ) -> TransportError:
        if status == 418:
            return TransportError(ErrorType.SYSTEM_ERROR, "TEAPOT", retryable=False, teapot=True)
        return super().classify_error(operation, status, body, headers)


def _teapot(server: ScriptedServer, **overrides: Any) -> _TeapotProvider:
    return _teapot_with(server, FakeClock(), **overrides)


def _teapot_with(server: ScriptedServer, clock: FakeClock, **overrides: Any) -> _TeapotProvider:
    return _TeapotProvider(_section(**overrides), clock, transport=httpx.MockTransport(server))


async def given_custom_build_and_parse_when_init_then_call_sent_as_built_and_parsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "tok")
    server = ScriptedServer(httpx.Response(200, json={"thread": "th-1"}))
    provider = _teapot(server, gzip=False)

    remote = await provider.init_conversation("PROTO", {"session_id": "s"})

    assert remote == "th-1"
    request = server.last
    assert request.method == "PUT" and str(request.url) == "http://teapot.test/threads"
    assert json.loads(request.content) == {"i": "PROTO", "m": {"session_id": "s"}}
    assert request.headers["X-Custom"] == "1"
    assert request.headers["Authorization"] == "Bearer tok"
    assert request.headers["X-User-Id"] == "tester"
    assert request.headers["Accept"] == "application/json"
    assert request.headers["Content-Type"] == "application/json"


async def given_parse_raising_invalid_response_when_init_then_protocol_error_stamped_with_context() -> (
    None
):
    server = ScriptedServer(httpx.Response(200, json={"nope": 1}))
    error = await _error_of(_teapot(server).init_conversation("i", {}))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.retryable is False
    details = error.error.details
    assert details["operation"] == OP_INIT and details["http_status"] == 200
    assert details["url"] == "http://teapot.test/threads"
    assert details["reason"] == "missing:thread" and details["path"] == "thread"


async def given_parse_raising_invalid_response_with_code_when_post_then_that_code_used() -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": False, "why": "dup"}))
    error = await _error_of(_teapot(server).post_message("th-1", _payload("m-7")))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "POST_NOT_ACCEPTED"
    assert error.error.details["reason"] == "dup"
    assert error.error.details["operation"] == OP_POST and error.error.details["http_status"] == 200


async def given_parse_post_when_ack_built_from_payload_then_message_id_of_sent_message() -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": True}))
    ack = await _teapot(server).post_message("th-1", _payload("m-7"))
    assert ack == PostAck(message_id="m-7", accepted=True, http_status=200)
    assert str(server.last.url) == "http://teapot.test/threads/th-1"


async def given_parse_json_false_when_call_sent_then_non_json_body_accepted_and_none_parsed() -> (
    None
):
    seen: list[Any] = []

    class _NoBody(_TeapotProvider):
        def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
            call = super().build_post(remote_conversation_id, payload)
            return HttpCall(
                call.method, call.url, call.headers, call.json, call.expected_statuses, False
            )

        def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
            seen.append(body)
            return super().parse_post(status, body, payload=payload)

    server = ScriptedServer(httpx.Response(200, text="<not json>"))
    provider = _NoBody(_section(), FakeClock(), transport=httpx.MockTransport(server))
    ack = await provider.post_message("th-1", _payload("m-8"))
    assert ack.message_id == "m-8" and seen == [None]


async def given_non_json_body_when_parse_json_true_then_invalid_response_body_not_json() -> None:
    server = ScriptedServer(httpx.Response(200, text="<html>"))
    error = await _error_of(_teapot(server).post_message("th-1", _payload()))
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.details["reason"] == "not_json"
    assert error.error.details["body"] == "<html>"


async def given_custom_classify_error_when_status_matches_then_custom_error_stamped_with_context() -> (
    None
):
    server = ScriptedServer(httpx.Response(418, text="short and stout"))
    error = await _error_of(_teapot(server).get_messages("th-1", None))
    assert error.error.error_code == "TEAPOT" and error.error.error_type is ErrorType.SYSTEM_ERROR
    details = error.error.details
    assert details["teapot"] is True
    assert details["operation"] == OP_GET and details["http_status"] == 418
    assert details["url"] == "http://teapot.test/threads/th-1?since="


async def given_custom_classify_error_when_status_not_matched_then_default_table_applies() -> None:
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": "3"}))
    error = await _error_of(_teapot(server).get_messages("th-1", None))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert error.error.error_code == "HTTP_429" and error.error.details["retry_after_ms"] == 3_000


async def given_build_close_returning_none_when_close_conversation_then_no_request() -> None:
    server = ScriptedServer(httpx.Response(200))
    await _teapot(server).close_conversation("th-1")
    assert server.requests == []


async def given_base_polling_when_wait_for_reply_then_custom_get_used_until_a_message() -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(
        httpx.Response(200, json=[]), httpx.Response(200, json=[_model_message("x")])
    )
    provider = _TeapotProvider(
        _section(poll_interval_ms=500, reply_timeout_ms=5_000),
        clock,
        transport=httpx.MockTransport(server),
        sleep=sleep,
    )
    result = await provider.wait_for_reply("th-1", "m-1")
    assert [m["message_id"] for m in result.messages] == ["x"]
    assert sleeps == [0.5] and len(server.requests) == 2
    assert str(server.requests[0].url) == "http://teapot.test/threads/th-1?since=m-1"


async def given_base_polling_when_no_reply_then_model_get_timeout_with_custom_url() -> None:
    clock = FakeClock()
    sleep, _ = _sleeper(clock)
    server = ScriptedServer(httpx.Response(200, json=[]))
    provider = _TeapotProvider(
        _section(poll_interval_ms=1_000, reply_timeout_ms=1_000),
        clock,
        transport=httpx.MockTransport(server),
        sleep=sleep,
    )
    error = await _error_of(provider.wait_for_reply("th-1", None))
    assert error.error.error_code == "MODEL_GET_TIMEOUT"
    assert error.error.details["url"] == "http://teapot.test/threads/th-1?since="
    assert error.error.details["polls"] == 2


async def given_gzip_enabled_when_custom_call_has_body_then_gzip_applied_by_the_base() -> None:
    import gzip as gzip_module

    server = ScriptedServer(httpx.Response(200, json={"thread": "t"}))
    await _teapot(server, gzip=True).init_conversation("i", {"a": 1})
    request = server.last
    assert request.headers["Content-Encoding"] == "gzip"
    assert gzip_module.decompress(request.content).decode() == canonical_json(
        {"i": "i", "m": {"a": 1}}
    )


async def given_call_without_body_when_sent_then_no_content_type_nor_encoding() -> None:
    server = ScriptedServer(httpx.Response(200, json=[]))
    await _teapot(server, gzip=True).get_messages("th-1", None)
    assert "Content-Type" not in server.last.headers
    assert "Content-Encoding" not in server.last.headers


async def given_call_headers_with_content_type_when_sent_then_provider_value_kept() -> None:
    class _CustomType(_TeapotProvider):
        def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
            call = super().build_init(instructions, metadata)
            return HttpCall(
                call.method,
                call.url,
                {**call.headers, "Content-Type": "application/vnd.api+json"},
                call.json,
                call.expected_statuses,
            )

    server = ScriptedServer(httpx.Response(200, json={"thread": "t"}))
    provider = _CustomType(_section(), FakeClock(), transport=httpx.MockTransport(server))
    await provider.init_conversation("i", {})
    assert server.last.headers["Content-Type"] == "application/vnd.api+json"


async def given_naive_http_date_retry_after_when_429_then_delay_relative_to_utc_clock() -> None:
    clock = FakeClock()
    retry_at = clock.now() + timedelta(seconds=12)
    naive_date = retry_at.strftime("%a, %d %b %Y %H:%M:%S")  # no zone: read as UTC
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": naive_date}))
    error = await _error_of(_teapot_with(server, clock).get_messages("th-1", None))
    assert error.error.details["retry_after_ms"] == 12_000


@pytest.mark.parametrize(
    "exc", [httpx.UnsupportedProtocol("scheme"), httpx.LocalProtocolError("bad request")]
)
async def given_client_side_httpx_error_when_call_sent_then_http_client_error_not_retryable(
    exc: Exception,
) -> None:
    server = ScriptedServer(exc)
    error = await _error_of(_teapot(server).get_messages("th-1", None))
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.error_code == "HTTP_CLIENT_ERROR" and error.error.retryable is False
    assert error.error.details["cause"] == type(exc).__name__
    assert error.error.details["url"] == "http://teapot.test/threads/th-1?since="


async def given_connection_failure_when_custom_call_sent_then_network_error_from_the_base() -> None:
    server = ScriptedServer(httpx.ConnectError("refused"))
    error = await _error_of(_teapot(server).get_messages("th-1", None))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == "CONNECTION_ERROR"
    assert error.error.details["operation"] == OP_GET and error.error.details["http_status"] is None


def given_provider_with_options_model_none_when_built_with_options_then_options_invalid() -> None:
    error = _config_error_of(lambda: _TeapotProvider(_section(options={"x": 1}), FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "_TeapotProvider"
    assert "x" in str(error.error.details["errors"])


# ================================================================================================
# 3. GenericHttpProvider — the ADR-004 contract, unchanged
# ================================================================================================
def given_generic_provider_when_checked_then_http_transport_gateway_is_the_same_class() -> None:
    assert HttpTransportGateway is GenericHttpProvider
    assert gateway_module.HttpTransportGateway is GenericHttpProvider
    assert issubclass(GenericHttpProvider, HttpProviderBase)
    assert GenericHttpProvider.options_model is None


def given_gateway_module_when_imported_then_legacy_public_names_still_exported() -> None:
    for name in (
        "OP_CLOSE",
        "OP_GET",
        "OP_INIT",
        "OP_POST",
        "GetResult",
        "HttpTransportGateway",
        "InFlightGuard",
        "PostAck",
        "TransportGateway",
    ):
        assert name in gateway_module.__all__ and getattr(gateway_module, name) is not None


async def given_init_endpoint_when_generic_init_conversation_then_adr004_body_headers_and_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    server = ScriptedServer(httpx.Response(201, json={"conversation_id": "mock-conv-7"}))
    remote = await _generic(server).init_conversation("PROTOCOL", {"session_id": "sess-0001"})
    assert remote == "mock-conv-7"
    request = server.last
    assert request.method == "POST" and str(request.url) == BASE
    assert json.loads(request.content) == {
        "user_id": "tester",
        "instructions": "PROTOCOL",
        "metadata": {"session_id": "sess-0001"},
    }
    assert request.headers["X-User-Id"] == "tester"
    assert request.headers["Content-Type"] == "application/json"
    assert "Authorization" not in request.headers and "Content-Encoding" not in request.headers


async def given_gzip_enabled_when_generic_post_message_then_canonical_gzip_body_and_ack() -> None:
    import gzip as gzip_module

    server = ScriptedServer(httpx.Response(202, json={"accepted": True, "message_id": "msg-0001"}))
    ack = await _generic(server, gzip=True).post_message("conv/with space", _payload())
    assert ack == PostAck(message_id="msg-0001", accepted=True, http_status=202)
    assert str(server.last.url) == BASE + "/conv%2Fwith%20space/messages"
    assert server.last.headers["Content-Encoding"] == "gzip"
    assert gzip_module.decompress(server.last.content).decode() == canonical_json(_payload())


async def given_empty_gets_when_generic_wait_for_reply_then_polls_until_a_message() -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(
        httpx.Response(200, json={"messages": [], "cursor": None}),
        httpx.Response(200, json={"messages": [_model_message()], "cursor": "mock-msg-0001"}),
    )
    result = await _generic(server, clock=clock, sleep=sleep).wait_for_reply("conv/1", "msg 1")
    assert result.cursor == "mock-msg-0001" and sleeps == [1.0]
    assert str(server.requests[0].url) == BASE + "/conv%2F1/messages?after=msg%201"


async def given_429_when_generic_request_sent_then_rate_limit_error_with_retry_after() -> None:
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": "7"}))
    error = await _error_of(_generic(server).post_message(RID, _payload()))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert error.error.error_code == "HTTP_429" and error.error.details["retry_after_ms"] == 7_000
    assert error.error.details["operation"] == OP_POST and error.error.details["http_status"] == 429


async def given_close_method_delete_when_generic_close_conversation_then_delete_request() -> None:
    server = ScriptedServer(httpx.Response(204))
    gateway = _generic(server, close_url=BASE + "/{conversation_id}", close_method="DELETE")
    await gateway.close_conversation("c 1")
    assert server.last.method == "DELETE" and str(server.last.url) == BASE + "/c%201"
    assert server.last.content == b""


async def given_default_close_method_when_generic_close_conversation_then_post_request() -> None:
    server = ScriptedServer(httpx.Response(200, json={"closed": True}))
    await _generic(server, close_url=BASE + "/{conversation_id}/close").close_conversation(RID)
    assert server.last.method == "POST"


async def given_empty_close_url_when_generic_close_conversation_then_no_request() -> None:
    server = ScriptedServer(httpx.Response(200))
    await _generic(server, close_url="").close_conversation(RID)
    assert server.requests == []


async def given_close_error_when_generic_close_conversation_then_close_operation_in_details() -> (
    None
):
    server = ScriptedServer(httpx.Response(500, text="boom"))
    error = await _error_of(
        _generic(server, close_url=BASE + "/{conversation_id}/x").close_conversation(RID)
    )
    assert error.error.details["operation"] == OP_CLOSE
    assert error.error.details["url"] == BASE + f"/{RID}/x"


def given_generic_provider_when_built_with_options_then_options_invalid() -> None:
    error = _config_error_of(lambda: GenericHttpProvider(_section(options={"a": 1}), FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "GenericHttpProvider"


# ================================================================================================
# 4. TransportRegistry — names, import paths, entry points, errors, options, construction
# ================================================================================================
class RecordingProvider(TransportGateway):
    """A provider living in the test module: selectable by import path without any other change."""

    instances: ClassVar[list[RecordingProvider]] = []

    def __init__(self, config: TransportSection, clock: Clock, **kwargs: Any) -> None:
        self.config = config
        self.clock = clock
        self.kwargs = kwargs
        RecordingProvider.instances.append(self)

    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        return "recorded-1"

    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        return PostAck(message_id=str(payload.get("message_id")), accepted=True, http_status=202)

    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return GetResult(messages=[], cursor=after, http_status=200)

    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return GetResult(messages=[], cursor=after, http_status=200)

    async def close_conversation(self, remote_conversation_id: str) -> None:
        return None

    def abandon(self) -> None:
        return None


class _OptionsOfAcme(BaseModel):
    model_config = ConfigDict(extra="forbid")

    region: str = "eu"
    retries: int = Field(default=1, ge=0)


class AcmeProvider(RecordingProvider):
    """A provider with its own options model (the duck-typed ``options_model`` convention)."""

    options_model: ClassVar[type[BaseModel] | None] = _OptionsOfAcme


class NotAGateway:
    """Something an import path could point to by mistake."""


IMPORT_PATH = f"{__name__}:RecordingProvider"


class _FakeEntryPoint:
    def __init__(self, name: str, value: str, target: Any = None, error: Exception | None = None):
        self.name = name
        self.value = value
        self.group = ENTRY_POINT_GROUP
        self._target = target
        self._error = error
        self.loads = 0

    def load(self) -> Any:
        self.loads += 1
        if self._error is not None:
            raise self._error
        return self._target


def _entry_points(monkeypatch: pytest.MonkeyPatch, *points: _FakeEntryPoint) -> None:
    def fake_entry_points(**kwargs: Any) -> list[_FakeEntryPoint]:
        assert kwargs == {"group": ENTRY_POINT_GROUP}
        return list(points)

    monkeypatch.setattr("importlib.metadata.entry_points", fake_entry_points)


@pytest.fixture
def isolated_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Registrations made by a test do not leak into the process-wide table."""
    monkeypatch.setattr(TransportRegistry, "_registered", TransportRegistry.registered())
    _entry_points(monkeypatch)


def given_transport_package_imported_when_names_listed_then_builtins_present(
    isolated_registry: None,
) -> None:
    assert {"generic_http", "templated_http", "fake"} <= set(TransportRegistry.names())
    assert TransportRegistry.names() == sorted(TransportRegistry.names())


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("generic_http", GenericHttpProvider),
        ("templated_http", TemplatedHttpProvider),
        ("fake", FakeTransportProvider),
    ],
)
def given_builtin_name_when_resolved_then_builtin_class_returned(
    isolated_registry: None, name: str, expected: type[TransportGateway]
) -> None:
    assert TransportRegistry.resolve(name) is expected
    info, provider = TransportRegistry.describe(name)
    assert provider is expected and info.origin == ORIGIN_BUILTIN and info.name == name
    assert info.qualified_name == f"{expected.__module__}:{expected.__qualname__}"


def given_fake_provider_when_checked_then_fake_transport_gateway_subclass() -> None:
    assert issubclass(FakeTransportProvider, FakeTransportGateway)
    assert issubclass(FakeTransportProvider, TransportGateway)


def given_import_path_when_resolved_then_class_imported_without_registration(
    isolated_registry: None,
) -> None:
    assert TransportRegistry.resolve(IMPORT_PATH) is RecordingProvider
    info, _ = TransportRegistry.describe(f"  {IMPORT_PATH} ")
    assert info.origin == ORIGIN_IMPORT_PATH and info.qualified_name == IMPORT_PATH
    assert IMPORT_PATH not in TransportRegistry.names()


def given_entry_point_when_resolved_then_loaded_lazily_and_listed_with_its_origin(
    monkeypatch: pytest.MonkeyPatch, isolated_registry: None
) -> None:
    point = _FakeEntryPoint("acme_http", "acme.transport:AcmeProvider", target=AcmeProvider)
    _entry_points(monkeypatch, point)

    assert "acme_http" in TransportRegistry.names()
    listed = {info.name: info for info in TransportRegistry.list_providers()}
    assert listed["acme_http"].origin == ORIGIN_ENTRY_POINT
    assert listed["acme_http"].qualified_name == "acme.transport:AcmeProvider"
    assert point.loads == 0  # listing never imports third-party code

    assert TransportRegistry.resolve("acme_http") is AcmeProvider
    info, _ = TransportRegistry.describe("acme_http")
    assert point.loads == 2 and info.origin == ORIGIN_ENTRY_POINT
    assert info.qualified_name == f"{__name__}:AcmeProvider"


def given_entry_point_named_like_a_builtin_when_listed_then_builtin_wins_without_duplicate(
    monkeypatch: pytest.MonkeyPatch, isolated_registry: None
) -> None:
    point = _FakeEntryPoint("generic_http", "elsewhere:Other", target=AcmeProvider)
    _entry_points(monkeypatch, point)
    assert TransportRegistry.names().count("generic_http") == 1
    assert TransportRegistry.resolve("generic_http") is GenericHttpProvider
    infos = [i for i in TransportRegistry.list_providers() if i.name == "generic_http"]
    assert len(infos) == 1 and infos[0].origin == ORIGIN_BUILTIN and point.loads == 0


def given_entry_point_failing_to_load_when_resolved_then_provider_invalid(
    monkeypatch: pytest.MonkeyPatch, isolated_registry: None
) -> None:
    _entry_points(
        monkeypatch, _FakeEntryPoint("broken", "nope:X", error=ImportError("no module nope"))
    )
    error = _config_error_of(lambda: TransportRegistry.resolve("broken"))
    assert error.error.error_code == "TRANSPORT_PROVIDER_INVALID"
    assert error.error.details["reason"] == "entry_point_load_failed"
    assert error.error.details["entry_point"] == "nope:X"
    assert "no module nope" in error.error.details["error"]


def given_unknown_name_when_resolved_then_provider_unknown_with_available_names(
    isolated_registry: None,
) -> None:
    error = _config_error_of(lambda: TransportRegistry.resolve("carrier_pigeon"))
    assert error.error.error_code == "TRANSPORT_PROVIDER_UNKNOWN"
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.details["provider"] == "carrier_pigeon"
    assert {"generic_http", "templated_http", "fake"} <= set(error.error.details["available"])


def given_import_path_to_missing_module_when_resolved_then_provider_unknown_with_error(
    isolated_registry: None,
) -> None:
    error = _config_error_of(lambda: TransportRegistry.resolve("no.such.module:Provider"))
    assert error.error.error_code == "TRANSPORT_PROVIDER_UNKNOWN"
    assert error.error.details["provider"] == "no.such.module:Provider"
    assert "ModuleNotFoundError" in error.error.details["error"]
    assert "generic_http" in error.error.details["available"]


def given_import_path_to_missing_attribute_when_resolved_then_provider_unknown(
    isolated_registry: None,
) -> None:
    error = _config_error_of(lambda: TransportRegistry.resolve(f"{__name__}:Nope"))
    assert error.error.error_code == "TRANSPORT_PROVIDER_UNKNOWN"
    assert "AttributeError" in error.error.details["error"]


@pytest.mark.parametrize(
    ("spec", "reason"),
    [
        (f"{__name__}:NotAGateway", "not a TransportGateway subclass"),
        (f"{__name__}:IMPORT_PATH", "not a class"),
        ("agentic_local_app.transport.base:TransportGateway", "abstract class"),
        ("agentic_local_app.transport.http_base:HttpProviderBase", "abstract class"),
    ],
)
def given_import_path_to_unusable_object_when_resolved_then_provider_invalid(
    isolated_registry: None, spec: str, reason: str
) -> None:
    error = _config_error_of(lambda: TransportRegistry.resolve(spec))
    assert error.error.error_code == "TRANSPORT_PROVIDER_INVALID"
    assert error.error.details["provider"] == spec
    assert error.error.details["reason"] == reason


def given_register_decorator_when_applied_then_name_resolvable_and_class_returned_unchanged(
    isolated_registry: None,
) -> None:
    decorated = TransportRegistry.register("recording")(RecordingProvider)
    assert decorated is RecordingProvider
    assert TransportRegistry.resolve("recording") is RecordingProvider
    assert "recording" in TransportRegistry.names()
    assert TransportRegistry.registered()["recording"] is RecordingProvider


def given_registered_name_when_registered_again_with_another_class_then_value_error(
    isolated_registry: None,
) -> None:
    TransportRegistry.register("recording")(RecordingProvider)
    TransportRegistry.register("recording")(RecordingProvider)  # idempotent for the same class
    with pytest.raises(ValueError, match="already registered"):
        TransportRegistry.register("recording")(AcmeProvider)


@pytest.mark.parametrize("candidate", [NotAGateway, TransportGateway, "text"])
def given_unusable_class_when_registered_then_type_error(
    isolated_registry: None, candidate: Any
) -> None:
    with pytest.raises(TypeError):
        TransportRegistry.register("bad")(candidate)


def given_generic_http_with_options_when_created_then_options_invalid(
    isolated_registry: None,
) -> None:
    config = AppConfig(transport=_section(options={"init": {"url": "x"}}))
    error = _config_error_of(lambda: TransportRegistry.create(config, clock=FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "GenericHttpProvider"
    assert error.error.details["errors"][0]["loc"] == ["init"]


def given_fake_with_options_when_created_then_options_invalid(isolated_registry: None) -> None:
    config = AppConfig(transport=_section(provider="fake", options={"x": 1}))
    error = _config_error_of(lambda: TransportRegistry.create(config, clock=FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "FakeTransportProvider"


def given_provider_with_options_model_when_options_invalid_then_detailed_config_error(
    isolated_registry: None,
) -> None:
    spec = f"{__name__}:AcmeProvider"
    config = AppConfig(transport=_section(provider=spec, options={"retries": -1, "extra": 1}))
    error = _config_error_of(lambda: TransportRegistry.create(config, clock=FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "AcmeProvider"
    locations = {tuple(problem["loc"]) for problem in error.error.details["errors"]}
    assert locations == {("retries",), ("extra",)}
    assert all({"loc", "msg", "type"} == set(problem) for problem in error.error.details["errors"])


def given_fake_provider_when_created_then_fake_gateway_with_reply_timeout_of_the_section(
    isolated_registry: None,
) -> None:
    clock = FakeClock()
    config = AppConfig(transport=_section(provider="fake", reply_timeout_ms=4_321))
    gateway = TransportRegistry.create(config, clock=clock, sleep=None, transport=None)
    assert isinstance(gateway, FakeTransportGateway)
    assert gateway.reply_timeout_ms == 4_321 and gateway.clock is clock


def given_generic_provider_when_created_then_section_clock_and_kwargs_forwarded(
    isolated_registry: None,
) -> None:
    clock = FakeClock()
    server = ScriptedServer(httpx.Response(201, json={"conversation_id": "c"}))
    config = AppConfig(transport=_section(init_url="http://registry.test/init"))
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    gateway = TransportRegistry.create(
        config, clock=clock, transport=httpx.MockTransport(server), sleep=sleep
    )
    assert isinstance(gateway, GenericHttpProvider)
    assert asyncio.run(gateway.init_conversation("i", {})) == "c"
    assert str(server.last.url) == "http://registry.test/init"


def given_import_path_provider_when_created_then_constructed_with_section_clock_and_kwargs(
    isolated_registry: None,
) -> None:
    RecordingProvider.instances.clear()
    clock = FakeClock()
    config = AppConfig(transport=_section(provider=IMPORT_PATH, user_id="ocp"))
    gateway = TransportRegistry.create(config, clock=clock, sleep=asyncio.sleep)
    assert isinstance(gateway, RecordingProvider)
    assert gateway.config is config.transport and gateway.config.user_id == "ocp"
    assert gateway.clock is clock and gateway.kwargs == {"sleep": asyncio.sleep}


def given_acme_provider_with_valid_options_when_created_then_options_accepted(
    isolated_registry: None,
) -> None:
    config = AppConfig(
        transport=_section(provider=f"{__name__}:AcmeProvider", options={"region": "us"})
    )
    gateway = TransportRegistry.create(config, clock=FakeClock())
    assert isinstance(gateway, AcmeProvider) and gateway.config.options == {"region": "us"}


# ================================================================================================
# 5. wiring — build_application selects the provider from the configuration
# ================================================================================================
def _app_config(tmp_path: Any, **transport: Any) -> AppConfig:
    return AppConfig(
        app=AppSection(data_dir=str(tmp_path / "data")),
        transport=_section(**transport),
    )


def _build(config: AppConfig, **kwargs: Any) -> Application:
    clock = FakeClock()
    return build_application(
        config,
        store=InMemoryConversationStore(),
        executor=FakeCommandExecutor(clock),
        clock=clock,
        ids=SequentialIdGenerator(),
        run_recovery=False,
        translator=ShellTranslator(ShellDialect.POSIX),  # scripted machine, ADR-030
        **kwargs,
    )


def given_provider_fake_in_config_when_application_built_then_fake_gateway_used(
    tmp_path: Any, isolated_registry: None
) -> None:
    app = _build(_app_config(tmp_path, provider="fake", reply_timeout_ms=999))
    try:
        assert isinstance(app.transport, FakeTransportGateway)
        assert app.transport.reply_timeout_ms == 999
        assert app.transport.clock is app.clock
    finally:
        app.close()


def given_default_provider_when_application_built_then_generic_http_gateway_used(
    tmp_path: Any, isolated_registry: None
) -> None:
    app = _build(_app_config(tmp_path))
    try:
        assert isinstance(app.transport, GenericHttpProvider)
        assert isinstance(app.transport, HttpTransportGateway)
    finally:
        app.close()


def given_custom_provider_by_import_path_when_application_built_then_used_without_code_change(
    tmp_path: Any, isolated_registry: None
) -> None:
    RecordingProvider.instances.clear()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    app = _build(_app_config(tmp_path, provider=IMPORT_PATH), sleep=sleep)
    try:
        assert isinstance(app.transport, RecordingProvider)
        assert app.transport.kwargs == {"sleep": sleep}  # the test sleep reaches the provider
        assert app.orchestrator.transport is app.transport  # type: ignore[attr-defined]
    finally:
        app.close()


def given_unknown_provider_when_application_built_then_config_error_before_wiring(
    tmp_path: Any, isolated_registry: None
) -> None:
    error = _config_error_of(lambda: _build(_app_config(tmp_path, provider="nope")))
    assert error.error.error_code == "TRANSPORT_PROVIDER_UNKNOWN"


def given_injected_transport_when_application_built_then_registry_not_consulted(
    tmp_path: Any, isolated_registry: None
) -> None:
    fake = FakeTransportGateway()
    app = _build(_app_config(tmp_path, provider="nope"), transport=fake)
    try:
        assert app.transport is fake
    finally:
        app.close()


# ================================================================================================
# 6. TemplatedHttpProvider — any HTTP API described by transport.options
# ================================================================================================
API = "https://api.example.com/v1"
KEY_ENV = "PHASE7_PROVIDERS_API_KEY"


def _templated_options(**overrides: Any) -> dict[str, Any]:
    options: dict[str, Any] = {
        "headers": {"X-Api-Key": f"${{env:{KEY_ENV}}}"},
        "init": {
            "method": "POST",
            "url": API + "/threads",
            "body": {
                "instructions": "{instructions}",
                "user": "{user_id}",
                "meta": "{metadata_json}",
            },
            "conversation_id_path": "data.id",
            "expected_statuses": [200, 201],
        },
        "post": {
            "method": "POST",
            "url": API + "/threads/{conversation_id}/messages",
            "body": {"role": "user", "content": "{message_json}"},
            "accepted_path": "ok",
            "message_id_path": "id",
        },
        "get": {
            "method": "GET",
            "url": API + "/threads/{conversation_id}/messages?since={after}",
            "messages_path": "items",
            "cursor_path": "next_cursor",
        },
        "close": {"method": "DELETE", "url": API + "/threads/{conversation_id}"},
    }
    for key, value in overrides.items():
        if value is None:
            options.pop(key, None)
        elif isinstance(value, dict) and isinstance(options.get(key), dict):
            merged = dict(options[key])
            for sub_key, sub_value in value.items():
                if sub_value is None:
                    merged.pop(sub_key, None)
                else:
                    merged[sub_key] = sub_value
            options[key] = merged
        else:
            options[key] = value
    return options


def _templated(
    server: Callable[[httpx.Request], Awaitable[httpx.Response]],
    options: dict[str, Any] | None = None,
    clock: FakeClock | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    **overrides: Any,
) -> TemplatedHttpProvider:
    kwargs: dict[str, Any] = {"transport": httpx.MockTransport(server)}
    if sleep is not None:
        kwargs["sleep"] = sleep
    section = _section(
        provider="templated_http",
        options=options if options is not None else _templated_options(),
        **overrides,
    )
    return TemplatedHttpProvider(section, clock or FakeClock(), **kwargs)


@pytest.fixture
def api_key(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv(KEY_ENV, "k-secret")
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    return "k-secret"


def given_templated_provider_when_checked_then_registered_with_an_options_model() -> None:
    assert issubclass(TemplatedHttpProvider, HttpProviderBase)
    assert TemplatedHttpProvider.options_model is not None
    assert TemplatedHttpProvider.options_model.model_validate(_templated_options())


@pytest.mark.parametrize(
    ("options", "location"),
    [
        (_templated_options(init=None), ("init",)),
        (_templated_options(init={"url": None}), ("init", "url")),
        (_templated_options(init={"conversation_id_path": None}), ("init", "conversation_id_path")),
        (_templated_options(get={"messages_path": None}), ("get", "messages_path")),
        (_templated_options(post={"method": "FETCH"}), ("post", "method")),
        (_templated_options(post={"surprise": 1}), ("post", "surprise")),
        (_templated_options(init={"expected_statuses": [99]}), ("init", "expected_statuses", 0)),
        (_templated_options(init={"url": API + "/{after}"}), ("init",)),
        (_templated_options(get={"headers": {"X": "{message_json}"}}), ("get",)),
        (_templated_options(headers={"X": "{conversation_id}"}), ("headers",)),
    ],
)
def given_invalid_templated_options_when_provider_built_then_options_invalid_at_location(
    options: dict[str, Any], location: tuple[Any, ...]
) -> None:
    error = _config_error_of(
        lambda: TemplatedHttpProvider(
            _section(provider="templated_http", options=options), FakeClock()
        )
    )
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "TemplatedHttpProvider"
    assert any(
        tuple(problem["loc"])[: len(location)] == location
        for problem in error.error.details["errors"]
    ), error.error.details


@pytest.mark.parametrize(
    ("path", "steps"),
    [
        ("id", ("id",)),
        ("data.items[0].id", ("data", "items", 0, "id")),
        ("[1].x", (1, "x")),
        ("a[0][2]", ("a", 0, 2)),
    ],
)
def given_dotted_path_when_parsed_then_keys_and_indices(
    path: str, steps: tuple[str | int, ...]
) -> None:
    assert parse_path(path) == steps


@pytest.mark.parametrize("path", ["a..b", "items[x]", "", "a.", "a[0]b", "[]"])
def given_malformed_path_when_parsed_then_value_error(path: str) -> None:
    with pytest.raises(ValueError):
        parse_path(path)


@pytest.mark.parametrize("path", ["a..b", "items[x]", "a[0]b"])
def given_malformed_response_path_in_options_when_provider_built_then_options_invalid(
    path: str,
) -> None:
    options = _templated_options(init={"conversation_id_path": path})
    error = _config_error_of(
        lambda: TemplatedHttpProvider(
            _section(provider="templated_http", options=options), FakeClock()
        )
    )
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["errors"][0]["loc"] == ["init", "conversation_id_path"]


def given_empty_expected_statuses_when_provider_built_then_options_invalid() -> None:
    options = _templated_options(get={"expected_statuses": []})
    error = _config_error_of(
        lambda: TemplatedHttpProvider(
            _section(provider="templated_http", options=options), FakeClock()
        )
    )
    assert error.error.details["errors"][0]["loc"] == ["get", "expected_statuses"]


@pytest.mark.parametrize(
    ("body", "path", "found"),
    [
        ({"items": [{"id": "a"}]}, "items[0].id", "a"),
        ({"items": [{"id": "a"}]}, "items[1].id", None),
        ({"items": {"0": "x"}}, "items[0]", None),
        ({"items": [[1, 2]]}, "items[0][1]", 2),
    ],
)
def given_body_when_extract_path_then_value_or_path_not_found(
    body: Any, path: str, found: Any
) -> None:
    if found is None:
        with pytest.raises(InvalidResponseError) as exc:
            extract_path(body, path)
        assert exc.value.details == {"reason": "path_not_found", "path": path}
    else:
        assert extract_path(body, path) == found


async def given_accepted_false_without_message_id_in_body_when_post_then_sent_id_in_details(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": False}))
    error = await _error_of(_templated(server).post_message(RID, _payload("msg-0077")))
    assert error.error.error_code == "POST_NOT_ACCEPTED"
    assert error.error.details["message_id"] == "msg-0077"


async def given_env_reference_in_common_header_when_init_then_resolved_at_call_time(
    api_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = ScriptedServer(httpx.Response(201, json={"data": {"id": "th-1"}}))
    provider = _templated(server, _templated_options(init={"headers": {"X-Op": "init-{user_id}"}}))
    monkeypatch.setenv(KEY_ENV, "rotated")  # after construction: the value is read when calling

    remote = await provider.init_conversation("PROTO", {"session_id": "s-1"})

    assert remote == "th-1"
    headers = server.last.headers
    assert headers["X-Api-Key"] == "rotated" and headers["X-Op"] == "init-tester"
    assert headers["Accept"] == "application/json"
    assert headers["Content-Type"] == "application/json"
    assert "X-User-Id" not in headers and "Authorization" not in headers


async def given_missing_env_variable_when_call_built_then_transport_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(KEY_ENV, raising=False)
    server = ScriptedServer(httpx.Response(201, json={"data": {"id": "th-1"}}))
    with pytest.raises(ConfigError) as exc:
        await _templated(server).init_conversation("i", {})
    error = exc.value
    assert error.error.error_code == "TRANSPORT_ENV_MISSING"
    assert error.error.details["variable"] == KEY_ENV
    assert error.error.details["operation"] == OP_INIT
    assert server.requests == []


async def given_token_placeholder_when_token_in_environment_then_substituted(
    api_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "bearer-1")
    server = ScriptedServer(httpx.Response(201, json={"data": {"id": "th-1"}}))
    provider = _templated(server, _templated_options(headers={"Authorization": "Bearer {token}"}))
    await provider.init_conversation("i", {})
    assert server.last.headers["Authorization"] == "Bearer bearer-1"


async def given_token_placeholder_when_token_missing_then_transport_env_missing_names_token_env(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(201, json={"data": {"id": "th-1"}}))
    provider = _templated(server, _templated_options(headers={"Authorization": "Bearer {token}"}))
    with pytest.raises(ConfigError) as exc:
        await provider.init_conversation("i", {})
    assert exc.value.error.error_code == "TRANSPORT_ENV_MISSING"
    assert exc.value.error.details["variable"] == TOKEN_ENV


async def given_init_body_template_when_init_then_leaves_substituted_and_metadata_inserted_as_object(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(201, json={"data": {"id": "th-1"}}))
    options = _templated_options(
        init={
            "body": {
                "instructions": "{instructions}",
                "user": "u:{user_id}",
                "meta": "{metadata_json}",
                "meta_text": "m={metadata_json}",
                "count": 3,
                "flag": True,
                "nothing": None,
                "list": ["{user_id}", 1],
            }
        }
    )
    await _templated(server, options).init_conversation("PROTO", {"session_id": "s-1", "n": 2})
    assert json.loads(server.last.content) == {
        "instructions": "PROTO",
        "user": "u:tester",
        "meta": {"session_id": "s-1", "n": 2},
        "meta_text": 'm={"n":2,"session_id":"s-1"}',
        "count": 3,
        "flag": True,
        "nothing": None,
        "list": ["tester", 1],
    }


async def given_post_body_template_when_post_then_message_object_inserted_and_ids_substituted(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": True, "id": "srv-9"}))
    options = _templated_options(
        post={
            "body": {
                "role": "user",
                "content": "{message_json}",
                "ref": "{message_id}/{message_type}",
            }
        }
    )
    ack = await _templated(server, options).post_message("th 1", _payload("msg-0002"))
    assert ack == PostAck(message_id="srv-9", accepted=True, http_status=200)
    assert str(server.last.url) == API + "/threads/th%201/messages"
    assert server.last.method == "POST"
    assert json.loads(server.last.content) == {
        "role": "user",
        "content": _payload("msg-0002"),
        "ref": "msg-0002/user_request",
    }


async def given_post_body_as_whole_message_placeholder_when_post_then_message_is_the_body(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(202, json={"ok": True, "id": "x"}))
    await _templated(server, _templated_options(post={"body": "{message_json}"})).post_message(
        RID, _payload()
    )
    assert json.loads(server.last.content) == _payload()
    assert server.last.content.decode() == canonical_json(_payload())


async def given_no_message_id_path_when_post_then_ack_carries_the_sent_message_id(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": True}))
    options = _templated_options(post={"message_id_path": None})
    ack = await _templated(server, options).post_message(RID, _payload("msg-0042"))
    assert ack.message_id == "msg-0042" and ack.accepted is True


async def given_no_accepted_path_and_no_message_id_path_when_post_then_accepted_on_status_without_json(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(204))
    options = _templated_options(
        post={"accepted_path": None, "message_id_path": None, "expected_statuses": [204]}
    )
    ack = await _templated(server, options).post_message(RID, _payload("msg-1"))
    assert ack == PostAck(message_id="msg-1", accepted=True, http_status=204)


async def given_accepted_path_false_when_post_then_post_not_accepted(api_key: str) -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": False, "id": "srv-1"}))
    error = await _error_of(_templated(server).post_message(RID, _payload()))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "POST_NOT_ACCEPTED"
    assert error.error.retryable is False
    assert error.error.details["operation"] == OP_POST and error.error.details["http_status"] == 200
    assert error.error.details["message_id"] == "srv-1"
    assert error.error.details["path"] == "ok"


async def given_accepted_path_not_boolean_when_post_then_invalid_response_body(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"ok": "yes", "id": "srv-1"}))
    error = await _error_of(_templated(server).post_message(RID, _payload()))
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert (
        error.error.details["path"] == "ok" and error.error.details["reason"] == "unexpected_type"
    )


@pytest.mark.parametrize(
    ("body", "path"),
    [
        ({"data": {}}, "data.id"),
        ({"data": {"id": ""}}, "data.id"),
        ({"data": {"id": ["x"]}}, "data.id"),
        ({"data": []}, "data.id"),
        ([], "data.id"),
        ("text", "data.id"),
    ],
)
async def given_conversation_id_path_absent_or_invalid_when_init_then_invalid_response_body(
    api_key: str, body: Any, path: str
) -> None:
    server = ScriptedServer(httpx.Response(201, json=body))
    error = await _error_of(_templated(server).init_conversation("i", {}))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.details["path"] == path
    assert error.error.details["reason"] in {"path_not_found", "unexpected_type"}
    assert error.error.details["operation"] == OP_INIT


async def given_conversation_id_path_with_list_index_when_init_then_extracted(
    api_key: str,
) -> None:
    body = {"data": {"items": [{"id": "first"}, {"id": "second"}]}}
    server = ScriptedServer(httpx.Response(200, json=body))
    options = _templated_options(init={"conversation_id_path": "data.items[1].id"})
    assert await _templated(server, options).init_conversation("i", {}) == "second"


async def given_numeric_conversation_id_when_init_then_returned_as_text(api_key: str) -> None:
    server = ScriptedServer(httpx.Response(200, json={"data": {"id": 42}}))
    assert await _templated(server).init_conversation("i", {}) == "42"


async def given_get_url_template_when_get_messages_then_after_and_conversation_id_url_encoded(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"items": [], "next_cursor": None}))
    provider = _templated(server)
    result = await provider.get_messages("th/1", "msg 1/2")
    assert str(server.last.url) == API + "/threads/th%2F1/messages?since=msg%201%2F2"
    assert server.last.method == "GET"
    assert "Content-Type" not in server.last.headers and server.last.content == b""
    assert result == GetResult(messages=[], cursor=None, http_status=200)


async def given_no_cursor_when_get_messages_then_after_placeholder_empty(api_key: str) -> None:
    server = ScriptedServer(httpx.Response(200, json={"items": []}))
    await _templated(server).get_messages(RID, None)
    assert str(server.last.url) == API + f"/threads/{RID}/messages?since="


async def given_cursor_path_present_when_get_messages_then_cursor_read_from_body(
    api_key: str,
) -> None:
    body = {"items": [_model_message("a"), _model_message("b")], "next_cursor": "page-2"}
    server = ScriptedServer(httpx.Response(200, json=body))
    result = await _templated(server).get_messages(RID, None)
    assert result.cursor == "page-2"
    assert [m["message_id"] for m in result.messages] == ["a", "b"]


@pytest.mark.parametrize(
    "body",
    [{"items": [_model_message("m-9")], "next_cursor": None}, {"items": [_model_message("m-9")]}],
)
async def given_cursor_path_null_or_absent_when_get_messages_then_cursor_is_last_message_id(
    api_key: str, body: dict[str, Any]
) -> None:
    server = ScriptedServer(httpx.Response(200, json=body))
    result = await _templated(server).get_messages(RID, None)
    assert result.cursor == "m-9"


async def given_no_cursor_path_option_when_get_messages_then_cursor_is_last_message_id(
    api_key: str,
) -> None:
    body = {"items": [_model_message("m-1"), _model_message("m-2")], "next_cursor": "ignored"}
    server = ScriptedServer(httpx.Response(200, json=body))
    result = await _templated(server, _templated_options(get={"cursor_path": None})).get_messages(
        RID, None
    )
    assert result.cursor == "m-2"


async def given_cursor_path_with_non_string_when_get_messages_then_invalid_response_body(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"items": [], "next_cursor": 12}))
    error = await _error_of(_templated(server).get_messages(RID, None))
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.details["path"] == "next_cursor"


async def given_message_path_when_get_messages_then_protocol_message_extracted_from_each_item(
    api_key: str,
) -> None:
    body = {
        "items": [
            {"payload": _model_message("a"), "meta": {"ts": 1}},
            {"payload": _model_message("b"), "meta": {"ts": 2}},
        ]
    }
    server = ScriptedServer(httpx.Response(200, json=body))
    options = _templated_options(get={"message_path": "payload"})
    result = await _templated(server, options).get_messages(RID, None)
    assert result.messages == [_model_message("a"), _model_message("b")]
    assert result.cursor == "b"


@pytest.mark.parametrize(
    ("body", "message_path", "path", "reason"),
    [
        ({"items": "nope"}, None, "items", "unexpected_type"),
        ({"other": []}, None, "items", "path_not_found"),
        ({"items": ["not-a-dict"]}, None, "items[0]", "unexpected_type"),
        ({"items": [_model_message(), 3]}, None, "items[1]", "unexpected_type"),
        ({"items": [{"payload": "x"}]}, "payload", "items[0].payload", "unexpected_type"),
        ({"items": [{"nope": {}}]}, "payload", "items[0].payload", "path_not_found"),
        ({"items": ["not-a-dict"]}, "payload", "items[0].payload", "path_not_found"),
    ],
)
async def given_messages_not_objects_when_get_messages_then_invalid_response_body_with_path(
    api_key: str, body: Any, message_path: str | None, path: str, reason: str
) -> None:
    server = ScriptedServer(httpx.Response(200, json=body))
    options = _templated_options(get={"message_path": message_path})
    error = await _error_of(_templated(server, options).get_messages(RID, None))
    assert error.error.error_code == "INVALID_RESPONSE_BODY"
    assert error.error.details["path"] == path and error.error.details["reason"] == reason
    assert error.error.details["operation"] == OP_GET


async def given_status_outside_expected_when_init_then_unexpected_status_protocol_error(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"data": {"id": "x"}}))
    options = _templated_options(init={"expected_statuses": [201]})
    error = await _error_of(_templated(server, options).init_conversation("i", {}))
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert (
        error.error.error_code == "UNEXPECTED_STATUS" and error.error.details["http_status"] == 200
    )


async def given_429_when_templated_request_sent_then_inherited_rate_limit_mapping(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(429, headers={"Retry-After": "2"}, text="slow"))
    error = await _error_of(_templated(server).post_message(RID, _payload()))
    assert error.error.error_type is ErrorType.RATE_LIMIT_ERROR
    assert error.error.error_code == "HTTP_429" and error.error.retryable is True
    assert error.error.details["retry_after_ms"] == 2_000
    assert error.error.details["operation"] == OP_POST
    assert error.error.details["url"] == API + f"/threads/{RID}/messages"
    assert error.error.details["body"] == "slow"


async def given_context_window_body_when_templated_request_sent_then_context_window_error(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(400, json={"error": "context_window_exceeded"}))
    error = await _error_of(_templated(server).post_message(RID, _payload()))
    assert error.error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR
    assert error.error.error_code == "CONTEXT_WINDOW_EXCEEDED"


async def given_empty_gets_when_templated_wait_for_reply_then_inherited_polling(
    api_key: str,
) -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(
        httpx.Response(200, json={"items": []}),
        httpx.Response(200, json={"items": []}),
        httpx.Response(200, json={"items": [_model_message("m-3")], "next_cursor": "c-3"}),
    )
    provider = _templated(
        server, clock=clock, sleep=sleep, poll_interval_ms=700, reply_timeout_ms=5_000
    )
    result = await provider.wait_for_reply(RID, "c-2")
    assert result.cursor == "c-3" and sleeps == [0.7, 0.7]
    assert all(str(r.url).endswith("since=c-2") for r in server.requests)


async def given_silent_api_when_templated_wait_for_reply_then_model_get_timeout(
    api_key: str,
) -> None:
    clock = FakeClock()
    sleep, sleeps = _sleeper(clock)
    server = ScriptedServer(httpx.Response(200, json={"items": []}))
    provider = _templated(
        server, clock=clock, sleep=sleep, poll_interval_ms=1_000, reply_timeout_ms=2_000
    )
    error = await _error_of(provider.wait_for_reply(RID, None))
    assert error.error.error_code == "MODEL_GET_TIMEOUT" and sleeps == [1.0, 1.0]
    assert error.error.details["url"] == API + f"/threads/{RID}/messages?since="


async def given_close_option_when_close_conversation_then_delete_sent_to_templated_url(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(204))
    await _templated(server).close_conversation("th 1")
    assert server.last.method == "DELETE" and str(server.last.url) == API + "/threads/th%201"
    assert server.last.headers["X-Api-Key"] == "k-secret"
    assert server.last.content == b""


async def given_close_option_with_body_when_close_conversation_then_body_sent(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"closed": True}))
    options = _templated_options(
        close={"method": "POST", "url": API + "/close", "body": {"thread": "{conversation_id}"}}
    )
    await _templated(server, options).close_conversation("th-1")
    assert server.last.method == "POST" and json.loads(server.last.content) == {"thread": "th-1"}


async def given_no_close_option_when_close_conversation_then_no_request(api_key: str) -> None:
    server = ScriptedServer(httpx.Response(200))
    await _templated(server, _templated_options(close=None)).close_conversation(RID)
    assert server.requests == []


async def given_close_error_when_close_conversation_then_close_operation_in_details(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(503))
    error = await _error_of(_templated(server).close_conversation(RID))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.details["operation"] == OP_CLOSE


async def given_env_secret_in_url_when_error_raised_then_url_detail_redacted(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(500))
    options = _templated_options(init={"url": API + "/threads?key=${env:" + KEY_ENV + "}"})
    error = await _error_of(_templated(server, options).init_conversation("i", {}))
    assert str(server.last.url) == API + "/threads?key=k-secret"
    assert "k-secret" not in error.error.details["url"]
    assert error.error.details["url"] == API + "/threads?key=***"


async def given_gzip_enabled_when_templated_post_then_body_gzip_encoded_by_the_base(
    api_key: str,
) -> None:
    import gzip as gzip_module

    server = ScriptedServer(httpx.Response(200, json={"ok": True, "id": "x"}))
    await _templated(server, gzip=True).post_message(RID, _payload())
    assert server.last.headers["Content-Encoding"] == "gzip"
    assert json.loads(gzip_module.decompress(server.last.content)) == {
        "role": "user",
        "content": _payload(),
    }


async def given_per_operation_header_when_sent_then_overrides_common_header_case_insensitively(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"items": []}))
    options = _templated_options(
        headers={"X-Api-Key": "${env:" + KEY_ENV + "}", "Accept": "text/plain"},
        get={"headers": {"x-api-key": "override", "X-Trace": "{conversation_id}"}},
    )
    await _templated(server, options).get_messages("th-1", None)
    headers = server.last.headers
    assert headers["x-api-key"] == "override" and headers["X-Trace"] == "th-1"
    assert headers["Accept"] == "text/plain"
    assert len([name for name in headers if name.lower() == "x-api-key"]) == 1


async def given_get_with_body_option_when_get_messages_then_post_style_polling_supported(
    api_key: str,
) -> None:
    server = ScriptedServer(httpx.Response(200, json={"items": []}))
    options = _templated_options(
        get={
            "method": "POST",
            "url": API + "/poll",
            "body": {"thread": "{conversation_id}", "after": "{after}"},
        }
    )
    await _templated(server, options).get_messages("th-1", "c-1")
    assert server.last.method == "POST"
    assert json.loads(server.last.content) == {"thread": "th-1", "after": "c-1"}


# ================================================================================================
# 7. equivalence — a templated configuration reproducing ADR-004 behaves like generic_http
# ================================================================================================
MOCK = "http://mock/v1/conversations"


def _adr004_as_templated_options() -> dict[str, Any]:
    return {
        "headers": {"X-User-Id": "{user_id}"},
        "init": {
            "method": "POST",
            "url": MOCK,
            "body": {
                "user_id": "{user_id}",
                "instructions": "{instructions}",
                "metadata": "{metadata_json}",
            },
            "conversation_id_path": "conversation_id",
            "expected_statuses": [201],
        },
        "post": {
            "method": "POST",
            "url": MOCK + "/{conversation_id}/messages",
            "body": "{message_json}",
            "accepted_path": "accepted",
            "message_id_path": "message_id",
            "expected_statuses": [202],
        },
        "get": {
            "method": "GET",
            "url": MOCK + "/{conversation_id}/messages?after={after}",
            "messages_path": "messages",
            "cursor_path": "cursor",
        },
        "close": {"method": "POST", "url": MOCK + "/{conversation_id}/close"},
    }


def _mock_section(**overrides: Any) -> TransportSection:
    return _section(
        init_url=MOCK,
        post_url=MOCK + "/{conversation_id}/messages",
        get_url=MOCK + "/{conversation_id}/messages?after={after}",
        close_url=MOCK + "/{conversation_id}/close",
        gzip=True,
        **overrides,
    )


def _app_message(message_type: str, message_id: str) -> dict[str, Any]:
    return {
        "type": message_type,
        "conversation_id": "conv-0001",
        "message_id": message_id,
        "content": {"plan_id": "plan-0", "status": "completed", "results": []},
    }


async def _drive(gateway: TransportGateway) -> list[Any]:
    remote = await gateway.init_conversation("PROTOCOL", {"session_id": "sess-0001"})
    ack = await gateway.post_message(remote, _app_message("user_request", "msg-0001"))
    discovery = await gateway.wait_for_reply(remote, None)
    await gateway.post_message(remote, _app_message("execution_result", "msg-0002"))
    execution = await gateway.wait_for_reply(remote, discovery.cursor)
    await gateway.close_conversation(remote)
    empty = await gateway.get_messages(remote, execution.cursor)
    return [remote, ack, discovery, execution, empty]


async def given_templated_config_reproducing_adr004_when_driven_against_mock_then_same_as_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    generic_app = create_mock_app(default_java_debug_scenario())
    templated_app = create_mock_app(default_java_debug_scenario())
    generic = GenericHttpProvider(
        _mock_section(), FakeClock(), transport=httpx.ASGITransport(app=generic_app)
    )
    templated = TemplatedHttpProvider(
        _mock_section(provider="templated_http", options=_adr004_as_templated_options()),
        FakeClock(),
        transport=httpx.ASGITransport(app=templated_app),
    )

    generic_run = await _drive(generic)
    templated_run = await _drive(templated)

    assert templated_run == generic_run
    assert generic_run[0] == "mock-conv-0001"
    assert [m["type"] for m in generic_run[2].messages] == ["discovery_plan"]
    assert [m["type"] for m in generic_run[3].messages] == ["execution_plan"]
    assert generic_run[4].messages == []
    generic_engine, templated_engine = generic_app.state.engine, templated_app.state.engine
    assert templated_engine.inits == generic_engine.inits
    assert templated_engine.inits[0] == {
        "user_id": "tester",
        "instructions": "PROTOCOL",
        "metadata": {"session_id": "sess-0001"},
    }
    assert templated_engine.received == generic_engine.received
    assert templated_engine.closed == generic_engine.closed == ["mock-conv-0001"]


async def given_templated_config_when_mock_injects_503_then_same_error_as_generic() -> None:
    fault = Fault(status=503, body={"error": "unavailable"}, times=1, on_operation="post")
    scenario = Scenario(steps=[Step(on="user_request", respond=[], fault=fault)])
    templated = TemplatedHttpProvider(
        _mock_section(provider="templated_http", options=_adr004_as_templated_options()),
        FakeClock(),
        transport=httpx.ASGITransport(app=create_mock_app(scenario)),
    )
    remote = await templated.init_conversation("i", {})
    error = await _error_of(templated.post_message(remote, _app_message("user_request", "m1")))
    assert error.error.error_type is ErrorType.NETWORK_ERROR
    assert error.error.error_code == "HTTP_503" and error.error.retryable is True
    ack = await templated.post_message(remote, _app_message("user_request", "m1"))
    assert ack.accepted and ack.message_id == "m1"
