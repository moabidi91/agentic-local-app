"""The out-of-tree plugin quoted by the guides (``examples/acme_model_plugin``, guides 03 and 04).

These tests pin what the guides promise: a provider written outside the repository is selected by
its import path and validated like a built-in one, ``HttpProviderBase`` does the HTTP for it
(headers, statuses, error table, polling, abandonment), a codec written outside the repository is
pure and reports every unreadable reply as ``UNPARSEABLE_REPLY`` with an excerpt, and
``examples/config.acme.toml`` loads and resolves both. No network: ``httpx.MockTransport``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from acme_model_plugin import AcmeHttpProvider, AcmeOptions, StreamedTextCodec, StreamedTextOptions
from agentic_local_app.config import TransportSection, load_config
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ConfigError, ErrorType, TransportError
from agentic_local_app.transport.base import OP_GET, OP_INIT, OP_POST
from agentic_local_app.transport.codecs import (
    UNPARSEABLE_REPLY,
    CodecError,
    CodecRegistry,
    CodecTransport,
    apply_codec,
)
from agentic_local_app.transport.registry import ORIGIN_IMPORT_PATH, TransportRegistry

pytestmark = pytest.mark.phase7

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_CONFIG = REPO_ROOT / "examples" / "config.acme.toml"
PROVIDER_PATH = "acme_model_plugin.provider:AcmeHttpProvider"
CODEC_PATH = "acme_model_plugin.codec:StreamedTextCodec"
BASE_URL = "https://acme.test/api/v2"
THREAD = "thr_0001"
ENV = {"ACME_API_KEY": "k-secret"}


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def _section(**options: Any) -> TransportSection:
    values = {"base_url": BASE_URL, "workspace": "demo", "api_key_env": "ACME_API_KEY"}
    values.update(options)
    return TransportSection(provider=PROVIDER_PATH, gzip=False, options=values)


class AcmeServer:
    """A scripted ACME API: each handler answers by path suffix; requests are recorded."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.responses: dict[str, Callable[[httpx.Request], httpx.Response]] = {}

    def on(
        self, method: str, suffix: str, response: Callable[[httpx.Request], httpx.Response]
    ) -> None:
        self.responses[f"{method} {suffix}"] = response

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        for key, response in self.responses.items():
            method, suffix = key.split(" ", 1)
            if request.method == method and request.url.path.endswith(suffix):
                return response(request)
        return httpx.Response(404, json={"error": {"code": "not_found"}})

    @property
    def last(self) -> httpx.Request:
        return self.requests[-1]


def _provider(
    server: AcmeServer,
    *,
    environ: dict[str, str] | None = None,
    clock: FakeClock | None = None,
    **options: Any,
) -> AcmeHttpProvider:
    return AcmeHttpProvider(
        _section(**options),
        clock or FakeClock(),
        transport=httpx.MockTransport(server),
        environ=ENV if environ is None else environ,
    )


def _envelope(message_id: str = "m-1") -> dict[str, Any]:
    return {
        "type": "final_answer",
        "conversation_id": THREAD,
        "message_id": message_id,
        "content": {"status": "completed", "diagnosis": "ok", "evidence": []},
    }


def _chunked(text: str, size: int = 7) -> dict[str, Any]:
    return {"chunks": [{"delta": text[i : i + size]} for i in range(0, len(text), size)]}


# ================================================================================================
# provider — options and selection
# ================================================================================================
def given_import_path_when_resolved_then_provider_class_found_with_import_path_origin() -> None:
    info, plugin = TransportRegistry.describe(PROVIDER_PATH)
    assert plugin is AcmeHttpProvider
    assert info.origin == ORIGIN_IMPORT_PATH
    assert info.qualified_name == PROVIDER_PATH


def given_import_path_when_listed_then_not_in_the_builtin_names() -> None:
    assert "acme" not in " ".join(TransportRegistry.names())


def given_missing_workspace_when_options_validated_then_transport_options_invalid() -> None:
    with pytest.raises(ConfigError) as exc:
        AcmeHttpProvider(
            TransportSection(provider=PROVIDER_PATH, options={"base_url": BASE_URL}), FakeClock()
        )
    error = exc.value.error
    assert error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.details["provider"] == "AcmeHttpProvider"
    assert [e["loc"] for e in error.details["errors"]] == [["workspace"]]


def given_unknown_option_when_options_validated_then_rejected_not_silently_ignored() -> None:
    with pytest.raises(ConfigError) as exc:
        _provider(AcmeServer(), page_sizee=10)
    assert exc.value.error.details["errors"][0]["type"] == "extra_forbidden"


def given_base_url_with_trailing_slash_when_validated_then_normalised() -> None:
    assert AcmeOptions(base_url="https://x.test/v1/", workspace="w").base_url == "https://x.test/v1"


def given_base_url_without_scheme_when_validated_then_refused() -> None:
    with pytest.raises(ConfigError):
        _provider(AcmeServer(), base_url="acme.test/v1")


# ================================================================================================
# provider — the four calls
# ================================================================================================
async def given_api_key_in_environment_when_init_then_acme_headers_and_body_and_thread_id() -> None:
    server = AcmeServer()
    server.on(
        "POST",
        "/workspaces/demo/threads",
        lambda r: httpx.Response(201, json={"thread": {"id": THREAD}}),
    )
    provider = _provider(server)
    remote = await provider.init_conversation("INSTRUCTIONS", {"session_id": "s-1"})
    assert remote == THREAD
    request = server.last
    assert str(request.url) == f"{BASE_URL}/workspaces/demo/threads"
    assert request.headers["X-Api-Key"] == "k-secret"
    assert request.headers["X-Workspace"] == "demo"
    assert "Authorization" not in request.headers  # the ADR-004 bearer token never leaks here
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(request.content) == {
        "labels": {"session_id": "s-1"},
        "owner": "local-user",
        "system": "INSTRUCTIONS",
    }


async def given_missing_api_key_when_init_then_transport_env_missing_names_the_variable() -> None:
    provider = _provider(AcmeServer(), environ={})
    with pytest.raises(ConfigError) as exc:
        await provider.init_conversation("i", {})
    assert exc.value.error.error_code == "TRANSPORT_ENV_MISSING"
    assert exc.value.error.details == {"variable": "ACME_API_KEY", "operation": OP_INIT}


async def given_init_reply_without_thread_id_when_parsed_then_invalid_response_body_stamped() -> (
    None
):
    server = AcmeServer()
    server.on("POST", "/threads", lambda r: httpx.Response(201, json={"thread": {}}))
    with pytest.raises(TransportError) as exc:
        await _provider(server).init_conversation("i", {})
    error = exc.value.error
    assert (error.error_type, error.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "INVALID_RESPONSE_BODY",
    )
    assert error.details["path"] == "thread.id"
    assert error.details["operation"] == OP_INIT and error.details["http_status"] == 201
    assert error.retryable is False


async def given_envelope_when_posted_then_wrapped_as_message_event_and_ack_names_the_message() -> (
    None
):
    server = AcmeServer()
    server.on("POST", "/events", lambda r: httpx.Response(202, json={"event": {"id": "evt_9"}}))
    ack = await _provider(server).post_message(THREAD, _envelope("m-7"))
    assert (ack.message_id, ack.accepted, ack.http_status) == ("m-7", True, 202)
    assert str(server.last.url) == f"{BASE_URL}/threads/{THREAD}/events"
    assert json.loads(server.last.content) == {"kind": "message", "payload": _envelope("m-7")}


async def given_text_payload_when_posted_then_ack_message_id_empty_and_decorator_restores_it() -> (
    None
):
    server = AcmeServer()
    server.on("POST", "/events", lambda r: httpx.Response(202, json={"event": {"id": "evt_1"}}))
    provider = _provider(server)
    text = canonical_json(_envelope("m-3"))
    bare = await provider.post_message(THREAD, text)  # type: ignore[arg-type]
    assert bare.message_id == ""
    assert json.loads(server.last.content) == {"kind": "message", "payload": text}
    decorated = apply_codec(provider, StreamedTextCodec(StreamedTextOptions(outbound="text")))
    assert isinstance(decorated, CodecTransport)
    ack = await decorated.post_message(THREAD, _envelope("m-3"))
    assert ack.message_id == "m-3"


async def given_events_of_several_kinds_when_get_then_only_message_payloads_and_next_cursor() -> (
    None
):
    server = AcmeServer()
    server.on(
        "GET",
        "/events",
        lambda r: httpx.Response(
            200,
            json={
                "events": [
                    {"id": "evt_1", "kind": "status", "payload": {"state": "thinking"}},
                    {"id": "evt_2", "kind": "message", "payload": _envelope("m-1")},
                ],
                "next": "evt_2",
            },
        ),
    )
    result = await _provider(server, page_size=25).get_messages(THREAD, "evt_0")
    assert result.messages == [_envelope("m-1")]
    assert result.cursor == "evt_2" and result.http_status == 200
    assert str(server.last.url) == f"{BASE_URL}/threads/{THREAD}/events?after=evt_0&limit=25"


async def given_no_message_events_when_wait_for_reply_then_polls_until_model_get_timeout() -> None:
    server = AcmeServer()
    server.on("GET", "/events", lambda r: httpx.Response(200, json={"events": [], "next": None}))
    clock = FakeClock()
    section = _section()
    section = section.model_copy(update={"poll_interval_ms": 1_000, "reply_timeout_ms": 3_000})

    async def sleep(seconds: float) -> None:
        clock.advance(int(seconds * 1000))

    provider = AcmeHttpProvider(
        section, clock, transport=httpx.MockTransport(server), sleep=sleep, environ=ENV
    )
    with pytest.raises(TransportError) as exc:
        await provider.wait_for_reply(THREAD, None)
    assert exc.value.error.error_code == "MODEL_GET_TIMEOUT"
    assert exc.value.error.details["polls"] == 4  # t=0, 1000, 2000, 3000


async def given_throttled_reply_when_get_then_rate_limit_error_with_retry_after_from_body() -> None:
    server = AcmeServer()
    server.on(
        "GET",
        "/events",
        lambda r: httpx.Response(429, json={"error": {"code": "throttled", "retry_in_ms": 1500}}),
    )
    with pytest.raises(TransportError) as exc:
        await _provider(server).get_messages(THREAD, None)
    error = exc.value.error
    assert (error.error_type, error.error_code, error.retryable) == (
        ErrorType.RATE_LIMIT_ERROR,
        "HTTP_429",
        True,
    )
    assert error.details["retry_after_ms"] == 1500
    assert error.details["operation"] == OP_GET and error.details["http_status"] == 429


async def given_unauthorised_reply_when_post_then_base_error_table_applies_unchanged() -> None:
    server = AcmeServer()
    server.on("POST", "/events", lambda r: httpx.Response(401, json={"error": "bad key"}))
    with pytest.raises(TransportError) as exc:
        await _provider(server).post_message(THREAD, _envelope())
    assert exc.value.error.error_type is ErrorType.AUTHN_ERROR
    assert exc.value.error.details["operation"] == OP_POST


async def given_close_when_204_without_body_then_delete_sent_and_nothing_raised() -> None:
    server = AcmeServer()
    server.on("DELETE", f"/threads/{THREAD}", lambda r: httpx.Response(204))
    await _provider(server).close_conversation(THREAD)
    assert server.last.method == "DELETE"
    assert str(server.last.url) == f"{BASE_URL}/threads/{THREAD}"


# ================================================================================================
# codec — pure, options, errors
# ================================================================================================
def given_import_path_when_resolved_then_codec_class_found_with_import_path_origin() -> None:
    info, plugin = CodecRegistry.describe(CODEC_PATH)
    assert plugin is StreamedTextCodec and info.origin == ORIGIN_IMPORT_PATH


def given_chunks_of_a_fenced_message_when_decoded_then_one_envelope() -> None:
    codec = StreamedTextCodec(StreamedTextOptions(chunks_path="chunks", delta_path="delta"))
    text = "Here you go:\n```json\n" + json.dumps(_envelope("m-1"), indent=2) + "\n```\nDone."
    assert codec.decode_inbound([_chunked(text)]) == [_envelope("m-1")]


def given_plain_string_chunks_when_no_paths_configured_then_the_item_is_the_list() -> None:
    codec = StreamedTextCodec()
    parts = [canonical_json(_envelope("m-2"))[i : i + 5] for i in range(0, 200, 5)]
    assert codec.decode_inbound([parts]) == [_envelope("m-2")]


def given_chunks_without_any_json_when_decoded_then_unparseable_reply_with_excerpt() -> None:
    codec = StreamedTextCodec(StreamedTextOptions(chunks_path="chunks", delta_path="delta"))
    raw = _chunked("I cannot produce a plan right now, sorry.")
    with pytest.raises(CodecError) as exc:
        codec.decode_inbound([raw])
    error = exc.value.error
    assert (error.error_type, error.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        UNPARSEABLE_REPLY,
    )
    assert error.retryable is False
    assert error.details["codec"] == "streamed_text"
    assert error.details["index"] == 0 and error.details["reason"] == "no_json_found"
    assert error.details["excerpt"] == canonical_json(raw)[:500]


def given_chunk_without_delta_when_decoded_then_path_not_found_names_the_chunk() -> None:
    codec = StreamedTextCodec(StreamedTextOptions(chunks_path="chunks", delta_path="delta"))
    with pytest.raises(CodecError) as exc:
        codec.decode_inbound([{"chunks": [{"delta": "{"}, {"text": "}"}]}])
    assert exc.value.error.details["reason"] == "path_not_found"
    assert exc.value.error.details["chunk"] == 1


def given_missing_chunks_path_when_decoded_then_path_not_found() -> None:
    codec = StreamedTextCodec(StreamedTextOptions(chunks_path="chunks"))
    with pytest.raises(CodecError) as exc:
        codec.decode_inbound([{"data": []}])
    assert exc.value.error.details["reason"] == "path_not_found"
    assert exc.value.error.details["path"] == "chunks"


def given_empty_chunk_list_when_decoded_then_no_json_found() -> None:
    with pytest.raises(CodecError) as exc:
        StreamedTextCodec().decode_inbound([[]])
    assert exc.value.error.details["reason"] == "no_json_found"


def given_outbound_text_when_encoded_then_canonical_json_else_the_envelope() -> None:
    envelope = _envelope("m-5")
    assert StreamedTextCodec().encode_outbound(envelope) is envelope
    text = StreamedTextCodec(StreamedTextOptions(outbound="text")).encode_outbound(envelope)
    assert text == canonical_json(envelope)


def given_malformed_path_option_when_validated_then_codec_options_invalid() -> None:
    with pytest.raises(ConfigError) as exc:
        CodecRegistry.validate(StreamedTextCodec, {"chunks_path": "a..b"})
    assert exc.value.error.error_code == "CODEC_OPTIONS_INVALID"
    assert exc.value.error.details["codec"] == "StreamedTextCodec"


# ================================================================================================
# the example configuration
# ================================================================================================
def given_example_config_when_loaded_then_both_plugins_resolved_and_options_validated() -> None:
    config = load_config(EXAMPLE_CONFIG, environ=dict(ENV), load_env_file=False)
    assert config.transport.provider == PROVIDER_PATH and config.transport.codec == CODEC_PATH
    provider = TransportRegistry.create(
        config, clock=FakeClock(), transport=httpx.MockTransport(AcmeServer())
    )
    assert isinstance(provider, AcmeHttpProvider)
    assert provider.settings == AcmeOptions(
        base_url="https://acme.example/api/v2", workspace="demo", api_key_env="ACME_API_KEY"
    )
    codec = CodecRegistry.create(config)
    assert isinstance(codec, StreamedTextCodec)
    assert codec.settings == StreamedTextOptions(
        chunks_path="chunks", delta_path="delta", outbound="text"
    )


def given_example_config_when_masked_then_nothing_secret_is_written() -> None:
    config = load_config(EXAMPLE_CONFIG, environ=dict(ENV), load_env_file=False)
    masked = config.masked()
    assert "k-secret" not in json.dumps(masked)
    # ADR-020 §5: any option under a key that looks like a secret (*key*) is masked, even when it
    # only holds the *name* of the variable — conservative by design (`transport show` output).
    assert masked["transport"]["options"]["api_key_env"] == "***"
    assert masked["transport"]["options"]["workspace"] == "demo"
    assert masked["transport"]["codec_options"] == {
        "chunks_path": "chunks",
        "delta_path": "delta",
        "outbound": "text",
    }
