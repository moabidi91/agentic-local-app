"""Phase 7 — message codecs per model (ADR-021).

A model renders its replies in its own shape (bare text, chat completion, tool call...) and expects
its own input shape. A ``MessageCodec`` converts that raw shape into protocol envelopes (and the
outbound envelopes into what the transport must post); ``CodecTransport`` applies it around any
``TransportGateway`` transparently; ``CodecRegistry`` selects it by configuration exactly like the
transport providers (registered name, import path, entry point).

No network, no clock: the codecs are pure; the decorator is exercised on ``FakeTransportGateway``.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest
from pydantic import BaseModel, ConfigDict, Field
from typer.testing import CliRunner

from agentic_local_app.config import (
    DEFAULT_CODEC,
    AppConfig,
    AppSection,
    TransportSection,
    load_config,
)
from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import ConfigError, ErrorType, TransportError
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.interfaces.cli import app as cli_app
from agentic_local_app.orchestration import Application, build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.base import OP_GET, OP_POST, GetResult, PostAck, TransportGateway
from agentic_local_app.transport.codecs import (
    CODEC_ENTRY_POINT_GROUP,
    EXCERPT_CHARS,
    UNPARSEABLE_REPLY,
    CodecError,
    CodecRegistry,
    CodecTransport,
    JsonTextCodec,
    JsonTextOptions,
    MessageCodec,
    PassthroughCodec,
    ToolCallCodec,
    ToolCallOptions,
    apply_codec,
    excerpt_of,
)
from agentic_local_app.transport.codecs.json_text import extract_first_json, find_json_spans
from agentic_local_app.transport.fake import FakeTransportGateway
from agentic_local_app.transport.providers.generic_http import HttpTransportGateway
from agentic_local_app.transport.providers.templated_http import TemplatedHttpProvider
from agentic_local_app.transport.registry import (
    ENTRY_POINT_GROUP,
    ORIGIN_BUILTIN,
    ORIGIN_ENTRY_POINT,
    ORIGIN_IMPORT_PATH,
    TransportRegistry,
)

pytestmark = pytest.mark.phase7

RID = "remote-0001"


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def _envelope(message_id: str = "model-msg-0001", **content: Any) -> dict[str, Any]:
    return {
        "type": "final_answer",
        "conversation_id": RID,
        "message_id": message_id,
        "content": content or {"status": "completed", "diagnosis": "ok", "evidence": []},
    }


def _payload(message_id: str = "msg-0001") -> dict[str, Any]:
    return {
        "type": "user_request",
        "conversation_id": RID,
        "message_id": message_id,
        "content": {"goal": "g", "user_message": "m"},
    }


def _json_text(**options: Any) -> JsonTextCodec:
    return JsonTextCodec(JsonTextOptions(**options))


def _codec_error_of(codec: MessageCodec, raw: list[Any]) -> CodecError:
    with pytest.raises(CodecError) as exc:
        codec.decode_inbound(raw)
    return exc.value


def _config_error_of(call: Any) -> ConfigError:
    with pytest.raises(ConfigError) as exc:
        call()
    return exc.value


# ================================================================================================
# 1. configuration (additive keys of ADR-021)
# ================================================================================================
def given_default_transport_section_when_built_then_codec_passthrough_without_options() -> None:
    section = TransportSection()
    assert section.codec == DEFAULT_CODEC == "passthrough"
    assert section.codec_options == {}


def given_codec_and_options_in_toml_when_config_loaded_then_kept_verbatim(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        "\n".join(
            [
                "[transport]",
                'codec = "json_text"',
                "[transport.codec_options]",
                'content_path = "choices[0].message.content"',
                'outbound = "text"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    config = load_config(path, environ={}, load_env_file=False)
    assert config.transport.codec == "json_text"
    assert config.transport.codec_options == {
        "content_path": "choices[0].message.content",
        "outbound": "text",
    }


def given_codec_options_as_json_in_environment_when_config_loaded_then_parsed() -> None:
    config = load_config(
        None,
        environ={
            "AGENTIC__TRANSPORT__CODEC": "json_text",
            "AGENTIC__TRANSPORT__CODEC_OPTIONS": '{"strip_code_fences": false}',
        },
        load_env_file=False,
    )
    assert config.transport.codec == "json_text"
    assert config.transport.codec_options == {"strip_code_fences": False}


def given_blank_codec_when_config_loaded_then_config_invalid() -> None:
    error = _config_error_of(
        lambda: load_config(None, environ={"AGENTIC__TRANSPORT__CODEC": " "}, load_env_file=False)
    )
    assert error.error.error_code == "CONFIG_INVALID"
    assert any("codec" in str(p.get("loc")) for p in error.error.details["errors"])


def given_codec_options_with_secrets_when_masked_then_hidden_like_provider_options() -> None:
    config = AppConfig(
        transport=TransportSection(
            codec="json_text",
            codec_options={"content_path": "text", "api_key": "k", "tag": "${env:X}"},
        )
    )
    masked = config.masked()["transport"]
    assert masked["codec"] == "json_text"
    assert masked["codec_options"] == {"content_path": "text", "api_key": "***", "tag": "***"}


# ================================================================================================
# 2. the contract: MessageCodec, CodecError
# ================================================================================================
def given_message_codec_when_checked_then_abstract_with_identity_encoding() -> None:
    assert MessageCodec.options_model is None
    with pytest.raises(TypeError):
        MessageCodec()  # type: ignore[abstract]

    class _Decoder(MessageCodec):
        def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
            return [dict(m) for m in raw_messages]

    codec = _Decoder()
    assert codec.options is None
    payload = _payload()
    assert codec.encode_outbound(payload) is payload


def given_codec_error_when_built_then_transport_error_model_protocol_unparseable_with_details() -> (
    None
):
    error = CodecError(codec="json_text", index=2, excerpt="x" * 600, reason="no_json_found", k=1)
    assert isinstance(error, TransportError)
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == UNPARSEABLE_REPLY == "UNPARSEABLE_REPLY"
    assert error.error.retryable is False
    details = error.error.details
    assert details["codec"] == "json_text" and details["index"] == 2
    assert details["reason"] == "no_json_found" and details["k"] == 1
    assert details["excerpt"] == "x" * EXCERPT_CHARS and EXCERPT_CHARS == 500


def given_raw_forms_when_excerpt_taken_then_text_json_or_repr_cut_at_500() -> None:
    assert excerpt_of("x" * 600) == "x" * 500
    assert excerpt_of({"b": 1, "a": [True, None]}) == '{"a":[true,null],"b":1}'
    marker = object()
    assert excerpt_of(marker) == repr(marker)


def given_codec_error_when_details_added_then_new_error_keeps_the_original_ones() -> None:
    error = CodecError(codec="c", index=0, excerpt="e", reason="r")
    stamped = error.with_details(operation=OP_GET, http_status=200)
    assert isinstance(stamped, CodecError)
    assert stamped.error.details == {
        "codec": "c",
        "index": 0,
        "excerpt": "e",
        "reason": "r",
        "operation": OP_GET,
        "http_status": 200,
    }
    assert error.error.details == {"codec": "c", "index": 0, "excerpt": "e", "reason": "r"}


# ================================================================================================
# 3. passthrough — identity both ways
# ================================================================================================
def given_passthrough_when_decoding_then_same_envelopes_and_encoding_is_identity() -> None:
    codec = PassthroughCodec()
    messages = [_envelope("a"), _envelope("b")]
    decoded = codec.decode_inbound(messages)
    assert decoded == messages and decoded is not messages
    payload = _payload()
    assert codec.encode_outbound(payload) is payload
    assert PassthroughCodec.options_model is None and PassthroughCodec.name == "passthrough"


# ================================================================================================
# 4. json_text — text (bare, prose, fences) to envelopes
# ================================================================================================
def given_json_text_when_checked_then_options_defaults() -> None:
    options = JsonTextOptions()
    assert options.content_path is None and options.id_path is None
    assert options.strip_code_fences is True and options.extract_first_json_object is True
    assert options.conversation_id_fallback is True and options.outbound == "object"
    assert JsonTextCodec.options_model is JsonTextOptions and JsonTextCodec.name == "json_text"
    assert JsonTextCodec().options == JsonTextOptions()


def given_bare_json_string_when_decoded_then_one_envelope() -> None:
    assert _json_text().decode_inbound([json.dumps(_envelope())]) == [_envelope()]


def given_json_surrounded_by_prose_when_decoded_then_envelope_extracted() -> None:
    text = "Sure! Here is the plan you asked for:\n" + json.dumps(_envelope()) + "\nTell me more."
    assert _json_text().decode_inbound([text]) == [_envelope()]


@pytest.mark.parametrize(
    "text",
    [
        "```json\n" + json.dumps(_envelope(), indent=2) + "\n```",
        "Here you go:\n\n```\n" + json.dumps(_envelope()) + "\n```\n\nAnything else?",
        "```JSON\r\n" + json.dumps(_envelope()) + "\r\n```",
    ],
)
def given_markdown_fenced_json_when_decoded_then_envelope_extracted(text: str) -> None:
    assert _json_text().decode_inbound([text]) == [_envelope()]


def given_array_of_envelopes_when_decoded_then_every_envelope_in_order() -> None:
    text = "Two messages:\n" + json.dumps([_envelope("a"), _envelope("b")])
    assert _json_text().decode_inbound([text]) == [_envelope("a"), _envelope("b")]


def given_several_raw_items_when_decoded_then_envelopes_concatenated_in_order() -> None:
    raw = [json.dumps(_envelope("a")), "```json\n" + json.dumps([_envelope("b")]) + "\n```"]
    assert _json_text().decode_inbound(raw) == [_envelope("a"), _envelope("b")]


def given_empty_batch_when_decoded_then_empty_list() -> None:
    assert _json_text().decode_inbound([]) == []


def given_chat_completion_object_when_content_path_set_then_envelope_read_from_the_path() -> None:
    raw = {
        "id": "chatcmpl-1",
        "choices": [{"index": 0, "message": {"role": "model", "content": json.dumps(_envelope())}}],
    }
    codec = _json_text(content_path="choices[0].message.content")
    assert codec.decode_inbound([raw]) == [_envelope()]


def given_id_path_when_envelope_lacks_message_id_then_synthesised_from_the_raw_item() -> None:
    envelope = _envelope()
    del envelope["message_id"]
    raw = {"id": "chatcmpl-42", "choices": [{"message": {"content": json.dumps(envelope)}}]}
    codec = _json_text(content_path="choices[0].message.content", id_path="id")
    decoded = codec.decode_inbound([raw])
    assert decoded == [{**envelope, "message_id": "chatcmpl-42"}]


def given_id_path_when_envelope_has_message_id_then_kept() -> None:
    raw = {"id": "chatcmpl-42", "text": json.dumps(_envelope("model-msg-9"))}
    codec = _json_text(content_path="text", id_path="id")
    assert codec.decode_inbound([raw])[0]["message_id"] == "model-msg-9"


def given_numeric_id_at_id_path_when_synthesised_then_rendered_as_text() -> None:
    envelope = _envelope()
    del envelope["message_id"]
    codec = _json_text(content_path="text", id_path="id")
    assert codec.decode_inbound([{"id": 7, "text": json.dumps(envelope)}])[0]["message_id"] == "7"


@pytest.mark.parametrize("value", [True, {"nested": 1}, "", 1.5])
def given_id_path_to_non_identifier_when_message_id_missing_then_unparseable_unexpected_type(
    value: Any,
) -> None:
    envelope = _envelope()
    del envelope["message_id"]
    codec = _json_text(content_path="text", id_path="id")
    error = _codec_error_of(codec, [{"id": value, "text": json.dumps(envelope)}])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["path"] == "id" and error.error.details["expected"] == "identifier"


def given_options_of_another_codec_when_codec_built_then_type_error() -> None:
    with pytest.raises(TypeError):
        JsonTextCodec(ToolCallOptions())
    with pytest.raises(TypeError):
        ToolCallCodec(JsonTextOptions())


def given_id_path_absent_in_raw_item_when_message_id_missing_then_unparseable_with_path() -> None:
    envelope = _envelope()
    del envelope["message_id"]
    codec = _json_text(content_path="text", id_path="meta.id")
    error = _codec_error_of(codec, [{"text": json.dumps(envelope)}])
    assert error.error.error_code == UNPARSEABLE_REPLY
    assert error.error.details["reason"] == "path_not_found"
    assert error.error.details["path"] == "meta.id"


def given_envelope_without_conversation_id_when_fallback_on_then_left_as_is() -> None:
    envelope = _envelope()
    del envelope["conversation_id"]
    assert _json_text().decode_inbound([json.dumps(envelope)]) == [envelope]


def given_envelope_without_conversation_id_when_fallback_off_then_unparseable() -> None:
    envelope = _envelope()
    del envelope["conversation_id"]
    error = _codec_error_of(_json_text(conversation_id_fallback=False), [json.dumps(envelope)])
    assert error.error.details["reason"] == "missing_conversation_id"
    assert error.error.details["index"] == 0


def given_prose_without_json_when_decoded_then_unparseable_with_truncated_excerpt() -> None:
    text = "I cannot produce a plan right now. " * 30  # well over 500 characters
    error = _codec_error_of(_json_text(), [text])
    assert error.error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error.error_code == UNPARSEABLE_REPLY
    assert error.error.retryable is False
    details = error.error.details
    assert details["codec"] == "json_text" and details["index"] == 0
    assert details["reason"] == "no_json_found"
    assert details["excerpt"] == text[:EXCERPT_CHARS] and len(details["excerpt"]) == 500


def given_json_with_missing_closing_brace_when_decoded_then_unparseable_unbalanced() -> None:
    text = json.dumps(_envelope())[:-1]  # the last "}" is missing
    error = _codec_error_of(_json_text(), [text])
    assert error.error.error_code == UNPARSEABLE_REPLY
    assert error.error.details["reason"] == "json_unbalanced"
    assert error.error.details["excerpt"] == text


def given_balanced_but_invalid_json_when_decoded_then_unparseable_invalid_with_error() -> None:
    error = _codec_error_of(_json_text(), ["{type: final_answer}"])
    assert error.error.details["reason"] == "json_invalid"
    assert "Expecting property name" in error.error.details["error"]


def given_content_path_absent_when_decoded_then_unparseable_with_path() -> None:
    codec = _json_text(content_path="choices[0].message.content")
    error = _codec_error_of(codec, [{"choices": []}])
    assert error.error.error_code == UNPARSEABLE_REPLY
    assert error.error.details["reason"] == "path_not_found"
    assert error.error.details["path"] == "choices[0].message.content"
    assert error.error.details["excerpt"] == '{"choices":[]}'


def given_content_path_to_non_string_when_decoded_then_unparseable_unexpected_type() -> None:
    codec = _json_text(content_path="text")
    error = _codec_error_of(codec, [{"text": {"nested": 1}}])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["path"] == "text" and error.error.details["expected"] == "string"


def given_object_item_without_content_path_when_decoded_then_unparseable_unexpected_type() -> None:
    error = _codec_error_of(_json_text(), [_envelope()])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["expected"] == "string"
    assert error.error.details["excerpt"] == canonical_json(_envelope())


def given_json_scalar_when_decoded_then_unparseable_unexpected_type() -> None:
    error = _codec_error_of(_json_text(extract_first_json_object=False), ["42"])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["expected"] == "object or array"


def given_array_with_non_object_when_decoded_then_unparseable_unexpected_type() -> None:
    error = _codec_error_of(_json_text(), [json.dumps([_envelope(), "oops"])])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["expected"] == "object" and error.error.details["index"] == 0


def given_second_item_unparseable_when_decoded_then_index_of_that_item() -> None:
    error = _codec_error_of(_json_text(), [json.dumps(_envelope()), "no json here"])
    assert error.error.details["index"] == 1 and error.error.details["excerpt"] == "no json here"


def given_json_with_braces_inside_strings_when_decoded_then_balanced_extraction_correct() -> None:
    envelope = _envelope(
        status="completed", diagnosis='Use "{}" or "[x]" {literally} \\" ok', evidence=[]
    )
    text = "Look at {this} first: " + json.dumps(envelope) + " and then ]}"
    assert _json_text().decode_inbound([text]) == [envelope]


def given_prose_with_brackets_before_json_when_decoded_then_first_valid_json_object_used() -> None:
    text = "[note] see {below} -> " + json.dumps(_envelope()) + " {trailing}"
    assert _json_text().decode_inbound([text]) == [_envelope()]


def given_extraction_disabled_when_text_has_prose_then_unparseable_invalid() -> None:
    codec = _json_text(extract_first_json_object=False)
    error = _codec_error_of(codec, ["prose " + json.dumps(_envelope())])
    assert error.error.details["reason"] == "json_invalid"
    assert codec.decode_inbound(["  " + json.dumps(_envelope()) + "\n"]) == [_envelope()]


def given_fence_stripping_disabled_when_fenced_then_extraction_still_finds_the_object() -> None:
    codec = _json_text(strip_code_fences=False)
    assert codec.decode_inbound(["```json\n" + json.dumps(_envelope()) + "\n```"]) == [_envelope()]


def given_fence_stripping_and_extraction_disabled_when_fenced_then_unparseable() -> None:
    codec = _json_text(strip_code_fences=False, extract_first_json_object=False)
    error = _codec_error_of(codec, ["```json\n" + json.dumps(_envelope()) + "\n```"])
    assert error.error.details["reason"] == "json_invalid"


@pytest.mark.parametrize(
    ("text", "spans"),
    [
        ("", []),
        ("no brackets", []),
        ('{"a": 1}', [(0, 8)]),
        ('x {"a": "}"} y', [(2, 12)]),
        ('{"a": [1, 2]} [3]', [(0, 13), (14, 17)]),
        ('{"a": [1, 2}', []),
        ('{ oops {"a":1}', []),
        ('{"a": "\\"}"} tail', [(0, 12)]),
        ("[note] {bad} {}", [(0, 6), (7, 12), (13, 15)]),
    ],
)
def given_text_when_json_spans_found_then_top_level_balanced_spans_in_order(
    text: str, spans: list[tuple[int, int]]
) -> None:
    assert find_json_spans(text) == spans


def given_text_when_first_json_extracted_then_first_parsable_top_level_span() -> None:
    assert extract_first_json('[note] {bad} {"ok": true} {"later": 1}') == {"ok": True}
    assert extract_first_json("[1, 2] tail") == [1, 2]


def given_outbound_object_when_encoded_then_same_payload_object() -> None:
    payload = _payload()
    assert _json_text(outbound="object").encode_outbound(payload) is payload


def given_outbound_text_when_encoded_then_canonical_json_string() -> None:
    payload = _payload()
    encoded = _json_text(outbound="text").encode_outbound(payload)
    assert isinstance(encoded, str) and encoded == canonical_json(payload)
    assert json.loads(encoded) == payload


@pytest.mark.parametrize(
    "options",
    [
        {"outbound": "yaml"},
        {"content_path": "a..b"},
        {"id_path": "items[x]"},
        {"content_path": ""},
        {"surprise": 1},
    ],
)
def given_invalid_json_text_options_when_validated_then_rejected(options: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        JsonTextOptions(**options)


# ================================================================================================
# 4b. tool_call — the arguments of a tool call as the envelope
# ================================================================================================
def _tool_call(**options: Any) -> ToolCallCodec:
    return ToolCallCodec(ToolCallOptions(**options))


def _completion_call(
    arguments: Any, *, name: str = "send_message", call_id: str = "call-1"
) -> dict[str, Any]:
    return {
        "id": "chatcmpl-9",
        "choices": [
            {
                "message": {
                    "role": "model",
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {"name": name, "arguments": arguments},
                        }
                    ],
                }
            }
        ],
    }


CALL_ARGUMENTS = "choices[0].message.tool_calls[0].function.arguments"
CALL_NAME = "choices[0].message.tool_calls[0].function.name"
CALL_ID = "choices[0].message.tool_calls[0].id"


def given_tool_call_codec_when_checked_then_registered_with_defaults() -> None:
    options = ToolCallOptions()
    assert options.arguments_path == "arguments" and options.name_path is None
    assert options.tool_name is None and options.id_path is None
    assert options.conversation_id_fallback is True and options.outbound == "object"
    assert ToolCallCodec.options_model is ToolCallOptions and ToolCallCodec.name == "tool_call"
    assert CodecRegistry.resolve("tool_call") is ToolCallCodec


def given_tool_call_with_json_text_arguments_when_decoded_then_envelope() -> None:
    raw = {"name": "send_message", "arguments": json.dumps(_envelope())}
    assert _tool_call().decode_inbound([raw]) == [_envelope()]


def given_tool_call_with_object_arguments_when_decoded_then_envelope_copied() -> None:
    envelope = _envelope()
    raw = {"name": "send_message", "arguments": envelope}
    decoded = _tool_call().decode_inbound([raw])
    assert decoded == [envelope] and decoded[0] is not envelope


def given_chat_completion_tool_call_when_paths_configured_then_envelope_with_call_id() -> None:
    envelope = _envelope()
    del envelope["message_id"]
    codec = _tool_call(
        arguments_path=CALL_ARGUMENTS,
        name_path=CALL_NAME,
        tool_name="send_message",
        id_path=CALL_ID,
    )
    decoded = codec.decode_inbound([_completion_call(json.dumps(envelope), call_id="call-77")])
    assert decoded == [{**envelope, "message_id": "call-77"}]


def given_other_tool_called_when_tool_name_expected_then_unparseable_unexpected_tool() -> None:
    codec = _tool_call(arguments_path=CALL_ARGUMENTS, name_path=CALL_NAME, tool_name="send_message")
    error = _codec_error_of(codec, [_completion_call(json.dumps(_envelope()), name="search")])
    assert error.error.error_code == UNPARSEABLE_REPLY
    assert error.error.details["reason"] == "unexpected_tool"
    assert error.error.details["expected"] == "send_message"
    assert error.error.details["received"] == "search"


def given_tool_call_arguments_absent_when_decoded_then_unparseable_path_not_found() -> None:
    error = _codec_error_of(_tool_call(arguments_path=CALL_ARGUMENTS), [{"choices": []}])
    assert error.error.details["reason"] == "path_not_found"
    assert error.error.details["path"] == CALL_ARGUMENTS


def given_tool_call_arguments_invalid_json_when_decoded_then_unparseable_json_invalid() -> None:
    error = _codec_error_of(_tool_call(), [{"arguments": "{not json"}])
    assert error.error.details["reason"] == "json_invalid" and "error" in error.error.details


def given_tool_call_arguments_of_wrong_type_when_decoded_then_unparseable_unexpected_type() -> None:
    error = _codec_error_of(_tool_call(), [{"arguments": 42}])
    assert error.error.details["reason"] == "unexpected_type"
    assert error.error.details["expected"] == "string or object"


def given_raw_string_item_when_tool_call_decoded_then_unparseable_path_not_found() -> None:
    error = _codec_error_of(_tool_call(), ["just text"])
    assert error.error.details["reason"] == "path_not_found" and error.error.details["index"] == 0


def given_tool_call_outbound_text_when_encoded_then_canonical_json() -> None:
    assert _tool_call(outbound="text").encode_outbound(_payload()) == canonical_json(_payload())
    payload = _payload()
    assert _tool_call().encode_outbound(payload) is payload


# ================================================================================================
# 5. CodecTransport — the transparent decorator
# ================================================================================================
class _RecordingCodec(MessageCodec):
    """Marks what goes through it: decoded envelopes gain ``decoded``, payloads ``encoded``."""

    name: ClassVar[str] = "recording"

    def __init__(self, options: BaseModel | None = None, *, fail: bool = False) -> None:
        super().__init__(options)
        self.fail = fail
        self.decoded: list[list[Any]] = []
        self.encoded: list[dict[str, Any]] = []

    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        self.decoded.append(list(raw_messages))
        if self.fail:
            raise CodecError(codec=self.name, index=0, excerpt="raw", reason="unexpected_type")
        return [{**m, "decoded": True} for m in raw_messages]

    def encode_outbound(self, payload: dict[str, Any]) -> Any:
        self.encoded.append(payload)
        return {"wrapped": payload}


def _decorated(
    codec: MessageCodec | None = None, **fake_kwargs: Any
) -> tuple[CodecTransport, FakeTransportGateway]:
    fake = FakeTransportGateway(FakeClock(), **fake_kwargs)
    codec = codec if codec is not None else _RecordingCodec()
    return CodecTransport(fake, codec), fake


def given_codec_transport_when_built_then_transport_gateway_exposing_inner_and_codec() -> None:
    codec = _RecordingCodec()
    transport, fake = _decorated(codec)
    assert isinstance(transport, TransportGateway)
    assert transport.inner is fake and transport.codec is codec


async def given_codec_transport_when_init_then_delegated_unchanged() -> None:
    transport, fake = _decorated()
    remote = await transport.init_conversation("PROTO", {"session_id": "s-1"})
    assert remote == "remote-0001"
    assert fake.inits == [{"instructions": "PROTO", "metadata": {"session_id": "s-1"}}]


async def given_codec_transport_when_post_then_payload_encoded_before_the_inner_transport() -> None:
    codec = _RecordingCodec()
    transport, fake = _decorated(codec)
    ack = await transport.post_message(RID, _payload("msg-0007"))
    assert fake.posted == [(RID, {"wrapped": _payload("msg-0007")})]
    assert codec.encoded == [_payload("msg-0007")]
    # the fake saw no message_id in the encoded form: the ack keeps the protocol message_id
    assert ack == PostAck(message_id="msg-0007", accepted=True, http_status=202)


async def given_codec_failing_to_encode_when_post_then_codec_error_stamped_with_post() -> None:
    class _Refusing(_RecordingCodec):
        def encode_outbound(self, payload: dict[str, Any]) -> Any:
            raise CodecError(codec="refusing", index=0, excerpt="p", reason="unexpected_type")

    transport, fake = _decorated(_Refusing())
    with pytest.raises(CodecError) as exc:
        await transport.post_message(RID, _payload())
    assert exc.value.error.details["operation"] == OP_POST
    assert exc.value.error.details["http_status"] is None
    assert fake.posted == []


async def given_inner_ack_with_message_id_when_post_then_that_ack_returned_as_is() -> None:
    transport, fake = _decorated(PassthroughCodec())
    ack = await transport.post_message(RID, _payload("msg-0008"))
    assert ack == PostAck(message_id="msg-0008", accepted=True, http_status=202)
    assert fake.posted == [(RID, _payload("msg-0008"))]


async def given_codec_transport_when_get_messages_then_decoded_with_cursor_of_the_inner() -> None:
    codec = _RecordingCodec()
    transport, fake = _decorated(codec)
    fake.enqueue_messages(RID, [_envelope("a"), _envelope("b")])
    result = await transport.get_messages(RID, "c-0")
    assert result == GetResult(
        messages=[{**_envelope("a"), "decoded": True}, {**_envelope("b"), "decoded": True}],
        cursor="b",
        http_status=200,
    )
    assert codec.decoded == [[_envelope("a"), _envelope("b")]]
    assert fake.get_calls == [(RID, "c-0")]


async def given_empty_get_when_decoded_then_empty_result_with_cursor_unchanged() -> None:
    transport, _ = _decorated()
    assert await transport.get_messages(RID, "c-9") == GetResult([], "c-9", 200)


async def given_codec_transport_when_wait_for_reply_then_decoded() -> None:
    transport, fake = _decorated()
    fake.enqueue_messages(RID, [_envelope("x")])
    result = await transport.wait_for_reply(RID, None)
    assert result.messages == [{**_envelope("x"), "decoded": True}] and result.cursor == "x"


async def given_raw_strings_when_inner_cursor_not_advanced_then_cursor_is_last_decoded_id() -> None:
    transport, fake = _decorated(_json_text())
    fake.enqueue_messages(RID, [json.dumps(_envelope("model-msg-0002"))])
    result = await transport.wait_for_reply(RID, "model-msg-0001")
    assert result.messages == [_envelope("model-msg-0002")]
    assert result.cursor == "model-msg-0002"


async def given_inner_cursor_advanced_when_decoded_then_inner_cursor_kept() -> None:
    class _Paged(FakeTransportGateway):
        async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
            return GetResult([json.dumps(_envelope("m-2"))], "page-2", 200)

    transport = CodecTransport(_Paged(FakeClock()), _json_text())
    result = await transport.get_messages(RID, "page-1")
    assert result.cursor == "page-2" and result.messages == [_envelope("m-2")]


async def given_codec_failing_when_get_then_codec_error_stamped_with_operation_and_status() -> None:
    transport, fake = _decorated(_RecordingCodec(fail=True))
    fake.enqueue_messages(RID, [_envelope()])
    with pytest.raises(CodecError) as exc:
        await transport.wait_for_reply(RID, None)
    error = exc.value.error
    assert error.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert error.error_code == UNPARSEABLE_REPLY
    assert error.details["operation"] == OP_GET and error.details["http_status"] == 200
    assert error.details["codec"] == "recording" and error.details["excerpt"] == "raw"


async def given_reply_decoding_to_no_envelope_when_wait_for_reply_then_unparseable_no_envelope() -> (
    None
):
    transport, fake = _decorated(_json_text())
    fake.enqueue_messages(RID, ["Nothing to say: []"])
    with pytest.raises(CodecError) as exc:
        await transport.wait_for_reply(RID, None)
    details = exc.value.error.details
    assert details["reason"] == "no_envelope" and details["codec"] == "json_text"
    assert details["index"] == 0 and details["excerpt"] == "Nothing to say: []"
    assert details["operation"] == OP_GET and details["http_status"] == 200


async def given_reply_decoding_to_no_envelope_when_get_messages_then_empty_result() -> None:
    transport, fake = _decorated(_json_text())
    fake.enqueue_messages(RID, ["[]"])
    assert (await transport.get_messages(RID, "c-1")).messages == []


async def given_inner_transport_error_when_get_then_propagated_unchanged() -> None:
    transport, fake = _decorated()
    boom = TransportError(ErrorType.NETWORK_ERROR, "HTTP_503", retryable=True, operation=OP_GET)
    fake.enqueue_error("get", boom)
    with pytest.raises(TransportError) as exc:
        await transport.get_messages(RID, None)
    assert exc.value is boom


async def given_codec_transport_when_close_and_abandon_then_delegated() -> None:
    transport, fake = _decorated()
    await transport.close_conversation(RID)
    assert fake.closed == [RID]
    fake.hang_next("get")
    task = asyncio.ensure_future(transport.get_messages(RID, None))
    await fake.wait_until_hanging()
    transport.abandon()
    with pytest.raises(TransportError) as exc:
        await task
    assert exc.value.error.error_type is ErrorType.INTERRUPTED
    assert exc.value.error.error_code == "ABANDONED"


async def given_inner_with_aclose_when_decorator_aclosed_then_inner_closed() -> None:
    class _Closable(FakeTransportGateway):
        aclosed = False

        async def aclose(self) -> None:
            self.aclosed = True

    inner = _Closable(FakeClock())
    transport = CodecTransport(inner, PassthroughCodec())
    await transport.aclose()
    assert inner.aclosed is True
    await CodecTransport(FakeTransportGateway(), PassthroughCodec()).aclose()  # no-op without one


def given_passthrough_when_applied_then_bare_transport_else_decorated() -> None:
    fake = FakeTransportGateway()
    assert apply_codec(fake, PassthroughCodec()) is fake
    decorated = apply_codec(fake, _json_text())
    assert isinstance(decorated, CodecTransport) and decorated.inner is fake


# ================================================================================================
# 5b. the documented chat-completions setup: templated_http + json_text (outbound text)
# ================================================================================================
API = "https://api.example.com/v1"
KEY_ENV = "PHASE7_CODECS_MODEL_KEY"


def _chat_completions_transport_options() -> dict[str, Any]:
    return {
        "headers": {"Authorization": "Bearer ${env:" + KEY_ENV + "}"},
        "init": {
            "url": API + "/threads",
            "body": {"instructions": "{instructions}", "user": "{user_id}"},
            "conversation_id_path": "id",
        },
        "post": {
            "url": API + "/threads/{conversation_id}/chat/completions",
            "body": {"messages": [{"role": "user", "content": "{message_json}"}]},
        },
        "get": {
            "url": API + "/threads/{conversation_id}/completions?since={after}",
            "messages_path": "data",
            "cursor_path": "next_cursor",
        },
    }


async def given_chat_completions_config_of_the_adr_when_driven_then_text_posted_and_fenced_reply_decoded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(KEY_ENV, "k-secret")
    requests: list[httpx.Request] = []
    reply = "Here it is:\n```json\n" + json.dumps(_envelope("model-msg-0002"), indent=2) + "\n```"

    def server(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/threads"):
            return httpx.Response(201, json={"id": "th-1"})
        if request.url.path.endswith("/chat/completions"):
            return httpx.Response(200, json={"id": "chatcmpl-0", "status": "queued"})
        return httpx.Response(
            200,
            json={
                "data": [{"id": "chatcmpl-1", "choices": [{"message": {"content": reply}}]}],
                "next_cursor": "c-2",
            },
        )

    section = TransportSection(
        provider="templated_http",
        options=_chat_completions_transport_options(),
        codec="json_text",
        codec_options={"content_path": "choices[0].message.content", "outbound": "text"},
        token_env="PHASE7_CODECS_UNUSED_TOKEN",
        gzip=False,
    )
    config = AppConfig(transport=section)
    provider = TemplatedHttpProvider(section, FakeClock(), transport=httpx.MockTransport(server))
    transport = apply_codec(provider, CodecRegistry.create(config))
    assert isinstance(transport, CodecTransport)

    remote = await transport.init_conversation("PROTO", {"session_id": "s-1"})
    ack = await transport.post_message(remote, _payload("msg-0001"))
    result = await transport.wait_for_reply(remote, "c-1")

    assert remote == "th-1"
    assert json.loads(requests[1].content) == {
        "messages": [{"role": "user", "content": canonical_json(_payload("msg-0001"))}]
    }
    assert requests[1].headers["Authorization"] == "Bearer k-secret"
    assert ack == PostAck(message_id="msg-0001", accepted=True, http_status=200)
    assert str(requests[2].url) == API + "/threads/th-1/completions?since=c-1"
    assert result == GetResult(
        messages=[_envelope("model-msg-0002")], cursor="c-2", http_status=200
    )


# ================================================================================================
# 6. CodecRegistry — builtins, import path, entry point, errors
# ================================================================================================
class _UpperCodec(MessageCodec):
    """A codec living in the test module: selectable by import path without any other change."""

    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        return [{key.upper(): value for key, value in m.items()} for m in raw_messages]


class _AcmeOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    prefix: str = "acme"
    depth: int = Field(default=1, ge=0)


class _AcmeCodec(_UpperCodec):
    options_model: ClassVar[type[BaseModel] | None] = _AcmeOptions


class _NotACodec:
    """Something an import path could point to by mistake."""


CODEC_IMPORT_PATH = f"{__name__}:_UpperCodec"


class _FakeEntryPoint:
    def __init__(
        self, group: str, name: str, value: str, target: Any = None, error: Exception | None = None
    ) -> None:
        self.group = group
        self.name = name
        self.value = value
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
        group = kwargs["group"]
        assert group in {ENTRY_POINT_GROUP, CODEC_ENTRY_POINT_GROUP}
        return [point for point in points if point.group == group]

    monkeypatch.setattr("importlib.metadata.entry_points", fake_entry_points)


@pytest.fixture
def isolated_registries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(CodecRegistry, "_registered", CodecRegistry.registered())
    monkeypatch.setattr(TransportRegistry, "_registered", TransportRegistry.registered())
    _entry_points(monkeypatch)


def given_codecs_package_imported_when_names_listed_then_builtins_present(
    isolated_registries: None,
) -> None:
    assert {"passthrough", "json_text"} <= set(CodecRegistry.names())
    assert CodecRegistry.names() == sorted(CodecRegistry.names())
    assert CODEC_ENTRY_POINT_GROUP == "agentic_local_app.codecs"


@pytest.mark.parametrize(
    ("name", "expected"),
    [("passthrough", PassthroughCodec), ("json_text", JsonTextCodec)],
)
def given_builtin_codec_name_when_resolved_then_builtin_class(
    isolated_registries: None, name: str, expected: type[MessageCodec]
) -> None:
    assert CodecRegistry.resolve(name) is expected
    info, codec = CodecRegistry.describe(name)
    assert codec is expected and info.origin == ORIGIN_BUILTIN and info.name == name
    assert info.qualified_name == f"{expected.__module__}:{expected.__qualname__}"


def given_codec_registries_when_names_compared_then_independent_of_transport_providers(
    isolated_registries: None,
) -> None:
    assert "generic_http" not in CodecRegistry.names()
    assert "json_text" not in TransportRegistry.names()
    listed = {info.name: info for info in CodecRegistry.list_codecs()}
    assert listed["json_text"].origin == ORIGIN_BUILTIN
    assert listed["json_text"].qualified_name.endswith(":JsonTextCodec")


def given_import_path_when_codec_resolved_then_class_imported_without_registration(
    isolated_registries: None,
) -> None:
    assert CodecRegistry.resolve(CODEC_IMPORT_PATH) is _UpperCodec
    info, _ = CodecRegistry.describe(f" {CODEC_IMPORT_PATH} ")
    assert info.origin == ORIGIN_IMPORT_PATH and info.qualified_name == CODEC_IMPORT_PATH
    assert CODEC_IMPORT_PATH not in CodecRegistry.names()


def given_codec_entry_point_when_resolved_then_loaded_lazily_with_its_origin(
    monkeypatch: pytest.MonkeyPatch, isolated_registries: None
) -> None:
    point = _FakeEntryPoint(CODEC_ENTRY_POINT_GROUP, "acme_codec", "acme.codec:Acme", _AcmeCodec)
    transport_point = _FakeEntryPoint(ENTRY_POINT_GROUP, "acme_http", "acme.t:T", _NotACodec)
    _entry_points(monkeypatch, point, transport_point)

    assert "acme_codec" in CodecRegistry.names() and "acme_http" not in CodecRegistry.names()
    listed = {info.name: info for info in CodecRegistry.list_codecs()}
    assert listed["acme_codec"].origin == ORIGIN_ENTRY_POINT
    assert listed["acme_codec"].qualified_name == "acme.codec:Acme"
    assert point.loads == 0

    assert CodecRegistry.resolve("acme_codec") is _AcmeCodec
    assert point.loads == 1 and transport_point.loads == 0


def given_codec_entry_point_failing_to_load_when_resolved_then_codec_invalid(
    monkeypatch: pytest.MonkeyPatch, isolated_registries: None
) -> None:
    point = _FakeEntryPoint(
        CODEC_ENTRY_POINT_GROUP, "broken", "nope:X", error=ImportError("no module nope")
    )
    _entry_points(monkeypatch, point)
    error = _config_error_of(lambda: CodecRegistry.resolve("broken"))
    assert error.error.error_code == "CODEC_INVALID"
    assert error.error.details["codec"] == "broken"
    assert error.error.details["reason"] == "entry_point_load_failed"
    assert "no module nope" in error.error.details["error"]


def given_unknown_codec_when_resolved_then_codec_unknown_with_available_names(
    isolated_registries: None,
) -> None:
    error = _config_error_of(lambda: CodecRegistry.resolve("morse"))
    assert error.error.error_code == "CODEC_UNKNOWN"
    assert error.error.error_type is ErrorType.SYSTEM_ERROR
    assert error.error.details["codec"] == "morse"
    assert {"passthrough", "json_text"} <= set(error.error.details["available"])


def given_import_path_to_missing_module_when_codec_resolved_then_codec_unknown(
    isolated_registries: None,
) -> None:
    error = _config_error_of(lambda: CodecRegistry.resolve("no.such.module:Codec"))
    assert error.error.error_code == "CODEC_UNKNOWN"
    assert "ModuleNotFoundError" in error.error.details["error"]


@pytest.mark.parametrize(
    ("spec", "reason"),
    [
        (f"{__name__}:_NotACodec", "not a MessageCodec subclass"),
        (f"{__name__}:CODEC_IMPORT_PATH", "not a class"),
        ("agentic_local_app.transport.codecs.base:MessageCodec", "abstract class"),
        ("agentic_local_app.transport.fake:FakeTransportProvider", "not a MessageCodec subclass"),
    ],
)
def given_import_path_to_unusable_object_when_codec_resolved_then_codec_invalid(
    isolated_registries: None, spec: str, reason: str
) -> None:
    error = _config_error_of(lambda: CodecRegistry.resolve(spec))
    assert error.error.error_code == "CODEC_INVALID"
    assert error.error.details["codec"] == spec and error.error.details["reason"] == reason


def given_register_decorator_when_applied_then_codec_resolvable_and_class_unchanged(
    isolated_registries: None,
) -> None:
    decorated = CodecRegistry.register("upper")(_UpperCodec)
    assert decorated is _UpperCodec
    assert CodecRegistry.resolve("upper") is _UpperCodec
    assert "upper" in CodecRegistry.names() and "upper" not in TransportRegistry.names()
    CodecRegistry.register("upper")(_UpperCodec)  # idempotent for the same class
    with pytest.raises(ValueError, match="already registered"):
        CodecRegistry.register("upper")(_AcmeCodec)
    with pytest.raises(TypeError):
        CodecRegistry.register("bad")(_NotACodec)


def given_codec_without_options_model_when_created_with_options_then_codec_options_invalid(
    isolated_registries: None,
) -> None:
    config = AppConfig(transport=TransportSection(codec="passthrough", codec_options={"x": 1}))
    error = _config_error_of(lambda: CodecRegistry.create(config))
    assert error.error.error_code == "CODEC_OPTIONS_INVALID"
    assert error.error.details["codec"] == "PassthroughCodec"
    assert error.error.details["errors"] == [
        {
            "loc": ["x"],
            "msg": "unknown option: this codec takes no options",
            "type": "extra_forbidden",
        }
    ]


def given_json_text_with_invalid_options_when_created_then_codec_options_invalid_with_locations(
    isolated_registries: None,
) -> None:
    config = AppConfig(
        transport=TransportSection(
            codec="json_text", codec_options={"outbound": "yaml", "content_path": "a..b", "x": 1}
        )
    )
    error = _config_error_of(lambda: CodecRegistry.create(config))
    assert error.error.error_code == "CODEC_OPTIONS_INVALID"
    assert error.error.details["codec"] == "JsonTextCodec"
    locations = {tuple(problem["loc"]) for problem in error.error.details["errors"]}
    assert locations == {("outbound",), ("content_path",), ("x",)}
    assert all({"loc", "msg", "type"} == set(problem) for problem in error.error.details["errors"])


def given_json_text_with_valid_options_when_created_then_codec_carries_validated_options(
    isolated_registries: None,
) -> None:
    config = AppConfig(
        transport=TransportSection(
            codec="json_text", codec_options={"content_path": "text", "outbound": "text"}
        )
    )
    codec = CodecRegistry.create(config)
    assert isinstance(codec, JsonTextCodec)
    assert codec.options == JsonTextOptions(content_path="text", outbound="text")


def given_import_path_codec_with_options_model_when_created_then_options_validated(
    isolated_registries: None,
) -> None:
    good = AppConfig(
        transport=TransportSection(codec=f"{__name__}:_AcmeCodec", codec_options={"prefix": "p"})
    )
    codec = CodecRegistry.create(good)
    assert isinstance(codec, _AcmeCodec) and codec.options == _AcmeOptions(prefix="p")
    bad = AppConfig(
        transport=TransportSection(codec=f"{__name__}:_AcmeCodec", codec_options={"depth": -1})
    )
    error = _config_error_of(lambda: CodecRegistry.create(bad))
    assert error.error.error_code == "CODEC_OPTIONS_INVALID"
    assert error.error.details["errors"][0]["loc"] == ["depth"]


def given_transport_registry_when_options_invalid_then_provider_error_codes_unchanged(
    isolated_registries: None,
) -> None:
    config = AppConfig(transport=TransportSection(provider="fake", options={"x": 1}))
    error = _config_error_of(lambda: TransportRegistry.create(config, clock=FakeClock()))
    assert error.error.error_code == "TRANSPORT_OPTIONS_INVALID"
    assert error.error.details["provider"] == "FakeTransportProvider"
    assert (
        error.error.details["errors"][0]["msg"] == "unknown option: this provider takes no options"
    )


# ================================================================================================
# 7. wiring — build_application applies the configured (or injected) codec
# ================================================================================================
def _app_config(tmp_path: Path, **transport: Any) -> AppConfig:
    return AppConfig(
        app=AppSection(data_dir=str(tmp_path / "data")),
        transport=TransportSection(**transport),
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


def given_default_codec_when_application_built_then_transport_not_wrapped(
    tmp_path: Path, isolated_registries: None
) -> None:
    app = _build(_app_config(tmp_path))
    try:
        assert isinstance(app.transport, HttpTransportGateway)
        assert not isinstance(app.transport, CodecTransport)
    finally:
        app.close()


def given_json_text_codec_in_config_when_application_built_then_transport_wrapped(
    tmp_path: Path, isolated_registries: None
) -> None:
    app = _build(
        _app_config(
            tmp_path,
            provider="fake",
            codec="json_text",
            codec_options={"content_path": "choices[0].message.content"},
        )
    )
    try:
        assert isinstance(app.transport, CodecTransport)
        assert isinstance(app.transport.inner, FakeTransportGateway)
        assert isinstance(app.transport.codec, JsonTextCodec)
        assert app.transport.codec.options == JsonTextOptions(
            content_path="choices[0].message.content"
        )
        assert app.orchestrator.transport is app.transport  # type: ignore[attr-defined]
    finally:
        app.close()


def given_injected_transport_and_configured_codec_when_built_then_injected_transport_wrapped(
    tmp_path: Path, isolated_registries: None
) -> None:
    fake = FakeTransportGateway()
    app = _build(_app_config(tmp_path, provider="nope", codec="json_text"), transport=fake)
    try:
        assert isinstance(app.transport, CodecTransport) and app.transport.inner is fake
    finally:
        app.close()


def given_injected_codec_when_application_built_then_used_whatever_the_config_says(
    tmp_path: Path, isolated_registries: None
) -> None:
    codec = _UpperCodec()
    app = _build(_app_config(tmp_path, provider="fake", codec="nope"), codec=codec)
    try:
        assert isinstance(app.transport, CodecTransport) and app.transport.codec is codec
    finally:
        app.close()


def given_injected_passthrough_codec_when_application_built_then_transport_not_wrapped(
    tmp_path: Path, isolated_registries: None
) -> None:
    fake = FakeTransportGateway()
    app = _build(_app_config(tmp_path), transport=fake, codec=PassthroughCodec())
    try:
        assert app.transport is fake
    finally:
        app.close()


def given_unknown_codec_when_application_built_then_codec_unknown_before_wiring(
    tmp_path: Path, isolated_registries: None
) -> None:
    error = _config_error_of(lambda: _build(_app_config(tmp_path, provider="fake", codec="morse")))
    assert error.error.error_code == "CODEC_UNKNOWN"


def given_passthrough_with_options_when_application_built_then_codec_options_invalid(
    tmp_path: Path, isolated_registries: None
) -> None:
    error = _config_error_of(
        lambda: _build(_app_config(tmp_path, provider="fake", codec_options={"x": 1}))
    )
    assert error.error.error_code == "CODEC_OPTIONS_INVALID"


# ================================================================================================
# 8. CLI — codec list · codec show · transport show
# ================================================================================================
PASSTHROUGH_CLASS = "agentic_local_app.transport.codecs.passthrough:PassthroughCodec"
JSON_TEXT_CLASS = "agentic_local_app.transport.codecs.json_text:JsonTextCodec"


def _config_file(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text("\n".join(["[transport]", *lines, ""]), encoding="utf-8")
    return path


def given_cli_when_codec_list_then_builtin_codecs_with_class_and_origin() -> None:
    result = CliRunner().invoke(cli_app, ["codec", "list"])
    assert result.exit_code == 0, result.output
    rows = {line.split()[0]: line for line in result.output.splitlines() if line.strip()}
    assert "passthrough" in rows and PASSTHROUGH_CLASS in rows["passthrough"]
    assert "json_text" in rows and JSON_TEXT_CLASS in rows["json_text"]
    assert all("builtin" in rows[name] for name in ("passthrough", "json_text"))


def given_cli_when_codec_list_json_then_machine_readable_sorted_rows() -> None:
    result = CliRunner().invoke(cli_app, ["codec", "list", "--json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert [row["name"] for row in rows] == sorted(row["name"] for row in rows)
    by_name = {row["name"]: row for row in rows}
    assert by_name["json_text"] == {
        "name": "json_text",
        "class": JSON_TEXT_CLASS,
        "origin": "builtin",
    }
    assert by_name["passthrough"]["class"] == PASSTHROUGH_CLASS


def given_default_config_when_codec_show_then_passthrough_without_options(tmp_path: Path) -> None:
    path = _config_file(tmp_path)
    result = CliRunner().invoke(cli_app, ["codec", "show", "--config", str(path)])
    assert result.exit_code == 0, result.output
    assert "codec: passthrough" in result.output
    assert f"class: {PASSTHROUGH_CLASS}" in result.output
    assert "origin: builtin" in result.output
    assert "options_model: -" in result.output
    assert "options: {}" in result.output


def given_json_text_config_when_codec_show_json_then_document_with_masked_options(
    tmp_path: Path,
) -> None:
    path = _config_file(
        tmp_path,
        'codec = "json_text"',
        "[transport.codec_options]",
        'content_path = "choices[0].message.content"',
        'outbound = "text"',
    )
    result = CliRunner().invoke(cli_app, ["codec", "show", "--json", "--config", str(path)])
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document == {
        "codec": "json_text",
        "class": JSON_TEXT_CLASS,
        "origin": "builtin",
        "options_model": "JsonTextOptions",
        "options": {"content_path": "choices[0].message.content", "outbound": "text"},
    }


def given_unknown_codec_when_codec_show_then_exit_1_with_available_names(tmp_path: Path) -> None:
    path = _config_file(tmp_path, 'codec = "morse"')
    result = CliRunner().invoke(cli_app, ["codec", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "CODEC_UNKNOWN" in result.output
    assert "morse" in result.output and "json_text" in result.output


def given_invalid_codec_options_when_codec_show_then_exit_1_with_location(tmp_path: Path) -> None:
    path = _config_file(tmp_path, 'codec = "json_text"', 'codec_options = { outbound = "yaml" }')
    result = CliRunner().invoke(cli_app, ["codec", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "CODEC_OPTIONS_INVALID" in result.output and "outbound" in result.output


def given_json_text_config_when_transport_show_then_effective_codec_displayed(
    tmp_path: Path,
) -> None:
    path = _config_file(
        tmp_path,
        'provider = "fake"',
        'codec = "json_text"',
        'codec_options = { content_path = "text" }',
    )
    result = CliRunner().invoke(cli_app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 0, result.output
    assert "provider: fake" in result.output
    assert "codec: json_text" in result.output
    assert f"codec_class: {JSON_TEXT_CLASS}" in result.output
    assert "codec_options_model: JsonTextOptions" in result.output
    assert '"content_path": "text"' in result.output

    as_json = CliRunner().invoke(cli_app, ["transport", "show", "--json", "--config", str(path)])
    assert as_json.exit_code == 0, as_json.output
    document = json.loads(as_json.stdout)
    assert document["provider"] == "fake"
    assert document["codec"] == {
        "codec": "json_text",
        "class": JSON_TEXT_CLASS,
        "origin": "builtin",
        "options_model": "JsonTextOptions",
        "options": {"content_path": "text"},
    }
    assert document["transport"]["codec"] == "json_text"
    assert document["transport"]["codec_options"] == {"content_path": "text"}


def given_default_config_when_transport_show_then_passthrough_codec_displayed(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        cli_app, ["transport", "show", "--config", str(_config_file(tmp_path))]
    )
    assert result.exit_code == 0, result.output
    assert "codec: passthrough" in result.output


def given_unknown_codec_when_transport_show_then_exit_1_codec_unknown(tmp_path: Path) -> None:
    path = _config_file(tmp_path, 'codec = "morse"')
    result = CliRunner().invoke(cli_app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "CODEC_UNKNOWN" in result.output


# ================================================================================================
# 9. fake gateway — raw items and text payloads (what a codec puts through it)
# ================================================================================================
async def given_fake_with_raw_string_items_when_get_then_items_returned_and_cursor_unchanged() -> (
    None
):
    fake = FakeTransportGateway()
    fake.enqueue_messages(RID, ["raw text one", "raw text two"])
    result = await fake.wait_for_reply(RID, "c-1")
    assert result.messages == ["raw text one", "raw text two"] and result.cursor == "c-1"


async def given_fake_when_text_payload_posted_then_recorded_with_empty_ack_message_id() -> None:
    fake = FakeTransportGateway()
    ack = await fake.post_message(RID, canonical_json(_payload()))  # type: ignore[arg-type]
    assert fake.posted == [(RID, canonical_json(_payload()))]
    assert ack == PostAck(message_id="", accepted=True, http_status=202)


# ================================================================================================
# 10. hygiene
# ================================================================================================
def given_codecs_package_when_inspected_then_public_surface_exported() -> None:
    from agentic_local_app import transport
    from agentic_local_app.transport import codecs

    for name in (
        "CODEC_ENTRY_POINT_GROUP",
        "EXCERPT_CHARS",
        "UNPARSEABLE_REPLY",
        "CodecError",
        "CodecRegistry",
        "CodecTransport",
        "JsonTextCodec",
        "JsonTextOptions",
        "MessageCodec",
        "PassthroughCodec",
        "apply_codec",
    ):
        assert name in codecs.__all__ and getattr(codecs, name) is not None
    for name in ("CodecError", "CodecRegistry", "CodecTransport", "MessageCodec"):
        assert name in transport.__all__ and getattr(transport, name) is not None
    assert OP_POST == "POST"
