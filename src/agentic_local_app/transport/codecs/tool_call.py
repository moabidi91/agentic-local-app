"""``tool_call`` — the model answers by calling a tool whose arguments are the message (ADR-021).

Each raw item of a GET is an object carrying a tool call: its ``arguments`` (JSON text, as the
chat-completion APIs render them, or an object already decoded) are the protocol envelope — or an
array of envelopes. Options::

    [transport]
    codec = "tool_call"
    [transport.codec_options]
    arguments_path = "choices[0].message.tool_calls[0].function.arguments"   # default: "arguments"
    name_path = "choices[0].message.tool_calls[0].function.name"             # optional
    tool_name = "send_message"        # optional: with name_path, any other tool is refused
    id_path = "choices[0].message.tool_calls[0].id"   # optional: message_id when missing
    conversation_id_fallback = true   # as json_text
    outbound = "object"               # "text": the envelope is posted as canonical JSON

Errors are ``CodecError(UNPARSEABLE_REPLY)`` with the reasons of ``json_text`` plus
``unexpected_tool`` (``expected`` / ``received``) when the called tool is not ``tool_name``.
"""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, field_validator

from agentic_local_app.transport.codecs.base import MessageCodec
from agentic_local_app.transport.codecs.json_text import (
    OutboundForm,
    UnparseableTextError,
    encode_payload,
    envelopes_of,
    parse_strict,
)
from agentic_local_app.transport.codecs.registry import CodecRegistry
from agentic_local_app.transport.http_base import InvalidResponseError
from agentic_local_app.transport.providers.templated_http import extract_path, parse_path

__all__ = ["ToolCallCodec", "ToolCallOptions"]


def _validate_path(path: str | None) -> str | None:
    if path is not None:
        parse_path(path)
    return path


class ToolCallOptions(BaseModel):
    """``transport.codec_options`` of the ``tool_call`` codec."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    arguments_path: str = "arguments"
    name_path: str | None = None
    tool_name: str | None = None
    id_path: str | None = None
    conversation_id_fallback: bool = True
    outbound: OutboundForm = "object"

    _paths = field_validator("arguments_path", "name_path", "id_path")(_validate_path)


@CodecRegistry.register("tool_call")
class ToolCallCodec(MessageCodec):
    """The arguments of a tool call (JSON text or object at a path) <-> protocol envelopes."""

    options_model: ClassVar[type[BaseModel] | None] = ToolCallOptions
    name: ClassVar[str] = "tool_call"

    def __init__(self, options: BaseModel | None = None) -> None:
        settings = options if options is not None else ToolCallOptions()
        if not isinstance(settings, ToolCallOptions):
            raise TypeError(f"ToolCallCodec expects ToolCallOptions, got {type(settings).__name__}")
        super().__init__(settings)
        #: The typed view of :attr:`options`.
        self.settings: ToolCallOptions = settings

    # ------------------------------------------------------------------ inbound ------------
    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        envelopes: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_messages):
            self._check_tool(index, raw)
            document = self._arguments_of(index, raw)
            envelopes.extend(
                envelopes_of(
                    self,
                    index,
                    raw,
                    document,
                    id_path=self.settings.id_path,
                    conversation_id_fallback=self.settings.conversation_id_fallback,
                )
            )
        return envelopes

    def _check_tool(self, index: int, raw: Any) -> None:
        options = self.settings
        if options.name_path is None or options.tool_name is None:
            return
        received = self._at(index, raw, options.name_path)
        if received != options.tool_name:
            raise self.error(
                index,
                raw,
                "unexpected_tool",
                path=options.name_path,
                expected=options.tool_name,
                received=received
                if isinstance(received, str | int | float | bool)
                else str(received),
            )

    def _arguments_of(self, index: int, raw: Any) -> Any:
        path = self.settings.arguments_path
        arguments = self._at(index, raw, path)
        if isinstance(arguments, dict):
            return arguments
        if isinstance(arguments, str):
            try:
                return parse_strict(arguments)
            except UnparseableTextError as exc:
                raise self.error(index, raw, exc.reason, path=path, **exc.details) from exc
        raise self.error(index, raw, "unexpected_type", path=path, expected="string or object")

    def _at(self, index: int, raw: Any, path: str) -> Any:
        try:
            return extract_path(raw, path)
        except InvalidResponseError as exc:
            raise self.error(index, raw, "path_not_found", path=path) from exc

    # ------------------------------------------------------------------ outbound -----------
    def encode_outbound(self, payload: dict[str, Any]) -> Any:
        return encode_payload(payload, self.settings.outbound)
