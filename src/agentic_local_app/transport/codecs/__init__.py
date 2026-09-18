"""Message codecs (ADR-021): the raw shape of a model's replies <-> protocol envelopes.

A transport provider (ADR-020) speaks an API; a **codec** speaks a model. It is chosen by
``transport.codec`` through the :class:`~agentic_local_app.transport.codecs.registry.CodecRegistry`
(registered name, import path ``package.module:ClassName`` or entry point of the
``agentic_local_app.codecs`` group) and applied around any ``TransportGateway`` by the
:class:`~agentic_local_app.transport.codecs.decorator.CodecTransport` decorator:

- ``passthrough`` — :class:`~agentic_local_app.transport.codecs.passthrough.PassthroughCodec`,
  identity both ways, the default (the transport is used bare);
- ``json_text`` — :class:`~agentic_local_app.transport.codecs.json_text.JsonTextCodec`, text
  carrying the JSON of the message (bare, in prose, in a Markdown fence), optionally read at a
  path of an object (a chat completion), posted as an object or as canonical JSON text;
- ``tool_call`` — :class:`~agentic_local_app.transport.codecs.tool_call.ToolCallCodec`, the
  arguments of a tool call (JSON text or object at a path) as the envelope.

A reply the codec cannot read is a ``CodecError``: ``MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY``,
never retried, with ``codec``, ``index``, ``excerpt`` and ``reason`` in its details. Importing this
package registers the built-in codecs.
"""

from agentic_local_app.transport.codecs.base import (
    EXCERPT_CHARS,
    UNPARSEABLE_REPLY,
    CodecError,
    MessageCodec,
    excerpt_of,
)
from agentic_local_app.transport.codecs.decorator import CodecTransport, apply_codec
from agentic_local_app.transport.codecs.json_text import JsonTextCodec, JsonTextOptions
from agentic_local_app.transport.codecs.passthrough import PassthroughCodec
from agentic_local_app.transport.codecs.registry import CODEC_ENTRY_POINT_GROUP, CodecRegistry
from agentic_local_app.transport.codecs.tool_call import ToolCallCodec, ToolCallOptions

__all__ = [
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
    "ToolCallCodec",
    "ToolCallOptions",
    "apply_codec",
    "excerpt_of",
]
