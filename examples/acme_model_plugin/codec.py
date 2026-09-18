"""``StreamedTextCodec`` — a message codec for a model that answers in streamed chunks (guide 04).

The ACME model streams its answer: what the API hands back for one reply is not a text but a
**list of chunks**, each carrying a fragment (``{"chunks": [{"delta": "```json\\n{"}, {"delta":
"\\"type\\": ..."}, ...]}``). The protocol message only exists once the fragments are joined — and
then it is ordinary ``json_text`` material: JSON possibly wrapped in prose or a Markdown fence.

So the codec does exactly two things: concatenate, then reuse the text -> envelopes pipeline of the
built-in ``json_text`` codec (``strip_code_fence``, ``extract_first_json``, ``envelopes_of``).
Every impossibility is a ``CodecError`` (``MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY``) raised
through ``self.error(index, raw, reason, ...)``, which takes the excerpt of the raw item for the
failure record. The codec is pure: no I/O, no clock, trivially unit-tested.

Selected from ``config.toml`` without any registration::

    [transport]
    codec = "acme_model_plugin.codec:StreamedTextCodec"
    [transport.codec_options]
    chunks_path = "chunks"      # where the list of chunks is in a raw item (absent: the item is the list)
    delta_path = "delta"        # where the text is in a chunk (absent: the chunks are strings)
    outbound = "text"           # the envelope is posted as canonical JSON text ("object" by default)
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
    extract_first_json,
    strip_code_fence,
)
from agentic_local_app.transport.http_base import InvalidResponseError
from agentic_local_app.transport.providers.templated_http import extract_path, parse_path

__all__ = ["StreamedTextCodec", "StreamedTextOptions"]


def _validate_path(path: str | None) -> str | None:
    if path is not None:
        parse_path(path)  # a malformed path is refused when the options are validated
    return path


# ------------------------------------------------------------------------------------------------
# 1. the options: what the operator writes under [transport.codec_options]
# ------------------------------------------------------------------------------------------------
class StreamedTextOptions(BaseModel):
    """``transport.codec_options`` of :class:`StreamedTextCodec`."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    chunks_path: str | None = None
    delta_path: str | None = None
    id_path: str | None = None
    conversation_id_fallback: bool = True
    outbound: OutboundForm = "object"

    _paths = field_validator("chunks_path", "delta_path", "id_path")(_validate_path)


# ------------------------------------------------------------------------------------------------
# 2. the codec: raw items -> envelopes, envelope -> what is posted
# ------------------------------------------------------------------------------------------------
class StreamedTextCodec(MessageCodec):
    """Streamed chunks of text <-> protocol envelopes."""

    options_model: ClassVar[type[BaseModel] | None] = StreamedTextOptions
    name: ClassVar[str] = "streamed_text"  # ``details["codec"]`` of the errors it raises

    def __init__(self, options: BaseModel | None = None) -> None:
        settings = options if options is not None else StreamedTextOptions()
        if not isinstance(settings, StreamedTextOptions):
            raise TypeError(f"StreamedTextCodec expects StreamedTextOptions, got {type(settings)}")
        super().__init__(settings)
        self.settings: StreamedTextOptions = settings

    # ---- inbound ---------------------------------------------------------------------------
    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        envelopes: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_messages):
            text = self._join_chunks(index, raw)
            try:
                document = extract_first_json(strip_code_fence(text))
            except UnparseableTextError as exc:
                raise self.error(index, raw, exc.reason, **exc.details) from exc
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

    def _join_chunks(self, index: int, raw: Any) -> str:
        options = self.settings
        chunks: Any = raw
        if options.chunks_path is not None:
            try:
                chunks = extract_path(raw, options.chunks_path)
            except InvalidResponseError as exc:
                raise self.error(index, raw, "path_not_found", path=options.chunks_path) from exc
        if not isinstance(chunks, list):
            raise self.error(
                index, raw, "unexpected_type", path=options.chunks_path, expected="array"
            )
        fragments: list[str] = []
        for position, chunk in enumerate(chunks):
            fragment: Any = chunk
            if options.delta_path is not None:
                try:
                    fragment = extract_path(chunk, options.delta_path)
                except InvalidResponseError as exc:
                    raise self.error(
                        index, raw, "path_not_found", path=options.delta_path, chunk=position
                    ) from exc
            if not isinstance(fragment, str):
                raise self.error(index, raw, "unexpected_type", expected="string", chunk=position)
            fragments.append(fragment)
        if not fragments:
            raise self.error(index, raw, "no_json_found", chunks=0)
        return "".join(fragments)

    # ---- outbound --------------------------------------------------------------------------
    def encode_outbound(self, payload: dict[str, Any]) -> Any:
        return encode_payload(payload, self.settings.outbound)
