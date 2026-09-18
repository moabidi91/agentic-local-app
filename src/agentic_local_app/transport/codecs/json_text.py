"""``json_text`` — the model answers with text that contains the JSON of the message (ADR-021).

Each raw item of a GET is either a string, or an object (a chat completion, for instance) whose
``content_path`` gives the string. The string may carry the JSON of the message **surrounded by
prose or Markdown fences** — the codec strips the first fence (``strip_code_fences``) and extracts
the first balanced JSON object or array (``extract_first_json_object``, strings and escapes
respected, a candidate that is not valid JSON is skipped). The JSON is one envelope (an object) or
an array of envelopes. Options::

    [transport]
    codec = "json_text"
    [transport.codec_options]
    content_path = "choices[0].message.content"   # optional: the raw items are objects, not strings
    strip_code_fences = true                       # the first ```json ... ``` (or ``` ... ```) block
    extract_first_json_object = true               # false: the whole text must be JSON
    id_path = "id"                                 # optional: message_id taken there when missing
    conversation_id_fallback = true                # false: a missing conversation_id is refused here
    outbound = "object"                            # "text": the envelope is posted as canonical JSON

The codec never invents a ``conversation_id``: with ``conversation_id_fallback`` (the default) an
envelope without one is handed to the ``ProtocolAdapter`` as is, which rejects it as
``SCHEMA_INVALID`` (the reply is then persisted and counted); without it the codec refuses the
reply itself (``missing_conversation_id``). Every impossibility is a
``CodecError(UNPARSEABLE_REPLY)`` whose ``reason`` is one of ``path_not_found``,
``unexpected_type``, ``no_json_found``, ``json_unbalanced``, ``json_invalid``,
``missing_conversation_id`` (plus ``path``, ``expected`` or ``error`` when relevant).
"""

from __future__ import annotations

import json
import re
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, field_validator

from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.transport.codecs.base import MessageCodec
from agentic_local_app.transport.codecs.registry import CodecRegistry
from agentic_local_app.transport.http_base import InvalidResponseError
from agentic_local_app.transport.providers.templated_http import extract_path, parse_path

__all__ = [
    "MAX_JSON_CANDIDATES",
    "JsonTextCodec",
    "JsonTextOptions",
    "OutboundForm",
    "UnparseableTextError",
    "complete_envelope",
    "encode_payload",
    "envelopes_of",
    "extract_first_json",
    "find_json_spans",
    "parse_strict",
    "strip_code_fence",
]

#: The forms ``encode_outbound`` can hand to the transport (``outbound`` option).
OutboundForm = Literal["object", "text"]

#: Balanced spans tried at most by :func:`extract_first_json` (bounds pathological texts).
MAX_JSON_CANDIDATES = 64
_CLOSER_OF = {"{": "}", "[": "]"}
_CLOSERS = frozenset(_CLOSER_OF.values())
_FENCE_RE = re.compile(r"```[ \t]*[A-Za-z0-9_+-]*[ \t]*\r?\n?(.*?)```", re.S)


class UnparseableTextError(Exception):
    """Why a text carries no usable JSON; the codecs turn it into a ``CodecError``."""

    def __init__(self, reason: str, **details: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.details = details


# ------------------------------------------------------------------------------------------------
# text -> JSON
# ------------------------------------------------------------------------------------------------
def strip_code_fence(text: str) -> str:
    """The content of the first Markdown fenced block of ``text`` (any language tag or none), or
    ``text`` itself when it has no fence."""
    match = _FENCE_RE.search(text)
    return match.group(1) if match is not None else text


def find_json_spans(text: str) -> list[tuple[int, int]]:
    """The top-level balanced ``{...}`` / ``[...]`` spans of ``text``, in order, as ``(start, end)``
    slices. Brackets inside a JSON string (quotes and escapes respected) do not count; a stray
    closer discards everything opened before it; an opener never closed yields nothing."""
    spans: list[tuple[int, int]] = []
    stack: list[tuple[str, int]] = []
    in_string = False
    escaped = False
    for position, char in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = bool(stack)  # quotes only matter inside a candidate
        elif char in _CLOSER_OF:
            stack.append((_CLOSER_OF[char], position))
        elif char in _CLOSERS:
            if stack and stack[-1][0] == char:
                _, start = stack.pop()
                if not stack:
                    spans.append((start, position + 1))
            else:
                stack.clear()
    return spans


def extract_first_json(text: str) -> Any:
    """The first top-level balanced span of ``text`` that parses as JSON. Raises ``UnparseableTextError``
    with ``no_json_found`` (no bracket at all), ``json_unbalanced`` (no balanced span) or
    ``json_invalid`` (no span parses; ``error`` is the message of the first one tried)."""
    spans = find_json_spans(text)
    if not spans:
        if any(char in text for char in _CLOSER_OF):
            raise UnparseableTextError("json_unbalanced")
        raise UnparseableTextError("no_json_found")
    first_error: str | None = None
    for start, end in spans[:MAX_JSON_CANDIDATES]:
        try:
            return json.loads(text[start:end])
        except (ValueError, RecursionError) as exc:
            if first_error is None:
                first_error = str(exc)
    raise UnparseableTextError("json_invalid", error=first_error)


def parse_strict(text: str) -> Any:
    """``text`` (stripped) as JSON, or ``UnparseableTextError(json_invalid)``."""
    try:
        return json.loads(text.strip())
    except (ValueError, RecursionError) as exc:
        raise UnparseableTextError("json_invalid", error=str(exc)) from exc


# ------------------------------------------------------------------------------------------------
# options
# ------------------------------------------------------------------------------------------------
def _validate_path(path: str | None) -> str | None:
    if path is not None:
        parse_path(path)
    return path


class JsonTextOptions(BaseModel):
    """``transport.codec_options`` of the ``json_text`` codec."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    content_path: str | None = None
    strip_code_fences: bool = True
    extract_first_json_object: bool = True
    id_path: str | None = None
    conversation_id_fallback: bool = True
    outbound: OutboundForm = "object"

    _paths = field_validator("content_path", "id_path")(_validate_path)


# ------------------------------------------------------------------------------------------------
# the codec
# ------------------------------------------------------------------------------------------------
@CodecRegistry.register("json_text")
class JsonTextCodec(MessageCodec):
    """Text carrying JSON (bare, in prose, in a Markdown fence) <-> protocol envelopes."""

    options_model: ClassVar[type[BaseModel] | None] = JsonTextOptions
    name: ClassVar[str] = "json_text"

    def __init__(self, options: BaseModel | None = None) -> None:
        settings = options if options is not None else JsonTextOptions()
        if not isinstance(settings, JsonTextOptions):
            raise TypeError(f"JsonTextCodec expects JsonTextOptions, got {type(settings).__name__}")
        super().__init__(settings)
        #: The typed view of :attr:`options`.
        self.settings: JsonTextOptions = settings

    # ------------------------------------------------------------------ inbound ------------
    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        envelopes: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_messages):
            text = self._text_of(index, raw)
            document = self._json_of(index, raw, text)
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

    def _text_of(self, index: int, raw: Any) -> str:
        options = self.settings
        if options.content_path is None:
            if not isinstance(raw, str):
                raise self.error(index, raw, "unexpected_type", expected="string")
            return raw
        try:
            value = extract_path(raw, options.content_path)
        except InvalidResponseError as exc:
            raise self.error(index, raw, "path_not_found", path=options.content_path) from exc
        if not isinstance(value, str):
            raise self.error(
                index, raw, "unexpected_type", path=options.content_path, expected="string"
            )
        return value

    def _json_of(self, index: int, raw: Any, text: str) -> Any:
        options = self.settings
        if options.strip_code_fences:
            text = strip_code_fence(text)
        try:
            if options.extract_first_json_object:
                return extract_first_json(text)
            return parse_strict(text)
        except UnparseableTextError as exc:
            raise self.error(index, raw, exc.reason, **exc.details) from exc

    # ------------------------------------------------------------------ outbound -----------
    def encode_outbound(self, payload: dict[str, Any]) -> Any:
        return encode_payload(payload, self.settings.outbound)


# ------------------------------------------------------------------------------------------------
# shared by the codecs producing envelopes from a JSON document
# ------------------------------------------------------------------------------------------------
def envelopes_of(
    codec: MessageCodec,
    index: int,
    raw: Any,
    document: Any,
    *,
    id_path: str | None,
    conversation_id_fallback: bool,
) -> list[dict[str, Any]]:
    """The envelopes of a decoded JSON ``document`` (one object, or an array of objects), each
    completed by :func:`complete_envelope`; anything else is ``unexpected_type``."""
    if isinstance(document, dict):
        items: list[Any] = [document]
    elif isinstance(document, list):
        items = document
    else:
        raise codec.error(index, raw, "unexpected_type", expected="object or array")
    envelopes: list[dict[str, Any]] = []
    for element, item in enumerate(items):
        if not isinstance(item, dict):
            raise codec.error(index, raw, "unexpected_type", expected="object", element=element)
        envelopes.append(
            complete_envelope(
                codec,
                index,
                raw,
                dict(item),
                id_path=id_path,
                conversation_id_fallback=conversation_id_fallback,
            )
        )
    return envelopes


def complete_envelope(
    codec: MessageCodec,
    index: int,
    raw: Any,
    envelope: dict[str, Any],
    *,
    id_path: str | None,
    conversation_id_fallback: bool,
) -> dict[str, Any]:
    """A missing ``message_id`` is taken at ``id_path`` of the raw item (a non-empty string or an
    integer, rendered as text); a missing ``conversation_id`` is left to the adapter unless
    ``conversation_id_fallback`` is off (``missing_conversation_id``). Never invents anything."""
    if _missing(envelope.get("message_id")) and id_path is not None:
        try:
            value = extract_path(raw, id_path)
        except InvalidResponseError as exc:
            raise codec.error(index, raw, "path_not_found", path=id_path) from exc
        if isinstance(value, bool) or not isinstance(value, str | int) or _missing(value):
            raise codec.error(index, raw, "unexpected_type", path=id_path, expected="identifier")
        envelope["message_id"] = str(value)
    if _missing(envelope.get("conversation_id")) and not conversation_id_fallback:
        raise codec.error(index, raw, "missing_conversation_id")
    return envelope


def encode_payload(payload: dict[str, Any], outbound: str) -> Any:
    """The ``outbound`` forms: ``object`` (the envelope itself) or ``text`` (canonical JSON)."""
    return canonical_json(payload) if outbound == "text" else payload


def _missing(value: Any) -> bool:
    return value is None or value == ""
