"""The message codec contract (ADR-021): :class:`MessageCodec` and :class:`CodecError`.

A transport provider (ADR-020) speaks an API's HTTP dialect; a **codec** speaks a *model's* shape.
The transport yields what the API returned for each message (an item of ``GetResult.messages``:
a protocol envelope when the API is the ADR-004 contract, otherwise raw text, a chat completion
object, a tool call...) and the codec turns that raw shape into the protocol envelopes the
:class:`~agentic_local_app.protocol.adapter.ProtocolAdapter` validates — and, on the way out, the
protocol envelope into what the transport must post. The codec is pure: no I/O, no clock.

A codec is instantiated by the registry as ``cls(options)`` where ``options`` is the validated
``transport.codec_options`` (an ``options_model`` instance) or ``None``. Decoding failures are a
:class:`CodecError`: a ``TransportError`` of type ``MODEL_PROTOCOL_ERROR`` and code
``UNPARSEABLE_REPLY``, never retried (§7.2), whose details always carry ``codec``, the ``index`` of
the raw item, an ``excerpt`` of its raw form (at most :data:`EXCERPT_CHARS` characters) and a
``reason`` code — enough for a correction policy to quote the reply back to the model.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar

from pydantic import BaseModel

from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.errors import ErrorType, TransportError

__all__ = ["EXCERPT_CHARS", "UNPARSEABLE_REPLY", "CodecError", "MessageCodec", "excerpt_of"]

#: ``error_code`` of every decoding failure (``error_type`` is ``MODEL_PROTOCOL_ERROR``).
UNPARSEABLE_REPLY = "UNPARSEABLE_REPLY"
#: Maximum length of ``details["excerpt"]``: the beginning of the raw form that failed.
EXCERPT_CHARS = 500


def excerpt_of(raw: Any) -> str:
    """The raw form as text, cut at :data:`EXCERPT_CHARS`: a string as is, anything else as
    canonical JSON (or its ``repr`` when it is not JSON-serialisable)."""
    if isinstance(raw, str):
        text = raw
    else:
        try:
            text = canonical_json(raw)
        except (TypeError, ValueError):
            text = repr(raw)
    return text[:EXCERPT_CHARS]


class CodecError(TransportError):
    """A raw reply the codec cannot turn into protocol envelopes (``UNPARSEABLE_REPLY``).

    Raised from the transport boundary (the decorator is a ``TransportGateway``), hence the
    ``TransportGateway`` origin: the orchestrator handles it like any transport failure.
    """

    def __init__(
        self, *, codec: str, index: int, excerpt: str, reason: str, **details: Any
    ) -> None:
        super().__init__(
            ErrorType.MODEL_PROTOCOL_ERROR,
            UNPARSEABLE_REPLY,
            retryable=False,
            codec=codec,
            index=index,
            excerpt=excerpt[:EXCERPT_CHARS],
            reason=reason,
            **details,
        )

    def with_details(self, **more: Any) -> CodecError:
        """The same error with ``more`` merged into its details (``operation``, ``http_status``)."""
        merged: dict[str, Any] = {**self.error.details, **more}
        return CodecError(**merged)


class MessageCodec(ABC):
    """The conversion between a model's raw shape and the protocol envelopes (ADR-021).

    ``options_model`` (a pydantic model or ``None``) validates ``transport.codec_options``; without
    it no option is accepted. ``name`` is what the codec calls itself in error details (the
    registered name for the built-in codecs); empty means the class name.
    """

    options_model: ClassVar[type[BaseModel] | None] = None
    name: ClassVar[str] = ""

    def __init__(self, options: BaseModel | None = None) -> None:
        #: The validated ``transport.codec_options`` (an ``options_model`` instance) or ``None``.
        self.options = options

    @property
    def label(self) -> str:
        """``details["codec"]`` of the errors this codec raises."""
        return self.name or type(self).__name__

    @abstractmethod
    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        """The protocol envelopes carried by the raw items of one GET, in order (one raw item may
        carry several envelopes, or none). Raises :class:`CodecError` when an item cannot be read."""

    def encode_outbound(self, payload: dict[str, Any]) -> Any:
        """What the transport must post for the protocol envelope ``payload`` (default: itself)."""
        return payload

    def error(self, index: int, raw: Any, reason: str, **details: Any) -> CodecError:
        """A :class:`CodecError` for the raw item at ``index`` (its excerpt is taken here)."""
        return CodecError(
            codec=self.label, index=index, excerpt=excerpt_of(raw), reason=reason, **details
        )
