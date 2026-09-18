"""``passthrough`` — the identity codec (ADR-021), the default.

The transport already yields protocol envelopes (the ADR-004 contract, or a ``templated_http``
configuration whose ``messages_path`` / ``message_path`` point at the envelopes) and posts the
envelope objects as they are. Selecting this codec leaves the transport bare: the wiring never
wraps it (:func:`~agentic_local_app.transport.codecs.decorator.apply_codec`). No option accepted.
"""

from __future__ import annotations

from typing import Any, ClassVar

from agentic_local_app.transport.codecs.base import MessageCodec
from agentic_local_app.transport.codecs.registry import CodecRegistry

__all__ = ["PassthroughCodec"]


@CodecRegistry.register("passthrough")
class PassthroughCodec(MessageCodec):
    """Identity both ways: the raw items are the envelopes, the envelope is what is posted."""

    name: ClassVar[str] = "passthrough"

    def decode_inbound(self, raw_messages: list[Any]) -> list[dict[str, Any]]:
        return list(raw_messages)
