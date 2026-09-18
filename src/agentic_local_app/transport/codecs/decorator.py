"""``CodecTransport`` — a ``TransportGateway`` decorated with a ``MessageCodec`` (ADR-021).

The decorator is transparent for the orchestrator, the rotation and the recovery: it is a
``TransportGateway`` like the provider it wraps, and it only touches the messages —

- ``post_message(remote, payload)`` posts ``codec.encode_outbound(payload)``; when the provider
  cannot read a ``message_id`` from what it posted (a text form, for instance) its acknowledgement
  carries an empty one, which the decorator replaces by the protocol ``message_id`` (ADR-004: the
  acknowledgement names the message sent);
- ``get_messages`` / ``wait_for_reply`` decode ``result.messages`` with ``codec.decode_inbound``
  and keep the provider's cursor, unless it did not advance (``None`` or still ``after``): a
  provider reading raw items cannot derive the ADR-004 cursor, so it becomes the ``message_id`` of
  the last decoded envelope;
- a :class:`~agentic_local_app.transport.codecs.base.CodecError` raised by the codec is stamped
  with ``operation`` and ``http_status`` like every transport error, then propagates; a reply
  ``wait_for_reply`` waited for that decodes to **no** envelope at all is unparseable too
  (``no_envelope``), since the contract promises at least one message;
- ``init_conversation``, ``close_conversation``, ``abandon`` and ``aclose`` (when the provider has
  one) are delegated as they are.

:func:`apply_codec` is what the wiring calls: the ``passthrough`` codec leaves the transport bare.
"""

from __future__ import annotations

import asyncio
from typing import Any

from agentic_local_app.transport.base import OP_GET, OP_POST, GetResult, PostAck, TransportGateway
from agentic_local_app.transport.codecs.base import CodecError, MessageCodec
from agentic_local_app.transport.codecs.passthrough import PassthroughCodec

__all__ = ["CodecTransport", "apply_codec"]


class CodecTransport(TransportGateway):
    """The transport ``inner`` seen through ``codec``."""

    def __init__(self, inner: TransportGateway, codec: MessageCodec) -> None:
        self._inner = inner
        self._codec = codec

    @property
    def inner(self) -> TransportGateway:
        return self._inner

    @property
    def codec(self) -> MessageCodec:
        return self._codec

    # ------------------------------------------------------------------ gateway ------------
    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        return await self._inner.init_conversation(instructions, metadata)

    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        try:
            encoded = self._codec.encode_outbound(payload)
        except CodecError as exc:
            raise exc.with_details(operation=OP_POST, http_status=None) from exc
        ack = await self._inner.post_message(remote_conversation_id, encoded)
        if ack.message_id:
            return ack
        message_id = payload.get("message_id")
        return PostAck(
            message_id=str(message_id) if message_id is not None else "",
            accepted=ack.accepted,
            http_status=ack.http_status,
        )

    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return self._decode(await self._inner.get_messages(remote_conversation_id, after), after)

    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        """The provider waited for at least one raw item; it must carry at least one envelope,
        otherwise the reply is unparseable (``no_envelope``)."""
        result = await self._inner.wait_for_reply(remote_conversation_id, after)
        decoded = self._decode(result, after)
        if result.messages and not decoded.messages:
            error = self._codec.error(0, result.messages[0], "no_envelope")
            raise error.with_details(operation=OP_GET, http_status=result.http_status)
        return decoded

    async def close_conversation(self, remote_conversation_id: str) -> None:
        await self._inner.close_conversation(remote_conversation_id)

    def abandon(self) -> None:
        self._inner.abandon()

    async def aclose(self) -> None:
        """Release the wrapped provider's resources when it has an ``aclose`` (HTTP client)."""
        aclose = getattr(self._inner, "aclose", None)
        if callable(aclose):
            result: Any = aclose()
            if asyncio.iscoroutine(result):
                await result

    # ------------------------------------------------------------------ internals ----------
    def _decode(self, result: GetResult, after: str | None) -> GetResult:
        try:
            messages = self._codec.decode_inbound(result.messages)
        except CodecError as exc:
            raise exc.with_details(operation=OP_GET, http_status=result.http_status) from exc
        cursor = result.cursor
        if cursor is None or cursor == after:
            last_id = messages[-1].get("message_id") if messages else None
            if isinstance(last_id, str) and last_id:
                cursor = last_id
        return GetResult(messages=messages, cursor=cursor, http_status=result.http_status)


def apply_codec(transport: TransportGateway, codec: MessageCodec) -> TransportGateway:
    """``transport`` decorated with ``codec`` — or bare when the codec is the identity."""
    if isinstance(codec, PassthroughCodec):
        return transport
    return CodecTransport(transport, codec)
