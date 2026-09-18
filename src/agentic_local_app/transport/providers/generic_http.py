"""``generic_http`` — the ADR-004 contract over :class:`HttpProviderBase` (ADR-020).

Contract (ADR-004):

- three configurable endpoints (``init_url``, ``post_url``, ``get_url``) plus an optional
  ``close_url``; URL templates accept the ``{conversation_id}`` and ``{after}`` placeholders, both
  percent-encoded when substituted;
- **init**: ``POST init_url`` with ``{"user_id", "instructions", "metadata"}`` -> ``200``/``201``
  ``{"conversation_id"}``;
- **post**: ``POST post_url`` with the complete protocol message -> ``200``/``202``
  ``{"accepted": true, "message_id"}`` (``accepted: false`` -> ``POST_NOT_ACCEPTED``);
- **get**: ``GET get_url`` -> ``200 {"messages": [...], "cursor"}``; a missing cursor is derived
  from the last message's ``message_id``;
- **close** (option): ``close_url`` with the method ``transport.close_method`` (``POST`` by
  default, ``DELETE`` otherwise), any 2xx accepted, body ignored; an empty ``close_url`` means a
  local close only.

Headers, gzip, polling, the HTTP -> ``ErrorType`` table and ``abandon()`` are inherited unchanged.
``HttpTransportGateway`` is the historical name of this class and stays importable from
:mod:`agentic_local_app.transport.gateway`; no option is accepted (``options_model = None``).
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from agentic_local_app.transport.base import OP_CLOSE, OP_GET, OP_INIT, OP_POST, GetResult, PostAck
from agentic_local_app.transport.http_base import HttpCall, HttpProviderBase, InvalidResponseError
from agentic_local_app.transport.registry import TransportRegistry

__all__ = ["GenericHttpProvider", "HttpTransportGateway"]

INIT_STATUSES: frozenset[int] = frozenset({200, 201})
POST_STATUSES: frozenset[int] = frozenset({200, 202})
GET_STATUSES: frozenset[int] = frozenset({200})
CLOSE_STATUSES: frozenset[int] = frozenset(range(200, 300))


@TransportRegistry.register("generic_http")
class GenericHttpProvider(HttpProviderBase):
    """The ADR-004 endpoints and response shapes; the reference implementation of the contract."""

    def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
        body = {
            "user_id": self._config.user_id,
            "instructions": instructions,
            "metadata": metadata,
        }
        url = format_url(self._config.init_url)
        return HttpCall("POST", url, self.headers(OP_INIT), body, INIT_STATUSES)

    def parse_init(self, status: int, body: Any) -> str:
        if not isinstance(body, dict) or not _non_empty_str(body.get("conversation_id")):
            raise InvalidResponseError(reason="missing_or_invalid:conversation_id")
        return str(body["conversation_id"])

    def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
        url = format_url(self._config.post_url, remote_conversation_id)
        return HttpCall("POST", url, self.headers(OP_POST), payload, POST_STATUSES)

    def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("accepted"), bool)
            or not _non_empty_str(body.get("message_id"))
        ):
            raise InvalidResponseError(reason="missing_or_invalid:accepted,message_id")
        if body["accepted"] is not True:
            raise InvalidResponseError(
                "POST_NOT_ACCEPTED", message_id=body["message_id"], reason=body.get("reason")
            )
        return PostAck(message_id=str(body["message_id"]), accepted=True, http_status=status)

    def build_get(self, remote_conversation_id: str, after: str | None) -> HttpCall:
        url = format_url(self._config.get_url, remote_conversation_id, after)
        return HttpCall("GET", url, self.headers(OP_GET), None, GET_STATUSES)

    def parse_get(self, status: int, body: Any) -> GetResult:
        if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
            raise InvalidResponseError(reason="missing_or_invalid:messages")
        messages: list[Any] = body["messages"]
        if not all(isinstance(message, dict) for message in messages):
            raise InvalidResponseError(reason="messages_items_not_objects")
        cursor = body.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise InvalidResponseError(reason="invalid:cursor")
        if cursor is None and messages:
            last_id = messages[-1].get("message_id")
            cursor = last_id if _non_empty_str(last_id) else None
        return GetResult(messages=list(messages), cursor=cursor, http_status=status)

    def build_close(self, remote_conversation_id: str) -> HttpCall | None:
        if not self._config.close_url:
            return None
        url = format_url(self._config.close_url, remote_conversation_id)
        return HttpCall(
            self._config.close_method,
            url,
            self.headers(OP_CLOSE),
            None,
            CLOSE_STATUSES,
            parse_json=False,
        )


#: Historical name of the ADR-004 provider (module map §3); the same class, not a subclass, so that
#: ``isinstance`` checks hold whichever name the caller uses.
HttpTransportGateway = GenericHttpProvider


def format_url(template: str, conversation_id: str | None = None, after: str | None = None) -> str:
    """Fill the ``{conversation_id}`` / ``{after}`` placeholders of an ADR-004 URL template
    (percent-encoded; a missing cursor gives an empty ``after``)."""
    url = template
    if conversation_id is not None:
        url = url.replace("{conversation_id}", quote(conversation_id, safe=""))
    return url.replace("{after}", quote(after or "", safe=""))


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)
