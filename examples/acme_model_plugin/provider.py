"""``AcmeHttpProvider`` — a transport provider for the fictional *ACME Threads* API (guide 03).

The API differs from the ADR-004 contract on every point a real API usually does:

- **authentication** by an ``X-Api-Key`` header whose value lives in an environment variable named
  by the options (read at call time, never stored), plus an ``X-Workspace`` header;
- **URLs** built from a base URL and a workspace segment, not from ``transport.init_url`` & co;
- **body shapes** of its own: ``{"thread": {"id": ...}}`` at init, ``{"event": {"id": ...}}`` at
  post, ``{"events": [{"kind": "message", "payload": ...}, ...], "next": ...}`` at get;
- a **close** by ``DELETE`` answering ``204`` without a body;
- a **throttling** reply ``429 {"error": {"code": "throttled", "retry_in_ms": 1500}}`` carrying the
  delay in the body rather than in a ``Retry-After`` header.

Everything HTTP that does *not* depend on those shapes — the ``httpx`` client and its timeouts,
gzip, the polling of ``wait_for_reply``, ``abandon()``, the HTTP -> ``ErrorType`` table, the
``details`` of every error — comes from ``HttpProviderBase``. This file only **describes** the
four calls (``build_*``) and **reads** the four replies (``parse_*``), overriding ``headers`` for
the authentication and ``classify_error`` for the one reply the base cannot know about.

Selected from ``config.toml`` without any registration::

    [transport]
    provider = "acme_model_plugin.provider:AcmeHttpProvider"
    [transport.options]
    base_url = "https://acme.example/api/v2"
    workspace = "demo"
    api_key_env = "ACME_API_KEY"        # the key is read from this variable at call time
    page_size = 50
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, ClassVar, cast
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ConfigError, ErrorType, TransportError
from agentic_local_app.transport.base import OP_CLOSE, OP_GET, OP_INIT, OP_POST, GetResult, PostAck
from agentic_local_app.transport.http_base import HttpCall, HttpProviderBase, InvalidResponseError

__all__ = ["ENV_MISSING_CODE", "AcmeHttpProvider", "AcmeOptions"]

#: The ``ConfigError`` code of a missing API key — the same as ``templated_http`` uses for a missing
#: ``${env:VAR}``, so that one troubleshooting table covers both.
ENV_MISSING_CODE = "TRANSPORT_ENV_MISSING"
_INIT_STATUSES: frozenset[int] = frozenset({200, 201})
_POST_STATUSES: frozenset[int] = frozenset({200, 202})
_GET_STATUSES: frozenset[int] = frozenset({200})
_CLOSE_STATUSES: frozenset[int] = frozenset({200, 204})
_EVENT_KIND_MESSAGE = "message"


# ------------------------------------------------------------------------------------------------
# 1. the options: what the operator writes under [transport.options]
# ------------------------------------------------------------------------------------------------
class AcmeOptions(BaseModel):
    """``transport.options`` of :class:`AcmeHttpProvider`; ``extra = "forbid"`` so that a typo is a
    ``TRANSPORT_OPTIONS_INVALID`` at start-up instead of a silent default."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    base_url: str = Field(min_length=1)
    workspace: str = Field(min_length=1)
    api_key_env: str = "ACME_API_KEY"
    page_size: int = Field(default=50, ge=1, le=500)

    @field_validator("base_url")
    @classmethod
    def _without_trailing_slash(cls, value: str) -> str:
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return value


# ------------------------------------------------------------------------------------------------
# 2. the provider: four calls described, four replies read
# ------------------------------------------------------------------------------------------------
class AcmeHttpProvider(HttpProviderBase):
    """The ACME Threads dialect over :class:`HttpProviderBase`."""

    options_model: ClassVar[type[BaseModel] | None] = AcmeOptions

    def __init__(
        self,
        config: TransportSection,
        clock: Clock,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        # the base validates ``config.options`` against ``options_model`` and builds the client
        super().__init__(config, clock, transport=transport, sleep=sleep)
        self.settings: AcmeOptions = cast(AcmeOptions, self.options)
        self._environ: Mapping[str, str] = os.environ if environ is None else environ

    # ---- authentication: one hook, every call ---------------------------------------------
    def headers(self, operation: str) -> dict[str, str]:
        """``X-Api-Key`` from the environment (at call time) and the workspace; nothing else
        identifying is sent, so the ADR-004 bearer token never leaks to this API."""
        key = self._environ.get(self.settings.api_key_env, "").strip()
        if not key:
            raise ConfigError(
                ENV_MISSING_CODE, variable=self.settings.api_key_env, operation=operation
            )
        return {
            "Accept": "application/json",
            "X-Api-Key": key,
            "X-Workspace": self.settings.workspace,
        }

    # ---- init: POST /workspaces/{ws}/threads -> 201 {"thread": {"id": "thr_..."}} ----------
    def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
        url = (
            f"{self.settings.base_url}/workspaces/{quote(self.settings.workspace, safe='')}/threads"
        )
        body = {"system": instructions, "owner": self._config.user_id, "labels": metadata}
        return HttpCall("POST", url, self.headers(OP_INIT), body, _INIT_STATUSES)

    def parse_init(self, status: int, body: Any) -> str:
        thread = body.get("thread") if isinstance(body, dict) else None
        thread_id = thread.get("id") if isinstance(thread, dict) else None
        if not isinstance(thread_id, str) or not thread_id:
            raise InvalidResponseError(reason="path_not_found", path="thread.id")
        return thread_id

    # ---- post: POST /threads/{id}/events -> 202 {"event": {"id": "evt_..."}} ---------------
    def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
        # ``payload`` is the protocol envelope — or its text form when a codec posts text
        # (ADR-021); the API takes either as the event payload.
        body = {"kind": _EVENT_KIND_MESSAGE, "payload": payload}
        return HttpCall(
            "POST",
            self._thread_url(remote_conversation_id, "/events"),
            self.headers(OP_POST),
            body,
            _POST_STATUSES,
        )

    def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
        event = body.get("event") if isinstance(body, dict) else None
        if not isinstance(event, dict) or not event.get("id"):
            raise InvalidResponseError(reason="path_not_found", path="event.id")
        # ADR-004: the acknowledgement names the *protocol* message. The API only knows its own
        # event id, so the message_id is the one we sent; empty for a text payload — the codec
        # decorator then restores it from the envelope.
        message_id = payload.get("message_id") if isinstance(payload, dict) else None
        return PostAck(
            message_id=str(message_id) if message_id else "", accepted=True, http_status=status
        )

    # ---- get: GET /threads/{id}/events?after=...&limit=... -> 200 {"events": [...], "next"} --
    def build_get(self, remote_conversation_id: str, after: str | None) -> HttpCall:
        query = f"?after={quote(after or '', safe='')}&limit={self.settings.page_size}"
        return HttpCall(
            "GET",
            self._thread_url(remote_conversation_id, "/events") + query,
            self.headers(OP_GET),
            None,
            _GET_STATUSES,
        )

    def parse_get(self, status: int, body: Any) -> GetResult:
        events = body.get("events") if isinstance(body, dict) else None
        if not isinstance(events, list):
            raise InvalidResponseError(reason="path_not_found", path="events")
        messages: list[dict[str, Any]] = []
        for index, event in enumerate(events):
            if not isinstance(event, dict):
                raise InvalidResponseError(
                    reason="unexpected_type", path=f"events[{index}]", expected="object"
                )
            if event.get("kind") != _EVENT_KIND_MESSAGE:
                continue  # "status", "typing"... events carry no protocol message
            payload = event.get("payload")
            if not isinstance(payload, dict):
                raise InvalidResponseError(
                    reason="unexpected_type", path=f"events[{index}].payload", expected="object"
                )
            messages.append(payload)
        cursor = body.get("next")
        if cursor is not None and not isinstance(cursor, str):
            raise InvalidResponseError(reason="unexpected_type", path="next", expected="string")
        return GetResult(messages=messages, cursor=cursor or None, http_status=status)

    # ---- close: DELETE /threads/{id} -> 204 (no body) --------------------------------------
    def build_close(self, remote_conversation_id: str) -> HttpCall | None:
        return HttpCall(
            "DELETE",
            self._thread_url(remote_conversation_id),
            self.headers(OP_CLOSE),
            None,
            _CLOSE_STATUSES,
            parse_json=False,
        )

    # ---- the one reply the base cannot know about --------------------------------------------
    def classify_error(
        self, operation: str, status: int, body: str, headers: Mapping[str, str]
    ) -> TransportError:
        """ACME throttles with ``429 {"error": {"code": "throttled", "retry_in_ms": N}}``: keep the
        base's classification (RATE_LIMIT_ERROR, retryable) and add the delay it asks for."""
        error = super().classify_error(operation, status, body, headers)
        if status == 429:
            retry_in_ms = _retry_in_ms(body)
            if retry_in_ms is not None:
                normalized = error.error
                return TransportError(
                    ErrorType.RATE_LIMIT_ERROR,
                    normalized.error_code,
                    retryable=True,
                    **{**normalized.details, "retry_after_ms": retry_in_ms},
                )
        return error

    # ---- helpers -------------------------------------------------------------------------------
    def _thread_url(self, remote_conversation_id: str, suffix: str = "") -> str:
        return f"{self.settings.base_url}/threads/{quote(remote_conversation_id, safe='')}{suffix}"


def _retry_in_ms(body: str) -> int | None:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    error = parsed.get("error") if isinstance(parsed, dict) else None
    value = error.get("retry_in_ms") if isinstance(error, dict) else None
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None
