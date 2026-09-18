"""``TransportGateway`` — the only component that talks to the remote model (§2.1, §3.12, ADR-004).

Contract (ADR-004):

- three configurable endpoints (``init_url``, ``post_url``, ``get_url``) plus an optional
  ``close_url``; URL templates accept the ``{conversation_id}`` and ``{after}`` placeholders, both
  percent-encoded when substituted;
- ``X-User-Id`` on every request, ``Authorization: Bearer <token>`` when the token is present in the
  environment (read at call time, never stored, never logged), ``Accept: application/json``, and for
  bodies ``Content-Type: application/json`` with ``Content-Encoding: gzip`` when ``gzip`` is on;
- outbound bodies use the canonical JSON serialisation (ADR-017); the gzip header carries
  ``mtime=0`` so that identical payloads produce identical bytes;
- ``get_messages`` performs **one** GET; ``wait_for_reply`` polls every ``poll_interval_ms`` until a
  message arrives or ``reply_timeout_ms`` elapses on the injected clock (``MODEL_GET_TIMEOUT``);
- every failure is surfaced as a :class:`~agentic_local_app.domain.errors.TransportError` whose
  ``error_type`` follows the HTTP mapping table of ADR-004 (see :meth:`HttpTransportGateway._map_status`)
  and whose ``details`` always carry ``operation``, ``http_status`` and a token-free ``url``;
- ``abandon()`` (synchronous) cancels every in-flight call, which then raises
  ``TransportError(INTERRUPTED, "ABANDONED")``; the gateway is immediately usable again (§2.9).

Time comes from the injected ``Clock`` and waiting from the injected ``sleep`` (ADR-017): no
``time.*`` here, and tests never wait for real.
"""

from __future__ import annotations

import asyncio
import gzip
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any, TypeVar
from urllib.parse import quote

import httpx

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.canonical import canonical_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ErrorType, TransportError

__all__ = [
    "OP_CLOSE",
    "OP_GET",
    "OP_INIT",
    "OP_POST",
    "GetResult",
    "HttpTransportGateway",
    "InFlightGuard",
    "PostAck",
    "TransportGateway",
]

#: ``details["operation"]`` values (ADR-004 operations, §12.10 ``details.operation``).
OP_INIT = "INIT"
OP_POST = "POST"
OP_GET = "GET"
OP_CLOSE = "CLOSE"

_INIT_STATUSES: frozenset[int] = frozenset({200, 201})
_POST_STATUSES: frozenset[int] = frozenset({200, 202})
_GET_STATUSES: frozenset[int] = frozenset({200})
_CLOSE_STATUSES: frozenset[int] = frozenset(range(200, 300))
_BODY_EXCERPT_BYTES = 256

T = TypeVar("T")


@dataclass(frozen=True)
class PostAck:
    """Acknowledgement of a POST (ADR-004: ``{"accepted": true, "message_id": ...}``)."""

    message_id: str
    accepted: bool
    http_status: int


@dataclass(frozen=True)
class GetResult:
    """Result of one GET: the model's messages after the cursor and the new cursor."""

    messages: list[dict[str, Any]]
    cursor: str | None
    http_status: int


class InFlightGuard:
    """Tracks in-flight calls so that :meth:`abandon` can cancel them (§2.9, §3.12).

    Each guarded coroutine runs in its own task. ``abandon()`` cancels those tasks and marks them;
    the awaiting caller then receives ``TransportError(INTERRUPTED, "ABANDONED")`` instead of a bare
    ``CancelledError``. A cancellation that did **not** come from ``abandon()`` (the caller's own task
    being cancelled) propagates unchanged, so the guard never swallows a real cancellation.
    """

    def __init__(self) -> None:
        self._in_flight: dict[asyncio.Task[Any], str] = {}
        self._abandoned: set[asyncio.Task[Any]] = set()

    @property
    def count(self) -> int:
        return len(self._in_flight)

    async def run(self, operation: str, coro: Coroutine[Any, Any, T]) -> T:
        task: asyncio.Task[T] = asyncio.ensure_future(coro)
        self._in_flight[task] = operation
        try:
            return await task
        except BaseException:
            if task in self._abandoned:
                raise TransportError(
                    ErrorType.INTERRUPTED, "ABANDONED", retryable=False, operation=operation
                ) from None
            raise
        finally:
            self._in_flight.pop(task, None)
            self._abandoned.discard(task)

    def abandon(self) -> None:
        """Cancel every in-flight call. Safe to call when nothing is in flight."""
        for task in list(self._in_flight):
            if not task.done():
                self._abandoned.add(task)
                task.cancel()


class TransportGateway(ABC):
    """The transport boundary (module map §2.3): the rest of the code knows only this ABC."""

    @abstractmethod
    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        """Create a remote conversation; return its remote identifier."""

    @abstractmethod
    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        """POST one complete protocol message (idempotent on ``message_id`` server side)."""

    @abstractmethod
    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        """A single GET of the model's messages after ``after``."""

    @abstractmethod
    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        """Poll ``get_messages`` until at least one message or ``MODEL_GET_TIMEOUT``."""

    @abstractmethod
    async def close_conversation(self, remote_conversation_id: str) -> None:
        """Close the remote conversation (no-op when no close endpoint is configured)."""

    @abstractmethod
    def abandon(self) -> None:
        """Cancel every in-flight call (they raise ``INTERRUPTED / ABANDONED``); reset afterwards."""


class HttpTransportGateway(TransportGateway):
    """httpx implementation of the ADR-004 contract."""

    def __init__(
        self,
        config: TransportSection,
        clock: Clock,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._guard = InFlightGuard()
        self._closed = False
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(config.request_timeout_ms / 1000),
            verify=config.verify_tls,
            transport=transport,
        )

    # ------------------------------------------------------------------ public -------------
    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        return await self._guard.run(OP_INIT, self._init(instructions, metadata))

    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        return await self._guard.run(OP_POST, self._post(remote_conversation_id, payload))

    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return await self._guard.run(OP_GET, self._get(remote_conversation_id, after))

    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return await self._guard.run(OP_GET, self._wait(remote_conversation_id, after))

    async def close_conversation(self, remote_conversation_id: str) -> None:
        if not self._config.close_url:
            return
        await self._guard.run(OP_CLOSE, self._close(remote_conversation_id))

    def abandon(self) -> None:
        self._guard.abandon()

    @property
    def in_flight(self) -> int:
        return self._guard.count

    @property
    def closed(self) -> bool:
        return self._closed

    async def aclose(self) -> None:
        """Release the HTTP client (connection pool). The gateway must not be used afterwards."""
        self._closed = True
        await self._client.aclose()

    # ------------------------------------------------------------------ operations ---------
    async def _init(self, instructions: str, metadata: dict[str, Any]) -> str:
        url = self._url(self._config.init_url)
        body = {
            "user_id": self._config.user_id,
            "instructions": instructions,
            "metadata": metadata,
        }
        status, parsed = await self._request(
            OP_INIT, "POST", url, body=body, expected=_INIT_STATUSES
        )
        if not isinstance(parsed, dict) or not _non_empty_str(parsed.get("conversation_id")):
            raise self._invalid_body(OP_INIT, url, status, "missing_or_invalid:conversation_id")
        return str(parsed["conversation_id"])

    async def _post(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        url = self._url(self._config.post_url, remote_conversation_id)
        status, parsed = await self._request(
            OP_POST, "POST", url, body=payload, expected=_POST_STATUSES
        )
        if (
            not isinstance(parsed, dict)
            or not isinstance(parsed.get("accepted"), bool)
            or not _non_empty_str(parsed.get("message_id"))
        ):
            raise self._invalid_body(OP_POST, url, status, "missing_or_invalid:accepted,message_id")
        if parsed["accepted"] is not True:
            raise TransportError(
                ErrorType.MODEL_PROTOCOL_ERROR,
                "POST_NOT_ACCEPTED",
                retryable=False,
                operation=OP_POST,
                http_status=status,
                url=self._redact(url),
                message_id=parsed["message_id"],
                reason=parsed.get("reason"),
            )
        return PostAck(message_id=str(parsed["message_id"]), accepted=True, http_status=status)

    async def _get(self, remote_conversation_id: str, after: str | None) -> GetResult:
        url = self._url(self._config.get_url, remote_conversation_id, after)
        status, parsed = await self._request(OP_GET, "GET", url, expected=_GET_STATUSES)
        if not isinstance(parsed, dict) or not isinstance(parsed.get("messages"), list):
            raise self._invalid_body(OP_GET, url, status, "missing_or_invalid:messages")
        messages: list[Any] = parsed["messages"]
        if not all(isinstance(message, dict) for message in messages):
            raise self._invalid_body(OP_GET, url, status, "messages_items_not_objects")
        cursor = parsed.get("cursor")
        if cursor is not None and not isinstance(cursor, str):
            raise self._invalid_body(OP_GET, url, status, "invalid:cursor")
        if cursor is None and messages:
            last_id = messages[-1].get("message_id")
            cursor = last_id if _non_empty_str(last_id) else None
        return GetResult(messages=list(messages), cursor=cursor, http_status=status)

    async def _wait(self, remote_conversation_id: str, after: str | None) -> GetResult:
        timeout_ms = self._config.reply_timeout_ms
        interval_ms = self._config.poll_interval_ms
        start = self._clock.monotonic_ms()
        deadline = start + timeout_ms
        polls = 0
        while True:
            result = await self._get(remote_conversation_id, after)
            polls += 1
            if result.messages:
                return result
            now = self._clock.monotonic_ms()
            if now >= deadline:
                raise TransportError(
                    ErrorType.TIMEOUT_ERROR,
                    "MODEL_GET_TIMEOUT",
                    retryable=True,
                    operation=OP_GET,
                    http_status=None,
                    url=self._redact(
                        self._url(self._config.get_url, remote_conversation_id, after)
                    ),
                    timeout_ms=timeout_ms,
                    poll_interval_ms=interval_ms,
                    elapsed_ms=now - start,
                    polls=polls,
                )
            await self._sleep(min(interval_ms, deadline - now) / 1000)

    async def _close(self, remote_conversation_id: str) -> None:
        url = self._url(self._config.close_url, remote_conversation_id)
        await self._request(OP_CLOSE, "POST", url, expected=_CLOSE_STATUSES, parse_json=False)

    # ------------------------------------------------------------------ HTTP plumbing ------
    def _headers(self, *, with_body: bool) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self._config.user_id:
            headers["X-User-Id"] = self._config.user_id
        token = self._config.token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if with_body:
            headers["Content-Type"] = "application/json"
            if self._config.gzip:
                headers["Content-Encoding"] = "gzip"
        return headers

    def _encode(self, payload: Any) -> bytes:
        raw = canonical_bytes(payload)
        return gzip.compress(raw, mtime=0) if self._config.gzip else raw

    @staticmethod
    def _url(template: str, conversation_id: str | None = None, after: str | None = None) -> str:
        url = template
        if conversation_id is not None:
            url = url.replace("{conversation_id}", quote(conversation_id, safe=""))
        return url.replace("{after}", quote(after or "", safe=""))

    def _redact(self, url: str) -> str:
        """The URL for error details: the bearer token never appears in it, even by misconfiguration."""
        token = self._config.token
        return url.replace(token, "***") if token else url

    async def _request(
        self,
        operation: str,
        method: str,
        url: str,
        *,
        expected: frozenset[int],
        body: Any | None = None,
        parse_json: bool = True,
    ) -> tuple[int, Any]:
        """Send one request; return ``(status, parsed body)`` on an expected status, else raise."""
        headers = self._headers(with_body=body is not None)
        content = self._encode(body) if body is not None else None
        try:
            response = await self._client.request(method, url, headers=headers, content=content)
        except httpx.TimeoutException as exc:
            raise self._exception_error(
                ErrorType.TIMEOUT_ERROR, "REQUEST_TIMEOUT", operation, url, exc, retryable=True
            ) from exc
        except (httpx.NetworkError, httpx.RemoteProtocolError, httpx.ProxyError) as exc:
            raise self._exception_error(
                ErrorType.NETWORK_ERROR, "CONNECTION_ERROR", operation, url, exc, retryable=True
            ) from exc
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise self._exception_error(
                ErrorType.SYSTEM_ERROR, "HTTP_CLIENT_ERROR", operation, url, exc, retryable=False
            ) from exc
        status = response.status_code
        if status not in expected:
            raise self._map_status(operation, url, response)
        if not parse_json:
            return status, None
        try:
            return status, response.json()
        except ValueError as exc:
            raise self._invalid_body(
                operation, url, status, "not_json", body=_excerpt(response)
            ) from exc

    def _exception_error(
        self,
        error_type: ErrorType,
        code: str,
        operation: str,
        url: str,
        exc: Exception,
        *,
        retryable: bool,
    ) -> TransportError:
        return TransportError(
            error_type,
            code,
            retryable=retryable,
            operation=operation,
            http_status=None,
            url=self._redact(url),
            cause=type(exc).__name__,
            message=str(exc),
        )

    def _invalid_body(
        self, operation: str, url: str, status: int, reason: str, **details: Any
    ) -> TransportError:
        return TransportError(
            ErrorType.MODEL_PROTOCOL_ERROR,
            "INVALID_RESPONSE_BODY",
            retryable=False,
            operation=operation,
            http_status=status,
            url=self._redact(url),
            reason=reason,
            **details,
        )

    def _map_status(self, operation: str, url: str, response: httpx.Response) -> TransportError:
        """The HTTP -> ``ErrorType`` table of ADR-004.

        4xx: a JSON body ``{"error": "context_window_exceeded"}`` (any status) or 413 ->
        MODEL_CONTEXT_WINDOW_ERROR; 401 -> AUTHN_ERROR; 403 -> AUTHZ_ERROR; 429 -> RATE_LIMIT_ERROR
        (``retry_after_ms`` from ``Retry-After``); 408 -> TIMEOUT_ERROR; other 4xx -> SYSTEM_ERROR
        (not retryable). 5xx: 504 -> TIMEOUT_ERROR; 502/503 -> NETWORK_ERROR; other 5xx ->
        SYSTEM_ERROR transient (retryable). Anything else (1xx, unexpected 2xx, 3xx) breaks the
        contract -> MODEL_PROTOCOL_ERROR ``UNEXPECTED_STATUS``.
        """
        status = response.status_code
        code = f"HTTP_{status}"
        common: dict[str, Any] = {
            "operation": operation,
            "http_status": status,
            "url": self._redact(url),
            "body": _excerpt(response),
        }
        if 400 <= status < 500:
            parsed = _try_json(response)
            if isinstance(parsed, dict) and parsed.get("error") == "context_window_exceeded":
                return TransportError(
                    ErrorType.MODEL_CONTEXT_WINDOW_ERROR,
                    "CONTEXT_WINDOW_EXCEEDED",
                    retryable=False,
                    **common,
                )
            if status == 413:
                return TransportError(
                    ErrorType.MODEL_CONTEXT_WINDOW_ERROR, code, retryable=False, **common
                )
            if status == 401:
                return TransportError(ErrorType.AUTHN_ERROR, code, retryable=False, **common)
            if status == 403:
                return TransportError(ErrorType.AUTHZ_ERROR, code, retryable=False, **common)
            if status == 429:
                retry_after_ms = self._retry_after_ms(response)
                if retry_after_ms is not None:
                    common["retry_after_ms"] = retry_after_ms
                return TransportError(ErrorType.RATE_LIMIT_ERROR, code, retryable=True, **common)
            if status == 408:
                return TransportError(ErrorType.TIMEOUT_ERROR, code, retryable=True, **common)
            return TransportError(ErrorType.SYSTEM_ERROR, code, retryable=False, **common)
        if status >= 500:
            if status == 504:
                return TransportError(ErrorType.TIMEOUT_ERROR, code, retryable=True, **common)
            if status in (502, 503):
                return TransportError(ErrorType.NETWORK_ERROR, code, retryable=True, **common)
            return TransportError(
                ErrorType.SYSTEM_ERROR, code, retryable=True, transient=True, **common
            )
        return TransportError(
            ErrorType.MODEL_PROTOCOL_ERROR, "UNEXPECTED_STATUS", retryable=False, **common
        )

    def _retry_after_ms(self, response: httpx.Response) -> int | None:
        """``Retry-After`` as milliseconds: delay-seconds, or an HTTP-date relative to the clock."""
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        value = value.strip()
        if value.isdigit():
            return int(value) * 1000
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        return max(0, int((when - self._clock.now()).total_seconds() * 1000))


# ------------------------------------------------------------------------------------------------
# module helpers
# ------------------------------------------------------------------------------------------------
def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value)


def _try_json(response: httpx.Response) -> Any | None:
    try:
        return response.json()
    except ValueError:
        return None


def _excerpt(response: httpx.Response) -> str:
    return response.text[:_BODY_EXCERPT_BYTES]
