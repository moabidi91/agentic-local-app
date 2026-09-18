"""``HttpProviderBase`` — the template method shared by every HTTP provider (ADR-020).

The base owns everything that does not depend on the remote API's shape: the injectable ``httpx``
client and its timeouts, the :class:`~agentic_local_app.transport.base.InFlightGuard` behind
``abandon()`` (§2.9), the canonical JSON encoding of bodies (ADR-017) and their optional gzip
compression (§3.12), the polling of ``wait_for_reply`` bounded by ``reply_timeout_ms``
(``MODEL_GET_TIMEOUT``), the expected-status check, the HTTP -> ``ErrorType`` table of ADR-004
(``Retry-After``, ``context_window_exceeded``) and the ``details`` every ``TransportError`` carries:
``operation``, ``http_status`` and a token-free ``url``.

A provider describes each operation as an :class:`HttpCall` and reads each reply through the
extension points, all overridable:

| hook | role | default |
|---|---|---|
| ``headers(operation)`` | common headers of a call | ``Accept``, ``X-User-Id``, ``Authorization: Bearer`` when a token is set |
| ``build_init`` / ``parse_init`` | create the remote conversation, read its id | abstract |
| ``build_post`` / ``parse_post`` | deposit a message, read the acknowledgement | abstract |
| ``build_get`` / ``parse_get`` | read the messages after a cursor | abstract |
| ``build_close`` | close the conversation; ``None`` = no remote call | abstract |
| ``classify_error(operation, status, body, headers)`` | a status outside ``expected_statuses`` | the ADR-004 table |
| ``redact_url(url)`` | the ``url`` written in error details | the bearer token masked |
| ``options_model`` | pydantic model of ``transport.options`` | ``None`` (no option accepted) |

``Content-Type: application/json`` (unless the call sets its own) and ``Content-Encoding: gzip``
are added by the base to every call that carries a body, because the base is what encodes it. A
``parse_*`` hook signals a body outside the provider's contract by raising :class:`InvalidResponseError`;
the base turns it into ``TransportError(MODEL_PROTOCOL_ERROR, <code>)`` and stamps the context. The
error returned by ``classify_error`` is stamped the same way, so a provider never has to thread the
operation or the URL through its hooks.

Time comes from the injected ``Clock`` and waiting from the injected ``sleep`` (ADR-017): no
``time.*`` here, and tests never wait for real.
"""

from __future__ import annotations

import asyncio
import gzip
import json
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.canonical import canonical_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ErrorType, TransportError
from agentic_local_app.transport.base import (
    OP_CLOSE,
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    InFlightGuard,
    PostAck,
    TransportGateway,
    validate_options,
)

__all__ = ["BODY_EXCERPT_BYTES", "HttpCall", "HttpProviderBase", "InvalidResponseError"]

#: Length of the response excerpt written in ``details["body"]`` of a classified error.
BODY_EXCERPT_BYTES = 256
_CONTENT_TYPE = "content-type"


@dataclass(frozen=True)
class HttpCall:
    """One HTTP request as a provider describes it; the base sends it.

    ``json`` is the body (``None`` = no body), encoded by the base as canonical JSON, gzip-compressed
    when ``transport.gzip`` is on. ``expected_statuses`` are the statuses that carry a valid reply;
    any other status goes through ``classify_error``. ``parse_json=False`` hands ``None`` to the
    ``parse_*`` hook instead of decoding the body (a close endpoint answering ``204``, for instance).
    """

    method: str
    url: str
    headers: dict[str, str]
    json: Any | None
    expected_statuses: frozenset[int]
    parse_json: bool = True


class InvalidResponseError(Exception):
    """Raised by a ``parse_*`` hook: an expected status carries a body outside the contract.

    The base turns it into ``TransportError(MODEL_PROTOCOL_ERROR, code)`` (not retryable) whose
    ``details`` carry ``operation``, ``http_status``, the token-free ``url`` and ``**details``.
    """

    def __init__(self, code: str = "INVALID_RESPONSE_BODY", **details: Any) -> None:
        super().__init__(f"{code}: {details}")
        self.code = code
        self.details = details


class HttpProviderBase(TransportGateway):
    """Template method over httpx: subclasses describe calls, the base performs them."""

    #: pydantic model validating ``transport.options`` for this provider; ``None`` = no options.
    options_model: ClassVar[type[BaseModel] | None] = None

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
        #: The validated ``transport.options`` (an ``options_model`` instance) or ``None``.
        self.options: BaseModel | None = validate_options(type(self), config.options)
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
        call = self.build_close(remote_conversation_id)
        if call is None:
            return
        await self._guard.run(OP_CLOSE, self._close(call))

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

    # ------------------------------------------------------------------ extension points ---
    def headers(self, operation: str) -> dict[str, str]:
        """Common headers of every call of ``operation`` (ADR-004): ``Accept``, ``X-User-Id`` and
        ``Authorization: Bearer <token>`` when the token is present in the environment (read at
        call time, never stored)."""
        headers = {"Accept": "application/json"}
        if self._config.user_id:
            headers["X-User-Id"] = self._config.user_id
        token = self._config.token
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @abstractmethod
    def build_init(self, instructions: str, metadata: dict[str, Any]) -> HttpCall:
        """The call creating the remote conversation."""

    @abstractmethod
    def parse_init(self, status: int, body: Any) -> str:
        """The remote conversation id read from an accepted init reply."""

    @abstractmethod
    def build_post(self, remote_conversation_id: str, payload: dict[str, Any]) -> HttpCall:
        """The call depositing one protocol message."""

    @abstractmethod
    def parse_post(self, status: int, body: Any, *, payload: dict[str, Any]) -> PostAck:
        """The acknowledgement read from an accepted POST reply (``payload`` is what was sent)."""

    @abstractmethod
    def build_get(self, remote_conversation_id: str, after: str | None) -> HttpCall:
        """The call reading the model's messages after the cursor ``after``."""

    @abstractmethod
    def parse_get(self, status: int, body: Any) -> GetResult:
        """The messages and cursor read from an accepted GET reply."""

    @abstractmethod
    def build_close(self, remote_conversation_id: str) -> HttpCall | None:
        """The call closing the remote conversation; ``None`` when nothing is sent."""

    def classify_error(
        self, operation: str, status: int, body: str, headers: Mapping[str, str]
    ) -> TransportError:
        """The HTTP -> ``ErrorType`` table of ADR-004 for a status outside ``expected_statuses``.

        4xx: a JSON body ``{"error": "context_window_exceeded"}`` (any status) or 413 ->
        MODEL_CONTEXT_WINDOW_ERROR; 401 -> AUTHN_ERROR; 403 -> AUTHZ_ERROR; 429 -> RATE_LIMIT_ERROR
        (``retry_after_ms`` from ``Retry-After``); 408 -> TIMEOUT_ERROR; other 4xx -> SYSTEM_ERROR
        (not retryable). 5xx: 504 -> TIMEOUT_ERROR; 502/503 -> NETWORK_ERROR; other 5xx ->
        SYSTEM_ERROR transient (retryable). Anything else (1xx, unexpected 2xx, 3xx) breaks the
        contract -> MODEL_PROTOCOL_ERROR ``UNEXPECTED_STATUS``. The base adds ``operation``,
        ``http_status`` and ``url`` to the details of the returned error.
        """
        code = f"HTTP_{status}"
        common: dict[str, Any] = {"body": body[:BODY_EXCERPT_BYTES]}
        if 400 <= status < 500:
            parsed = _try_json(body)
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
                retry_after_ms = self._retry_after_ms(headers.get("Retry-After"))
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

    def redact_url(self, url: str) -> str:
        """The URL for error details: the bearer token never appears in it, even by misconfiguration."""
        token = self._config.token
        return url.replace(token, "***") if token else url

    # ------------------------------------------------------------------ template ------------
    async def _init(self, instructions: str, metadata: dict[str, Any]) -> str:
        call = self.build_init(instructions, metadata)
        status, body = await self._send(OP_INIT, call)
        with self._parsing(OP_INIT, call.url, status):
            return self.parse_init(status, body)

    async def _post(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        call = self.build_post(remote_conversation_id, payload)
        status, body = await self._send(OP_POST, call)
        with self._parsing(OP_POST, call.url, status):
            return self.parse_post(status, body, payload=payload)

    async def _get(self, remote_conversation_id: str, after: str | None) -> GetResult:
        call = self.build_get(remote_conversation_id, after)
        status, body = await self._send(OP_GET, call)
        with self._parsing(OP_GET, call.url, status):
            return self.parse_get(status, body)

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
                    url=self.redact_url(self.build_get(remote_conversation_id, after).url),
                    timeout_ms=timeout_ms,
                    poll_interval_ms=interval_ms,
                    elapsed_ms=now - start,
                    polls=polls,
                )
            await self._sleep(min(interval_ms, deadline - now) / 1000)

    async def _close(self, call: HttpCall) -> None:
        await self._send(OP_CLOSE, call)

    # ------------------------------------------------------------------ HTTP plumbing ------
    def _encode(self, payload: Any) -> bytes:
        raw = canonical_bytes(payload)
        return gzip.compress(raw, mtime=0) if self._config.gzip else raw

    async def _send(self, operation: str, call: HttpCall) -> tuple[int, Any]:
        """Send one call; return ``(status, parsed body)`` on an expected status, else raise."""
        headers = dict(call.headers)
        content: bytes | None = None
        if call.json is not None:
            content = self._encode(call.json)
            if not any(name.lower() == _CONTENT_TYPE for name in headers):
                headers["Content-Type"] = "application/json"
            if self._config.gzip:
                headers["Content-Encoding"] = "gzip"
        try:
            response = await self._client.request(
                call.method, call.url, headers=headers, content=content
            )
        except httpx.TimeoutException as exc:
            raise self._exception_error(
                ErrorType.TIMEOUT_ERROR, "REQUEST_TIMEOUT", operation, call.url, exc, retryable=True
            ) from exc
        except (httpx.NetworkError, httpx.RemoteProtocolError, httpx.ProxyError) as exc:
            raise self._exception_error(
                ErrorType.NETWORK_ERROR,
                "CONNECTION_ERROR",
                operation,
                call.url,
                exc,
                retryable=True,
            ) from exc
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise self._exception_error(
                ErrorType.SYSTEM_ERROR,
                "HTTP_CLIENT_ERROR",
                operation,
                call.url,
                exc,
                retryable=False,
            ) from exc
        status = response.status_code
        if status not in call.expected_statuses:
            error = self.classify_error(operation, status, response.text, response.headers)
            raise self._stamped(error, operation, call.url, status)
        if not call.parse_json:
            return status, None
        try:
            return status, response.json()
        except ValueError as exc:
            raise self._protocol_error(
                operation,
                call.url,
                status,
                InvalidResponseError(reason="not_json", body=response.text[:BODY_EXCERPT_BYTES]),
            ) from exc

    @contextmanager
    def _parsing(self, operation: str, url: str, status: int) -> Iterator[None]:
        """Turn an :class:`InvalidResponseError` raised by a ``parse_*`` hook into a stamped error."""
        try:
            yield
        except InvalidResponseError as exc:
            raise self._protocol_error(operation, url, status, exc) from exc

    def _stamped(
        self, error: TransportError, operation: str, url: str, status: int | None
    ) -> TransportError:
        """The same error with ``operation``, ``http_status`` and the redacted ``url`` in details."""
        normalized = error.error
        details = {
            **normalized.details,
            "operation": operation,
            "http_status": status,
            "url": self.redact_url(url),
        }
        return TransportError(
            normalized.error_type,
            normalized.error_code,
            retryable=normalized.retryable,
            **details,
        )

    def _protocol_error(
        self, operation: str, url: str, status: int, exc: InvalidResponseError
    ) -> TransportError:
        return TransportError(
            ErrorType.MODEL_PROTOCOL_ERROR,
            exc.code,
            retryable=False,
            operation=operation,
            http_status=status,
            url=self.redact_url(url),
            **exc.details,
        )

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
            url=self.redact_url(url),
            cause=type(exc).__name__,
            message=str(exc),
        )

    def _retry_after_ms(self, value: str | None) -> int | None:
        """``Retry-After`` as milliseconds: delay-seconds, or an HTTP-date relative to the clock."""
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


def _try_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except ValueError:
        return None
