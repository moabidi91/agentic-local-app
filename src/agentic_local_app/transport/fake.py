"""``FakeTransportGateway`` — the scripted network double (§18.3, module map §4).

No network, no waiting: every call returns immediately. The scenario is written by the test:

- ``init_conversation`` returns ``remote-0001``, ``remote-0002``, ... and records the init payload;
- ``enqueue_messages(remote_id, messages)`` queues one reply batch; each ``get_messages`` /
  ``wait_for_reply`` consumes **one** batch (the cursor is the ``message_id`` of its last message
  when it is an envelope; a batch may hold raw items — text, chat completions — for a gateway
  wrapped by a ``CodecTransport`` (ADR-021), the cursor then stays ``after``); an empty queue gives
  an empty ``GetResult`` to ``get_messages`` and ``MODEL_GET_TIMEOUT`` to ``wait_for_reply`` (when a
  ``FakeClock`` is injected it is advanced by ``reply_timeout_ms`` first, so that budget deadlines
  observe the time a real polling would have consumed);
- ``enqueue_error(operation, error, times)`` makes the next ``times`` calls of that operation raise
  ``error`` before normal behaviour resumes;
- ``hang_next(operation, times)`` makes the next call(s) block until ``abandon()`` cancels them,
  which raises ``TransportError(INTERRUPTED, "ABANDONED")`` exactly like the HTTP gateway (§2.9).

Everything the orchestrator sent is recorded: ``inits``, ``posted`` (the payload as received — a
protocol envelope, or the text form a codec produced: its acknowledgement then carries an empty
``message_id``, restored by the decorator), ``get_calls``, ``closed``.

``FakeTransportProvider`` is the same double under the registry convention of ADR-020 (built from
``config.transport`` and a clock), selectable with ``transport.provider = "fake"`` for a run that
must never reach a network.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any

from agentic_local_app.config import TransportSection
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
from agentic_local_app.transport.registry import TransportRegistry

__all__ = ["FakeTransportGateway", "FakeTransportProvider"]

_OPERATIONS: dict[str, str] = {"init": OP_INIT, "post": OP_POST, "get": OP_GET, "close": OP_CLOSE}


class FakeTransportGateway(TransportGateway):
    def __init__(self, clock: Clock | None = None, *, reply_timeout_ms: int = 120_000) -> None:
        self.clock = clock
        self.reply_timeout_ms = reply_timeout_ms
        self.inits: list[dict[str, Any]] = []
        self.posted: list[tuple[str, Any]] = []
        self.get_calls: list[tuple[str, str | None]] = []
        self.closed: list[str] = []
        self._queues: defaultdict[str, deque[list[Any]]] = defaultdict(deque)
        self._errors: defaultdict[str, deque[TransportError]] = defaultdict(deque)
        self._hangs: defaultdict[str, int] = defaultdict(int)
        self._hanging = 0
        self._hang_started = asyncio.Event()
        self._init_counter = 0
        self._guard = InFlightGuard()

    # ------------------------------------------------------------------ scripting ----------
    def enqueue_messages(self, remote_conversation_id: str, messages: list[Any]) -> None:
        """Queue one reply batch for ``remote_conversation_id`` (consumed by a single GET): protocol
        envelopes, or raw items when the gateway is wrapped by a codec (ADR-021)."""
        self._queues[remote_conversation_id].append(list(messages))

    def enqueue_error(self, operation: str, error: TransportError, times: int = 1) -> None:
        """Make the next ``times`` calls of ``operation`` (init|post|get|close) raise ``error``."""
        self._check_operation(operation)
        if times < 1:
            raise ValueError("times must be >= 1")
        self._errors[operation].extend([error] * times)

    def hang_next(self, operation: str, times: int = 1) -> None:
        """Make the next ``times`` calls of ``operation`` block until :meth:`abandon`."""
        self._check_operation(operation)
        if times < 1:
            raise ValueError("times must be >= 1")
        self._hangs[operation] += times

    def pending(self, remote_conversation_id: str) -> int:
        """Number of reply batches still queued for a conversation."""
        return len(self._queues.get(remote_conversation_id, ()))

    @property
    def in_flight(self) -> int:
        return self._guard.count

    @property
    def is_hanging(self) -> bool:
        """``True`` while at least one call is blocked by :meth:`hang_next` (awaiting ``abandon()``)."""
        return self._hanging > 0

    async def wait_until_hanging(self) -> None:
        """Return once a call blocked by :meth:`hang_next` is actually in flight (test synchronisation)."""
        await self._hang_started.wait()

    # ------------------------------------------------------------------ gateway ------------
    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        return await self._guard.run(OP_INIT, self._init(instructions, metadata))

    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        return await self._guard.run(OP_POST, self._post(remote_conversation_id, payload))

    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return await self._guard.run(OP_GET, self._get(remote_conversation_id, after, wait=False))

    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        return await self._guard.run(OP_GET, self._get(remote_conversation_id, after, wait=True))

    async def close_conversation(self, remote_conversation_id: str) -> None:
        await self._guard.run(OP_CLOSE, self._close(remote_conversation_id))

    def abandon(self) -> None:
        self._guard.abandon()

    # ------------------------------------------------------------------ behaviour ----------
    async def _init(self, instructions: str, metadata: dict[str, Any]) -> str:
        await self._before("init")
        self.inits.append({"instructions": instructions, "metadata": dict(metadata)})
        self._init_counter += 1
        return f"remote-{self._init_counter:04d}"

    async def _post(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        await self._before("post")
        self.posted.append((remote_conversation_id, payload))
        message_id = payload.get("message_id") if isinstance(payload, dict) else None
        return PostAck(
            message_id=str(message_id) if message_id is not None else "",
            accepted=True,
            http_status=202,
        )

    async def _get(
        self, remote_conversation_id: str, after: str | None, *, wait: bool
    ) -> GetResult:
        self.get_calls.append((remote_conversation_id, after))  # every attempt is logged
        await self._before("get")
        queue = self._queues.get(remote_conversation_id)
        if queue:
            messages = queue.popleft()
            cursor = after
            if messages and isinstance(messages[-1], dict):
                last_id = messages[-1].get("message_id")
                cursor = str(last_id) if last_id is not None else after
            return GetResult(messages=messages, cursor=cursor, http_status=200)
        if not wait:
            return GetResult(messages=[], cursor=after, http_status=200)
        self._consume_reply_timeout()
        raise TransportError(
            ErrorType.TIMEOUT_ERROR,
            "MODEL_GET_TIMEOUT",
            retryable=True,
            operation=OP_GET,
            http_status=None,
            url=f"fake://{remote_conversation_id}/messages?after={after or ''}",
            timeout_ms=self.reply_timeout_ms,
        )

    async def _close(self, remote_conversation_id: str) -> None:
        await self._before("close")
        self.closed.append(remote_conversation_id)

    async def _before(self, operation: str) -> None:
        """Apply the scripted hang / error for ``operation`` (hang first, then error queue)."""
        if self._hangs[operation] > 0:
            self._hangs[operation] -= 1
            self._hanging += 1
            self._hang_started.set()
            try:
                await asyncio.Event().wait()  # only abandon() (cancellation) gets us out of here
            finally:
                self._hanging -= 1
                if self._hanging == 0:
                    self._hang_started.clear()
        errors = self._errors[operation]
        if errors:
            raise errors.popleft()

    def _consume_reply_timeout(self) -> None:
        advance = getattr(self.clock, "advance", None)
        if callable(advance):
            advance(self.reply_timeout_ms)

    @staticmethod
    def _check_operation(operation: str) -> None:
        if operation not in _OPERATIONS:
            raise ValueError(
                f"unknown operation {operation!r}; expected one of {sorted(_OPERATIONS)}"
            )


@TransportRegistry.register("fake")
class FakeTransportProvider(FakeTransportGateway):
    """The ``fake`` provider: the scripted double built like any provider (ADR-020).

    ``reply_timeout_ms`` comes from the transport section; no option is accepted; the keyword
    arguments of the registry convention (``transport``, ``sleep``...) are ignored.
    """

    def __init__(self, config: TransportSection, clock: Clock, **kwargs: Any) -> None:
        validate_options(type(self), config.options)
        super().__init__(clock, reply_timeout_ms=config.reply_timeout_ms)
