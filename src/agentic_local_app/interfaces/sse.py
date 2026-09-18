"""``SseBroker`` — the live event stream of ADR-018 as a non-critical ``EventBus`` subscriber.

The bus is synchronous (ADR-015): ``handle`` runs inside ``bus.publish``, in the publisher's
thread, and must never block. It only turns the event into an :class:`SseFrame` and drops it into
the **bounded** queue of every client whose filter matches (session, task, event types). A client
that does not read fast enough fills its queue: it is unregistered on the spot, receives one last
``event: dropped`` frame and its stream ends — it reconnects with ``Last-Event-ID`` (ADR-018). The
bus never waits for a client and never sees an exception from here.

Frame identifiers follow the audit chain, the only durable order (§3.16, ADR-017):

- an **audited** event has ``id = "<sequence>"`` — the sequence of the ``AuditEvent`` the
  ``AuditLog`` has just chained for the session (it is subscribed first, so ``audit.last`` is
  exactly this event when the broker runs) — and its ``data`` carries ``event_id`` and
  ``sequence`` next to the ``Event`` fields;
- a **non-audited** event (``task.output``) has ``id = "<sequence>.<n>"``: the last audited
  sequence of the session and a counter reset by every audited event. Such frames are never
  replayed (the blob is the truth; ``GET .../output`` re-reads it).

``data`` is the canonical JSON (ADR-017) of the event dump; a replayed frame built from an
``AuditEvent`` has the same keys and the same bytes as the live frame it repeats.

Resume: ``subscribe(session_id, last_event_id=...)`` first replays, from
``store.list_audit_events(session_id, after_sequence=...)``, every audited event after the given
position (through the client's filters), then switches to the live queue, skipping any live frame
whose position is not after the last one delivered (events published during the replay are queued,
then deduplicated). Heartbeats (``: keep-alive``) are emitted after ``heartbeat_s`` seconds without
a frame, when enabled. The injected ``Clock`` only dates the ``dropped`` frame.
"""

from __future__ import annotations

import asyncio
import enum
import json as _json
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.models import AuditEvent
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "DROPPED_EVENT",
    "HEARTBEAT_COMMENT",
    "SSE_SUBSCRIBER_NAME",
    "SseBroker",
    "SseFrame",
    "frame_from_audit_event",
    "parse_event_id",
]

#: Name under which the broker registers on the bus (after the ADR-015 subscribers).
SSE_SUBSCRIBER_NAME = "sse"
#: ``event`` of the last frame a too-slow client receives before its stream ends.
DROPPED_EVENT = "dropped"
#: Comment sent as a heartbeat.
HEARTBEAT_COMMENT = "keep-alive"
#: Audit events read per page during a replay.
REPLAY_PAGE_SIZE = 500

_AUDIT_DATA_KEYS: tuple[str, ...] = (
    "event_id",
    "sequence",
    "event_type",
    "timestamp",
    "session_id",
    "conversation_id",
    "cycle_id",
    "plan_id",
    "task_id",
    "payload",
)


@dataclass(frozen=True)
class SseFrame:
    """One server-sent event. ``comment`` alone makes a heartbeat (``: keep-alive``)."""

    id: str | None = None
    event: str | None = None
    data: str | None = None
    comment: str | None = None

    def encode(self) -> bytes:
        lines: list[str] = []
        if self.comment is not None:
            lines.append(f": {self.comment}")
        if self.id is not None:
            lines.append(f"id: {self.id}")
        if self.event is not None:
            lines.append(f"event: {self.event}")
        if self.data is not None:
            lines.extend(f"data: {line}" for line in self.data.split("\n"))
        return ("\n".join(lines) + "\n\n").encode("utf-8")

    @property
    def json(self) -> dict[str, Any]:
        """``data`` parsed (an event frame always carries a JSON object)."""
        if self.data is None:
            raise ValueError("frame without data")
        parsed: dict[str, Any] = _json.loads(self.data)
        return parsed

    @classmethod
    def heartbeat(cls) -> SseFrame:
        return cls(comment=HEARTBEAT_COMMENT)


def parse_event_id(value: str | None) -> tuple[int, int] | None:
    """``"42"`` -> ``(42, 0)``, ``"42.3"`` -> ``(42, 3)``, anything else -> ``None``."""
    if not value:
        return None
    sequence, _, tail = value.strip().partition(".")
    if not sequence.isdigit() or (tail and not tail.isdigit()):
        return None
    return int(sequence), int(tail) if tail else 0


def _iso(moment: datetime) -> str:
    """ISO 8601 the way pydantic serialises it (``Z`` for UTC), so every frame looks the same."""
    text = moment.isoformat()
    return text[:-6] + "Z" if text.endswith("+00:00") else text


def frame_from_audit_event(event: AuditEvent) -> SseFrame:
    """The frame an audited event produces — live or replayed, byte for byte the same."""
    dumped = event.model_dump(mode="json")
    data = {key: dumped[key] for key in _AUDIT_DATA_KEYS}
    return SseFrame(id=str(event.sequence), event=event.event_type, data=canonical_json(data))


class _Signal(enum.Enum):
    DROPPED = "dropped"
    CLOSED = "closed"


@dataclass(eq=False)
class _Client:
    session_id: str | None
    task_id: str | None
    event_types: frozenset[EventType] | None
    queue: asyncio.Queue[SseFrame | _Signal]
    loop: asyncio.AbstractEventLoop
    active: bool = True

    def accepts(self, event: Event) -> bool:
        if self.session_id is not None and event.session_id != self.session_id:
            return False
        if self.task_id is not None and event.task_id != self.task_id:
            return False
        return self.event_types is None or event.event_type in self.event_types

    def accepts_audit(self, event: AuditEvent) -> bool:
        if self.task_id is not None and event.task_id != self.task_id:
            return False
        return self.event_types is None or event.event_type in {t.value for t in self.event_types}


class SseBroker:
    """Fan-out of the bus to bounded per-client queues (ADR-018, ADR-015)."""

    def __init__(
        self,
        bus: EventBus,
        clock: Clock,
        *,
        queue_size: int,
        audit: AuditLog | None = None,
        store: ConversationStore | None = None,
        name: str = SSE_SUBSCRIBER_NAME,
    ) -> None:
        if queue_size <= 0:
            raise ValueError("queue_size must be > 0")
        self._bus = bus
        self._clock = clock
        self._queue_size = queue_size
        self._audit = audit
        self._store = store
        self._name = name
        self._clients: list[_Client] = []
        #: non-audited events since the last audited one, per session (the ``.<n>`` of the id)
        self._tail: dict[str, int] = {}
        #: sequence counter per session when neither an audit log nor a store is available
        self._fallback: dict[str, int] = {}
        self.dropped_count = 0
        bus.subscribe(self.handle, name=name)

    # ------------------------------------------------------------------ bus side ----------
    @property
    def client_count(self) -> int:
        return len(self._clients)

    @property
    def queue_size(self) -> int:
        return self._queue_size

    def handle(self, event: Event) -> None:
        """Bus subscriber: advance the id counters, build the frame once, offer it to every
        matching client; never blocks, never raises for a client."""
        frame_id, sequence, event_id = self._advance(event)
        targets = [c for c in self._clients if c.active and c.accepts(event)]
        if not targets:
            return
        frame = self._live_frame(event, frame_id, sequence, event_id)
        for client in targets:
            self._offer(client, frame)

    def close(self) -> None:
        """Unsubscribe from the bus and end every open stream."""
        self._bus.unsubscribe(self._name)
        for client in list(self._clients):
            client.active = False
            self._signal(client, _Signal.CLOSED)
        self._clients.clear()

    # ------------------------------------------------------------------ frames ------------
    def _advance(self, event: Event) -> tuple[str, int | None, str | None]:
        """The frame id of ``event`` — ``"<sequence>"`` or ``"<sequence>.<n>"`` — plus, for an
        audited event, its audit sequence and ``event_id`` when an audit source is available."""
        session_id = event.session_id
        if not event.audited:
            count = self._tail[session_id] = self._tail.get(session_id, 0) + 1
            return f"{self._current_sequence(session_id)}.{count}", None, None
        self._tail[session_id] = 0
        last = self._last_audit(session_id)
        if last is not None:
            return str(last.sequence), last.sequence, last.event_id
        sequence = self._fallback[session_id] = self._fallback.get(session_id, 0) + 1
        return str(sequence), sequence, None

    @staticmethod
    def _live_frame(
        event: Event, frame_id: str, sequence: int | None, event_id: str | None
    ) -> SseFrame:
        data: dict[str, Any] = event.model_dump(mode="json")
        if sequence is not None:
            data["sequence"] = sequence
        if event_id is not None:
            data["event_id"] = event_id
        return SseFrame(id=frame_id, event=event.event_type.value, data=canonical_json(data))

    def _last_audit(self, session_id: str) -> AuditEvent | None:
        if self._audit is not None:
            return self._audit.last(session_id)
        if self._store is not None:
            return self._store.get_last_audit_event(session_id)
        return None

    def _current_sequence(self, session_id: str) -> int:
        last = self._last_audit(session_id)
        if last is not None:
            return last.sequence
        return self._fallback.get(session_id, 0)

    def _dropped_frame(self, client: _Client) -> SseFrame:
        data = {
            "reason": "queue_full",
            "queue_size": self._queue_size,
            "session_id": client.session_id,
            "task_id": client.task_id,
            "timestamp": _iso(self._clock.now()),
        }
        return SseFrame(event=DROPPED_EVENT, data=canonical_json(data))

    # ------------------------------------------------------------------ delivery ----------
    def _offer(self, client: _Client, frame: SseFrame) -> None:
        """Enqueue on the client's loop: directly when we are on it, thread-safely otherwise."""
        try:
            running: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is client.loop:
            self._enqueue(client, frame)
            return
        try:
            client.loop.call_soon_threadsafe(self._enqueue, client, frame)
        except RuntimeError:  # the client's loop is closed: nobody will ever read
            self._forget(client)

    def _enqueue(self, client: _Client, frame: SseFrame) -> None:
        if not client.active:
            return
        if client.queue.qsize() >= self._queue_size:
            self._drop(client)
            return
        client.queue.put_nowait(frame)

    def _drop(self, client: _Client) -> None:
        """Too slow: unregister now, one ``dropped`` frame in the reserved slot, stream over."""
        self._forget(client)
        self.dropped_count += 1
        self._signal(client, _Signal.DROPPED)

    def _signal(self, client: _Client, signal: _Signal) -> None:
        try:
            client.queue.put_nowait(signal)
        except asyncio.QueueFull:  # pragma: no cover - the reserved slot is normally free
            client.queue.get_nowait()
            client.queue.put_nowait(signal)

    def _forget(self, client: _Client) -> None:
        client.active = False
        if client in self._clients:
            self._clients.remove(client)

    # ------------------------------------------------------------------ client side -------
    async def subscribe(
        self,
        session_id: str | None = None,
        *,
        task_id: str | None = None,
        event_types: Iterable[EventType] | None = None,
        last_event_id: str | None = None,
        heartbeat_s: float | None = None,
    ) -> AsyncIterator[SseFrame]:
        """Frames for one session (or all when ``None``), optionally one task and some event types.

        Registers the client **before** the replay so nothing published meanwhile is lost; ends
        after a ``dropped`` frame or when the broker closes; unregisters when the consumer stops.
        """
        client = _Client(
            session_id=session_id,
            task_id=task_id,
            event_types=frozenset(event_types) if event_types is not None else None,
            queue=asyncio.Queue(maxsize=self._queue_size + 1),  # + the dropped / closed slot
            loop=asyncio.get_running_loop(),
        )
        self._clients.append(client)
        try:
            position = parse_event_id(last_event_id)
            if position is not None and session_id is not None:
                for replayed in self._replay(session_id, position[0], client):
                    position = (replayed.sequence, 0)
                    yield frame_from_audit_event(replayed)
            while True:
                item = await self._next(client, heartbeat_s)
                if item is None:
                    yield SseFrame.heartbeat()
                elif item is _Signal.DROPPED:
                    yield self._dropped_frame(client)
                    return
                elif item is _Signal.CLOSED:
                    return
                elif isinstance(item, SseFrame):
                    if position is not None:
                        key = parse_event_id(item.id)
                        if key is not None and key <= position:
                            continue  # already delivered by the replay
                    yield item
        finally:
            self._forget(client)

    async def stream(
        self,
        session_id: str | None = None,
        *,
        task_id: str | None = None,
        event_types: Iterable[EventType] | None = None,
        last_event_id: str | None = None,
        heartbeat_s: float | None = None,
    ) -> AsyncIterator[bytes]:
        """:meth:`subscribe` encoded for a ``text/event-stream`` response."""
        async for frame in self.subscribe(
            session_id,
            task_id=task_id,
            event_types=event_types,
            last_event_id=last_event_id,
            heartbeat_s=heartbeat_s,
        ):
            yield frame.encode()

    def _replay(self, session_id: str, after: int, client: _Client) -> Iterator[AuditEvent]:
        """Audited events of ``session_id`` after ``after``, by pages, through the client's filters."""
        if self._store is None:
            return
        cursor: int | None = after
        while True:
            page = self._store.list_audit_events(
                session_id, after_sequence=cursor, limit=REPLAY_PAGE_SIZE
            )
            for event in page:
                if client.accepts_audit(event):
                    yield event
            if len(page) < REPLAY_PAGE_SIZE:
                return
            cursor = page[-1].sequence

    @staticmethod
    async def _next(client: _Client, heartbeat_s: float | None) -> SseFrame | _Signal | None:
        if heartbeat_s is None:
            return await client.queue.get()
        try:
            return await asyncio.wait_for(client.queue.get(), timeout=heartbeat_s)
        except TimeoutError:
            return None
