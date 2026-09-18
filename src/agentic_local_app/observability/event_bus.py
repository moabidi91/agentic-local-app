"""Synchronous in-process EventBus (§3.20, ADR-015).

- ``publish`` calls subscribers in subscription order, in the caller's thread, and returns once all
  of them have been notified: delivery order is total and the snapshot is consistent immediately.
- A failing subscriber is isolated (logged, counted) **unless** it was registered as *critical*
  (the AuditLog): then the exception propagates, because the audit trail is part of the critical
  state (ADR-015).
- Subscribers may filter on event types to keep hot paths cheap (``task.output`` volume, ADR-018).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from agentic_local_app.domain.events import Event, EventType

log = logging.getLogger(__name__)

Subscriber = Callable[[Event], None]


@dataclass
class _Subscription:
    name: str
    handler: Subscriber
    critical: bool
    event_types: frozenset[EventType] | None


@dataclass
class EventBus:
    _subscriptions: list[_Subscription] = field(default_factory=list)
    published_count: int = 0
    subscriber_errors: int = 0

    def subscribe(
        self,
        handler: Subscriber,
        *,
        name: str,
        critical: bool = False,
        event_types: Iterable[EventType] | None = None,
    ) -> None:
        if any(s.name == name for s in self._subscriptions):
            raise ValueError(f"subscriber already registered: {name}")
        self._subscriptions.append(
            _Subscription(
                name=name,
                handler=handler,
                critical=critical,
                event_types=frozenset(event_types) if event_types is not None else None,
            )
        )

    def unsubscribe(self, name: str) -> None:
        self._subscriptions = [s for s in self._subscriptions if s.name != name]

    @property
    def subscriber_names(self) -> list[str]:
        return [s.name for s in self._subscriptions]

    def publish(self, event: Event) -> None:
        self.published_count += 1
        for sub in list(self._subscriptions):
            if sub.event_types is not None and event.event_type not in sub.event_types:
                continue
            try:
                sub.handler(event)
            except Exception:
                self.subscriber_errors += 1
                if sub.critical:
                    raise
                log.exception("event subscriber %s failed on %s", sub.name, event.event_type.value)


class RecordingSubscriber:
    """Test helper: keeps every event it receives, in order."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def __call__(self, event: Event) -> None:
        self.events.append(event)

    def of_type(self, event_type: EventType) -> list[Event]:
        return [e for e in self.events if e.event_type == event_type]

    def clear(self) -> None:
        self.events.clear()
