"""``TelemetryService`` — counters, histograms and a throughput gauge fed by the EventBus (§3.17).

A **non-critical** subscriber (``telemetry``, third in the ADR-015 order). Everything is derived from
the events' ``payload`` and nothing is persisted: the service is an in-process aggregate, exposed
by ``render_text()`` in the Prometheus text exposition format (``GET /metrics``, ADR-018) and by
``metrics()`` as a JSON-friendly dictionary.

Metrics (labels in braces):

- counters — ``events_total{event_type}``, ``task_terminal_total{status}`` (``task.state_changed``
  with a terminal ``to``), ``plan_terminal_total{status}``, ``failures_total{error_type}``
  (``failure.recorded``), ``retries_total`` (``retry.scheduled``), ``rotations_total{outcome}``
  (``rotation.completed`` / ``rotation.failed``), ``interruptions_total``
  (``interruption.requested``), ``breaker_transitions_total{to}``, ``messages_total{direction}``
  (outbound: ``message.outbound`` + ``message.retransmitted``; inbound: ``message.inbound`` +
  ``message.rejected``), ``messages_rejected_total``, ``context_saturations_total``
  (``context.window_state_changed`` to ``SATURATED``), ``budget_exceeded_total``;
- histograms (fixed buckets, count, sum, min, max) — ``task_duration_ms``
  (``task.state_changed`` payload ``duration_ms``), ``cycle_duration_ms`` (``cycle.ended`` payload
  ``duration_ms``), ``message_size_bytes`` (``message.*`` payload ``size_bytes``);
- gauge — ``tasks_completed_per_minute``: tasks that reached ``COMPLETED`` during the last 60 s,
  measured with the injected ``Clock.monotonic_ms()`` (sliding window).

A payload field of the wrong type is ignored, never an error: telemetry must not fail the caller.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.states import ContextWindowState, TaskState
from agentic_local_app.domain.transitions import TERMINAL_PLAN_STATES, TERMINAL_TASK_STATES
from agentic_local_app.observability.event_bus import EventBus

__all__ = [
    "CYCLE_DURATION_BUCKETS_MS",
    "MESSAGE_SIZE_BUCKETS_BYTES",
    "RATE_WINDOW_MS",
    "TASK_DURATION_BUCKETS_MS",
    "TELEMETRY_SUBSCRIBER_NAME",
    "TelemetryService",
]

#: Name under which the service registers on the bus (ADR-015 order: third).
TELEMETRY_SUBSCRIBER_NAME = "telemetry"

#: Sliding window of the ``tasks_completed_per_minute`` gauge.
RATE_WINDOW_MS = 60_000

TASK_DURATION_BUCKETS_MS: tuple[int, ...] = (
    10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 300_000,
)  # fmt: skip
CYCLE_DURATION_BUCKETS_MS: tuple[int, ...] = (
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000,
)  # fmt: skip
MESSAGE_SIZE_BUCKETS_BYTES: tuple[int, ...] = (
    256, 1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576,
)  # fmt: skip

_TERMINAL_TASK_VALUES = frozenset(state.value for state in TERMINAL_TASK_STATES)
_TERMINAL_PLAN_VALUES = frozenset(state.value for state in TERMINAL_PLAN_STATES)
_OUTBOUND_EVENTS = frozenset({EventType.MESSAGE_OUTBOUND, EventType.MESSAGE_RETRANSMITTED})
_INBOUND_EVENTS = frozenset({EventType.MESSAGE_INBOUND, EventType.MESSAGE_REJECTED})

Number = int | float
LabelKey = tuple[tuple[str, str], ...]


def _number(value: Any) -> Number | None:
    """``int`` or ``float`` (``bool`` excluded), else ``None``."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return value


def _label_value(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format(value: Number) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


class _Counter:
    def __init__(self, help_text: str, *label_names: str) -> None:
        self.help = help_text
        self.label_names = label_names
        self.values: dict[LabelKey, int] = {}
        self.reset()

    def reset(self) -> None:
        # an unlabelled counter is always exposed (as 0); labelled ones appear once observed
        self.values = {} if self.label_names else {(): 0}

    def inc(self, **labels: str) -> None:
        key: LabelKey = tuple(sorted(labels.items()))
        self.values[key] = self.values.get(key, 0) + 1

    def render(self, name: str) -> list[str]:
        lines = [f"# HELP {name} {self.help}", f"# TYPE {name} counter"]
        for key, value in sorted(self.values.items()):
            lines.append(f"{name}{_labels(key)} {value}")
        return lines

    def as_dict(self) -> list[dict[str, Any]]:
        return [{"labels": dict(key), "value": value} for key, value in sorted(self.values.items())]


class _Histogram:
    def __init__(self, help_text: str, buckets: tuple[int, ...]) -> None:
        self.help = help_text
        self.buckets = buckets
        self.count = 0
        self.sum: Number = 0
        self.min: Number | None = None
        self.max: Number | None = None
        self._per_bucket: list[int] = []  # observations in (previous bound, bound]
        self._above = 0
        self.reset()

    def reset(self) -> None:
        self.count = 0
        self.sum = 0
        self.min = None
        self.max = None
        self._per_bucket = [0] * len(self.buckets)
        self._above = 0

    def observe(self, value: Number) -> None:
        self.count += 1
        self.sum += value
        self.min = value if self.min is None else min(self.min, value)
        self.max = value if self.max is None else max(self.max, value)
        for index, bound in enumerate(self.buckets):
            if value <= bound:
                self._per_bucket[index] += 1
                return
        self._above += 1

    def cumulative(self) -> list[tuple[str, int]]:
        """Prometheus semantics: ``le`` buckets are cumulative, ``+Inf`` equals ``count``."""
        rows: list[tuple[str, int]] = []
        running = 0
        for bound, count in zip(self.buckets, self._per_bucket, strict=True):
            running += count
            rows.append((str(bound), running))
        rows.append(("+Inf", running + self._above))
        return rows

    def render(self, name: str) -> list[str]:
        lines = [f"# HELP {name} {self.help}", f"# TYPE {name} histogram"]
        for le, value in self.cumulative():
            lines.append(f'{name}_bucket{{le="{le}"}} {value}')
        lines.append(f"{name}_sum {_format(self.sum)}")
        lines.append(f"{name}_count {self.count}")
        return lines

    def render_extrema(self, name: str) -> dict[str, list[str]]:
        """``<name>_max`` / ``<name>_min`` gauge families, present once something was observed."""
        if self.max is None or self.min is None:
            return {}
        return {
            f"{name}_max": _gauge(f"{name}_max", f"Largest observation of {name}", self.max),
            f"{name}_min": _gauge(f"{name}_min", f"Smallest observation of {name}", self.min),
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "sum": self.sum,
            "min": self.min,
            "max": self.max,
            "buckets": dict(self.cumulative()),
        }


def _labels(key: LabelKey) -> str:
    if not key:
        return ""
    return "{" + ",".join(f'{name}="{_escape(value)}"' for name, value in key) + "}"


def _gauge(name: str, help_text: str, value: Number) -> list[str]:
    return [f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {_format(value)}"]


class TelemetryService:
    """Latency, retry, saturation, failure, interruption and throughput indicators (§3.17)."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._counters: dict[str, _Counter] = {
            "events_total": _Counter("Events published on the bus, by type", "event_type"),
            "task_terminal_total": _Counter("Tasks that reached a terminal state", "status"),
            "plan_terminal_total": _Counter("Plans that reached a terminal state", "status"),
            "failures_total": _Counter("Failures recorded, by error type", "error_type"),
            "retries_total": _Counter("Transport retries scheduled"),
            "rotations_total": _Counter("Context rotations, by outcome", "outcome"),
            "interruptions_total": _Counter("User interruptions requested"),
            "breaker_transitions_total": _Counter("Circuit breaker transitions, by target", "to"),
            "messages_total": _Counter("Protocol messages exchanged, by direction", "direction"),
            "messages_rejected_total": _Counter("Inbound messages rejected by the protocol"),
            "context_saturations_total": _Counter("Context windows that became SATURATED"),
            "budget_exceeded_total": _Counter("Sessions that exceeded their budget"),
        }
        self._histograms: dict[str, _Histogram] = {
            "task_duration_ms": _Histogram(
                "Duration of finished tasks in milliseconds", TASK_DURATION_BUCKETS_MS
            ),
            "cycle_duration_ms": _Histogram(
                "Duration of protocol cycles in milliseconds", CYCLE_DURATION_BUCKETS_MS
            ),
            "message_size_bytes": _Histogram(
                "Size of protocol messages in bytes", MESSAGE_SIZE_BUCKETS_BYTES
            ),
        }
        self._completions: deque[int] = deque()

    # ------------------------------------------------------------------ bus ----------------
    def subscribe(self, bus: EventBus) -> None:
        """Register as the non-critical subscriber ``telemetry``."""
        bus.subscribe(self.handle, name=TELEMETRY_SUBSCRIBER_NAME)

    # ------------------------------------------------------------------ events -------------
    def handle(self, event: Event) -> None:
        event_type, payload = event.event_type, event.payload
        self._inc("events_total", event_type=event_type.value)
        target = _label_value(payload.get("to"))
        if event_type is EventType.TASK_STATE_CHANGED:
            if target in _TERMINAL_TASK_VALUES:
                self._inc("task_terminal_total", status=target)
                if target == TaskState.COMPLETED.value:
                    self._completions.append(self._clock.monotonic_ms())
            self._observe("task_duration_ms", payload.get("duration_ms"))
        elif event_type is EventType.PLAN_STATE_CHANGED:
            if target in _TERMINAL_PLAN_VALUES:
                self._inc("plan_terminal_total", status=target)
        elif event_type is EventType.CYCLE_ENDED:
            self._observe("cycle_duration_ms", payload.get("duration_ms"))
        elif event_type is EventType.FAILURE_RECORDED:
            self._inc(
                "failures_total", error_type=_label_value(payload.get("error_type")) or "unknown"
            )
        elif event_type is EventType.RETRY_SCHEDULED:
            self._inc("retries_total")
        elif event_type is EventType.ROTATION_COMPLETED:
            self._inc("rotations_total", outcome="completed")
        elif event_type is EventType.ROTATION_FAILED:
            self._inc("rotations_total", outcome="failed")
        elif event_type is EventType.INTERRUPTION_REQUESTED:
            self._inc("interruptions_total")
        elif event_type is EventType.BREAKER_STATE_CHANGED:
            if target is not None:
                self._inc("breaker_transitions_total", to=target)
        elif event_type in _OUTBOUND_EVENTS:
            self._inc("messages_total", direction="outbound")
            self._observe("message_size_bytes", payload.get("size_bytes"))
        elif event_type in _INBOUND_EVENTS:
            self._inc("messages_total", direction="inbound")
            if event_type is EventType.MESSAGE_REJECTED:
                self._inc("messages_rejected_total")
            self._observe("message_size_bytes", payload.get("size_bytes"))
        elif event_type is EventType.CONTEXT_WINDOW_STATE_CHANGED:
            if target == ContextWindowState.SATURATED.value:
                self._inc("context_saturations_total")
        elif event_type is EventType.BUDGET_EXCEEDED:
            self._inc("budget_exceeded_total")

    def _inc(self, name: str, **labels: str) -> None:
        self._counters[name].inc(**labels)

    def _observe(self, name: str, raw: Any) -> None:
        value = _number(raw)
        if value is not None:
            self._histograms[name].observe(value)

    # ------------------------------------------------------------------ reads --------------
    def tasks_completed_per_minute(self) -> int:
        """Tasks COMPLETED within the last :data:`RATE_WINDOW_MS` (sliding window, monotonic clock)."""
        horizon = self._clock.monotonic_ms() - RATE_WINDOW_MS
        while self._completions and self._completions[0] <= horizon:
            self._completions.popleft()
        return len(self._completions)

    def metrics(self) -> dict[str, Any]:
        """``{"counters": {name: [{labels, value}]}, "histograms": {name: {...}}, "gauges": {...}}``."""
        return {
            "counters": {name: counter.as_dict() for name, counter in self._counters.items()},
            "histograms": {name: hist.as_dict() for name, hist in self._histograms.items()},
            "gauges": {"tasks_completed_per_minute": self.tasks_completed_per_minute()},
        }

    def render_text(self) -> str:
        """Prometheus text exposition format, families sorted by name, label sets sorted."""
        families: dict[str, list[str]] = {}
        for name, counter in self._counters.items():
            families[name] = counter.render(name)
        for name, hist in self._histograms.items():
            families[name] = hist.render(name)
            families.update(hist.render_extrema(name))
        families["tasks_completed_per_minute"] = _gauge(
            "tasks_completed_per_minute",
            "Tasks COMPLETED during the last 60 seconds",
            self.tasks_completed_per_minute(),
        )
        lines = [line for name in sorted(families) for line in families[name]]
        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        for counter in self._counters.values():
            counter.reset()
        for hist in self._histograms.values():
            hist.reset()
        self._completions.clear()
