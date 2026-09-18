"""``build_application`` — assemble every component with the injection points of §18.3.

Production defaults: ``open_store(config)`` (SQLite under ``app.data_dir``), the transport provider
named by ``transport.provider`` (``TransportRegistry.create``, ADR-020; ``generic_http`` by default)
decorated with the message codec named by ``transport.codec`` (``CodecRegistry.create``, ADR-021;
``passthrough`` by default, which leaves the transport bare — an injected transport is decorated
the same way), ``SubprocessCommandExecutor`` (with its platform adapter, used by the recovery to
terminate orphans, ADR-016), ``SystemClock``, ``UuidIdGenerator``. Tests inject their doubles
through the keyword arguments; an injected executor is a test double, so no platform adapter (and
no orphan termination against the real process table) unless one is given explicitly.

Subscription order fixed by ADR-015: ``AuditLog`` (critical) → ``ExecutionTracker`` →
``TelemetryService`` (when ``telemetry.enabled``). The ``RecoveryCoordinator`` runs **after** the
subscribers are registered so that its events are audited (ADR-016 §4), and its report is handed
to the ``ConversationManager``.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from agentic_local_app.config import AppConfig
from agentic_local_app.context.reducer import ContextReducer
from agentic_local_app.context.rotation import RotationCoordinator
from agentic_local_app.context.window import ContextWindowMonitor
from agentic_local_app.domain.clock import Clock, SystemClock
from agentic_local_app.domain.ids import IdGenerator, UuidIdGenerator
from agentic_local_app.execution.executor import CommandExecutor, SubprocessCommandExecutor
from agentic_local_app.execution.payload_guard import PayloadGuard
from agentic_local_app.execution.plan_runner import PlanRunner
from agentic_local_app.execution.platform import PlatformAdapter
from agentic_local_app.interruption.handler import InterruptionHandler
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.audit_log import AuditLog
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.observability.execution_tracker import ExecutionTracker
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.orchestration.conversation_manager import ConversationManager
from agentic_local_app.orchestration.protocol_orchestrator import ProtocolOrchestrator, SleepFn
from agentic_local_app.orchestration.recovery import RecoveryCoordinator, RecoveryReport
from agentic_local_app.persistence.factory import open_store
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.adapter import ProtocolAdapter, render_instructions
from agentic_local_app.resilience.circuit_breaker import CircuitBreaker
from agentic_local_app.resilience.failure_manager import FailureManager
from agentic_local_app.resilience.retry_controller import RetryController
from agentic_local_app.transport.base import TransportGateway
from agentic_local_app.transport.codecs import CodecRegistry, MessageCodec, apply_codec
from agentic_local_app.transport.registry import TransportRegistry

__all__ = ["Application", "build_application"]


@dataclass
class Application:
    """Every wired component, for the interfaces and the tests."""

    config: AppConfig
    store: ConversationStore
    bus: EventBus
    clock: Clock
    ids: IdGenerator
    lifecycle: ConversationLifecycleManager
    adapter: ProtocolAdapter
    transport: TransportGateway
    executor: CommandExecutor
    payload_guard: PayloadGuard
    retry: RetryController
    breaker: CircuitBreaker
    failure_manager: FailureManager
    monitor: ContextWindowMonitor
    reducer: ContextReducer
    rotation: RotationCoordinator
    interruption: InterruptionHandler
    plan_runner: PlanRunner
    orchestrator: ProtocolOrchestrator
    audit: AuditLog
    tracker: ExecutionTracker
    telemetry: TelemetryService
    recovery: RecoveryCoordinator
    manager: ConversationManager
    instructions: str
    recovery_report: RecoveryReport | None = None

    def close(self) -> None:
        """Release the store synchronously (the HTTP client, if any, needs :meth:`aclose`)."""
        self.store.close()

    async def aclose(self) -> None:
        """Stop every running loop, close the transport when it owns a client, close the store."""
        await self.manager.shutdown()
        aclose = getattr(self.transport, "aclose", None)
        if callable(aclose):
            result: Any = aclose()
            if asyncio.iscoroutine(result):
                await result
        self.store.close()


def build_application(
    config: AppConfig,
    *,
    store: ConversationStore | None = None,
    transport: TransportGateway | None = None,
    executor: CommandExecutor | None = None,
    clock: Clock | None = None,
    ids: IdGenerator | None = None,
    run_recovery: bool = True,
    bus: EventBus | None = None,
    instructions: str | None = None,
    sleep: SleepFn | None = None,
    platform: PlatformAdapter | None = None,
    codec: MessageCodec | None = None,
) -> Application:
    """Wire the application; ``None`` selects the production implementation of each boundary."""
    clock = clock if clock is not None else SystemClock()
    ids = ids if ids is not None else UuidIdGenerator()
    bus = bus if bus is not None else EventBus()
    # before the store and the transport: a misconfiguration must not open a database or a client
    if codec is None:
        codec = CodecRegistry.create(config)
    if transport is None:
        transport_kwargs: dict[str, Any] = {"sleep": sleep} if sleep is not None else {}
        transport = TransportRegistry.create(config, clock=clock, **transport_kwargs)
    transport = apply_codec(transport, codec)  # the passthrough codec leaves it bare (ADR-021)
    store = store if store is not None else open_store(config)
    if executor is None:
        subprocess_executor = SubprocessCommandExecutor(config.execution, clock)
        executor = subprocess_executor
        if platform is None:
            platform = subprocess_executor.platform
    text = instructions if instructions is not None else render_instructions(config)

    # observability first: the subscribers must see every event of the wiring (ADR-015 order)
    audit = AuditLog(store, clock, ids)
    audit.subscribe(bus)
    tracker = ExecutionTracker(store, clock)
    tracker.subscribe(bus)
    telemetry = TelemetryService(clock)
    if config.telemetry.enabled:
        telemetry.subscribe(bus)

    lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
    adapter = ProtocolAdapter(config)
    payload_guard = PayloadGuard(config.payload)
    retry = RetryController(config.retry)
    breaker = CircuitBreaker(config.circuit_breaker, clock, bus)
    failure_manager = FailureManager(config, store, bus, clock, ids, retry=retry, breaker=breaker)
    monitor = ContextWindowMonitor(config.context)
    reducer = ContextReducer(config, store, clock, ids)
    rotation = RotationCoordinator(
        config, store, bus, clock, ids, lifecycle, adapter, transport, reducer, monitor, text
    )
    interruption = InterruptionHandler(
        store, bus, lifecycle, clock, ids, config, transport=transport
    )
    plan_runner = PlanRunner(
        store, bus, executor, payload_guard, clock, ids, config, failure_manager=failure_manager
    )
    orchestrator_kwargs: dict[str, Any] = {}
    if sleep is not None:
        orchestrator_kwargs["sleep"] = sleep
    orchestrator = ProtocolOrchestrator(
        config,
        store,
        bus,
        clock,
        ids,
        lifecycle,
        adapter,
        transport,
        plan_runner,
        payload_guard,
        failure_manager,
        breaker,
        monitor,
        rotation,
        interruption,
        text,
        **orchestrator_kwargs,
    )
    recovery = RecoveryCoordinator(store, lifecycle, bus, clock, ids, config, platform=platform)
    report = recovery.recover() if run_recovery else None
    manager = ConversationManager(
        config=config,
        store=store,
        bus=bus,
        clock=clock,
        ids=ids,
        lifecycle=lifecycle,
        orchestrator=orchestrator,
        interruption=interruption,
        tracker=tracker,
        audit=audit,
        telemetry=telemetry,
        recovery_report=report,
    )
    return Application(
        config=config,
        store=store,
        bus=bus,
        clock=clock,
        ids=ids,
        lifecycle=lifecycle,
        adapter=adapter,
        transport=transport,
        executor=executor,
        payload_guard=payload_guard,
        retry=retry,
        breaker=breaker,
        failure_manager=failure_manager,
        monitor=monitor,
        reducer=reducer,
        rotation=rotation,
        interruption=interruption,
        plan_runner=plan_runner,
        orchestrator=orchestrator,
        audit=audit,
        tracker=tracker,
        telemetry=telemetry,
        recovery=recovery,
        manager=manager,
        instructions=text,
        recovery_report=report,
    )
