"""agentic_local_app — local orchestrator for a single remote model over a strict POST/GET protocol.

The package is organised by architectural layer (see docs/architecture/09-module-map.md):

- ``domain``        pure state, transitions, records and errors (no I/O)
- ``lifecycle``     ConversationLifecycleManager (sole owner of conversation transitions)
- ``protocol``      message schemas, ProtocolAdapter (build / parse / validate)
- ``persistence``   ConversationStore (in-memory and SQLite) and blob storage
- ``execution``     CommandExecutor, PayloadGuard, ResultCollector, PlanRunner
- ``interruption``  InterruptionHandler
- ``transport``     TransportGateway (HTTP, configurable endpoints) and fakes
- ``resilience``    FailureManager, RetryController, CircuitBreaker
- ``context``       ContextReducer and context-window monitoring
- ``observability`` EventBus, AuditLog, TelemetryService, ExecutionTracker
- ``orchestration`` ProtocolOrchestrator, ConversationManager, RecoveryCoordinator
- ``interfaces``    CLI and local HTTP API
- ``testing``       test doubles and the mock model server
"""

__version__ = "0.1.0"
