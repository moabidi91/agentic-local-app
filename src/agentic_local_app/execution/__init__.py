"""Execution layer: the shell boundary and what surrounds one task (phase 4), the plan runner (phase 5).

- ``platform``          PlatformAdapter (POSIX / Windows): shell, launch argv, spawn flags, termination
- ``executor``          CommandExecutor (ABC), SubprocessCommandExecutor, CommandSpec, RawExecution
- ``payload_guard``     PayloadGuard: budgets, truncation, message cap, chunk serving
- ``result_collector``  ResultCollector: one execution_result per terminal plan
- ``plan_runner``       PlanRunner: DAG, locks, workers, stop conditions, drains -> PlanOutcome
"""

from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandExecutor,
    CommandSpec,
    OutputChunk,
    RawExecution,
    SubprocessCommandExecutor,
)
from agentic_local_app.execution.payload_guard import (
    ChunkError,
    ChunkResult,
    PayloadGuard,
    TruncatedOutput,
    decode_output,
)
from agentic_local_app.execution.plan_runner import (
    BUDGET_DURATION_STOP_REASON,
    BUDGET_EXCEEDED_REASON,
    INTERRUPT_REASON,
    PLAN_STOPPED_REASON,
    SPAWN_FAILED_REASON,
    FailureRecorder,
    PlanOutcome,
    PlanRunner,
)
from agentic_local_app.execution.platform import (
    LaunchSpec,
    PlatformAdapter,
    PosixPlatformAdapter,
    ProcessTable,
    WindowsPlatformAdapter,
    select_platform,
)
from agentic_local_app.execution.result_collector import ResultCollector

__all__ = [
    "BUDGET_DURATION_STOP_REASON",
    "BUDGET_EXCEEDED_REASON",
    "INTERRUPT_REASON",
    "PLAN_STOPPED_REASON",
    "SPAWN_FAILED_REASON",
    "CancellationToken",
    "ChunkError",
    "ChunkResult",
    "CommandExecutor",
    "CommandSpec",
    "FailureRecorder",
    "LaunchSpec",
    "OutputChunk",
    "PayloadGuard",
    "PlanOutcome",
    "PlanRunner",
    "PlatformAdapter",
    "PosixPlatformAdapter",
    "ProcessTable",
    "RawExecution",
    "ResultCollector",
    "SubprocessCommandExecutor",
    "TruncatedOutput",
    "WindowsPlatformAdapter",
    "decode_output",
    "select_platform",
]
