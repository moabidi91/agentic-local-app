"""Resilience layer (§3.13–§3.15, §7): failure classification and policy, bounded deterministic
backoff, circuit breaking.

``FailureManager`` is the single place where the §7 policy is decided; ``RetryController`` computes
the delays; ``CircuitBreaker`` protects the remote endpoint from repeated failing calls.
"""

from agentic_local_app.resilience.circuit_breaker import CircuitBreaker
from agentic_local_app.resilience.failure_manager import (
    BREAKER_FED_ERROR_TYPES,
    Decision,
    DecisionKind,
    FailureManager,
)
from agentic_local_app.resilience.retry_controller import RetryController

__all__ = [
    "BREAKER_FED_ERROR_TYPES",
    "CircuitBreaker",
    "Decision",
    "DecisionKind",
    "FailureManager",
    "RetryController",
]
