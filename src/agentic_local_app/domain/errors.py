"""Error taxonomy (§6) and the exceptions that carry a normalized error through the system.

Every failure that crosses a component boundary is a :class:`NormalizedError` (the attributes of
§6 verbatim). Exceptions wrap one so that ``FailureManager`` can classify by ``error_type`` without
inspecting exception classes.
"""

from __future__ import annotations

from enum import unique
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.domain.states import StrEnum


@unique
class ErrorType(StrEnum):
    AUTHN_ERROR = "AUTHN_ERROR"
    AUTHZ_ERROR = "AUTHZ_ERROR"
    NETWORK_ERROR = "NETWORK_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    RATE_LIMIT_ERROR = "RATE_LIMIT_ERROR"
    MODEL_PROTOCOL_ERROR = "MODEL_PROTOCOL_ERROR"
    MODEL_CONTEXT_WINDOW_ERROR = "MODEL_CONTEXT_WINDOW_ERROR"
    TASK_EXECUTION_ERROR = "TASK_EXECUTION_ERROR"
    PERSISTENCE_ERROR = "PERSISTENCE_ERROR"
    BUDGET_EXCEEDED = "BUDGET_EXCEEDED"
    ROTATION_FAILED = "ROTATION_FAILED"
    INTERRUPTED = "INTERRUPTED"
    SYSTEM_ERROR = "SYSTEM_ERROR"


@unique
class Severity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


#: Error types that may be retried (§7.1). SYSTEM_ERROR is retryable only when ``transient`` is set
#: in ``details`` (FailureManager decides).
RETRYABLE_ERROR_TYPES: frozenset[ErrorType] = frozenset(
    {ErrorType.NETWORK_ERROR, ErrorType.TIMEOUT_ERROR, ErrorType.RATE_LIMIT_ERROR}
)


class NormalizedError(BaseModel):
    """The normalized error attributes of §6."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    error_type: ErrorType
    error_code: str
    severity: Severity = Severity.HIGH
    origin: str
    retryable: bool = False
    recoverable: bool = True
    attempt: int = 1
    max_attempts: int = 1
    details: dict[str, Any] = Field(default_factory=dict)

    def with_attempt(self, attempt: int, max_attempts: int) -> NormalizedError:
        return self.model_copy(update={"attempt": attempt, "max_attempts": max_attempts})


class AppError(Exception):
    """Base class of every exception raised by the application. Carries a :class:`NormalizedError`."""

    def __init__(self, error: NormalizedError, message: str | None = None) -> None:
        self.error = error
        super().__init__(message or f"{error.error_type.value}/{error.error_code}: {error.details}")

    @property
    def error_type(self) -> ErrorType:
        return self.error.error_type


def _err(
    error_type: ErrorType,
    code: str,
    origin: str,
    *,
    severity: Severity = Severity.HIGH,
    retryable: bool = False,
    recoverable: bool = True,
    **details: Any,
) -> NormalizedError:
    return NormalizedError(
        error_type=error_type,
        error_code=code,
        severity=severity,
        origin=origin,
        retryable=retryable,
        recoverable=recoverable,
        details=details,
    )


class InvalidTransitionError(AppError):
    """A state change absent from the transition tables (docs/architecture/01-state-machines.md)."""

    def __init__(self, *, entity: str, current: str, target: str) -> None:
        self.entity, self.current, self.target = entity, current, target
        super().__init__(
            _err(
                ErrorType.SYSTEM_ERROR,
                "INVALID_TRANSITION",
                "StateMachine",
                severity=Severity.CRITICAL,
                recoverable=False,
                entity=entity,
                current=current,
                target=target,
            ),
            f"invalid {entity} transition {current} -> {target}",
        )


class ProtocolError(AppError):
    """Malformed or unexpected model message (MODEL_PROTOCOL_ERROR, never retried)."""

    def __init__(self, code: str, **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.MODEL_PROTOCOL_ERROR,
                code,
                "ProtocolAdapter",
                recoverable=False,
                **details,
            )
        )


class PersistenceError(AppError):
    def __init__(self, code: str, *, transient: bool = False, **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.PERSISTENCE_ERROR,
                code,
                "ConversationStore",
                severity=Severity.CRITICAL,
                retryable=transient,
                recoverable=transient,
                transient=transient,
                **details,
            )
        )


class BudgetExceededError(AppError):
    def __init__(self, limit: str, limit_value: int, consumed: int) -> None:
        self.limit = limit
        super().__init__(
            _err(
                ErrorType.BUDGET_EXCEEDED,
                f"BUDGET_{limit.upper()}",
                "ProtocolOrchestrator",
                recoverable=False,
                limit=limit,
                limit_value=limit_value,
                consumed=consumed,
            )
        )


class RotationFailedError(AppError):
    def __init__(self, code: str, **details: Any) -> None:
        super().__init__(
            _err(ErrorType.ROTATION_FAILED, code, "ContextReducer", recoverable=False, **details)
        )


class SessionInterruptedError(AppError):
    """Raised inside the protocol loop when the user interrupt signal is observed."""

    def __init__(self, origin: str = "InterruptionHandler", **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.INTERRUPTED,
                "USER_INTERRUPT",
                origin,
                severity=Severity.MEDIUM,
                recoverable=True,
                **details,
            )
        )


class TransportError(AppError):
    """Any failure of the TransportGateway, already classified (ADR-004 mapping table)."""

    def __init__(
        self, error_type: ErrorType, code: str, *, retryable: bool, **details: Any
    ) -> None:
        super().__init__(_err(error_type, code, "TransportGateway", retryable=retryable, **details))


class TaskExecutionError(AppError):
    """The executor itself failed (spawn error...). A failing *command* is not an error but a result."""

    def __init__(self, code: str, **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.TASK_EXECUTION_ERROR, code, "CommandExecutor", recoverable=True, **details
            )
        )


class ConfigError(AppError):
    def __init__(self, code: str, **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.SYSTEM_ERROR,
                code,
                "Config",
                severity=Severity.CRITICAL,
                recoverable=False,
                **details,
            )
        )


class GenericSystemError(AppError):
    """Generic SYSTEM_ERROR (the builtin ``SystemError`` name is deliberately avoided)."""

    def __init__(self, code: str, origin: str, *, transient: bool = False, **details: Any) -> None:
        super().__init__(
            _err(
                ErrorType.SYSTEM_ERROR,
                code,
                origin,
                retryable=transient,
                recoverable=transient,
                transient=transient,
                **details,
            )
        )
