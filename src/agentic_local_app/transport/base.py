"""The transport contract (§2.1, §3.12, ADR-004, ADR-020): the ABC, its value objects and the
in-flight guard shared by every implementation.

- :class:`TransportGateway` is the boundary (module map §2.3): the rest of the code knows only
  this ABC; providers (ADR-020) are its implementations, selected by configuration through
  :mod:`agentic_local_app.transport.registry`;
- :class:`PostAck` / :class:`GetResult` are the results of a POST and of a GET;
- :class:`InFlightGuard` implements ``abandon()`` (§2.9): every guarded call can be cancelled and
  then raises ``TransportError(INTERRUPTED, "ABANDONED")``;
- ``OP_*`` are the ``details["operation"]`` values of every transport error (§12.10).
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from agentic_local_app.domain.errors import ConfigError, ErrorType, TransportError

__all__ = [
    "OP_CLOSE",
    "OP_GET",
    "OP_INIT",
    "OP_POST",
    "GetResult",
    "InFlightGuard",
    "PostAck",
    "TransportGateway",
    "validate_options",
]

#: ``details["operation"]`` values (ADR-004 operations, §12.10 ``details.operation``).
OP_INIT = "INIT"
OP_POST = "POST"
OP_GET = "GET"
OP_CLOSE = "CLOSE"

T = TypeVar("T")


@dataclass(frozen=True)
class PostAck:
    """Acknowledgement of a POST (ADR-004: ``{"accepted": true, "message_id": ...}``)."""

    message_id: str
    accepted: bool
    http_status: int


@dataclass(frozen=True)
class GetResult:
    """Result of one GET: the model's messages after the cursor and the new cursor."""

    messages: list[dict[str, Any]]
    cursor: str | None
    http_status: int


class InFlightGuard:
    """Tracks in-flight calls so that :meth:`abandon` can cancel them (§2.9, §3.12).

    Each guarded coroutine runs in its own task. ``abandon()`` cancels those tasks and marks them;
    the awaiting caller then receives ``TransportError(INTERRUPTED, "ABANDONED")`` instead of a bare
    ``CancelledError``. A cancellation that did **not** come from ``abandon()`` (the caller's own task
    being cancelled) propagates unchanged, so the guard never swallows a real cancellation.
    """

    def __init__(self) -> None:
        self._in_flight: dict[asyncio.Task[Any], str] = {}
        self._abandoned: set[asyncio.Task[Any]] = set()

    @property
    def count(self) -> int:
        return len(self._in_flight)

    async def run(self, operation: str, coro: Coroutine[Any, Any, T]) -> T:
        task: asyncio.Task[T] = asyncio.ensure_future(coro)
        self._in_flight[task] = operation
        try:
            return await task
        except BaseException:
            if task in self._abandoned:
                raise TransportError(
                    ErrorType.INTERRUPTED, "ABANDONED", retryable=False, operation=operation
                ) from None
            raise
        finally:
            self._in_flight.pop(task, None)
            self._abandoned.discard(task)

    def abandon(self) -> None:
        """Cancel every in-flight call. Safe to call when nothing is in flight."""
        for task in list(self._in_flight):
            if not task.done():
                self._abandoned.add(task)
                task.cancel()


class TransportGateway(ABC):
    """The transport boundary (module map §2.3): the rest of the code knows only this ABC.

    A provider (ADR-020) is instantiated by the registry as ``cls(config.transport, clock,
    **kwargs)``; the optional keyword arguments currently used by the wiring and the tests are
    ``transport`` (an ``httpx`` transport) and ``sleep`` (the waiting function), and a provider
    ignores those it does not know. A class attribute ``options_model`` (a pydantic model or
    ``None``) validates ``config.transport.options``; without it no option is accepted.
    """

    @abstractmethod
    async def init_conversation(self, instructions: str, metadata: dict[str, Any]) -> str:
        """Create a remote conversation; return its remote identifier."""

    @abstractmethod
    async def post_message(self, remote_conversation_id: str, payload: dict[str, Any]) -> PostAck:
        """POST one complete protocol message (idempotent on ``message_id`` server side)."""

    @abstractmethod
    async def get_messages(self, remote_conversation_id: str, after: str | None) -> GetResult:
        """A single GET of the model's messages after ``after``."""

    @abstractmethod
    async def wait_for_reply(self, remote_conversation_id: str, after: str | None) -> GetResult:
        """Poll ``get_messages`` until at least one message or ``MODEL_GET_TIMEOUT``."""

    @abstractmethod
    async def close_conversation(self, remote_conversation_id: str) -> None:
        """Close the remote conversation (no-op when no close endpoint is configured)."""

    @abstractmethod
    def abandon(self) -> None:
        """Cancel every in-flight call (they raise ``INTERRUPTED / ABANDONED``); reset afterwards."""


def validate_options(provider: type[Any], options: Mapping[str, Any]) -> BaseModel | None:
    """Validate ``transport.options`` for ``provider`` against its ``options_model`` (ADR-020).

    Returns the validated model, or ``None`` when the provider declares no ``options_model`` — the
    options must then be empty. Any problem is a ``ConfigError(TRANSPORT_OPTIONS_INVALID)`` whose
    details name the provider and list the errors (``loc``, ``msg``, ``type``).
    """
    model: type[BaseModel] | None = getattr(provider, "options_model", None)
    name = provider.__name__
    if model is None:
        if options:
            raise ConfigError(
                "TRANSPORT_OPTIONS_INVALID",
                provider=name,
                errors=[
                    {
                        "loc": [key],
                        "msg": "unknown option: this provider takes no options",
                        "type": "extra_forbidden",
                    }
                    for key in sorted(options)
                ],
            )
        return None
    try:
        return model.model_validate(dict(options))
    except ValidationError as exc:
        raise ConfigError(
            "TRANSPORT_OPTIONS_INVALID",
            provider=name,
            errors=[
                {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
                for error in exc.errors(include_url=False)
            ],
        ) from exc
