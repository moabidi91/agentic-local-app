"""``ContextWindowMonitor`` — the saturation metric and its thresholds (spec §5.4, §10 ; ADR-013,
ADR-019 §2).

Everything here is **pure**: the monitor reads the persisted ``context_bytes`` of a conversation,
the size of the message about to be sent and the classified error, and answers with a
``ContextWindowState``. Applying the state (``transition_context_window``) and triggering the
rotation are the orchestrator's job (phase 9) and the ``RotationCoordinator``'s (phase 8).

The metric (ADR-013 §1) is the number of bytes of the canonical JSON serialisation, **before**
gzip, of everything sent and received in the conversation, plus the UTF-8 size of the protocol
instructions sent at init (ADR-004). ``account`` and ``instructions_bytes`` are the two helpers
that keep this definition in one place.

Thresholds are computed with exact decimal arithmetic on the configured ratios (``Fraction`` of
the ratio's shortest repr): ``0.7 × 100`` is 70, never 70.000000000000014.
"""

from __future__ import annotations

import math
from fractions import Fraction
from typing import NamedTuple

from agentic_local_app.config import ContextSection
from agentic_local_app.domain.errors import ErrorType, NormalizedError
from agentic_local_app.domain.models import ConversationRecord
from agentic_local_app.domain.states import ContextWindowState

__all__ = ["GET_TIMEOUT_CODE", "ContextThresholds", "ContextWindowMonitor"]

#: ``error_code`` of an exhausted GET (``TransportGateway.wait_for_reply``), ADR-019 §2.
GET_TIMEOUT_CODE = "MODEL_GET_TIMEOUT"

#: HEALTHY < WARNING < SATURATED — the state of a conversation never goes back on its own.
_SEVERITY: dict[ContextWindowState, int] = {
    ContextWindowState.HEALTHY: 0,
    ContextWindowState.WARNING: 1,
    ContextWindowState.SATURATED: 2,
}


class ContextThresholds(NamedTuple):
    """The three byte thresholds of ADR-013 for the configured budget."""

    warning_bytes: int
    saturation_bytes: int
    budget_bytes: int


def _threshold(ratio: float, budget: int) -> int:
    """``ceil(ratio × budget)`` computed exactly from the decimal the ratio was written as."""
    return math.ceil(Fraction(repr(ratio)) * budget)


class ContextWindowMonitor:
    """Pure evaluation of the context window state (§5.4) from bytes, projection and errors."""

    def __init__(self, config: ContextSection) -> None:
        self._config = config
        self._thresholds = ContextThresholds(
            warning_bytes=_threshold(config.warning_ratio, config.budget_bytes),
            saturation_bytes=_threshold(config.saturation_ratio, config.budget_bytes),
            budget_bytes=config.budget_bytes,
        )

    # ------------------------------------------------------------------ metric -------------
    def thresholds(self) -> ContextThresholds:
        """``(warning_bytes, saturation_bytes, budget_bytes)`` — 280 000 / 360 000 / 400 000 by default."""
        return self._thresholds

    @staticmethod
    def account(conversation_bytes: int, message_bytes: int) -> int:
        """The counter after one more message: a plain sum of canonical JSON sizes (before gzip)."""
        if conversation_bytes < 0 or message_bytes < 0:
            raise ValueError("byte counts cannot be negative")
        return conversation_bytes + message_bytes

    @staticmethod
    def instructions_bytes(instructions: str) -> int:
        """Size of the protocol instructions sent at init, counted once per conversation (ADR-004)."""
        return len(instructions.encode("utf-8"))

    # ------------------------------------------------------------------ evaluation ---------
    def evaluate(
        self,
        conversation: ConversationRecord,
        *,
        projected_outbound_bytes: int = 0,
        error: NormalizedError | None = None,
    ) -> ContextWindowState:
        """The window state after ``error`` and/or the projected POST (06 §1.1, ADR-013 §3).

        In order: ``MODEL_CONTEXT_WINDOW_ERROR`` → SATURATED; an unusable reply while the window
        is WARNING (ADR-019 §2, see :meth:`should_rotate_on_unusable_reply`) → SATURATED;
        ``context_bytes + projected > budget`` → SATURATED (the message must not be sent);
        ``context_bytes ≥ saturation`` → SATURATED; ``context_bytes ≥ warning`` → WARNING; else
        HEALTHY. The result is **monotone**: never below the conversation's current state — the
        only way down is ``SATURATED → HEALTHY`` on the child's ack, done by the rotation.
        """
        if projected_outbound_bytes < 0:
            raise ValueError("projected_outbound_bytes cannot be negative")
        computed = self._compute(conversation, projected_outbound_bytes, error)
        current = conversation.context_window_state
        return computed if _SEVERITY[computed] >= _SEVERITY[current] else current

    def should_rotate_on_unusable_reply(
        self, conversation: ConversationRecord, error: NormalizedError
    ) -> bool:
        """ADR-019 §2: a protocol error or an exhausted GET timeout **while WARNING** rotates once
        instead of failing, when ``rotate_on_unusable_reply_in_warning`` is on."""
        if not self._config.rotate_on_unusable_reply_in_warning:
            return False
        if conversation.context_window_state is not ContextWindowState.WARNING:
            return False
        if error.error_type is ErrorType.MODEL_PROTOCOL_ERROR:
            return True
        return error.error_type is ErrorType.TIMEOUT_ERROR and error.error_code == GET_TIMEOUT_CODE

    # ------------------------------------------------------------------ internals ----------
    def _compute(
        self,
        conversation: ConversationRecord,
        projected_outbound_bytes: int,
        error: NormalizedError | None,
    ) -> ContextWindowState:
        if error is not None:
            if error.error_type is ErrorType.MODEL_CONTEXT_WINDOW_ERROR:
                return ContextWindowState.SATURATED
            if self.should_rotate_on_unusable_reply(conversation, error):
                return ContextWindowState.SATURATED
        current_bytes = conversation.context_bytes
        thresholds = self._thresholds
        if current_bytes + projected_outbound_bytes > thresholds.budget_bytes:
            return ContextWindowState.SATURATED
        if current_bytes >= thresholds.saturation_bytes:
            return ContextWindowState.SATURATED
        if current_bytes >= thresholds.warning_bytes:
            return ContextWindowState.WARNING
        return ContextWindowState.HEALTHY
