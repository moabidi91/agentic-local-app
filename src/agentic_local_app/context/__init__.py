"""Context window and rotation (spec §2.6, §3.11, §5.4, §10 ; phase 8).

- :mod:`~agentic_local_app.context.window` — ``ContextWindowMonitor``: the byte metric, the
  thresholds and the pure evaluation of the window state (ADR-013, ADR-019 §2);
- :mod:`~agentic_local_app.context.reducer` — ``ContextReducer``: the structured summary and its
  deterministic reduction steps (ADR-005);
- :mod:`~agentic_local_app.context.rotation` — ``RotationCoordinator``: the rotation sequence with
  the retransmission of the pending message (ADR-014, ADR-007, ADR-012, ADR-019 §5).
"""

from __future__ import annotations

from agentic_local_app.context.reducer import (
    REDUCTION_STEPS,
    STATE_SUMMARY_SECTIONS,
    SUMMARY_EXCEEDS_BUDGET,
    ContextReducer,
    SummaryDraft,
)
from agentic_local_app.context.rotation import (
    ROTATION_LIMIT_REACHED,
    PendingOutbound,
    RotationCoordinator,
    RotationResult,
)
from agentic_local_app.context.window import (
    GET_TIMEOUT_CODE,
    ContextThresholds,
    ContextWindowMonitor,
)

__all__ = [
    "GET_TIMEOUT_CODE",
    "REDUCTION_STEPS",
    "ROTATION_LIMIT_REACHED",
    "STATE_SUMMARY_SECTIONS",
    "SUMMARY_EXCEEDS_BUDGET",
    "ContextReducer",
    "ContextThresholds",
    "ContextWindowMonitor",
    "PendingOutbound",
    "RotationCoordinator",
    "RotationResult",
    "SummaryDraft",
]
