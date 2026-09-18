"""Interruption layer: the user interrupt procedure of §9 amended by ADR-006 (phase 6).

- ``handler``  InterruptionHandler: session token, loop registration, bounded drain, idempotent
               sweep of tasks / plan / cycle / conversation, session back to READY -> InterruptionReport
"""

from agentic_local_app.interruption.handler import (
    CYCLE_ENTITY,
    INTERRUPTION_FAILED_REASON,
    USER_INTERRUPT_REASON,
    InterruptionHandler,
    InterruptionReport,
)

__all__ = [
    "CYCLE_ENTITY",
    "INTERRUPTION_FAILED_REASON",
    "USER_INTERRUPT_REASON",
    "InterruptionHandler",
    "InterruptionReport",
]
