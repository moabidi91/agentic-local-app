"""Orchestration layer (spec §3.1, §3.2, §3.18 ; phase 9a).

- :mod:`~agentic_local_app.orchestration.protocol_orchestrator` — ``ProtocolOrchestrator``: the
  protocol loop of §14 amended by the ADRs (budget, saturation check before every POST, rotation
  with retransmission, failure policy, interruption);
- :mod:`~agentic_local_app.orchestration.conversation_manager` — ``ConversationManager``: the
  façade used by the interfaces (start, follow-up, resume, interrupt, wait, reads, shutdown);
- :mod:`~agentic_local_app.orchestration.recovery` — ``RecoveryCoordinator``: the restart policy of
  ADR-016 and its ``RecoveryReport``;
- :mod:`~agentic_local_app.orchestration.wiring` — ``build_application``: assembles every component
  with the injection points of §18.3.
"""

from __future__ import annotations

from agentic_local_app.orchestration.conversation_manager import ConversationManager
from agentic_local_app.orchestration.protocol_orchestrator import ProtocolOrchestrator
from agentic_local_app.orchestration.recovery import (
    RESTART_REASON,
    RecoveryAction,
    RecoveryCoordinator,
    RecoveryReport,
)
from agentic_local_app.orchestration.wiring import Application, build_application

__all__ = [
    "RESTART_REASON",
    "Application",
    "ConversationManager",
    "ProtocolOrchestrator",
    "RecoveryAction",
    "RecoveryCoordinator",
    "RecoveryReport",
    "build_application",
]
