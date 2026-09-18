"""Lifecycle layer: the sole owner of session and conversation state transitions (spec §3.3).

Every transition goes through the tables of :mod:`agentic_local_app.domain.transitions`, is
persisted, then published on the EventBus (ADR-015).
"""

from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager

__all__ = ["ConversationLifecycleManager"]
