"""Persistence: the ConversationStore interface, its in-memory and SQLite implementations."""

from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.persistence.memory import InMemoryConversationStore

__all__ = ["ConversationStore", "InMemoryConversationStore"]
