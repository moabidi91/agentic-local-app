"""Persistence: the ConversationStore interface, its in-memory and SQLite implementations."""

from agentic_local_app.persistence.factory import open_store
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.persistence.sqlite_store import SqliteConversationStore

__all__ = [
    "ConversationStore",
    "InMemoryConversationStore",
    "SqliteConversationStore",
    "open_store",
]
