"""``open_store(config)`` — the runtime ``ConversationStore`` (ADR-001, ADR-018).

The database lives at ``<app.data_dir>/agentic.db``; the directory is created when missing. Any
filesystem problem surfaces as :class:`~agentic_local_app.domain.errors.PersistenceError`
(``DATA_DIR_UNAVAILABLE``), any SQLite problem as ``SQLITE_ERROR`` (see ``sqlite_store``).
"""

from __future__ import annotations

from pathlib import Path

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.errors import PersistenceError
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.persistence.sqlite_store import SqliteConversationStore

__all__ = ["DB_FILENAME", "open_store"]

#: Name of the SQLite database inside ``app.data_dir``.
DB_FILENAME = "agentic.db"


def open_store(config: AppConfig) -> ConversationStore:
    """Create ``config.app.data_dir`` if needed and open ``<data_dir>/agentic.db``."""
    data_dir = Path(config.app.data_dir)
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise PersistenceError("DATA_DIR_UNAVAILABLE", path=str(data_dir), error=str(exc)) from exc
    return SqliteConversationStore(data_dir / DB_FILENAME)
