"""SQLite ``ConversationStore`` (ADR-001, ADR-011, ADR-015) — the runtime persistence layer.

Semantics are those of :class:`~agentic_local_app.persistence.memory.InMemoryConversationStore`
(the phase-3 contract suite runs against both): upserts keyed per record type, listings oldest
first in insertion order (``rowid``) except sessions (newest first), tasks by plan insertion order
then ``order_index`` (ADR-017), an append-only audit chain, range reads on blobs. One refinement:
thanks to ``SAVEPOINT``, an exception escaping a *nested* ``transaction()`` block undoes that block's
writes even when the outer block catches it and commits (the in-memory store keeps them).

Design:

- **one ``sqlite3`` connection per store** (``check_same_thread=False``, ``isolation_level=None``):
  the store issues ``BEGIN IMMEDIATE`` / ``COMMIT`` / ``ROLLBACK`` itself, nested ``transaction()``
  blocks become ``SAVEPOINT``s (an exception escaping a block undoes that block's writes and joins
  the outer block otherwise);
- **one table per record**, generated from the pydantic ``model_fields``: columns are typed from the
  field annotation (``TEXT`` / ``INTEGER`` / ``REAL`` / ``BLOB``), ``NOT NULL`` when the annotation
  does not allow ``None``; dict / list / tuple / nested models are stored as canonical JSON
  (ADR-017), enums as their value, datetimes as fixed-width ISO-8601 UTC text (so that text order
  is chronological order), bytes as ``BLOB``;
- records are written from ``record.model_dump()`` and read back with ``Model.model_validate``,
  so **no field can be lost silently**: a new field in a model is a new column in the schema;
- ``PRAGMA journal_mode=WAL`` (files only), ``synchronous=FULL``, ``foreign_keys=ON``; the schema
  is created idempotently (``IF NOT EXISTS``) and stamped in ``schema_version``;
- every ``sqlite3.Error`` surfaces as :class:`~agentic_local_app.domain.errors.PersistenceError`
  (``SQLITE_ERROR``, ``details.sqlite``), transient when the database is locked or busy; any
  operation after ``close()`` is ``STORE_CLOSED``.

No wall clock, no identifier generation: every timestamp comes from the records (ADR-017).
"""

from __future__ import annotations

import json
import os
import sqlite3
import types
from collections.abc import Iterable, Iterator, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Generic, TypeVar, Union, get_args, get_origin

from pydantic import BaseModel, ValidationError

from agentic_local_app.domain.canonical import canonical_json
from agentic_local_app.domain.errors import PersistenceError
from agentic_local_app.domain.models import (
    AuditEvent,
    BlobRecord,
    ContextSummaryRecord,
    ConversationRecord,
    CycleRecord,
    FailureRecord,
    MessageRecord,
    PlanRecord,
    Record,
    RetryDecisionRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ConversationState,
    MessageDirection,
    OutputStream,
    PlanState,
    SessionState,
    TaskState,
)
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "MEMORY_PATH",
    "SCHEMA_VERSION",
    "SqliteConversationStore",
    "persistence_error_from_sqlite",
]

#: Version stamped in ``schema_version``; a database with another version is refused (no migration in v1).
SCHEMA_VERSION = 1
#: The ``sqlite3`` spelling of a private in-memory database.
MEMORY_PATH = ":memory:"

_TRANSIENT_MARKERS = ("locked", "busy")

R = TypeVar("R", bound=Record)


# ------------------------------------------------------------------------------------------------
# Errors
# ------------------------------------------------------------------------------------------------
def persistence_error_from_sqlite(exc: sqlite3.Error) -> PersistenceError:
    """Map any ``sqlite3.Error`` onto ``PersistenceError(SQLITE_ERROR)``.

    ``OperationalError`` mentioning a locked or busy database is transient (retryable); everything
    else is not.
    """
    message = str(exc)
    lowered = message.lower()
    transient = isinstance(exc, sqlite3.OperationalError) and any(
        marker in lowered for marker in _TRANSIENT_MARKERS
    )
    return PersistenceError(
        "SQLITE_ERROR", transient=transient, sqlite=message, sqlite_type=type(exc).__name__
    )


# ------------------------------------------------------------------------------------------------
# Record <-> row mapping, driven by the model annotations
# ------------------------------------------------------------------------------------------------
class _Kind(Enum):
    TEXT = "TEXT"
    INTEGER = "INTEGER"
    BOOLEAN = "BOOLEAN"
    REAL = "REAL"
    BLOB = "BLOB"
    DATETIME = "DATETIME"
    ENUM = "ENUM"
    JSON = "JSON"


_SQL_TYPES: dict[_Kind, str] = {
    _Kind.TEXT: "TEXT",
    _Kind.INTEGER: "INTEGER",
    _Kind.BOOLEAN: "INTEGER",
    _Kind.REAL: "REAL",
    _Kind.BLOB: "BLOB",
    _Kind.DATETIME: "TEXT",
    _Kind.ENUM: "TEXT",
    _Kind.JSON: "TEXT",
}


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """``X | None`` -> ``(X, True)``; anything else -> ``(annotation, False)``."""
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        inner = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(inner) != 1:
            raise TypeError(f"unsupported union annotation for a column: {annotation!r}")
        return inner[0], True
    return annotation, False


def _kind_for(annotation: Any) -> tuple[_Kind, type[Enum] | None]:
    origin = get_origin(annotation) or annotation
    if isinstance(origin, type):
        if issubclass(origin, bool):
            return _Kind.BOOLEAN, None
        if issubclass(origin, Enum):  # before ``str``: StrEnum is a str subclass
            return _Kind.ENUM, origin
        if issubclass(origin, int):
            return _Kind.INTEGER, None
        if issubclass(origin, float):
            return _Kind.REAL, None
        if issubclass(origin, str):
            return _Kind.TEXT, None
        if issubclass(origin, bytes | bytearray):
            return _Kind.BLOB, None
        if issubclass(origin, datetime):
            return _Kind.DATETIME, None
        if issubclass(origin, BaseModel | dict | list | tuple | set | frozenset):
            return _Kind.JSON, None
    raise TypeError(f"unsupported annotation for a column: {annotation!r}")


def _datetime_to_text(value: datetime, field: str) -> str:
    """Fixed-width ISO-8601 in UTC (``YYYY-MM-DDTHH:MM:SS.ffffff+00:00``): text order = time order."""
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise PersistenceError("DATETIME_NOT_TZ_AWARE", field=field, value=value.isoformat())
    return value.astimezone(UTC).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class _Column:
    name: str
    kind: _Kind
    nullable: bool
    enum_type: type[Enum] | None = None

    @property
    def ddl(self) -> str:
        return f'"{self.name}" {_SQL_TYPES[self.kind]}' + ("" if self.nullable else " NOT NULL")

    def encode(self, value: Any) -> Any:
        """Python value (from ``model_dump()``) -> SQLite parameter."""
        if value is None:
            return None
        kind = self.kind
        if kind is _Kind.TEXT:
            return value if type(value) is str else str(value)
        if kind is _Kind.INTEGER:
            return int(value)
        if kind is _Kind.BOOLEAN:
            return 1 if value else 0
        if kind is _Kind.REAL:
            return float(value)
        if kind is _Kind.BLOB:
            return bytes(value)
        if kind is _Kind.DATETIME:
            return _datetime_to_text(value, self.name)
        if kind is _Kind.ENUM:
            return value.value if isinstance(value, Enum) else str(value)
        if isinstance(value, BaseModel):
            value = value.model_dump()
        return canonical_json(value)

    def decode(self, value: Any) -> Any:
        """SQLite value -> Python value accepted by ``Model.model_validate``."""
        if value is None:
            return None
        kind = self.kind
        if kind is _Kind.TEXT:
            return str(value)
        if kind is _Kind.INTEGER:
            return int(value)
        if kind is _Kind.BOOLEAN:
            return bool(value)
        if kind is _Kind.REAL:
            return float(value)
        if kind is _Kind.BLOB:
            return bytes(value)
        if kind is _Kind.DATETIME:
            return datetime.fromisoformat(str(value))
        if kind is _Kind.ENUM and self.enum_type is not None:
            return self.enum_type(value)
        return json.loads(value)


@dataclass(frozen=True)
class _Table(Generic[R]):
    """One table per record type; everything is derived from ``model.model_fields``."""

    name: str
    model: type[R]
    primary_key: tuple[str, ...]
    columns: tuple[_Column, ...]
    indexes: tuple[tuple[str, ...], ...] = ()
    unique: tuple[tuple[str, ...], ...] = ()

    @classmethod
    def build(
        cls,
        name: str,
        model: type[R],
        primary_key: tuple[str, ...],
        *,
        indexes: tuple[tuple[str, ...], ...] = (),
        unique: tuple[tuple[str, ...], ...] = (),
    ) -> _Table[R]:
        columns: list[_Column] = []
        for field_name, field in model.model_fields.items():
            inner, nullable = _unwrap_optional(field.annotation)
            kind, enum_type = _kind_for(inner)
            columns.append(_Column(field_name, kind, nullable, enum_type))
        return cls(name, model, primary_key, tuple(columns), indexes, unique)

    # ---- SQL text -------------------------------------------------------------------------
    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def quoted(self, alias: str | None = None) -> str:
        prefix = f"{alias}." if alias else ""
        return ", ".join(f'{prefix}"{name}"' for name in self.column_names)

    def create_statements(self) -> list[str]:
        columns = ", ".join(column.ddl for column in self.columns)
        primary = ", ".join(f'"{name}"' for name in self.primary_key)
        uniques = "".join(
            ", UNIQUE (" + ", ".join(f'"{name}"' for name in group) + ")" for group in self.unique
        )
        statements = [
            f"CREATE TABLE IF NOT EXISTS {self.name} ({columns}, PRIMARY KEY ({primary}){uniques})"
        ]
        for group in self.indexes:
            index_name = f"idx_{self.name}_{'_'.join(group)}"
            cols = ", ".join(f'"{name}"' for name in group)
            statements.append(f"CREATE INDEX IF NOT EXISTS {index_name} ON {self.name} ({cols})")
        return statements

    @property
    def insert_sql(self) -> str:
        placeholders = ", ".join("?" for _ in self.columns)
        return f"INSERT INTO {self.name} ({self.quoted()}) VALUES ({placeholders})"

    @property
    def upsert_sql(self) -> str:
        """Insert or update in place: the ``rowid`` (insertion order) of an existing row is kept."""
        conflict = ", ".join(f'"{name}"' for name in self.primary_key)
        updates = ", ".join(
            f'"{name}" = excluded."{name}"'
            for name in self.column_names
            if name not in self.primary_key
        )
        return f"{self.insert_sql} ON CONFLICT ({conflict}) DO UPDATE SET {updates}"

    def select_sql(self, suffix: str, alias: str | None = None) -> str:
        source = f"{self.name} {alias}" if alias else self.name
        return f"SELECT {self.quoted(alias)} FROM {source} {suffix}"

    # ---- mapping --------------------------------------------------------------------------
    def encode(self, record: R) -> tuple[Any, ...]:
        data = record.model_dump()
        return tuple(column.encode(data[column.name]) for column in self.columns)

    def decode(self, row: Sequence[Any]) -> R:
        """Row -> validated record; a row the model rejects (corruption) is ``RECORD_INVALID``."""
        try:
            data = {
                column.name: column.decode(value)
                for column, value in zip(self.columns, row, strict=True)
            }
            return self.model.model_validate(data)
        except (ValidationError, ValueError, TypeError) as exc:
            raise PersistenceError("RECORD_INVALID", table=self.name, error=str(exc)) from exc


_SESSIONS = _Table.build(
    "sessions", SessionRecord, ("session_id",), indexes=(("status",), ("created_at",))
)
_CONVERSATIONS = _Table.build(
    "conversations",
    ConversationRecord,
    ("conversation_id",),
    indexes=(("session_id",), ("status",)),
)
_CYCLES = _Table.build(
    "cycles",
    CycleRecord,
    ("cycle_id",),
    indexes=(("conversation_id",), ("session_id",), ("status",)),
)
_PLANS = _Table.build(
    "plans",
    PlanRecord,
    ("session_id", "plan_id"),
    indexes=(("conversation_id",), ("status",), ("plan_id",)),
)
_TASKS = _Table.build(
    "tasks",
    TaskRecord,
    ("session_id", "task_id"),
    indexes=(("session_id", "plan_id"), ("conversation_id",), ("status",)),
)
_MESSAGES = _Table.build(
    "messages", MessageRecord, ("message_id",), indexes=(("conversation_id",), ("session_id",))
)
_FAILURES = _Table.build("failures", FailureRecord, ("failure_id",), indexes=(("session_id",),))
_RETRY_DECISIONS = _Table.build(
    "retry_decisions", RetryDecisionRecord, ("decision_id",), indexes=(("session_id",),)
)
_CONTEXT_SUMMARIES = _Table.build(
    "context_summaries",
    ContextSummaryRecord,
    ("summary_id",),
    indexes=(("session_id",), ("target_conversation_id",)),
)
_BLOBS = _Table.build(
    "blobs", BlobRecord, ("blob_id",), indexes=(("session_id", "task_id", "blob_type"),)
)
_AUDIT_EVENTS = _Table.build(
    "audit_events",
    AuditEvent,
    ("session_id", "sequence"),
    unique=(("event_id",),),  # the UNIQUE constraint carries its own index
)

_TABLES: tuple[_Table[Any], ...] = (
    _SESSIONS,
    _CONVERSATIONS,
    _CYCLES,
    _PLANS,
    _TASKS,
    _MESSAGES,
    _FAILURES,
    _RETRY_DECISIONS,
    _CONTEXT_SUMMARIES,
    _BLOBS,
    _AUDIT_EVENTS,
)

#: ``list_tasks`` / ``find_tasks_in_states``: plan insertion order (a task whose plan is unknown sorts
#: first, like the in-memory store), then ``order_index``, then task insertion order (stable).
_TASK_JOIN = 'LEFT JOIN plans p ON p."session_id" = t."session_id" AND p."plan_id" = t."plan_id"'
_TASK_ORDER = 'ORDER BY COALESCE(p.rowid, 0), t."order_index", t.rowid'


class _Filter:
    """Accumulates the ``WHERE`` conditions of a listing and their bound parameters."""

    def __init__(self, alias: str | None = None) -> None:
        self._prefix = f"{alias}." if alias else ""
        self._conditions: list[str] = []
        self.params: list[Any] = []

    def equals(self, column: str, value: Any) -> None:
        self._conditions.append(f'{self._prefix}"{column}" = ?')
        self.params.append(value)

    def in_states(self, column: str, states: Iterable[Enum]) -> bool:
        """``column IN (...)`` over the distinct values (sorted: deterministic SQL).

        Returns ``False`` when no value is wanted: the query would match nothing.
        """
        wanted = tuple(sorted({str(state.value) for state in states}))
        if not wanted:
            return False
        placeholders = ", ".join("?" for _ in wanted)
        self._conditions.append(f'{self._prefix}"{column}" IN ({placeholders})')
        self.params.extend(wanted)
        return True

    @property
    def where(self) -> str:
        return "WHERE " + " AND ".join(self._conditions) if self._conditions else ""


# ------------------------------------------------------------------------------------------------
# The store
# ------------------------------------------------------------------------------------------------
class SqliteConversationStore(ConversationStore):
    """``ConversationStore`` over a single SQLite database (a file path or ``":memory:"``).

    ``busy_timeout_ms`` bounds the wait on a database locked by another connection before the
    operation fails with a transient ``SQLITE_ERROR``.
    """

    def __init__(self, path: str | os.PathLike[str], *, busy_timeout_ms: int = 5_000) -> None:
        raw_path = os.fspath(path)
        self._path = MEMORY_PATH if raw_path == MEMORY_PATH else raw_path
        self._tx_depth = 0
        self.closed = False
        try:
            self._conn = sqlite3.connect(
                self._path,
                timeout=busy_timeout_ms / 1000.0,
                isolation_level=None,
                check_same_thread=False,
            )
        except sqlite3.Error as exc:
            self.closed = True
            raise persistence_error_from_sqlite(exc) from exc
        try:
            self._configure()
            self._create_schema()
        except BaseException:
            self._conn.close()
            self.closed = True
            raise

    # ---- context manager ------------------------------------------------------------------
    def __enter__(self) -> SqliteConversationStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ---- properties -----------------------------------------------------------------------
    @property
    def path(self) -> str:
        return self._path

    @property
    def in_memory(self) -> bool:
        return self._path == MEMORY_PATH

    @property
    def journal_mode(self) -> str:
        return str(self.pragma("journal_mode"))

    def pragma(self, name: str) -> Any:
        """Read one PRAGMA value of this connection (diagnostics)."""
        row = self._execute(f"PRAGMA {name}").fetchone()
        return None if row is None else row[0]

    # ---- low level ------------------------------------------------------------------------
    def _ensure_open(self) -> None:
        if self.closed:
            raise PersistenceError("STORE_CLOSED", path=self._path)

    def _execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        self._ensure_open()
        try:
            return self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            raise persistence_error_from_sqlite(exc) from exc

    def _configure(self) -> None:
        if not self.in_memory:
            self._execute("PRAGMA journal_mode=WAL")
        self._execute("PRAGMA synchronous=FULL")  # ADR-019: durable checkpoints
        self._execute("PRAGMA foreign_keys=ON")

    def _create_schema(self) -> None:
        """Idempotent: opening an existing database (even twice at once) changes nothing."""
        with self.transaction():
            self._execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)")
            row = self._execute("SELECT MAX(version) FROM schema_version").fetchone()
            found = None if row is None else row[0]
            if found is not None and found != SCHEMA_VERSION:
                raise PersistenceError(
                    "SCHEMA_VERSION_UNSUPPORTED", found=found, supported=SCHEMA_VERSION
                )
            for table in _TABLES:
                for statement in table.create_statements():
                    self._execute(statement)
            if found is None:
                self._execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def _upsert(self, table: _Table[R], record: R) -> None:
        self._ensure_open()
        self._execute(table.upsert_sql, table.encode(record))

    def _fetch_one(
        self, table: _Table[R], suffix: str, params: Sequence[Any] = (), *, alias: str | None = None
    ) -> R | None:
        row = self._execute(table.select_sql(suffix, alias), params).fetchone()
        return None if row is None else table.decode(row)

    def _fetch_all(
        self, table: _Table[R], suffix: str, params: Sequence[Any] = (), *, alias: str | None = None
    ) -> list[R]:
        rows = self._execute(table.select_sql(suffix, alias), params).fetchall()
        return [table.decode(row) for row in rows]

    def _nothing(self) -> list[Any]:
        """The empty result of a listing whose state filter wants nothing (still guards ``closed``)."""
        self._ensure_open()
        return []

    # ---- transactions ---------------------------------------------------------------------
    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._ensure_open()
        depth = self._tx_depth
        if depth == 0:
            self._execute("BEGIN IMMEDIATE")
        else:
            self._execute(f"SAVEPOINT sp_{depth}")
        self._tx_depth = depth + 1
        try:
            yield
        except BaseException:
            self._tx_depth = depth
            self._rollback(depth)
            raise
        else:
            self._tx_depth = depth
            self._commit(depth)

    def _rollback(self, depth: int) -> None:
        if self.closed:  # closed inside the block: the connection already discarded everything
            return
        if depth == 0:
            self._execute("ROLLBACK")
        else:
            self._execute(f"ROLLBACK TO SAVEPOINT sp_{depth}")
            self._execute(f"RELEASE SAVEPOINT sp_{depth}")

    def _commit(self, depth: int) -> None:
        if depth == 0:
            try:
                self._execute("COMMIT")
            except PersistenceError:
                self._rollback(0)
                raise
        else:
            self._execute(f"RELEASE SAVEPOINT sp_{depth}")

    def transaction(self) -> AbstractContextManager[None]:
        return self._transaction()

    # ---- sessions -------------------------------------------------------------------------
    def save_session(self, record: SessionRecord) -> None:
        self._upsert(_SESSIONS, record)

    def get_session(self, session_id: str) -> SessionRecord | None:
        return self._fetch_one(_SESSIONS, 'WHERE "session_id" = ?', (session_id,))

    def list_sessions(
        self,
        *,
        statuses: Iterable[SessionState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[SessionRecord]:
        where = _Filter()
        if statuses is not None and not where.in_states("status", statuses):
            return self._nothing()
        return self._fetch_all(
            _SESSIONS,
            f'{where.where} ORDER BY "created_at" DESC, "session_id" DESC LIMIT ? OFFSET ?',
            (*where.params, limit, offset),
        )

    # ---- conversations --------------------------------------------------------------------
    def save_conversation(self, record: ConversationRecord) -> None:
        self._upsert(_CONVERSATIONS, record)

    def get_conversation(self, conversation_id: str) -> ConversationRecord | None:
        return self._fetch_one(_CONVERSATIONS, 'WHERE "conversation_id" = ?', (conversation_id,))

    def list_conversations(self, session_id: str) -> list[ConversationRecord]:
        return self._fetch_all(
            _CONVERSATIONS, 'WHERE "session_id" = ? ORDER BY rowid', (session_id,)
        )

    def find_conversations_in_states(
        self, states: Iterable[ConversationState]
    ) -> list[ConversationRecord]:
        where = _Filter()
        if not where.in_states("status", states):
            return self._nothing()
        return self._fetch_all(_CONVERSATIONS, f"{where.where} ORDER BY rowid", where.params)

    # ---- cycles ---------------------------------------------------------------------------
    def save_cycle(self, record: CycleRecord) -> None:
        self._upsert(_CYCLES, record)

    def get_cycle(self, cycle_id: str) -> CycleRecord | None:
        return self._fetch_one(_CYCLES, 'WHERE "cycle_id" = ?', (cycle_id,))

    def list_cycles(self, conversation_id: str) -> list[CycleRecord]:
        return self._fetch_all(
            _CYCLES, 'WHERE "conversation_id" = ? ORDER BY rowid', (conversation_id,)
        )

    # ---- plans ----------------------------------------------------------------------------
    def save_plan(self, record: PlanRecord) -> None:
        self._upsert(_PLANS, record)

    def get_plan(self, session_id: str, plan_id: str) -> PlanRecord | None:
        return self._fetch_one(
            _PLANS, 'WHERE "session_id" = ? AND "plan_id" = ?', (session_id, plan_id)
        )

    def list_plans(
        self, session_id: str, *, conversation_id: str | None = None
    ) -> list[PlanRecord]:
        where = _Filter()
        where.equals("session_id", session_id)
        if conversation_id is not None:
            where.equals("conversation_id", conversation_id)
        return self._fetch_all(_PLANS, f"{where.where} ORDER BY rowid", where.params)

    def find_plans_in_states(self, states: Iterable[PlanState]) -> list[PlanRecord]:
        where = _Filter()
        if not where.in_states("status", states):
            return self._nothing()
        return self._fetch_all(_PLANS, f"{where.where} ORDER BY rowid", where.params)

    # ---- tasks ----------------------------------------------------------------------------
    def save_task(self, record: TaskRecord) -> None:
        self._upsert(_TASKS, record)

    def save_tasks(self, records: Iterable[TaskRecord]) -> None:
        with self.transaction():
            for record in records:
                self.save_task(record)

    def get_task(self, session_id: str, task_id: str) -> TaskRecord | None:
        return self._fetch_one(
            _TASKS, 'WHERE "session_id" = ? AND "task_id" = ?', (session_id, task_id)
        )

    def list_tasks(
        self,
        session_id: str,
        *,
        plan_id: str | None = None,
        statuses: Iterable[TaskState] | None = None,
    ) -> list[TaskRecord]:
        where = _Filter("t")
        where.equals("session_id", session_id)
        if plan_id is not None:
            where.equals("plan_id", plan_id)
        if statuses is not None and not where.in_states("status", statuses):
            return self._nothing()
        suffix = f"{_TASK_JOIN} {where.where} {_TASK_ORDER}"
        return self._fetch_all(_TASKS, suffix, where.params, alias="t")

    def find_tasks_in_states(self, states: Iterable[TaskState]) -> list[TaskRecord]:
        where = _Filter("t")
        if not where.in_states("status", states):
            return self._nothing()
        suffix = f"{_TASK_JOIN} {where.where} {_TASK_ORDER}"
        return self._fetch_all(_TASKS, suffix, where.params, alias="t")

    # ---- messages -------------------------------------------------------------------------
    def save_message(self, record: MessageRecord) -> None:
        self._upsert(_MESSAGES, record)

    def get_message(self, message_id: str) -> MessageRecord | None:
        return self._fetch_one(_MESSAGES, 'WHERE "message_id" = ?', (message_id,))

    def list_messages(
        self,
        conversation_id: str,
        *,
        direction: MessageDirection | None = None,
    ) -> list[MessageRecord]:
        where = _Filter()
        where.equals("conversation_id", conversation_id)
        if direction is not None:
            where.equals("direction", direction.value)
        return self._fetch_all(_MESSAGES, f"{where.where} ORDER BY rowid", where.params)

    # ---- failures / retry decisions -------------------------------------------------------
    def save_failure(self, record: FailureRecord) -> None:
        self._upsert(_FAILURES, record)

    def list_failures(self, session_id: str) -> list[FailureRecord]:
        return self._fetch_all(_FAILURES, 'WHERE "session_id" = ? ORDER BY rowid', (session_id,))

    def save_retry_decision(self, record: RetryDecisionRecord) -> None:
        self._upsert(_RETRY_DECISIONS, record)

    def list_retry_decisions(self, session_id: str) -> list[RetryDecisionRecord]:
        return self._fetch_all(
            _RETRY_DECISIONS, 'WHERE "session_id" = ? ORDER BY rowid', (session_id,)
        )

    # ---- context summaries ----------------------------------------------------------------
    def save_context_summary(self, record: ContextSummaryRecord) -> None:
        self._upsert(_CONTEXT_SUMMARIES, record)

    def get_context_summary_for_target(
        self, target_conversation_id: str
    ) -> ContextSummaryRecord | None:
        return self._fetch_one(
            _CONTEXT_SUMMARIES,
            'WHERE "target_conversation_id" = ? ORDER BY rowid LIMIT 1',
            (target_conversation_id,),
        )

    def list_context_summaries(self, session_id: str) -> list[ContextSummaryRecord]:
        return self._fetch_all(
            _CONTEXT_SUMMARIES, 'WHERE "session_id" = ? ORDER BY rowid', (session_id,)
        )

    # ---- blobs ----------------------------------------------------------------------------
    def save_blob(self, record: BlobRecord) -> None:
        self._ensure_open()
        if record.size_bytes != len(record.content):
            raise PersistenceError(
                "BLOB_SIZE_MISMATCH",
                blob_id=record.blob_id,
                declared=record.size_bytes,
                actual=len(record.content),
            )
        self._upsert(_BLOBS, record)

    def get_blob(self, blob_id: str) -> BlobRecord | None:
        return self._fetch_one(_BLOBS, 'WHERE "blob_id" = ?', (blob_id,))

    def get_blob_for_task(
        self, session_id: str, task_id: str, blob_type: OutputStream
    ) -> BlobRecord | None:
        return self._fetch_one(
            _BLOBS,
            'WHERE "session_id" = ? AND "task_id" = ? AND "blob_type" = ? ORDER BY rowid LIMIT 1',
            (session_id, task_id, blob_type.value),
        )

    def read_blob_range(self, blob_id: str, offset: int, max_bytes: int) -> bytes:
        """Range read done by SQLite (``substr`` on the BLOB): the whole blob is never loaded."""
        row = self._execute('SELECT length("content") FROM blobs WHERE "blob_id" = ?', (blob_id,))
        found = row.fetchone()
        if found is None:
            raise PersistenceError("BLOB_NOT_FOUND", blob_id=blob_id)
        if offset < 0 or max_bytes < 0:
            raise PersistenceError(
                "BLOB_RANGE_INVALID", blob_id=blob_id, offset=offset, max_bytes=max_bytes
            )
        size = int(found[0] or 0)
        length = min(max_bytes, size - offset)
        if length <= 0:
            return b""
        data = self._execute(
            'SELECT substr("content", ?, ?) FROM blobs WHERE "blob_id" = ?',
            (offset + 1, length, blob_id),
        ).fetchone()
        if data is None or data[0] is None:
            return b""
        return bytes(data[0])

    # ---- audit ----------------------------------------------------------------------------
    def append_audit_event(self, event: AuditEvent) -> None:
        with self.transaction():
            duplicate = self._execute(
                'SELECT 1 FROM audit_events WHERE "event_id" = ? '
                'OR ("session_id" = ? AND "sequence" = ?) LIMIT 1',
                (event.event_id, event.session_id, event.sequence),
            ).fetchone()
            if duplicate is not None:
                raise PersistenceError(
                    "AUDIT_APPEND_ONLY_VIOLATION", event_id=event.event_id, sequence=event.sequence
                )
            last_row = self._execute(
                'SELECT MAX("sequence") FROM audit_events WHERE "session_id" = ?',
                (event.session_id,),
            ).fetchone()
            last = None if last_row is None else last_row[0]
            if last is not None and event.sequence != int(last) + 1:
                raise PersistenceError(
                    "AUDIT_SEQUENCE_GAP", expected=int(last) + 1, got=event.sequence
                )
            self._execute(_AUDIT_EVENTS.insert_sql, _AUDIT_EVENTS.encode(event))

    def get_last_audit_event(self, session_id: str) -> AuditEvent | None:
        return self._fetch_one(
            _AUDIT_EVENTS,
            'WHERE "session_id" = ? ORDER BY "sequence" DESC LIMIT 1',
            (session_id,),
        )

    def list_audit_events(
        self,
        session_id: str,
        *,
        after_sequence: int | None = None,
        limit: int = 1000,
    ) -> list[AuditEvent]:
        if after_sequence is None:
            return self._fetch_all(
                _AUDIT_EVENTS,
                'WHERE "session_id" = ? ORDER BY "sequence" ASC LIMIT ?',
                (session_id, limit),
            )
        return self._fetch_all(
            _AUDIT_EVENTS,
            'WHERE "session_id" = ? AND "sequence" > ? ORDER BY "sequence" ASC LIMIT ?',
            (session_id, after_sequence, limit),
        )

    def count_audit_events(self, session_id: str) -> int:
        row = self._execute(
            'SELECT COUNT(*) FROM audit_events WHERE "session_id" = ?', (session_id,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    # ---- maintenance ----------------------------------------------------------------------
    def close(self) -> None:
        """Idempotent. An open transaction is discarded by the connection (rollback)."""
        if self.closed:
            return
        self.closed = True
        self._tx_depth = 0
        try:
            self._conn.close()
        except sqlite3.Error as exc:
            raise persistence_error_from_sqlite(exc) from exc
