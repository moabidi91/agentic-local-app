"""``ScratchManager`` — one working space per session (ADR-026).

The application itself writes no file: outputs live in memory then in SQLite blobs. The **model's
commands** do write files, and until now they landed wherever ``[execution] cwd`` pointed, for ever.
This module gives every session one folder it may write into, tells the commands where it is through
the environment, can list what was left behind, and applies a policy to it when the session ends.

Three rules shape the whole module:

1. **We only ever delete what we created.** A folder generated under ``[scratch] root`` belongs to
   the application and follows the policy; a ``working_space`` handed over by the user
   (:meth:`ScratchManager.bind`, the *Working folder* field of the desktop front) is used as it is,
   never created with restrictive permissions, and never deleted nor archived.
2. **Cleaning up must never fail a session.** :meth:`ScratchManager.release` reports a filesystem
   problem in its :class:`ScratchOutcome` instead of raising; a session that finished its work is
   not retroactively broken by a locked file.
3. **Nothing global, nothing implicit.** The manager holds its own state, the clock is injected
   (ADR-017) and nothing is created before something actually needs it: a disabled section creates
   no directory and exports no variable.

Paths are handled through :class:`pathlib.Path` only, so the module behaves the same on POSIX and on
Windows (ADR-003); the ``0o700`` mode is applied on POSIX alone, where it means something.
"""

from __future__ import annotations

import os
import shutil
from datetime import UTC, datetime
from enum import StrEnum, unique
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from agentic_local_app.config import ScratchSection
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ConfigError

__all__ = [
    "DIRECTORY_MODE",
    "ENV_SCRATCH_DIR",
    "ENV_SESSION_ID",
    "ENV_WORKING_SPACE",
    "REASON_BOUND_WORKING_SPACE",
    "REASON_DISABLED",
    "REASON_KEEP_ON_FAILURE",
    "REASON_NEVER_CREATED",
    "REASON_POLICY_ARCHIVE",
    "REASON_POLICY_DELETE",
    "REASON_POLICY_KEEP",
    "WORKING_SPACE_INVALID",
    "ScratchAction",
    "ScratchFile",
    "ScratchInventory",
    "ScratchManager",
    "ScratchOutcome",
    "validate_working_space",
]

#: Mode of a **generated** session folder (POSIX only: owner-only read/write/traverse).
DIRECTORY_MODE = 0o700

#: The folder a command may write into — the shell habit, next to ``TMPDIR``.
ENV_SCRATCH_DIR = "AGENTIC_SCRATCH_DIR"
#: The same folder under the vocabulary of the desktop front (its *Working folder* field).
ENV_WORKING_SPACE = "AGENTIC_WORKING_SPACE"
#: The session the command belongs to, so that a command can name its own outputs.
ENV_SESSION_ID = "AGENTIC_SESSION_ID"

#: ``error_code`` of a ``working_space`` that cannot be used as it is.
WORKING_SPACE_INVALID = "WORKING_SPACE_INVALID"

#: ``reason`` values of a :class:`ScratchOutcome` — why that action, in one token.
REASON_DISABLED = "disabled"
REASON_NEVER_CREATED = "never_created"
REASON_BOUND_WORKING_SPACE = "bound_working_space"
REASON_KEEP_ON_FAILURE = "keep_on_failure"
REASON_POLICY_DELETE = "policy_delete"
REASON_POLICY_KEEP = "policy_keep"
REASON_POLICY_ARCHIVE = "policy_archive"

#: Timestamp suffix of an archived folder (sortable, filename-safe on Windows too).
ARCHIVE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"


@unique
class ScratchAction(StrEnum):
    """What :meth:`ScratchManager.release` actually did with the folder."""

    NONE = "none"  # there was nothing of ours to act on
    KEPT = "kept"
    DELETED = "deleted"
    ARCHIVED = "archived"
    FAILED = "failed"  # the filesystem refused; the folder is still there


class _Value(BaseModel):
    """Frozen, JSON-serialisable value object (house style: records never mutate)."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ScratchFile(_Value):
    """One file left in a working space. ``relative_path`` always uses ``/`` separators so that the
    same inventory reads identically on POSIX and on Windows."""

    relative_path: str
    size_bytes: int = Field(ge=0)
    modified_at: datetime


class ScratchInventory(_Value):
    """What :meth:`ScratchManager.inventory` found, bounded by ``max_inventory_entries``.

    The bound applies to what is **reported**, not to what is walked: the whole folder is listed and
    sorted first, so the reported head is deterministic (ADR-017) rather than dependent on the order
    the filesystem happens to yield.
    """

    entries: tuple[ScratchFile, ...] = ()
    #: ``True`` when the folder held more files than ``max_inventory_entries``.
    truncated: bool = False


class ScratchOutcome(_Value):
    """What :meth:`ScratchManager.release` did, and what was there just before it acted."""

    session_id: str
    action: ScratchAction
    reason: str
    #: The folder the outcome is about; ``None`` when there was none (disabled, never created).
    path: str | None = None
    #: Where an ``archive`` moved the folder.
    archived_to: str | None = None
    #: Taken **before** acting: what the session left behind.
    inventory: ScratchInventory = Field(default_factory=ScratchInventory)
    #: ``"<ExceptionType>: <message>"`` when the filesystem refused; never raised.
    error: str | None = None


class ScratchManager:
    """The working spaces of the sessions of one application (ADR-026).

    Holds no global state: two managers built on two ``[scratch]`` sections are independent. The
    clock is injected (ADR-017) and is used for one thing only, the timestamp of an archive.
    """

    def __init__(self, config: ScratchSection, clock: Clock) -> None:
        self._config = config
        self._clock = clock
        #: Sessions whose folder **we** generated — the only ones a policy may act on.
        self._created: dict[str, Path] = {}
        #: Sessions bound to a folder the user owns — never created, never removed.
        self._bound: dict[str, Path] = {}

    @property
    def config(self) -> ScratchSection:
        return self._config

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    # ---- the folder ----------------------------------------------------------------------
    def path_for(self, session_id: str) -> Path | None:
        """The working space of ``session_id``, created on first use.

        ``None`` when ``[scratch] enabled`` is false: there is no working space at all, and the
        type says so rather than handing back a path to a folder that does not exist. A session
        bound to a ``working_space`` gets that folder as it is (no creation, no ``chmod``).

        :raises ValueError: ``session_id`` is blank or is not a plain directory name — it is used
            as the folder's name and must never climb out of ``root``.
        """
        if not self._config.enabled:
            return None
        bound = self._bound.get(session_id)
        if bound is not None:
            return bound
        known = self._created.get(session_id)
        if known is not None:
            return known
        folder = self._root() / _leaf(session_id)
        _make_directory(folder, restrict=True)
        self._created[session_id] = folder
        return folder

    def bind(self, session_id: str, working_space: str | Path | None) -> Path | None:
        """Replace the generated folder of ``session_id`` by one the **user** owns.

        This is the *Working folder* of the desktop front. The folder is used exactly as it is: it
        is not created, its permissions are not touched, and no policy will ever delete or archive
        it — the application only cleans up what it made itself.

        Returns the bound folder, or ``None`` when ``working_space`` is ``None`` (any previous
        binding is dropped and the generated folder applies again, still lazily) or when the section
        is disabled.

        :raises ConfigError: ``WORKING_SPACE_INVALID`` with ``path`` and ``reason``
            (``blank``, ``not_absolute``, ``does_not_exist``, ``not_a_directory``, ``unreadable``)
            — a path the user typed is configuration, and a bad one must be told, not guessed at.
        """
        if working_space is None:
            self._bound.pop(session_id, None)
            return None
        folder = validate_working_space(working_space)
        if not self._config.enabled:
            return None
        self._bound[session_id] = folder
        return folder

    # ---- what a command sees -------------------------------------------------------------
    def environment(self, session_id: str) -> dict[str, str]:
        """The variables added to a command's environment (ADR-026 §3), folder created on the way.

        Empty when the section is disabled: nothing is created and nothing is exported, which is
        exactly the behaviour that predates this module.

        ``AGENTIC_SCRATCH_DIR`` and ``AGENTIC_WORKING_SPACE`` name the **same** folder on purpose —
        one is the shell habit, the other the vocabulary of the front — so a command written against
        either name works without the model having to guess which one this build uses.
        """
        folder = self.path_for(session_id)
        if folder is None:
            return {}
        location = str(folder)
        return {
            ENV_SCRATCH_DIR: location,
            ENV_WORKING_SPACE: location,
            ENV_SESSION_ID: session_id,
        }

    # ---- auditing ------------------------------------------------------------------------
    def inventory(self, session_id: str) -> ScratchInventory:
        """The files present in the working space of ``session_id``, sorted and bounded.

        Empty for an unknown session, a disabled section or a folder that does not exist. Never
        raises: an entry that cannot be read is skipped — an audit of what was left behind must not
        be able to break the caller that asks for it.
        """
        folder = self._known_path(session_id)
        if folder is None:
            return ScratchInventory()
        return self._inventory_of(folder)

    # ---- end of session ------------------------------------------------------------------
    def release(self, session_id: str, *, failed: bool) -> ScratchOutcome:
        """Apply the policy to the working space of ``session_id``; never raises.

        - a **bound** ``working_space`` is left alone whatever the policy (rule 1 of this module);
          a session bound *after* a folder was already generated for it still has that generated
          folder cleaned up — the policy always applies to what the application made, and never to
          anything else;
        - ``keep_on_failure`` wins over ``delete`` **and** over ``archive`` when ``failed``: a
          session that went wrong is exactly the one whose files you want to look at;
        - otherwise ``delete`` removes the tree, ``keep`` leaves it, ``archive`` moves it to
          ``<archive_root>/<session_id>-<timestamp>``.

        The returned :class:`ScratchOutcome` carries the inventory taken **before** acting, so that
        a deleted folder can still be reported on. A filesystem error becomes
        :attr:`ScratchAction.FAILED` plus ``error``; the folder is then still on disk.

        Releasing a session twice is a no-op (``NONE`` / ``never_created``): the manager forgets a
        session as soon as it has acted on it.
        """
        if not self._config.enabled:
            return ScratchOutcome(
                session_id=session_id, action=ScratchAction.NONE, reason=REASON_DISABLED
            )
        bound = self._bound.pop(session_id, None)
        folder = self._created.pop(session_id, None)
        if folder is None:
            # nothing of ours: either a folder the user owns, or a session that ran no command
            if bound is None:
                return ScratchOutcome(
                    session_id=session_id, action=ScratchAction.NONE, reason=REASON_NEVER_CREATED
                )
            return ScratchOutcome(
                session_id=session_id,
                action=ScratchAction.NONE,
                reason=REASON_BOUND_WORKING_SPACE,
                path=str(bound),
                inventory=self._inventory_of(bound),
            )
        inventory = self._inventory_of(folder)
        if failed and self._config.keep_on_failure:
            return self._kept(session_id, folder, inventory, REASON_KEEP_ON_FAILURE)
        if self._config.policy == "keep":
            return self._kept(session_id, folder, inventory, REASON_POLICY_KEEP)
        if self._config.policy == "archive":
            return self._archive(session_id, folder, inventory)
        return self._delete(session_id, folder, inventory)

    def release_all(self, *, failed: bool = False) -> list[ScratchOutcome]:
        """Release every session this manager still tracks, in session order (ADR-017).

        Called by ``Application.close`` / ``Application.aclose``: whatever the application created
        is dealt with when it shuts down, even if no per-session hook ran. Never raises.
        """
        sessions = sorted(set(self._created) | set(self._bound))
        return [self.release(session_id, failed=failed) for session_id in sessions]

    # ---- pieces --------------------------------------------------------------------------
    def _root(self) -> Path:
        return Path(self._config.root).expanduser()

    def _known_path(self, session_id: str) -> Path | None:
        """The folder of ``session_id`` **without** creating anything."""
        if not self._config.enabled:
            return None
        return self._bound.get(session_id) or self._created.get(session_id)

    def _inventory_of(self, folder: Path) -> ScratchInventory:
        found: list[ScratchFile] = []
        try:
            for entry in folder.rglob("*"):
                try:
                    if not entry.is_file():
                        continue
                    info = entry.stat()
                    relative = entry.relative_to(folder)
                except (OSError, ValueError):
                    continue  # vanished, unreadable, or outside the folder through a link
                found.append(
                    ScratchFile(
                        relative_path=relative.as_posix(),
                        size_bytes=info.st_size,
                        modified_at=datetime.fromtimestamp(info.st_mtime, tz=UTC),
                    )
                )
        except OSError:
            pass  # an unreadable folder yields what could be read, never an exception
        found.sort(key=lambda item: item.relative_path)
        bound = self._config.max_inventory_entries
        return ScratchInventory(entries=tuple(found[:bound]), truncated=len(found) > bound)

    def _kept(
        self, session_id: str, folder: Path, inventory: ScratchInventory, reason: str
    ) -> ScratchOutcome:
        return ScratchOutcome(
            session_id=session_id,
            action=ScratchAction.KEPT,
            reason=reason,
            path=str(folder),
            inventory=inventory,
        )

    def _delete(self, session_id: str, folder: Path, inventory: ScratchInventory) -> ScratchOutcome:
        try:
            shutil.rmtree(folder)
        except OSError as exc:
            return self._failed(session_id, folder, inventory, REASON_POLICY_DELETE, exc)
        return ScratchOutcome(
            session_id=session_id,
            action=ScratchAction.DELETED,
            reason=REASON_POLICY_DELETE,
            path=str(folder),
            inventory=inventory,
        )

    def _archive(
        self, session_id: str, folder: Path, inventory: ScratchInventory
    ) -> ScratchOutcome:
        stamp = self._clock.now().strftime(ARCHIVE_TIMESTAMP_FORMAT)
        destination = Path(self._config.archive_root).expanduser() / f"{_leaf(session_id)}-{stamp}"
        try:
            if destination.exists():
                # ``shutil.move`` would move the folder *inside* it and silently nest two sessions
                raise FileExistsError(str(destination))
            _make_directory(destination.parent, restrict=True)
            shutil.move(str(folder), str(destination))
        except OSError as exc:
            return self._failed(session_id, folder, inventory, REASON_POLICY_ARCHIVE, exc)
        return ScratchOutcome(
            session_id=session_id,
            action=ScratchAction.ARCHIVED,
            reason=REASON_POLICY_ARCHIVE,
            path=str(folder),
            archived_to=str(destination),
            inventory=inventory,
        )

    @staticmethod
    def _failed(
        session_id: str,
        folder: Path,
        inventory: ScratchInventory,
        reason: str,
        exc: OSError,
    ) -> ScratchOutcome:
        return ScratchOutcome(
            session_id=session_id,
            action=ScratchAction.FAILED,
            reason=reason,
            path=str(folder),
            inventory=inventory,
            error=f"{type(exc).__name__}: {exc}",
        )


# ------------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------------
def _leaf(session_id: str) -> str:
    """``session_id`` as a plain directory name — never a path, never a way out of ``root``.

    Both separators are refused on both platforms: a backslash is a separator on Windows and a
    perfectly legal filename character on POSIX, and a folder named ``a\\b`` on one machine and
    ``a/b`` on another would be the same session with two different working spaces.
    """
    name = session_id.strip()
    invalid = (
        not name or name in {".", ".."} or "/" in name or "\\" in name or name != Path(name).name
    )
    if invalid:
        raise ValueError(f"session_id must be a plain directory name, got {session_id!r}")
    return name


def _make_directory(folder: Path, *, restrict: bool) -> None:
    """Create ``folder`` (and its parents) if needed; ``restrict`` narrows it to the owner.

    ``mkdir(mode=...)`` is subject to the umask and does nothing for an existing directory, so the
    mode is applied explicitly — on POSIX only, where ``chmod`` carries the meaning we want.
    """
    folder.mkdir(parents=True, exist_ok=True)
    if restrict and os.name != "nt":
        folder.chmod(DIRECTORY_MODE)


def validate_working_space(working_space: str | Path) -> Path:
    """The user's folder, or :class:`ConfigError` ``WORKING_SPACE_INVALID`` saying what is wrong.

    Public because a caller often has to refuse the path **before** it has anything to bind it to:
    the API validates the ``working_space`` of ``POST /sessions`` before a session exists, so a path
    the user mistyped costs no record at all.
    """
    raw = str(working_space)
    if not raw.strip():
        raise ConfigError(WORKING_SPACE_INVALID, path=raw, reason="blank")
    folder = Path(raw).expanduser()
    if not folder.is_absolute():
        raise ConfigError(WORKING_SPACE_INVALID, path=str(folder), reason="not_absolute")
    try:
        exists, is_directory = folder.exists(), folder.is_dir()
    except OSError as exc:
        raise ConfigError(
            WORKING_SPACE_INVALID, path=str(folder), reason="unreadable", error=str(exc)
        ) from exc
    if not exists:
        raise ConfigError(WORKING_SPACE_INVALID, path=str(folder), reason="does_not_exist")
    if not is_directory:
        raise ConfigError(WORKING_SPACE_INVALID, path=str(folder), reason="not_a_directory")
    return folder
