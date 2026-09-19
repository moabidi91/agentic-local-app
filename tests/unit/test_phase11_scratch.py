"""ADR-026 — the working space of a session: where the model's commands may write, and what
happens to that folder afterwards.

The application writes no file of its own; its commands do. ``ScratchManager`` is the leaf module
that gives each session a folder, names it in the environment of every command, can list what was
left in it, and applies the ``[scratch]`` policy when the session ends. Marked ``phase4``: it exists
for task execution, and the last section checks the variables on the ``CommandSpec`` of a real run.

Four invariants are pinned here as much as the behaviour:

1. **Nothing is created before something needs it** — a manager that is built, or a disabled
   section, touches no directory at all;
2. **we only delete what we created** — a ``working_space`` handed over by the user survives every
   policy, and its permissions are never touched;
3. **a cleanup never fails a session** — a filesystem refusal is reported in the outcome;
4. **the inventory is taken before acting**, so a deleted folder can still be reported on.

Everything runs on ``tmp_path``; no test ever looks at the real ``./data``.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app.config import AppConfig, ExecutionSection, ScratchSection
from agentic_local_app.domain.clock import FakeClock, SystemClock
from agentic_local_app.domain.errors import ConfigError
from agentic_local_app.domain.states import TaskState
from agentic_local_app.execution import scratch as scratch_module
from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandSpec,
    SubprocessCommandExecutor,
)
from agentic_local_app.execution.scratch import (
    ENV_SCRATCH_DIR,
    ENV_SESSION_ID,
    ENV_WORKING_SPACE,
    REASON_BOUND_WORKING_SPACE,
    REASON_DISABLED,
    REASON_KEEP_ON_FAILURE,
    REASON_NEVER_CREATED,
    REASON_POLICY_ARCHIVE,
    REASON_POLICY_DELETE,
    REASON_POLICY_KEEP,
    WORKING_SPACE_INVALID,
    ScratchAction,
    ScratchManager,
)

pytestmark = pytest.mark.phase4

SESSION = "sess-0001"
OTHER = "sess-0002"
IS_WINDOWS = sys.platform == "win32"


# ================================================================================================
# helpers
# ================================================================================================
def _section(tmp_path: Path, **overrides: Any) -> ScratchSection:
    values: dict[str, Any] = {
        "root": str(tmp_path / "scratch"),
        "archive_root": str(tmp_path / "archive"),
    }
    values.update(overrides)
    return ScratchSection(**values)


def _manager(tmp_path: Path, **overrides: Any) -> ScratchManager:
    return ScratchManager(_section(tmp_path, **overrides), FakeClock())


def _folder(manager: ScratchManager, session_id: str = SESSION) -> Path:
    folder = manager.path_for(session_id)
    assert folder is not None
    return folder


def _write(folder: Path, relative: str, payload: bytes = b"x") -> Path:
    target = folder / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return target


# ================================================================================================
# 1. the folder: lazy creation, permissions, isolation
# ================================================================================================
def given_freshly_built_manager_when_nothing_asked_then_no_directory_exists(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    assert not (tmp_path / "scratch").exists()
    assert manager.enabled is True


def given_new_session_when_path_for_then_folder_created_under_root(tmp_path: Path) -> None:
    manager = _manager(tmp_path)

    folder = _folder(manager)

    assert folder == tmp_path / "scratch" / SESSION
    assert folder.is_dir()


@pytest.mark.skipif(IS_WINDOWS, reason="POSIX permission bits only (ADR-003)")
def given_generated_folder_when_created_then_mode_is_owner_only(tmp_path: Path) -> None:
    folder = _folder(_manager(tmp_path))
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700


def given_two_sessions_when_path_for_then_two_distinct_folders(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    assert _folder(manager, SESSION) != _folder(manager, OTHER)


def given_same_session_when_path_for_twice_then_same_folder_and_no_error(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    assert _folder(manager) == _folder(manager)


def given_existing_root_when_path_for_then_reused_without_error(tmp_path: Path) -> None:
    (tmp_path / "scratch").mkdir()
    assert _folder(_manager(tmp_path)).is_dir()


@pytest.mark.parametrize(
    "session_id", ["", "   ", ".", "..", "a/b", "a\\b", os.path.join("a", "b"), "../escape"]
)
def given_session_id_that_is_not_a_plain_name_when_path_for_then_value_error(
    tmp_path: Path, session_id: str
) -> None:
    """The identifier becomes a directory name: it must never climb out of ``root``."""
    with pytest.raises(ValueError, match="plain directory name"):
        _manager(tmp_path).path_for(session_id)


def given_disabled_scratch_when_path_for_then_none_and_nothing_created(tmp_path: Path) -> None:
    manager = _manager(tmp_path, enabled=False)

    assert manager.path_for(SESSION) is None
    assert manager.enabled is False
    assert not (tmp_path / "scratch").exists()


# ================================================================================================
# 2. what a command sees
# ================================================================================================
def given_enabled_scratch_when_environment_then_both_names_point_at_the_folder(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path)

    env = manager.environment(SESSION)

    folder = tmp_path / "scratch" / SESSION
    assert env == {
        ENV_SCRATCH_DIR: str(folder),
        ENV_WORKING_SPACE: str(folder),
        ENV_SESSION_ID: SESSION,
    }
    assert folder.is_dir()  # asking for the environment is the first use


def given_disabled_scratch_when_environment_then_empty_and_nothing_created(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, enabled=False)

    assert manager.environment(SESSION) == {}
    assert not (tmp_path / "scratch").exists()


# ================================================================================================
# 3. the inventory — « qu'as-tu laissé derrière toi ? »
# ================================================================================================
def given_files_in_the_folder_when_inventory_then_sorted_entries_with_size_and_time(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path)
    folder = _folder(manager)
    _write(folder, "b.txt", b"12345")
    _write(folder, "a.txt", b"1")
    _write(folder, "sub/inner.bin", b"123")

    inventory = manager.inventory(SESSION)

    assert [entry.relative_path for entry in inventory.entries] == [
        "a.txt",
        "b.txt",
        "sub/inner.bin",
    ]
    assert [entry.size_bytes for entry in inventory.entries] == [1, 5, 3]
    assert inventory.truncated is False
    assert all(entry.modified_at.tzinfo is not None for entry in inventory.entries)
    assert all(entry.modified_at.tzinfo == UTC for entry in inventory.entries)


def given_nested_file_when_inventory_then_relative_path_uses_posix_separators(
    tmp_path: Path,
) -> None:
    """One spelling of a path whatever the platform (ADR-003): the inventory is read by a model."""
    manager = _manager(tmp_path)
    _write(_folder(manager), "deep/inside/here.txt")

    assert manager.inventory(SESSION).entries[0].relative_path == "deep/inside/here.txt"


def given_more_files_than_the_bound_when_inventory_then_truncated_head_reported(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, max_inventory_entries=3)
    folder = _folder(manager)
    for index in range(5):
        _write(folder, f"f{index}.txt")

    inventory = manager.inventory(SESSION)

    assert [entry.relative_path for entry in inventory.entries] == ["f0.txt", "f1.txt", "f2.txt"]
    assert inventory.truncated is True


def given_exactly_the_bound_when_inventory_then_not_truncated(tmp_path: Path) -> None:
    manager = _manager(tmp_path, max_inventory_entries=3)
    folder = _folder(manager)
    for index in range(3):
        _write(folder, f"f{index}.txt")

    assert manager.inventory(SESSION).truncated is False


def given_directories_only_when_inventory_then_no_entry(tmp_path: Path) -> None:
    """Directories are structure, not what the session produced."""
    manager = _manager(tmp_path)
    (_folder(manager) / "empty").mkdir()

    assert manager.inventory(SESSION).entries == ()


def given_unknown_or_disabled_session_when_inventory_then_empty(tmp_path: Path) -> None:
    assert _manager(tmp_path).inventory("sess-9999").entries == ()
    assert _manager(tmp_path, enabled=False).inventory(SESSION).entries == ()


def given_inventory_when_serialised_then_json_round_trips(tmp_path: Path) -> None:
    """The inventory travels to an interface and to the model: it must be plain JSON."""
    manager = _manager(tmp_path)
    _write(_folder(manager), "a.txt")

    dumped = manager.inventory(SESSION).model_dump(mode="json")

    assert json.loads(json.dumps(dumped))["entries"][0]["relative_path"] == "a.txt"


# ================================================================================================
# 4. the three policies, and keep_on_failure
# ================================================================================================
def given_policy_delete_when_release_then_tree_removed_and_inventory_kept(tmp_path: Path) -> None:
    manager = _manager(tmp_path, policy="delete")
    folder = _folder(manager)
    _write(folder, "left.txt", b"abc")

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.DELETED
    assert outcome.reason == REASON_POLICY_DELETE
    assert outcome.path == str(folder) and outcome.error is None
    assert not folder.exists()
    # the inventory was taken *before* acting: the folder is gone, what it held is not
    assert [entry.relative_path for entry in outcome.inventory.entries] == ["left.txt"]
    assert outcome.inventory.entries[0].size_bytes == 3


def given_policy_keep_when_release_then_folder_left_in_place(tmp_path: Path) -> None:
    manager = _manager(tmp_path, policy="keep")
    folder = _folder(manager)
    _write(folder, "left.txt")

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.KEPT and outcome.reason == REASON_POLICY_KEEP
    assert folder.is_dir() and (folder / "left.txt").is_file()


def given_policy_archive_when_release_then_folder_moved_under_archive_root(
    tmp_path: Path,
) -> None:
    clock = FakeClock(start=datetime(2026, 9, 19, 14, 30, 5, tzinfo=UTC))
    manager = ScratchManager(_section(tmp_path, policy="archive"), clock)
    folder = _folder(manager)
    _write(folder, "left.txt", b"abc")

    outcome = manager.release(SESSION, failed=False)

    destination = tmp_path / "archive" / f"{SESSION}-20260919T143005"
    assert outcome.action is ScratchAction.ARCHIVED
    assert outcome.reason == REASON_POLICY_ARCHIVE
    assert outcome.archived_to == str(destination)
    assert not folder.exists()
    assert (destination / "left.txt").read_bytes() == b"abc"


def given_policy_delete_and_failed_session_when_release_then_folder_kept(tmp_path: Path) -> None:
    """A session that went wrong is exactly the one whose files you want to look at."""
    manager = _manager(tmp_path, policy="delete")
    folder = _folder(manager)
    _write(folder, "crash.log")

    outcome = manager.release(SESSION, failed=True)

    assert outcome.action is ScratchAction.KEPT and outcome.reason == REASON_KEEP_ON_FAILURE
    assert (folder / "crash.log").is_file()


def given_policy_archive_and_failed_session_when_release_then_folder_kept_where_it_is(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, policy="archive")
    folder = _folder(manager)

    outcome = manager.release(SESSION, failed=True)

    assert outcome.action is ScratchAction.KEPT and outcome.reason == REASON_KEEP_ON_FAILURE
    assert folder.is_dir() and not (tmp_path / "archive").exists()


def given_keep_on_failure_false_and_failed_session_when_release_then_policy_applies(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, policy="delete", keep_on_failure=False)
    folder = _folder(manager)

    outcome = manager.release(SESSION, failed=True)

    assert outcome.action is ScratchAction.DELETED and outcome.reason == REASON_POLICY_DELETE
    assert not folder.exists()


def given_released_session_when_released_again_then_nothing_happens(tmp_path: Path) -> None:
    manager = _manager(tmp_path, policy="delete")
    _folder(manager)
    manager.release(SESSION, failed=False)

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.NONE and outcome.reason == REASON_NEVER_CREATED
    assert outcome.path is None


def given_session_that_never_ran_a_command_when_release_then_nothing_happens(
    tmp_path: Path,
) -> None:
    outcome = _manager(tmp_path).release("sess-9999", failed=False)
    assert outcome.action is ScratchAction.NONE and outcome.reason == REASON_NEVER_CREATED


def given_disabled_scratch_when_release_then_no_action_and_nothing_created(
    tmp_path: Path,
) -> None:
    outcome = _manager(tmp_path, enabled=False).release(SESSION, failed=True)

    assert outcome.action is ScratchAction.NONE and outcome.reason == REASON_DISABLED
    assert not (tmp_path / "scratch").exists()


def given_several_sessions_when_release_all_then_each_handled_in_session_order(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, policy="delete")
    for session_id in (OTHER, SESSION):
        _folder(manager, session_id)

    outcomes = manager.release_all()

    assert [outcome.session_id for outcome in outcomes] == [SESSION, OTHER]
    assert all(outcome.action is ScratchAction.DELETED for outcome in outcomes)
    assert list((tmp_path / "scratch").iterdir()) == []
    assert manager.release_all() == []  # the manager forgot everything it handled


def given_outcome_when_serialised_then_json_round_trips(tmp_path: Path) -> None:
    manager = _manager(tmp_path, policy="delete")
    _write(_folder(manager), "a.txt")

    dumped = manager.release(SESSION, failed=False).model_dump(mode="json")

    assert json.loads(json.dumps(dumped))["action"] == "deleted"


# ================================================================================================
# 5. a working space the user owns — we never delete what we did not create
# ================================================================================================
def given_bound_working_space_when_environment_then_variables_name_the_user_folder(
    tmp_path: Path,
) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir()
    manager = _manager(tmp_path)

    bound = manager.bind(SESSION, user_folder)

    assert bound == user_folder
    assert manager.environment(SESSION)[ENV_WORKING_SPACE] == str(user_folder)
    assert manager.environment(SESSION)[ENV_SCRATCH_DIR] == str(user_folder)
    assert not (tmp_path / "scratch").exists()  # no generated folder for this session


@pytest.mark.skipif(IS_WINDOWS, reason="POSIX permission bits only (ADR-003)")
def given_bound_working_space_when_used_then_its_permissions_are_untouched(
    tmp_path: Path,
) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir(mode=0o755)
    user_folder.chmod(0o755)
    manager = _manager(tmp_path)

    manager.bind(SESSION, user_folder)
    manager.environment(SESSION)

    assert stat.S_IMODE(user_folder.stat().st_mode) == 0o755


@pytest.mark.parametrize("policy", ["delete", "archive"])
def given_bound_working_space_when_release_then_folder_untouched_whatever_the_policy(
    tmp_path: Path, policy: str
) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir()
    (user_folder / "mine.txt").write_bytes(b"precious")
    manager = _manager(tmp_path, policy=policy)
    manager.bind(SESSION, user_folder)

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.NONE
    assert outcome.reason == REASON_BOUND_WORKING_SPACE
    assert outcome.path == str(user_folder)
    assert [entry.relative_path for entry in outcome.inventory.entries] == ["mine.txt"]
    assert (user_folder / "mine.txt").read_bytes() == b"precious"
    assert not (tmp_path / "archive").exists()


def given_generated_folder_when_bound_afterwards_then_only_the_generated_one_is_cleaned(
    tmp_path: Path,
) -> None:
    """The policy follows what the application made, never what it was handed."""
    user_folder = tmp_path / "project"
    user_folder.mkdir()
    (user_folder / "mine.txt").write_bytes(b"precious")
    manager = _manager(tmp_path, policy="delete")
    generated = _folder(manager)

    manager.bind(SESSION, user_folder)
    assert manager.environment(SESSION)[ENV_WORKING_SPACE] == str(user_folder)
    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.DELETED and outcome.path == str(generated)
    assert not generated.exists()
    assert (user_folder / "mine.txt").read_bytes() == b"precious"


def given_bound_session_when_bound_to_none_then_the_generated_folder_applies_again(
    tmp_path: Path,
) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir()
    manager = _manager(tmp_path)
    manager.bind(SESSION, user_folder)

    assert manager.bind(SESSION, None) is None
    assert _folder(manager) == tmp_path / "scratch" / SESSION


def given_disabled_scratch_when_bind_then_none_and_no_variable_exported(tmp_path: Path) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir()
    manager = _manager(tmp_path, enabled=False)

    assert manager.bind(SESSION, user_folder) is None
    assert manager.environment(SESSION) == {}


def given_working_space_as_a_plain_string_when_bind_then_accepted(tmp_path: Path) -> None:
    user_folder = tmp_path / "project"
    user_folder.mkdir()

    assert _manager(tmp_path).bind(SESSION, str(user_folder)) == user_folder


# ---- refusals ----------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("value", "reason"),
    [("", "blank"), ("   ", "blank"), ("relative/dir", "not_absolute")],
)
def given_unusable_working_space_when_bind_then_config_error_naming_the_reason(
    tmp_path: Path, value: str, reason: str
) -> None:
    with pytest.raises(ConfigError) as caught:
        _manager(tmp_path).bind(SESSION, value)

    assert caught.value.error.error_code == WORKING_SPACE_INVALID
    assert caught.value.error.details["reason"] == reason


def given_missing_working_space_when_bind_then_config_error_does_not_exist(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "nowhere"

    with pytest.raises(ConfigError) as caught:
        _manager(tmp_path).bind(SESSION, missing)

    assert caught.value.error.details == {"path": str(missing), "reason": "does_not_exist"}


def given_file_as_working_space_when_bind_then_config_error_not_a_directory(
    tmp_path: Path,
) -> None:
    target = tmp_path / "a-file.txt"
    target.write_bytes(b"x")

    with pytest.raises(ConfigError) as caught:
        _manager(tmp_path).bind(SESSION, target)

    assert caught.value.error.details["reason"] == "not_a_directory"


def given_refused_working_space_when_error_serialised_then_details_are_json(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError) as caught:
        _manager(tmp_path).bind(SESSION, "relative/dir")

    assert json.loads(json.dumps(caught.value.error.details))["reason"] == "not_absolute"


def given_refused_working_space_when_bind_then_nothing_is_bound(tmp_path: Path) -> None:
    manager = _manager(tmp_path)
    with pytest.raises(ConfigError):
        manager.bind(SESSION, "relative/dir")

    assert _folder(manager) == tmp_path / "scratch" / SESSION


# ================================================================================================
# 6. a cleanup that fails is reported, never raised
# ================================================================================================
def given_unwritable_folder_when_release_deletes_then_failure_reported_and_not_raised(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unwritable directory (the archetypal case) must not turn into a failed session."""
    manager = _manager(tmp_path, policy="delete")
    folder = _folder(manager)
    _write(folder, "left.txt")

    def refuse(path: Any, *args: Any, **kwargs: Any) -> None:
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(scratch_module.shutil, "rmtree", refuse)
    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.FAILED
    assert outcome.reason == REASON_POLICY_DELETE
    assert outcome.error is not None and outcome.error.startswith("PermissionError:")
    assert [entry.relative_path for entry in outcome.inventory.entries] == ["left.txt"]
    assert folder.is_dir()  # nothing was removed, and the caller was not interrupted


def given_folder_removed_behind_the_manager_when_release_deletes_then_failure_reported(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, policy="delete")
    folder = _folder(manager)
    shutil.rmtree(folder)

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.FAILED
    assert outcome.error is not None and "FileNotFoundError" in outcome.error


def given_occupied_archive_destination_when_release_archives_then_failure_reported(
    tmp_path: Path,
) -> None:
    """``shutil.move`` would nest the folder inside the existing one: refuse instead."""
    clock = FakeClock(start=datetime(2026, 9, 19, 14, 30, 5, tzinfo=UTC))
    manager = ScratchManager(_section(tmp_path, policy="archive"), clock)
    folder = _folder(manager)
    (tmp_path / "archive" / f"{SESSION}-20260919T143005").mkdir(parents=True)

    outcome = manager.release(SESSION, failed=False)

    assert outcome.action is ScratchAction.FAILED
    assert outcome.reason == REASON_POLICY_ARCHIVE
    assert outcome.error is not None and "FileExistsError" in outcome.error
    assert folder.is_dir()


def given_failing_cleanup_when_release_all_then_every_session_still_handled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager = _manager(tmp_path, policy="delete")
    _folder(manager, SESSION)
    _folder(manager, OTHER)

    def refuse(path: Any, *args: Any, **kwargs: Any) -> None:
        raise OSError(5, "Input/output error", str(path))

    monkeypatch.setattr(scratch_module.shutil, "rmtree", refuse)
    outcomes = manager.release_all()

    assert [outcome.action for outcome in outcomes] == [ScratchAction.FAILED] * 2


# ================================================================================================
# 7. the ``[scratch]`` section
# ================================================================================================
def given_no_file_when_config_loaded_then_scratch_defaults_apply() -> None:
    scratch = AppConfig().scratch
    assert scratch.enabled is True
    assert (scratch.root, scratch.archive_root) == ("./data/scratch", "./data/scratch-archive")
    assert scratch.policy == "delete"
    assert scratch.keep_on_failure is True
    assert scratch.max_inventory_entries == 200


@pytest.mark.parametrize("key", ["root", "archive_root"])
def given_blank_directory_when_config_built_then_rejected(key: str) -> None:
    with pytest.raises(ValueError, match="must name a directory"):
        ScratchSection(**{key: "   "})


@pytest.mark.parametrize(
    ("root", "archive_root"),
    [
        ("/tmp/scratch", "/tmp/scratch"),
        ("/tmp/scratch", "/tmp/scratch/archive"),
        ("/tmp/scratch/sessions", "/tmp/scratch"),
    ],
)
def given_archive_root_nested_with_root_when_config_built_then_rejected(
    root: str, archive_root: str
) -> None:
    """An archive inside ``root`` would read as a session folder of the same run."""
    with pytest.raises(ValueError, match="outside scratch.root"):
        ScratchSection(root=root, archive_root=archive_root)


def given_unknown_policy_when_config_built_then_rejected() -> None:
    with pytest.raises(ValueError):
        ScratchSection(policy="burn")  # type: ignore[arg-type]


def given_non_positive_inventory_bound_when_config_built_then_rejected() -> None:
    with pytest.raises(ValueError):
        ScratchSection(max_inventory_entries=0)


# ================================================================================================
# 8. the variables actually reach a command (real process, ADR-003)
# ================================================================================================
@pytest.mark.real_subprocess
async def given_scratch_variables_in_the_spec_when_a_real_command_runs_then_it_reads_them(
    tmp_path: Path,
) -> None:
    """End to end, without the runner: the overlay of ``CommandSpec.env`` reaches the process.

    The probe is a file rather than a ``-c`` snippet so that the command line stays quote-free and
    identical on bash/sh and PowerShell (the convention of ``test_phase4_real_subprocess``).
    """
    manager = _manager(tmp_path)
    env = manager.environment(SESSION)
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import os\n"
        f"for name in ({ENV_WORKING_SPACE!r}, {ENV_SCRATCH_DIR!r}, {ENV_SESSION_ID!r}):\n"
        "    print(os.environ[name])\n",
        encoding="utf-8",
    )
    if IS_WINDOWS:
        cmd = f"& '{sys.executable}' '{probe}'"
    else:
        cmd = f"{shlex.quote(sys.executable)} {shlex.quote(str(probe))}"
    executor = SubprocessCommandExecutor(
        ExecutionSection(cancel_drain_timeout_ms=500, live_output_interval_ms=0), SystemClock()
    )

    raw = await executor.execute(
        CommandSpec(task_id="t1", cmd=cmd, timeout_ms=10_000, cwd=str(Path.cwd()), env=env),
        cancel=CancellationToken(),
    )

    folder = str(tmp_path / "scratch" / SESSION)
    assert raw.outcome is TaskState.COMPLETED, raw.stderr
    assert raw.stdout.decode().splitlines() == [folder, folder, SESSION]
