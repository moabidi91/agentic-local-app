"""Platform layer of the executor (ADR-003, ADR-016): shell, launch, spawn flags, termination.

Two adapters, selected from ``sys.platform`` by :func:`select_platform` (and injectable in tests):

+----------------------+-------------------------------------------+---------------------------------------------------+
|                      | POSIX                                     | Windows                                           |
+======================+===========================================+===================================================+
| default shell        | ``bash`` if found, else ``sh``            | ``powershell``                                    |
| launch               | ``<shell> -c <cmd>``                      | ``powershell -NoProfile -NonInteractive -Command`` |
|                      |                                           | ``<cmd>`` · ``cmd /c <cmd>`` when the shell is cmd |
| spawn                | ``start_new_session=True`` (own group)    | ``creationflags=CREATE_NEW_PROCESS_GROUP``        |
| soft termination     | ``SIGTERM`` to the process group          | ``CTRL_BREAK_EVENT`` on the process handle        |
| hard termination     | ``SIGKILL`` to the process group          | ``taskkill /T /F /PID`` + ``TerminateProcess``     |
+----------------------+-------------------------------------------+---------------------------------------------------+

The interpreter is launched with ``create_subprocess_exec(shell, "-c", cmd)`` rather than the
``create_subprocess_shell(cmd, executable=shell)`` wording of ADR-003: the latter keeps ``argv[0]``
equal to ``/bin/sh``, and bash invoked under that name silently switches to POSIX mode. The command
string itself is never rewritten (§1, §2.3).

Known Windows PowerShell limitation: ``powershell -Command <cmd>`` reports exit code 1 for any
native command that exited with a code other than 0 or 1, and decorates the stderr of native
commands with error-record text. The command is still run untouched (§1); the model sees a failure
with the real stderr content inside the decoration.

Orphan termination after a crash (ADR-016) goes through a :class:`ProcessTable`, the only object
that touches processes the executor did not spawn. It is injectable so that the two-phase mechanics
are unit-tested on a double; the real tables are best effort and documented as such.

Bounded blocking waits (``ProcessTable.wait_exit``) use ``threading.Event().wait`` so that no
component here reads the wall clock (ADR-017): durations are counted in polling steps.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from agentic_local_app.config import ExecutionSection

__all__ = [
    "CREATE_NEW_PROCESS_GROUP",
    "CTRL_BREAK_EVENT",
    "ORPHAN_START_TOLERANCE_MS",
    "LaunchSpec",
    "ProcessTable",
    "PosixProcessTable",
    "WindowsProcessTable",
    "PlatformAdapter",
    "PosixPlatformAdapter",
    "WindowsPlatformAdapter",
    "parse_proc_stat_start_ticks",
    "select_platform",
]

#: Windows constants, with their documented values as fallback so that the Windows adapter can be
#: unit-tested (build/spawn kwargs only) on a POSIX host.
CREATE_NEW_PROCESS_GROUP: int = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
CTRL_BREAK_EVENT: int = getattr(signal, "CTRL_BREAK_EVENT", 1)
_SIGKILL: int = getattr(signal, "SIGKILL", 9)

#: An orphan is ours only if its start time is within this window around the task's ``started_at``
#: (ADR-016): a pid reused by a process started later — or one that predates the task — is never
#: signalled. The window absorbs the spawn latency and the one-second granularity of ``btime``.
ORPHAN_START_TOLERANCE_MS = 5_000

#: Polling step of the bounded blocking waits.
_POLL_STEP_MS = 50


@dataclass(frozen=True)
class LaunchSpec:
    """The interpreter invocation for one command: ``program`` is the interpreter executable and
    ``args`` its arguments, the last of which is the untouched ``cmd``."""

    program: str
    args: tuple[str, ...]

    @property
    def argv(self) -> tuple[str, ...]:
        return (self.program, *self.args)


# ------------------------------------------------------------------------------------------------
# Process tables (processes not spawned by this executor: recovery of orphans)
# ------------------------------------------------------------------------------------------------
class ProcessTable(ABC):
    """What the platform needs to know and do about an arbitrary pid."""

    @abstractmethod
    def start_time(self, pid: int) -> datetime | None:
        """UTC start time of the process, ``None`` when it does not exist or cannot be determined."""

    @abstractmethod
    def terminate(self, pid: int, pgid: int | None) -> None:
        """Soft termination (SIGTERM to the group / CTRL_BREAK or ``taskkill /T``). Never raises."""

    @abstractmethod
    def kill(self, pid: int, pgid: int | None) -> None:
        """Hard termination (SIGKILL to the group / ``taskkill /T /F``). Never raises."""

    @abstractmethod
    def wait_exit(self, pid: int, timeout_ms: int) -> bool:
        """Block at most ``timeout_ms`` until the process is gone. ``True`` when it is."""


def _bounded_wait(is_gone: Callable[[], bool], timeout_ms: int) -> bool:
    steps = max(1, -(-timeout_ms // _POLL_STEP_MS))  # ceil division
    pause = threading.Event()
    for _ in range(steps):
        if is_gone():
            return True
        pause.wait(_POLL_STEP_MS / 1000)
    return is_gone()


def parse_proc_stat_start_ticks(line: str) -> int:
    """Field 22 (``starttime``, clock ticks since boot) of a ``/proc/<pid>/stat`` line.

    The command name (field 2) is parenthesised and may contain spaces or parentheses, so fields are
    counted after the **last** closing parenthesis.
    """
    end = line.rfind(")")
    if end < 0:
        raise ValueError("not a /proc/<pid>/stat line")
    rest = line[end + 1 :].split()
    if len(rest) < 20:
        raise ValueError("truncated /proc/<pid>/stat line")
    return int(rest[19])  # fields 3.. → starttime is field 22 → index 19


class PosixProcessTable(ProcessTable):
    """``/proc`` based table (Linux). Where ``/proc`` is absent (macOS) start times are unknown, so
    :meth:`PlatformAdapter.terminate_orphan` never signals anything there — documented limitation."""

    def __init__(self, proc_root: str | os.PathLike[str] = "/proc") -> None:
        self._proc = Path(proc_root)

    def _boot_time(self) -> datetime | None:
        try:
            for raw in (self._proc / "stat").read_text(encoding="ascii").splitlines():
                if raw.startswith("btime "):
                    return datetime.fromtimestamp(int(raw.split()[1]), tz=UTC)
        except (OSError, ValueError):
            return None
        return None

    def start_time(self, pid: int) -> datetime | None:
        try:
            line = (self._proc / str(pid) / "stat").read_text(encoding="ascii", errors="replace")
            ticks = parse_proc_stat_start_ticks(line)
            clk_tck = os.sysconf("SC_CLK_TCK")
        except (OSError, ValueError, AttributeError):
            return None
        boot = self._boot_time()
        if boot is None or clk_tck <= 0:
            return None
        return boot + timedelta(seconds=ticks / clk_tck)

    @staticmethod
    def _signal(pid: int, pgid: int | None, sig: int) -> None:
        if sys.platform == "win32":  # pragma: no cover - POSIX table never used on Windows
            return
        try:
            if pgid is not None:
                os.killpg(pgid, sig)
            else:
                os.kill(pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def terminate(self, pid: int, pgid: int | None) -> None:
        self._signal(pid, pgid, signal.SIGTERM)

    def kill(self, pid: int, pgid: int | None) -> None:
        self._signal(pid, pgid, _SIGKILL)

    def _is_gone(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        except PermissionError:
            return False
        # a zombie still answers to signal 0: read its state when /proc is available
        try:
            line = (self._proc / str(pid) / "stat").read_text(encoding="ascii", errors="replace")
        except OSError:
            return False
        state = line[line.rfind(")") + 1 :].split()
        return bool(state) and state[0] in {"Z", "X"}

    def wait_exit(self, pid: int, timeout_ms: int) -> bool:
        return _bounded_wait(lambda: self._is_gone(pid), timeout_ms)


class WindowsProcessTable(ProcessTable):
    """Best-effort table built on ``powershell``, ``tasklist`` and ``taskkill`` (psutil is not a
    dependency). Any failure of these tools reads as "unknown", which never triggers a kill."""

    _RUN_TIMEOUT_S = 10

    @staticmethod
    def _run(*argv: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            timeout=WindowsProcessTable._RUN_TIMEOUT_S,
            check=False,
        )

    def start_time(self, pid: int) -> datetime | None:
        script = f"(Get-Process -Id {int(pid)}).StartTime.ToUniversalTime().ToString('o')"
        try:
            done = self._run("powershell", "-NoProfile", "-NonInteractive", "-Command", script)
            if done.returncode != 0 or not done.stdout.strip():
                return None
            parsed = datetime.fromisoformat(done.stdout.strip().replace("Z", "+00:00"))
        except (OSError, ValueError, subprocess.SubprocessError):
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def terminate(self, pid: int, pgid: int | None) -> None:
        try:
            self._run("taskkill", "/PID", str(int(pid)), "/T")
        except (OSError, subprocess.SubprocessError):
            pass

    def kill(self, pid: int, pgid: int | None) -> None:
        try:
            self._run("taskkill", "/PID", str(int(pid)), "/T", "/F")
        except (OSError, subprocess.SubprocessError):
            pass

    def _is_gone(self, pid: int) -> bool:
        try:
            done = self._run("tasklist", "/FI", f"PID eq {int(pid)}", "/NH", "/FO", "CSV")
        except (OSError, subprocess.SubprocessError):
            return False
        return f'"{int(pid)}"' not in done.stdout

    def wait_exit(self, pid: int, timeout_ms: int) -> bool:
        return _bounded_wait(lambda: self._is_gone(pid), timeout_ms)


# ------------------------------------------------------------------------------------------------
# Platform adapters
# ------------------------------------------------------------------------------------------------
class PlatformAdapter(ABC):
    def __init__(
        self, config: ExecutionSection, *, process_table: ProcessTable | None = None
    ) -> None:
        self._config = config
        self._table = process_table or self._default_process_table()

    @abstractmethod
    def _default_process_table(self) -> ProcessTable: ...

    @abstractmethod
    def default_shell(self) -> str:
        """The interpreter used when ``execution.shell`` is empty."""

    @abstractmethod
    def build_launch(self, cmd: str, shell: str | None) -> LaunchSpec:
        """The argv running ``cmd`` (untouched) under ``shell`` (or the default shell)."""

    @abstractmethod
    def spawn_kwargs(self) -> dict[str, Any]:
        """Extra keyword arguments of ``asyncio.create_subprocess_exec`` (own process group)."""

    @abstractmethod
    def process_group_id(self, pid: int) -> int | None:
        """Group id to persist next to the pid (ADR-016); ``None`` on Windows."""

    @abstractmethod
    async def terminate_gracefully(
        self, proc: asyncio.subprocess.Process, pgid: int | None
    ) -> None:
        """First phase of the termination (ADR-003). Never raises."""

    @abstractmethod
    async def kill(self, proc: asyncio.subprocess.Process, pgid: int | None) -> None:
        """Second phase, after ``cancel_drain_timeout_ms`` without exit. Never raises."""

    def terminate_orphan(self, pid: int, pgid: int | None, started_at: datetime) -> bool:
        """Two-phase termination of a process left behind by a crash (ADR-016).

        Signals are sent only when the process exists **and** started within
        :data:`ORPHAN_START_TOLERANCE_MS` of ``started_at``; otherwise ``False`` is returned and
        nothing is touched (a reused pid is never killed). Returns ``True`` once the soft signal was
        sent, whether or not the process had to be killed afterwards.
        """
        start = self._table.start_time(pid)
        if start is None:
            return False
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        drift_ms = abs((start - started_at) / timedelta(milliseconds=1))
        if drift_ms > ORPHAN_START_TOLERANCE_MS:
            return False
        drain_ms = self._config.cancel_drain_timeout_ms
        self._table.terminate(pid, pgid)
        if not self._table.wait_exit(pid, drain_ms):
            self._table.kill(pid, pgid)
            self._table.wait_exit(pid, drain_ms)
        return True


class PosixPlatformAdapter(PlatformAdapter):
    """Linux / macOS: ``bash`` (else ``sh``) ``-c``, own session, signals to the process group."""

    _FALLBACK_SHELL = "/bin/sh"

    def __init__(
        self,
        config: ExecutionSection,
        *,
        process_table: ProcessTable | None = None,
        which: Callable[[str], str | None] = shutil.which,
    ) -> None:
        super().__init__(config, process_table=process_table)
        self._which = which

    def _default_process_table(self) -> ProcessTable:
        return PosixProcessTable()

    def default_shell(self) -> str:
        for name in ("bash", "sh"):
            found = self._which(name)
            if found:
                return found
        return self._FALLBACK_SHELL

    def build_launch(self, cmd: str, shell: str | None) -> LaunchSpec:
        return LaunchSpec(program=shell or self.default_shell(), args=("-c", cmd))

    def spawn_kwargs(self) -> dict[str, Any]:
        return {"start_new_session": True}

    def process_group_id(self, pid: int) -> int | None:
        if sys.platform == "win32":  # pragma: no cover
            return None
        try:
            return os.getpgid(pid)
        except (ProcessLookupError, PermissionError):
            return pid  # start_new_session=True made the child the leader of its own group

    async def terminate_gracefully(
        self, proc: asyncio.subprocess.Process, pgid: int | None
    ) -> None:
        self._table.terminate(proc.pid, pgid)

    async def kill(self, proc: asyncio.subprocess.Process, pgid: int | None) -> None:
        self._table.kill(proc.pid, pgid)


class WindowsPlatformAdapter(PlatformAdapter):
    """Windows: PowerShell by default (``cmd /c`` when configured), new process group,
    ``CTRL_BREAK_EVENT`` then ``taskkill /T /F`` + ``TerminateProcess``."""

    _POWERSHELL_ARGS = ("-NoProfile", "-NonInteractive", "-Command")

    def _default_process_table(self) -> ProcessTable:
        return WindowsProcessTable()

    def default_shell(self) -> str:
        return "powershell"

    def build_launch(self, cmd: str, shell: str | None) -> LaunchSpec:
        program = shell or self.default_shell()
        name = Path(program).stem.lower()
        if "cmd" in name:
            return LaunchSpec(program=program, args=("/c", cmd))
        if "powershell" in name or name == "pwsh":
            return LaunchSpec(program=program, args=(*self._POWERSHELL_ARGS, cmd))
        return LaunchSpec(program=program, args=("-c", cmd))  # e.g. Git bash

    def spawn_kwargs(self) -> dict[str, Any]:
        return {"creationflags": CREATE_NEW_PROCESS_GROUP}

    def process_group_id(self, pid: int) -> int | None:
        return None

    async def terminate_gracefully(
        self, proc: asyncio.subprocess.Process, pgid: int | None
    ) -> None:
        try:
            proc.send_signal(CTRL_BREAK_EVENT)
        except (ProcessLookupError, OSError, ValueError):
            self._table.terminate(proc.pid, None)

    async def kill(self, proc: asyncio.subprocess.Process, pgid: int | None) -> None:
        self._table.kill(proc.pid, None)  # the whole tree first (grandchildren)
        try:
            proc.kill()
        except (ProcessLookupError, OSError):
            pass


def select_platform(
    config: ExecutionSection,
    *,
    platform: str | None = None,
    process_table: ProcessTable | None = None,
) -> PlatformAdapter:
    """The adapter for ``platform`` (``sys.platform`` by default): ``win32`` → Windows, else POSIX."""
    if (platform or sys.platform) == "win32":
        return WindowsPlatformAdapter(config, process_table=process_table)
    return PosixPlatformAdapter(config, process_table=process_table)
