"""Which shell the machine actually runs, and the vocabulary used to name it (ADR-030 §1).

ADR-003 §3 gave the application two environment settings, ``[execution] shell`` and
``[execution] cwd``, and left the interpreter to a one-line default (``bash`` else ``sh`` on POSIX,
``powershell`` on Windows). That default was enough to *launch* a command and not enough to *reason*
about one: nothing said which **dialect** the machine speaks, so nothing could announce it to the
model (ADR-030 §3) nor decide whether a command had to be translated (ADR-030 §4).

This module answers that question and nothing else:

- :class:`ShellDialect` — the four values the rest of the application talks in: ``posix``,
  ``powershell``, ``cmd``, and ``unknown`` for an interpreter nobody here recognises;
- :class:`DetectedShell` — the answer as a value object: the ``program`` that will be launched, its
  bare ``name``, its ``dialect`` and the ``source`` that decided it (pinned by the operator, found
  on the ``PATH``, or the documented fallback);
- :func:`detect_shell` / :func:`describe_environment` — the detection itself, with ``which``
  injected (``shutil.which`` by default) so that a test never depends on the machine it runs on;
- :func:`command_not_run_reason` (ADR-032) — what an exit code means when it is the **shell's**
  answer rather than the program's: every command runs as ``<shell> -c <cmd>``, so a program that
  does not exist is reported by a shell that started perfectly well, with the conventional code of
  its dialect (127 "not found" and 126 "found but not executable" on POSIX, the same codes from
  PowerShell as the launch script of ADR-032 wraps it, 9009 from ``cmd``).

Nothing here raises: an absent ``PATH``, a ``which`` that returns nothing and an interpreter nobody
recognises all degrade to a documented value (``/bin/sh``, ``powershell``, ``ShellDialect.UNKNOWN``).
"""

from __future__ import annotations

import os
import re
import shutil
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum, unique
from types import MappingProxyType

__all__ = [
    "COMMAND_NOT_EXECUTABLE",
    "COMMAND_NOT_FOUND",
    "DEFAULT_POSIX_SHELL",
    "DEFAULT_WINDOWS_SHELL",
    "NOT_RUN_EXIT_CODES",
    "POSIX_SHELL_CANDIDATES",
    "WINDOWS_SHELL_CANDIDATES",
    "DetectedShell",
    "ExecutionEnvironment",
    "ShellDialect",
    "ShellSource",
    "Which",
    "classify_shell",
    "command_not_run_reason",
    "describe_environment",
    "detect_shell",
    "operating_system_name",
    "shell_name",
]

#: The probe used to look an interpreter up on the ``PATH``; ``shutil.which`` in production.
Which = Callable[[str], str | None]


@unique
class ShellDialect(StrEnum):
    """The language a command is written in — and the one the machine will read it in."""

    POSIX = "posix"
    POWERSHELL = "powershell"
    CMD = "cmd"
    #: An interpreter the application does not recognise (an operator may pin anything). Nothing is
    #: ever translated to or from it, and the announcement says so rather than guessing.
    UNKNOWN = "unknown"


@unique
class ShellSource(StrEnum):
    """How the shell was decided — the third field of :class:`DetectedShell`."""

    #: ``[execution] shell`` names it: the operator's choice always wins, it is never probed.
    CONFIGURED = "configured"
    #: Found on the ``PATH`` by ``which``, in the platform's candidate order.
    DETECTED = "detected"
    #: Nothing was found: the documented fallback of the platform.
    DEFAULT = "default"


#: Probed in this order when ``[execution] shell`` is empty. ``sh`` stays last because it exists
#: everywhere: reaching it means neither of the two interactive shells was installed.
POSIX_SHELL_CANDIDATES: tuple[str, ...] = ("bash", "zsh", "sh")
#: PowerShell 7 (``pwsh``, cross-platform, still installed under that name on Windows) is preferred
#: over Windows PowerShell 5.1 (``powershell``); both speak the same dialect.
WINDOWS_SHELL_CANDIDATES: tuple[str, ...] = ("pwsh", "powershell")

#: Fallbacks when no candidate is on the ``PATH``. They are what the platform adapters launched
#: before this module existed, so a machine without a ``PATH`` behaves exactly as it used to.
DEFAULT_POSIX_SHELL = "/bin/sh"
DEFAULT_WINDOWS_SHELL = "powershell"

#: Bare interpreter name -> dialect. Anything absent is :data:`ShellDialect.UNKNOWN`.
_DIALECT_BY_NAME: Mapping[str, ShellDialect] = MappingProxyType(
    {
        "bash": ShellDialect.POSIX,
        "sh": ShellDialect.POSIX,
        "zsh": ShellDialect.POSIX,
        "dash": ShellDialect.POSIX,
        "ash": ShellDialect.POSIX,
        "ksh": ShellDialect.POSIX,
        "powershell": ShellDialect.POWERSHELL,
        "pwsh": ShellDialect.POWERSHELL,
        "cmd": ShellDialect.CMD,
    }
)

#: Extensions an interpreter carries on Windows; stripped before the name is matched.
_PROGRAM_SUFFIXES = (".exe", ".cmd", ".bat", ".com")
_PATH_SEPARATORS = re.compile(r"[\\/]")

#: ``sys.platform`` -> the name announced to the model. Anything else is announced verbatim.
_OPERATING_SYSTEMS: Mapping[str, str] = MappingProxyType(
    {"win32": "Windows", "darwin": "macOS", "linux": "Linux"}
)

#: ADR-032 — the ``reason`` of a task result whose exit code is the shell's answer, not the
#: program's: the shell found no such program, or found it and could not run it.
COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"
COMMAND_NOT_EXECUTABLE = "COMMAND_NOT_EXECUTABLE"

#: POSIX.1, Shell Command Language, 2.8.2 "Exit Status for Commands": a command that is not found
#: exits 127, one that is found but is not an executable utility exits 126.
_POSIX_NOT_RUN: Mapping[int, str] = MappingProxyType(
    {127: COMMAND_NOT_FOUND, 126: COMMAND_NOT_EXECUTABLE}
)

#: ADR-032 — per dialect, the exit codes by which the **shell** says it could not run the program a
#: command names, and the reason each one stands for. These codes are conventions of the shell, not
#: a promise of the program: a program may exit with one of them on its own, and the reason is then
#: a presumption. That is acceptable because the reading only ever makes the application more
#: cautious — such an exit is never a verdict (ADR-029 §2), so the plan stops as for any failure.
NOT_RUN_EXIT_CODES: Mapping[ShellDialect, Mapping[int, str]] = MappingProxyType(
    {
        ShellDialect.POSIX: _POSIX_NOT_RUN,
        # the launch script of ADR-032 turns PowerShell's own "not found" and "cannot be run"
        # errors into the POSIX codes (execution/platform.py)
        ShellDialect.POWERSHELL: _POSIX_NOT_RUN,
        # cmd's own code for "is not recognized as an internal or external command"; nothing it
        # answers tells "found but cannot be run" apart from an ordinary failure
        ShellDialect.CMD: MappingProxyType({9009: COMMAND_NOT_FOUND}),
        # launched as ``<shell> -c <cmd>`` like a POSIX shell, and read with the POSIX convention:
        # withholding a verdict is the safe side of a presumption
        ShellDialect.UNKNOWN: _POSIX_NOT_RUN,
    }
)


def shell_name(program: str) -> str:
    """``C:\\Windows\\System32\\cmd.exe`` -> ``cmd``, ``/usr/bin/bash`` -> ``bash``, ``""`` -> ``""``.

    Both separators are honoured whatever the host, so a Windows path is read correctly by a test
    running on Linux (and the reverse).
    """
    name = _PATH_SEPARATORS.split(program.strip().strip("\"'"))[-1].lower()
    for suffix in _PROGRAM_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def classify_shell(program: str) -> ShellDialect:
    """The dialect of an interpreter, from its name alone. Never raises.

    An interpreter nobody recognises is :data:`ShellDialect.UNKNOWN`: the application still launches
    it (ADR-003 §3 keeps the operator in charge) but it neither announces a dialect it invented nor
    translates anything towards it.
    """
    return _DIALECT_BY_NAME.get(shell_name(program), ShellDialect.UNKNOWN)


def command_not_run_reason(dialect: ShellDialect, exit_code: int | None) -> str | None:
    """``COMMAND_NOT_FOUND`` or ``COMMAND_NOT_EXECUTABLE`` when ``exit_code`` is the conventional
    answer of a ``dialect`` shell that could not find, or could not run, the program the command
    names (ADR-032); ``None`` for any other code, and for a command that never returned one.

    The shell did run, so the command is not ``not_started``; the program behind it never did, so
    its exit code is no verdict, whatever the program.
    """
    if exit_code is None:
        return None
    return NOT_RUN_EXIT_CODES[dialect].get(exit_code)


@dataclass(frozen=True)
class DetectedShell:
    """What will actually receive the commands: the program, its dialect, and how it was decided."""

    program: str
    name: str
    dialect: ShellDialect
    source: ShellSource

    @classmethod
    def of(cls, program: str, source: ShellSource) -> DetectedShell:
        return cls(
            program=program,
            name=shell_name(program),
            dialect=classify_shell(program),
            source=source,
        )


@dataclass(frozen=True)
class ExecutionEnvironment:
    """What the application knows about where the commands run — and announces (ADR-030 §3).

    Three facts, no more: the operating system, the shell (hence the dialect), and the working
    directory the commands start in. Everything else the model still discovers by itself with a
    ``discovery_plan``, exactly as ADR-003 §3 prescribes.
    """

    operating_system: str
    shell: DetectedShell
    cwd: str

    @property
    def dialect(self) -> ShellDialect:
        return self.shell.dialect


def operating_system_name(platform: str) -> str:
    """``win32`` -> ``Windows``, ``darwin`` -> ``macOS``, ``linux`` -> ``Linux``, else verbatim."""
    return _OPERATING_SYSTEMS.get(platform, platform)


def detect_shell(
    configured: str | None,
    *,
    windows: bool,
    which: Which | None = None,
) -> DetectedShell:
    """The interpreter that will run the commands, and how that was decided.

    ``configured`` is ``[execution] shell``: when it names something, that something is used as is
    and the source is ``configured`` — the operator is never second-guessed, even when the name is
    unknown. Otherwise the platform candidates are probed in order with ``which`` (``detected``),
    and a machine where none of them resolves falls back to the documented default (``default``).
    """
    probe = which if which is not None else shutil.which
    pinned = (configured or "").strip()
    if pinned:
        return DetectedShell.of(pinned, ShellSource.CONFIGURED)
    candidates = WINDOWS_SHELL_CANDIDATES if windows else POSIX_SHELL_CANDIDATES
    for candidate in candidates:
        try:
            found = probe(candidate)
        except OSError:  # a broken PATH must not stop a command from running
            found = None
        if found:
            return DetectedShell.of(found, ShellSource.DETECTED)
    fallback = DEFAULT_WINDOWS_SHELL if windows else DEFAULT_POSIX_SHELL
    return DetectedShell.of(fallback, ShellSource.DEFAULT)


def describe_environment(
    configured_shell: str | None,
    cwd: str,
    *,
    platform: str,
    which: Which | None = None,
) -> ExecutionEnvironment:
    """The three facts of :class:`ExecutionEnvironment` for this configuration and this platform.

    ``cwd`` is made absolute **lexically** (``os.path.abspath``, like ``config._as_absolute``): the
    announcement must not depend on what exists on the disk right now, nor follow a symbolic link.
    """
    return ExecutionEnvironment(
        operating_system=operating_system_name(platform),
        shell=detect_shell(configured_shell, windows=platform == "win32", which=which),
        cwd=os.path.abspath(os.path.expanduser(cwd or ".")),
    )
