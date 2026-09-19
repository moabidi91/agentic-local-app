"""Machine identity (ADR-024 §2): who the user is on the machine that runs the application.

The front pre-fills the user with this, and shows where it comes from: ``source`` names the step
that answered, so an interface can tell a name read from the environment from the fallback of
``transport.user_id``. Resolution stops at the first step that yields something:

+------+-----------------------------------+--------------------------------+
| Step | Windows                           | POSIX                          |
+======+===================================+================================+
| 1    | ``%USERNAME%`` (``env:USERNAME``) | ``$USER`` (``env:USER``)       |
+------+-----------------------------------+--------------------------------+
| 2    | ``whoami`` (``cmd:whoami``)       | ``$LOGNAME`` (``env:LOGNAME``) |
+------+-----------------------------------+--------------------------------+
| 3    | —                                 | ``id -un`` (``cmd:id``)        |
+------+-----------------------------------+--------------------------------+
| 4    | the configured user id (``config``), then ``unknown`` (``unknown``) |
+------+-----------------------------------+--------------------------------+

Nothing here reads the configuration: the fallback user id is a parameter, and the command runner
is injected (:data:`CommandRunner`) so that a test never spawns a process. The default runner is
best effort — short timeout, ``None`` on any failure — because identity must never block or break
the start-up (ADR-017: everything that touches the outside world is injectable).
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from collections.abc import Callable, Mapping

from pydantic import BaseModel, ConfigDict

__all__ = [
    "COMMAND_TIMEOUT_S",
    "CommandRunner",
    "SOURCE_CONFIG",
    "SOURCE_UNKNOWN",
    "UNKNOWN_USER",
    "UserIdentity",
    "current_hostname",
    "current_user",
    "run_identity_command",
]

#: The user id when even the configuration has nothing to say.
UNKNOWN_USER = "unknown"
#: ``source`` of the configured fallback and of the floor.
SOURCE_CONFIG = "config"
SOURCE_UNKNOWN = "unknown"
#: Wall-clock bound of the identity command: a start-up never waits on ``whoami`` (seconds).
COMMAND_TIMEOUT_S = 2.0

#: A command runner: the raw standard output of the command, or ``None`` when it did not answer.
CommandRunner = Callable[[list[str]], str | None]


class UserIdentity(BaseModel):
    """Who the user is on this machine, and how it was found (ADR-024 §2)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    user_id: str
    source: str
    host: str | None = None


def run_identity_command(command: list[str]) -> str | None:
    """The default :data:`CommandRunner`: never raises, never blocks, ``None`` on any failure."""
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_S,
            check=False,
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout


def current_hostname() -> str | None:
    """The machine name, best effort: ``None`` when the resolver has nothing (never raises)."""
    try:
        return socket.gethostname().strip() or None
    except OSError:  # pragma: no cover - the resolver is not supposed to fail
        return None


def current_user(
    environ: Mapping[str, str] | None = None,
    runner: CommandRunner | None = None,
    *,
    fallback_user_id: str = "",
    platform: str | None = None,
    hostname: Callable[[], str | None] | None = None,
) -> UserIdentity:
    """Resolve the identity of the user running the application (ADR-024 §2).

    ``environ``, ``runner``, ``platform`` (a ``sys.platform`` value) and ``hostname`` are the
    injection points; ``fallback_user_id`` is the configured ``transport.user_id``, used last.
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    run: CommandRunner = run_identity_command if runner is None else runner
    resolve_host = current_hostname if hostname is None else hostname
    host = resolve_host()
    windows = (platform or sys.platform) == "win32"

    for variable in ("USERNAME",) if windows else ("USER", "LOGNAME"):
        value = _clean(env.get(variable))
        if value:
            return UserIdentity(user_id=value, source=f"env:{variable}", host=host)

    command = ["whoami"] if windows else ["id", "-un"]
    value = _clean(run(list(command)))
    if value:
        return UserIdentity(user_id=value, source=f"cmd:{command[0]}", host=host)

    value = _clean(fallback_user_id)
    if value:
        return UserIdentity(user_id=value, source=SOURCE_CONFIG, host=host)
    return UserIdentity(user_id=UNKNOWN_USER, source=SOURCE_UNKNOWN, host=host)


def _clean(value: str | None) -> str:
    """First line, stripped, without the Windows domain prefix (``DOMAIN\\user`` → ``user``)."""
    if not value:
        return ""
    lines = value.strip().splitlines()
    if not lines:
        return ""
    return lines[0].strip().rpartition("\\")[2].strip()
