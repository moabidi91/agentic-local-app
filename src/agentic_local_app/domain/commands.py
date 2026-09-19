"""What a command line invokes, and the programs whose non-zero exit is a verdict (ADR-029 §2).

Pure text rules, no I/O: nothing here starts a process, resolves a ``PATH`` or looks at the
filesystem. Two questions are answered from the command line alone:

- :func:`invoked` — the program a command line starts (its first token, path and extension
  stripped, lower-cased) and its first non-option argument (``run`` in ``npm run build``);
- :class:`VerdictPrograms` — whether that program is one of those whose non-zero exit is a
  **result to interpret** (a compiler, a build tool, a test runner, a linter) rather than a task
  that went wrong.

The rule is deliberately literal, and what it cannot see is written down in ADR-029: a program
reached through a shell variable (``$BUILD_TOOL install``), a pipeline whose exit code is the last
command's (``mvn install | tail -80``), or a wrapper script named after nothing in particular
(``./build.sh``).
"""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterable

__all__ = ["VerdictPrograms", "invoked", "normalise_program_entry"]

#: Suffixes a program name carries on Windows or as a shell wrapper; stripped before matching, so
#: that ``mvn.cmd``, ``gradlew.bat`` and ``mvn`` are the same program.
_PROGRAM_SUFFIXES = (".exe", ".cmd", ".bat", ".ps1", ".sh")
#: ``NAME=value`` prefixes of a command line (``JAVA_HOME=/opt/jdk21 mvn test``): not the program.
_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_PATH_SEPARATORS = re.compile(r"[\\/]")
_QUOTES = "\"'"


def _tokens(cmd: str) -> list[str]:
    """Words of a command line, quotes respected but **not** interpreted (``posix=False`` keeps the
    backslashes of a Windows path). An unbalanced quote is not an error here: the plain split is
    good enough to read a first token."""
    try:
        return shlex.split(cmd, posix=False)
    except ValueError:
        return cmd.split()


def _bare(token: str) -> str:
    return token.strip().strip(_QUOTES)


def _program_name(token: str) -> str:
    """``/usr/bin/mvn`` -> ``mvn``, ``.\\gradlew.bat`` -> ``gradlew``, ``MVN`` -> ``mvn``."""
    name = _PATH_SEPARATORS.split(_bare(token))[-1].lower()
    for suffix in _PROGRAM_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def invoked(cmd: str | None) -> tuple[str, str | None]:
    """``(program, first non-option argument)`` of a command line.

    Leading ``NAME=value`` assignments are skipped, the program is reduced to its bare name and the
    argument is the first following word that does not start with ``-`` (so ``npm --silent run
    build`` still answers ``("npm", "run")``). An empty or blank command line answers ``("", None)``.
    """
    tokens = _tokens(cmd or "")
    index = 0
    while index < len(tokens) and _ASSIGNMENT.match(tokens[index]):
        index += 1
    if index >= len(tokens):
        return "", None
    program = _program_name(tokens[index])
    argument = next(
        (
            word.lower()
            for word in (_bare(token) for token in tokens[index + 1 :])
            if word and not word.startswith("-")
        ),
        None,
    )
    return program, argument


def normalise_program_entry(entry: str) -> str:
    """One configured entry, normalised: ``" MVN "`` -> ``"mvn"``, ``"/usr/bin/npm  RUN"`` ->
    ``"npm run"``.

    An entry is a program (``mvn``) or a program and the sub-command that matters (``npm run``).
    Anything else — blank, or three words — is a configuration error.
    """
    parts = entry.split()
    if not parts:
        raise ValueError("must name a program")
    if len(parts) > 2:
        raise ValueError(
            f"{entry!r}: an entry is a program, optionally followed by one sub-command"
        )
    program = _program_name(parts[0])
    if not program:
        raise ValueError(f"{entry!r}: must name a program")
    if len(parts) == 1:
        return program
    argument = _bare(parts[1]).lower()
    if not argument:
        raise ValueError(f"{entry!r}: must name a sub-command after the program")
    return f"{program} {argument}"


class VerdictPrograms:
    """The programs whose non-zero exit is a verdict (``execution.verdict_programs``, ADR-029 §2).

    Built from the configured entries — a program (``mvn``) or a program and the sub-command that
    matters (``npm run``, because ``npm install`` is not a build and ``npm build`` does not exist).
    An empty set recognises nothing, which is how an operator turns the rule off.
    """

    __slots__ = ("_pairs", "_programs")

    def __init__(self, entries: Iterable[str] = ()) -> None:
        programs: set[str] = set()
        pairs: set[tuple[str, str]] = set()
        for entry in entries:
            program, _, argument = normalise_program_entry(entry).partition(" ")
            if argument:
                pairs.add((program, argument))
            else:
                programs.add(program)
        self._programs = frozenset(programs)
        self._pairs = frozenset(pairs)

    def __bool__(self) -> bool:
        return bool(self._programs or self._pairs)

    def __len__(self) -> int:
        return len(self._programs) + len(self._pairs)

    def matches(self, cmd: str | None) -> bool:
        """Whether ``cmd`` invokes one of the recognised programs."""
        program, argument = invoked(cmd)
        if not program:
            return False
        if program in self._programs:
            return True
        return argument is not None and (program, argument) in self._pairs

    def is_verdict(self, cmd: str | None, exit_code: int | None, *, timed_out: bool) -> bool:
        """Whether this exit code is a **verdict** of a recognised program: the command ran to the
        end (an exit code exists, no timeout) and answered something other than zero."""
        if timed_out or exit_code is None or exit_code == 0:
            return False
        return self.matches(cmd)
