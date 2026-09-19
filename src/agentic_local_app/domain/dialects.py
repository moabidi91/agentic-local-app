"""The dictionary between shell dialects: bash <-> PowerShell, bounded and auditable (ADR-030 §4).

The model writes one command; the machine runs one shell. When the two do not agree — a POSIX
command line sent to PowerShell, or the reverse — this module rewrites the command **when, and only
when, it can map it exactly**, and says what it did. Three properties hold by construction:

- **never silent.** :meth:`ShellTranslator.translate` returns a :class:`CommandTranslation` carrying
  both forms and the rule identifiers that fired; the caller writes it to the hash-chained audit log
  and the task result carries it back to the model (ADR-030 §4). Nothing is rewritten without
  leaving that trace. The translator is **pure**, which is what lets the result field be derived
  from the stored command instead of stored next to it — no schema, nothing to drift.
- **confident only.** A command the table does not cover is returned **unchanged** with a ``reason``
  naming what stopped the translation, so the model can correct itself. A plausible translation is
  worse than none: the model would reason on a command it never wrote and cannot see.
- **reads only.** Every rule maps a command that *inspects* the machine — list a directory, print a
  file, print the working directory, read an environment variable, locate a program. **Not one rule
  writes, creates, moves or deletes anything**, which is how "refuse rather than guess on anything
  destructive" is enforced: there is nothing destructive to get wrong.

What the dictionary deliberately refuses is as much of the decision as what it accepts: pipelines,
redirections, chaining operators, wildcards, command substitutions, and every program whose pattern
language, exit-code convention or effect differs between the two dialects. ADR-030 lists them and
says why; :data:`REFUSED_PROGRAMS` is the machine-readable form of the same list, printed by
``agentic-app shell rules``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from agentic_local_app.domain.shell import ShellDialect, shell_name

__all__ = [
    "REFUSED_PROGRAMS",
    "TRANSLATION_RULES",
    "CommandTranslation",
    "RefusedProgram",
    "ShellTranslator",
    "TranslationRule",
    "describe_dictionary",
    "rules_for",
    "source_dialect_for",
]

#: The two dialects the dictionary knows how to move between; anything else is left alone.
_TRANSLATABLE = (ShellDialect.POSIX, ShellDialect.POWERSHELL)


def source_dialect_for(target: ShellDialect) -> ShellDialect:
    """The dialect a command must be written in for ``target`` to need a translation."""
    return ShellDialect.POSIX if target is ShellDialect.POWERSHELL else ShellDialect.POWERSHELL


# ------------------------------------------------------------------------------------------------
# Value objects
# ------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class CommandTranslation:
    """What became of one command line. ``reason`` is set **iff** nothing was rewritten."""

    source: ShellDialect
    target: ShellDialect
    original: str
    executed: str
    rules: tuple[str, ...] = ()
    reason: str | None = None

    @property
    def translated(self) -> bool:
        return self.reason is None


@dataclass(frozen=True)
class TranslationRule:
    """One line of the dictionary: what it recognises, what it produces, and how to read it.

    ``source_form`` and ``target_form`` are the documentation of the rule — the shapes printed by
    ``agentic-app shell rules`` and quoted in a refusal when a command names a program the rule
    claims but in a form it does not cover.
    """

    rule_id: str
    source: ShellDialect
    target: ShellDialect
    programs: tuple[str, ...]
    source_form: str
    target_form: str
    apply: Callable[[Sequence[str]], list[str] | None]


@dataclass(frozen=True)
class RefusedProgram:
    """A program the dictionary recognises and deliberately does **not** translate."""

    program: str
    source: ShellDialect
    target: ShellDialect
    reason: str


# ------------------------------------------------------------------------------------------------
# Scanning: what a command line may contain before any rule is even looked up
# ------------------------------------------------------------------------------------------------
#: Characters that mean something the two dialects do not read the same way. Seen outside quotes,
#: any of them ends the translation: the command runs exactly as the model wrote it.
_REFUSED_CHARACTERS: Mapping[str, str] = MappingProxyType(
    {
        "|": "a pipeline",
        "&": "a chaining or background operator",
        ">": "a redirection",
        "<": "a redirection",
        "`": "a backquote",
        "(": "a grouping",
        ")": "a grouping",
        "{": "a block",
        "}": "a block",
        "*": "a wildcard",
        "?": "a wildcard",
        "[": "a wildcard",
        "]": "a wildcard",
        "~": "a home-directory shortcut",
        "\\": "a backslash escape",
        "#": "a comment",
        "!": "an expansion or negation operator",
        "\n": "a line break",
        "\r": "a line break",
    }
)

#: ``$NAME``, ``${NAME}`` (POSIX) and ``$env:NAME`` (PowerShell). Nothing else is a variable here.
_POSIX_VARIABLE = re.compile(r"\$(?:\{(?P<braced>[A-Za-z_]\w*)\}|(?P<plain>[A-Za-z_]\w*))")
_POWERSHELL_VARIABLE = re.compile(r"\$(?i:env):(?P<name>[A-Za-z_]\w*)")

#: Names a POSIX shell or a POSIX system provides. Windows sets none of them, so ``$env:HOME``
#: would read empty where ``$HOME`` read the home directory: the rewrite is not exact, so it is not
#: done. The same reasoning, mirrored, gives :data:`_WINDOWS_OWNED_NAMES`.
_POSIX_OWNED_NAMES = frozenset(
    {
        "HOME",
        "PWD",
        "OLDPWD",
        "SHELL",
        "USER",
        "LOGNAME",
        "HOSTNAME",
        "UID",
        "EUID",
        "PPID",
        "IFS",
        "PS1",
        "PS2",
        "PS4",
        "RANDOM",
        "SECONDS",
        "LINENO",
        "SHLVL",
        "BASH",
        "BASH_VERSION",
        "BASHPID",
        "FUNCNAME",
        "REPLY",
        "OPTARG",
        "OPTIND",
    }
)
_WINDOWS_OWNED_NAMES = frozenset(
    {
        "USERPROFILE",
        "USERNAME",
        "USERDOMAIN",
        "COMPUTERNAME",
        "APPDATA",
        "LOCALAPPDATA",
        "ALLUSERSPROFILE",
        "PROGRAMFILES",
        "PROGRAMDATA",
        "PUBLIC",
        "SYSTEMROOT",
        "SYSTEMDRIVE",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "HOMEDRIVE",
        "HOMEPATH",
        "TEMP",
        "TMP",
        "OS",
        "PROCESSOR_ARCHITECTURE",
    }
)

#: ``${NAME}`` as a whole, so that the braces of a variable are not read as a block.
_BRACED_VARIABLE = re.compile(r"\$\{[A-Za-z_]\w*\}")

#: ``NAME=value ls`` — a POSIX prefix PowerShell has no syntax for.
_ASSIGNMENT = re.compile(r"^[A-Za-z_]\w*=")

#: The identifier of the rule that rewrites variable syntax, reported like any other rule.
_VARIABLE_RULE_ID = "environment-variable"

#: Splits the first word of a command line off, a ``;`` counting as a delimiter like a space.
_FIRST_WORD = re.compile(r"[\s;]+")


@dataclass(frozen=True)
class _Scanned:
    """A command line split into simple commands, or the reason it could not be."""

    segments: tuple[tuple[str, ...], ...] = ()
    variables: bool = False
    refusal: str | None = None


def _variable_name(match: re.Match[str], source: ShellDialect) -> str:
    if source is ShellDialect.POWERSHELL:
        return match.group("name")
    return match.group("braced") or match.group("plain")


def _rewrite_variables(text: str, source: ShellDialect) -> tuple[str, bool, str | None]:
    """Rewrite the variable references of ``text``. Returns ``(text, rewritten, refusal)``.

    Every ``$`` of the token must be one the dictionary reads, otherwise nothing is rewritten: a
    positional parameter, a special parameter or a PowerShell variable that is not an environment
    variable have no counterpart, and half a translation is not one.
    """
    if "$" not in text:
        return text, False, None
    if source is ShellDialect.POSIX:
        pattern, owned, other = _POSIX_VARIABLE, _POSIX_OWNED_NAMES, "PowerShell"
    else:
        pattern, owned, other = _POWERSHELL_VARIABLE, _WINDOWS_OWNED_NAMES, "a POSIX shell"
    unreadable = f"`{text}` carries a `$` this dictionary does not read as an environment variable"
    matches = list(pattern.finditer(text))
    if len(matches) != text.count("$"):
        return text, False, unreadable
    for match in matches:
        name = _variable_name(match, source)
        if source is ShellDialect.POSIX and text[match.end() : match.end() + 1] == ":":
            return text, False, unreadable  # `$env:NAME` read by the POSIX pattern as `$env`
        if name.upper() in owned:
            return (
                text,
                False,
                (
                    f"`{match.group(0)}` names something the shell itself provides, and {other} has "
                    "no variable with the same meaning"
                ),
            )
    rewritten = pattern.sub(
        lambda match: (
            f"$env:{_variable_name(match, source)}"
            if source is ShellDialect.POSIX
            else f"${_variable_name(match, source)}"
        ),
        text,
    )
    return rewritten, rewritten != text, None


def _scan(cmd: str, source: ShellDialect) -> _Scanned:
    """Split ``cmd`` into simple commands on top-level ``;``, quotes respected and kept.

    The scan is the first half of the decision: everything a rule could not reason about — a
    pipeline, a redirection, a wildcard, a substitution, an escape — stops here, before a single
    rule is consulted.
    """
    segments: list[tuple[str, ...]] = []
    tokens: list[str] = []
    token: list[str] = []
    quote: str | None = None
    variables = False

    def end_token() -> str | None:
        nonlocal variables
        if not token:
            return None
        text = "".join(token)
        token.clear()
        rewritten, changed, refusal = _rewrite_variables(text, source)
        if refusal is not None:
            return refusal
        variables = variables or changed
        tokens.append(rewritten)
        return None

    index = 0
    while index < len(cmd):
        char = cmd[index]
        if quote is not None:
            token.append(char)
            if char == quote:
                quote = None
            index += 1
            continue
        if char in "\"'":
            quote = char
            token.append(char)
            index += 1
            continue
        if char == "$" and index + 1 < len(cmd) and cmd[index + 1] == "(":
            return _Scanned(refusal="the command uses a command substitution")
        if char == "$" and index + 1 < len(cmd) and cmd[index + 1] == "{":
            braced = _BRACED_VARIABLE.match(cmd, index)
            if braced is None:
                return _Scanned(refusal="the command uses a block")
            token.append(braced.group(0))  # ``${NAME}`` travels whole, braces and all
            index = braced.end()
            continue
        if char in _REFUSED_CHARACTERS:
            return _Scanned(refusal=f"the command uses {_REFUSED_CHARACTERS[char]}")
        if char == ";":
            refusal = end_token()
            if refusal is not None:
                return _Scanned(refusal=refusal)
            if tokens:
                segments.append(tuple(tokens))
                tokens = []
            index += 1
            continue
        if char.isspace():
            refusal = end_token()
            if refusal is not None:
                return _Scanned(refusal=refusal)
            index += 1
            continue
        token.append(char)
        index += 1
    if quote is not None:
        return _Scanned(refusal="the command leaves a quote open")
    refusal = end_token()
    if refusal is not None:
        return _Scanned(refusal=refusal)
    if tokens:
        segments.append(tuple(tokens))
    if not segments:
        return _Scanned(refusal="the command is empty")
    return _Scanned(segments=tuple(segments), variables=variables)


# ------------------------------------------------------------------------------------------------
# The rules themselves — every one of them reads, none of them writes
# ------------------------------------------------------------------------------------------------
def _is_option(token: str) -> bool:
    return token.startswith("-") and len(token) > 1


def _single_operand(tokens: Sequence[str]) -> str | None:
    """The one operand of a command that takes exactly one and no option."""
    operands = tokens[1:]
    if len(operands) != 1 or _is_option(operands[0]):
        return None
    return operands[0]


def _no_operand(tokens: Sequence[str]) -> bool:
    return len(tokens) == 1


def _posix_list_directory(tokens: Sequence[str]) -> list[str] | None:
    """``ls``, with at most the ``-a`` / ``-A`` / ``-l`` short options and **one** path.

    One path only: ``Get-ChildItem a b`` does not list two directories, it binds ``b`` to another
    parameter. A rule that reads almost right is the kind this dictionary refuses.
    """
    force, paths = False, []
    for token in tokens[1:]:
        if token.startswith("-"):
            letters = set(token[1:])
            if not letters or not letters <= {"a", "A", "l"}:
                return None
            force = force or bool(letters & {"a", "A"})
        else:
            paths.append(token)
    if len(paths) > 1:
        return None
    return ["Get-ChildItem", *(["-Force"] if force else []), *paths]


def _posix_print_file(tokens: Sequence[str]) -> list[str] | None:
    operand = _single_operand(tokens)
    return None if operand is None else ["Get-Content", operand]


_LINE_COUNT = re.compile(r"^-(?:n(?P<attached>\d+)?|(?P<obsolete>\d+))$")


def _lines_and_file(tokens: Sequence[str]) -> tuple[int, str] | None:
    """``head``/``tail`` with ``-n N``, ``-nN`` or ``-N``, and exactly one file. Default 10."""
    count, file, index = 10, None, 1
    while index < len(tokens):
        token = tokens[index]
        if token.startswith("-"):
            match = _LINE_COUNT.match(token)
            if match is None:
                return None
            digits = match.group("attached") or match.group("obsolete")
            if digits is None:
                index += 1
                if index >= len(tokens) or not tokens[index].isdigit():
                    return None
                digits = tokens[index]
            count = int(digits)
        elif file is None:
            file = token
        else:
            return None
        index += 1
    return None if file is None else (count, file)


def _posix_head(tokens: Sequence[str]) -> list[str] | None:
    parsed = _lines_and_file(tokens)
    return None if parsed is None else ["Get-Content", parsed[1], "-TotalCount", str(parsed[0])]


def _posix_tail(tokens: Sequence[str]) -> list[str] | None:
    parsed = _lines_and_file(tokens)
    return None if parsed is None else ["Get-Content", parsed[1], "-Tail", str(parsed[0])]


def _posix_pwd(tokens: Sequence[str]) -> list[str] | None:
    return ["Get-Location"] if _no_operand(tokens) else None


def _posix_echo(tokens: Sequence[str]) -> list[str] | None:
    """One argument only: ``Write-Output a b`` is not ``echo a b``, it is a parameter error."""
    operand = _single_operand(tokens)
    return None if operand is None else ["Write-Output", operand]


def _posix_which(tokens: Sequence[str]) -> list[str] | None:
    operand = _single_operand(tokens)
    return None if operand is None else ["Get-Command", operand]


def _posix_env(tokens: Sequence[str]) -> list[str] | None:
    return ["Get-ChildItem", "Env:"] if _no_operand(tokens) else None


def _powershell_operands(tokens: Sequence[str]) -> list[str] | None:
    """The positional operands of a cmdlet call that carries no parameter at all."""
    operands = list(tokens[1:])
    return None if any(token.startswith("-") for token in operands) else operands


def _powershell_list_directory(tokens: Sequence[str]) -> list[str] | None:
    force, paths = False, []
    for token in tokens[1:]:
        if token.lower() == "-force":
            force = True
        elif token.startswith("-"):
            return None
        else:
            paths.append(token)
    if len(paths) > 1 or (paths and paths[0].lower().rstrip("\\/") == "env:"):
        return None
    return ["ls", *(["-a"] if force else []), *paths]


def _powershell_list_environment(tokens: Sequence[str]) -> list[str] | None:
    operands = _powershell_operands(tokens)
    if operands is None or len(operands) != 1 or operands[0].lower().rstrip("\\/") != "env:":
        return None
    return ["env"]


def _powershell_get_content(tokens: Sequence[str]) -> tuple[str, str | None, int] | None:
    """``(file, mode, count)`` of a ``Get-Content`` call limited to ``-TotalCount`` / ``-Tail``."""
    file, mode, count, index = None, None, 0, 1
    while index < len(tokens):
        token = tokens[index]
        lowered = token.lower()
        if lowered in ("-totalcount", "-head", "-first", "-tail", "-last"):
            if mode is not None or index + 1 >= len(tokens) or not tokens[index + 1].isdigit():
                return None
            mode = "head" if lowered in ("-totalcount", "-head", "-first") else "tail"
            count = int(tokens[index + 1])
            index += 2
            continue
        if token.startswith("-"):
            return None
        if file is not None:
            return None
        file, index = token, index + 1
    return None if file is None else (file, mode, count)


def _powershell_print_file(tokens: Sequence[str]) -> list[str] | None:
    parsed = _powershell_get_content(tokens)
    return None if parsed is None or parsed[1] is not None else ["cat", parsed[0]]


def _powershell_head(tokens: Sequence[str]) -> list[str] | None:
    parsed = _powershell_get_content(tokens)
    if parsed is None or parsed[1] != "head":
        return None
    return ["head", "-n", str(parsed[2]), parsed[0]]


def _powershell_tail(tokens: Sequence[str]) -> list[str] | None:
    parsed = _powershell_get_content(tokens)
    if parsed is None or parsed[1] != "tail":
        return None
    return ["tail", "-n", str(parsed[2]), parsed[0]]


def _powershell_pwd(tokens: Sequence[str]) -> list[str] | None:
    return ["pwd"] if _no_operand(tokens) else None


def _powershell_echo(tokens: Sequence[str]) -> list[str] | None:
    operands = _powershell_operands(tokens)
    return None if operands is None or len(operands) != 1 else ["echo", operands[0]]


def _powershell_which(tokens: Sequence[str]) -> list[str] | None:
    operands = _powershell_operands(tokens)
    return None if operands is None or len(operands) != 1 else ["command", "-v", operands[0]]


_POSIX_TO_POWERSHELL: tuple[TranslationRule, ...] = (
    TranslationRule(
        "list-environment",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("env", "printenv"),
        "env",
        "Get-ChildItem Env:",
        _posix_env,
    ),
    TranslationRule(
        "list-directory",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("ls",),
        "ls [-a|-A|-l] [PATH]",
        "Get-ChildItem [-Force] [PATH]",
        _posix_list_directory,
    ),
    TranslationRule(
        "head-lines",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("head",),
        "head [-n N] FILE",
        "Get-Content FILE -TotalCount N",
        _posix_head,
    ),
    TranslationRule(
        "tail-lines",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("tail",),
        "tail [-n N] FILE",
        "Get-Content FILE -Tail N",
        _posix_tail,
    ),
    TranslationRule(
        "print-file",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("cat",),
        "cat FILE",
        "Get-Content FILE",
        _posix_print_file,
    ),
    TranslationRule(
        "print-working-directory",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("pwd",),
        "pwd",
        "Get-Location",
        _posix_pwd,
    ),
    TranslationRule(
        "print-text",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("echo",),
        "echo ARG",
        "Write-Output ARG",
        _posix_echo,
    ),
    TranslationRule(
        "locate-program",
        ShellDialect.POSIX,
        ShellDialect.POWERSHELL,
        ("which",),
        "which NAME",
        "Get-Command NAME",
        _posix_which,
    ),
)

_POWERSHELL_TO_POSIX: tuple[TranslationRule, ...] = (
    TranslationRule(
        "list-environment",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-childitem", "gci"),
        "Get-ChildItem Env:",
        "env",
        _powershell_list_environment,
    ),
    TranslationRule(
        "list-directory",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-childitem", "gci"),
        "Get-ChildItem [-Force] [PATH]",
        "ls [-a] [PATH]",
        _powershell_list_directory,
    ),
    TranslationRule(
        "head-lines",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-content", "gc"),
        "Get-Content FILE -TotalCount N",
        "head -n N FILE",
        _powershell_head,
    ),
    TranslationRule(
        "tail-lines",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-content", "gc"),
        "Get-Content FILE -Tail N",
        "tail -n N FILE",
        _powershell_tail,
    ),
    TranslationRule(
        "print-file",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-content", "gc"),
        "Get-Content FILE",
        "cat FILE",
        _powershell_print_file,
    ),
    TranslationRule(
        "print-working-directory",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-location", "gl"),
        "Get-Location",
        "pwd",
        _powershell_pwd,
    ),
    TranslationRule(
        "print-text",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("write-output",),
        "Write-Output ARG",
        "echo ARG",
        _powershell_echo,
    ),
    TranslationRule(
        "locate-program",
        ShellDialect.POWERSHELL,
        ShellDialect.POSIX,
        ("get-command",),
        "Get-Command NAME",
        "command -v NAME",
        _powershell_which,
    ),
)

#: The whole dictionary, both directions, in the order the rules are consulted.
TRANSLATION_RULES: tuple[TranslationRule, ...] = _POSIX_TO_POWERSHELL + _POWERSHELL_TO_POSIX

#: Why a recognised program is deliberately left alone. The wording reaches the model as is.
_WRITES = (
    "the dictionary translates only commands that read, never one that creates, moves or deletes"
)
_PATTERN = "its pattern, field or expression language is not the same in the other dialect"
_EXIT_CODE = "it answers by its exit code, and the two dialects do not agree on exit codes"
_NO_EQUIVALENT = "the other dialect has no command that answers exactly the same question"

_POSIX_REFUSALS: Mapping[str, str] = MappingProxyType(
    {
        name: _WRITES
        for name in (
            "rm",
            "rmdir",
            "mv",
            "cp",
            "ln",
            "mkdir",
            "touch",
            "chmod",
            "chown",
            "dd",
            "truncate",
            "tee",
            "install",
            "kill",
            "pkill",
            "killall",
        )
    }
    | {
        name: _PATTERN
        for name in (
            "grep",
            "egrep",
            "fgrep",
            "sed",
            "awk",
            "find",
            "cut",
            "tr",
            "sort",
            "uniq",
            "xargs",
            "wc",
        )
    }
    | {name: _EXIT_CODE for name in ("test",)}
    | {
        name: _NO_EQUIVALENT
        for name in (
            "uname",
            "df",
            "du",
            "ps",
            "stat",
            "date",
            "export",
            "source",
            "basename",
            "dirname",
            "readlink",
        )
    }
)
_POWERSHELL_REFUSALS: Mapping[str, str] = MappingProxyType(
    {
        name: _WRITES
        for name in (
            "remove-item",
            "move-item",
            "copy-item",
            "rename-item",
            "new-item",
            "set-content",
            "add-content",
            "clear-content",
            "out-file",
            "set-itemproperty",
            "stop-process",
        )
    }
    | {
        name: _PATTERN
        for name in (
            "select-string",
            "where-object",
            "foreach-object",
            "sort-object",
            "select-object",
            "measure-object",
            "group-object",
            "compare-object",
        )
    }
    | {name: _EXIT_CODE for name in ("test-path",)}
    | {
        name: _NO_EQUIVALENT
        for name in (
            "get-process",
            "get-service",
            "get-date",
            "start-sleep",
            "get-ciminstance",
            "get-wmiobject",
            "get-computerinfo",
            "get-acl",
            "get-item",
            "get-member",
        )
    }
)

#: The refusals as data, for ``agentic-app shell rules`` and for the ADR's table.
REFUSED_PROGRAMS: tuple[RefusedProgram, ...] = tuple(
    RefusedProgram(program, source, source_dialect_for(source), reason)
    for source, table in (
        (ShellDialect.POSIX, _POSIX_REFUSALS),
        (ShellDialect.POWERSHELL, _POWERSHELL_REFUSALS),
    )
    for program, reason in sorted(table.items())
)

_RULES_BY_SOURCE: Mapping[ShellDialect, tuple[TranslationRule, ...]] = MappingProxyType(
    {ShellDialect.POSIX: _POSIX_TO_POWERSHELL, ShellDialect.POWERSHELL: _POWERSHELL_TO_POSIX}
)
_REFUSALS_BY_SOURCE: Mapping[ShellDialect, Mapping[str, str]] = MappingProxyType(
    {ShellDialect.POSIX: _POSIX_REFUSALS, ShellDialect.POWERSHELL: _POWERSHELL_REFUSALS}
)


def rules_for(source: ShellDialect) -> tuple[TranslationRule, ...]:
    """The rules that read a command written in ``source``; empty for a dialect with no table."""
    return _RULES_BY_SOURCE.get(source, ())


def _known_programs(source: ShellDialect) -> frozenset[str]:
    """Every program name the dictionary recognises as belonging to ``source``, mapped or refused."""
    return frozenset(name for rule in rules_for(source) for name in rule.programs) | frozenset(
        _REFUSALS_BY_SOURCE.get(source, {})
    )


# ------------------------------------------------------------------------------------------------
# The translator
# ------------------------------------------------------------------------------------------------
class ShellTranslator:
    """Rewrites a command for ``target`` when — and only when — the dictionary is sure.

    ``translate`` answers ``None`` when nothing was even attempted: translation is disabled, the
    target dialect has no table (``cmd``, ``unknown``), or the command names no program the
    dictionary recognises as belonging to the other dialect. A dialect-neutral command line
    (``mvn clean install``) therefore costs nothing and says nothing.
    """

    __slots__ = ("_enabled", "_known", "_source", "_target")

    def __init__(self, target: ShellDialect, *, enabled: bool = True) -> None:
        self._target = target
        self._source = source_dialect_for(target)
        self._enabled = enabled and target in _TRANSLATABLE
        self._known = _known_programs(self._source) if self._enabled else frozenset()

    @property
    def target(self) -> ShellDialect:
        return self._target

    @property
    def source(self) -> ShellDialect:
        return self._source

    @property
    def enabled(self) -> bool:
        return self._enabled

    def translate(self, cmd: str | None) -> CommandTranslation | None:
        """The decision for one command line, or ``None`` when nothing was attempted."""
        if not self._enabled or not cmd or not cmd.strip():
            return None
        if not self._recognises(cmd):
            return None
        scanned = _scan(cmd, self._source)
        if scanned.refusal is not None:
            return self._unchanged(cmd, scanned.refusal)
        rewritten: list[str] = []
        fired: list[str] = [_VARIABLE_RULE_ID] if scanned.variables else []
        for segment in scanned.segments:
            outcome = self._translate_segment(segment)
            if isinstance(outcome, str):
                return self._unchanged(cmd, outcome)
            tokens, rule_id = outcome
            rewritten.append(" ".join(tokens))
            if rule_id not in fired:
                fired.append(rule_id)
        return CommandTranslation(
            source=self._source,
            target=self._target,
            original=cmd,
            executed="; ".join(rewritten),
            rules=tuple(fired),
        )

    # ---- pieces --------------------------------------------------------------------------
    def _recognises(self, cmd: str) -> bool:
        """Whether the command line opens with a program of the source dialect.

        Read on the raw text, before the scan: a command the dictionary has no business touching
        must not produce a refusal, and a refusal must not depend on a scan that never ran.
        """
        first = _FIRST_WORD.split(cmd.strip(), maxsplit=1)[0]
        return shell_name(first) in self._known or (
            self._source is ShellDialect.POSIX and bool(_ASSIGNMENT.match(first))
        )

    def _translate_segment(self, segment: Sequence[str]) -> tuple[list[str], str] | str:
        """The translated tokens and the rule that produced them, or the reason there are none."""
        program = shell_name(segment[0])
        if self._source is ShellDialect.POSIX and _ASSIGNMENT.match(segment[0]):
            return f"`{segment[0]}` is a `NAME=value` prefix, and PowerShell has no syntax for one"
        refusal = _REFUSALS_BY_SOURCE.get(self._source, {}).get(program)
        if refusal is not None:
            return f"no rule maps `{program}`: {refusal}"
        claimed: list[TranslationRule] = []
        for rule in rules_for(self._source):
            if program not in rule.programs:
                continue
            claimed.append(rule)
            translated = rule.apply(segment)
            if translated is not None:
                return translated, rule.rule_id
        if claimed:
            forms = " or ".join(f"`{rule.source_form}`" for rule in claimed)
            return f"the rule for `{program}` covers {forms} only, and this command is outside it"
        return f"no rule maps `{program}`"

    def _unchanged(self, cmd: str, reason: str) -> CommandTranslation:
        return CommandTranslation(
            source=self._source, target=self._target, original=cmd, executed=cmd, reason=reason
        )


#: The variable rule is applied token by token during the scan rather than per program, so it has
#: no entry in the tables above; it is described here so that the printed dictionary is complete.
_VARIABLE_FORMS: tuple[tuple[ShellDialect, str, str], ...] = (
    (ShellDialect.POSIX, "$NAME / ${NAME}", "$env:NAME"),
    (ShellDialect.POWERSHELL, "$env:NAME", "$NAME"),
)


def describe_dictionary() -> list[dict[str, str]]:
    """The whole dictionary as plain rows — what ``agentic-app shell rules`` prints."""
    rows = [
        {
            "rule": rule.rule_id,
            "from": rule.source.value,
            "to": rule.target.value,
            "source_form": rule.source_form,
            "target_form": rule.target_form,
        }
        for rule in TRANSLATION_RULES
    ]
    rows += [
        {
            "rule": _VARIABLE_RULE_ID,
            "from": source.value,
            "to": source_dialect_for(source).value,
            "source_form": source_form,
            "target_form": target_form,
        }
        for source, source_form, target_form in _VARIABLE_FORMS
    ]
    return rows
