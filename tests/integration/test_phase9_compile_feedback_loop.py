"""Phase 9 — a compilation that fails, end to end: the diagnostic travels to the model over HTTP and
the model repairs the line the compiler named (ADR-004, ADR-008, ADR-009, ADR-029, ADR-030,
ADR-032).

The question this module answers: when the model runs a compiler and the compilation fails, does
the error go back through the gateway to the model, and can the model readjust from what it
received? Every other test pins one piece (truncation, the verdict rule, the result builder, the
contract examples) or runs the loop on a ``FakeCommandExecutor``; this one closes the loop.

**Everything is production code except the model.** The configuration is the demo one
(``demarrage/config-demo.toml``: ``generic_http`` provider, ``passthrough`` codec, gzip bodies),
loaded by ``load_config`` with the operator's ``AGENTIC__SECTION__KEY`` overrides rooting every path
under ``tmp_path``. ``build_application`` keeps its production defaults — the SQLite store, the
``SubprocessCommandExecutor`` launching the shell detected on this machine, the ``PlanRunner``, the
``PayloadGuard``, the ``ResultCollector``, the ``ProtocolAdapter``, the dialect dictionary, the
``SystemClock`` and ``asyncio.sleep`` — and receives the provider ``TransportRegistry.create`` builds
from that configuration, whose ``httpx`` client talks to an ``httpx.MockTransport``. The compilers
are the real ones, run by the real shell on sources written under ``tmp_path``. The one other
injection is ``SequentialIdGenerator``, so that the identifiers of a trace read ``msg-0003``.

**The model is a reactive double.** :class:`ModelApi` answers the ADR-004 routes of the demo
configuration and hands every protocol message it receives to a double that computes its reply
from that message — nothing is scripted in advance. :class:`CompileRepairer` asks the compiler's
version, compiles and lists the folder, then **parses the diagnostic out of the execution_result it
received** and repairs exactly the line named there before compiling again. It knows what a
developer knows about a toolchain (:class:`Toolchain`: how to ask its version, how to compile, how
the compiler writes a position) and nothing about the project: the planted line lives in
:class:`Project`, which the double never sees, and differs from one language to the next. A repair
of the right line therefore proves the diagnostic reached the model; the test whose diagnostic is
lost on the way shows the converse. "The diagnostic left the application" is literal: it is
asserted in the decompressed body of the POST that carried the ``execution_result``.

**What the compilers do**, pinned per language in :data:`PROJECTS`: ``gcc``, ``javac``, ``go`` and
``rustc`` write their diagnostic on stderr and exit 1; ``tsc`` writes it on **stdout** and exits
**2**, having emitted ``main.js`` anyway. Every one of them is in ``DEFAULT_VERDICT_PROGRAMS`` —
``rustc`` since ADR-032, next to ``cargo`` — so the demo configuration needs no override.

**Controls.** A script that raises is no verdict program: the plan stops (ADR-009), the listing is
skipped, and the traceback still reaches the model, which reruns the script correctly. A program the
shell cannot find is **not** a command that never started — the shell starts, answers ``127`` and
says so on stderr — and it is **no verdict** either, even for a recognised compiler (ADR-032): the
result reads ``execution: "ran"`` with ``reason: "COMMAND_NOT_FOUND"``, the plan stops as ADR-009
says, and the model, which reads ``reason`` and then the shell's words, calls the compiler its
discovery confirmed. Two cases: a misspelled name (``gccc``) and a recognised compiler on a path
that does not exist (``/opt/no-such-jdk/bin/javac``). ``not_started`` is what a shell that cannot
be launched produces, with ``SPAWN_FAILED`` recorded.

**Skips.** A compiler absent from the ``PATH`` skips its case. The whole module is skipped where the
detected shell is not POSIX (Windows PowerShell): the double writes POSIX commands (``sed -i``,
``ls``), and a PowerShell double has not been written.

**Trace.** ``AGENTIC_TRACE_OUT=<file>`` makes the C case write every HTTP message of its session to
``<file>`` (Markdown, one French header per message, indented JSON). Without it nothing is written.
"""

from __future__ import annotations

import copy
import gzip
import json
import os
import re
import shutil
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

from agentic_local_app.config import DEFAULT_VERDICT_PROGRAMS, load_config
from agentic_local_app.domain.clock import SystemClock
from agentic_local_app.domain.commands import VerdictPrograms
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    FailureRecord,
    PlanRecord,
    RetryDecisionRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.shell import ShellDialect, detect_shell
from agentic_local_app.domain.states import PlanState, SessionState, TaskState
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import build_application
from agentic_local_app.transport.providers.generic_http import GenericHttpProvider
from agentic_local_app.transport.registry import TransportRegistry

IS_POSIX_SHELL = detect_shell("", windows=sys.platform == "win32").dialect is ShellDialect.POSIX

pytestmark = [
    pytest.mark.phase9,
    pytest.mark.real_subprocess,
    pytest.mark.skipif(
        not IS_POSIX_SHELL,
        reason="the model double writes POSIX commands (sed -i, ls); no PowerShell double exists",
    ),
]

REPO_ROOT = Path(__file__).resolve().parents[2]
DEMO_CONFIG = REPO_ROOT / "demarrage" / "config-demo.toml"
#: The routes of ``[transport]`` in the demo configuration, as ``generic_http`` fills them.
API_ROOT = "/v1/conversations"
#: The remote conversation the model's API hands out.
REMOTE = "remote-0001"
#: Real-time bound of one session: a cold toolchain (Go's first build) takes a few seconds.
LOOP_BOUND_MS = 90_000
#: Set to a file path, it makes the C case write its HTTP trace there.
TRACE_ENV = "AGENTIC_TRACE_OUT"
#: The failure machinery (ADR-008): none of it may move for a compilation that answered.
FAILURE_EVENTS = frozenset(
    {EventType.FAILURE_RECORDED, EventType.RETRY_SCHEDULED, EventType.BREAKER_STATE_CHANGED}
)


# ================================================================================================
# What the model knows, what the test plants
# ================================================================================================
@dataclass(frozen=True)
class Toolchain:
    """What the model knows about a language — what any developer brings, nothing about the
    project. There is deliberately no line number here: the model learns it from the compiler."""

    key: str
    compiler: str
    version_cmd: str
    #: The file the user names in the request.
    source: str
    compile_cmd: str
    #: How the compiler writes the position of an error, with the named groups ``file``, ``line``
    #: and ``message``. It keys on the position, never on the word "error": locale-proof.
    location: re.Pattern[str]


@dataclass(frozen=True)
class Diagnostic:
    """A position read from a compiler's output, and the words the model quotes."""

    file: str
    line: int
    message: str
    #: The diagnostic exactly as it stands in the output (two lines for ``rustc``).
    text: str

    @property
    def quote(self) -> str:
        return " ".join(self.text.split())


def read_diagnostic(toolchain: Toolchain, output: str) -> Diagnostic | None:
    """The first position the compiler reported in ``output``, if any."""
    match = toolchain.location.search(output)
    if match is None:
        return None
    return Diagnostic(match["file"], int(match["line"]), match["message"].strip(), match.group(0))


@dataclass(frozen=True)
class Project:
    """What is on disk and how the compiler was observed to treat it — never shown to the model."""

    toolchain: Toolchain
    files: Mapping[str, str]
    #: The planted error, verbatim: an identifier nothing declares, in a statement of its own.
    bad_line: str
    #: What a successful compilation leaves on disk.
    artifact: str
    #: Where this compiler writes its diagnostic, and its exit code on the planted error.
    diagnostic_stream: str
    failing_exit_code: int
    #: ``tsc`` emits JavaScript even when it reports an error (``noEmitOnError`` is off by default).
    emits_despite_errors: bool = False
    #: Programs that must be on the ``PATH`` for the case to run.
    requires: tuple[str, ...] = ()

    @property
    def planted_line(self) -> int:
        return self.files[self.toolchain.source].splitlines().index(self.bad_line) + 1


C = Toolchain(
    key="c",
    compiler="gcc",
    version_cmd="gcc --version",
    source="main.c",
    compile_cmd="gcc -c main.c -o main.o",
    location=re.compile(r"^(?P<file>main\.c):(?P<line>\d+):\d+: (?P<message>.+)$", re.M),
)
JAVA = Toolchain(
    key="java",
    compiler="javac",
    version_cmd="javac -version",
    source="Main.java",
    compile_cmd="javac Main.java",
    location=re.compile(r"^(?P<file>Main\.java):(?P<line>\d+): (?P<message>.+)$", re.M),
)
GO = Toolchain(
    key="go",
    compiler="go",
    version_cmd="go version",
    source="main.go",
    compile_cmd="go build -o hello main.go",
    location=re.compile(r"^(?P<file>(?:\./)?main\.go):(?P<line>\d+):\d+: (?P<message>.+)$", re.M),
)
TYPESCRIPT = Toolchain(
    key="typescript",
    compiler="tsc",
    version_cmd="tsc --version",
    source="main.ts",
    compile_cmd="tsc -p tsconfig.json",
    location=re.compile(r"^(?P<file>main\.ts)\((?P<line>\d+),\d+\): (?P<message>.+)$", re.M),
)
RUST = Toolchain(
    key="rust",
    compiler="rustc",
    version_cmd="rustc --version",
    source="main.rs",
    compile_cmd="rustc --emit=obj -o main.o main.rs",
    # rustc puts the position on the line after the message: `  --> main.rs:17:20`
    location=re.compile(r"^(?P<message>\S.*)\n\s*--> (?P<file>main\.rs):(?P<line>\d+):\d+$", re.M),
)

C_SOURCE = r"""/*
 * banner.c - prints what the build produced.
 */
#include <stdio.h>

static const char *banner(void) {
    return "build ok";
}

static int answer(void) {
    return 40 + 2;
}

int main(void) {
    printf("%s\n", banner());
    printf("%d\n", missing_symbol);
    printf("%d\n", answer());
    return 0;
}
"""
JAVA_SOURCE = """/**
 * Prints what the build produced.
 */
public class Main {
    private static final String BANNER = "build ok";

    static String banner() {
        return BANNER;
    }

    static int answer() {
        return 40 + 2;
    }

    public static void main(String[] args) {
        System.out.println(banner());
        System.out.println(answer());
        // the value the old build step used to compute
        System.out.println(missingSymbol);
    }
}
"""
GO_SOURCE = """// Command main prints what the build produced.
package main

func banner() string {
\treturn "build ok"
}

func answer() int {
\treturn 40 + 2
}

func main() {
\tprintln(missingSymbol)
\tprintln(banner())
\tprintln(answer())
}
"""
TYPESCRIPT_SOURCE = """// Prints what the build produced.
function banner(): string {
  return "build ok";
}

function answer(): number {
  return 40 + 2;
}

console.log(banner());
console.log(missingSymbol);
console.log(answer());
"""
#: A project file of its own: ``tsc -p`` never looks for a ``tsconfig.json`` above the folder.
TSCONFIG = """{
  "compilerOptions": {
    "strict": true
  },
  "files": ["main.ts"]
}
"""
RUST_SOURCE = """//! Prints what the build produced.

const BANNER: &str = "build ok";

fn banner() -> &'static str {
    BANNER
}

fn answer() -> i32 {
    40 + 2
}

fn main() {
    println!("{}", banner());
    println!("{}", answer());
    // the value the old build step used to compute
    println!("{}", missing_symbol);
}
"""

PROJECTS: dict[str, Project] = {
    "c": Project(
        C,
        files={"main.c": C_SOURCE},
        bad_line=r'    printf("%d\n", missing_symbol);',
        artifact="main.o",
        diagnostic_stream="stderr",
        failing_exit_code=1,
        requires=("gcc",),
    ),
    "java": Project(
        JAVA,
        files={"Main.java": JAVA_SOURCE},
        bad_line="        System.out.println(missingSymbol);",
        artifact="Main.class",
        diagnostic_stream="stderr",
        failing_exit_code=1,
        requires=("javac",),
    ),
    "go": Project(
        GO,
        files={"main.go": GO_SOURCE},
        bad_line="\tprintln(missingSymbol)",
        artifact="hello",
        diagnostic_stream="stderr",
        failing_exit_code=1,
        requires=("go",),
    ),
    "typescript": Project(
        TYPESCRIPT,
        files={"main.ts": TYPESCRIPT_SOURCE, "tsconfig.json": TSCONFIG},
        bad_line="console.log(missingSymbol);",
        artifact="main.js",
        diagnostic_stream="stdout",
        failing_exit_code=2,
        emits_despite_errors=True,
        requires=("tsc", "node"),
    ),
    "rust": Project(
        RUST,
        files={"main.rs": RUST_SOURCE},
        bad_line='    println!("{}", missing_symbol);',
        artifact="main.o",
        diagnostic_stream="stderr",
        failing_exit_code=1,
        requires=("rustc",),
    ),
}


# ================================================================================================
# The model: an ADR-004 endpoint in front of a reactive double
# ================================================================================================
def task_result(content: Mapping[str, Any], task_id: str) -> dict[str, Any] | None:
    return next((r for r in content.get("results", []) if r["task_id"] == task_id), None)


def output_of(result: Mapping[str, Any]) -> str:
    """Both streams, the way a model reads a result: the evidence may be on either."""
    return f"{result.get('stdout', '')}\n{result.get('stderr', '')}"


def first_line(result: Mapping[str, Any]) -> str:
    return next((line.strip() for line in output_of(result).splitlines() if line.strip()), "")


class ModelDouble:
    """The model's side of the conversation: it reads each message and computes its reply.

    It keeps what it received (a model has its conversation in context), numbers its own messages,
    and writes POSIX commands because the instructions announced a POSIX shell (ADR-030 §3) — any
    other announcement and it declines at once.
    """

    def __init__(self) -> None:
        self.instructions = ""
        self.received: list[dict[str, Any]] = []
        self.sent: list[dict[str, Any]] = []

    def open(self, instructions: str) -> None:
        self.instructions = instructions

    def reply(self, message: dict[str, Any]) -> dict[str, Any]:
        self.received.append(message)
        dialect = re.search(r"\| Shell dialect \| \*\*(\w+)\*\* \|", self.instructions)
        if dialect is None or dialect[1] != ShellDialect.POSIX.value:
            answer = self.final("failed", "This model only writes POSIX shell commands.", [])
        else:
            answer = self.decide(message)
        self.sent.append(answer)
        return answer

    def decide(self, message: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    # ---- writing messages ------------------------------------------------------------------
    def envelope(self, kind: str, content: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": kind,
            "conversation_id": REMOTE,
            "message_id": f"model-{len(self.sent) + 1:04d}",
            "content": content,
        }

    def plan(
        self, kind: str, plan_id: str, objective: str, *commands: tuple[str, str]
    ) -> dict[str, Any]:
        """A sequential plan of plain commands: no ``critical``, no ``continue_on_error``, no stop
        flag — whatever keeps a plan going after a failure is the application's rule, not ours."""
        tasks = [{"task_id": task_id, "type": "cmd", "cmd": cmd} for task_id, cmd in commands]
        content = {
            "plan_id": plan_id,
            "objective": objective,
            "execution_policy": "sequential",
            "tasks": tasks,
        }
        return self.envelope(kind, content)

    def final(
        self, status: str, diagnosis: str, evidence: list[str], next_step: str | None = None
    ) -> dict[str, Any]:
        content: dict[str, Any] = {"status": status, "diagnosis": diagnosis, "evidence": evidence}
        if next_step is not None:
            content["recommended_next_step"] = next_step
        return self.envelope("final_answer", content)


class CompileRepairer(ModelDouble):
    """Version, then compile and list, then repair the line the compiler named, then conclude."""

    DISCOVER, COMPILE, REPAIR = "plan-discover", "plan-compile", "plan-repair"

    def __init__(self, toolchain: Toolchain) -> None:
        super().__init__()
        self.toolchain = toolchain
        self.version = ""
        self.diagnostic: Diagnostic | None = None

    def decide(self, message: dict[str, Any]) -> dict[str, Any]:
        tc = self.toolchain
        if message["type"] == "user_request":
            objective = f"Check that {tc.compiler} is installed"
            return self.plan("discovery_plan", self.DISCOVER, objective, ("t1", tc.version_cmd))
        content = message["content"]
        if content["plan_id"] == self.DISCOVER:
            return self.after_discovery(task_result(content, "t1"))
        if content["plan_id"] == self.COMPILE:
            return self.after_compile(task_result(content, "t2"))
        return self.after_repair(task_result(content, "t5"))

    def after_discovery(self, version: dict[str, Any] | None) -> dict[str, Any]:
        tc = self.toolchain
        if version is None or version["execution"] == "not_started":
            reason = version.get("reason") if version is not None else None
            return self.final(
                "failed",
                f"`{tc.version_cmd}` never started ({reason}): no command can run on this "
                "machine, so nothing can be compiled.",
                [f"t1: execution not_started, reason {reason}, no output"],
                "Check the shell and the working directory this application is configured with.",
            )
        if version.get("exit_code") != 0:
            return self.final("failed", f"{tc.compiler} is not usable here.", [first_line(version)])
        self.version = first_line(version)
        return self.plan(
            "execution_plan",
            self.COMPILE,
            f"Compile {tc.source} with {self.version or tc.compiler}, then look at the folder",
            ("t2", tc.compile_cmd),
            ("t3", "ls"),
        )

    def after_compile(self, compiled: dict[str, Any] | None) -> dict[str, Any]:
        tc = self.toolchain
        if compiled is None:
            return self.final("failed", "The compilation came back without a result.", [])
        if compiled.get("exit_code") == 0:
            return self.final("completed", f"{tc.source} compiles as it is.", ["t2: exit code 0"])
        found = read_diagnostic(tc, output_of(compiled))
        if found is None:
            return self.final(
                "failed",
                f"`{tc.compile_cmd}` failed but its output names no position: there is nothing "
                "to repair from.",
                [f"t2: exit code {compiled.get('exit_code')}, no diagnostic in stdout or stderr"],
            )
        self.diagnostic = found
        return self.plan(
            "execution_plan",
            self.REPAIR,
            f"{tc.compiler} rejects {found.file} line {found.line}: comment it out, compile again",
            ("t4", f"sed -i.orig '{found.line}s|^|// |' {found.file}"),
            ("t5", tc.compile_cmd),
        )

    def after_repair(self, rebuilt: dict[str, Any] | None) -> dict[str, Any]:
        tc, found = self.toolchain, self.diagnostic
        if found is None or rebuilt is None or rebuilt.get("exit_code") != 0:
            return self.final("failed", f"{tc.source} still does not compile.", [])
        return self.final(
            "completed",
            f"{tc.compiler} rejected {found.file} line {found.line} ({found.message}); with that "
            f"line commented out, {tc.source} compiles.",
            [f"t2: {found.quote}", f"t5: `{tc.compile_cmd}` exited 0 after the repair"],
            f"Restore what line {found.line} was meant to print ({found.file}.orig keeps it).",
        )


class ScriptFixer(ModelDouble):
    """Runs a configuration script, reads its traceback, runs it again with a mode it lists."""

    def __init__(self) -> None:
        super().__init__()
        self.error = ""

    def decide(self, message: dict[str, Any]) -> dict[str, Any]:
        if message["type"] == "user_request":
            return self.plan(
                "discovery_plan", "plan-discover", "Check Python", ("t1", "python3 --version")
            )
        content = message["content"]
        if content["plan_id"] == "plan-discover":
            return self.plan(
                "execution_plan",
                "plan-configure",
                "Configure the build as asked, then look at the folder",
                ("t2", "python3 configure.py fast"),
                ("t3", "ls"),
            )
        if content["plan_id"] == "plan-configure":
            failed = task_result(content, "t2") or {}
            raised = re.findall(r"^\w+(?:Error|Exception): .+$", failed.get("stderr", ""), re.M)
            known = re.search(r"known modes: (?P<modes>.+)$", raised[-1]) if raised else None
            if known is None:
                return self.final("failed", "The script failed without saying why.", [])
            self.error = raised[-1]
            mode = known["modes"].split(",")[0].strip()
            return self.plan(
                "execution_plan",
                "plan-reconfigure",
                f"The script knows {known['modes']}: configure with {mode}",
                ("t4", f"python3 configure.py {mode}"),
            )
        rerun = task_result(content, "t4") or {}
        if rerun.get("exit_code") != 0:
            return self.final("failed", "The script still fails.", [])
        return self.final(
            "completed",
            "`fast` is no build mode of configure.py; the project is configured in the mode the "
            "script's own error listed first.",
            [f"t2: {self.error}", f"t4: {first_line(rerun)}"],
        )


#: How POSIX shells say a program does not exist: bash and dash for a name (``gccc: command not
#: found``, ``gccc: not found``) or a path (``/opt/jdk/bin/javac: No such file or directory``,
#: ``/opt/jdk/bin/javac: not found``), then zsh (``command not found: gccc``, ``no such file or
#: directory: /opt/jdk/bin/javac``).
NOT_FOUND = (
    re.compile(r"(?P<program>[^\s:]+): (?:command )?not found"),
    re.compile(r"(?P<program>[^\s:]+): No such file or directory"),
    re.compile(r"(?:command not found|no such file or directory): (?P<program>\S+)"),
)
#: What the model double reads first, as the contract tells it to (ADR-032).
COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"


class MissingProgramFixer(ModelDouble):
    """Discovers a compiler, then calls it by a name or a path the shell cannot find; reads what
    came back — ``reason`` first, as the contract says, then the shell's words, which name the
    program — and compiles again with the compiler the discovery confirmed."""

    def __init__(
        self, compiler: str, version_cmd: str, source: str, wrong_cmd: str, *, diagnosis: str
    ) -> None:
        super().__init__()
        self.compiler = compiler
        self.version_cmd = version_cmd
        self.source = source
        self.wrong_cmd = wrong_cmd
        self.diagnosis = diagnosis
        self.confirmed = ""
        self.evidence = ""

    @property
    def wrong(self) -> str:
        """The program the command names — a misspelt name or a path."""
        return self.wrong_cmd.split()[0]

    def decide(self, message: dict[str, Any]) -> dict[str, Any]:
        if message["type"] == "user_request":
            objective = f"Check that {self.compiler} is installed"
            return self.plan("discovery_plan", "plan-discover", objective, ("t1", self.version_cmd))
        content = message["content"]
        if content["plan_id"] == "plan-discover":
            version = task_result(content, "t1") or {}
            if version.get("exit_code") != 0:
                return self.final("failed", f"{self.compiler} is not usable here.", [])
            self.confirmed = self.compiler  # what the discovery just confirmed
            return self.plan(
                "execution_plan",
                "plan-compile",
                f"Compile {self.source}, then look at the folder",
                ("t2", self.wrong_cmd),
                ("t3", "ls"),
            )
        if content["plan_id"] == "plan-compile":
            typed = task_result(content, "t2") or {}
            said = output_of(typed)
            missing = next((m for pattern in NOT_FOUND if (m := pattern.search(said))), None)
            if (
                typed.get("reason") != COMMAND_NOT_FOUND
                or missing is None
                or missing["program"] != self.wrong
            ):
                return self.final("failed", "The compiler failed for a reason I cannot read.", [])
            self.evidence = missing.group(0)
            corrected = " ".join([self.confirmed, *self.wrong_cmd.split()[1:]])
            return self.plan(
                "execution_plan",
                "plan-retype",
                f"`{self.wrong}` does not exist here, `{self.confirmed}` does: compile again",
                ("t4", corrected),
            )
        retyped = task_result(content, "t4") or {}
        status = "completed" if retyped.get("exit_code") == 0 else "failed"
        return self.final(
            status,
            self.diagnosis,
            [f"t2: {self.evidence}", f"t4: exit code {retyped.get('exit_code')}"],
        )


@dataclass(frozen=True)
class Exchange:
    """One HTTP round trip, as it crossed the boundary."""

    method: str
    #: Path and query, e.g. ``/v1/conversations/remote-0001/messages?after=model-0001``.
    target: str
    #: ``Content-Encoding`` of the request body (``gzip`` under the demo configuration).
    encoding: str | None
    #: The request body once decompressed: the bytes of the canonical JSON the provider encoded.
    body: bytes
    status: int
    reply: Any

    @property
    def request(self) -> Any:
        return json.loads(self.body) if self.body else None

    @property
    def is_message_post(self) -> bool:
        return self.method == "POST" and self.target.endswith("/messages")


Tamper = Callable[[dict[str, Any]], dict[str, Any]]


class ModelApi:
    """The model's HTTP endpoint (ADR-004, the routes of ``demarrage/config-demo.toml``) in front of
    a double. A posted protocol message is handed to the double at once and its reply queued, so the
    GET that follows finds it; every round trip is recorded as it crossed the boundary. ``tamper``
    alters what the double sees — never what is recorded."""

    def __init__(self, model: ModelDouble, *, tamper: Tamper | None = None) -> None:
        self.model = model
        self.tamper = tamper
        self.exchanges: list[Exchange] = []
        self.outbox: list[dict[str, Any]] = []

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        encoding = request.headers.get("content-encoding")
        body = gzip.decompress(request.content) if encoding == "gzip" else request.content
        payload = json.loads(body) if body else None
        after = request.url.params.get("after")
        status, reply = self.answer(request.method, request.url.path, after, payload)
        target = request.url.raw_path.decode("ascii")
        self.exchanges.append(Exchange(request.method, target, encoding, body, status, reply))
        return httpx.Response(status, json=reply)

    def answer(
        self, method: str, path: str, after: str | None, payload: Any
    ) -> tuple[int, dict[str, Any]]:
        messages = f"{API_ROOT}/{REMOTE}/messages"
        if method == "POST" and path == API_ROOT:
            self.model.open(payload["instructions"])
            return 201, {"conversation_id": REMOTE}
        if method == "POST" and path == messages:
            seen = self.tamper(copy.deepcopy(payload)) if self.tamper is not None else payload
            self.outbox.append(self.model.reply(seen))
            return 202, {"accepted": True, "message_id": payload["message_id"]}
        if method == "GET" and path == messages:
            ids = [message["message_id"] for message in self.outbox]
            pending = self.outbox[ids.index(after) + 1 :] if after in ids else list(self.outbox)
            cursor = pending[-1]["message_id"] if pending else (after or None)
            return 200, {"messages": pending, "cursor": cursor}
        if method == "POST" and path == f"{API_ROOT}/{REMOTE}/close":
            return 200, {"closed": True, "conversation_id": REMOTE}
        return 404, {"error": "not_found", "path": path}


def drop_outputs(message: dict[str, Any]) -> dict[str, Any]:
    """A transport that loses the output of every task on its way to the model."""
    for result in message.get("content", {}).get("results", []):
        result["stdout"] = result["stderr"] = ""
    return message


# ================================================================================================
# The rig: the demo configuration, the production wiring, one session
# ================================================================================================
@dataclass
class LoopOutcome:
    """Everything a test reads once the session is over (the store closes with the application)."""

    session: SessionRecord
    exchanges: list[Exchange]
    tasks: dict[str, TaskRecord]
    plans: dict[str, PlanRecord]
    failures: list[FailureRecord]
    retry_decisions: list[RetryDecisionRecord]
    events: list[Event]

    @property
    def posted(self) -> list[dict[str, Any]]:
        """The protocol messages the application POSTed, in order."""
        return [exchange.request for exchange in self.exchanges if exchange.is_message_post]

    @property
    def replies(self) -> list[dict[str, Any]]:
        """The model's messages, in the order the GETs handed them over."""
        return [
            message
            for exchange in self.exchanges
            if exchange.method == "GET"
            for message in exchange.reply["messages"]
        ]

    def carrying(self, plan_id: str) -> Exchange:
        """The POST that carried the ``execution_result`` of ``plan_id``."""
        return next(
            exchange
            for exchange in self.exchanges
            if exchange.is_message_post
            and exchange.request["type"] == "execution_result"
            and exchange.request["content"]["plan_id"] == plan_id
        )

    def result(self, plan_id: str) -> dict[str, Any]:
        content: dict[str, Any] = self.carrying(plan_id).request["content"]
        return content

    def plan(self, plan_id: str) -> dict[str, Any]:
        """The model's message that sent ``plan_id``."""
        return next(m for m in self.replies if m["content"].get("plan_id") == plan_id)

    def failure_events(self) -> list[Event]:
        return [event for event in self.events if event.event_type in FAILURE_EVENTS]


async def run_loop(
    tmp_path: Path,
    files: Mapping[str, str],
    model: ModelDouble,
    *,
    goal: str,
    user_message: str,
    environ: Mapping[str, str] | None = None,
    tamper: Tamper | None = None,
) -> tuple[LoopOutcome, Path, ModelApi]:
    """Write the project, wire the application on the demo configuration, run one session."""
    project = tmp_path / "project"
    project.mkdir()
    for name, text in files.items():
        (project / name).write_text(text, encoding="utf-8")
    config = load_config(
        DEMO_CONFIG,
        environ={
            # what an operator overrides, with the variables config.py documents
            "AGENTIC__APP__DATA_DIR": str(tmp_path / "data"),
            "AGENTIC__EXECUTION__CWD": str(project),
            "AGENTIC__SCRATCH__ROOT": str(tmp_path / "scratch"),
            "AGENTIC__SCRATCH__ARCHIVE_ROOT": str(tmp_path / "scratch-archive"),
            **(environ or {}),
        },
        load_env_file=False,
    )
    api = ModelApi(model, tamper=tamper)
    clock = SystemClock()
    provider = TransportRegistry.create(config, clock=clock, transport=httpx.MockTransport(api))
    assert isinstance(provider, GenericHttpProvider)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="compile-loop-recorder")
    app = build_application(
        config, transport=provider, clock=clock, ids=SequentialIdGenerator(), bus=bus
    )
    assert app.transport is provider  # the passthrough codec leaves the provider bare (ADR-021)
    try:
        started = await app.manager.start_session(goal=goal, user_message=user_message)
        session = await app.manager.wait(started.session_id, timeout_ms=LOOP_BOUND_MS)
        sid = session.session_id
        outcome = LoopOutcome(
            session=session,
            exchanges=list(api.exchanges),
            tasks={task.task_id: task for task in app.store.list_tasks(sid)},
            plans={plan.plan_id: plan for plan in app.store.list_plans(sid)},
            failures=app.store.list_failures(sid),
            retry_decisions=app.store.list_retry_decisions(sid),
            events=list(recorder.events),
        )
    finally:
        await app.aclose()
    return outcome, project, api


def json_fragment(text: str) -> bytes:
    """``text`` as it stands inside a string of the canonical JSON (ADR-017: UTF-8 kept)."""
    return json.dumps(text, ensure_ascii=False)[1:-1].encode("utf-8")


# ================================================================================================
# The trace (written only when AGENTIC_TRACE_OUT names a file)
# ================================================================================================
def describe(message: Mapping[str, Any]) -> str:
    """What a protocol message is, in one French line."""
    kind, content = message["type"], message["content"]
    if kind == "user_request":
        return "`user_request` — la demande de l'utilisateur et le budget de la session"
    if kind in ("discovery_plan", "execution_plan"):
        commands = " puis ".join(f"`{task['cmd']}`" for task in content["tasks"])
        return f"`{kind}` `{content['plan_id']}` — le modèle demande {commands}"
    if kind == "execution_result":
        text = f"`execution_result` du plan `{content['plan_id']}` — statut `{content['status']}`"
        for result in content["results"]:
            if result.get("failure_is_verdict"):
                text += (
                    f" ; `{result['task_id']}` en échec, `exit_code` {result['exit_code']}, "
                    "`failure_is_verdict` : **le diagnostic du compilateur part vers le modèle**"
                )
        return text
    if kind == "final_answer":
        return f"`final_answer` — statut `{content['status']}` : la réponse finale du modèle"
    return f"`{kind}`"


def trace_entries(exchange: Exchange) -> list[tuple[str, Any]]:
    """The request then the reply of one round trip: a French header and the JSON (or ``None``)."""
    where = f"`{exchange.method} {exchange.target}`"
    request, reply, status = exchange.request, exchange.reply, exchange.status
    if exchange.method == "POST" and exchange.target == API_ROOT:
        shown = dict(request)
        shown["instructions"] = (
            f"[{len(request['instructions'])} caractères : PROTOCOL_INSTRUCTIONS.md rendu pour "
            "cette machine, abrégé dans cette trace]"
        )
        return [
            (
                f"Application → modèle · {where} · ouverture de la conversation : l'utilisateur, "
                "les instructions du protocole (abrégées ici), les métadonnées",
                shown,
            ),
            (
                f"Modèle → application · `{status}` · l'identifiant de la conversation distante",
                reply,
            ),
        ]
    if exchange.is_message_post:
        return [
            (f"Application → modèle · {where} · {describe(request)}", request),
            (
                f"Modèle → application · `{status}` · accusé de réception de "
                f"`{request['message_id']}`",
                reply,
            ),
        ]
    if exchange.method == "GET":
        messages = reply.get("messages", [])
        what = describe(messages[0]) if len(messages) == 1 else f"{len(messages)} messages"
        return [
            (f"Application → modèle · {where} · relève de la réponse du modèle", None),
            (f"Modèle → application · `{status}` · {what}", reply),
        ]
    return [
        (f"Application → modèle · {where}", request),
        (f"Modèle → application · `{status}`", reply),
    ]


def write_trace(
    path: Path, outcome: LoopOutcome, case: Project, api: ModelApi, project: Path
) -> None:
    """Every message of the session that crossed the HTTP boundary, in order, readable by a human."""
    tc = case.toolchain
    entries = [entry for exchange in outcome.exchanges for entry in trace_entries(exchange)]

    def number_of(predicate: Callable[[Any], bool]) -> int:
        return next(i for i, (_, payload) in enumerate(entries, start=1) if predicate(payload))

    def plan_message(plan_id: str) -> Callable[[Any], bool]:
        return lambda payload: (
            isinstance(payload, dict)
            and any(
                message["content"].get("plan_id") == plan_id
                for message in payload.get("messages", [])
            )
        )

    diagnostic_at = number_of(
        lambda payload: (
            isinstance(payload, dict)
            and payload.get("type") == "execution_result"
            and payload["content"]["plan_id"] == CompileRepairer.COMPILE
        )
    )
    repair_at = number_of(plan_message(CompileRepairer.REPAIR))
    final_at = number_of(
        lambda payload: (
            isinstance(payload, dict)
            and any(m["type"] == "final_answer" for m in payload.get("messages", []))
        )
    )
    announced = re.findall(
        r"^\| (Operating system|Shell|Shell dialect|Working directory) \| (.+) \|$",
        api.model.instructions,
        re.M,
    )
    repaired = (project / tc.source).read_text(encoding="utf-8").splitlines()[case.planted_line - 1]
    version = first_line(outcome.result(CompileRepairer.DISCOVER)["results"][0])
    lines = [
        f"# Boucle de compilation ({tc.compiler}) — la trace HTTP d'une session",
        "",
        "Écrite par `tests/integration/test_phase9_compile_feedback_loop.py` (cas "
        f"`{tc.key}`) parce que `{TRACE_ENV}` était défini. Chaque bloc est un message tel qu'il a "
        "traversé la frontière HTTP entre l'application (fournisseur `generic_http` de "
        "`demarrage/config-demo.toml`) et le modèle (un double réactif derrière "
        "`httpx.MockTransport`) : le corps JSON, décompressé (gzip) et indenté, sous un en-tête "
        "qui dit dans quel sens il est passé et ce qu'il est.",
        "",
        f"- Session `{outcome.session.session_id}`, terminée `{outcome.session.status.value}` ; "
        f"compilateur `{version}`, commande `{tc.compile_cmd}`.",
        f"- Erreur plantée dans `{tc.source}`, ligne {case.planted_line} : `{case.bad_line.strip()}`. "
        "Le modèle ne lit jamais le fichier : il ne connaît cette ligne que par le diagnostic.",
        f"- Le diagnostic quitte l'application au message {diagnostic_at} ; le modèle en tire la "
        f"réparation au message {repair_at} ; sa réponse finale, qui le cite, est au message "
        f"{final_at}.",
        "- Instructions abrégées ; leur section 6 annonce : "
        + " · ".join(f"{name} {value}" for name, value in announced)
        + ".",
        f"- `FailureRecord` : {len(outcome.failures)} ; décisions de reprise : "
        f"{len(outcome.retry_decisions)} — l'échec du compilateur est une réponse, pas une panne.",
        "",
    ]
    for number, (header, payload) in enumerate(entries, start=1):
        lines += [f"### {number}. {header}", ""]
        if payload is None:
            lines += ["*(requête sans corps)*", ""]
        else:
            lines += ["```json", json.dumps(payload, indent=2, ensure_ascii=False), "```", ""]
    lines += [
        "## Sur le disque après la session",
        "",
        f"- `{tc.source}`, ligne {case.planted_line} : `{repaired}`",
        f"- `{tc.source}.orig` : l'original, gardé par `sed -i.orig`",
        f"- `{case.artifact}` : {(project / case.artifact).stat().st_size} octets",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


# ================================================================================================
# 1. Every compiler: the diagnostic reaches the model, which repairs the line it names
# ================================================================================================
def _compile_cases() -> list[Any]:
    return [
        pytest.param(
            case,
            id=key,
            marks=pytest.mark.skipif(
                any(shutil.which(program) is None for program in case.requires),
                reason=f"{' / '.join(case.requires)} not on the PATH",
            ),
        )
        for key, case in PROJECTS.items()
    ]


@pytest.mark.timeout(120)
@pytest.mark.parametrize("case", _compile_cases())
async def given_planted_compile_error_when_model_compiles_then_diagnostic_reaches_it_and_it_repairs_that_line(
    case: Project, tmp_path: Path
) -> None:
    tc, line = case.toolchain, case.planted_line
    # ADR-032: every compiler of this module is recognised by the default list, rustc included
    assert VerdictPrograms(DEFAULT_VERDICT_PROGRAMS).matches(tc.compile_cmd)
    model = CompileRepairer(tc)
    outcome, project, api = await run_loop(
        tmp_path,
        case.files,
        model,
        goal=f"Make {tc.source} compile",
        user_message=f"`{tc.compile_cmd}` fails on my project. Can you fix {tc.source}?",
    )

    # ---- the loop went round four times, over HTTP, and ended on the model's answer ------------
    assert outcome.session.status is SessionState.COMPLETED
    assert [m["type"] for m in outcome.posted] == ["user_request"] + ["execution_result"] * 3
    assert [m["type"] for m in outcome.replies] == [
        "discovery_plan",
        "execution_plan",
        "execution_plan",
        "final_answer",
    ]

    # ---- 1. the diagnostic left the application, in the body of the POST -----------------------
    carrying = outcome.carrying(CompileRepairer.COMPILE)
    assert carrying.encoding == "gzip"  # the demo configuration compresses; `body` is decompressed
    result = outcome.result(CompileRepairer.COMPILE)
    compiled, listed = result["results"]
    diagnostic = read_diagnostic(tc, compiled[case.diagnostic_stream])
    assert diagnostic is not None, compiled
    assert diagnostic.line == line and Path(diagnostic.file).name == tc.source
    assert json_fragment(diagnostic.text) in carrying.body  # literally, in the bytes posted
    assert compiled["task_id"] == "t2" and compiled["status"] == "failed"
    assert compiled["execution"] == "ran"
    assert compiled["exit_code"] == case.failing_exit_code
    assert compiled["failure_is_verdict"] is True
    assert "reason" not in compiled and compiled["truncated"] is False

    # ---- 2. the listing ran — ADR-029 kept the plan going, nothing in the plan asked for it ------
    compile_plan_sent = outcome.plan(CompileRepairer.COMPILE)["content"]
    assert set(compile_plan_sent) == {"plan_id", "objective", "execution_policy", "tasks"}
    assert all(set(task) == {"task_id", "type", "cmd"} for task in compile_plan_sent["tasks"])
    assert listed["task_id"] == "t3" and listed["status"] == "completed"
    assert listed["execution"] == "ran" and listed["exit_code"] == 0
    assert result["skipped_tasks"] == [] and result["cancelled_tasks"] == []
    assert result["status"] == "completed" and "stop_reason" not in result  # ADR-029 §2
    assert (case.artifact in listed["stdout"].split()) is case.emits_despite_errors

    # ---- 3. the repair names the line of the diagnostic, which nothing else gave the model ------
    repair = outcome.plan(CompileRepairer.REPAIR)
    repair_cmd, recompile_cmd = [task["cmd"] for task in repair["content"]["tasks"]]
    assert repair_cmd == f"sed -i.orig '{line}s|^|// |' {diagnostic.file}"
    assert recompile_cmd == tc.compile_cmd
    request = outcome.posted[0]["content"]
    assert not re.search(r"\d", request["goal"] + request["user_message"])  # no line from the user
    commands_before = [
        task["cmd"]
        for plan_id in (CompileRepairer.DISCOVER, CompileRepairer.COMPILE)
        for task in outcome.plan(plan_id)["content"]["tasks"]
    ]
    assert commands_before == [tc.version_cmd, tc.compile_cmd, "ls"]  # nothing read the source
    assert model.received[2]["content"] == result  # the model read what the application sent

    # ---- 4. the recompile passed, the artifact is on disk, the answer quotes the diagnostic -----
    repaired = outcome.result(CompileRepairer.REPAIR)
    assert repaired["status"] == "completed"
    assert [(r["task_id"], r["status"], r["exit_code"]) for r in repaired["results"]] == [
        ("t4", "completed", 0),
        ("t5", "completed", 0),
    ]
    assert (project / case.artifact).is_file()
    source = (project / tc.source).read_text(encoding="utf-8").splitlines()
    assert source[line - 1] == f"// {case.bad_line}"
    assert (project / f"{tc.source}.orig").read_text(encoding="utf-8") == case.files[tc.source]
    final = outcome.session.final_answer
    assert final is not None and final == outcome.replies[-1]["content"]
    assert final["status"] == "completed"
    assert final["evidence"][0] == f"t2: {diagnostic.quote}"

    # ---- 5. a compiler that answered is no failure: no record, no retry, no breaker ------------
    assert outcome.failures == [] and outcome.retry_decisions == []
    assert outcome.failure_events() == []
    compile_task = outcome.tasks["t2"]
    assert compile_task.status is TaskState.FAILED and compile_task.reason is None
    assert compile_task.attempt_count == 1 and compile_task.exit_code == case.failing_exit_code
    assert compile_task.stops_plan_on_failure is True  # ADR-009 §2 unchanged: read at run time
    compile_plan = outcome.plans[CompileRepairer.COMPILE]
    assert compile_plan.status is PlanState.COMPLETED and compile_plan.stop_reason is None
    assert (compile_plan.completed_task_count, compile_plan.failed_task_count) == (1, 1)

    trace = os.environ.get(TRACE_ENV)
    if trace and tc is C:
        write_trace(Path(trace), outcome, case, api, project)


# ================================================================================================
# 2. The converse: a diagnostic that does not reach the model cannot be repaired from
# ================================================================================================
@pytest.mark.skipif(shutil.which("gcc") is None, reason="gcc not on the PATH")
async def given_diagnostic_lost_on_the_way_when_compile_fails_then_model_cannot_repair(
    tmp_path: Path,
) -> None:
    """The same C project, the same double; between the application and the double the outputs
    of every result are lost. The application sent the diagnostic — the double never saw it, has
    no line to repair, and says so. The repairs above came from the diagnostic and nowhere else."""
    case = PROJECTS["c"]
    model = CompileRepairer(C)
    outcome, project, _ = await run_loop(
        tmp_path,
        case.files,
        model,
        goal="Make main.c compile",
        user_message="`gcc -c main.c -o main.o` fails on my project. Can you fix main.c?",
        tamper=drop_outputs,
    )

    sent = outcome.result(CompileRepairer.COMPILE)["results"][0]
    assert read_diagnostic(C, sent["stderr"]) is not None  # it did leave the application...
    seen = task_result(model.received[2]["content"], "t2")
    assert seen is not None and seen["stderr"] == "" and seen["exit_code"] == 1  # ...not arrive
    assert [m["type"] for m in outcome.replies] == [
        "discovery_plan",
        "execution_plan",
        "final_answer",
    ]
    final = outcome.session.final_answer
    assert final is not None and final["status"] == "failed"
    assert "names no position" in final["diagnosis"]
    assert (project / "main.c").read_text(encoding="utf-8") == case.files["main.c"]
    assert not (project / "main.o").exists()


# ================================================================================================
# 3. Controls: a failure that is no verdict, a misspelled compiler, a shell that cannot start
# ================================================================================================
CONFIGURE_SCRIPT = r"""# Writes build.cfg for the requested build mode.
import sys

MODES = {"release": "-O2", "debug": "-O0 -g"}


def configure(mode: str) -> str:
    if mode not in MODES:
        known = ", ".join(MODES)
        raise ValueError(f"unknown build mode {mode!r}; known modes: {known}")
    return f"mode={mode}\nflags={MODES[mode]}\n"


if __name__ == "__main__":
    text = configure(sys.argv[1])
    with open("build.cfg", "w", encoding="utf-8") as out:
        out.write(text)
    print("configured", sys.argv[1])
"""


@pytest.mark.skipif(shutil.which("python3") is None, reason="python3 not on the PATH")
async def given_script_raising_when_it_is_no_verdict_program_then_plan_stops_and_traceback_still_reaches_model(
    tmp_path: Path,
) -> None:
    """``python3`` is not in ``verdict_programs``: its non-zero exit is an ordinary failure, and
    ADR-009 §1 stops the plan — the listing after it is skipped. The traceback reaches the model
    all the same, and the model reruns the script with a mode the error itself lists."""
    model = ScriptFixer()
    outcome, project, _ = await run_loop(
        tmp_path,
        {"configure.py": CONFIGURE_SCRIPT},
        model,
        goal="Configure the build",
        user_message="Configure my project for a fast build: `python3 configure.py fast`.",
    )

    carrying = outcome.carrying("plan-configure")
    result = outcome.result("plan-configure")
    (failed,) = result["results"]
    assert failed["task_id"] == "t2" and failed["status"] == "failed"
    assert failed["execution"] == "ran" and failed["exit_code"] == 1
    assert "failure_is_verdict" not in failed  # not a recognised program
    assert "Traceback (most recent call last)" in failed["stderr"]
    raised = "ValueError: unknown build mode 'fast'; known modes: release, debug"
    assert raised in failed["stderr"].splitlines()
    assert json_fragment(raised) in carrying.body
    # ADR-009 §1 and §5: the plan stops, the listing is skipped and says why
    assert result["status"] == "stopped_on_failure"
    assert result["stop_reason"] == "task_failed:t2"
    assert result["skipped_tasks"] == [
        {"task_id": "t3", "reason": "plan_stopped:task_failed:t2", "execution": "not_started"}
    ]
    assert outcome.plans["plan-configure"].status is PlanState.STOPPED_ON_FAILURE
    assert outcome.tasks["t3"].status is TaskState.SKIPPED
    # the model reacted to the traceback it received
    assert model.received[2]["content"] == result
    (rerun,) = outcome.plan("plan-reconfigure")["content"]["tasks"]
    assert rerun["cmd"] == "python3 configure.py release"
    rerun_result = outcome.result("plan-reconfigure")["results"][0]
    assert rerun_result["exit_code"] == 0 and rerun_result["stdout"] == "configured release\n"
    assert (project / "build.cfg").read_text(encoding="utf-8") == "mode=release\nflags=-O2\n"
    final = outcome.session.final_answer
    assert outcome.session.status is SessionState.COMPLETED
    assert final is not None and final["status"] == "completed"
    assert final["evidence"][0] == f"t2: {raised}"
    # an exit code is an answer, even an unwelcome one: no failure record, no retry (ADR-008 §3)
    assert outcome.failures == [] and outcome.retry_decisions == []
    assert outcome.failure_events() == []


VALID_C_SOURCE = """#include <stdio.h>

int main(void) {
    printf("build ok\\n");
    return 0;
}
"""


TYPO = "gccc -c main.c -o main.o"


@pytest.mark.skipif(shutil.which("gcc") is None, reason="gcc not on the PATH")
async def given_misspelled_compiler_when_shell_cannot_find_it_then_exit_127_reaches_model_and_it_retypes_the_name(
    tmp_path: Path,
) -> None:
    """A misspelled compiler is a program that never ran — but not a command that never started.

    The executor launches ``bash -c 'gccc …'``: the shell starts, looks ``gccc`` up, prints
    ``gccc: command not found`` on stderr and exits **127**. The result reads ``execution: "ran"``
    — the shell did run — with ``exit_code: 127`` and, derived from that code under a POSIX shell,
    ``reason: "COMMAND_NOT_FOUND"`` (ADR-032); no ``FailureRecord`` is written, nothing is stored
    on the record, and there is no verdict. ``gccc`` is recognised as nothing anyway, so the plan
    stops as ADR-009 says. The model reads ``reason``, finds the program named in the shell's words,
    and types the name again.
    """
    model = MissingProgramFixer(
        "gcc",
        "gcc --version",
        "main.c",
        TYPO,
        diagnosis="The compiler name was mistyped; with the right name main.c compiles.",
    )
    outcome, project, _ = await run_loop(
        tmp_path,
        {"main.c": VALID_C_SOURCE},
        model,
        goal="Compile main.c",
        user_message="Please compile `main.c` with gcc.",
    )

    carrying = outcome.carrying("plan-compile")
    result = outcome.result("plan-compile")
    (typed,) = result["results"]
    assert typed["task_id"] == "t2" and typed["status"] == "failed"
    assert typed["execution"] == "ran"  # the shell ran: this is no spawn failure
    assert typed["exit_code"] == 127  # POSIX: command not found
    assert typed["reason"] == COMMAND_NOT_FOUND  # ADR-032, derived from the exit code
    assert "failure_is_verdict" not in typed
    assert typed["stdout"] == ""
    said = next(m for pattern in NOT_FOUND if (m := pattern.search(typed["stderr"])))
    assert said["program"] == "gccc"
    assert json_fragment(said.group(0)) in carrying.body
    assert result["status"] == "stopped_on_failure" and result["stop_reason"] == "task_failed:t2"
    assert result["skipped_tasks"] == [
        {"task_id": "t3", "reason": "plan_stopped:task_failed:t2", "execution": "not_started"}
    ]
    assert outcome.failures == [] and outcome.retry_decisions == []  # no SPAWN_FAILED recorded
    assert outcome.tasks["t2"].reason is None and outcome.tasks["t2"].exit_code == 127
    # the model read the reason and the shell's words, and typed the name again
    (retyped,) = outcome.plan("plan-retype")["content"]["tasks"]
    assert retyped["cmd"] == "gcc -c main.c -o main.o"
    assert outcome.result("plan-retype")["results"][0]["exit_code"] == 0
    assert (project / "main.o").is_file()
    final = outcome.session.final_answer
    assert final is not None and final["status"] == "completed"
    assert final["evidence"][0] == f"t2: {said.group(0)}"


VALID_JAVA_SOURCE = """public class Main {
    public static void main(String[] args) {
        System.out.println("build ok");
    }
}
"""
#: A JDK the model believes in and the machine does not have.
MISSING_JAVAC = "/opt/no-such-jdk/bin/javac"


@pytest.mark.skipif(shutil.which("javac") is None, reason="javac not on the PATH")
async def given_recognised_compiler_on_a_missing_path_when_compiled_then_no_verdict_the_plan_stops_and_the_model_recovers(
    tmp_path: Path,
) -> None:
    """The defect ADR-032 closes: a **recognised** compiler that the shell cannot find.

    ``javac`` is in ``verdict_programs``, and the command names it by a path that does not exist.
    Before ADR-032 the shell's ``127`` was read as the compiler's answer: ``failure_is_verdict:
    true`` and the plan went on, about a compiler that never ran. Now a verdict requires evidence
    that the program ran: ``execution: "ran"`` (the shell ran), ``exit_code: 127``,
    ``reason: "COMMAND_NOT_FOUND"``, no ``failure_is_verdict``; the plan stops as ADR-009 says for
    any failure, the listing is skipped, the shell's words still reach the model — and the model
    compiles with the ``javac`` its discovery found on the ``PATH``.
    """
    wrong_cmd = f"{MISSING_JAVAC} Main.java"
    assert VerdictPrograms(DEFAULT_VERDICT_PROGRAMS).matches(wrong_cmd)  # a recognised compiler
    model = MissingProgramFixer(
        "javac",
        "javac -version",
        "Main.java",
        wrong_cmd,
        diagnosis="The JDK path does not exist here; with the javac on the PATH Main.java compiles.",
    )
    outcome, project, _ = await run_loop(
        tmp_path,
        {"Main.java": VALID_JAVA_SOURCE},
        model,
        goal="Compile Main.java",
        user_message=f"Please compile `Main.java` with the JDK in {MISSING_JAVAC.rsplit('/', 2)[0]}.",
    )

    # ---- no verdict: the shell answered, not the compiler --------------------------------------
    carrying = outcome.carrying("plan-compile")
    result = outcome.result("plan-compile")
    (typed,) = result["results"]
    assert typed["task_id"] == "t2" and typed["status"] == "failed"
    assert typed["execution"] == "ran"  # the shell ran
    assert typed["exit_code"] == 127  # POSIX: command not found
    assert typed["reason"] == COMMAND_NOT_FOUND
    assert "failure_is_verdict" not in typed  # javac is recognised, and still no verdict
    assert typed["stdout"] == ""

    # ---- the plan stopped exactly as ADR-009 says for any failure ----------------------------
    assert result["status"] == "stopped_on_failure" and result["stop_reason"] == "task_failed:t2"
    assert result["skipped_tasks"] == [
        {"task_id": "t3", "reason": "plan_stopped:task_failed:t2", "execution": "not_started"}
    ]
    assert outcome.plans["plan-compile"].status is PlanState.STOPPED_ON_FAILURE
    assert outcome.tasks["t3"].status is TaskState.SKIPPED
    compile_task = outcome.tasks["t2"]
    assert compile_task.stops_plan_on_failure is True  # ADR-009 §2, no flag in the plan
    assert compile_task.reason is None and compile_task.exit_code == 127  # the reason is derived
    # a program the shell could not find is no failure of the application either
    assert outcome.failures == [] and outcome.retry_decisions == []
    assert outcome.failure_events() == []

    # ---- the shell's words reached the model, which read them -------------------------------
    said = next(m for pattern in NOT_FOUND if (m := pattern.search(typed["stderr"])))
    assert said["program"] == MISSING_JAVAC
    assert json_fragment(said.group(0)) in carrying.body
    assert model.received[2]["content"] == result

    # ---- and recovered: the compiler the discovery confirmed ---------------------------------
    (retyped,) = outcome.plan("plan-retype")["content"]["tasks"]
    assert retyped["cmd"] == "javac Main.java"
    assert outcome.result("plan-retype")["results"][0]["exit_code"] == 0
    assert (project / "Main.class").is_file()
    final = outcome.session.final_answer
    assert outcome.session.status is SessionState.COMPLETED
    assert final is not None and final["status"] == "completed"
    assert final["evidence"] == [f"t2: {said.group(0)}", "t4: exit code 0"]


async def given_shell_that_cannot_start_when_a_plan_runs_then_not_started_and_spawn_failed_reach_the_model(
    tmp_path: Path,
) -> None:
    """What ``not_started`` really is: the interpreter itself cannot be launched.

    The operator pinned ``[execution] shell`` to a ``bash`` that does not exist. The spawn fails
    before any command exists: the task is ``failed`` with ``execution: "not_started"``, no
    ``exit_code``, ``reason: "SPAWN_FAILED"`` and empty streams; a ``FailureRecord``
    (``TASK_EXECUTION_ERROR`` / ``SPAWN_FAILED``) keeps the spawn error; the plan stops (ADR-029
    §2: a command that never ran gives no verdict). The model reads ``execution`` and concludes
    that nothing can run — it has nothing to correct in its own command.
    """
    missing_shell = tmp_path / "missing" / "bash"
    model = CompileRepairer(C)
    outcome, project, _ = await run_loop(
        tmp_path,
        PROJECTS["c"].files,
        model,
        goal="Make main.c compile",
        user_message="`gcc -c main.c -o main.o` fails on my project. Can you fix main.c?",
        environ={"AGENTIC__EXECUTION__SHELL": str(missing_shell)},
    )

    result = outcome.result(CompileRepairer.DISCOVER)
    (version,) = result["results"]
    assert version["task_id"] == "t1" and version["status"] == "failed"
    assert version["execution"] == "not_started"
    assert "exit_code" not in version  # absent, not null (exclude_none)
    assert version["reason"] == "SPAWN_FAILED"
    assert version["stdout"] == "" and version["stderr"] == ""
    assert result["status"] == "stopped_on_failure" and result["stop_reason"] == "task_failed:t1"
    (failure,) = outcome.failures
    assert failure.error_type is ErrorType.TASK_EXECUTION_ERROR
    assert failure.error_code == "SPAWN_FAILED" and failure.task_id == "t1"
    assert str(missing_shell) in failure.details["error"]
    assert outcome.retry_decisions == []  # recorded, never retried
    assert [e.event_type for e in outcome.failure_events()] == [EventType.FAILURE_RECORDED]
    # the model reacted to `not_started`: it does not blame the compiler, it stops
    assert [m["type"] for m in outcome.replies] == ["discovery_plan", "final_answer"]
    final = outcome.session.final_answer
    assert outcome.session.status is SessionState.COMPLETED
    assert final is not None and final["status"] == "failed"
    assert final["evidence"] == ["t1: execution not_started, reason SPAWN_FAILED, no output"]
    assert not (project / "main.o").exists()
