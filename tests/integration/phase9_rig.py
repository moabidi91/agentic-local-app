"""Shared harness of the phase 9 integration tests (spec §18.2 phase 9, §18.3).

Everything is assembled by ``build_application`` with the doubles of §18.3 injected:
``FakeTransportGateway`` (scripted model), ``FakeCommandExecutor`` (no process),
``InMemoryConversationStore`` (or a ``SqliteConversationStore`` on ``tmp_path`` for the recovery
tests), ``FakeClock`` and ``SequentialIdGenerator``. Waiting never happens for real: the injected
``sleep`` advances the fake clock by the requested delay and yields once to the event loop.

The scripted model messages are the examples of spec §12.2, §12.3 and §12.7, adapted to the remote
conversation identifiers handed out by the fake gateway (``remote-0001``, ``remote-0002``, ...).
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from agentic_local_app.config import (
    AppConfig,
    AppSection,
    BudgetSection,
    CircuitBreakerSection,
    ContextSection,
    ExecutionSection,
    PayloadSection,
    ProtocolSection,
    RetrySection,
    ScratchSection,
)
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    ConversationRecord,
    CycleRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import Application, ConversationManager, build_application
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.fake import FakeTransportGateway

GOAL = "Understand the root cause of a Java build failure"
USER_MESSAGE = "Please debug the Java error in my project."
REMOTE_1 = "remote-0001"
REMOTE_2 = "remote-0002"
REMOTE_3 = "remote-0003"

#: Real-time bound of every wait in the phase 9 suite (the fake clock does the actual timing).
BOUND_S = 5.0

CMD_UNAME = "uname -a && echo $SHELL && echo $PWD"
CMD_JAVA = "java -version"
CMD_MVN = "mvn -version"
CMD_POM = "test -f pom.xml && sed -n '1,220p' pom.xml"
CMD_BUILD = "mvn clean install 2>&1 | tail -80"
CMD_JAVA_HOME = "echo $JAVA_HOME"
CMD_GREP = 'grep -n "maven.compiler.source\\|maven.compiler.target" pom.xml'

OUT_UNAME = b"Linux dev 5.15.0 x86_64\n/bin/bash\n/workspace/project"
OUT_JAVA = b'openjdk version "17.0.12" 2024-07-16'
OUT_MVN = b"Apache Maven 3.9.6\nJava version: 17.0.12"
OUT_POM = (
    b"<project><properties><maven.compiler.source>21</maven.compiler.source></properties></project>"
)
ERR_BUILD = b"[ERROR] invalid target release: 21"
OUT_JAVA_HOME = b"/usr/lib/jvm/java-17"
OUT_GREP = b"12:    <maven.compiler.source>21</maven.compiler.source>"


# =============================================================================================
# Scripted model messages (spec §12)
# =============================================================================================
def spec_discovery_tasks() -> list[dict[str, Any]]:
    """The five tasks of the §12.2 ``discovery_plan``."""
    return [
        {
            "task_id": "t1",
            "type": "cmd",
            "cmd": CMD_UNAME,
            "critical": False,
            "continue_on_error": True,
            "max_output_bytes": 2048,
        },
        {
            "task_id": "t2",
            "type": "cmd",
            "cmd": CMD_JAVA,
            "critical": False,
            "continue_on_error": True,
            "max_output_bytes": 1024,
        },
        {
            "task_id": "t3",
            "type": "cmd",
            "cmd": CMD_MVN,
            "critical": False,
            "continue_on_error": True,
            "max_output_bytes": 1024,
        },
        {
            "task_id": "t4",
            "type": "cmd",
            "cmd": CMD_POM,
            "critical": True,
            "continue_on_error": False,
            "stop_plan_on_failure": True,
            "max_output_bytes": 16384,
        },
        {
            "task_id": "t5",
            "type": "cmd",
            "cmd": CMD_BUILD,
            "critical": True,
            "continue_on_error": False,
            "stop_plan_on_failure": True,
            "depends_on": ["t4"],
            "max_output_bytes": 32768,
        },
    ]


def discovery_plan(
    remote: str = REMOTE_1,
    *,
    message_id: str = "model-msg-0001",
    plan_id: str = "plan-0",
    tasks: list[dict[str, Any]] | None = None,
    state_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """§12.2, with the remote id of the conversation and a deterministic model message id."""
    content: dict[str, Any] = {
        "plan_id": plan_id,
        "objective": "Discover execution environment and build context",
        "execution_policy": "sequential",
        "tasks": copy.deepcopy(tasks) if tasks is not None else spec_discovery_tasks(),
    }
    if state_summary is not None:
        content["state_summary"] = state_summary
    return {
        "type": "discovery_plan",
        "conversation_id": remote,
        "message_id": message_id,
        "content": content,
    }


def execution_plan(
    remote: str = REMOTE_1,
    *,
    message_id: str = "model-msg-0002",
    plan_id: str = "plan-1",
    tasks: list[dict[str, Any]] | None = None,
    execution_policy: str = "parallel",
    max_parallel_workers: int | None = 2,
    message_type: str = "execution_plan",
    objective: str = "Confirm Java version mismatch between Maven runtime and project target",
) -> dict[str, Any]:
    """§12.3 (parallel, two workers) — ``message_type`` may be ``priority_clarification`` (§12.4)."""
    content: dict[str, Any] = {
        "plan_id": plan_id,
        "objective": objective,
        "execution_policy": execution_policy,
        "tasks": copy.deepcopy(tasks)
        if tasks is not None
        else [
            {
                "task_id": "t6",
                "type": "cmd",
                "cmd": CMD_JAVA_HOME,
                "critical": False,
                "continue_on_error": True,
                "max_output_bytes": 512,
            },
            {
                "task_id": "t7",
                "type": "cmd",
                "cmd": CMD_GREP,
                "critical": True,
                "continue_on_error": False,
                "stop_plan_on_failure": True,
                "max_output_bytes": 2048,
            },
        ],
    }
    if max_parallel_workers is not None:
        content["max_parallel_workers"] = max_parallel_workers
    return {
        "type": message_type,
        "conversation_id": remote,
        "message_id": message_id,
        "content": content,
    }


FINAL_DIAGNOSIS = (
    "The build fails because the project targets Java 21 while Maven runs with Java 17."
)


def final_answer(
    remote: str = REMOTE_1, *, message_id: str = "model-msg-0003", evidence: bool = True
) -> dict[str, Any]:
    """§12.7."""
    content: dict[str, Any] = {
        "status": "completed",
        "diagnosis": FINAL_DIAGNOSIS,
        "recommended_next_step": (
            "Run Maven with JDK 21 or align the project target version with Java 17."
        ),
    }
    content["evidence"] = (
        [
            "uname confirms Linux x86_64 environment, shell is bash",
            "java -version shows OpenJDK 17.0.12",
            "mvn -version confirms Maven uses Java 17",
            "pom.xml targets maven.compiler.source = 21",
            "build fails with: invalid target release: 21",
        ]
        if evidence
        else []
    )
    return {
        "type": "final_answer",
        "conversation_id": remote,
        "message_id": message_id,
        "content": content,
    }


ANALYSIS_BODY = (
    "## Why the build fails\n\n"
    "`invalid target release: 21` means the project targets Java 21 while the compiler is "
    "older. Align `maven.compiler.release` with the installed JDK, or install JDK 21."
)
QUESTION_BODY = "Which module fails to build: the whole project or only `service-api`?"


def user_response(
    remote: str = REMOTE_1,
    *,
    message_id: str = "model-msg-0001",
    body: str = ANALYSIS_BODY,
    format: str = "markdown",
    status: str = "completed",
    expects_reply: bool = False,
) -> dict[str, Any]:
    """ADR-022: the model answers the user directly, without a plan."""
    return {
        "type": "user_response",
        "conversation_id": remote,
        "message_id": message_id,
        "content": {
            "format": format,
            "body": body,
            "status": status,
            "expects_reply": expects_reply,
        },
    }


def resume_ack(
    remote: str = REMOTE_2,
    original: str = REMOTE_1,
    *,
    message_id: str = "model-ack-0001",
    acknowledged: bool = True,
) -> dict[str, Any]:
    """§12.9, sent by the model in the child conversation."""
    return {
        "type": "context_resume_ack",
        "conversation_id": remote,
        "message_id": message_id,
        "content": {"original_conversation_id": original, "acknowledged": acknowledged},
    }


def cmd_task(task_id: str, cmd: str | None = None, **fields: Any) -> dict[str, Any]:
    task: dict[str, Any] = {"task_id": task_id, "type": "cmd", "cmd": cmd or f"run {task_id}"}
    task.update(fields)
    return task


def chunk_task(task_id: str, ref: str, *, offset: int = 0, max_bytes: int = 16) -> dict[str, Any]:
    return {
        "task_id": task_id,
        "type": "chunk_request",
        "ref_task_id": ref,
        "byte_offset": offset,
        "max_bytes": max_bytes,
    }


# =============================================================================================
# Configuration helpers
# =============================================================================================
def make_config(
    tmp_dir: str | None = None,
    *,
    budget: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    retry: dict[str, Any] | None = None,
    breaker: dict[str, Any] | None = None,
    execution: dict[str, Any] | None = None,
    protocol: dict[str, Any] | None = None,
    scratch: dict[str, Any] | None = None,
) -> AppConfig:
    """The default configuration with short drains and optional section overrides.

    ``[scratch]`` is **disabled** by default (ADR-026): the rig runs real plans through a double
    executor, so the defaults of the section would create ``./data/scratch/<session_id>`` next to
    the suite on every run. A test that wants a working space passes its own section, rooted under
    its ``tmp_path`` — nothing this suite runs ever writes outside a temporary directory.
    """
    exec_values: dict[str, Any] = {
        "interrupt_drain_timeout_ms": 500,
        "cancel_drain_timeout_ms": 500,
    }
    exec_values.update(execution or {})
    return AppConfig(
        app=AppSection(data_dir=tmp_dir or "./data-phase9-unused"),
        execution=ExecutionSection(**exec_values),
        scratch=ScratchSection(**(scratch if scratch is not None else {"enabled": False})),
        budget=BudgetSection(**(budget or {})),
        context=ContextSection(**(context or {})),
        payload=PayloadSection(**(payload or {})),
        protocol=ProtocolSection(**(protocol or {})),
        retry=RetrySection(**(retry or {})),
        circuit_breaker=CircuitBreakerSection(**(breaker or {})),
    )


def advancing_sleep(clock: FakeClock) -> Callable[[float], Awaitable[None]]:
    """A ``sleep`` that moves the fake clock instead of waiting (ADR-017)."""

    async def sleep(seconds: float) -> None:
        clock.advance(max(0, round(seconds * 1000)))
        await asyncio.sleep(0)

    return sleep


# =============================================================================================
# The rig
# =============================================================================================
@dataclass
class Rig:
    app: Application
    config: AppConfig
    clock: FakeClock
    ids: SequentialIdGenerator
    store: ConversationStore
    transport: FakeTransportGateway
    executor: FakeCommandExecutor
    recorder: RecordingSubscriber

    @property
    def manager(self) -> ConversationManager:
        return self.app.manager

    # ---- scripting -------------------------------------------------------------------------
    def reply(self, remote: str, *messages: dict[str, Any]) -> None:
        """Queue one reply batch (one GET) per message, in order."""
        for message in messages:
            self.transport.enqueue_messages(remote, [message])

    def script_spec_outputs(self) -> None:
        """Outputs of the §12 commands: everything succeeds except the Maven build (t5)."""
        self.executor.script(cmd=CMD_UNAME, stdout=OUT_UNAME)
        self.executor.script(cmd=CMD_JAVA, stderr=OUT_JAVA)
        self.executor.script(cmd=CMD_MVN, stdout=OUT_MVN)
        self.executor.script(cmd=CMD_POM, stdout=OUT_POM)
        self.executor.script(cmd=CMD_BUILD, stderr=ERR_BUILD, exit_code=1)
        self.executor.script(cmd=CMD_JAVA_HOME, stdout=OUT_JAVA_HOME)
        self.executor.script(cmd=CMD_GREP, stdout=OUT_GREP)

    def script_java_scenario(self, remote: str = REMOTE_1) -> None:
        """The full §12 loop for ``remote``: discovery_plan, execution_plan, final_answer."""
        self.script_spec_outputs()
        self.reply(remote, discovery_plan(remote), execution_plan(remote), final_answer(remote))

    # ---- driving ---------------------------------------------------------------------------
    async def start(self, **kwargs: Any) -> SessionRecord:
        kwargs.setdefault("goal", GOAL)
        kwargs.setdefault("user_message", USER_MESSAGE)
        if isinstance(kwargs.get("budget"), dict):
            kwargs["budget"] = SessionBudget.model_validate(kwargs["budget"])
        return await self.manager.start_session(**kwargs)

    async def wait(self, session_id: str, *, timeout_ms: int | None = None) -> SessionRecord:
        return await asyncio.wait_for(self.manager.wait(session_id, timeout_ms=timeout_ms), BOUND_S)

    async def run(self, **kwargs: Any) -> SessionRecord:
        session = await self.start(**kwargs)
        return await self.wait(session.session_id)

    # ---- reading back ------------------------------------------------------------------------
    def session(self, session_id: str) -> SessionRecord:
        record = self.store.get_session(session_id)
        assert record is not None
        return record

    def conversation(self, conversation_id: str) -> ConversationRecord:
        record = self.store.get_conversation(conversation_id)
        assert record is not None
        return record

    def current_conversation(self, session_id: str) -> ConversationRecord:
        session = self.session(session_id)
        assert session.current_conversation_id is not None
        return self.conversation(session.current_conversation_id)

    def conversations(self, session_id: str) -> list[ConversationRecord]:
        return self.store.list_conversations(session_id)

    def cycles(self, conversation_id: str) -> list[CycleRecord]:
        return self.store.list_cycles(conversation_id)

    def plan(self, session_id: str, plan_id: str) -> PlanRecord:
        record = self.store.get_plan(session_id, plan_id)
        assert record is not None
        return record

    def tasks(self, session_id: str, plan_id: str | None = None) -> list[TaskRecord]:
        return self.store.list_tasks(session_id, plan_id=plan_id)

    def task(self, session_id: str, task_id: str) -> TaskRecord:
        record = self.store.get_task(session_id, task_id)
        assert record is not None
        return record

    def posted_types(self) -> list[str]:
        return [str(payload["type"]) for _, payload in self.transport.posted]

    def posted(self, index: int) -> dict[str, Any]:
        return self.transport.posted[index][1]

    def events(self, event_type: EventType | None = None) -> list[Event]:
        if event_type is None:
            return list(self.recorder.events)
        return self.recorder.of_type(event_type)

    def event_kinds(self) -> list[tuple[str, str | None]]:
        """``(event_type, to)`` of every event, ``to`` from the payload when present."""
        return [(e.event_type.value, e.payload.get("to")) for e in self.recorder.events]


def make_rig(
    config: AppConfig | None = None,
    *,
    store: ConversationStore | None = None,
    clock: FakeClock | None = None,
    ids: SequentialIdGenerator | None = None,
    run_recovery: bool = False,
    instructions: str | None = None,
    reply_timeout_ms: int = 120_000,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    translator: ShellTranslator | None = None,
) -> Rig:
    """Assemble a full application on the §18.3 doubles and record every event of its bus."""
    cfg = config or make_config()
    clk = clock or FakeClock()
    generator = ids or SequentialIdGenerator()
    backing: ConversationStore = store if store is not None else InMemoryConversationStore()
    transport = FakeTransportGateway(clk, reply_timeout_ms=reply_timeout_ms)
    executor = FakeCommandExecutor(clk)
    # the recorder is subscribed before the wiring so that it also sees the recovery events
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="phase9-recorder")
    app = build_application(
        cfg,
        store=backing,
        transport=transport,
        executor=executor,
        clock=clk,
        ids=generator,
        bus=bus,
        run_recovery=run_recovery,
        instructions=instructions,
        sleep=sleep if sleep is not None else advancing_sleep(clk),
        # the scripted machine of this rig is POSIX (see OUT_UNAME): pinning the dialect
        # keeps the suite identical on Windows, where the default would target PowerShell
        translator=translator or ShellTranslator(ShellDialect.POSIX),
    )
    return Rig(
        app=app,
        config=cfg,
        clock=clk,
        ids=generator,
        store=backing,
        transport=transport,
        executor=executor,
        recorder=recorder,
    )


async def settle(rounds: int = 25) -> None:
    """Let every ready callback of the event loop run."""
    for _ in range(rounds):
        await asyncio.sleep(0)
