"""Phase 4 — task execution (spec §2.5, §3.8, §3.9, §3.10, §8.2, §8.5, §12.5, §12.6, §18.2 ;
ADR-003, ADR-008, ADR-009, ADR-010, ADR-011, ADR-016, ADR-017, ADR-018).

Four surfaces are pinned here, **without spawning a single process** (§18.3):

1. the executor contract (``CommandSpec``, ``CancellationToken``, ``RawExecution.outcome``) and the
   ``FakeCommandExecutor`` double that every later phase relies on;
2. the platform adapters (shell resolution, launch argv, spawn kwargs, two-phase termination and
   orphan termination) on doubles: a fake process table and a fake process handle;
3. ``PayloadGuard``: the pure truncation algorithm of ADR-011 as a parametrised (E, O, B) table,
   the budget rule of ADR-010, ``fit_message`` and ``serve_chunk`` on the in-memory store;
4. ``ResultCollector``: one ``execution_result`` per terminal plan, in plan order (ADR-017).

The real ``SubprocessCommandExecutor`` is exercised in ``test_phase4_real_subprocess.py`` only.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app.config import ExecutionSection, PayloadSection
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.clock import FakeClock, SystemClock
from agentic_local_app.domain.models import BlobRecord, PlanRecord, TaskRecord
from agentic_local_app.domain.states import (
    ExecutionPolicy,
    OutputStream,
    PlanState,
    PlanType,
    TaskState,
    TaskType,
)
from agentic_local_app.execution import executor as executor_module
from agentic_local_app.execution import payload_guard as payload_guard_module
from agentic_local_app.execution import platform as platform_module
from agentic_local_app.execution import result_collector as result_collector_module
from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandExecutor,
    CommandSpec,
    OutputChunk,
    RawExecution,
    SubprocessCommandExecutor,
)
from agentic_local_app.execution.payload_guard import (
    ChunkError,
    ChunkResult,
    PayloadGuard,
    TruncatedOutput,
)
from agentic_local_app.execution.platform import (
    ORPHAN_START_TOLERANCE_MS,
    LaunchSpec,
    PlatformAdapter,
    PosixPlatformAdapter,
    PosixProcessTable,
    ProcessTable,
    WindowsPlatformAdapter,
    parse_proc_stat_start_ticks,
    select_platform,
)
from agentic_local_app.execution.result_collector import ResultCollector
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.messages import ExecutionResultContent, TaskRef, TaskResult
from agentic_local_app.testing import fake_executor as fake_executor_module
from agentic_local_app.testing.fake_executor import FakeCommandExecutor

pytestmark = pytest.mark.phase4

NOW = datetime(2026, 1, 1, tzinfo=UTC)


# ================================================================================================
# helpers
# ================================================================================================
def _spec(task_id: str = "t1", cmd: str = "echo hi", timeout_ms: int = 1_000) -> CommandSpec:
    return CommandSpec(task_id=task_id, cmd=cmd, timeout_ms=timeout_ms, cwd=".")


def _raw(**overrides: Any) -> RawExecution:
    base: dict[str, Any] = {
        "stdout": b"",
        "stderr": b"",
        "exit_code": 0,
        "timed_out": False,
        "cancelled": False,
        "spawn_error": None,
        "pid": 1,
        "process_group_id": 1,
        "started_monotonic_ms": 0,
        "ended_monotonic_ms": 0,
        "duration_ms": 0,
    }
    base.update(overrides)
    return RawExecution(**base)


def _pattern(n: int, seed: int = 0) -> bytes:
    """``n`` distinct-looking bytes so that slices can be checked against the original."""
    return bytes(((i * 7 + seed) % 251) for i in range(n))


def _plan(
    status: PlanState = PlanState.COMPLETED,
    stop_reason: str | None = None,
    plan_id: str = "plan-1",
) -> PlanRecord:
    return PlanRecord(
        plan_id=plan_id,
        session_id="sess-0001",
        conversation_id="conv-0001",
        cycle_id="cyc-0001",
        plan_type=PlanType.EXECUTION_PLAN,
        objective="objective",
        execution_policy=ExecutionPolicy.SEQUENTIAL,
        status=status,
        stop_reason=stop_reason,
        created_at=NOW,
        updated_at=NOW,
    )


def _task(
    task_id: str,
    order_index: int,
    status: TaskState,
    *,
    plan_id: str = "plan-1",
    type: TaskType = TaskType.CMD,  # noqa: A002 - mirrors the record field
    **fields: Any,
) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        plan_id=plan_id,
        session_id="sess-0001",
        conversation_id="conv-0001",
        order_index=order_index,
        type=type,
        cmd="echo" if type is TaskType.CMD else None,
        status=status,
        created_at=NOW,
        updated_at=NOW,
        **fields,
    )


def _result(
    task_id: str,
    stdout: str = "",
    stderr: str = "",
    stdout_range: tuple[int, int] | None = None,
    stderr_range: tuple[int, int] | None = None,
) -> TaskResult:
    return TaskResult(
        task_id=task_id,
        status="completed",
        exit_code=0,
        stdout=stdout,
        stderr=stderr,
        stdout_range=stdout_range,
        stderr_range=stderr_range,
    )


def _content(*results: TaskResult) -> ExecutionResultContent:
    return ExecutionResultContent(plan_id="plan-1", status="completed", results=list(results))


class FakeProcessTable(ProcessTable):
    """Process table double: start times are scripted, signals are recorded, exits are scripted."""

    def __init__(
        self,
        start_times: dict[int, datetime] | None = None,
        *,
        exits_on_terminate: frozenset[int] = frozenset(),
    ) -> None:
        self.start_times = start_times or {}
        self.exits_on_terminate = exits_on_terminate
        self.calls: list[tuple[str, int, int | None]] = []
        self.waits: list[tuple[int, int]] = []
        self._gone: set[int] = set()

    def start_time(self, pid: int) -> datetime | None:
        return None if pid in self._gone else self.start_times.get(pid)

    def terminate(self, pid: int, pgid: int | None) -> None:
        self.calls.append(("terminate", pid, pgid))
        if pid in self.exits_on_terminate:
            self._gone.add(pid)

    def kill(self, pid: int, pgid: int | None) -> None:
        self.calls.append(("kill", pid, pgid))
        self._gone.add(pid)

    def wait_exit(self, pid: int, timeout_ms: int) -> bool:
        self.waits.append((pid, timeout_ms))
        return pid in self._gone


class FakeProcessHandle:
    """Stands in for ``asyncio.subprocess.Process`` in adapter tests."""

    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self.signals: list[int] = []
        self.killed = 0

    def send_signal(self, sig: int) -> None:
        self.signals.append(sig)

    def kill(self) -> None:
        self.killed += 1


def _posix(
    which: Callable[[str], str | None] | None = None,
    table: ProcessTable | None = None,
    **config: Any,
) -> PosixPlatformAdapter:
    return PosixPlatformAdapter(
        ExecutionSection(**config),
        process_table=table or FakeProcessTable(),
        which=which or (lambda name: f"/usr/bin/{name}"),
    )


def _windows(table: ProcessTable | None = None, **config: Any) -> WindowsPlatformAdapter:
    return WindowsPlatformAdapter(
        ExecutionSection(**config), process_table=table or FakeProcessTable()
    )


# ================================================================================================
# 1. Executor contract: CommandSpec, CancellationToken, RawExecution.outcome
# ================================================================================================
def given_command_spec_when_created_then_frozen_with_defaults() -> None:
    spec = _spec()
    assert spec.shell is None and spec.env is None and spec.cwd == "."
    with pytest.raises((AttributeError, TypeError)):
        spec.cmd = "other"  # type: ignore[misc]


@pytest.mark.parametrize("timeout_ms", [0, -1])
def given_command_spec_with_non_positive_timeout_when_created_then_value_error(
    timeout_ms: int,
) -> None:
    with pytest.raises(ValueError):
        CommandSpec(task_id="t1", cmd="echo", timeout_ms=timeout_ms, cwd=".")


def given_command_spec_with_blank_cmd_when_created_then_value_error() -> None:
    with pytest.raises(ValueError):
        CommandSpec(task_id="t1", cmd="   ", timeout_ms=1, cwd=".")


async def given_fresh_token_when_cancelled_then_flag_reason_set_and_waiters_released() -> None:
    token = CancellationToken()
    assert token.is_cancelled is False and token.reason is None
    waiter = asyncio.ensure_future(token.wait())
    await asyncio.sleep(0)
    assert waiter.done() is False
    token.cancel("stop_plan_on_failure:t2")
    await asyncio.wait_for(waiter, timeout=1)
    assert token.is_cancelled is True and token.reason == "stop_plan_on_failure:t2"


async def given_cancelled_token_when_cancelled_again_then_first_reason_kept() -> None:
    token = CancellationToken()
    token.cancel("first")
    token.cancel("second")
    assert token.reason == "first" and token.is_cancelled is True
    await asyncio.wait_for(token.wait(), timeout=1)  # already released


def given_raw_execution_with_exit_zero_when_outcome_then_completed() -> None:
    assert _raw(exit_code=0).outcome is TaskState.COMPLETED


@pytest.mark.parametrize("exit_code", [1, 2, 127, -9])
def given_raw_execution_with_non_zero_exit_when_outcome_then_failed(exit_code: int) -> None:
    assert _raw(exit_code=exit_code).outcome is TaskState.FAILED


def given_raw_execution_timed_out_when_outcome_then_timed_out_even_with_exit_zero() -> None:
    assert _raw(exit_code=0, timed_out=True).outcome is TaskState.TIMED_OUT
    assert _raw(exit_code=None, timed_out=True).outcome is TaskState.TIMED_OUT


def given_raw_execution_cancelled_and_timed_out_when_outcome_then_cancelled_has_priority() -> None:
    assert _raw(exit_code=None, timed_out=True, cancelled=True).outcome is TaskState.CANCELLED
    assert _raw(exit_code=0, cancelled=True).outcome is TaskState.CANCELLED


def given_raw_execution_with_spawn_error_when_outcome_then_failed() -> None:
    raw = _raw(exit_code=None, spawn_error="FileNotFoundError: no such shell", pid=None)
    assert raw.outcome is TaskState.FAILED and raw.exit_code is None


def given_command_executor_abc_when_instantiated_then_type_error() -> None:
    with pytest.raises(TypeError):
        CommandExecutor()  # type: ignore[abstract]


def given_fake_executor_when_type_checked_then_it_is_a_command_executor(clock: FakeClock) -> None:
    assert isinstance(FakeCommandExecutor(clock), CommandExecutor)
    assert isinstance(SubprocessCommandExecutor(ExecutionSection(), SystemClock()), CommandExecutor)


# ================================================================================================
# 2. FakeCommandExecutor (the double of §18.3, module map §4)
# ================================================================================================
async def given_scripted_success_when_executed_then_completed_with_outputs_and_call_recorded(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"Linux dev\n", stderr=b"", exit_code=0)
    spec = _spec("t1", "uname -a")
    raw = await fake.execute(spec, cancel=CancellationToken())
    assert raw.outcome is TaskState.COMPLETED
    assert (raw.stdout, raw.stderr, raw.exit_code) == (b"Linux dev\n", b"", 0)
    assert raw.timed_out is False and raw.cancelled is False and raw.spawn_error is None
    assert fake.calls == [spec] and fake.cancellations == []


async def given_scripted_exit_2_when_executed_then_failed_with_stderr(clock: FakeClock) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(cmd="grep pattern pom.xml", stderr=b"grep: pom.xml: No such file\n", exit_code=2)
    raw = await fake.execute(_spec("t7", "grep pattern pom.xml"), cancel=CancellationToken())
    assert raw.outcome is TaskState.FAILED
    assert raw.exit_code == 2 and raw.stderr.startswith(b"grep:")


async def given_duration_above_timeout_when_executed_then_timed_out_and_clock_stops_at_timeout(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"partial", duration_ms=10_000)
    start = clock.monotonic_ms()
    raw = await fake.execute(_spec("t1", timeout_ms=300), cancel=CancellationToken())
    assert raw.outcome is TaskState.TIMED_OUT and raw.timed_out is True
    assert raw.exit_code is None
    assert raw.stdout == b"partial"  # everything scripted is kept (documented simplification)
    assert clock.monotonic_ms() - start == 300
    assert raw.duration_ms == 300
    assert (raw.started_monotonic_ms, raw.ended_monotonic_ms) == (start, start + 300)


async def given_duration_within_timeout_when_executed_then_fake_clock_advanced_by_duration(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", duration_ms=120)
    before = clock.now()
    raw = await fake.execute(_spec("t1", timeout_ms=1_000), cancel=CancellationToken())
    assert raw.duration_ms == 120 and raw.timed_out is False
    assert clock.now() - before == timedelta(milliseconds=120)


async def given_hanging_script_when_token_cancelled_then_cancelled_and_cancellation_recorded(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t3", stdout=b"before cancel", hang_until_cancelled=True)
    token = CancellationToken()
    running = asyncio.ensure_future(fake.execute(_spec("t3"), cancel=token))
    await asyncio.sleep(0)
    assert running.done() is False
    token.cancel("stop_plan_on_success:t2")
    raw = await asyncio.wait_for(running, timeout=1)
    assert raw.outcome is TaskState.CANCELLED and raw.cancelled is True
    assert raw.exit_code is None and raw.timed_out is False
    assert raw.stdout == b"before cancel"
    assert fake.cancellations == [("t3", "stop_plan_on_success:t2")]


async def given_already_cancelled_token_when_executed_then_cancelled_without_spawn(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"never")
    token = CancellationToken()
    token.cancel("user_interrupt")
    spawned: list[tuple[int, int | None]] = []
    raw = await fake.execute(
        _spec("t1"), cancel=token, on_spawn=lambda p, g: spawned.append((p, g))
    )
    assert raw.outcome is TaskState.CANCELLED and raw.pid is None and spawned == []
    assert raw.stdout == b""
    assert fake.cancellations == [("t1", "user_interrupt")]


async def given_spawn_error_script_when_executed_then_failed_with_spawn_error_and_no_pid(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", spawn_error="FileNotFoundError: /bin/zsh")
    spawned: list[tuple[int, int | None]] = []
    raw = await fake.execute(
        _spec("t1"), cancel=CancellationToken(), on_spawn=lambda p, g: spawned.append((p, g))
    )
    assert raw.outcome is TaskState.FAILED
    assert raw.spawn_error == "FileNotFoundError: /bin/zsh"
    assert raw.exit_code is None and raw.pid is None and raw.process_group_id is None
    assert spawned == []


async def given_two_executions_when_on_spawn_observed_then_pids_increase_and_pgid_equals_pid(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    spawned: list[tuple[int, int | None]] = []
    await fake.execute(
        _spec("t1"), cancel=CancellationToken(), on_spawn=lambda p, g: spawned.append((p, g))
    )
    await fake.execute(
        _spec("t2"), cancel=CancellationToken(), on_spawn=lambda p, g: spawned.append((p, g))
    )
    assert len(spawned) == 2
    (pid1, pgid1), (pid2, pgid2) = spawned
    assert pid1 > 0 and pid2 > pid1
    assert pgid1 == pid1 and pgid2 == pid2


async def given_scripted_chunks_when_executed_then_emitted_in_order_with_coherent_offsets(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    chunks = [
        OutputChunk(OutputStream.STDOUT, 0, b"hel"),
        OutputChunk(OutputStream.STDERR, 0, b"w"),
        OutputChunk(OutputStream.STDOUT, 3, b"lo\n"),
        OutputChunk(OutputStream.STDERR, 1, b"arn\n"),
    ]
    fake.script(task_id="t1", stdout=b"hello\n", stderr=b"warn\n", output_chunks=chunks)
    received: list[OutputChunk] = []
    raw = await fake.execute(_spec("t1"), cancel=CancellationToken(), on_output=received.append)
    assert received == chunks
    for stream in OutputStream:
        expected_offset = 0
        for chunk in (c for c in received if c.stream is stream):
            assert chunk.offset == expected_offset
            expected_offset += len(chunk.data)
    assert b"".join(c.data for c in received if c.stream is OutputStream.STDOUT) == raw.stdout
    assert b"".join(c.data for c in received if c.stream is OutputStream.STDERR) == raw.stderr


async def given_no_scripted_chunks_when_executed_then_one_chunk_per_non_empty_stream(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(task_id="t1", stdout=b"out", stderr=b"")
    received: list[OutputChunk] = []
    await fake.execute(_spec("t1"), cancel=CancellationToken(), on_output=received.append)
    assert received == [OutputChunk(OutputStream.STDOUT, 0, b"out")]


async def given_scripts_by_task_id_and_cmd_when_executed_then_task_id_wins_then_cmd_then_default(
    clock: FakeClock,
) -> None:
    fake = FakeCommandExecutor(clock)
    fake.script(stdout=b"default")
    fake.script(cmd="echo x", stdout=b"by cmd")
    fake.script(task_id="t1", stdout=b"by task")
    token = CancellationToken()
    assert (await fake.execute(_spec("t1", "echo x"), cancel=token)).stdout == b"by task"
    assert (await fake.execute(_spec("t2", "echo x"), cancel=token)).stdout == b"by cmd"
    assert (await fake.execute(_spec("t3", "echo y"), cancel=token)).stdout == b"default"


async def given_unscripted_executor_when_executed_then_empty_success_by_default(
    clock: FakeClock,
) -> None:
    raw = await FakeCommandExecutor(clock).execute(_spec(), cancel=CancellationToken())
    assert raw.outcome is TaskState.COMPLETED and raw.stdout == b"" and raw.exit_code == 0


async def given_non_fake_clock_when_fake_executes_then_no_sleep_and_duration_from_clock() -> None:
    fake = FakeCommandExecutor(SystemClock())
    fake.script(task_id="t1", duration_ms=60_000)
    raw = await asyncio.wait_for(
        fake.execute(_spec("t1", timeout_ms=100_000), cancel=CancellationToken()), 2
    )
    assert raw.outcome is TaskState.COMPLETED and raw.duration_ms < 60_000


# ================================================================================================
# 3. Platform adapters (ADR-003, ADR-016) — no process is ever spawned here
# ================================================================================================
def given_bash_available_when_posix_default_shell_resolved_then_bash() -> None:
    adapter = _posix(which=lambda name: "/usr/bin/bash" if name == "bash" else None)
    assert adapter.default_shell() == "/usr/bin/bash"


def given_no_bash_when_posix_default_shell_resolved_then_sh() -> None:
    adapter = _posix(which=lambda name: "/bin/sh" if name == "sh" else None)
    assert adapter.default_shell() == "/bin/sh"


def given_no_shell_found_when_posix_default_shell_resolved_then_bin_sh_fallback() -> None:
    assert _posix(which=lambda name: None).default_shell() == "/bin/sh"


def given_posix_adapter_when_launch_built_then_shell_dash_c_and_cmd_untouched() -> None:
    cmd = 'grep -n "maven.compiler.source\\|maven.compiler.target" pom.xml && echo $SHELL'
    launch = _posix().build_launch(cmd, None)
    assert launch == LaunchSpec(program="/usr/bin/bash", args=("-c", cmd))
    assert launch.argv == ("/usr/bin/bash", "-c", cmd)


def given_configured_shell_when_posix_launch_built_then_configured_shell_used() -> None:
    assert _posix().build_launch("ls", "/bin/zsh").program == "/bin/zsh"


def given_posix_adapter_when_spawn_kwargs_read_then_new_session_only() -> None:
    assert _posix().spawn_kwargs() == {"start_new_session": True}


def given_windows_adapter_when_default_shell_resolved_then_powershell() -> None:
    assert _windows().default_shell() == "powershell"


@pytest.mark.parametrize(
    ("shell", "expected_program", "expected_args"),
    [
        (None, "powershell", ("-NoProfile", "-NonInteractive", "-Command")),
        ("", "powershell", ("-NoProfile", "-NonInteractive", "-Command")),
        ("pwsh", "pwsh", ("-NoProfile", "-NonInteractive", "-Command")),
        ("cmd", "cmd", ("/c",)),
        ("C:\\Windows\\System32\\cmd.exe", "C:\\Windows\\System32\\cmd.exe", ("/c",)),
        ("C:\\Git\\bin\\bash.exe", "C:\\Git\\bin\\bash.exe", ("-c",)),
    ],
)
def given_windows_shell_setting_when_launch_built_then_interpreter_flags_match(
    shell: str | None, expected_program: str, expected_args: tuple[str, ...]
) -> None:
    launch = _windows().build_launch("Get-ChildItem", shell)
    assert launch.program == expected_program
    assert launch.args == (*expected_args, "Get-ChildItem")


def given_windows_adapter_when_spawn_kwargs_read_then_new_process_group_flag() -> None:
    assert _windows().spawn_kwargs() == {"creationflags": 0x00000200}


@pytest.mark.parametrize(
    ("platform", "expected"),
    [
        ("linux", PosixPlatformAdapter),
        ("darwin", PosixPlatformAdapter),
        ("win32", WindowsPlatformAdapter),
    ],
)
def given_sys_platform_when_platform_selected_then_matching_adapter(
    platform: str, expected: type[PlatformAdapter]
) -> None:
    adapter = select_platform(
        ExecutionSection(), platform=platform, process_table=FakeProcessTable()
    )
    assert type(adapter) is expected


def given_no_platform_override_when_platform_selected_then_current_platform_used() -> None:
    assert isinstance(
        select_platform(ExecutionSection(), process_table=FakeProcessTable()), PlatformAdapter
    )


async def given_posix_adapter_when_terminated_gracefully_then_sigterm_sent_to_group() -> None:
    table = FakeProcessTable()
    await _posix(table=table).terminate_gracefully(FakeProcessHandle(pid=77), 77)  # type: ignore[arg-type]
    assert table.calls == [("terminate", 77, 77)]


async def given_posix_adapter_when_killed_then_sigkill_sent_to_group() -> None:
    table = FakeProcessTable()
    await _posix(table=table).kill(FakeProcessHandle(pid=77), 77)  # type: ignore[arg-type]
    assert table.calls == [("kill", 77, 77)]


async def given_windows_adapter_when_terminated_gracefully_then_ctrl_break_sent_to_handle() -> None:
    handle = FakeProcessHandle(pid=88)
    table = FakeProcessTable()
    await _windows(table=table).terminate_gracefully(handle, None)  # type: ignore[arg-type]
    assert handle.signals == [platform_module.CTRL_BREAK_EVENT] and table.calls == []


async def given_windows_adapter_when_killed_then_tree_killed_and_handle_killed() -> None:
    handle = FakeProcessHandle(pid=88)
    table = FakeProcessTable()
    await _windows(table=table).kill(handle, None)  # type: ignore[arg-type]
    assert table.calls == [("kill", 88, None)] and handle.killed == 1


def given_running_orphan_started_with_task_when_terminated_then_soft_signal_suffices() -> None:
    table = FakeProcessTable(
        {501: NOW + timedelta(milliseconds=40)}, exits_on_terminate=frozenset({501})
    )
    adapter = _posix(table=table, cancel_drain_timeout_ms=700)
    assert adapter.terminate_orphan(501, 501, NOW) is True
    assert table.calls == [("terminate", 501, 501)]
    assert table.waits == [(501, 700)]


def given_orphan_ignoring_sigterm_when_terminated_then_killed_after_drain() -> None:
    table = FakeProcessTable({501: NOW})
    adapter = _posix(table=table, cancel_drain_timeout_ms=700)
    assert adapter.terminate_orphan(501, 501, NOW) is True
    assert table.calls == [("terminate", 501, 501), ("kill", 501, 501)]
    assert table.waits[0] == (501, 700)


def given_absent_process_when_orphan_terminated_then_false_and_no_signal() -> None:
    table = FakeProcessTable({})
    assert _posix(table=table).terminate_orphan(999, 999, NOW) is False
    assert table.calls == []


def given_pid_reused_by_later_process_when_orphan_terminated_then_false_and_no_signal() -> None:
    later = NOW + timedelta(milliseconds=ORPHAN_START_TOLERANCE_MS + 1)
    table = FakeProcessTable({501: later})
    assert _posix(table=table).terminate_orphan(501, 501, NOW) is False
    assert table.calls == []


def given_process_started_before_task_when_orphan_terminated_then_false_and_no_signal() -> None:
    earlier = NOW - timedelta(milliseconds=ORPHAN_START_TOLERANCE_MS + 1)
    table = FakeProcessTable({501: earlier})
    assert _posix(table=table).terminate_orphan(501, 501, NOW) is False
    assert table.calls == []


def given_start_time_within_tolerance_either_side_when_orphan_terminated_then_signalled() -> None:
    for delta in (-ORPHAN_START_TOLERANCE_MS, ORPHAN_START_TOLERANCE_MS):
        table = FakeProcessTable(
            {501: NOW + timedelta(milliseconds=delta)}, exits_on_terminate=frozenset({501})
        )
        assert _posix(table=table).terminate_orphan(501, None, NOW) is True
        assert table.calls == [("terminate", 501, None)]


def given_naive_started_at_when_orphan_terminated_then_treated_as_utc() -> None:
    table = FakeProcessTable({501: NOW}, exits_on_terminate=frozenset({501}))
    assert _posix(table=table).terminate_orphan(501, 501, NOW.replace(tzinfo=None)) is True


def given_windows_adapter_when_orphan_terminated_then_same_two_phase_mechanics() -> None:
    table = FakeProcessTable({601: NOW})
    adapter = _windows(table=table, cancel_drain_timeout_ms=300)
    assert adapter.terminate_orphan(601, None, NOW) is True
    assert table.calls == [("terminate", 601, None), ("kill", 601, None)]


def given_proc_stat_line_with_spaces_in_comm_when_parsed_then_start_ticks_is_field_22() -> None:
    line = (
        "3591 (my prog (x)) R 3589 3591 3589 0 -1 4194304 116 0 0 0 0 0 0 0 20 0 1 0 564924 "
        "2928640 366 18446744073709551615 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
    )
    assert parse_proc_stat_start_ticks(line) == 564924


def given_malformed_proc_stat_line_when_parsed_then_value_error() -> None:
    with pytest.raises(ValueError):
        parse_proc_stat_start_ticks("garbage")


HAS_PROC = Path("/proc/self/stat").exists()


@pytest.mark.skipif(not HAS_PROC, reason="/proc is Linux-specific")
def given_current_process_when_start_time_read_from_proc_then_recent_utc_datetime() -> None:
    start = PosixProcessTable().start_time(os.getpid())
    assert start is not None and start.tzinfo is not None
    now = datetime.now(UTC)  # allowed in tests only
    assert timedelta(0) <= now - start < timedelta(hours=12)


@pytest.mark.skipif(not HAS_PROC, reason="/proc is Linux-specific")
def given_live_current_process_when_wait_exit_polled_then_false_after_bounded_wait() -> None:
    assert PosixProcessTable().wait_exit(os.getpid(), timeout_ms=60) is False


def given_missing_proc_root_when_start_time_read_then_none_and_orphan_untouched(
    tmp_path: Path,
) -> None:
    table = PosixProcessTable(proc_root=tmp_path / "no-proc")
    assert table.start_time(os.getpid()) is None
    adapter = PosixPlatformAdapter(ExecutionSection(), process_table=table)
    assert adapter.terminate_orphan(os.getpid(), None, NOW) is False  # never signalled


def given_impossible_pid_when_real_table_queried_then_none_and_no_signal() -> None:
    table = PosixProcessTable()
    assert table.start_time(2**31 - 1) is None
    assert (
        PosixPlatformAdapter(ExecutionSection(), process_table=table).terminate_orphan(
            2**31 - 1, None, NOW
        )
        is False
    )


# ================================================================================================
# 4. PayloadGuard.apply — ADR-011 truncation table (E = stderr size, O = stdout size, B = budget)
# ================================================================================================
TRUNCATION_TABLE = [
    pytest.param(10, 10, 5, id="E>=B:stderr-only"),
    pytest.param(5, 5, 5, id="E==B:stderr-only"),
    pytest.param(3, 4, 10, id="E+O<=B:nothing-truncated"),
    pytest.param(3, 7, 10, id="E+O==B:nothing-truncated"),
    pytest.param(0, 20, 8, id="O-alone-too-big"),
    pytest.param(6, 20, 10, id="both-too-big"),
    pytest.param(0, 0, 5, id="empty-streams"),
    pytest.param(0, 0, 0, id="empty-streams-zero-budget"),
    pytest.param(3, 3, 1, id="B=1"),
    pytest.param(0, 3, 1, id="B=1-stdout-only"),
    pytest.param(2, 2, 0, id="B=0-everything-dropped"),
    pytest.param(4, 0, 2, id="stderr-only-stream"),
]


@pytest.mark.parametrize(("stderr_size", "stdout_size", "budget"), TRUNCATION_TABLE)
def given_streams_and_budget_when_applied_then_budget_respected_and_ranges_match_origin(
    stderr_size: int, stdout_size: int, budget: int
) -> None:
    stdout, stderr = _pattern(stdout_size, seed=1), _pattern(stderr_size, seed=2)
    out = PayloadGuard(PayloadSection()).apply(stdout, stderr, budget)

    assert isinstance(out, TruncatedOutput)
    assert len(out.stdout_kept) + len(out.stderr_kept) <= budget
    # ADR-011 step 1 and 2: stderr first (end kept), then the end of stdout with what is left
    expected_stderr_kept = min(stderr_size, budget)
    expected_stdout_kept = min(stdout_size, budget - expected_stderr_kept)
    assert len(out.stderr_kept) == expected_stderr_kept
    assert len(out.stdout_kept) == expected_stdout_kept
    assert out.stderr_kept == stderr[stderr_size - expected_stderr_kept :]
    assert out.stdout_kept == stdout[stdout_size - expected_stdout_kept :]
    # ranges are [start, end) of the origin stream and reproduce the kept bytes
    assert out.stderr_range == (stderr_size - expected_stderr_kept, stderr_size)
    assert out.stdout_range == (stdout_size - expected_stdout_kept, stdout_size)
    assert stderr[out.stderr_range[0] : out.stderr_range[1]] == out.stderr_kept
    assert stdout[out.stdout_range[0] : out.stdout_range[1]] == out.stdout_kept
    # metadata
    assert out.truncated is (
        expected_stderr_kept < stderr_size or expected_stdout_kept < stdout_size
    )
    assert out.original_size_bytes == stderr_size + stdout_size
    assert (out.stdout_total, out.stderr_total) == (stdout_size, stderr_size)


def given_stderr_at_least_budget_when_applied_then_stdout_not_transmitted_at_all() -> None:
    out = PayloadGuard(PayloadSection()).apply(b"x" * 100, b"error " * 20, 16)
    assert out.stdout_kept == b"" and out.stdout_range == (100, 100)
    assert out.stderr_kept == (b"error " * 20)[-16:] and out.truncated is True


def given_negative_budget_when_applied_then_value_error() -> None:
    with pytest.raises(ValueError):
        PayloadGuard(PayloadSection()).apply(b"", b"", -1)


def given_multibyte_char_cut_when_decoded_then_replacement_char_and_no_exception() -> None:
    out = PayloadGuard(PayloadSection()).apply("héllo".encode(), b"", 4)  # cuts inside "é"
    assert out.stdout_kept == b"\xa9llo"
    assert PayloadGuard.decode(out.stdout_kept) == "\ufffdllo"


def given_valid_utf8_when_decoded_then_text_preserved() -> None:
    assert PayloadGuard.decode("é ✓ ok".encode()) == "é ✓ ok"


# ================================================================================================
# 5. PayloadGuard.effective_budget — ADR-010
# ================================================================================================
@pytest.mark.parametrize(
    ("task_max", "plan_default", "expected"),
    [
        pytest.param(512, 2048, 512, id="task-declared"),
        pytest.param(None, 2048, 2048, id="plan-default"),
        pytest.param(None, None, 8_192, id="app-default"),
        pytest.param(10_000_000, None, 131_072, id="capped-by-hard-max"),
        pytest.param(None, 10_000_000, 131_072, id="plan-default-capped"),
    ],
)
def given_declared_budgets_when_effective_budget_computed_then_min_rule_applied(
    task_max: int | None, plan_default: int | None, expected: int
) -> None:
    assert PayloadGuard(PayloadSection()).effective_budget(task_max, plan_default) == expected


# ================================================================================================
# 6. PayloadGuard.fit_message — ADR-010 message cap
# ================================================================================================
def given_content_within_limit_when_fitted_then_returned_unchanged() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(_result("t1", stdout="a" * 100))
    fitted = guard.fit_message(content, guard.message_size(content))
    assert fitted == content


def _halved(
    result: TaskResult, stream: str, text: str, range_: tuple[int, int], total: int
) -> TaskResult:
    """The result as ``fit_message`` must leave it after halving ``stream`` (ADR-010)."""
    return result.model_copy(
        update={
            stream: text,
            f"{stream}_range": range_,
            f"{stream}_total": total,
            "truncated": True,
        }
    )


def given_oversized_content_when_fitted_then_longest_stdout_halved_and_range_updated() -> None:
    guard = PayloadGuard(PayloadSection())
    t1, t2 = (
        _result("t1", stdout="a" * 100, stdout_range=(900, 1000)),
        _result("t2", stdout="b" * 60),
    )
    content = _content(t1, t2)
    expected = _content(_halved(t1, "stdout", "a" * 50, (950, 1000), 1000), t2)
    limit = guard.message_size(expected)
    assert guard.message_size(content) > limit  # the first halving is enough

    fitted = guard.fit_message(content, limit)
    assert fitted == expected  # t1 keeps its last half, t2 untouched
    assert guard.message_size(fitted) <= limit


def given_untruncated_result_when_halved_then_range_and_total_derived_from_its_length() -> None:
    guard = PayloadGuard(PayloadSection())
    t1 = _result("t1", stdout="a" * 100)
    expected = _content(_halved(t1, "stdout", "a" * 50, (50, 100), 100))
    fitted = guard.fit_message(_content(t1), guard.message_size(expected))
    assert fitted == expected


def given_metadata_overhead_when_halving_saves_less_than_needed_then_halving_repeats() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(_result("t1", stdout="a" * 100))
    # limit below what one halving reaches (50 chars saved, ~43 bytes of range/total added)
    fitted = guard.fit_message(content, guard.message_size(content) - 20)
    (t1,) = fitted.results
    assert t1.stdout == "a" * 25 and t1.stdout_range == (75, 100) and t1.stdout_total == 100
    assert guard.message_size(fitted) <= guard.message_size(content) - 20


def given_two_equal_stdouts_when_fitted_then_first_in_list_reduced_first() -> None:
    guard = PayloadGuard(PayloadSection())
    t1, t2 = _result("t1", stdout="x" * 200), _result("t2", stdout="x" * 200)
    expected = _content(_halved(t1, "stdout", "x" * 100, (100, 200), 200), t2)
    fitted = guard.fit_message(_content(t1, t2), guard.message_size(expected))
    assert fitted == expected


def given_same_content_when_fitted_twice_then_identical_results() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(
        _result("t1", stdout="a" * 300, stderr="e" * 50), _result("t2", stdout="b" * 200)
    )
    limit = guard.message_size(content) - 250
    assert guard.fit_message(content, limit) == guard.fit_message(content, limit)


def given_still_oversized_when_all_stdouts_empty_then_stderr_reduced_last() -> None:
    guard = PayloadGuard(PayloadSection())
    t1 = _result("t1", stdout="s" * 10, stderr="e" * 200, stderr_range=(0, 200))
    # stdout is exhausted first (10 → 5 → 2 → 1 → 0), only then stderr is halved once
    expected = _content(
        _halved(_halved(t1, "stdout", "", (10, 10), 10), "stderr", "e" * 100, (100, 200), 200)
    )
    assert guard.message_size(_content(t1)) > guard.message_size(expected)
    fitted = guard.fit_message(_content(t1), guard.message_size(expected))
    assert fitted == expected
    (r,) = fitted.results
    assert r.stdout == "" and r.stderr == "e" * 100 and r.truncated is True


def given_oversized_stderr_only_when_fitted_then_stderr_halved() -> None:
    guard = PayloadGuard(PayloadSection())
    t1 = _result("t1", stderr="e" * 100)
    expected = _content(_halved(t1, "stderr", "e" * 50, (50, 100), 100))
    fitted = guard.fit_message(_content(t1), guard.message_size(expected))
    assert fitted == expected


def given_limit_unreachable_when_fitted_then_all_text_emptied_and_returned_best_effort() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(_result("t1", stdout="s" * 40, stderr="e" * 40))
    fitted = guard.fit_message(content, 10)
    (r,) = fitted.results
    assert r.stdout == "" and r.stderr == "" and r.truncated is True
    assert r.stdout_range == (40, 40) and r.stderr_range == (40, 40)
    assert guard.message_size(fitted) > 10  # cannot fit: metadata alone exceeds the cap


def given_everything_empty_and_still_oversized_when_fitted_then_returned_as_is() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(*(_result(f"t{i}") for i in range(20)))
    assert guard.fit_message(content, 10) == content


def given_fitted_content_when_measured_then_size_uses_canonical_json_without_none_fields() -> None:
    guard = PayloadGuard(PayloadSection())
    content = _content(_result("t1", stdout="é"))
    assert guard.message_size(content) == size_bytes(
        content.model_dump(mode="json", exclude_none=True)
    )


# ================================================================================================
# 7. PayloadGuard.serve_chunk — ADR-011 chunk_request on stored blobs
# ================================================================================================
def _store_with_blob(
    content: bytes, stream: OutputStream = OutputStream.STDOUT
) -> InMemoryConversationStore:
    store = InMemoryConversationStore()
    store.save_blob(
        BlobRecord(
            blob_id=f"blob-{stream.value}",
            session_id="sess-0001",
            task_id="t4",
            blob_type=stream,
            content=content,
            size_bytes=len(content),
            created_at=NOW,
        )
    )
    return store


def given_blob_when_chunk_served_from_start_then_data_range_total_and_not_eof() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 0, 4
    )
    assert result == ChunkResult(data=b"0123", range=(0, 4), total=10, eof=False)


def given_blob_when_chunk_served_from_middle_then_exact_window() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 4, 3
    )
    assert result == ChunkResult(data=b"456", range=(4, 7), total=10, eof=False)


def given_blob_when_chunk_reaches_end_then_clipped_and_eof() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 8, 100
    )
    assert result == ChunkResult(data=b"89", range=(8, 10), total=10, eof=True)


def given_blob_when_chunk_ends_exactly_at_total_then_eof() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 5, 5
    )
    assert result == ChunkResult(data=b"56789", range=(5, 10), total=10, eof=True)


@pytest.mark.parametrize("offset", [10, 11, -1])
def given_offset_outside_blob_when_chunk_served_then_chunk_range_invalid(offset: int) -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, offset, 4
    )
    assert isinstance(result, ChunkError)
    assert result.code == "CHUNK_RANGE_INVALID"
    assert result.details["offset"] == offset and result.details["total"] == 10


def given_unknown_ref_task_when_chunk_served_then_chunk_ref_not_found() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t-unknown", OutputStream.STDOUT, 0, 4
    )
    assert isinstance(result, ChunkError)
    assert result.code == "CHUNK_REF_NOT_FOUND"
    assert result.details == {"ref_task_id": "t-unknown", "stream": "stdout"}


def given_blob_of_other_session_when_chunk_served_then_chunk_ref_not_found() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0002", "t4", OutputStream.STDOUT, 0, 4
    )
    assert isinstance(result, ChunkError) and result.code == "CHUNK_REF_NOT_FOUND"


def given_stderr_stream_requested_when_chunk_served_then_stderr_blob_read() -> None:
    store = _store_with_blob(b"error text", OutputStream.STDERR)
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDERR, 6, 10
    )
    assert result == ChunkResult(data=b"text", range=(6, 10), total=10, eof=True)
    missing = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 0, 1
    )
    assert isinstance(missing, ChunkError) and missing.code == "CHUNK_REF_NOT_FOUND"


def given_max_bytes_above_hard_cap_when_chunk_served_then_capped() -> None:
    store = _store_with_blob(b"0123456789")
    guard = PayloadGuard(PayloadSection(hard_max_output_bytes=3))
    result = guard.serve_chunk(store, "sess-0001", "t4", OutputStream.STDOUT, 0, 1_000)
    assert result == ChunkResult(data=b"012", range=(0, 3), total=10, eof=False)


def given_non_positive_max_bytes_when_chunk_served_then_chunk_range_invalid() -> None:
    store = _store_with_blob(b"0123456789")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 0, 0
    )
    assert isinstance(result, ChunkError) and result.code == "CHUNK_RANGE_INVALID"


def given_empty_blob_when_chunk_served_then_chunk_range_invalid_with_total_zero() -> None:
    store = _store_with_blob(b"")
    result = PayloadGuard(PayloadSection()).serve_chunk(
        store, "sess-0001", "t4", OutputStream.STDOUT, 0, 4
    )
    assert isinstance(result, ChunkError)
    assert result.code == "CHUNK_RANGE_INVALID" and result.details["total"] == 0


# ================================================================================================
# 8. ResultCollector — one execution_result per terminal plan (§3.9, §12.5, ADR-009, ADR-017)
# ================================================================================================
def _output(stdout: bytes, stderr: bytes = b"", budget: int = 8_192) -> TruncatedOutput:
    return PayloadGuard(PayloadSection()).apply(stdout, stderr, budget)


def given_completed_plan_when_built_then_status_results_and_task_fields_mapped() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [
        _task(
            "t1",
            0,
            TaskState.COMPLETED,
            exit_code=0,
            duration_ms=12,
            max_output_bytes_applied=512,
            timeout_ms_applied=60_000,
        ),
        _task(
            "t5",
            1,
            TaskState.FAILED,
            exit_code=1,
            duration_ms=30,
            max_output_bytes_applied=2_048,
            timeout_ms_applied=60_000,
        ),
    ]
    outputs = {
        "t1": _output(b"Linux dev 5.15.0 x86_64\n/bin/bash\n/workspace/project"),
        "t5": _output(b"", b"invalid target release: 21"),
    }
    content = ResultCollector().build(plan, tasks, outputs, {})

    assert isinstance(content, ExecutionResultContent)
    assert content.plan_id == "plan-1" and content.status == "completed"
    assert content.stop_reason is None
    assert (
        content.skipped_tasks == []
        and content.cancelled_tasks == []
        and content.interrupted_tasks == []
    )
    t1, t5 = content.results
    assert t1.task_id == "t1" and t1.status == "completed" and t1.exit_code == 0
    assert t1.stdout == "Linux dev 5.15.0 x86_64\n/bin/bash\n/workspace/project" and t1.stderr == ""
    assert t1.truncated is False and t1.original_size_bytes == 52
    assert (t1.stdout_total, t1.stderr_total) == (52, 0)
    assert t1.stdout_range == (0, 52) and t1.stderr_range == (0, 0)
    assert t1.max_output_bytes_applied == 512 and t1.timeout_ms_applied == 60_000
    assert t1.timed_out is False and t1.duration_ms == 12 and t1.reason is None
    assert t1.ref_task_id is None and t1.data is None
    assert t5.status == "failed" and t5.exit_code == 1 and t5.stderr == "invalid target release: 21"


def given_completed_plan_when_dumped_then_matches_spec_12_5_shape() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [_task("t1", 0, TaskState.COMPLETED, exit_code=0)]
    content = ResultCollector().build(plan, tasks, {"t1": _output(b"ok")}, {})
    dumped = content.model_dump(mode="json", exclude_none=True)
    assert dumped == {
        "plan_id": "plan-1",
        "status": "completed",
        "results": [
            {
                "task_id": "t1",
                "status": "completed",
                "exit_code": 0,
                "stdout": "ok",
                "stderr": "",
                "truncated": False,
                "original_size_bytes": 2,
                "stdout_total": 2,
                "stderr_total": 0,
                "stdout_range": [0, 2],
                "stderr_range": [0, 0],
                "timed_out": False,
            }
        ],
        "skipped_tasks": [],
        "cancelled_tasks": [],
        "interrupted_tasks": [],
    }


def given_truncated_output_when_built_then_truncation_metadata_reported() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [_task("t4", 0, TaskState.COMPLETED, exit_code=0, max_output_bytes_applied=16)]
    content = ResultCollector().build(
        plan, tasks, {"t4": _output(b"x" * 100, b"e" * 6, budget=16)}, {}
    )
    (t4,) = content.results
    assert t4.truncated is True and t4.original_size_bytes == 106
    assert t4.stdout == "x" * 10 and t4.stderr == "e" * 6
    assert t4.stdout_range == (90, 100) and t4.stderr_range == (0, 6)
    assert t4.max_output_bytes_applied == 16


def given_plan_stopped_on_failure_when_built_then_skipped_tasks_with_reasons_and_stop_reason() -> (
    None
):
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="critical_task_failed:t2")
    tasks = [
        _task("t1", 0, TaskState.COMPLETED, exit_code=0),
        _task("t2", 1, TaskState.FAILED, exit_code=2, critical=True),
        _task("t3", 2, TaskState.SKIPPED, reason="critical_task_failed:t2"),
        _task("t4", 3, TaskState.SKIPPED, reason="dependency_skipped:t3"),
    ]
    outputs = {"t1": _output(b"ok"), "t2": _output(b"", b"boom")}
    content = ResultCollector().build(plan, tasks, outputs, {})
    assert content.status == "stopped_on_failure"
    assert content.stop_reason == "critical_task_failed:t2"
    assert [r.task_id for r in content.results] == ["t1", "t2"]
    assert content.results[1].status == "failed" and content.results[1].exit_code == 2
    assert content.skipped_tasks == [
        TaskRef(task_id="t3", reason="critical_task_failed:t2"),
        TaskRef(task_id="t4", reason="dependency_skipped:t3"),
    ]


def given_short_circuited_plan_when_built_then_cancelled_tasks_listed_in_plan_order() -> None:
    plan = _plan(PlanState.SHORT_CIRCUITED_ON_SUCCESS, stop_reason="stop_plan_on_success:t1")
    tasks = [
        _task("t3", 2, TaskState.CANCELLED, reason="stop_plan_on_success:t1"),
        _task("t1", 0, TaskState.COMPLETED, exit_code=0),
        _task("t2", 1, TaskState.CANCELLED, reason="stop_plan_on_success:t1"),
        _task("t4", 3, TaskState.SKIPPED, reason="stop_plan_on_success:t1"),
    ]
    content = ResultCollector().build(plan, tasks, {"t1": _output(b"found")}, {})
    assert content.status == "short_circuited_on_success"
    assert [r.task_id for r in content.results] == ["t1"]
    assert [c.task_id for c in content.cancelled_tasks] == ["t2", "t3"]
    assert content.cancelled_tasks[0].reason == "stop_plan_on_success:t1"
    assert [s.task_id for s in content.skipped_tasks] == ["t4"]


def given_timed_out_task_when_built_then_status_timed_out_lower_case_and_null_exit_code() -> None:
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="task_failed:t1")
    tasks = [
        _task(
            "t1",
            0,
            TaskState.TIMED_OUT,
            exit_code=None,
            timed_out=True,
            timeout_ms_applied=300,
            duration_ms=305,
        )
    ]
    content = ResultCollector().build(plan, tasks, {"t1": _output(b"partial")}, {})
    (t1,) = content.results
    assert t1.status == "timed_out" and t1.exit_code is None
    assert t1.timed_out is True and t1.timeout_ms_applied == 300 and t1.duration_ms == 305
    assert t1.stdout == "partial"


def given_interrupted_plan_when_built_then_value_error() -> None:
    plan = _plan(PlanState.INTERRUPTED, stop_reason="user_interrupt")
    tasks = [_task("t1", 0, TaskState.INTERRUPTED, reason="user_interrupt")]
    with pytest.raises(ValueError, match="INTERRUPTED"):
        ResultCollector().build(plan, tasks, {}, {})


@pytest.mark.parametrize("status", [PlanState.PENDING, PlanState.RUNNING])
def given_non_terminal_plan_when_built_then_value_error(status: PlanState) -> None:
    with pytest.raises(ValueError):
        ResultCollector().build(_plan(status), [], {}, {})


def given_failed_plan_when_built_then_status_failed_and_budget_stop_reason() -> None:
    plan = _plan(PlanState.FAILED, stop_reason="budget_exceeded:max_total_duration_ms")
    tasks = [
        _task("t1", 0, TaskState.COMPLETED, exit_code=0),
        _task("t2", 1, TaskState.SKIPPED, reason="budget_exceeded:max_total_duration_ms"),
    ]
    content = ResultCollector().build(plan, tasks, {"t1": _output(b"")}, {})
    assert (
        content.status == "failed"
        and content.stop_reason == "budget_exceeded:max_total_duration_ms"
    )
    assert [s.task_id for s in content.skipped_tasks] == ["t2"]


def given_chunk_request_task_when_built_then_chunk_fields_and_decoded_data() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [
        _task(
            "t-chunk-1",
            0,
            TaskState.COMPLETED,
            type=TaskType.CHUNK_REQUEST,
            ref_task_id="t4",
            stream=OutputStream.STDOUT,
            byte_offset=16_384,
            max_bytes=16_384,
            duration_ms=1,
        )
    ]
    chunk = ChunkResult(
        data="début du fichier".encode(), range=(16_384, 16_401), total=48_211, eof=False
    )
    content = ResultCollector().build(plan, tasks, {}, {"t-chunk-1": chunk})
    (r,) = content.results
    assert r.task_id == "t-chunk-1" and r.status == "completed"
    assert r.ref_task_id == "t4" and r.stream is OutputStream.STDOUT
    assert r.range == (16_384, 16_401) and r.total == 48_211 and r.eof is False
    assert r.data == "début du fichier"
    assert r.exit_code is None and r.stdout == "" and r.stderr == ""
    assert r.truncated is False and r.original_size_bytes is None


def given_chunk_request_without_stream_when_built_then_stdout_assumed() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [
        _task(
            "c1",
            0,
            TaskState.COMPLETED,
            type=TaskType.CHUNK_REQUEST,
            ref_task_id="t4",
            byte_offset=0,
            max_bytes=4,
        )
    ]
    chunk = ChunkResult(data=b"0123", range=(0, 4), total=10, eof=False)
    content = ResultCollector().build(plan, tasks, {}, {"c1": chunk})
    assert content.results[0].stream is OutputStream.STDOUT


def given_failed_chunk_request_when_built_then_failed_with_reason_code_and_no_data() -> None:
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="task_failed:c1")
    tasks = [
        _task(
            "c1",
            0,
            TaskState.FAILED,
            type=TaskType.CHUNK_REQUEST,
            ref_task_id="t-unknown",
            stream=OutputStream.STDERR,
            byte_offset=0,
            max_bytes=4,
            reason="CHUNK_REF_NOT_FOUND",
        )
    ]
    content = ResultCollector().build(plan, tasks, {}, {})
    (r,) = content.results
    assert r.status == "failed" and r.reason == "CHUNK_REF_NOT_FOUND"
    assert r.ref_task_id == "t-unknown" and r.stream is OutputStream.STDERR
    assert r.data is None and r.range is None and r.total is None and r.eof is None


def given_disordered_tasks_and_outputs_when_built_then_results_follow_order_index() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [
        _task("t3", 2, TaskState.COMPLETED, exit_code=0),
        _task("t1", 0, TaskState.COMPLETED, exit_code=0),
        _task("t2", 1, TaskState.FAILED, exit_code=1, continue_on_error=True),
    ]
    outputs = {"t2": _output(b"2"), "t3": _output(b"3"), "t1": _output(b"1")}
    content = ResultCollector().build(plan, tasks, outputs, {})
    assert [r.task_id for r in content.results] == ["t1", "t2", "t3"]
    assert [r.stdout for r in content.results] == ["1", "2", "3"]


def given_every_terminal_task_state_when_built_then_protocol_statuses_are_lower_case() -> None:
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="task_failed:t2")
    tasks = [
        _task("t1", 0, TaskState.COMPLETED, exit_code=0),
        _task("t2", 1, TaskState.FAILED, exit_code=1),
        _task("t3", 2, TaskState.TIMED_OUT, timed_out=True),
        _task("t4", 3, TaskState.SKIPPED, reason="task_failed:t2"),
        _task("t5", 4, TaskState.CANCELLED, reason="task_failed:t2"),
    ]
    content = ResultCollector().build(plan, tasks, {}, {})
    assert [r.status for r in content.results] == ["completed", "failed", "timed_out"]
    assert all(r.status == r.status.lower() for r in content.results)
    assert content.status == "stopped_on_failure"


def given_task_without_output_entry_when_built_then_record_fields_used_and_empty_text() -> None:
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="task_failed:t1")
    tasks = [
        _task(
            "t1",
            0,
            TaskState.FAILED,
            exit_code=None,
            reason="SPAWN_FAILED",
            truncated=False,
            original_size_bytes=0,
            stdout_total=0,
            stderr_total=0,
            stdout_range=(0, 0),
            stderr_range=(0, 0),
        )
    ]
    content = ResultCollector().build(plan, tasks, {}, {})
    (t1,) = content.results
    assert t1.stdout == "" and t1.stderr == "" and t1.reason == "SPAWN_FAILED"
    assert t1.original_size_bytes == 0 and t1.stdout_range == (0, 0) and t1.exit_code is None


def given_non_terminal_task_in_terminal_plan_when_built_then_value_error() -> None:
    plan = _plan(PlanState.COMPLETED)
    with pytest.raises(ValueError, match="RUNNING"):
        ResultCollector().build(plan, [_task("t1", 0, TaskState.RUNNING)], {}, {})


def given_task_of_another_plan_when_built_then_value_error() -> None:
    plan = _plan(PlanState.COMPLETED)
    with pytest.raises(ValueError, match="plan-2"):
        ResultCollector().build(
            plan, [_task("t1", 0, TaskState.COMPLETED, plan_id="plan-2", exit_code=0)], {}, {}
        )


def given_interrupted_tasks_in_non_interrupted_plan_when_built_then_listed_with_reason() -> None:
    plan = _plan(PlanState.FAILED, stop_reason="restart")
    tasks = [_task("t1", 0, TaskState.INTERRUPTED, reason="restart")]
    content = ResultCollector().build(plan, tasks, {}, {})
    assert content.interrupted_tasks == [TaskRef(task_id="t1", reason="restart")]


def given_task_without_reason_when_listed_as_skipped_then_state_name_used_as_reason() -> None:
    plan = _plan(PlanState.STOPPED_ON_FAILURE, stop_reason="task_failed:t0")
    content = ResultCollector().build(plan, [_task("t1", 0, TaskState.SKIPPED)], {}, {})
    assert content.skipped_tasks == [TaskRef(task_id="t1", reason="skipped")]


def given_built_result_when_fitted_by_payload_guard_then_pipeline_composes() -> None:
    plan = _plan(PlanState.COMPLETED)
    tasks = [_task("t1", 0, TaskState.COMPLETED, exit_code=0, max_output_bytes_applied=8_192)]
    content = ResultCollector().build(plan, tasks, {"t1": _output(b"z" * 4_000)}, {})
    guard = PayloadGuard(PayloadSection())
    fitted = guard.fit_message(content, 2_500)
    assert guard.message_size(fitted) <= 2_500
    assert fitted.results[0].truncated is True and fitted.results[0].stdout_range == (2_000, 4_000)


# ================================================================================================
# 9. Determinism guard (ADR-017, module map §2 rule 4)
# ================================================================================================
@pytest.mark.parametrize(
    "module",
    [
        executor_module,
        payload_guard_module,
        result_collector_module,
        platform_module,
        fake_executor_module,
    ],
    ids=lambda m: m.__name__.rsplit(".", 1)[-1],
)
def given_execution_module_when_inspected_then_no_wall_clock_or_randomness_used(
    module: Any,
) -> None:
    source = Path(module.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "datetime.now",
        "time.time",
        "time.monotonic",
        "time.sleep",
        "uuid",
        "random",
    ):
        assert forbidden not in source, forbidden
