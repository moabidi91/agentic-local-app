"""Phase 4 — ``SubprocessCommandExecutor`` against real processes (ADR-003, ADR-008, ADR-016, ADR-018).

These are the **only** tests allowed to spawn a process (marker ``real_subprocess``). Every command
runs the current interpreter (``sys.executable``) with a quote-free ``-c`` snippet so that the same
test is portable across bash/sh (POSIX) and PowerShell (Windows): the interpreter path is quoted
with ``shlex.quote`` on POSIX and with PowerShell single quotes on Windows.

Timing assertions use ``time.perf_counter`` — allowed in tests, never in production code.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import sys
import time
from pathlib import Path

import pytest

from agentic_local_app.config import ExecutionSection
from agentic_local_app.domain.clock import SystemClock
from agentic_local_app.domain.states import OutputStream, TaskState
from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandSpec,
    OutputChunk,
    SubprocessCommandExecutor,
)

pytestmark = [pytest.mark.phase4, pytest.mark.real_subprocess]

IS_WINDOWS = sys.platform == "win32"


def _python(code: str) -> str:
    """A shell command running ``code`` with the current interpreter. ``code`` must not contain quotes."""
    assert "'" not in code and '"' not in code, "keep snippets quote-free for portability"
    if IS_WINDOWS:
        return f"& '{sys.executable}' -c '{code}'"
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"


def _executor(**overrides: int) -> SubprocessCommandExecutor:
    settings: dict[str, int] = {
        "cancel_drain_timeout_ms": 500,
        "live_output_interval_ms": 0,
        "live_output_chunk_bytes": 4_096,
    }
    settings.update(overrides)
    return SubprocessCommandExecutor(ExecutionSection(**settings), SystemClock())  # type: ignore[arg-type]


def _spec(cmd: str, timeout_ms: int = 10_000, **fields: object) -> CommandSpec:
    return CommandSpec(task_id="t1", cmd=cmd, timeout_ms=timeout_ms, cwd=str(Path.cwd()), **fields)  # type: ignore[arg-type]


async def given_printing_command_when_executed_then_completed_with_stdout_hi() -> None:
    raw = await _executor().execute(
        _spec(_python("print(chr(104)+chr(105))")), cancel=CancellationToken()
    )
    assert raw.outcome is TaskState.COMPLETED
    assert raw.exit_code == 0 and raw.stdout.strip() == b"hi" and raw.stderr == b""
    assert raw.timed_out is False and raw.cancelled is False and raw.spawn_error is None
    assert raw.pid is not None and raw.pid > 0
    assert raw.duration_ms >= 0 and raw.ended_monotonic_ms >= raw.started_monotonic_ms


async def given_command_exiting_3_when_executed_then_failed_with_exit_code_3() -> None:
    raw = await _executor().execute(
        _spec(_python("import sys; sys.exit(3)")), cancel=CancellationToken()
    )
    assert raw.outcome is TaskState.FAILED and raw.exit_code not in (0, None)
    if not IS_WINDOWS:  # ``powershell -Command`` maps any code other than 0/1 onto 1
        assert raw.exit_code == 3


async def given_command_writing_stderr_when_executed_then_stderr_captured_separately() -> None:
    code = "import sys; sys.stderr.write(chr(101)*3); sys.stdout.write(chr(111)); sys.exit(2)"
    raw = await _executor().execute(_spec(_python(code)), cancel=CancellationToken())
    assert raw.outcome is TaskState.FAILED and raw.exit_code not in (0, None)
    assert b"eee" in raw.stderr and raw.stdout.strip() == b"o"  # PowerShell decorates stderr
    if not IS_WINDOWS:
        assert raw.stderr == b"eee" and raw.stdout == b"o" and raw.exit_code == 2


async def given_sleeping_command_when_timeout_300ms_then_timed_out_quickly() -> None:
    started = time.perf_counter()
    raw = await _executor().execute(
        _spec(_python("import time; time.sleep(5)"), timeout_ms=300), cancel=CancellationToken()
    )
    elapsed = time.perf_counter() - started
    assert raw.outcome is TaskState.TIMED_OUT and raw.timed_out is True
    assert raw.exit_code is None and raw.cancelled is False
    assert elapsed < 3.0, f"two-phase termination took {elapsed:.2f}s"
    assert raw.duration_ms >= 300
    assert raw.pid is not None
    _assert_process_gone(raw.pid)


@pytest.mark.skipif(IS_WINDOWS, reason="SIGTERM handling is POSIX-specific")
async def given_command_ignoring_sigterm_when_timeout_then_killed_after_drain_and_timed_out() -> (
    None
):
    code = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print(1, flush=True); time.sleep(10)"
    started = time.perf_counter()
    raw = await _executor(cancel_drain_timeout_ms=400).execute(
        _spec(_python(code), timeout_ms=300), cancel=CancellationToken()
    )
    elapsed = time.perf_counter() - started
    assert raw.outcome is TaskState.TIMED_OUT
    assert raw.stdout.strip() == b"1"  # output read before termination is kept
    assert 0.7 <= elapsed < 3.0, f"expected timeout + drain then SIGKILL, took {elapsed:.2f}s"
    assert raw.pid is not None
    _assert_process_gone(raw.pid)


async def given_sleeping_command_when_token_cancelled_then_cancelled_and_process_gone() -> None:
    executor = _executor()
    token = CancellationToken()
    running = asyncio.ensure_future(
        executor.execute(
            _spec(_python("import time; print(0, flush=True); time.sleep(5)")), cancel=token
        )
    )
    await asyncio.sleep(0.4)
    assert running.done() is False
    started = time.perf_counter()
    token.cancel("stop_plan_on_failure:t2")
    raw = await asyncio.wait_for(running, timeout=5)
    assert time.perf_counter() - started < 3.0
    assert raw.outcome is TaskState.CANCELLED and raw.cancelled is True
    assert raw.timed_out is False and raw.exit_code is None
    assert raw.stdout.strip() == b"0"
    assert raw.pid is not None
    _assert_process_gone(raw.pid)


async def given_running_command_when_execute_coroutine_cancelled_then_process_killed_hard() -> None:
    spawned: list[tuple[int, int | None]] = []
    running = asyncio.ensure_future(
        _executor().execute(
            _spec(_python("import time; time.sleep(5)")),
            cancel=CancellationToken(),
            on_spawn=lambda p, g: spawned.append((p, g)),
        )
    )
    await asyncio.sleep(0.3)
    assert spawned and running.done() is False
    started = time.perf_counter()
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert time.perf_counter() - started < 3.0
    _assert_process_gone(spawned[0][0])


def _assert_process_gone(pid: int) -> None:
    """The interpreter (and its group) must not survive a termination by the executor."""
    if IS_WINDOWS:
        return  # ``os.kill(pid, 0)`` is not a liveness probe on Windows
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def given_streaming_command_when_executed_then_live_chunks_received_in_order() -> None:
    code = "import time; [(print(i, flush=True), time.sleep(0.05)) for i in range(3)]"
    received: list[OutputChunk] = []
    raw = await _executor().execute(
        _spec(_python(code)), cancel=CancellationToken(), on_output=received.append
    )
    assert raw.outcome is TaskState.COMPLETED
    stdout_chunks = [c for c in received if c.stream is OutputStream.STDOUT]
    assert len(stdout_chunks) >= 1
    assert b"".join(c.data for c in stdout_chunks) == raw.stdout
    expected_offset = 0
    for chunk in stdout_chunks:
        assert chunk.offset == expected_offset and len(chunk.data) > 0
        expected_offset += len(chunk.data)
    assert raw.stdout.split() == [b"0", b"1", b"2"]


async def given_interval_configured_when_streaming_then_slices_coalesced_but_complete() -> None:
    code = "import time; [(print(i, flush=True), time.sleep(0.03)) for i in range(5)]"
    received: list[OutputChunk] = []
    raw = await _executor(live_output_interval_ms=200).execute(
        _spec(_python(code)), cancel=CancellationToken(), on_output=received.append
    )
    stdout_chunks = [c for c in received if c.stream is OutputStream.STDOUT]
    assert b"".join(c.data for c in stdout_chunks) == raw.stdout  # nothing lost by coalescing
    assert raw.stdout.split() == [b"0", b"1", b"2", b"3", b"4"]
    assert 1 <= len(stdout_chunks) < 5  # five writes 30 ms apart never yield five emissions
    expected_offset = 0
    for chunk in stdout_chunks:
        assert chunk.offset == expected_offset
        expected_offset += len(chunk.data)


async def given_large_output_when_executed_then_fully_captured_and_chunks_bounded() -> None:
    received: list[OutputChunk] = []
    raw = await _executor(live_output_chunk_bytes=1_024).execute(
        _spec(_python("print(chr(120)*200000)")),
        cancel=CancellationToken(),
        on_output=received.append,
    )
    assert raw.outcome is TaskState.COMPLETED
    assert len(raw.stdout.strip()) == 200_000
    assert received and all(len(c.data) <= 1_024 for c in received)
    assert b"".join(c.data for c in received if c.stream is OutputStream.STDOUT) == raw.stdout


async def given_command_when_spawned_then_on_spawn_receives_pid_before_completion() -> None:
    spawned: list[tuple[int, int | None]] = []
    raw = await _executor().execute(
        _spec(_python("print(1)")),
        cancel=CancellationToken(),
        on_spawn=lambda p, g: spawned.append((p, g)),
    )
    assert len(spawned) == 1
    pid, pgid = spawned[0]
    assert pid == raw.pid and pid > 0
    if IS_WINDOWS:
        assert pgid is None and raw.process_group_id is None
    else:
        assert (
            pgid == pid == raw.process_group_id
        )  # start_new_session=True: the shell leads its group


async def given_custom_env_when_executed_then_merged_with_inherited_environment() -> None:
    var = "chr(65)*3"  # AAA
    path_var = "chr(80)+chr(65)+chr(84)+chr(72)"  # PATH
    code = f"import os; print(os.environ[{var}] + str({path_var} in os.environ))"
    raw = await _executor().execute(
        _spec(_python(code), env={"AAA": "yes-42"}), cancel=CancellationToken()
    )
    assert raw.outcome is TaskState.COMPLETED, raw.stderr
    assert raw.stdout.strip() == b"yes-42True"


async def given_missing_cwd_when_executed_then_spawn_error_reported_not_raised(
    tmp_path: Path,
) -> None:
    spawned: list[tuple[int, int | None]] = []
    spec = CommandSpec(
        task_id="t1", cmd=_python("print(1)"), timeout_ms=1_000, cwd=str(tmp_path / "missing")
    )
    raw = await _executor().execute(
        spec, cancel=CancellationToken(), on_spawn=lambda p, g: spawned.append((p, g))
    )
    assert raw.outcome is TaskState.FAILED
    assert raw.spawn_error is not None and raw.exit_code is None and raw.pid is None
    assert spawned == [] and raw.stdout == b"" and raw.stderr == b""


async def given_missing_shell_when_executed_then_spawn_error_names_the_interpreter() -> None:
    spec = _spec(_python("print(1)"), shell="/nonexistent/shell-xyz")
    raw = await _executor().execute(spec, cancel=CancellationToken())
    assert raw.outcome is TaskState.FAILED and raw.spawn_error is not None
    assert "shell-xyz" in raw.spawn_error or "FileNotFoundError" in raw.spawn_error


async def given_already_cancelled_token_when_executed_then_nothing_spawned() -> None:
    token = CancellationToken()
    token.cancel("user_interrupt")
    spawned: list[tuple[int, int | None]] = []
    raw = await _executor().execute(
        _spec(_python("print(1)")), cancel=token, on_spawn=lambda p, g: spawned.append((p, g))
    )
    assert raw.outcome is TaskState.CANCELLED and raw.pid is None and spawned == []
