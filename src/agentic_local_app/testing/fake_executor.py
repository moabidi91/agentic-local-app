"""``FakeCommandExecutor`` — the shell double of §18.3 (module map §4). No process is ever spawned.

Behaviour is scripted per ``task_id`` (first match), per exact ``cmd`` (second) or by a default
(third), with :meth:`FakeCommandExecutor.script`. Each execution:

1. records the :class:`~agentic_local_app.execution.executor.CommandSpec` in ``calls``;
2. returns a cancelled result at once when the token is already set (as the real executor);
3. reports a ``spawn_error`` without pid when scripted so;
4. calls ``on_spawn`` with an increasing fake pid (``pgid == pid``, as a POSIX session leader);
5. emits the scripted ``output_chunks`` through ``on_output`` — or, when none were scripted, one
   chunk per non-empty stream so that live-output consumers see something realistic;
6. advances the :class:`~agentic_local_app.domain.clock.FakeClock` (when it is one) by
   ``min(duration_ms, spec.timeout_ms)``: a duration above the timeout yields ``timed_out=True``
   with the clock stopped at the timeout, exactly where the real executor terminates the process;
   the scripted outputs are kept in full (simplification: the fake does not model how much would
   have been produced before the termination);
7. ``hang_until_cancelled`` waits on the token and yields ``cancelled=True``, recording
   ``(task_id, reason)`` in ``cancellations``. A hang that nobody cancels blocks the test — the
   30 s ``pytest-timeout`` guard catches it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

from agentic_local_app.domain.clock import Clock, FakeClock
from agentic_local_app.domain.states import OutputStream
from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandExecutor,
    CommandSpec,
    OutputCallback,
    OutputChunk,
    RawExecution,
    SpawnCallback,
)

__all__ = ["FakeCommandExecutor", "ScriptedExecution"]


@dataclass(frozen=True)
class ScriptedExecution:
    stdout: bytes = b""
    stderr: bytes = b""
    exit_code: int = 0
    duration_ms: int = 0
    spawn_error: str | None = None
    hang_until_cancelled: bool = False
    output_chunks: tuple[OutputChunk, ...] | None = None


class FakeCommandExecutor(CommandExecutor):
    _FIRST_PID = 4_000

    def __init__(self, clock: Clock, *, default: ScriptedExecution | None = None) -> None:
        self._clock = clock
        self._default = default or ScriptedExecution()
        self._by_task: dict[str, ScriptedExecution] = {}
        self._by_cmd: dict[str, ScriptedExecution] = {}
        self._next_pid = self._FIRST_PID
        self.calls: list[CommandSpec] = []
        self.cancellations: list[tuple[str, str]] = []

    # ---- scripting -----------------------------------------------------------------------
    def script(
        self,
        *,
        cmd: str | None = None,
        task_id: str | None = None,
        stdout: bytes = b"",
        stderr: bytes = b"",
        exit_code: int = 0,
        duration_ms: int = 0,
        spawn_error: str | None = None,
        hang_until_cancelled: bool = False,
        output_chunks: Sequence[OutputChunk] | None = None,
    ) -> ScriptedExecution:
        """Register a behaviour for ``task_id``, for the exact ``cmd``, or as the default when
        neither is given. Returns the registered script."""
        if duration_ms < 0:
            raise ValueError("duration_ms must be >= 0")
        scripted = ScriptedExecution(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            duration_ms=duration_ms,
            spawn_error=spawn_error,
            hang_until_cancelled=hang_until_cancelled,
            output_chunks=tuple(output_chunks) if output_chunks is not None else None,
        )
        if task_id is not None:
            self._by_task[task_id] = scripted
        elif cmd is not None:
            self._by_cmd[cmd] = scripted
        else:
            self._default = scripted
        return scripted

    def lookup(self, spec: CommandSpec) -> ScriptedExecution:
        if spec.task_id in self._by_task:
            return self._by_task[spec.task_id]
        return self._by_cmd.get(spec.cmd, self._default)

    # ---- CommandExecutor -----------------------------------------------------------------
    async def execute(
        self,
        spec: CommandSpec,
        *,
        cancel: CancellationToken,
        on_output: OutputCallback | None = None,
        on_spawn: SpawnCallback | None = None,
    ) -> RawExecution:
        self.calls.append(spec)
        started = self._clock.monotonic_ms()
        scripted = self.lookup(spec)

        if cancel.is_cancelled:
            self.cancellations.append((spec.task_id, cancel.reason or ""))
            return self._result(b"", b"", None, started, cancelled=True)

        if scripted.spawn_error is not None:
            return self._result(b"", b"", None, started, spawn_error=scripted.spawn_error)

        pid = self._next_pid
        self._next_pid += 1
        if on_spawn is not None:
            on_spawn(pid, pid)
        await asyncio.sleep(0)  # let concurrent workers interleave, as a real spawn would

        if on_output is not None:
            for chunk in self._chunks(scripted):
                on_output(chunk)

        if scripted.hang_until_cancelled:
            await cancel.wait()
            self.cancellations.append((spec.task_id, cancel.reason or ""))
            return self._result(
                scripted.stdout, scripted.stderr, None, started, cancelled=True, pid=pid
            )

        timed_out = scripted.duration_ms > spec.timeout_ms
        self._advance(min(scripted.duration_ms, spec.timeout_ms))
        return self._result(
            scripted.stdout,
            scripted.stderr,
            None if timed_out else scripted.exit_code,
            started,
            timed_out=timed_out,
            pid=pid,
        )

    # ---- helpers -------------------------------------------------------------------------
    @staticmethod
    def _chunks(scripted: ScriptedExecution) -> tuple[OutputChunk, ...]:
        if scripted.output_chunks is not None:
            return scripted.output_chunks
        derived: list[OutputChunk] = []
        if scripted.stdout:
            derived.append(OutputChunk(OutputStream.STDOUT, 0, scripted.stdout))
        if scripted.stderr:
            derived.append(OutputChunk(OutputStream.STDERR, 0, scripted.stderr))
        return tuple(derived)

    def _advance(self, ms: int) -> None:
        if ms > 0 and isinstance(self._clock, FakeClock):
            self._clock.advance(ms)

    def _result(
        self,
        stdout: bytes,
        stderr: bytes,
        exit_code: int | None,
        started: int,
        *,
        timed_out: bool = False,
        cancelled: bool = False,
        spawn_error: str | None = None,
        pid: int | None = None,
    ) -> RawExecution:
        ended = self._clock.monotonic_ms()
        return RawExecution(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            timed_out=timed_out,
            cancelled=cancelled,
            spawn_error=spawn_error,
            pid=pid,
            process_group_id=pid,
            started_monotonic_ms=started,
            ended_monotonic_ms=ended,
            duration_ms=max(0, ended - started),
        )
