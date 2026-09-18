"""``CommandExecutor`` — the shell boundary (§3.8, §18.3 ; ADR-003, ADR-008, ADR-016, ADR-018).

One ABC, one real implementation and one double:

- :class:`CommandExecutor` runs a :class:`CommandSpec` and returns a :class:`RawExecution`. A command
  that **fails is a result** (``exit_code != 0``), a command that cannot even be started is a result
  too (``spawn_error`` set, ``exit_code`` None) — neither raises. :class:`TaskExecutionError` is
  reserved for defects of the executor itself (a pipe reader crashing, for instance).
- :class:`SubprocessCommandExecutor` spawns the interpreter chosen by the :class:`PlatformAdapter`,
  reports ``pid`` / ``pgid`` through ``on_spawn`` **before** waiting (ADR-016), reads stdout and
  stderr concurrently in bounded slices published through ``on_output`` (ADR-018), enforces the
  per-task timeout and the cancellation token with the two-phase termination of ADR-003.
- :class:`~agentic_local_app.testing.fake_executor.FakeCommandExecutor` scripts all of the above
  without a process.

Timestamps and durations come from the injected :class:`~agentic_local_app.domain.clock.Clock`
(``monotonic_ms``); only the *waiting* itself uses the asyncio loop (ADR-017).
"""

from __future__ import annotations

import asyncio
import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from agentic_local_app.config import ExecutionSection
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import TaskExecutionError
from agentic_local_app.domain.states import OutputStream, TaskState
from agentic_local_app.execution.platform import PlatformAdapter, select_platform

__all__ = [
    "CancellationToken",
    "CommandExecutor",
    "CommandSpec",
    "OutputCallback",
    "OutputChunk",
    "RawExecution",
    "SpawnCallback",
    "SubprocessCommandExecutor",
]


# ------------------------------------------------------------------------------------------------
# Value objects
# ------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class CommandSpec:
    """What to run: the model's ``cmd`` as-is, the effective timeout (ADR-008, already capped by the
    caller), the working directory and interpreter of the application (ADR-003), extra environment
    variables merged over the inherited environment."""

    task_id: str
    cmd: str
    timeout_ms: int
    cwd: str
    shell: str | None = None
    env: dict[str, str] | None = None

    def __post_init__(self) -> None:
        if not self.cmd or not self.cmd.strip():
            raise ValueError("cmd must not be blank")
        if self.timeout_ms <= 0:
            raise ValueError("timeout_ms must be > 0")


class CancellationToken:
    """Set once by the plan runner (stop condition, interruption); observed by the executor.

    The first ``reason`` is kept (it names the cause in the task record, ADR-009 §5).
    """

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._reason: str | None = None

    def cancel(self, reason: str) -> None:
        if self._reason is None:
            self._reason = reason
        self._event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        return self._reason

    async def wait(self) -> None:
        await self._event.wait()


@dataclass(frozen=True)
class OutputChunk:
    """A live slice of one stream: ``offset`` is its position in the full stream (ADR-018)."""

    stream: OutputStream
    offset: int
    data: bytes


@dataclass(frozen=True)
class RawExecution:
    """Everything the executor observed. ``outcome`` maps it onto a terminal task state.

    ``exit_code`` is ``None`` when the process was terminated by the executor (timeout,
    cancellation) or never started (``spawn_error``).
    """

    stdout: bytes
    stderr: bytes
    exit_code: int | None
    timed_out: bool
    cancelled: bool
    spawn_error: str | None
    pid: int | None
    process_group_id: int | None
    started_monotonic_ms: int
    ended_monotonic_ms: int
    duration_ms: int

    @property
    def outcome(self) -> TaskState:
        """Priority: ``cancelled`` > ``timed_out`` > ``exit_code``.

        A cancellation is a decision of the plan runner and always wins; a timeout is a decision of
        the executor and wins over whatever code the terminated process returned; otherwise the
        command completed when ``exit_code == 0`` and failed in every other case, including a
        spawn error (``exit_code`` None, ADR-008 §4).
        """
        if self.cancelled:
            return TaskState.CANCELLED
        if self.timed_out:
            return TaskState.TIMED_OUT
        if self.spawn_error is None and self.exit_code == 0:
            return TaskState.COMPLETED
        return TaskState.FAILED


OutputCallback = Callable[[OutputChunk], None]
SpawnCallback = Callable[[int, int | None], None]


# ------------------------------------------------------------------------------------------------
# Boundary
# ------------------------------------------------------------------------------------------------
class CommandExecutor(ABC):
    @abstractmethod
    async def execute(
        self,
        spec: CommandSpec,
        *,
        cancel: CancellationToken,
        on_output: OutputCallback | None = None,
        on_spawn: SpawnCallback | None = None,
    ) -> RawExecution:
        """Run ``spec`` to completion, timeout or cancellation.

        ``on_spawn(pid, pgid)`` is called as soon as the process exists (ADR-016); ``on_output`` for
        every live slice (ADR-018). Never raises for a failing or unstartable command.
        """


class SubprocessCommandExecutor(CommandExecutor):
    """The real executor. Waiting uses the event loop; every timestamp comes from ``clock``."""

    def __init__(
        self, config: ExecutionSection, clock: Clock, platform: PlatformAdapter | None = None
    ) -> None:
        self._config = config
        self._clock = clock
        self._platform = platform or select_platform(config)
        self.callback_errors = 0  # ``on_output`` exceptions swallowed (the blob stays the truth)

    @property
    def platform(self) -> PlatformAdapter:
        return self._platform

    # ---- public --------------------------------------------------------------------------
    async def execute(
        self,
        spec: CommandSpec,
        *,
        cancel: CancellationToken,
        on_output: OutputCallback | None = None,
        on_spawn: SpawnCallback | None = None,
    ) -> RawExecution:
        started = self._clock.monotonic_ms()
        if cancel.is_cancelled:
            return self._result(b"", b"", None, started, cancelled=True)

        launch = self._platform.build_launch(spec.cmd, spec.shell or self._config.shell or None)
        env = dict(os.environ)
        env.update(spec.env or {})
        try:
            proc = await asyncio.create_subprocess_exec(
                *launch.argv,
                cwd=spec.cwd,
                env=env,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **self._platform.spawn_kwargs(),
            )
        except (OSError, ValueError) as exc:
            return self._result(b"", b"", None, started, spawn_error=f"{type(exc).__name__}: {exc}")

        pid = proc.pid
        pgid = self._platform.process_group_id(pid)
        assert proc.stdout is not None and proc.stderr is not None  # PIPE requested
        stdout_buf, stderr_buf = bytearray(), bytearray()
        readers: list[asyncio.Task[None]] = [
            asyncio.ensure_future(
                self._pump(proc.stdout, OutputStream.STDOUT, stdout_buf, on_output)
            ),
            asyncio.ensure_future(
                self._pump(proc.stderr, OutputStream.STDERR, stderr_buf, on_output)
            ),
        ]
        try:
            if on_spawn is not None:
                try:
                    on_spawn(pid, pgid)
                except BaseException:
                    await self._abort(proc, pgid)
                    raise
            timed_out, cancelled = await self._supervise(proc, pgid, cancel, spec.timeout_ms)
        finally:
            await self._settle_readers(readers)

        exit_code = None if (timed_out or cancelled) else proc.returncode
        return self._result(
            bytes(stdout_buf),
            bytes(stderr_buf),
            exit_code,
            started,
            timed_out=timed_out,
            cancelled=cancelled,
            pid=pid,
            pgid=pgid,
        )

    # ---- pieces --------------------------------------------------------------------------
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
        pgid: int | None = None,
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
            process_group_id=pgid,
            started_monotonic_ms=started,
            ended_monotonic_ms=ended,
            duration_ms=max(0, ended - started),
        )

    async def _supervise(
        self,
        proc: asyncio.subprocess.Process,
        pgid: int | None,
        cancel: CancellationToken,
        timeout_ms: int,
    ) -> tuple[bool, bool]:
        """Wait for exit, timeout or cancellation; returns ``(timed_out, cancelled)``.

        A process that exits by itself — even if the token was set at the very same moment — is
        reported by its exit code: only a termination actually carried out by the executor sets
        ``timed_out`` / ``cancelled``.

        The token is the graceful path. Cancelling the ``execute`` coroutine itself (an outer
        ``asyncio`` cancellation) kills the process hard, waits at most the drain, and re-raises:
        no process is ever left behind by the executor.
        """
        exit_task: asyncio.Future[int] = asyncio.ensure_future(proc.wait())
        cancel_task: asyncio.Future[None] = asyncio.ensure_future(cancel.wait())
        waiting: set[asyncio.Future[Any]] = {exit_task, cancel_task}
        try:
            done, _ = await asyncio.wait(
                waiting, timeout=timeout_ms / 1000, return_when=asyncio.FIRST_COMPLETED
            )
            if exit_task in done:
                return False, False
            cancelled = cancel_task in done
            await self._terminate_two_phase(proc, pgid, exit_task)
            return (not cancelled, cancelled)
        except asyncio.CancelledError:
            await self._abort(proc, pgid)
            raise
        finally:
            for task in waiting:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiting, return_exceptions=True)

    async def _terminate_two_phase(
        self,
        proc: asyncio.subprocess.Process,
        pgid: int | None,
        exit_task: asyncio.Future[int],
    ) -> None:
        """ADR-003: soft signal, ``cancel_drain_timeout_ms`` of grace, hard kill, bounded wait."""
        drain_s = self._config.cancel_drain_timeout_ms / 1000
        await self._platform.terminate_gracefully(proc, pgid)
        done, _ = await asyncio.wait({exit_task}, timeout=drain_s)
        if done:
            return
        await self._platform.kill(proc, pgid)
        await asyncio.wait({exit_task}, timeout=drain_s)

    async def _abort(self, proc: asyncio.subprocess.Process, pgid: int | None) -> None:
        """Hard stop when the caller failed right after the spawn (nothing was persisted)."""
        await self._platform.kill(proc, pgid)
        exit_task: asyncio.Future[int] = asyncio.ensure_future(proc.wait())
        await asyncio.wait({exit_task}, timeout=self._config.cancel_drain_timeout_ms / 1000)
        if not exit_task.done():
            exit_task.cancel()
            await asyncio.gather(exit_task, return_exceptions=True)

    async def _settle_readers(self, readers: list[asyncio.Task[None]]) -> None:
        """Readers end with the pipes; if a descendant keeps a pipe open they are cut after the drain."""
        _, pending = await asyncio.wait(
            readers, timeout=self._config.cancel_drain_timeout_ms / 1000
        )
        for reader in pending:
            reader.cancel()
        outcomes = await asyncio.gather(*readers, return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, Exception) and not isinstance(outcome, asyncio.CancelledError):
                raise TaskExecutionError(
                    "OUTPUT_READ_FAILED", error=f"{type(outcome).__name__}: {outcome}"
                ) from outcome

    async def _pump(
        self,
        stream: asyncio.StreamReader,
        kind: OutputStream,
        buffer: bytearray,
        on_output: OutputCallback | None,
    ) -> None:
        """Copy ``stream`` into ``buffer`` (the blob) and publish live slices (ADR-018).

        Slices are at most ``live_output_chunk_bytes`` long and published at most once per
        ``live_output_interval_ms`` per stream: bytes arriving in between are coalesced and flushed
        either when the interval elapses (timer) or at EOF (last slice, exempt from the cadence).
        """
        chunk_bytes = self._config.live_output_chunk_bytes
        interval_ms = self._config.live_output_interval_ms
        pending = bytearray()
        pending_offset = 0
        last_emit_ms: int | None = None
        read_task: asyncio.Task[bytes] | None = None

        def flush() -> None:
            nonlocal last_emit_ms
            if on_output is not None and pending:
                view = bytes(pending)
                offset = pending_offset
                for start in range(0, len(view), chunk_bytes):
                    piece = view[start : start + chunk_bytes]
                    self._emit(on_output, OutputChunk(kind, offset, piece))
                    offset += len(piece)
                last_emit_ms = self._clock.monotonic_ms()
            pending.clear()

        try:
            while True:
                if read_task is None:
                    read_task = asyncio.ensure_future(stream.read(chunk_bytes))
                timeout: float | None = None
                if pending and last_emit_ms is not None:
                    remaining = interval_ms - (self._clock.monotonic_ms() - last_emit_ms)
                    timeout = max(remaining, 0) / 1000
                done, _ = await asyncio.wait({read_task}, timeout=timeout)
                if not done:
                    flush()  # the interval elapsed with data waiting
                    continue
                data = read_task.result()
                read_task = None
                if not data:
                    flush()  # EOF: last slice
                    return
                if not pending:
                    pending_offset = len(buffer)
                buffer += data
                if on_output is None:
                    continue
                pending += data
                now = self._clock.monotonic_ms()
                if last_emit_ms is None or interval_ms == 0 or now - last_emit_ms >= interval_ms:
                    flush()
        finally:
            if read_task is not None and not read_task.done():
                read_task.cancel()

    def _emit(self, on_output: OutputCallback, chunk: OutputChunk) -> None:
        try:
            on_output(chunk)
        except Exception:  # a live-stream subscriber must never break the task (ADR-018)
            self.callback_errors += 1
