"""``ResultCollector`` — one ``execution_result`` per terminal plan (§3.9, §8.4, §12.5 ; ADR-009,
ADR-011, ADR-017, ADR-029).

The collector is a pure mapping from persisted records to the protocol content:

- ``status`` is the plan status in lower case (ADR-009 §4) and ``stop_reason`` the plan's;
- ``results`` holds the tasks that ran (``COMPLETED`` / ``FAILED`` / ``TIMED_OUT``), the
  ``skipped_tasks`` / ``cancelled_tasks`` / ``interrupted_tasks`` lists hold ``{task_id, reason}``
  references (ADR-009 §5) — all in **plan declaration order** (``order_index``, ADR-017) whatever
  the order of completion or of the inputs;
- a ``cmd`` task result carries the decoded kept output and the truncation metadata of its
  :class:`~agentic_local_app.execution.payload_guard.TruncatedOutput`; a ``chunk_request`` result
  carries the served :class:`~agentic_local_app.execution.payload_guard.ChunkResult`;
- every entry says plainly what became of its command (ADR-029 §4): ``execution`` is ``ran``,
  ``not_started``, ``timed_out`` or ``stopped``, and ``failure_is_verdict`` marks the non-zero exit
  of a recognised program — a result to interpret, not a task that went wrong;
- a command whose exit code is the **shell's** answer for a program it could not find or run
  (ADR-032: 127 / 126 on POSIX and PowerShell, 9009 from ``cmd``) keeps ``execution: "ran"`` — the
  shell did run — and carries ``reason`` ``COMMAND_NOT_FOUND`` / ``COMMAND_NOT_EXECUTABLE``, never
  ``failure_is_verdict``. Like the two fields above, that reason is **derived** at build time from
  the exit code and the dialect of the shell, never stored on the record;
- a command the dialect dictionary was consulted about carries ``translation`` (ADR-030 §4): what
  the model wrote, what actually ran, which rules fired — or, when nothing was rewritten, why. Like
  ``execution`` and ``failure_is_verdict``, it is **derived** from the record rather than stored:
  the translator is a pure function of the command, so re-asking it here gives the answer the
  runner acted on, and no column can drift from what ran;
- an ``INTERRUPTED`` plan has **no** ``execution_result`` (§8.4): ``build`` raises ``ValueError``.
"""

from __future__ import annotations

from agentic_local_app.domain.commands import VerdictPrograms
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.models import PlanRecord, TaskRecord
from agentic_local_app.domain.shell import ShellDialect, command_not_run_reason
from agentic_local_app.domain.states import OutputStream, PlanState, TaskState, TaskType
from agentic_local_app.execution.payload_guard import ChunkResult, TruncatedOutput, decode_output
from agentic_local_app.protocol.messages import (
    ExecutionResultContent,
    TaskExecution,
    TaskRef,
    TaskResult,
    TaskTranslation,
)

__all__ = ["ResultCollector"]

_RESULT_STATES = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMED_OUT})
_REFERENCE_LISTS: dict[TaskState, str] = {
    TaskState.SKIPPED: "skipped_tasks",
    TaskState.CANCELLED: "cancelled_tasks",
    TaskState.INTERRUPTED: "interrupted_tasks",
}
#: ADR-029 §4 — a task the application ended before its command could answer.
_STOPPED_STATES = frozenset({TaskState.CANCELLED, TaskState.INTERRUPTED})


class ResultCollector:
    """``verdict_programs`` is the recogniser of ADR-029 §2; an empty one marks no verdict, which
    is what an operator who cleared ``execution.verdict_programs`` asked for. ``translator`` is the
    dialect dictionary of ADR-030 §4, aimed at the shell that ran the commands; the default one
    translates nothing, so a result then carries no ``translation`` field at all.

    The translator's target is also what ADR-032 needs to read an exit code: the dialect of the
    shell that ran the commands, whose conventional codes for "no such program" and "cannot run it"
    are never a verdict. The default one (``unknown``) reads them with the POSIX convention."""

    def __init__(
        self,
        verdict_programs: VerdictPrograms | None = None,
        translator: ShellTranslator | None = None,
    ) -> None:
        self._verdict_programs = verdict_programs or VerdictPrograms()
        self._translator = translator or ShellTranslator(ShellDialect.UNKNOWN)

    @property
    def dialect(self) -> ShellDialect:
        """The dialect of the shell that ran the commands (the one the translator is aimed at)."""
        return self._translator.target

    def build(
        self,
        plan: PlanRecord,
        tasks: list[TaskRecord],
        outputs: dict[str, TruncatedOutput],
        chunk_results: dict[str, ChunkResult],
    ) -> ExecutionResultContent:
        """Assemble the content for a terminal, non-interrupted plan.

        ``outputs`` maps ``task_id`` to the truncated output of executed ``cmd`` tasks and
        ``chunk_results`` to the served chunk of executed ``chunk_request`` tasks. A task without an
        entry falls back to the metadata persisted on its record with empty text (e.g. a spawn
        failure). Raises ``ValueError`` for an interrupted or non-terminal plan, a task of another
        plan, or a task still non-terminal.
        """
        if plan.status is PlanState.INTERRUPTED:
            raise ValueError(
                f"plan {plan.plan_id} is INTERRUPTED: no execution_result is built (§8.4)"
            )
        if plan.status in (PlanState.PENDING, PlanState.RUNNING):
            raise ValueError(f"plan {plan.plan_id} is not terminal ({plan.status.value})")

        results: list[TaskResult] = []
        references: dict[str, list[TaskRef]] = {name: [] for name in _REFERENCE_LISTS.values()}
        for task in sorted(tasks, key=lambda t: t.order_index):
            if task.plan_id != plan.plan_id:
                raise ValueError(
                    f"task {task.task_id} belongs to plan {task.plan_id}, not {plan.plan_id}"
                )
            if task.status in _RESULT_STATES:
                results.append(
                    self._task_result(
                        task, outputs.get(task.task_id), chunk_results.get(task.task_id)
                    )
                )
            elif task.status in _REFERENCE_LISTS:
                reason = task.reason or task.status.protocol_value
                references[_REFERENCE_LISTS[task.status]].append(
                    TaskRef(
                        task_id=task.task_id,
                        reason=reason,
                        execution="stopped" if task.status in _STOPPED_STATES else "not_started",
                    )
                )
            else:
                raise ValueError(
                    f"task {task.task_id} is still {task.status.value} in terminal plan {plan.plan_id}"
                )

        return ExecutionResultContent(
            plan_id=plan.plan_id,
            status=plan.status.protocol_value,
            results=results,
            skipped_tasks=references["skipped_tasks"],
            cancelled_tasks=references["cancelled_tasks"],
            interrupted_tasks=references["interrupted_tasks"],
            stop_reason=plan.stop_reason,
        )

    # ---- per task ------------------------------------------------------------------------
    def _task_result(
        self, task: TaskRecord, output: TruncatedOutput | None, chunk: ChunkResult | None
    ) -> TaskResult:
        if task.type is TaskType.CHUNK_REQUEST:
            return self._chunk_result(task, chunk)
        return self._cmd_result(task, output)

    def _translation(self, task: TaskRecord) -> TaskTranslation | None:
        """ADR-030 §4, **derived** from the command exactly as the plan runner derived it.

        The translator is pure and the command is stored verbatim, so asking it again here answers
        what the runner acted on a moment earlier — the ``execution_result`` is built at the end of
        the very run that executed the commands, then persisted as a message and never rebuilt.
        ``None`` — the ordinary case — means the dictionary was never consulted, and the result then
        carries no field at all. The durable proof of what ran is the hash-chained
        ``task.state_changed`` event of the ``RUNNING`` transition, not a column.
        """
        decided = self._translator.translate(task.cmd)
        if decided is None:
            return None
        return TaskTranslation(
            status="translated" if decided.translated else "unchanged",
            from_dialect=decided.source.value,
            to_dialect=decided.target.value,
            original_cmd=decided.original,
            executed_cmd=decided.executed,
            rules=list(decided.rules),
            reason=decided.reason,
        )

    @staticmethod
    def _execution(task: TaskRecord) -> TaskExecution:
        """ADR-029 §4, for a ``cmd`` task that reached a result state: a timeout is a timeout, an
        absent ``exit_code`` means no command ever ran (a spawn error, or an executor that could
        not carry it out), and everything else ran to the end."""
        if task.timed_out or task.status is TaskState.TIMED_OUT:
            return "timed_out"
        if task.exit_code is None:
            return "not_started"
        return "ran"

    def _cmd_result(self, task: TaskRecord, output: TruncatedOutput | None) -> TaskResult:
        if output is not None:
            stdout, stderr = decode_output(output.stdout_kept), decode_output(output.stderr_kept)
            truncated = output.truncated
            original_size_bytes: int | None = output.original_size_bytes
            stdout_total: int | None = output.stdout_total
            stderr_total: int | None = output.stderr_total
            stdout_range: tuple[int, int] | None = output.stdout_range
            stderr_range: tuple[int, int] | None = output.stderr_range
        else:
            stdout, stderr = "", ""
            truncated = task.truncated
            original_size_bytes = task.original_size_bytes
            stdout_total, stderr_total = task.stdout_total, task.stderr_total
            stdout_range, stderr_range = task.stdout_range, task.stderr_range
        execution = self._execution(task)
        verdict = task.status is TaskState.FAILED and self._verdict_programs.is_verdict(
            task.cmd, task.exit_code, timed_out=task.timed_out, dialect=self.dialect
        )
        return TaskResult(
            task_id=task.task_id,
            status=task.status.protocol_value,
            execution=execution,
            exit_code=task.exit_code,
            failure_is_verdict=True if verdict else None,
            translation=self._translation(task),
            stdout=stdout,
            stderr=stderr,
            truncated=truncated,
            original_size_bytes=original_size_bytes,
            stdout_total=stdout_total,
            stderr_total=stderr_total,
            stdout_range=stdout_range,
            stderr_range=stderr_range,
            max_output_bytes_applied=task.max_output_bytes_applied,
            timed_out=task.timed_out,
            timeout_ms_applied=task.timeout_ms_applied,
            duration_ms=task.duration_ms,
            reason=self._reason(task, execution),
        )

    def _reason(self, task: TaskRecord, execution: TaskExecution) -> str | None:
        """The stored reason (``SPAWN_FAILED``, an executor error code…), or — ADR-032 — the one
        derived from an exit code that is the shell's own answer for a program it could not find
        or run. Only a command that ran to its end has such a code; nothing is ever persisted."""
        if task.reason is not None or execution != "ran":
            return task.reason
        return command_not_run_reason(self.dialect, task.exit_code)

    @staticmethod
    def _chunk_result(task: TaskRecord, chunk: ChunkResult | None) -> TaskResult:
        # a chunk_request runs no command: the read is local and always happened, served or refused
        return TaskResult(
            task_id=task.task_id,
            status=task.status.protocol_value,
            execution="ran",
            exit_code=None,
            duration_ms=task.duration_ms,
            reason=task.reason,
            ref_task_id=task.ref_task_id,
            stream=task.stream or OutputStream.STDOUT,
            range=chunk.range if chunk is not None else None,
            total=chunk.total if chunk is not None else None,
            eof=chunk.eof if chunk is not None else None,
            data=decode_output(chunk.data) if chunk is not None else None,
        )
