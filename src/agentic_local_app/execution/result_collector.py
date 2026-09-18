"""``ResultCollector`` — one ``execution_result`` per terminal plan (§3.9, §8.4, §12.5 ; ADR-009,
ADR-011, ADR-017).

The collector is a pure mapping from persisted records to the protocol content:

- ``status`` is the plan status in lower case (ADR-009 §4) and ``stop_reason`` the plan's;
- ``results`` holds the tasks that ran (``COMPLETED`` / ``FAILED`` / ``TIMED_OUT``), the
  ``skipped_tasks`` / ``cancelled_tasks`` / ``interrupted_tasks`` lists hold ``{task_id, reason}``
  references (ADR-009 §5) — all in **plan declaration order** (``order_index``, ADR-017) whatever
  the order of completion or of the inputs;
- a ``cmd`` task result carries the decoded kept output and the truncation metadata of its
  :class:`~agentic_local_app.execution.payload_guard.TruncatedOutput`; a ``chunk_request`` result
  carries the served :class:`~agentic_local_app.execution.payload_guard.ChunkResult`;
- an ``INTERRUPTED`` plan has **no** ``execution_result`` (§8.4): ``build`` raises ``ValueError``.
"""

from __future__ import annotations

from agentic_local_app.domain.models import PlanRecord, TaskRecord
from agentic_local_app.domain.states import OutputStream, PlanState, TaskState, TaskType
from agentic_local_app.execution.payload_guard import ChunkResult, TruncatedOutput, decode_output
from agentic_local_app.protocol.messages import ExecutionResultContent, TaskRef, TaskResult

__all__ = ["ResultCollector"]

_RESULT_STATES = frozenset({TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMED_OUT})
_REFERENCE_LISTS: dict[TaskState, str] = {
    TaskState.SKIPPED: "skipped_tasks",
    TaskState.CANCELLED: "cancelled_tasks",
    TaskState.INTERRUPTED: "interrupted_tasks",
}


class ResultCollector:
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
                    TaskRef(task_id=task.task_id, reason=reason)
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

    @staticmethod
    def _cmd_result(task: TaskRecord, output: TruncatedOutput | None) -> TaskResult:
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
        return TaskResult(
            task_id=task.task_id,
            status=task.status.protocol_value,
            exit_code=task.exit_code,
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
            reason=task.reason,
        )

    @staticmethod
    def _chunk_result(task: TaskRecord, chunk: ChunkResult | None) -> TaskResult:
        return TaskResult(
            task_id=task.task_id,
            status=task.status.protocol_value,
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
