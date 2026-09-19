"""``PlanRunner`` — execute one plan exactly as received (§3.7, §8 ; ADR-003, ADR-006, ADR-008,
ADR-009, ADR-011, ADR-012 §3, ADR-015, ADR-016, ADR-017, ADR-018, ADR-019 §1, ADR-026, ADR-029).

One runner, one ``run`` per plan. The runner owns every plan and task transition (module map §2,
rule 5): each one is validated against the tables of :mod:`agentic_local_app.domain.transitions`,
**persisted** (the task record together with its blobs, in one store transaction) and only then
**published** on the bus (ADR-015). Nothing is hard-coded outside the tables.

Scheduling (§2.4, §8.2 amended by the ADRs), the same loop for both policies:

- ``sequential`` is one worker in strict ``order_index`` order; ``parallel`` runs up to
  ``max_parallel_workers`` tasks, chosen among the **ready** tasks (``PENDING``, every dependency
  ``COMPLETED``, ``resource_lock`` free) in declaration order — deterministic launch order
  (ADR-017), only the completion order varies;
- a task whose dependencies are not yet complete is ``WAITING_DEPENDENCY`` (parallel plans) until
  they are, and ``SKIPPED`` (``dependency_failed:<id>`` / ``dependency_skipped:<id>``, transitively)
  as soon as one of them ends otherwise than ``COMPLETED``, even when the plan continues (ADR-009);
- ``resource_lock`` is enforced by the scheduler itself: a task whose key is held stays ``PENDING``
  and never consumes a worker while it waits;
- before every launch the runner looks at the interruption token, then at the session duration
  budget: after the deadline no new task starts, the running ones finish normally, the rest is
  ``SKIPPED`` (``budget_exceeded``) and the plan ``FAILED`` (ADR-012 §3);
- a non-zero exit from one of the ``execution.verdict_programs`` — a compiler, a build tool, a test
  runner, a linter that **ran** — does not stop the plan (ADR-029 §2): its answer is the result the
  plan exists to produce. An explicit ``critical`` / ``stop_plan_on_failure`` on that task still
  stops it, a spawn error or a timeout is untouched, and the dependants are skipped as always;
- after every task the stop conditions of §8.3 / ADR-009 apply; a stop in parallel mode cancels
  the running tasks (``plan_stopped``) and drains them for ``cancel_drain_timeout_ms`` — the
  executor does the two-phase termination — before the plan goes terminal (§17.2); an interruption
  does the same with ``user_interrupt`` and ``interrupt_drain_timeout_ms`` and produces **no**
  ``execution_result`` (§8.4). A task that still has not returned when its drain elapses is
  terminated by cancelling its coroutine (the executor kills hard) and recorded without output;
- a command that fails, times out or cannot be started is a **result** of the executor; a defect
  of the executor itself (``TaskExecutionError``) is a ``FAILED`` task carrying the error code, with
  a ``FailureRecord`` when a failure manager is given (ADR-008 §4) — never a plan left ``RUNNING``.

Before the first write, ``run`` checks that the plan and every task can start, that the tasks
belong to the plan and that the dependency graph can be scheduled (no unknown dependency, no cycle,
only backward edges in a sequential plan): the adapter guarantees it for plans of the model
(ADR-007), hand-built records get the same guarantee here.

Time and identifiers are injected (``Clock``, ``IdGenerator``, ADR-017): durations are monotonic,
timestamps come from ``clock.now()``, blob identifiers from ``ids.blob_id()``.

An optional :class:`~agentic_local_app.execution.scratch.ScratchManager` (ADR-026) adds the working
space of the session to the environment of every ``cmd`` task; without one the environment overlay
stays empty and nothing changes.

The dialect dictionary of ADR-030 §4 is consulted **here**, at the ``RUNNING`` transition and
nowhere else: the runner is the only component that both knows the command and decides what runs.
What it decided travels in the ``task.state_changed`` payload of that transition — the command that
really ran, the rules that fired, or the reason nothing was rewritten — so the **hash-chained audit
log** carries the durable proof of what executed. Nothing is added to the persisted records: the
``execution_result`` re-derives the same decision from the same ``cmd`` (:class:`ResultCollector`),
exactly as ADR-029 §4 derives ``execution`` and ``failure_is_verdict`` rather than storing them.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol, TypeVar

from agentic_local_app.config import AppConfig, ExecutionSection
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.commands import VerdictPrograms
from agentic_local_app.domain.dialects import CommandTranslation, ShellTranslator
from agentic_local_app.domain.errors import GenericSystemError, NormalizedError, TaskExecutionError
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    BlobRecord,
    FailureRecord,
    PlanRecord,
    Record,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ExecutionPolicy,
    OutputStream,
    PlanState,
    TaskState,
    TaskType,
)
from agentic_local_app.domain.transitions import (
    FAILED_TASK_STATES,
    PLAN_TRANSITIONS,
    TASK_TRANSITIONS,
    TERMINAL_PLAN_STATES,
    TERMINAL_TASK_STATES,
    assert_transition,
)
from agentic_local_app.execution.executor import (
    CancellationToken,
    CommandExecutor,
    CommandSpec,
    OutputChunk,
    RawExecution,
)
from agentic_local_app.execution.payload_guard import (
    ChunkError,
    ChunkResult,
    PayloadGuard,
    TruncatedOutput,
)
from agentic_local_app.execution.platform import default_translator
from agentic_local_app.execution.result_collector import ResultCollector
from agentic_local_app.execution.scratch import ScratchManager
from agentic_local_app.observability.event_bus import EventBus
from agentic_local_app.persistence.interface import ConversationStore
from agentic_local_app.protocol.messages import ExecutionResultContent

__all__ = [
    "BUDGET_DURATION_STOP_REASON",
    "BUDGET_EXCEEDED_REASON",
    "INTERRUPT_REASON",
    "PLAN_ENTITY",
    "PLAN_STOPPED_REASON",
    "SPAWN_FAILED_REASON",
    "TASK_ENTITY",
    "FailureRecorder",
    "PlanOutcome",
    "PlanRunner",
]

#: ``entity`` values carried by :class:`~agentic_local_app.domain.errors.InvalidTransitionError`.
PLAN_ENTITY = "plan"
TASK_ENTITY = "task"

#: Reasons (ADR-009 §5, ADR-012 §3, §8.4). ``plan_stopped`` is the token reason of a stop condition;
#: the records then carry ``plan_stopped:<stop_reason>``.
INTERRUPT_REASON = "user_interrupt"
PLAN_STOPPED_REASON = "plan_stopped"
BUDGET_EXCEEDED_REASON = "budget_exceeded"
BUDGET_DURATION_STOP_REASON = "budget_exceeded:max_total_duration_ms"
SPAWN_FAILED_REASON = "SPAWN_FAILED"

_STOP_ON_SUCCESS = "stop_plan_on_success"
_STOP_LABELS = ("critical_task_failed", "stop_plan_on_failure", "task_failed")
_DEPENDENCY_FAILED = "dependency_failed"
_DEPENDENCY_SKIPPED = "dependency_skipped"
_SCHEDULABLE_STATES = frozenset({TaskState.PENDING, TaskState.WAITING_DEPENDENCY})

R = TypeVar("R", bound=Record)


class FailureRecorder(Protocol):
    """What the runner needs from the ``FailureManager`` (structural, keeps ``execution`` free of
    any import of ``resilience``): persist a failure then publish ``failure.recorded``."""

    def record(
        self,
        error: NormalizedError,
        *,
        session_id: str,
        conversation_id: str | None = None,
        plan_id: str | None = None,
        task_id: str | None = None,
    ) -> FailureRecord: ...


@dataclass(frozen=True)
class PlanOutcome:
    """What ``run`` returns: the terminal plan, its tasks in plan order, and the single
    ``execution_result`` of the plan — ``None`` when the plan was interrupted (§8.4)."""

    plan: PlanRecord
    tasks: list[TaskRecord]
    execution_result: ExecutionResultContent | None
    interrupted: bool
    budget_exceeded: bool
    stop_reason: str | None


def _apply(record: R, changes: dict[str, Any]) -> R:
    """A new, fully validated record (``model_copy`` validates nothing)."""
    data = record.model_dump()
    data.update(changes)
    return type(record).model_validate(data)


@dataclass(frozen=True)
class _Change:
    """One task transition to persist and publish."""

    task_id: str
    to: TaskState
    reason: str | None = None
    fields: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class _Ended:
    """What a worker coroutine returns: the executor's result for a ``cmd`` task, the served
    chunk (or its error) for a ``chunk_request``, the normalized error of an executor defect
    (``TaskExecutionError``, ADR-008 §4); all ``None`` for a forced termination."""

    raw: RawExecution | None = None
    chunk: ChunkResult | ChunkError | None = None
    error: NormalizedError | None = None


class PlanRunner:
    def __init__(
        self,
        store: ConversationStore,
        bus: EventBus,
        executor: CommandExecutor,
        payload_guard: PayloadGuard,
        clock: Clock,
        ids: IdGenerator,
        config: AppConfig,
        *,
        failure_manager: FailureRecorder | None = None,
        result_collector: ResultCollector | None = None,
        scratch: ScratchManager | None = None,
        translator: ShellTranslator | None = None,
    ) -> None:
        self.store = store
        self.bus = bus
        self.executor = executor
        self.payload_guard = payload_guard
        self.clock = clock
        self.ids = ids
        self.config = config
        self.failure_manager = failure_manager
        #: ADR-029 §2: the programs whose non-zero exit is a verdict, read once per runner.
        self.verdict_programs = VerdictPrograms(config.execution.verdict_programs)
        #: ADR-026: the working spaces. ``None`` exports nothing, as before the ADR.
        self.scratch = scratch
        #: ADR-030 §4: the dialect dictionary, aimed at the shell this machine actually runs.
        #: Injected in tests so that no plan depends on the machine the suite runs on.
        self.translator = (
            translator if translator is not None else default_translator(config.execution)
        )
        self.result_collector = result_collector or ResultCollector(
            self.verdict_programs, self.translator
        )

    async def run(
        self,
        plan: PlanRecord,
        tasks: list[TaskRecord],
        session: SessionRecord,
        *,
        interrupt: CancellationToken,
    ) -> PlanOutcome:
        """Run a ``PENDING`` plan to a terminal state.

        Raises :class:`~agentic_local_app.domain.errors.InvalidTransitionError` when the plan or a
        task cannot start (not ``PENDING``), ``ValueError`` for tasks that do not belong to the
        plan, and lets a ``PersistenceError`` propagate: the transition it prevented is neither
        applied in memory nor published (ADR-015).
        """
        return await _PlanExecution(self, plan, tasks, session, interrupt).run()


class _PlanExecution:
    """The mutable state of one ``run``: current records, tokens, futures, locks, outputs."""

    def __init__(
        self,
        runner: PlanRunner,
        plan: PlanRecord,
        tasks: Sequence[TaskRecord],
        session: SessionRecord,
        interrupt: CancellationToken,
    ) -> None:
        self._runner = runner
        self._plan = plan
        self._session = session
        self._interrupt = interrupt
        ordered = sorted(tasks, key=lambda t: t.order_index)
        self._order: list[str] = [t.task_id for t in ordered]
        self._tasks: dict[str, TaskRecord] = {t.task_id: t for t in ordered}
        self._workers = (
            1
            if plan.execution_policy is ExecutionPolicy.SEQUENTIAL
            else max(1, plan.max_parallel_workers)
        )
        self._tokens: dict[str, CancellationToken] = {}
        self._running: dict[str, asyncio.Task[_Ended]] = {}
        self._started_monotonic: dict[str, int] = {}
        self._held_locks: set[str] = set()
        self._outputs: dict[str, TruncatedOutput] = {}
        self._chunks: dict[str, ChunkResult] = {}
        #: ADR-030 §4: what the dialect dictionary decided, for the length of this run only —
        #: it names the command handed to the executor and what the audit event reports.
        self._translations: dict[str, CommandTranslation] = {}
        self._stop_reason: str | None = None
        self._interrupting = False
        self._budget_exceeded = False

    # ---- entry point ---------------------------------------------------------------------
    async def run(self) -> PlanOutcome:
        self._validate()
        waiter: asyncio.Future[Any] = asyncio.ensure_future(self._interrupt.wait())
        try:
            return await self._loop(waiter)
        finally:
            waiter.cancel()
            leftovers = [f for f in self._running.values() if not f.done()]
            for future in leftovers:
                future.cancel()
            await asyncio.gather(waiter, *leftovers, return_exceptions=True)

    def _validate(self) -> None:
        """Everything that must hold before the first write: the plan and every task can start,
        the tasks belong to the plan, and the dependency graph can be scheduled."""
        assert_transition(
            PLAN_TRANSITIONS, self._plan.status, PlanState.RUNNING, entity=PLAN_ENTITY
        )
        if len(self._tasks) != len(self._order):
            raise ValueError(f"plan {self._plan.plan_id}: duplicate task identifiers")
        for task in self._tasks.values():
            if task.plan_id != self._plan.plan_id:
                raise ValueError(
                    f"task {task.task_id} belongs to plan {task.plan_id}, not {self._plan.plan_id}"
                )
            assert_transition(TASK_TRANSITIONS, task.status, TaskState.RUNNING, entity=TASK_ENTITY)
        self._validate_dependencies()

    def _validate_dependencies(self) -> None:
        """The adapter already guarantees it for plans of the model (ADR-007); hand-built records
        get the same guarantee here rather than a scheduler that can never be satisfied: every
        dependency names a task of the plan, a sequential plan only depends backwards, and the
        graph has no cycle (Kahn's algorithm over the declaration order)."""
        plan_id = self._plan.plan_id
        sequential = self._plan.execution_policy is ExecutionPolicy.SEQUENTIAL
        position = {task_id: index for index, task_id in enumerate(self._order)}
        for task_id in self._order:
            for dependency in self._tasks[task_id].depends_on:
                if dependency not in self._tasks:
                    raise ValueError(
                        f"task {task_id} depends on unknown task {dependency} (plan {plan_id})"
                    )
                if sequential and position[dependency] >= position[task_id]:
                    raise ValueError(
                        f"task {task_id} depends on {dependency}, which does not precede it in "
                        f"sequential plan {plan_id}"
                    )
        remaining = {task_id: set(self._tasks[task_id].depends_on) for task_id in self._order}
        ready = deque(task_id for task_id in self._order if not remaining[task_id])
        scheduled = 0
        while ready:
            current = ready.popleft()
            scheduled += 1
            for task_id in self._order:
                dependencies = remaining[task_id]
                if current in dependencies:
                    dependencies.discard(current)
                    if not dependencies:
                        ready.append(task_id)
        if scheduled != len(self._order):
            stuck = [task_id for task_id in self._order if remaining[task_id]]
            raise ValueError(f"plan {plan_id}: dependency cycle among tasks {stuck}")

    async def _loop(self, waiter: asyncio.Future[Any]) -> PlanOutcome:
        started = False
        while True:
            if self._interrupt.is_cancelled:
                return await self._interrupted()
            if not self._budget_exceeded and self._deadline_passed():
                self._budget_exceeded = True
            if not self._budget_exceeded:
                if not started:
                    self._start_plan()
                    started = True
                self._launch_ready()
            if not self._running:
                break
            pending: set[asyncio.Future[Any]] = {waiter, *self._running.values()}
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            if self._interrupt.is_cancelled:
                continue  # the top of the loop drains everything, finished tasks included
            for task_id in self._finished(done):
                ended = self._running.pop(task_id).result()
                state = self._finish(task_id, ended, propagate=not self._budget_exceeded)
                if self._budget_exceeded:
                    continue
                stop = self._stop_condition(task_id, state)
                if stop is not None:
                    return await self._stopped(*stop)
        if self._budget_exceeded:
            return self._budget_failed()
        return self._completed()

    # ---- plan-level steps ----------------------------------------------------------------
    def _start_plan(self) -> None:
        self._transition_plan(PlanState.RUNNING)
        if self._plan.execution_policy is ExecutionPolicy.PARALLEL:
            self._apply_changes(
                _Change(task_id, TaskState.WAITING_DEPENDENCY)
                for task_id in self._order
                if self._tasks[task_id].depends_on
            )

    def _completed(self) -> PlanOutcome:
        self._require_all_terminal()
        self._transition_plan(PlanState.COMPLETED)
        return self._outcome(self._build_result())

    async def _stopped(self, target: PlanState, stop_reason: str) -> PlanOutcome:
        self._stop_reason = stop_reason
        await self._drain(PLAN_STOPPED_REASON, self._config.cancel_drain_timeout_ms)
        self._mark_remaining(TaskState.SKIPPED, f"{PLAN_STOPPED_REASON}:{stop_reason}")
        self._transition_plan(target, stop_reason=stop_reason)
        return self._outcome(self._build_result())

    async def _interrupted(self) -> PlanOutcome:
        self._interrupting = True
        await self._drain(INTERRUPT_REASON, self._config.interrupt_drain_timeout_ms)
        self._mark_remaining(TaskState.INTERRUPTED, INTERRUPT_REASON)
        self._transition_plan(PlanState.INTERRUPTED, stop_reason=INTERRUPT_REASON)
        return self._outcome(None, interrupted=True)

    def _budget_failed(self) -> PlanOutcome:
        self._mark_remaining(TaskState.SKIPPED, BUDGET_EXCEEDED_REASON)
        self._transition_plan(PlanState.FAILED, stop_reason=BUDGET_DURATION_STOP_REASON)
        return self._outcome(self._build_result(), budget_exceeded=True)

    def _outcome(
        self,
        result: ExecutionResultContent | None,
        *,
        interrupted: bool = False,
        budget_exceeded: bool = False,
    ) -> PlanOutcome:
        return PlanOutcome(
            plan=self._plan,
            tasks=self._ordered_tasks(),
            execution_result=result,
            interrupted=interrupted,
            budget_exceeded=budget_exceeded,
            stop_reason=self._plan.stop_reason,
        )

    def _build_result(self) -> ExecutionResultContent:
        content = self._runner.result_collector.build(
            self._plan, self._ordered_tasks(), self._outputs, self._chunks
        )
        return self._guard.fit_message(content, self._runner.config.payload.max_message_bytes)

    # ---- scheduling ----------------------------------------------------------------------
    def _deadline_passed(self) -> bool:
        """ADR-012 §3: ``now - session.started_at >= max_total_duration_ms`` (no ``started_at``,
        no check)."""
        started = self._session.started_at
        if started is None:
            return False
        limit = timedelta(milliseconds=self._session.budget.max_total_duration_ms)
        return self._clock.now() - started >= limit

    def _launch_ready(self) -> None:
        for task_id in self._ready():
            self._launch(task_id)
        if not self._running and any(
            t.status not in TERMINAL_TASK_STATES for t in self._tasks.values()
        ):
            raise GenericSystemError(
                "PLAN_SCHEDULER_STALLED",
                "PlanRunner",
                plan_id=self._plan.plan_id,
                tasks={
                    t.task_id: t.status.value
                    for t in self._tasks.values()
                    if t.status not in TERMINAL_TASK_STATES
                },
            )

    def _ready(self) -> list[str]:
        """Launchable tasks in declaration order, up to the free workers: ``PENDING``, every
        dependency ``COMPLETED``, lock free (also among the tasks picked in this round). A
        sequential plan never looks past its first non-terminal task."""
        sequential = self._plan.execution_policy is ExecutionPolicy.SEQUENTIAL
        picked: list[str] = []
        picked_locks: set[str] = set()
        slots = self._workers - len(self._running)
        for task_id in self._order:
            if slots <= 0:
                break
            task = self._tasks[task_id]
            if task.status in TERMINAL_TASK_STATES:
                continue
            lock = task.resource_lock
            launchable = (
                task.status is TaskState.PENDING
                and self._dependencies_completed(task)
                and (lock is None or (lock not in self._held_locks and lock not in picked_locks))
            )
            if launchable:
                picked.append(task_id)
                slots -= 1
                if lock is not None:
                    picked_locks.add(lock)
            elif sequential:
                break
        return picked

    def _dependencies_completed(self, task: TaskRecord) -> bool:
        return all(self._tasks[dep].status is TaskState.COMPLETED for dep in task.depends_on)

    def _launch(self, task_id: str) -> None:
        """§8.2 steps 3–5: hold the lock, ``PENDING -> RUNNING`` persisted then published, then
        hand the task to a worker coroutine.

        The dialect dictionary is consulted here (ADR-030 §4) and what it decided travels with the
        transition: it is written in the same transaction, so a command can never run without its
        record already saying which one ran.
        """
        task = self._tasks[task_id]
        if task.resource_lock is not None:
            self._held_locks.add(task.resource_lock)
        token = CancellationToken()
        self._tokens[task_id] = token
        self._decide_translation(task)
        self._apply_changes(
            [
                _Change(
                    task_id,
                    TaskState.RUNNING,
                    fields={"started_at": self._clock.now(), "attempt_count": 1},
                )
            ]
        )
        self._started_monotonic[task_id] = self._clock.monotonic_ms()
        self._running[task_id] = asyncio.ensure_future(self._execute(task_id, token))

    def _decide_translation(self, task: TaskRecord) -> None:
        """Consult the dialect dictionary for this command, once, before it runs (ADR-030 §4).

        The decision is kept for the length of the run: it names the command the executor receives
        and it is published with the ``RUNNING`` transition, which the audit log chains. Nothing is
        persisted on the record — the ``execution_result`` re-derives the same answer from the same
        ``cmd``. No entry — the ordinary case — means the dictionary was not even consulted: the
        command is dialect-neutral, the shell already speaks its dialect, or translation is off.
        """
        if task.type is not TaskType.CMD:
            return
        decided = self._runner.translator.translate(task.cmd)
        if decided is not None:
            self._translations[task.task_id] = decided

    async def _execute(self, task_id: str, token: CancellationToken) -> _Ended:
        task = self._tasks[task_id]
        if task.type is TaskType.CHUNK_REQUEST:
            return _Ended(chunk=self._serve_chunk(task))
        # ADR-026: the working space travels as environment variables, not as ``cwd`` — the
        # commands are about the user's project, the folder is offered, never imposed.
        scratch = self._runner.scratch
        env = scratch.environment(task.session_id) if scratch is not None else {}
        decided = self._translations.get(task_id)
        spec = CommandSpec(
            task_id=task_id,
            cmd=decided.executed if decided is not None else (task.cmd or ""),
            timeout_ms=self._timeout_ms(task),
            cwd=self._config.cwd,
            shell=self._config.shell or None,
            env=env or None,
        )
        try:
            raw = await self._runner.executor.execute(
                spec,
                cancel=token,
                on_output=lambda chunk: self._publish_output(task_id, chunk),
                on_spawn=lambda pid, pgid: self._persist_pid(task_id, pid, pgid),
            )
        except TaskExecutionError as exc:
            # a defect of the executor itself (ADR-008 §4): a FAILED task, not a crashed plan
            return _Ended(error=exc.error)
        return _Ended(raw=raw)

    def _serve_chunk(self, task: TaskRecord) -> ChunkResult | ChunkError:
        return self._guard.serve_chunk(
            self._store,
            task.session_id,
            task.ref_task_id or "",
            task.stream or OutputStream.STDOUT,
            task.byte_offset or 0,
            task.max_bytes if task.max_bytes is not None else self._budget(task),
        )

    def _finished(self, done: set[asyncio.Future[Any]]) -> list[str]:
        """Task identifiers whose worker completed, in declaration order (deterministic)."""
        return [task_id for task_id in self._order if self._running.get(task_id) in done]

    # ---- ending a task (§8.2 steps 6–10) --------------------------------------------------
    def _finish(self, task_id: str, ended: _Ended, *, propagate: bool) -> TaskState:
        """Blobs, truncation, terminal transition (persisted then published), lock release,
        then — while the plan runs normally — dependency propagation."""
        task = self._tasks[task_id]
        now = self._clock.now()
        duration = self._clock.monotonic_ms() - self._started_monotonic[task_id]
        blobs: list[BlobRecord] = []
        fields: dict[str, Any] = {"ended_at": now, "duration_ms": duration}
        failure: NormalizedError | None = None
        if ended.chunk is not None:
            to, reason = self._chunk_state(task_id, ended.chunk)
        elif ended.raw is not None:
            raw = ended.raw
            to, reason = self._raw_state(raw)
            blobs = self._blobs(task, raw, now)
            fields.update(self._output_fields(task, raw, blobs))
            if raw.spawn_error is not None:
                failure = self._spawn_failure(task, raw.spawn_error)
        elif ended.error is not None:
            to, reason, failure = TaskState.FAILED, ended.error.error_code, ended.error
        else:
            to, reason = self._cancelled_state()
        self._apply_changes([_Change(task_id, to, reason, fields)], blobs=blobs)
        if task.resource_lock is not None:
            self._held_locks.discard(task.resource_lock)
        if failure is not None:
            self._record_failure(self._tasks[task_id], failure)
        if propagate:
            if to is TaskState.COMPLETED:
                self._release_dependants()
            else:
                self._skip_dependants(task_id, to)
        return to

    def _chunk_state(
        self, task_id: str, chunk: ChunkResult | ChunkError
    ) -> tuple[TaskState, str | None]:
        if isinstance(chunk, ChunkResult):
            self._chunks[task_id] = chunk
            return TaskState.COMPLETED, None
        return TaskState.FAILED, chunk.code

    def _raw_state(self, raw: RawExecution) -> tuple[TaskState, str | None]:
        outcome = raw.outcome
        if outcome is TaskState.CANCELLED:
            return self._cancelled_state()
        if outcome is TaskState.FAILED and raw.spawn_error is not None:
            return TaskState.FAILED, SPAWN_FAILED_REASON
        return outcome, None

    def _cancelled_state(self) -> tuple[TaskState, str]:
        """A task ended by the runner's own signal: ``INTERRUPTED`` during an interruption,
        ``CANCELLED`` (``plan_stopped:<stop_reason>``) during a stop condition."""
        if self._interrupting:
            return TaskState.INTERRUPTED, INTERRUPT_REASON
        if self._stop_reason is not None:
            return TaskState.CANCELLED, f"{PLAN_STOPPED_REASON}:{self._stop_reason}"
        return TaskState.CANCELLED, PLAN_STOPPED_REASON

    def _blobs(self, task: TaskRecord, raw: RawExecution, now: datetime) -> list[BlobRecord]:
        """One blob per stream, even empty (ADR-019 §1), never truncated (ADR-003)."""
        return [
            BlobRecord(
                blob_id=self._runner.ids.blob_id(),
                session_id=task.session_id,
                task_id=task.task_id,
                blob_type=stream,
                content=content,
                size_bytes=len(content),
                created_at=now,
            )
            for stream, content in (
                (OutputStream.STDOUT, raw.stdout),
                (OutputStream.STDERR, raw.stderr),
            )
        ]

    def _output_fields(
        self, task: TaskRecord, raw: RawExecution, blobs: list[BlobRecord]
    ) -> dict[str, Any]:
        truncated = self._guard.apply(raw.stdout, raw.stderr, self._budget(task))
        self._outputs[task.task_id] = truncated
        stdout_blob, stderr_blob = blobs
        return {
            "exit_code": raw.exit_code,
            "timed_out": raw.timed_out,
            "pid": raw.pid if raw.pid is not None else task.pid,
            "process_group_id": (
                raw.process_group_id if raw.process_group_id is not None else task.process_group_id
            ),
            "stdout_ref": stdout_blob.blob_id,
            "stderr_ref": stderr_blob.blob_id,
            "truncated": truncated.truncated,
            "original_size_bytes": truncated.original_size_bytes,
            "stdout_total": truncated.stdout_total,
            "stderr_total": truncated.stderr_total,
            "stdout_range": truncated.stdout_range,
            "stderr_range": truncated.stderr_range,
        }

    @staticmethod
    def _spawn_failure(task: TaskRecord, spawn_error: str) -> NormalizedError:
        """ADR-008 §4: a command that could not be started is a ``TASK_EXECUTION_ERROR``."""
        return TaskExecutionError(
            SPAWN_FAILED_REASON, task_id=task.task_id, cmd=task.cmd, error=spawn_error
        ).error

    def _record_failure(self, task: TaskRecord, error: NormalizedError) -> None:
        """A ``FailureRecord`` (then ``failure.recorded``) when a failure manager was given; the
        task record already carries the code as its ``reason`` in any case."""
        manager = self._runner.failure_manager
        if manager is None:
            return
        manager.record(
            error,
            session_id=task.session_id,
            conversation_id=task.conversation_id,
            plan_id=task.plan_id,
            task_id=task.task_id,
        )

    # ---- dependencies (§2.4, ADR-009 §5) ---------------------------------------------------
    def _release_dependants(self) -> None:
        """``WAITING_DEPENDENCY -> PENDING`` for every task whose dependencies are all complete."""
        self._apply_changes(
            _Change(task_id, TaskState.PENDING)
            for task_id in self._order
            if self._tasks[task_id].status is TaskState.WAITING_DEPENDENCY
            and self._dependencies_completed(self._tasks[task_id])
        )

    def _skip_dependants(self, root_id: str, root_state: TaskState) -> None:
        """Transitive ``SKIPPED`` of the tasks depending on a task that did not complete."""
        changes: list[_Change] = []
        skipped: set[str] = set()
        worklist: deque[tuple[str, TaskState]] = deque([(root_id, root_state)])
        while worklist:
            dependency, state = worklist.popleft()
            label = _DEPENDENCY_FAILED if state in FAILED_TASK_STATES else _DEPENDENCY_SKIPPED
            for task_id in self._order:
                task = self._tasks[task_id]
                if task_id in skipped or task.status not in _SCHEDULABLE_STATES:
                    continue
                if dependency in task.depends_on:
                    changes.append(_Change(task_id, TaskState.SKIPPED, f"{label}:{dependency}"))
                    skipped.add(task_id)
                    worklist.append((task_id, TaskState.SKIPPED))
        self._apply_changes(changes)

    # ---- stop conditions and drains (§8.3, §8.4, ADR-003, ADR-009) -------------------------
    def _stop_condition(self, task_id: str, state: TaskState) -> tuple[PlanState, str] | None:
        task = self._tasks[task_id]
        if state is TaskState.COMPLETED and task.stop_plan_on_success:
            return PlanState.SHORT_CIRCUITED_ON_SUCCESS, f"{_STOP_ON_SUCCESS}:{task_id}"
        if state in FAILED_TASK_STATES and task.stops_plan_on_failure:
            if self._failure_is_verdict(task, state) and not (
                task.critical or task.stop_plan_on_failure
            ):
                return None
            critical, on_failure, plain = _STOP_LABELS
            label = (
                critical if task.critical else on_failure if task.stop_plan_on_failure else plain
            )
            return PlanState.STOPPED_ON_FAILURE, f"{label}:{task_id}"
        return None

    def _failure_is_verdict(self, task: TaskRecord, state: TaskState) -> bool:
        """ADR-029 §2: a recognised program that **ran** and answered with a non-zero exit code.

        Its answer is the result the plan was written to obtain, so the plan carries on reading it;
        only an explicit ``critical`` / ``stop_plan_on_failure`` still stops it. A command that
        could not be started or that timed out never produced an answer and is untouched by this.
        """
        return (
            state is TaskState.FAILED
            and task.type is TaskType.CMD
            and self._runner.verdict_programs.is_verdict(
                task.cmd, task.exit_code, timed_out=task.timed_out
            )
        )

    async def _drain(self, token_reason: str, timeout_ms: int) -> None:
        """Signal every running task, wait at most ``timeout_ms`` for the executor to come back,
        cancel the coroutines still pending (forced termination), then record every task in
        declaration order — a task that finished by itself keeps its real outcome."""
        futures = dict(self._running)
        for task_id, future in futures.items():
            if not future.done():
                self._tokens[task_id].cancel(token_reason)
        if futures:
            _, late = await asyncio.wait(set(futures.values()), timeout=timeout_ms / 1000)
            for future in late:
                future.cancel()
            if late:
                await asyncio.gather(*late, return_exceptions=True)
        for task_id in self._order:
            if task_id not in futures:
                continue
            settled = self._running.pop(task_id)
            ended = _Ended() if settled.cancelled() else settled.result()
            self._finish(task_id, ended, propagate=False)

    def _mark_remaining(self, to: TaskState, reason: str) -> None:
        self._apply_changes(
            _Change(task_id, to, reason)
            for task_id in self._order
            if self._tasks[task_id].status in _SCHEDULABLE_STATES
        )

    def _require_all_terminal(self) -> None:
        stuck = [t.task_id for t in self._tasks.values() if t.status not in TERMINAL_TASK_STATES]
        if stuck:
            raise GenericSystemError(
                "PLAN_TASKS_NOT_TERMINAL", "PlanRunner", plan_id=self._plan.plan_id, tasks=stuck
            )

    # ---- persistence then publication (ADR-015) --------------------------------------------
    def _transition_plan(self, to: PlanState, *, stop_reason: str | None = None) -> PlanRecord:
        current = self._plan
        assert_transition(PLAN_TRANSITIONS, current.status, to, entity=PLAN_ENTITY)
        now = self._clock.now()
        changes: dict[str, Any] = {"status": to, "updated_at": now, **self._counters()}
        if to is PlanState.RUNNING:
            changes["started_at"] = now
        if to in TERMINAL_PLAN_STATES:
            changes["ended_at"] = now
            changes["stop_reason"] = stop_reason
        plan = _apply(current, changes)
        with self._store.transaction():
            self._store.save_plan(plan)
        self._plan = plan
        payload = state_change_payload(current.status.value, to.value)
        if stop_reason is not None:
            payload["stop_reason"] = stop_reason
        self._publish(EventType.PLAN_STATE_CHANGED, now, task_id=None, payload=payload)
        return plan

    def _counters(self) -> dict[str, int]:
        statuses = [t.status for t in self._tasks.values()]
        return {
            "task_count": len(statuses),
            "completed_task_count": statuses.count(TaskState.COMPLETED),
            "failed_task_count": sum(1 for s in statuses if s in FAILED_TASK_STATES),
            "skipped_task_count": statuses.count(TaskState.SKIPPED),
            "cancelled_task_count": statuses.count(TaskState.CANCELLED),
            "interrupted_task_count": statuses.count(TaskState.INTERRUPTED),
        }

    def _apply_changes(
        self, changes: Iterable[_Change], *, blobs: Sequence[BlobRecord] = ()
    ) -> None:
        """Validate every change against the task table, persist the blobs and the new records in
        one transaction, then update the in-memory records and publish one event per change."""
        now = self._clock.now()
        applied: list[tuple[TaskRecord, TaskRecord, _Change]] = []
        for change in changes:
            current = self._tasks[change.task_id]
            assert_transition(TASK_TRANSITIONS, current.status, change.to, entity=TASK_ENTITY)
            fields: dict[str, Any] = dict(change.fields)
            fields.update(status=change.to, updated_at=now)
            if change.reason is not None:
                fields["reason"] = change.reason
            if change.to in TERMINAL_TASK_STATES:
                fields.setdefault("ended_at", now)
            applied.append((current, _apply(current, fields), change))
        if not applied and not blobs:
            return
        with self._store.transaction():
            for blob in blobs:
                self._store.save_blob(blob)
            for _, record, _ in applied:
                self._store.save_task(record)
        for current, record, change in applied:
            self._tasks[record.task_id] = record
            payload = state_change_payload(current.status.value, record.status.value, change.reason)
            if record.status is TaskState.RUNNING:
                payload.update(self._translation_payload(record.task_id))
            if current.status is TaskState.RUNNING:
                payload["exit_code"] = record.exit_code
                payload["duration_ms"] = record.duration_ms
                payload["timed_out"] = record.timed_out
                payload["truncated"] = record.truncated
            self._publish(
                EventType.TASK_STATE_CHANGED, now, task_id=record.task_id, payload=payload
            )

    def _translation_payload(self, task_id: str) -> dict[str, Any]:
        """ADR-030 §4: what the audit trail says about the command of a task that starts.

        The ``task.state_changed`` event is hash-chained by the audit log, so **this** is the
        durable proof of what really executed — there is no column, and none is needed.
        """
        decided = self._translations.get(task_id)
        if decided is None:
            return {}
        payload: dict[str, Any] = {
            "translated_to": decided.target.value,
            "translation_rules": list(decided.rules),
        }
        if decided.translated:
            payload["cmd_executed"] = decided.executed
        elif decided.reason is not None:
            payload["translation_note"] = decided.reason
        return payload

    def _persist_pid(self, task_id: str, pid: int, process_group_id: int | None) -> None:
        """ADR-016 §1: the process identity is written as soon as the process exists, silently."""
        current = self._tasks[task_id]
        record = _apply(
            current,
            {"pid": pid, "process_group_id": process_group_id, "updated_at": self._clock.now()},
        )
        self._store.save_task(record)
        self._tasks[task_id] = record

    def _publish_output(self, task_id: str, chunk: OutputChunk) -> None:
        """ADR-018: a live copy of the slice; the blob stays the truth. Never raw bytes."""
        self._publish(
            EventType.TASK_OUTPUT,
            self._clock.now(),
            task_id=task_id,
            payload={
                "stream": chunk.stream.value,
                "offset": chunk.offset,
                "size": len(chunk.data),
                "data": self._guard.decode(chunk.data),
            },
        )

    def _publish(
        self,
        event_type: EventType,
        timestamp: datetime,
        *,
        task_id: str | None,
        payload: dict[str, Any],
    ) -> None:
        self._runner.bus.publish(
            Event(
                event_type=event_type,
                timestamp=timestamp,
                session_id=self._plan.session_id,
                conversation_id=self._plan.conversation_id,
                cycle_id=self._plan.cycle_id,
                plan_id=self._plan.plan_id,
                task_id=task_id,
                payload=payload,
            )
        )

    # ---- small helpers ---------------------------------------------------------------------
    def _timeout_ms(self, task: TaskRecord) -> int:
        """``timeout_ms_applied`` as computed by the adapter (ADR-008), recomputed if absent."""
        if task.timeout_ms_applied is not None:
            return task.timeout_ms_applied
        declared = (
            task.timeout_ms if task.timeout_ms is not None else self._config.default_task_timeout_ms
        )
        return min(declared, self._config.max_task_timeout_ms)

    def _budget(self, task: TaskRecord) -> int:
        """``max_output_bytes_applied`` as computed by the adapter (ADR-010), recomputed if absent."""
        if task.max_output_bytes_applied is not None:
            return task.max_output_bytes_applied
        return self._guard.effective_budget(
            task.max_output_bytes, self._plan.default_max_output_bytes
        )

    def _ordered_tasks(self) -> list[TaskRecord]:
        return [self._tasks[task_id] for task_id in self._order]

    @property
    def _config(self) -> ExecutionSection:
        return self._runner.config.execution

    @property
    def _clock(self) -> Clock:
        return self._runner.clock

    @property
    def _store(self) -> ConversationStore:
        return self._runner.store

    @property
    def _guard(self) -> PayloadGuard:
        return self._runner.payload_guard
