"""``ContextReducer`` — the structured context summary (spec §3.11, §12.8 ; ADR-005, ADR-019 §7).

The reducer **interprets nothing**. It assembles, in a fixed order, data that is already
structured: the last ``state_summary`` written by the model (copied verbatim from the newest
``PlanRecord`` carrying one, all plans and conversations of the session), the plan ledger and the
pending outputs derived from the store, the budget of the session, and the two fields the child
conversation needs to know what comes next (``pending_message_type``, ``original_conversation_id``).

Sections, in assembly order (ADR-005 §2, 06 §2.1):

| section | source |
|---|---|
| ``goal``, ``user_message`` | ``SessionRecord`` |
| ``environment``, ``findings``, ``current_state``, ``next_expected_step`` | last non-null ``PlanRecord.state_summary`` (absent when no plan carried one) |
| ``plan_ledger`` | ``list_plans`` / ``list_tasks`` — protocol spellings of the statuses (§12.5) |
| ``pending_outputs`` | truncated tasks whose stored, non-empty stream was not fully delivered |
| ``budget`` | ``SessionRecord`` limits and consumed counters, duration from the injected clock |
| ``pending_message_type``, ``original_conversation_id`` | the rotation request (ADR-014) |

When the canonical size exceeds ``context.summary_budget_bytes`` the reduction steps of ADR-005 §3
apply cumulatively, stopping as soon as the summary fits: (1) drop ``cmd`` from the ledger,
(2) keep only the non-terminal plans and the last terminal one, (3) drop ``pending_outputs``.
Still too large after step 3 → ``RotationFailedError(SUMMARY_EXCEEDS_BUDGET)`` (§2.6: explicit,
never a silent loop). The applied step is recorded on the ``ContextSummaryRecord``.

``compose`` is pure (no write, may raise); ``persist`` writes the record; ``build`` does both.
The split lets the ``RotationCoordinator`` fail **before** creating the child conversation and
persist the record once the child's identifier exists (ADR-014 steps 1 and 2).
"""

from __future__ import annotations

import copy
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import RotationFailedError
from agentic_local_app.domain.ids import IdGenerator
from agentic_local_app.domain.models import (
    ContextSummaryRecord,
    ConversationRecord,
    PlanRecord,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import MessageType, OutputStream, PlanState
from agentic_local_app.domain.transitions import TERMINAL_PLAN_STATES
from agentic_local_app.persistence.interface import ConversationStore

__all__ = [
    "REDUCTION_STEPS",
    "STATE_SUMMARY_SECTIONS",
    "SUMMARY_EXCEEDS_BUDGET",
    "ContextReducer",
    "SummaryDraft",
    "known_remote_id",
]

#: ``RotationFailedError.error_code`` when the summary does not fit after every reduction step.
SUMMARY_EXCEEDS_BUDGET = "SUMMARY_EXCEEDS_BUDGET"

#: The sections copied verbatim from the model's ``state_summary`` (ADR-005 §1), in order.
STATE_SUMMARY_SECTIONS: tuple[str, ...] = (
    "environment",
    "findings",
    "current_state",
    "next_expected_step",
)

_ONE_MS = timedelta(milliseconds=1)


def known_remote_id(conversation: ConversationRecord) -> str:
    """The identifier the model knows a conversation by (the local id before any init)."""
    return conversation.remote_conversation_id or conversation.conversation_id


@dataclass(frozen=True)
class SummaryDraft:
    """A composed summary: the payload, its canonical size and the reduction step applied."""

    payload: dict[str, Any]
    size_bytes: int
    reduction_step: int


# ------------------------------------------------------------------------------------------------
# Reduction steps (ADR-005 §3) — each takes a payload and returns a new, smaller one
# ------------------------------------------------------------------------------------------------


def _without_commands(payload: dict[str, Any]) -> dict[str, Any]:
    """Step (a): remove ``cmd`` from every task of the ledger."""
    reduced = dict(payload)
    reduced["plan_ledger"] = [
        {**plan, "tasks": [{k: v for k, v in task.items() if k != "cmd"} for task in plan["tasks"]]}
        for plan in payload["plan_ledger"]
    ]
    return reduced


def _is_terminal_entry(plan: dict[str, Any]) -> bool:
    return PlanState(str(plan["status"]).upper()) in TERMINAL_PLAN_STATES


def _open_plans_and_last_terminal(payload: dict[str, Any]) -> dict[str, Any]:
    """Step (b): keep the non-terminal plans and the last terminal one, in their original order."""
    ledger: list[dict[str, Any]] = payload["plan_ledger"]
    last_terminal = next((p for p in reversed(ledger) if _is_terminal_entry(p)), None)
    reduced = dict(payload)
    reduced["plan_ledger"] = [
        plan for plan in ledger if not _is_terminal_entry(plan) or plan is last_terminal
    ]
    return reduced


def _without_pending_outputs(payload: dict[str, Any]) -> dict[str, Any]:
    """Step (c): drop the ``pending_outputs`` section."""
    return {k: v for k, v in payload.items() if k != "pending_outputs"}


#: The cumulative reduction steps, in order; ``reduction_step`` is the 1-based index applied.
REDUCTION_STEPS: tuple[Callable[[dict[str, Any]], dict[str, Any]], ...] = (
    _without_commands,
    _open_plans_and_last_terminal,
    _without_pending_outputs,
)


# ------------------------------------------------------------------------------------------------
# The reducer
# ------------------------------------------------------------------------------------------------


class ContextReducer:
    """Assemble the structured summary of a session for a ``context_resume_request`` (§12.8)."""

    def __init__(
        self, config: AppConfig, store: ConversationStore, clock: Clock, ids: IdGenerator
    ) -> None:
        self._config = config
        self._store = store
        self._clock = clock
        self._ids = ids

    # ------------------------------------------------------------------ public -------------
    def compose(
        self,
        session: SessionRecord,
        source: ConversationRecord,
        *,
        pending_message_type: MessageType,
    ) -> SummaryDraft:
        """The summary payload within ``context.summary_budget_bytes``, or ``RotationFailedError``.

        Pure: nothing is written. Reproducible: the same store (and clock instant) gives the same
        canonical bytes.
        """
        budget = self._config.context.summary_budget_bytes
        payload = self._assemble(session, source, pending_message_type)
        size = size_bytes(payload)
        step = 0
        for index, reduce in enumerate(REDUCTION_STEPS, start=1):
            if size <= budget:
                break
            payload = reduce(payload)
            size = size_bytes(payload)
            step = index
        if size > budget:
            raise RotationFailedError(
                SUMMARY_EXCEEDS_BUDGET, size_bytes=size, budget_bytes=budget, step=step
            )
        return SummaryDraft(payload=payload, size_bytes=size, reduction_step=step)

    def persist(
        self,
        draft: SummaryDraft,
        *,
        session_id: str,
        source_conversation_id: str,
        target_conversation_id: str,
    ) -> ContextSummaryRecord:
        """Write the ``ContextSummaryRecord`` of ``draft`` (identifier and timestamp injected)."""
        record = ContextSummaryRecord(
            summary_id=self._ids.summary_id(),
            session_id=session_id,
            source_conversation_id=source_conversation_id,
            target_conversation_id=target_conversation_id,
            summary_payload=draft.payload,
            summary_size_bytes=draft.size_bytes,
            reduction_step=draft.reduction_step,
            created_at=self._clock.now(),
        )
        self._store.save_context_summary(record)
        return record

    def build(
        self,
        session: SessionRecord,
        source: ConversationRecord,
        *,
        pending_message_type: MessageType,
        target_conversation_id: str,
    ) -> ContextSummaryRecord:
        """``compose`` then ``persist`` — the module map signature, in one call."""
        draft = self.compose(session, source, pending_message_type=pending_message_type)
        return self.persist(
            draft,
            session_id=session.session_id,
            source_conversation_id=source.conversation_id,
            target_conversation_id=target_conversation_id,
        )

    # ------------------------------------------------------------------ assembly -----------
    def _assemble(
        self,
        session: SessionRecord,
        source: ConversationRecord,
        pending_message_type: MessageType,
    ) -> dict[str, Any]:
        session_id = session.session_id
        plans = self._store.list_plans(session_id)
        payload: dict[str, Any] = {"goal": session.goal, "user_message": session.user_message}
        state_summary = _last_state_summary(plans)
        if state_summary is not None:
            for key in STATE_SUMMARY_SECTIONS:
                value = state_summary.get(key)
                if value is not None:
                    payload[key] = copy.deepcopy(value)
        payload["plan_ledger"] = [self._ledger_entry(plan) for plan in plans]
        payload["pending_outputs"] = self._pending_outputs(session_id)
        payload["budget"] = self._budget(session)
        payload["pending_message_type"] = pending_message_type.value
        payload["original_conversation_id"] = known_remote_id(source)
        return payload

    def _ledger_entry(self, plan: PlanRecord) -> dict[str, Any]:
        tasks = self._store.list_tasks(plan.session_id, plan_id=plan.plan_id)
        return {
            "plan_id": plan.plan_id,
            "plan_type": plan.plan_type.value,
            "objective": plan.objective,
            "status": plan.status.protocol_value,
            "stop_reason": plan.stop_reason,
            "tasks": [_task_entry(task) for task in tasks],
        }

    def _pending_outputs(self, session_id: str) -> list[dict[str, Any]]:
        """Streams the model can still fetch with ``chunk_request`` (ADR-011): stored, non-empty and
        not fully delivered, for every truncated task, in plan then task order."""
        entries: list[dict[str, Any]] = []
        for task in self._store.list_tasks(session_id):
            if not task.truncated:
                continue
            for stream, delivered in (
                (OutputStream.STDOUT, task.stdout_range),
                (OutputStream.STDERR, task.stderr_range),
            ):
                blob = self._store.get_blob_for_task(session_id, task.task_id, stream)
                if blob is None or blob.size_bytes == 0:
                    continue
                if delivered is not None and delivered[0] == 0 and delivered[1] >= blob.size_bytes:
                    continue  # the whole stream already reached the model
                entries.append(
                    {
                        "task_id": task.task_id,
                        "stream": stream.value,
                        "total_bytes": blob.size_bytes,
                    }
                )
        return entries

    def _budget(self, session: SessionRecord) -> dict[str, int]:
        consumed_duration_ms = 0
        if session.started_at is not None:
            end = session.ended_at or self._clock.now()
            consumed_duration_ms = max(0, (end - session.started_at) // _ONE_MS)
        return {
            "max_cycles": session.budget.max_cycles,
            "max_plans": session.budget.max_plans,
            "max_total_duration_ms": session.budget.max_total_duration_ms,
            "consumed_cycles": session.consumed_cycles,
            "consumed_plans": session.consumed_plans,
            "consumed_duration_ms": consumed_duration_ms,
        }


def _last_state_summary(plans: list[PlanRecord]) -> dict[str, Any] | None:
    """The newest ``state_summary`` of the session (plans are listed oldest first)."""
    for plan in reversed(plans):
        if plan.state_summary is not None:
            return plan.state_summary
    return None


def _task_entry(task: TaskRecord) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "cmd": task.cmd,
        "status": task.status.protocol_value,
        "exit_code": task.exit_code,
        "truncated": task.truncated,
        "original_size_bytes": task.original_size_bytes,
    }
