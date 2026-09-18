"""Phase 10 — audit and observability (spec §3.16, §3.17, §3.19, §4, §16, §17.3, §18.2 ; ADR-006,
ADR-012, ADR-015, ADR-017, ADR-018).

Three subscribers of the synchronous EventBus are pinned here:

1. ``AuditLog`` — the critical subscriber: every audited event becomes an ``AuditEvent`` chained by
   ``sha256`` per session (``GENESIS_HASH`` first), sequences ``1..n``, resumable after a restart,
   verifiable (``verify``) with the first broken sequence and its reason;
2. ``ExecutionTracker`` — the §4.1 runtime snapshot, two levels (session / conversation, ADR-006),
   a cache refreshed from the **store** on every event and always equal to a full ``rebuild()``;
3. ``TelemetryService`` — counters, fixed-bucket histograms, a sliding-window rate, rendered in the
   Prometheus text exposition format.

Plus the source inspection of ADR-017 / module map rule 4: no wall clock and no randomness outside
the allowed modules. Only the doubles of ``tests/conftest.py`` are used (§18.3). The tampering
tests are deliberately white-box: they rewrite ``InMemoryConversationStore._audit`` because the
store is append-only and offers no other way to corrupt a chain.
"""

from __future__ import annotations

import io
import re
import tokenize
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

import agentic_local_app
from agentic_local_app.domain.canonical import GENESIS_HASH, chain_hash
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import PersistenceError
from agentic_local_app.domain.events import Event, EventType, state_change_payload
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import (
    AuditEvent,
    ConversationRecord,
    CycleRecord,
    MessageRecord,
    PlanRecord,
    SessionBudget,
    SessionRecord,
    TaskRecord,
)
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    CycleType,
    ExecutionPolicy,
    MessageDirection,
    MessageType,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
    TaskType,
)
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability import audit_log as audit_log_module
from agentic_local_app.observability.audit_log import (
    REASON_HASH_MISMATCH,
    REASON_PREVIOUS_HASH_MISMATCH,
    REASON_SEQUENCE_GAP,
    AuditLog,
    AuditVerification,
    audit_hash_input,
)
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.observability.execution_tracker import (
    BudgetView,
    ConversationSummary,
    ConversationView,
    CycleView,
    ExecutionTracker,
    ModelInteractionView,
    PlanView,
    RuntimeSnapshot,
    SessionView,
    TaskView,
)
from agentic_local_app.observability.telemetry import TelemetryService
from agentic_local_app.persistence.memory import InMemoryConversationStore

pytestmark = pytest.mark.phase10

BUDGET = SessionBudget(max_cycles=10, max_plans=5, max_total_duration_ms=60_000)

# =============================================================================================
# Helpers
# =============================================================================================


def _new_session(
    lifecycle: ConversationLifecycleManager, *, auto_close: bool = False
) -> SessionRecord:
    return lifecycle.create_session(
        goal="diagnose disk usage",
        user_message="why is /var full?",
        user_id="local-user",
        budget=BUDGET,
        auto_close=auto_close,
    )


def _play_standard_sequence(
    lifecycle: ConversationLifecycleManager, clock: FakeClock
) -> tuple[SessionRecord, ConversationRecord]:
    """Ten real transitions through the phase 1 manager, 100 ms apart (10 audited events)."""
    session = _new_session(lifecycle)
    clock.advance(100)
    lifecycle.transition_session(session.session_id, SessionState.RUNNING, reason="user_request")
    clock.advance(100)
    conversation = lifecycle.create_conversation(session.session_id)
    steps: list[tuple[ConversationState, dict[str, Any]]] = [
        (ConversationState.ACTIVE, {}),
        (ConversationState.WAITING_MODEL_RESPONSE, {}),
        (ConversationState.RUNNING_PLAN, {"reason": "discovery_plan"}),
    ]
    for state, extra in steps:
        clock.advance(100)
        lifecycle.transition_conversation(conversation.conversation_id, state, **extra)
    clock.advance(100)
    lifecycle.transition_context_window(
        conversation.conversation_id, ContextWindowState.WARNING, reason="warning_ratio_reached"
    )
    clock.advance(100)
    lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    clock.advance(100)
    lifecycle.transition_conversation(
        conversation.conversation_id,
        ConversationState.COMPLETED,
        reason="final_answer",
        final_answer_received=True,
    )
    clock.advance(100)
    lifecycle.transition_session(session.session_id, SessionState.COMPLETED, reason="final_answer")
    final_session = lifecycle.get_session(session.session_id)
    final_conversation = lifecycle.get_conversation(conversation.conversation_id)
    assert final_session is not None and final_conversation is not None
    return final_session, final_conversation


def _event(
    clock: FakeClock,
    event_type: EventType,
    session_id: str = "sess-0001",
    *,
    conversation_id: str | None = None,
    cycle_id: str | None = None,
    plan_id: str | None = None,
    task_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> Event:
    return Event(
        event_type=event_type,
        timestamp=clock.now(),
        session_id=session_id,
        conversation_id=conversation_id,
        cycle_id=cycle_id,
        plan_id=plan_id,
        task_id=task_id,
        payload=payload or {},
    )


def _cycle(
    session: SessionRecord,
    conversation: ConversationRecord,
    clock: FakeClock,
    *,
    cycle_id: str = "cyc-0001",
    status: CycleState = CycleState.RUNNING,
    retry_count: int = 0,
) -> CycleRecord:
    return CycleRecord(
        cycle_id=cycle_id,
        conversation_id=conversation.conversation_id,
        session_id=session.session_id,
        cycle_type=CycleType.DISCOVERY,
        status=status,
        retry_count=retry_count,
        started_at=clock.now(),
    )


def _plan(
    session: SessionRecord,
    conversation: ConversationRecord,
    clock: FakeClock,
    *,
    plan_id: str = "plan-1",
    cycle_id: str = "cyc-0001",
    status: PlanState = PlanState.RUNNING,
    task_count: int = 0,
    **counters: int,
) -> PlanRecord:
    return PlanRecord(
        plan_id=plan_id,
        session_id=session.session_id,
        conversation_id=conversation.conversation_id,
        cycle_id=cycle_id,
        plan_type=PlanType.DISCOVERY_PLAN,
        objective="discover the environment",
        execution_policy=ExecutionPolicy.PARALLEL,
        max_parallel_workers=2,
        status=status,
        task_count=task_count,
        started_at=clock.now(),
        created_at=clock.now(),
        updated_at=clock.now(),
        **counters,
    )


def _task(
    session: SessionRecord,
    conversation: ConversationRecord,
    clock: FakeClock,
    *,
    task_id: str,
    order_index: int,
    plan_id: str = "plan-1",
    status: TaskState = TaskState.PENDING,
    **fields: Any,
) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        plan_id=plan_id,
        session_id=session.session_id,
        conversation_id=conversation.conversation_id,
        order_index=order_index,
        type=TaskType.CMD,
        cmd=f"echo {task_id}",
        status=status,
        created_at=clock.now(),
        updated_at=clock.now(),
        **fields,
    )


def _message(
    session: SessionRecord,
    conversation: ConversationRecord,
    clock: FakeClock,
    *,
    message_id: str,
    direction: MessageDirection,
    message_type: MessageType,
    validation_status: str | None = None,
) -> MessageRecord:
    return MessageRecord(
        message_id=message_id,
        session_id=session.session_id,
        conversation_id=conversation.conversation_id,
        direction=direction,
        message_type=message_type,
        payload={"type": message_type.value},
        size_bytes=64,
        validation_status=validation_status,
        created_at=clock.now(),
    )


def _require_snapshot_conversation(snapshot: RuntimeSnapshot) -> ConversationView:
    assert snapshot.conversation is not None
    return snapshot.conversation


class _StoreFailingOnAuditAppend(InMemoryConversationStore):
    """Fails the next ``append_audit_event`` only (the state write before it succeeds)."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_next_audit_append = False

    def append_audit_event(self, event: AuditEvent) -> None:
        if self.fail_next_audit_append:
            self.fail_next_audit_append = False
            raise PersistenceError("SIMULATED_AUDIT_WRITE_FAILURE")
        super().append_audit_event(event)


class _CountingStore(InMemoryConversationStore):
    """Counts ``list_audit_events`` calls (paging of ``verify``)."""

    def __init__(self) -> None:
        super().__init__()
        self.list_calls = 0

    def list_audit_events(
        self, session_id: str, *, after_sequence: int | None = None, limit: int = 1000
    ) -> list[AuditEvent]:
        self.list_calls += 1
        return super().list_audit_events(session_id, after_sequence=after_sequence, limit=limit)


class _StoreFailingOnConversationList(InMemoryConversationStore):
    """Fails a read only the tracker performs (the lifecycle manager never lists conversations)."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_reads = False

    def list_conversations(self, session_id: str) -> list[ConversationRecord]:
        if self.fail_reads:
            raise RuntimeError("store unreachable")
        return super().list_conversations(session_id)


@pytest.fixture
def audit(
    store: InMemoryConversationStore, clock: FakeClock, ids: SequentialIdGenerator
) -> AuditLog:
    return AuditLog(store, clock, ids)


@pytest.fixture
def tracker(store: InMemoryConversationStore, clock: FakeClock) -> ExecutionTracker:
    return ExecutionTracker(store, clock)


@pytest.fixture
def telemetry(clock: FakeClock) -> TelemetryService:
    return TelemetryService(clock)


# =============================================================================================
# A. AuditLog — hash chain (§3.16, §16, §17.3, ADR-015, ADR-017)
# =============================================================================================


def given_lifecycle_transitions_when_audited_then_chain_has_sequences_1_to_n_linked_from_genesis(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
    recorder: RecordingSubscriber,
) -> None:
    audit.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)

    chain = store.list_audit_events(session.session_id)
    assert len(chain) == len(recorder.events) == 10
    previous_hash = GENESIS_HASH
    for index, (audited, published) in enumerate(zip(chain, recorder.events, strict=True), start=1):
        assert audited.sequence == index
        assert audited.previous_event_hash == previous_hash
        assert audited.event_hash == audit.recompute_hash(audited)
        assert audited.event_hash == chain_hash(previous_hash, audit_hash_input(audited))
        assert len(audited.event_hash) == 64 and audited.event_hash != previous_hash
        # the audit event mirrors the bus event (ADR-018: the SSE stream replays exactly this)
        assert audited.event_type == published.event_type.value
        assert audited.timestamp == published.timestamp
        assert audited.session_id == published.session_id == session.session_id
        assert audited.conversation_id == published.conversation_id
        assert audited.payload == published.payload
        assert audited.event_id == f"evt-{index:04d}"
        previous_hash = audited.event_hash
    assert chain[0].previous_event_hash == GENESIS_HASH
    assert len({e.event_hash for e in chain}) == 10

    result = audit.verify(session.session_id)
    assert (result.valid, result.checked, result.first_broken_sequence, result.reason) == (
        True,
        10,
        None,
        None,
    )
    assert result.verified_at == clock.now()


def given_audit_event_when_hash_input_built_then_exactly_the_documented_fields(
    lifecycle: ConversationLifecycleManager, bus: EventBus, clock: FakeClock, audit: AuditLog
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    audited = audit.last(session.session_id)
    assert audited is not None

    expected = {
        "event_id": "evt-0001",
        "sequence": 1,
        "previous_event_hash": GENESIS_HASH,
        "session_id": session.session_id,
        "conversation_id": None,
        "cycle_id": None,
        "plan_id": None,
        "task_id": None,
        "event_type": "session.created",
        "timestamp": clock.now().isoformat(),
        "payload": {"goal": "diagnose disk usage", "budget": BUDGET.model_dump()},
    }
    assert audit_hash_input(audited) == expected
    assert audited.event_hash == chain_hash(GENESIS_HASH, expected)
    assert audit.recompute_hash(audited) == audited.event_hash


def given_two_transitions_when_published_then_audit_order_equals_publication_order(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    audit: AuditLog,
    recorder: RecordingSubscriber,
) -> None:
    """The test named by ADR-015."""
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)
    lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )

    audited = store.list_audit_events(session.session_id)
    assert [a.event_type for a in audited] == [e.event_type.value for e in recorder.events]
    assert [(a.payload.get("from"), a.payload.get("to")) for a in audited[-2:]] == [
        ("NEW", "ACTIVE"),
        ("ACTIVE", "WAITING_MODEL_RESPONSE"),
    ]
    assert [a.sequence for a in audited] == [1, 2, 3, 4]


def given_valid_chain_when_payload_of_one_event_altered_then_verify_reports_hash_mismatch_there(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)
    chain = store._audit[session.session_id]  # white-box: the store is append-only
    victim = chain[4]
    assert victim.sequence == 5
    chain[4] = victim.model_copy(update={"payload": {**victim.payload, "to": "TAMPERED"}})

    result = audit.verify(session.session_id)

    assert result.valid is False
    assert result.first_broken_sequence == 5
    assert result.reason == REASON_HASH_MISMATCH
    assert result.checked == 4  # the four events before the break were fine


def given_valid_chain_when_event_rehashed_consistently_then_next_event_reports_previous_hash_mismatch(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)
    chain = store._audit[session.session_id]
    forged = chain[2].model_copy(update={"payload": {**chain[2].payload, "reason": "forged"}})
    forged = forged.model_copy(update={"event_hash": audit.recompute_hash(forged)})
    chain[2] = forged

    result = audit.verify(session.session_id)

    assert result.valid is False
    assert result.first_broken_sequence == 4
    assert result.reason == REASON_PREVIOUS_HASH_MISMATCH
    assert result.checked == 3


def given_valid_chain_when_hash_field_overwritten_then_verify_reports_hash_mismatch(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    lifecycle.create_conversation(session.session_id)
    chain = store._audit[session.session_id]
    chain[1] = chain[1].model_copy(update={"event_hash": "f" * 64})

    result = audit.verify(session.session_id)
    assert (result.valid, result.first_broken_sequence, result.reason) == (
        False,
        2,
        REASON_HASH_MISMATCH,
    )


def given_valid_chain_when_an_event_removed_then_verify_reports_sequence_gap(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)
    chain = store._audit[session.session_id]
    del chain[1]  # sequence 2 disappears: 1, 3, 4, ...

    result = audit.verify(session.session_id)

    assert result.valid is False
    assert result.first_broken_sequence == 3
    assert result.reason == REASON_SEQUENCE_GAP
    assert result.checked == 1


def given_chain_missing_its_first_event_when_verified_then_sequence_gap_at_the_first_seen_sequence(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    lifecycle.create_conversation(session.session_id)
    del store._audit[session.session_id][0]

    result = audit.verify(session.session_id)
    assert (result.valid, result.checked, result.first_broken_sequence, result.reason) == (
        False,
        0,
        2,
        REASON_SEQUENCE_GAP,
    )


def given_session_without_audit_events_when_verified_then_valid_with_zero_checked(
    audit: AuditLog, clock: FakeClock
) -> None:
    result = audit.verify("sess-9999")
    assert result == AuditVerification(
        valid=True, checked=0, first_broken_sequence=None, reason=None, verified_at=clock.now()
    )


def given_long_chain_when_verified_with_small_pages_then_every_page_read_and_chain_valid(
    bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    store = _CountingStore()
    lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
    audit = AuditLog(store, clock, ids)
    audit.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)  # 10 events
    store.list_calls = 0

    result = audit.verify(session.session_id, page_size=3)

    assert (result.valid, result.checked) == (True, 10)
    assert store.list_calls == 4  # 3 + 3 + 3 + 1 (short page ends the loop)


def given_filled_store_when_new_audit_log_created_then_sequence_resumes_at_n_plus_1_and_chain_stays_valid(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    ids: SequentialIdGenerator,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)
    last_before = store.get_last_audit_event(session.session_id)
    assert last_before is not None and last_before.sequence == 3

    # "restart": a brand new AuditLog over the same store, the old one is gone
    bus.unsubscribe("audit_log")
    restarted = AuditLog(store, clock, ids)
    restarted.subscribe(bus)
    clock.advance(500)
    lifecycle.transition_conversation(
        conversation.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )

    chain = store.list_audit_events(session.session_id)
    assert [e.sequence for e in chain] == [1, 2, 3, 4]
    assert chain[3].previous_event_hash == last_before.event_hash
    assert chain[3].event_hash == restarted.recompute_hash(chain[3])
    assert chain[3].payload == {"from": "ACTIVE", "to": "WAITING_MODEL_RESPONSE"}
    assert restarted.verify(session.session_id).valid is True
    assert restarted.last(session.session_id) == chain[3]


def given_task_output_event_when_published_then_not_audited_and_handle_returns_none(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    output = _event(
        clock,
        EventType.TASK_OUTPUT,
        session.session_id,
        plan_id="plan-1",
        task_id="t1",
        payload={"stream": "stdout", "offset": 0, "data": "hello"},
    )
    assert output.audited is False

    bus.publish(output)
    assert audit.handle(output) is None

    assert store.count_audit_events(session.session_id) == 1  # session.created only
    lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    chain = store.list_audit_events(session.session_id)
    assert [e.sequence for e in chain] == [1, 2]  # no hole left by the ignored event
    assert audit.verify(session.session_id).valid is True


def given_store_failing_on_audit_append_when_event_published_then_error_propagates_and_no_partial_event(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    failing = _event(
        clock,
        EventType.BUDGET_UPDATED,
        session.session_id,
        payload={"consumed_cycles": 1},
    )
    store.fail_next_write = True  # the only write of this publish is the audit append

    with pytest.raises(PersistenceError):
        bus.publish(failing)

    assert store.count_audit_events(session.session_id) == 1
    assert bus.subscriber_errors == 1

    # the in-memory state was not advanced: the next event takes sequence 2 with the right link
    recovered = bus.publish(failing)
    assert recovered is None
    chain = store.list_audit_events(session.session_id)
    assert [e.sequence for e in chain] == [1, 2]
    assert chain[1].previous_event_hash == chain[0].event_hash
    assert audit.verify(session.session_id).valid is True


def given_lifecycle_with_store_failing_on_audit_append_when_transition_then_state_persisted_but_not_audited(
    bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    """ADR-015 consequence (phase 1 open point 2): the record is written, the event is not chained."""
    store = _StoreFailingOnAuditAppend()
    lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
    audit = AuditLog(store, clock, ids)
    audit.subscribe(bus)
    session = _new_session(lifecycle)
    store.fail_next_audit_append = True

    with pytest.raises(PersistenceError):
        lifecycle.transition_session(session.session_id, SessionState.RUNNING)

    persisted = store.get_session(session.session_id)
    assert persisted is not None and persisted.status is SessionState.RUNNING
    assert store.count_audit_events(session.session_id) == 1

    # the store recovers: the chain continues without a hole
    lifecycle.transition_session(session.session_id, SessionState.COMPLETED)
    chain = store.list_audit_events(session.session_id)
    assert [(e.sequence, e.payload.get("to")) for e in chain] == [(1, None), (2, "COMPLETED")]
    assert audit.verify(session.session_id).valid is True


def given_two_sessions_when_events_interleaved_then_two_independent_chains(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    first = _new_session(lifecycle)
    second = _new_session(lifecycle)
    lifecycle.transition_session(first.session_id, SessionState.RUNNING)
    lifecycle.transition_session(second.session_id, SessionState.RUNNING)
    lifecycle.create_conversation(first.session_id)

    chain_a = store.list_audit_events(first.session_id)
    chain_b = store.list_audit_events(second.session_id)
    assert [e.sequence for e in chain_a] == [1, 2, 3]
    assert [e.sequence for e in chain_b] == [1, 2]
    assert chain_a[0].previous_event_hash == chain_b[0].previous_event_hash == GENESIS_HASH
    assert chain_a[1].previous_event_hash == chain_a[0].event_hash
    assert chain_b[1].previous_event_hash == chain_b[0].event_hash
    assert {e.session_id for e in chain_a} == {first.session_id}
    assert {e.session_id for e in chain_b} == {second.session_id}
    assert not {e.event_hash for e in chain_a} & {e.event_hash for e in chain_b}
    # event ids are global (one generator), sequences are per session
    assert [e.event_id for e in chain_a + chain_b] == [
        "evt-0001",
        "evt-0003",
        "evt-0005",
        "evt-0002",
        "evt-0004",
    ]
    assert audit.verify(first.session_id).valid and audit.verify(second.session_id).valid


def given_same_transitions_on_two_fresh_systems_when_audited_then_hashes_identical(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    audit: AuditLog,
) -> None:
    """ADR-017: with the injected clock and ids the whole chain is reproducible byte for byte."""

    def play(lc: ConversationLifecycleManager) -> None:
        session = _new_session(lc)
        conversation = lc.create_conversation(session.session_id)
        lc.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)

    audit.subscribe(bus)
    play(lifecycle)
    other_store, other_bus, other_clock, other_ids = (
        InMemoryConversationStore(),
        EventBus(),
        FakeClock(),
        SequentialIdGenerator(),
    )
    AuditLog(other_store, other_clock, other_ids).subscribe(other_bus)
    play(ConversationLifecycleManager(other_store, other_bus, other_clock, other_ids))

    assert store.count_audit_events("sess-0001") == 3
    assert [e.event_hash for e in store.list_audit_events("sess-0001")] == [
        e.event_hash for e in other_store.list_audit_events("sess-0001")
    ]


def given_audit_log_when_subscribed_then_registered_as_critical_subscriber_named_audit_log(
    bus: EventBus, audit: AuditLog, clock: FakeClock, store: InMemoryConversationStore
) -> None:
    audit.subscribe(bus)
    assert bus.subscriber_names == ["audit_log"]
    with pytest.raises(ValueError, match="already registered"):
        audit.subscribe(bus)
    # critical: a failure is not isolated by the bus
    store.closed = True
    with pytest.raises(PersistenceError):
        bus.publish(_event(clock, EventType.SESSION_CREATED))


def given_unknown_session_when_last_requested_then_none(audit: AuditLog) -> None:
    assert audit.last("sess-9999") is None


# =============================================================================================
# B. ExecutionTracker — runtime snapshot (§3.19, §4.1, ADR-006, ADR-012, ADR-013, ADR-015)
# =============================================================================================

SESSION_FIELDS = {
    "session_id",
    "status",
    "goal",
    "auto_close_on_final_answer",
    "session_budget",
    "rotations_count",
    "current_conversation_id",
    "started_at",
    "ended_at",
    "interrupted_at",
    "created_at",
    "updated_at",
}
BUDGET_FIELDS = {
    "max_cycles",
    "max_plans",
    "max_total_duration_ms",
    "consumed_cycles",
    "consumed_plans",
    "consumed_duration_ms",
}
CONVERSATION_FIELDS = {
    "conversation_id",
    "parent_conversation_id",
    "status",
    "auto_close_on_final_answer",
    "context_window_state",
    "last_model_response_state",
    "current_cycle_id",
    "current_plan_id",
    "last_completed_plan_id",
    "final_answer_received",
    "interrupted_at",
    "session_budget",
    "created_at",
    "updated_at",
}
CYCLE_FIELDS = {
    "cycle_id",
    "cycle_type",
    "status",
    "started_at",
    "ended_at",
    "retry_count",
    "conversation_id",
}
PLAN_FIELDS = {
    "plan_id",
    "plan_type",
    "objective",
    "execution_policy",
    "max_parallel_workers",
    "status",
    "stop_reason",
    "task_count",
    "completed_task_count",
    "failed_task_count",
    "skipped_task_count",
    "cancelled_task_count",
    "interrupted_task_count",
    "started_at",
    "ended_at",
}
TASK_FIELDS = {
    "task_id",
    "plan_id",
    "type",
    "cmd",
    "status",
    "critical",
    "continue_on_error",
    "stop_plan_on_failure",
    "stop_plan_on_success",
    "depends_on",
    "resource_lock",
    "max_output_bytes",
    "attempt_count",
    "exit_code",
    "truncated",
    "original_size_bytes",
    "started_at",
    "ended_at",
    "duration_ms",
}
MODEL_INTERACTION_FIELDS = {
    "last_outbound_message_type",
    "last_inbound_message_type",
    "last_post_status",
    "last_get_status",
    "last_protocol_validation_status",
}


@pytest.mark.parametrize(
    ("view", "expected"),
    [
        pytest.param(SessionView, SESSION_FIELDS, id="session"),
        pytest.param(BudgetView, BUDGET_FIELDS, id="session_budget"),
        pytest.param(ConversationView, CONVERSATION_FIELDS | {"context_bytes"}, id="conversation"),
        pytest.param(
            ConversationSummary,
            {"conversation_id", "parent_conversation_id", "status"},
            id="conversation_summary",
        ),
        pytest.param(CycleView, CYCLE_FIELDS, id="cycle"),
        pytest.param(PlanView, PLAN_FIELDS, id="plan"),
        pytest.param(TaskView, TASK_FIELDS | {"timed_out", "reason"}, id="task"),
        pytest.param(ModelInteractionView, MODEL_INTERACTION_FIELDS, id="model_interaction"),
        pytest.param(
            RuntimeSnapshot,
            {
                "session",
                "conversation",
                "conversations",
                "cycle",
                "plan",
                "tasks",
                "running_task_ids",
                "model_interaction",
                "last_event_type",
                "last_event_sequence",
                "snapshot_at",
            },
            id="snapshot",
        ),
    ],
)
def given_snapshot_models_when_fields_listed_then_every_mandatory_field_of_4_1_present(
    view: type[Any], expected: set[str]
) -> None:
    assert set(view.model_fields) == expected


def given_lifecycle_sequence_when_each_event_handled_then_snapshot_equals_rebuild_after_every_step(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
    recorder: RecordingSubscriber,
) -> None:
    """§18.2 phase 10: snapshot consistency after each state transition."""
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    sid = session.session_id
    observed: list[tuple[str, str | None, str | None]] = []

    def check(
        expected_session: SessionState, expected_conversation: ConversationState | None
    ) -> None:
        snapshot = tracker.snapshot(sid)
        assert snapshot == tracker.rebuild(sid)
        assert snapshot == tracker.snapshot(sid)
        assert snapshot.session.status is expected_session
        if expected_conversation is None:
            assert snapshot.conversation is None
        else:
            assert _require_snapshot_conversation(snapshot).status is expected_conversation
        assert snapshot.last_event_type == recorder.events[-1].event_type.value
        assert snapshot.last_event_sequence == len(recorder.events)
        assert snapshot.snapshot_at == clock.now()
        observed.append(
            (
                snapshot.session.status.value,
                snapshot.conversation.status.value if snapshot.conversation else None,
                snapshot.conversation.context_window_state.value if snapshot.conversation else None,
            )
        )

    check(SessionState.READY, None)
    lifecycle.transition_session(sid, SessionState.RUNNING, reason="user_request")
    check(SessionState.RUNNING, None)
    conversation = lifecycle.create_conversation(sid)
    cid = conversation.conversation_id
    check(SessionState.RUNNING, ConversationState.NEW)
    for state in (
        ConversationState.ACTIVE,
        ConversationState.WAITING_MODEL_RESPONSE,
        ConversationState.RUNNING_PLAN,
    ):
        clock.advance(100)
        lifecycle.transition_conversation(cid, state)
        check(SessionState.RUNNING, state)
    lifecycle.transition_context_window(cid, ContextWindowState.WARNING)
    check(SessionState.RUNNING, ConversationState.RUNNING_PLAN)
    lifecycle.transition_conversation(cid, ConversationState.ROTATING, reason="context_saturated")
    check(SessionState.RUNNING, ConversationState.ROTATING)
    child = lifecycle.create_conversation(
        sid, parent_conversation_id=cid, context_window_state=ContextWindowState.SATURATED
    )
    check(SessionState.RUNNING, ConversationState.NEW)  # the session now points to the child
    lifecycle.transition_conversation(child.conversation_id, ConversationState.ACTIVE)
    lifecycle.transition_conversation(
        child.conversation_id, ConversationState.WAITING_MODEL_RESPONSE
    )
    check(SessionState.RUNNING, ConversationState.WAITING_MODEL_RESPONSE)
    lifecycle.transition_context_window(
        child.conversation_id, ContextWindowState.HEALTHY, reason="context_resume_ack"
    )
    lifecycle.transition_conversation(cid, ConversationState.CLOSED, reason="rotated")
    check(SessionState.RUNNING, ConversationState.WAITING_MODEL_RESPONSE)
    lifecycle.transition_conversation(
        child.conversation_id,
        ConversationState.COMPLETED,
        reason="final_answer",
        final_answer_received=True,
    )
    lifecycle.transition_session(sid, SessionState.COMPLETED, reason="final_answer")
    check(SessionState.COMPLETED, ConversationState.COMPLETED)

    assert observed[-1] == ("COMPLETED", "COMPLETED", "HEALTHY")
    final = tracker.snapshot(sid)
    assert [
        (c.conversation_id, c.parent_conversation_id, c.status.value) for c in final.conversations
    ] == [(cid, None, "CLOSED"), (child.conversation_id, cid, "COMPLETED")]
    assert _require_snapshot_conversation(final).final_answer_received is True
    assert final.session.current_conversation_id == child.conversation_id


def given_completed_conversation_when_snapshot_then_every_conversation_level_field_mirrors_the_record(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session, conversation = _play_standard_sequence(lifecycle, clock)
    snapshot = tracker.snapshot(session.session_id)

    view = _require_snapshot_conversation(snapshot)
    record = store.get_conversation(conversation.conversation_id)
    assert record is not None
    for name in CONVERSATION_FIELDS - {"session_budget"}:
        assert getattr(view, name) == getattr(record, name), name
    assert view.context_bytes == record.context_bytes == 0
    assert view.status is ConversationState.COMPLETED
    assert view.context_window_state is ContextWindowState.WARNING
    assert view.final_answer_received is True
    assert view.session_budget == snapshot.session.session_budget
    assert snapshot.session.goal == "diagnose disk usage"
    assert snapshot.session.started_at == session.started_at
    assert snapshot.session.ended_at == session.ended_at == clock.now()
    assert snapshot.conversations == [
        ConversationSummary(
            conversation_id=conversation.conversation_id,
            parent_conversation_id=None,
            status=ConversationState.COMPLETED,
        )
    ]
    assert snapshot.cycle is None and snapshot.plan is None
    assert snapshot.tasks == [] and snapshot.running_task_ids == []


def given_running_session_when_clock_advanced_then_consumed_duration_ms_follows_the_injected_clock(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    sid = session.session_id
    assert tracker.snapshot(sid).session.session_budget.consumed_duration_ms == 0  # never started

    lifecycle.transition_session(sid, SessionState.RUNNING)
    clock.advance(2_500)
    budget = tracker.snapshot(sid).session.session_budget
    assert budget == BudgetView(
        max_cycles=10,
        max_plans=5,
        max_total_duration_ms=60_000,
        consumed_cycles=0,
        consumed_plans=0,
        consumed_duration_ms=2_500,
    )
    clock.advance(1_000)  # no event in between: the value is derived at snapshot time
    assert tracker.snapshot(sid).session.session_budget.consumed_duration_ms == 3_500
    assert tracker.snapshot(sid).snapshot_at == clock.now()

    lifecycle.update_session(sid, consumed_cycles=3, consumed_plans=2)
    lifecycle.transition_session(sid, SessionState.COMPLETED)
    clock.advance(10_000)
    frozen = tracker.snapshot(sid).session.session_budget
    assert (frozen.consumed_cycles, frozen.consumed_plans) == (3, 2)
    assert frozen.consumed_duration_ms == 3_500  # ended_at - started_at, frozen after the end


def given_interrupted_session_when_snapshot_then_both_levels_visible_and_interrupted_at_kept(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    """ADR-006: the session is READY again while the conversation stays INTERRUPTED (terminal)."""
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    sid = session.session_id
    lifecycle.transition_session(sid, SessionState.RUNNING)
    conversation = lifecycle.create_conversation(sid)
    lifecycle.transition_conversation(conversation.conversation_id, ConversationState.ACTIVE)
    clock.advance(1_000)
    lifecycle.transition_session(sid, SessionState.INTERRUPTING, reason="user_interrupt")
    lifecycle.interrupt_conversation(conversation.conversation_id, reason="user_interrupt")
    lifecycle.transition_session(sid, SessionState.READY, reason="cleanup_persisted")

    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.session.status is SessionState.READY
    assert snapshot.session.interrupted_at == clock.now()
    view = _require_snapshot_conversation(snapshot)
    assert view.status is ConversationState.INTERRUPTED
    assert view.interrupted_at == clock.now()
    assert snapshot.last_event_type == "session.state_changed"


def given_cycle_plan_and_tasks_in_store_when_events_published_then_snapshot_shows_them_and_running_ids(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    sid = session.session_id
    lifecycle.transition_session(sid, SessionState.RUNNING)
    conversation = lifecycle.create_conversation(sid)
    cid = conversation.conversation_id
    lifecycle.transition_conversation(cid, ConversationState.ACTIVE)
    lifecycle.transition_conversation(cid, ConversationState.WAITING_MODEL_RESPONSE)

    # phase 9 persists the cycle and the pointer, then publishes cycle.started (ADR-015)
    store.save_cycle(_cycle(session, conversation, clock))
    lifecycle.update_conversation(cid, current_cycle_id="cyc-0001")
    bus.publish(
        _event(
            clock,
            EventType.CYCLE_STARTED,
            sid,
            conversation_id=cid,
            cycle_id="cyc-0001",
            payload={"cycle_type": "discovery"},
        )
    )
    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.cycle == CycleView(
        cycle_id="cyc-0001",
        cycle_type=CycleType.DISCOVERY,
        status=CycleState.RUNNING,
        started_at=clock.now(),
        ended_at=None,
        retry_count=0,
        conversation_id=cid,
    )
    assert snapshot.plan is None and snapshot.tasks == []

    # the plan and its tasks are persisted, the conversation runs it
    plan = _plan(session, conversation, clock, status=PlanState.PENDING, task_count=3)
    store.save_plan(plan)
    store.save_tasks(
        [
            _task(session, conversation, clock, task_id="t1", order_index=0),
            _task(
                session,
                conversation,
                clock,
                task_id="t2",
                order_index=1,
                depends_on=("t1",),
                resource_lock="pom.xml",
                critical=True,
                max_output_bytes=2048,
            ),
            _task(session, conversation, clock, task_id="t3", order_index=2),
        ]
    )
    lifecycle.transition_conversation(
        cid, ConversationState.RUNNING_PLAN, reason="discovery_plan", current_plan_id="plan-1"
    )
    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.plan is not None and snapshot.plan.status is PlanState.PENDING
    assert [t.task_id for t in snapshot.tasks] == ["t1", "t2", "t3"]
    assert snapshot.running_task_ids == []
    t2 = snapshot.tasks[1]
    assert (t2.depends_on, t2.resource_lock, t2.critical, t2.max_output_bytes) == (
        ["t1"],
        "pom.xml",
        True,
        2048,
    )
    assert (t2.type, t2.cmd, t2.status, t2.attempt_count, t2.exit_code) == (
        TaskType.CMD,
        "echo t2",
        TaskState.PENDING,
        0,
        None,
    )

    # the plan runner (phase 5) marks tasks RUNNING, persists, publishes task.state_changed
    store.save_plan(plan.model_copy(update={"status": PlanState.RUNNING}))
    bus.publish(
        _event(
            clock,
            EventType.PLAN_STATE_CHANGED,
            sid,
            conversation_id=cid,
            cycle_id="cyc-0001",
            plan_id="plan-1",
            payload=state_change_payload("PENDING", "RUNNING"),
        )
    )
    for task_id in ("t1", "t2"):
        current = store.get_task(sid, task_id)
        assert current is not None
        store.save_task(
            current.model_copy(update={"status": TaskState.RUNNING, "started_at": clock.now()})
        )
        bus.publish(
            _event(
                clock,
                EventType.TASK_STATE_CHANGED,
                sid,
                conversation_id=cid,
                plan_id="plan-1",
                task_id=task_id,
                payload=state_change_payload("PENDING", "RUNNING"),
            )
        )
    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.running_task_ids == ["t1", "t2"]
    assert snapshot.plan is not None and snapshot.plan.status is PlanState.RUNNING

    # t1 completes; the plan record's counters are stale on purpose: the tasks are the truth
    clock.advance(120)
    running_t1 = store.get_task(sid, "t1")
    assert running_t1 is not None
    store.save_task(
        running_t1.model_copy(
            update={
                "status": TaskState.COMPLETED,
                "exit_code": 0,
                "ended_at": clock.now(),
                "duration_ms": 120,
                "attempt_count": 1,
            }
        )
    )
    bus.publish(
        _event(
            clock,
            EventType.TASK_STATE_CHANGED,
            sid,
            conversation_id=cid,
            plan_id="plan-1",
            task_id="t1",
            payload={**state_change_payload("RUNNING", "COMPLETED"), "duration_ms": 120},
        )
    )
    snapshot = tracker.snapshot(sid)
    assert snapshot == tracker.rebuild(sid)
    assert snapshot.running_task_ids == ["t2"]
    assert snapshot.plan is not None
    assert (snapshot.plan.task_count, snapshot.plan.completed_task_count) == (3, 1)
    done = snapshot.tasks[0]
    assert (done.status, done.exit_code, done.duration_ms, done.attempt_count) == (
        TaskState.COMPLETED,
        0,
        120,
        1,
    )
    assert done.ended_at == clock.now()
    assert snapshot.last_event_type == "task.state_changed"


def given_plan_record_with_stale_counters_when_snapshot_then_counters_recomputed_from_task_records(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    store.save_cycle(_cycle(session, conversation, clock))
    store.save_plan(
        _plan(
            session,
            conversation,
            clock,
            status=PlanState.STOPPED_ON_FAILURE,
            task_count=2,  # stale: six tasks exist
            completed_task_count=0,
        ).model_copy(update={"stop_reason": "critical_task_failed:t3"})
    )
    statuses = [
        TaskState.COMPLETED,
        TaskState.COMPLETED,
        TaskState.FAILED,
        TaskState.TIMED_OUT,
        TaskState.SKIPPED,
        TaskState.CANCELLED,
    ]
    store.save_tasks(
        [
            _task(session, conversation, clock, task_id=f"t{i}", order_index=i, status=s)
            for i, s in enumerate(statuses, start=1)
        ]
    )
    lifecycle.update_conversation(
        conversation.conversation_id, current_cycle_id="cyc-0001", current_plan_id="plan-1"
    )
    bus.publish(
        _event(
            clock,
            EventType.PLAN_STATE_CHANGED,
            session.session_id,
            conversation_id=conversation.conversation_id,
            plan_id="plan-1",
            payload=state_change_payload("RUNNING", "STOPPED_ON_FAILURE"),
        )
    )

    plan = tracker.snapshot(session.session_id).plan
    assert plan is not None
    assert plan == PlanView(
        plan_id="plan-1",
        plan_type=PlanType.DISCOVERY_PLAN,
        objective="discover the environment",
        execution_policy=ExecutionPolicy.PARALLEL,
        max_parallel_workers=2,
        status=PlanState.STOPPED_ON_FAILURE,
        stop_reason="critical_task_failed:t3",
        task_count=6,
        completed_task_count=2,
        failed_task_count=2,  # FAILED + TIMED_OUT (ADR-008)
        skipped_task_count=1,
        cancelled_task_count=1,
        interrupted_task_count=0,
        started_at=clock.now(),
        ended_at=None,
    )
    assert tracker.snapshot(session.session_id) == tracker.rebuild(session.session_id)


def given_plan_without_task_records_when_snapshot_then_record_counters_kept(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    store.save_plan(
        _plan(session, conversation, clock, task_count=4, completed_task_count=4).model_copy(
            update={"status": PlanState.COMPLETED}
        )
    )
    lifecycle.update_conversation(conversation.conversation_id, current_plan_id="plan-1")
    bus.publish(_event(clock, EventType.PLAN_RECEIVED, session.session_id, plan_id="plan-1"))

    plan = tracker.snapshot(session.session_id).plan
    assert plan is not None
    assert (plan.task_count, plan.completed_task_count, plan.status) == (4, 4, PlanState.COMPLETED)


def given_message_events_following_the_contract_when_handled_then_model_interaction_reflects_them(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    sid = session.session_id
    conversation = lifecycle.create_conversation(sid)
    cid = conversation.conversation_id
    assert tracker.snapshot(sid).model_interaction == ModelInteractionView()

    bus.publish(
        _event(
            clock,
            EventType.MESSAGE_OUTBOUND,
            sid,
            conversation_id=cid,
            cycle_id="cyc-0001",
            payload={
                "message_type": "user_request",
                "message_id": "msg-0001",
                "post_status": 202,
                "size_bytes": 512,
            },
        )
    )
    assert tracker.snapshot(sid).model_interaction == ModelInteractionView(
        last_outbound_message_type="user_request", last_post_status=202
    )

    bus.publish(
        _event(
            clock,
            EventType.MESSAGE_INBOUND,
            sid,
            conversation_id=cid,
            cycle_id="cyc-0001",
            payload={
                "message_type": "discovery_plan",
                "message_id": "msg-0002",
                "get_status": 200,
                "validation_status": "valid",
                "size_bytes": 2048,
            },
        )
    )
    assert tracker.snapshot(sid).model_interaction == ModelInteractionView(
        last_outbound_message_type="user_request",
        last_inbound_message_type="discovery_plan",
        last_post_status=202,
        last_get_status=200,
        last_protocol_validation_status="valid",
    )

    bus.publish(
        _event(
            clock,
            EventType.MESSAGE_REJECTED,
            sid,
            conversation_id=cid,
            payload={
                "message_type": "execution_plan",
                "message_id": "msg-0003",
                "get_status": 200,
                "validation_status": "invalid",
                "error_code": "UNEXPECTED_MESSAGE_TYPE",
            },
        )
    )
    rejected = tracker.snapshot(sid).model_interaction
    assert rejected.last_inbound_message_type == "execution_plan"
    assert rejected.last_protocol_validation_status == "invalid"
    assert rejected.last_get_status == 200

    bus.publish(
        _event(
            clock,
            EventType.MESSAGE_RETRANSMITTED,
            sid,
            conversation_id=cid,
            payload={
                "message_type": "execution_result",
                "message_id": "msg-0004",
                "retransmission_of": "msg-0001",
                "post_status": 503,
            },
        )
    )
    retransmitted = tracker.snapshot(sid).model_interaction
    assert (retransmitted.last_outbound_message_type, retransmitted.last_post_status) == (
        "execution_result",
        503,
    )
    # a rebuild re-reads the store but keeps the event-fed interaction state (statuses are not persisted)
    assert tracker.rebuild(sid).model_interaction == retransmitted


def given_message_records_in_store_when_cold_tracker_snapshots_then_interaction_rebuilt_from_records(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    clock: FakeClock,
) -> None:
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    store.save_message(
        _message(
            session,
            conversation,
            clock,
            message_id="msg-0001",
            direction=MessageDirection.OUTBOUND,
            message_type=MessageType.USER_REQUEST,
        )
    )
    store.save_message(
        _message(
            session,
            conversation,
            clock,
            message_id="msg-0002",
            direction=MessageDirection.INBOUND,
            message_type=MessageType.DISCOVERY_PLAN,
            validation_status="valid",
        )
    )
    store.save_message(
        _message(
            session,
            conversation,
            clock,
            message_id="msg-0003",
            direction=MessageDirection.OUTBOUND,
            message_type=MessageType.EXECUTION_RESULT,
        )
    )

    cold = ExecutionTracker(store, clock)  # nothing handled: everything comes from the store
    assert cold.snapshot(session.session_id).model_interaction == ModelInteractionView(
        last_outbound_message_type="execution_result",
        last_inbound_message_type="discovery_plan",
        last_post_status=None,
        last_get_status=None,
        last_protocol_validation_status="valid",
    )


def given_audited_store_when_cold_tracker_snapshots_then_rebuilt_from_store_with_audit_position(
    lifecycle: ConversationLifecycleManager,
    store: InMemoryConversationStore,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
) -> None:
    audit.subscribe(bus)
    session, conversation = _play_standard_sequence(lifecycle, clock)

    cold = ExecutionTracker(store, clock)
    snapshot = cold.snapshot(session.session_id)

    assert snapshot.session.status is SessionState.COMPLETED
    assert _require_snapshot_conversation(snapshot).conversation_id == conversation.conversation_id
    assert snapshot.last_event_type == "session.state_changed"
    assert snapshot.last_event_sequence == 10
    assert snapshot.model_interaction == ModelInteractionView()
    assert snapshot == cold.rebuild(session.session_id) == cold.snapshot(session.session_id)


def given_tracker_without_audit_log_when_events_handled_then_last_event_sequence_counts_events(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    assert tracker.snapshot(session.session_id).last_event_sequence == 2
    bus.publish(
        _event(clock, EventType.BUDGET_UPDATED, session.session_id, payload={"sequence": 42})
    )
    assert tracker.snapshot(session.session_id).last_event_sequence == 42  # carried by the payload
    assert tracker.snapshot(session.session_id).last_event_type == "budget.updated"


def given_audit_log_and_tracker_subscribed_in_adr015_order_when_event_published_then_tracker_sees_audit_sequence(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    audit: AuditLog,
    tracker: ExecutionTracker,
    telemetry: TelemetryService,
) -> None:
    audit.subscribe(bus)
    tracker.subscribe(bus)
    telemetry.subscribe(bus)
    assert bus.subscriber_names == ["audit_log", "execution_tracker", "telemetry"]

    session = _new_session(lifecycle)
    lifecycle.transition_session(session.session_id, SessionState.RUNNING)
    lifecycle.create_conversation(session.session_id)

    snapshot = tracker.snapshot(session.session_id)
    last = audit.last(session.session_id)
    assert last is not None
    assert (
        (snapshot.last_event_type, snapshot.last_event_sequence)
        == (
            last.event_type,
            last.sequence,
        )
        == ("conversation.created", 3)
    )
    assert telemetry.metrics()["counters"]["events_total"] == [
        {"labels": {"event_type": "conversation.created"}, "value": 1},
        {"labels": {"event_type": "session.created"}, "value": 1},
        {"labels": {"event_type": "session.state_changed"}, "value": 1},
    ]


def given_task_output_event_when_handled_then_ignored_by_tracker(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session = _new_session(lifecycle)
    before = tracker.snapshot(session.session_id)
    output = _event(
        clock,
        EventType.TASK_OUTPUT,
        session.session_id,
        plan_id="plan-1",
        task_id="t1",
        payload={"stream": "stdout", "offset": 0, "data": "x"},
    )
    bus.publish(output)
    tracker.handle(output)
    after = tracker.snapshot(session.session_id)
    assert after == before
    assert after.last_event_type == "session.created"


def given_unknown_session_when_snapshot_requested_then_key_error(tracker: ExecutionTracker) -> None:
    with pytest.raises(KeyError, match="sess-9999"):
        tracker.snapshot("sess-9999")
    with pytest.raises(KeyError, match="sess-9999"):
        tracker.rebuild("sess-9999")


def given_event_for_unknown_session_when_handled_then_ignored_without_error(
    clock: FakeClock, tracker: ExecutionTracker, bus: EventBus
) -> None:
    tracker.subscribe(bus)
    bus.publish(_event(clock, EventType.SESSION_CREATED, "sess-9999"))
    assert bus.subscriber_errors == 0
    with pytest.raises(KeyError):
        tracker.snapshot("sess-9999")


def given_tracker_when_subscribed_then_non_critical_subscriber_named_execution_tracker(
    bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> None:
    store = _StoreFailingOnConversationList()
    lifecycle = ConversationLifecycleManager(store, bus, clock, ids)
    tracker = ExecutionTracker(store, clock)
    tracker.subscribe(bus)
    assert bus.subscriber_names == ["execution_tracker"]
    session = _new_session(lifecycle)
    assert tracker.snapshot(session.session_id).session.status is SessionState.READY
    store.fail_reads = True

    lifecycle.transition_session(session.session_id, SessionState.RUNNING)  # no exception

    assert bus.subscriber_errors == 1
    store.fail_reads = False
    # the failed refresh invalidated the cache: the next read rebuilds from the store
    assert tracker.snapshot(session.session_id).session.status is SessionState.RUNNING


def given_snapshot_when_dumped_then_json_serialisable_for_the_api(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    tracker: ExecutionTracker,
) -> None:
    tracker.subscribe(bus)
    session, _ = _play_standard_sequence(lifecycle, clock)
    dumped = tracker.snapshot(session.session_id).model_dump(mode="json")
    assert dumped["session"]["status"] == "COMPLETED"
    assert dumped["conversation"]["context_window_state"] == "WARNING"
    assert dumped["session"]["session_budget"]["max_cycles"] == 10
    assert isinstance(dumped["snapshot_at"], str)
    assert RuntimeSnapshot.model_validate(dumped) == tracker.snapshot(session.session_id)


# =============================================================================================
# C. TelemetryService (§3.17)
# =============================================================================================


def _counter(telemetry: TelemetryService, name: str) -> list[dict[str, Any]]:
    counters = telemetry.metrics()["counters"]
    assert isinstance(counters, dict)
    value = counters[name]
    assert isinstance(value, list)
    return value


def _single(telemetry: TelemetryService, name: str) -> int:
    rows = _counter(telemetry, name)
    assert len(rows) == 1 and rows[0]["labels"] == {}
    value = rows[0]["value"]
    assert isinstance(value, int)
    return value


def _labelled(telemetry: TelemetryService, name: str) -> dict[tuple[tuple[str, str], ...], int]:
    return {tuple(sorted(row["labels"].items())): row["value"] for row in _counter(telemetry, name)}


def given_events_of_several_types_when_handled_then_events_total_counted_per_type(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    telemetry: TelemetryService,
) -> None:
    telemetry.subscribe(bus)
    _play_standard_sequence(lifecycle, clock)
    bus.publish(_event(clock, EventType.TASK_OUTPUT, payload={"stream": "stdout"}))

    assert _labelled(telemetry, "events_total") == {
        (("event_type", "session.created"),): 1,
        (("event_type", "session.state_changed"),): 2,
        (("event_type", "conversation.created"),): 1,
        (("event_type", "conversation.state_changed"),): 5,
        (("event_type", "context.window_state_changed"),): 1,
        (("event_type", "task.output"),): 1,
    }


def given_task_state_changes_when_handled_then_only_terminal_states_counted_by_status(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    transitions = [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
        ("PENDING", "WAITING_DEPENDENCY"),
        ("RUNNING", "FAILED"),
        ("RUNNING", "TIMED_OUT"),
        ("PENDING", "SKIPPED"),
        ("RUNNING", "CANCELLED"),
        ("RUNNING", "INTERRUPTED"),
        ("RUNNING", "COMPLETED"),
    ]
    for previous, current in transitions:
        telemetry.handle(
            _event(
                clock,
                EventType.TASK_STATE_CHANGED,
                task_id="t1",
                payload=state_change_payload(previous, current),
            )
        )
    assert _labelled(telemetry, "task_terminal_total") == {
        (("status", "COMPLETED"),): 2,
        (("status", "FAILED"),): 1,
        (("status", "TIMED_OUT"),): 1,
        (("status", "SKIPPED"),): 1,
        (("status", "CANCELLED"),): 1,
        (("status", "INTERRUPTED"),): 1,
    }


def given_plan_state_changes_when_handled_then_terminal_plans_counted_by_status(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    for previous, current in [
        ("PENDING", "RUNNING"),
        ("RUNNING", "COMPLETED"),
        ("RUNNING", "STOPPED_ON_FAILURE"),
        ("RUNNING", "SHORT_CIRCUITED_ON_SUCCESS"),
        ("PENDING", "FAILED"),
        ("RUNNING", "INTERRUPTED"),
    ]:
        telemetry.handle(
            _event(
                clock,
                EventType.PLAN_STATE_CHANGED,
                plan_id="plan-1",
                payload=state_change_payload(previous, current),
            )
        )
    assert _labelled(telemetry, "plan_terminal_total") == {
        (("status", "COMPLETED"),): 1,
        (("status", "STOPPED_ON_FAILURE"),): 1,
        (("status", "SHORT_CIRCUITED_ON_SUCCESS"),): 1,
        (("status", "FAILED"),): 1,
        (("status", "INTERRUPTED"),): 1,
    }


def given_failures_recorded_when_handled_then_failures_total_by_error_type(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    for error_type in ("NETWORK_ERROR", "NETWORK_ERROR", "BUDGET_EXCEEDED"):
        telemetry.handle(
            _event(
                clock,
                EventType.FAILURE_RECORDED,
                payload={"error_type": error_type, "error_code": "X", "failure_id": "fail-1"},
            )
        )
    telemetry.handle(_event(clock, EventType.FAILURE_RECORDED, payload={}))
    assert _labelled(telemetry, "failures_total") == {
        (("error_type", "NETWORK_ERROR"),): 2,
        (("error_type", "BUDGET_EXCEEDED"),): 1,
        (("error_type", "unknown"),): 1,
    }


def given_retries_rotations_interruptions_and_budget_events_when_handled_then_counted(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    for _ in range(3):
        telemetry.handle(
            _event(
                clock,
                EventType.RETRY_SCHEDULED,
                payload={"operation": "GET", "attempt": 1, "delay_ms": 500},
            )
        )
    telemetry.handle(_event(clock, EventType.ROTATION_STARTED))
    telemetry.handle(_event(clock, EventType.ROTATION_COMPLETED))
    telemetry.handle(_event(clock, EventType.ROTATION_COMPLETED))
    telemetry.handle(_event(clock, EventType.ROTATION_FAILED))
    telemetry.handle(_event(clock, EventType.INTERRUPTION_REQUESTED, payload={"reason": "user"}))
    telemetry.handle(_event(clock, EventType.INTERRUPTION_COMPLETED, payload={"duration_ms": 40}))
    telemetry.handle(
        _event(clock, EventType.BUDGET_EXCEEDED, payload={"limit": "max_plans", "consumed": 5})
    )
    telemetry.handle(_event(clock, EventType.BUDGET_UPDATED, payload={"consumed_plans": 4}))

    assert _single(telemetry, "retries_total") == 3
    assert _labelled(telemetry, "rotations_total") == {
        (("outcome", "completed"),): 2,
        (("outcome", "failed"),): 1,
    }
    assert _single(telemetry, "interruptions_total") == 1
    assert _single(telemetry, "budget_exceeded_total") == 1


def given_breaker_transitions_when_handled_then_counted_by_target_state(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    for previous, current in [("CLOSED", "OPEN"), ("OPEN", "HALF_OPEN"), ("HALF_OPEN", "CLOSED")]:
        telemetry.handle(
            _event(
                clock,
                EventType.BREAKER_STATE_CHANGED,
                payload=state_change_payload(previous, current),
            )
        )
    assert _labelled(telemetry, "breaker_transitions_total") == {
        (("to", "OPEN"),): 1,
        (("to", "HALF_OPEN"),): 1,
        (("to", "CLOSED"),): 1,
    }


def given_message_events_when_handled_then_messages_total_by_direction_and_size_histogram(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    telemetry.handle(
        _event(
            clock,
            EventType.MESSAGE_OUTBOUND,
            payload={"message_type": "user_request", "message_id": "m1", "size_bytes": 300},
        )
    )
    telemetry.handle(
        _event(
            clock,
            EventType.MESSAGE_INBOUND,
            payload={"message_type": "discovery_plan", "message_id": "m2", "size_bytes": 5000},
        )
    )
    telemetry.handle(
        _event(
            clock,
            EventType.MESSAGE_REJECTED,
            payload={"message_type": None, "error_code": "SCHEMA", "size_bytes": 70000},
        )
    )
    telemetry.handle(
        _event(
            clock,
            EventType.MESSAGE_RETRANSMITTED,
            payload={"message_type": "execution_result", "message_id": "m3", "size_bytes": 300},
        )
    )

    assert _labelled(telemetry, "messages_total") == {
        (("direction", "outbound"),): 2,
        (("direction", "inbound"),): 2,
    }
    assert _single(telemetry, "messages_rejected_total") == 1
    sizes = telemetry.metrics()["histograms"]["message_size_bytes"]
    assert (sizes["count"], sizes["sum"], sizes["min"], sizes["max"]) == (4, 75600, 300, 70000)
    assert sizes["buckets"]["256"] == 0
    assert sizes["buckets"]["1024"] == 2
    assert sizes["buckets"]["16384"] == 3
    assert sizes["buckets"]["+Inf"] == 4


def given_context_window_events_when_handled_then_only_saturation_counted(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    telemetry: TelemetryService,
) -> None:
    telemetry.subscribe(bus)
    session = _new_session(lifecycle)
    conversation = lifecycle.create_conversation(session.session_id)
    lifecycle.transition_context_window(conversation.conversation_id, ContextWindowState.WARNING)
    assert _single(telemetry, "context_saturations_total") == 0
    lifecycle.transition_context_window(conversation.conversation_id, ContextWindowState.SATURATED)
    lifecycle.transition_context_window(conversation.conversation_id, ContextWindowState.HEALTHY)
    assert _single(telemetry, "context_saturations_total") == 1


def given_task_and_cycle_durations_when_handled_then_histograms_hold_count_sum_min_max_and_buckets(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    for duration in (10, 250, 4000):
        telemetry.handle(
            _event(
                clock,
                EventType.TASK_STATE_CHANGED,
                task_id="t",
                payload={"from": "RUNNING", "to": "COMPLETED", "duration_ms": duration},
            )
        )
    telemetry.handle(
        _event(
            clock,
            EventType.TASK_STATE_CHANGED,
            task_id="t",
            payload={"from": "PENDING", "to": "SKIPPED"},  # no duration: nothing observed
        )
    )
    telemetry.handle(
        _event(clock, EventType.CYCLE_ENDED, cycle_id="c", payload={"duration_ms": 1200})
    )
    telemetry.handle(
        _event(clock, EventType.CYCLE_ENDED, cycle_id="c", payload={"duration_ms": "bad"})
    )

    histograms = telemetry.metrics()["histograms"]
    task = histograms["task_duration_ms"]
    assert (task["count"], task["sum"], task["min"], task["max"]) == (3, 4260, 10, 4000)
    assert task["buckets"]["10"] == 1
    assert task["buckets"]["250"] == 2
    assert task["buckets"]["1000"] == 2
    assert task["buckets"]["5000"] == 3
    assert task["buckets"]["+Inf"] == 3
    assert list(task["buckets"]) == [
        "10",
        "50",
        "100",
        "250",
        "500",
        "1000",
        "2500",
        "5000",
        "10000",
        "30000",
        "60000",
        "300000",
        "+Inf",
    ]
    cycle = histograms["cycle_duration_ms"]
    assert (cycle["count"], cycle["sum"], cycle["min"], cycle["max"]) == (1, 1200, 1200, 1200)
    assert cycle["buckets"]["1000"] == 0 and cycle["buckets"]["2500"] == 1
    empty = histograms["message_size_bytes"]
    assert (empty["count"], empty["sum"], empty["min"], empty["max"]) == (0, 0, None, None)


def given_task_completions_when_clock_moves_then_tasks_completed_per_minute_uses_sliding_window(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    def complete(n: int) -> None:
        for _ in range(n):
            telemetry.handle(
                _event(
                    clock,
                    EventType.TASK_STATE_CHANGED,
                    task_id="t",
                    payload={"from": "RUNNING", "to": "COMPLETED"},
                )
            )

    def rate() -> int:
        value = telemetry.metrics()["gauges"]["tasks_completed_per_minute"]
        assert isinstance(value, int)
        return value

    assert rate() == 0
    complete(3)
    assert rate() == 3
    clock.advance(30_000)
    complete(2)
    assert rate() == 5
    clock.advance(31_000)  # the first three are now 61 s old
    assert rate() == 2
    clock.advance(30_000)
    assert rate() == 0
    telemetry.handle(
        _event(
            clock,
            EventType.TASK_STATE_CHANGED,
            task_id="t",
            payload={"from": "RUNNING", "to": "FAILED"},
        )
    )
    assert rate() == 0  # only COMPLETED counts
    assert "tasks_completed_per_minute 0" in telemetry.render_text().splitlines()


PROM_COMMENT = re.compile(r"^# (HELP|TYPE) [a-zA-Z_:][a-zA-Z0-9_:]* .+$")
PROM_SAMPLE = re.compile(
    r'^[a-zA-Z_:][a-zA-Z0-9_:]*(\{[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\.)*"'
    r'(,[a-zA-Z_][a-zA-Z0-9_]*="(?:[^"\\\n]|\\.)*")*\})? -?[0-9]+(\.[0-9]+)?$'
)


def given_metrics_when_rendered_then_prometheus_text_format_stable_and_sorted(
    lifecycle: ConversationLifecycleManager,
    bus: EventBus,
    clock: FakeClock,
    telemetry: TelemetryService,
) -> None:
    telemetry.subscribe(bus)
    _play_standard_sequence(lifecycle, clock)
    telemetry.handle(
        _event(
            clock,
            EventType.TASK_STATE_CHANGED,
            task_id="t1",
            payload={"from": "RUNNING", "to": "COMPLETED", "duration_ms": 42},
        )
    )
    telemetry.handle(
        _event(
            clock,
            EventType.FAILURE_RECORDED,
            payload={"error_type": 'weird"type\\with\nnewline'},
        )
    )

    text = telemetry.render_text()
    assert text.endswith("\n")
    lines = text.splitlines()
    for line in lines:
        assert PROM_COMMENT.match(line) or PROM_SAMPLE.match(line), line
    assert "# TYPE events_total counter" in lines
    assert 'events_total{event_type="conversation.state_changed"} 5' in lines
    assert "# TYPE task_duration_ms histogram" in lines
    assert 'task_duration_ms_bucket{le="50"} 1' in lines
    assert 'task_duration_ms_bucket{le="+Inf"} 1' in lines
    assert "task_duration_ms_sum 42" in lines
    assert "task_duration_ms_count 1" in lines
    assert "task_duration_ms_min 42" in lines and "task_duration_ms_max 42" in lines
    assert "# TYPE tasks_completed_per_minute gauge" in lines
    assert "tasks_completed_per_minute 1" in lines
    assert "retries_total 0" in lines  # unlabelled counters are always exposed
    assert 'failures_total{error_type="weird\\"type\\\\with\\nnewline"} 1' in lines
    type_lines = [line for line in lines if line.startswith("# TYPE ")]
    names = [line.split()[2] for line in type_lines]
    assert names == sorted(names) and len(names) == len(set(names))
    label_lines = [line for line in lines if line.startswith("events_total{")]
    assert label_lines == sorted(label_lines)  # label sets sorted inside a family
    assert telemetry.render_text() == text  # stable

    twin = TelemetryService(FakeClock())
    twin_bus, twin_store, twin_ids, twin_clock = (
        EventBus(),
        InMemoryConversationStore(),
        SequentialIdGenerator(),
        FakeClock(),
    )
    twin.subscribe(twin_bus)
    twin_lifecycle = ConversationLifecycleManager(twin_store, twin_bus, twin_clock, twin_ids)
    _play_standard_sequence(twin_lifecycle, twin_clock)
    twin.handle(
        _event(
            twin_clock,
            EventType.TASK_STATE_CHANGED,
            task_id="t1",
            payload={"from": "RUNNING", "to": "COMPLETED", "duration_ms": 42},
        )
    )
    twin.handle(
        _event(
            twin_clock,
            EventType.FAILURE_RECORDED,
            payload={"error_type": 'weird"type\\with\nnewline'},
        )
    )
    assert twin.render_text() == text


def given_metrics_when_reset_then_everything_back_to_zero(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    telemetry.handle(
        _event(
            clock,
            EventType.TASK_STATE_CHANGED,
            task_id="t",
            payload={"from": "RUNNING", "to": "COMPLETED", "duration_ms": 5},
        )
    )
    telemetry.handle(_event(clock, EventType.RETRY_SCHEDULED))
    pristine = TelemetryService(FakeClock()).render_text()
    assert telemetry.render_text() != pristine

    telemetry.reset()

    assert telemetry.render_text() == pristine
    assert _single(telemetry, "retries_total") == 0
    assert telemetry.metrics()["histograms"]["task_duration_ms"]["count"] == 0
    assert telemetry.metrics()["gauges"]["tasks_completed_per_minute"] == 0
    assert _counter(telemetry, "events_total") == []


def given_malformed_payloads_when_handled_then_ignored_without_exception(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    telemetry.handle(_event(clock, EventType.TASK_STATE_CHANGED, payload={}))
    telemetry.handle(_event(clock, EventType.TASK_STATE_CHANGED, payload={"to": 3}))
    telemetry.handle(
        _event(
            clock, EventType.TASK_STATE_CHANGED, payload={"to": "COMPLETED", "duration_ms": True}
        )
    )
    telemetry.handle(_event(clock, EventType.PLAN_STATE_CHANGED, payload={"to": None}))
    telemetry.handle(_event(clock, EventType.BREAKER_STATE_CHANGED, payload={}))
    telemetry.handle(_event(clock, EventType.CONTEXT_WINDOW_STATE_CHANGED, payload={}))
    telemetry.handle(_event(clock, EventType.MESSAGE_INBOUND, payload={"size_bytes": "big"}))

    assert _labelled(telemetry, "task_terminal_total") == {(("status", "COMPLETED"),): 1}
    assert telemetry.metrics()["histograms"]["task_duration_ms"]["count"] == 0
    assert telemetry.metrics()["histograms"]["message_size_bytes"]["count"] == 0
    assert _labelled(telemetry, "plan_terminal_total") == {}
    assert _labelled(telemetry, "breaker_transitions_total") == {}
    assert _labelled(telemetry, "events_total")[(("event_type", "task.state_changed"),)] == 3


def given_telemetry_when_subscribed_then_non_critical_subscriber_named_telemetry(
    bus: EventBus, telemetry: TelemetryService
) -> None:
    telemetry.subscribe(bus)
    assert bus.subscriber_names == ["telemetry"]
    with pytest.raises(ValueError, match="already registered"):
        telemetry.subscribe(bus)


def given_metrics_dict_when_read_then_documented_structure(
    clock: FakeClock, telemetry: TelemetryService
) -> None:
    telemetry.handle(_event(clock, EventType.SESSION_CREATED))
    metrics = telemetry.metrics()
    assert set(metrics) == {"counters", "histograms", "gauges"}
    assert set(metrics["counters"]) == {
        "events_total",
        "task_terminal_total",
        "plan_terminal_total",
        "failures_total",
        "retries_total",
        "rotations_total",
        "interruptions_total",
        "breaker_transitions_total",
        "messages_total",
        "messages_rejected_total",
        "context_saturations_total",
        "budget_exceeded_total",
    }
    assert set(metrics["histograms"]) == {
        "task_duration_ms",
        "cycle_duration_ms",
        "message_size_bytes",
    }
    assert set(metrics["gauges"]) == {"tasks_completed_per_minute"}
    assert metrics["counters"]["events_total"] == [
        {"labels": {"event_type": "session.created"}, "value": 1}
    ]


# =============================================================================================
# D. Source inspection (ADR-017, module map rule 4)
# =============================================================================================

SOURCE_ROOT = Path(agentic_local_app.__file__).resolve().parent
ALLOWED_CLOCK_OR_RANDOM_MODULES = frozenset(
    {
        "domain/clock.py",
        "domain/ids.py",
        "resilience/retry_controller.py",
        "testing/mock_model_server.py",  # test tool, never part of the runtime
    }
)
FORBIDDEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("datetime.now(", re.compile(r"\bdatetime\.now\(")),
    ("datetime.utcnow(", re.compile(r"\bdatetime\.utcnow\(")),
    ("time.time(", re.compile(r"\btime\.time(?:_ns)?\(")),
    ("time.monotonic(", re.compile(r"\btime\.monotonic(?:_ns)?\(")),
    ("time.perf_counter(", re.compile(r"\btime\.perf_counter(?:_ns)?\(")),
    ("uuid4(", re.compile(r"\buuid4\(")),
    ("import random", re.compile(r"^\s*(?:import\s+random\b|from\s+random\s+import\b)", re.M)),
    ("random.", re.compile(r"\brandom\.")),
)
_FSTRING_TOKEN_TYPES = frozenset(
    getattr(tokenize, name)
    for name in ("FSTRING_START", "FSTRING_MIDDLE", "FSTRING_END")
    if hasattr(tokenize, name)
)


def _blank_strings_and_comments(source: str) -> str:
    """The same source with every string literal and comment replaced by spaces (positions kept)."""
    lines = source.splitlines(keepends=True)
    skipped = {tokenize.STRING, tokenize.COMMENT} | _FSTRING_TOKEN_TYPES
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type not in skipped:
            continue
        (start_row, start_col), (end_row, end_col) = tok.start, tok.end
        for row in range(start_row, end_row + 1):
            line = lines[row - 1]
            start = start_col if row == start_row else 0
            end = end_col if row == end_row else len(line.rstrip("\r\n"))
            lines[row - 1] = line[:start] + " " * (end - start) + line[end:]
    return "".join(lines)


def _clock_or_random_violations(source: str, label: str) -> list[str]:
    code = _blank_strings_and_comments(source)
    violations = []
    for name, pattern in FORBIDDEN_PATTERNS:
        for match in pattern.finditer(code):
            line_no = code.count("\n", 0, match.start()) + 1
            violations.append(f"{label}:{line_no}: {name}")
    return violations


def given_source_tree_when_inspected_then_no_wall_clock_or_randomness_outside_allowed_modules() -> (
    None
):
    violations: list[str] = []
    scanned = 0
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        relative = path.relative_to(SOURCE_ROOT).as_posix()
        if relative in ALLOWED_CLOCK_OR_RANDOM_MODULES or "__pycache__" in path.parts:
            continue
        scanned += 1
        violations.extend(_clock_or_random_violations(path.read_text(encoding="utf-8"), relative))
    assert scanned > 0
    assert violations == [], "clock or randomness used outside the allowed modules:\n" + "\n".join(
        violations
    )


@pytest.mark.parametrize(
    ("snippet", "expected"),
    [
        pytest.param(
            "from datetime import datetime\nx = datetime.now()\n",
            ["s.py:2: datetime.now("],
            id="now",
        ),
        pytest.param(
            "import datetime as dt\nx = dt.datetime.utcnow()\n",
            ["s.py:2: datetime.utcnow("],
            id="utcnow",
        ),
        pytest.param(
            "import time\nt = time.time()\nm = time.monotonic_ns()\n",
            ["s.py:2: time.time(", "s.py:3: time.monotonic("],
            id="time",
        ),
        pytest.param(
            "import time\np = time.perf_counter()\n",
            ["s.py:2: time.perf_counter("],
            id="perf_counter",
        ),
        pytest.param("import uuid\ni = uuid.uuid4().hex\n", ["s.py:2: uuid4("], id="uuid4"),
        pytest.param("from uuid import uuid4\ni = uuid4()\n", ["s.py:2: uuid4("], id="uuid4_bare"),
        pytest.param(
            "import random\nx = random.random()\n",
            ["s.py:1: import random", "s.py:2: random."],
            id="random",
        ),
        pytest.param("from random import choice\n", ["s.py:1: import random"], id="from_random"),
        pytest.param(
            '"""Never call datetime.now() or time.time() here."""\n# uuid4() is banned\n',
            [],
            id="docstring_and_comment_ignored",
        ),
        pytest.param(
            "label = f\"{prefix}-{'uuid4('}\"\nclock.now()\nself._random_seed = 1\n",
            [],
            id="strings_and_lookalikes_ignored",
        ),
    ],
)
def given_snippet_when_inspected_then_violations_reported_with_file_and_line(
    snippet: str, expected: list[str]
) -> None:
    assert _clock_or_random_violations(snippet, "s.py") == expected


def given_phase10_modules_when_inspected_then_they_only_use_the_injected_clock() -> None:
    for module in ("audit_log.py", "execution_tracker.py", "telemetry.py"):
        source = (SOURCE_ROOT / "observability" / module).read_text(encoding="utf-8")
        assert _clock_or_random_violations(source, module) == []
    assert "Clock" in Path(audit_log_module.__file__).read_text(encoding="utf-8")


# =============================================================================================
# E. Package exports
# =============================================================================================


def given_observability_package_when_imported_then_phase10_components_exported() -> None:
    import agentic_local_app.observability as observability

    for name in (
        "EventBus",
        "RecordingSubscriber",
        "AuditLog",
        "AuditVerification",
        "ExecutionTracker",
        "RuntimeSnapshot",
        "TelemetryService",
    ):
        assert name in observability.__all__, name
        assert hasattr(observability, name), name


def given_verification_when_created_then_frozen_value_object(clock: FakeClock) -> None:
    result = AuditVerification(
        valid=False,
        checked=3,
        first_broken_sequence=4,
        reason=REASON_SEQUENCE_GAP,
        verified_at=clock.now() + timedelta(seconds=1),
    )
    with pytest.raises((TypeError, ValueError)):
        result.valid = True  # type: ignore[misc]
    assert result.model_dump()["reason"] == "SEQUENCE_GAP"
