"""Phase 0 — foundation smoke tests (domain, config, event bus, in-memory store).

The real, exhaustive suites live in the phase files (§18.2). These tests only pin the contracts
every phase relies on: canonical serialisation, config loading and env overrides, bus ordering
and isolation, and the in-memory store transaction rollback.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from agentic_local_app.config import AppConfig, load_config, load_dotenv
from agentic_local_app.domain.canonical import GENESIS_HASH, canonical_json, chain_hash, size_bytes
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.errors import ConfigError, PersistenceError
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.models import SessionBudget, SessionRecord
from agentic_local_app.domain.states import SessionState
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.persistence.memory import InMemoryConversationStore

pytestmark = pytest.mark.phase1


def _session(ids: SequentialIdGenerator, clock: FakeClock) -> SessionRecord:
    now = clock.now()
    return SessionRecord(
        session_id=ids.session_id(),
        status=SessionState.READY,
        goal="g",
        user_message="m",
        user_id="u",
        auto_close_on_final_answer=False,
        budget=SessionBudget(max_cycles=1, max_plans=1, max_total_duration_ms=1),
        created_at=now,
        updated_at=now,
    )


# ---------------------------------------------------------------- canonical -----------------
def given_dict_with_unordered_keys_when_canonicalised_then_keys_sorted_and_compact() -> None:
    assert canonical_json({"b": 1, "a": [1, 2], "c": {"z": True, "y": None}}) == (
        '{"a":[1,2],"b":1,"c":{"y":null,"z":true}}'
    )


def given_datetime_and_enum_when_canonicalised_then_isoformat_and_value_used() -> None:
    payload = {"t": datetime(2026, 1, 1, tzinfo=UTC), "s": SessionState.READY}
    assert canonical_json(payload) == '{"s":"READY","t":"2026-01-01T00:00:00+00:00"}'


def given_same_event_when_chain_hashed_twice_then_hashes_identical_and_depend_on_previous() -> None:
    event = {"event_type": "x", "sequence": 1}
    h1 = chain_hash(GENESIS_HASH, event)
    h2 = chain_hash(GENESIS_HASH, event)
    h3 = chain_hash("f" * 64, event)
    assert h1 == h2 and h1 != h3 and len(h1) == 64


def given_unicode_payload_when_size_measured_then_utf8_bytes_counted() -> None:
    assert size_bytes({"k": "é"}) == len('{"k":"é"}'.encode())


# ---------------------------------------------------------------- clock / ids ---------------
def given_fake_clock_when_advanced_then_both_times_move_together() -> None:
    clock = FakeClock()
    t0, m0 = clock.now(), clock.monotonic_ms()
    clock.advance(1500)
    assert clock.monotonic_ms() == m0 + 1500
    assert (clock.now() - t0).total_seconds() == 1.5


def given_sequential_ids_when_generated_then_reproducible_and_prefixed() -> None:
    ids = SequentialIdGenerator()
    assert [ids.conversation_id(), ids.conversation_id(), ids.message_id()] == [
        "conv-0001",
        "conv-0002",
        "msg-0001",
    ]


# ---------------------------------------------------------------- config --------------------
def given_no_file_when_config_loaded_then_defaults_apply(tmp_path: Path) -> None:
    cfg = load_config(None, environ={}, load_env_file=False)
    assert cfg.api.port == 8765 and cfg.payload.hard_max_output_bytes == 262_144


def given_repo_config_toml_when_loaded_then_valid() -> None:
    root = Path(__file__).resolve().parents[2]
    cfg = load_config(root / "config.toml", environ={}, load_env_file=False)
    assert cfg.transport.user_id == "local-user"
    assert "{conversation_id}" in cfg.transport.post_url


def given_env_override_when_config_loaded_then_value_replaced_and_coerced(tmp_path: Path) -> None:
    cfg = load_config(
        None,
        environ={"AGENTIC__API__PORT": "9001", "AGENTIC__TRANSPORT__GZIP": "false"},
        load_env_file=False,
    )
    assert cfg.api.port == 9001 and cfg.transport.gzip is False


def given_invalid_value_when_config_loaded_then_config_error_with_details(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as exc:
        load_config(None, environ={"AGENTIC__API__PORT": "not-a-port"}, load_env_file=False)
    assert exc.value.error.error_code == "CONFIG_INVALID"


def given_get_url_without_after_placeholder_when_validated_then_rejected() -> None:
    with pytest.raises(ConfigError):
        load_config(
            None,
            environ={"AGENTIC__TRANSPORT__GET_URL": "http://x/{conversation_id}/messages"},
            load_env_file=False,
        )


def given_token_env_set_when_token_read_then_value_returned_and_masked_in_dump(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = AppConfig()
    monkeypatch.setenv(cfg.transport.token_env, "secret-token")
    assert cfg.transport.token == "secret-token"
    assert cfg.masked()["transport"]["token"] == "***"


def given_dotenv_file_when_loaded_then_only_missing_variables_set(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("# comment\nAGENTIC_TRANSPORT_TOKEN=abc\nEXISTING=new\n")
    environ = {"EXISTING": "old"}
    assert load_dotenv(env_file, environ) == 1
    assert environ == {"EXISTING": "old", "AGENTIC_TRANSPORT_TOKEN": "abc"}


# ---------------------------------------------------------------- event bus -----------------
def _event(clock: FakeClock, event_type: EventType = EventType.SESSION_CREATED) -> Event:
    return Event(event_type=event_type, timestamp=clock.now(), session_id="sess-0001")


def given_two_subscribers_when_event_published_then_delivered_in_subscription_order() -> None:
    bus, clock, order = EventBus(), FakeClock(), []
    bus.subscribe(lambda e: order.append("first"), name="first")
    bus.subscribe(lambda e: order.append("second"), name="second")
    bus.publish(_event(clock))
    assert order == ["first", "second"]


def given_failing_non_critical_subscriber_when_event_published_then_others_still_notified() -> None:
    bus, clock, rec = EventBus(), FakeClock(), RecordingSubscriber()

    def boom(e: Event) -> None:
        raise RuntimeError("boom")

    bus.subscribe(boom, name="boom")
    bus.subscribe(rec, name="rec")
    bus.publish(_event(clock))
    assert len(rec.events) == 1 and bus.subscriber_errors == 1


def given_failing_critical_subscriber_when_event_published_then_exception_propagates() -> None:
    bus, clock = EventBus(), FakeClock()

    def boom(e: Event) -> None:
        raise RuntimeError("audit down")

    bus.subscribe(boom, name="audit", critical=True)
    with pytest.raises(RuntimeError):
        bus.publish(_event(clock))


def given_type_filtered_subscriber_when_other_event_published_then_not_called() -> None:
    bus, clock, rec = EventBus(), FakeClock(), RecordingSubscriber()
    bus.subscribe(rec, name="rec", event_types=[EventType.TASK_OUTPUT])
    bus.publish(_event(clock, EventType.SESSION_CREATED))
    bus.publish(_event(clock, EventType.TASK_OUTPUT))
    assert [e.event_type for e in rec.events] == [EventType.TASK_OUTPUT]


# ---------------------------------------------------------------- in-memory store -----------
def given_transaction_when_exception_escapes_then_all_writes_rolled_back() -> None:
    store, ids, clock = InMemoryConversationStore(), SequentialIdGenerator(), FakeClock()
    first = _session(ids, clock)
    store.save_session(first)
    with pytest.raises(RuntimeError), store.transaction():
        store.save_session(_session(ids, clock))
        raise RuntimeError("abort")
    assert store.list_sessions() == [first]


def given_fail_next_write_when_saving_then_persistence_error_and_nothing_stored() -> None:
    store, ids, clock = InMemoryConversationStore(), SequentialIdGenerator(), FakeClock()
    store.fail_next_write = True
    with pytest.raises(PersistenceError):
        store.save_session(_session(ids, clock))
    assert store.list_sessions() == []
