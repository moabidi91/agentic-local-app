"""Shared pytest fixtures.

Every unit test must be isolated from external dependencies (spec §18.3): no real shell,
no real network, no real database. These fixtures provide the injectable doubles used everywhere.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from agentic_local_app.config import AppConfig, AppSection
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.lifecycle.conversation_lifecycle import ConversationLifecycleManager
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.persistence.memory import InMemoryConversationStore


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def ids() -> SequentialIdGenerator:
    return SequentialIdGenerator()


@pytest.fixture
def store() -> InMemoryConversationStore:
    return InMemoryConversationStore()


@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def recorder(bus: EventBus) -> RecordingSubscriber:
    """A subscriber to every event, registered first so it observes the full ordered stream."""
    rec = RecordingSubscriber()
    bus.subscribe(rec, name="recorder")
    return rec


@pytest.fixture
def config(tmp_path: Path) -> AppConfig:
    """Default configuration with a temporary data directory."""
    return AppConfig(app=AppSection(data_dir=str(tmp_path / "data")))


@pytest.fixture
def lifecycle(
    store: InMemoryConversationStore, bus: EventBus, clock: FakeClock, ids: SequentialIdGenerator
) -> ConversationLifecycleManager:
    return ConversationLifecycleManager(store, bus, clock, ids)
