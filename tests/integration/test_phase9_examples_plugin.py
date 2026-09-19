"""Phase 9 — the whole protocol loop through the out-of-tree plugin of the guides.

``examples/config.acme.toml`` selects ``acme_model_plugin.provider:AcmeHttpProvider`` and
``acme_model_plugin.codec:StreamedTextCodec`` by import path. The wiring creates the codec from the
configuration and wraps the provider (built by the registry over an ``httpx.MockTransport`` that
plays the ACME Threads API); the orchestrator, the plan runner and the failure policy are the
production ones. The scripted API answers each protocol message the way a streaming model would —
a ``status`` event, then a ``message`` event whose payload is the fenced JSON cut into chunks — and
the session ends on a ``final_answer`` after a real (fake-executor) discovery plan.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from acme_model_plugin import AcmeHttpProvider, StreamedTextCodec
from agentic_local_app.config import load_config
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.states import SessionState
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import Application, build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.codecs import UNPARSEABLE_REPLY, CodecTransport
from agentic_local_app.transport.registry import TransportRegistry
from integration.phase9_rig import (
    CMD_JAVA,
    CMD_MVN,
    CMD_UNAME,
    GOAL,
    OUT_JAVA,
    OUT_MVN,
    OUT_UNAME,
    USER_MESSAGE,
    advancing_sleep,
    cmd_task,
    discovery_plan,
    final_answer,
)
from unit.test_phase7_examples_plugin import EXAMPLE_CONFIG

pytestmark = pytest.mark.phase9

THREAD = "thr_0001"
ENV = {"ACME_API_KEY": "k-secret"}


def fenced_chunks(message: dict[str, Any], size: int = 9) -> dict[str, Any]:
    """The reply as the ACME model streams it: prose + fence, cut into small deltas."""
    text = "Sure, here is the message:\n```json\n" + json.dumps(message, indent=1) + "\n```\n"
    return {"chunks": [{"delta": text[i : i + size]} for i in range(0, len(text), size)]}


class AcmeThreadsApi:
    """A stateful scripted ACME API: one reply (raw item) per protocol message type received,
    handed back after ``empty_gets`` GETs that carry only non-message events."""

    def __init__(self, replies: dict[str, Any], *, empty_gets: int = 0) -> None:
        self.replies = dict(replies)  # message type posted -> raw item of the next reply
        self.empty_gets = empty_gets
        self.posted: list[str] = []  # the text payloads as posted (outbound = "text")
        self.gets: list[dict[str, list[str]]] = []
        self.deleted: list[str] = []
        self._pending: list[Any] = []
        self._empty_left = 0
        self._events = 0

    def _event_id(self) -> str:
        self._events += 1
        return f"evt_{self._events}"

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path.endswith("/workspaces/demo/threads"):
            return httpx.Response(201, json={"thread": {"id": THREAD}})
        if request.method == "POST" and path.endswith(f"/threads/{THREAD}/events"):
            body = json.loads(request.content)
            assert body["kind"] == "message" and isinstance(body["payload"], str)
            self.posted.append(body["payload"])
            message_type = json.loads(body["payload"])["type"]
            if message_type in self.replies:
                self._pending.append(self.replies.pop(message_type))
                self._empty_left = self.empty_gets
            return httpx.Response(202, json={"event": {"id": self._event_id()}})
        if request.method == "GET" and path.endswith(f"/threads/{THREAD}/events"):
            self.gets.append(parse_qs(request.url.query.decode(), keep_blank_values=True))
            if not self._pending or self._empty_left > 0:
                self._empty_left = max(0, self._empty_left - 1)
                typing = {"id": self._event_id(), "kind": "typing", "payload": {}}
                return httpx.Response(200, json={"events": [typing], "next": typing["id"]})
            raw = self._pending.pop(0)
            events = [
                {"id": self._event_id(), "kind": "status", "payload": {"state": "done"}},
                {"id": self._event_id(), "kind": "message", "payload": raw},
            ]
            return httpx.Response(200, json={"events": events, "next": events[-1]["id"]})
        if request.method == "DELETE" and path.endswith(f"/threads/{THREAD}"):
            self.deleted.append(THREAD)
            return httpx.Response(204)
        return httpx.Response(404, json={"error": {"code": "not_found", "path": path}})


def build(
    api: AcmeThreadsApi, **env: str
) -> tuple[Application, RecordingSubscriber, FakeCommandExecutor]:
    """The production wiring over the example configuration; only the network is a double.

    ``env`` adds ``AGENTIC__SECTION__KEY`` overrides on top of :data:`ENV`, the way an operator
    would (used to pin the behaviour that predates the correction policy of ADR-023).
    """
    config = load_config(EXAMPLE_CONFIG, environ={**ENV, **env}, load_env_file=False)
    clock = FakeClock()
    sleep = advancing_sleep(clock)  # the polling of the provider moves the fake clock, never waits
    provider = TransportRegistry.create(
        config, clock=clock, transport=httpx.MockTransport(api), sleep=sleep
    )
    assert isinstance(provider, AcmeHttpProvider)
    provider._environ = dict(ENV)  # the key comes from the environment: injected for the test
    executor = FakeCommandExecutor(clock)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="acme-recorder")
    app = build_application(
        config,
        store=InMemoryConversationStore(),
        transport=provider,  # the wiring wraps it with the codec named by the configuration
        executor=executor,
        clock=clock,
        ids=SequentialIdGenerator(),
        bus=bus,
        run_recovery=False,
        sleep=sleep,
    )
    assert isinstance(app.transport, CodecTransport)
    assert isinstance(app.transport.codec, StreamedTextCodec)
    assert app.transport.inner is provider
    return app, recorder, executor


async def given_example_plugin_when_full_session_runs_then_final_answer_through_streamed_chunks() -> (
    None
):
    tasks = [cmd_task("t1", CMD_UNAME), cmd_task("t2", CMD_JAVA), cmd_task("t3", CMD_MVN)]
    api = AcmeThreadsApi(
        {
            "user_request": fenced_chunks(discovery_plan(THREAD, tasks=tasks)),
            "execution_result": fenced_chunks(final_answer(THREAD)),
        }
    )
    app, recorder, executor = build(api)
    executor.script(cmd=CMD_UNAME, stdout=OUT_UNAME)
    executor.script(cmd=CMD_JAVA, stderr=OUT_JAVA)
    executor.script(cmd=CMD_MVN, stdout=OUT_MVN)

    session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE, auto_close=True)
    session = await app.manager.wait(session.session_id, timeout_ms=10_000)

    assert session.status is SessionState.COMPLETED
    # what left the application: user_request then execution_result, as canonical JSON text
    assert [json.loads(text)["type"] for text in api.posted] == ["user_request", "execution_result"]
    result = json.loads(api.posted[1])
    assert result["content"]["status"] == "completed"
    assert [r["task_id"] for r in result["content"]["results"]] == ["t1", "t2", "t3"]
    assert result["content"]["results"][0]["stdout"] == OUT_UNAME.decode()
    # the cursor handed to the API is the one it returned ("next"), never a message_id
    assert api.gets[0]["after"] == [""] and api.gets[0]["limit"] == ["50"]
    assert all(after.startswith("evt_") for get in api.gets[1:] for after in get["after"])
    # auto_close: the DELETE of the thread went through the provider
    assert api.deleted == [THREAD]
    kinds = [event.event_type for event in recorder.events]
    assert kinds.count(EventType.MESSAGE_OUTBOUND) == 2
    assert kinds.count(EventType.MESSAGE_INBOUND) == 2
    final = app.manager.final_answer(session.session_id)
    assert final is not None and "Java 21" in json.dumps(final)


async def given_example_plugin_when_model_streams_no_json_then_unparseable_reply_fails_session() -> (
    None
):
    api = AcmeThreadsApi(
        {"user_request": {"chunks": [{"delta": "I cannot "}, {"delta": "help with that."}]}}
    )
    # the classification of an undecodable reply, without the correction loop of ADR-023
    app, recorder, _ = build(api, AGENTIC__PROTOCOL__MAX_CORRECTION_ATTEMPTS="0")
    session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE)
    session = await app.manager.wait(session.session_id, timeout_ms=10_000)

    assert session.status is SessionState.FAILED
    failures = [e for e in recorder.events if e.event_type is EventType.FAILURE_RECORDED]
    assert len(failures) == 1
    payload = failures[0].payload
    assert payload["error_code"] == UNPARSEABLE_REPLY and payload["retryable"] is False
    details = payload["details"]
    assert details["codec"] == "streamed_text" and details["reason"] == "no_json_found"
    # the excerpt is the raw item as the API handed it back (canonical JSON), cut at 500 chars
    assert '{"delta":"I cannot "}' in details["excerpt"]
    assert '{"delta":"help with that."}' in details["excerpt"]
    assert details["operation"] == "GET" and details["http_status"] == 200
    # nothing was retried: one user_request posted, the session failed at the first reply
    assert [json.loads(t)["type"] for t in api.posted] == ["user_request"]


async def given_example_plugin_when_first_gets_carry_no_message_then_polling_continues() -> None:
    """GETs returning only ``typing`` events are empty replies for the provider (it filters them
    out): the base keeps polling on the fake clock until the message event shows up."""
    api = AcmeThreadsApi(
        {
            "user_request": fenced_chunks(
                discovery_plan(THREAD, tasks=[cmd_task("t1", CMD_UNAME)])
            ),
            "execution_result": fenced_chunks(final_answer(THREAD)),
        },
        empty_gets=3,
    )
    app, _, executor = build(api)
    executor.script(cmd=CMD_UNAME, stdout=OUT_UNAME)

    session = await app.manager.start_session(goal=GOAL, user_message=USER_MESSAGE)
    session = await app.manager.wait(session.session_id, timeout_ms=10_000)

    assert session.status is SessionState.COMPLETED
    assert len(api.gets) == 2 * (3 + 1)  # two replies, each after three empty polls
    assert app.manager.final_answer(session.session_id) is not None
