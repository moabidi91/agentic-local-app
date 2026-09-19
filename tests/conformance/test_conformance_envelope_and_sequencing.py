"""Batterie de conformité protocolaire — enveloppe, séquencement, forme brute, politique de suite.

Ce module répond à une question : **que fait l'application quand le modèle ne respecte pas le
protocole ?** Chaque test rejoue une faute précise sur la vraie boucle (``ProtocolOrchestrator``,
``ProtocolAdapter``, ``FailureManager``, ``RotationCoordinator``, persistance et audit réels ; seuls
le réseau, le shell, l'horloge et les identifiants sont des doubles) et vérifie le code d'erreur, ce
qui est persisté, ce qui est publié et ce qu'il advient de la session. Chaque test porte un
``@case`` qui alimente ``docs/reports/conformance-protocole.md``.

Ordre de lecture (quatre familles, dans l'ordre où l'application les rencontre) :

1. **Enveloppe et forme du message** — la réponse est bien un message, mais sa forme est fausse :
   enveloppe non-objet, champ manquant ou vide, ``content`` mal typé, type inconnu, type réservé à
   l'application, mauvaise conversation, identifiant déjà vu, deux messages dans un seul GET,
   absence de réponse.
2. **Séquencement des messages** — la forme est bonne mais le message arrive au mauvais moment :
   grammaire ADR-007 amendée par ADR-022, y compris pendant une rotation (``context_resume_ack``).
3. **Forme brute de la réponse** — la réponse n'atteint même pas l'adaptateur : le codec
   (``transport.codec = "json_text"``, ADR-021) ne sait pas en extraire d'enveloppe.
4. **Politique appliquée** — la trace complète d'une erreur de protocole de bout en bout : rejet et
   échec, rejet et rotation en ``WARNING``, absence de rejeu, borne de la rotation.

Les écarts constatés sont portés par le verdict du ``@case`` correspondant, jamais corrigés ici.
"""

from __future__ import annotations

import copy
import json
from typing import Any

import pytest

from agentic_local_app.config import AppConfig, ProtocolSection, TransportSection
from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.domain.states import (
    ContextWindowState,
    ConversationState,
    CycleState,
    MessageDirection,
    MessageType,
    SessionState,
)
from agentic_local_app.observability.event_bus import EventBus, RecordingSubscriber
from agentic_local_app.orchestration import build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.protocol.adapter import render_instructions
from agentic_local_app.testing.fake_executor import FakeCommandExecutor
from agentic_local_app.transport.codecs import UNPARSEABLE_REPLY
from agentic_local_app.transport.fake import FakeTransportGateway
from conformance.harness import make_config, make_rig
from conformance.registry import case
from integration.phase9_rig import (
    REMOTE_1,
    REMOTE_2,
    Rig,
    advancing_sleep,
    discovery_plan,
    execution_plan,
    final_answer,
    resume_ack,
    user_response,
)

pytestmark = [pytest.mark.conformance]

#: The identifiers the sequential generator hands out first (the rig is deterministic, ADR-017).
SESSION = "sess-0001"
CONV = "conv-0001"
CHILD = "conv-0002"

#: The expectation rows of ADR-007 amended by ADR-022, as ``details["expected"]`` renders them.
EXPECTED_INITIAL = ["discovery_plan", "user_response"]
EXPECTED_INITIAL_STRICT = ["discovery_plan"]
EXPECTED_AFTER_RESULT = [
    "execution_plan",
    "final_answer",
    "priority_clarification",
    "user_response",
]
EXPECTED_FOLLOW_UP = [
    "discovery_plan",
    "execution_plan",
    "final_answer",
    "priority_clarification",
    "user_response",
]
EXPECTED_DURING_ROTATION = ["context_resume_ack"]


# ================================================================================================
# helpers
# ================================================================================================
def plan_message(**overrides: Any) -> dict[str, Any]:
    """The §12.2 ``discovery_plan`` with envelope fields replaced (``None`` removes the field)."""
    message = copy.deepcopy(discovery_plan())
    for key, value in overrides.items():
        if value is None:
            message.pop(key, None)
        else:
            message[key] = value
    return message


def inbound_records(rig: Rig, conversation_id: str = CONV) -> list[Any]:
    return [
        message
        for message in rig.store.list_messages(conversation_id)
        if message.direction is MessageDirection.INBOUND
    ]


def only_failure(rig: Rig, session_id: str = SESSION) -> Any:
    failures = rig.store.list_failures(session_id)
    assert len(failures) == 1, [f.error_code for f in failures]
    return failures[0]


async def run_rejecting(rig: Rig, *messages: Any, **start: Any) -> Any:
    """Queue one reply batch per message, run the session to its end, return the session record."""
    for message in messages:
        rig.transport.enqueue_messages(REMOTE_1, [message])
    return await rig.run(**start)


def assert_rejected_then_failed(rig: Rig, code: str) -> Any:
    """The common trail of a protocol error in a HEALTHY window: rejected, recorded, session FAILED.

    Returns the ``FailureRecord`` so the caller can assert on its ``details``.
    """
    session = rig.session(SESSION)
    assert session.status is SessionState.FAILED
    failure = only_failure(rig)
    assert (failure.error_type, failure.error_code) == (ErrorType.MODEL_PROTOCOL_ERROR, code)
    assert failure.retryable is False
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.FAILED
    assert conversation.protocol_error_count == 1
    assert conversation.last_model_response_state == "received_invalid"
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == code
    assert rig.events(EventType.MESSAGE_INBOUND) == []
    assert len(rig.transport.get_calls) == 1  # a protocol error is never replayed (§7.2)
    assert rig.app.audit.verify(SESSION).valid is True
    return failure


def make_codec_rig(
    codec_options: dict[str, Any] | None = None,
    *,
    config: AppConfig | None = None,
) -> Rig:
    """The phase 9 rig with ``transport.codec = "json_text"`` applied by the wiring (ADR-021)."""
    base = config or make_config()
    cfg = base.model_copy(
        update={"transport": TransportSection(codec="json_text", codec_options=codec_options or {})}
    )
    clock = FakeClock()
    ids = SequentialIdGenerator()
    store = InMemoryConversationStore()
    transport = FakeTransportGateway(clock, reply_timeout_ms=120_000)
    executor = FakeCommandExecutor(clock)
    bus = EventBus()
    recorder = RecordingSubscriber()
    bus.subscribe(recorder, name="conformance-recorder")
    app = build_application(
        cfg,
        store=store,
        transport=transport,
        executor=executor,
        clock=clock,
        ids=ids,
        bus=bus,
        run_recovery=False,
        sleep=advancing_sleep(clock),
        translator=ShellTranslator(ShellDialect.POSIX),  # scripted machine, ADR-030
    )
    return Rig(
        app=app,
        config=cfg,
        clock=clock,
        ids=ids,
        store=store,
        transport=transport,
        executor=executor,
        recorder=recorder,
    )


async def run_undecodable(rig: Rig, raw: Any) -> Any:
    """Queue one raw item the codec must choke on, run the session, return the ``FailureRecord``."""
    rig.transport.enqueue_messages(REMOTE_1, [raw])
    session = await rig.run()
    assert session.status is SessionState.FAILED
    failure = only_failure(rig)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        UNPARSEABLE_REPLY,
    )
    assert failure.origin == "TransportGateway" and failure.retryable is False
    return failure


def assert_raw_excerpt_persisted(rig: Rig, excerpt: str, reason: str) -> None:
    """La trace d'une réponse que le codec n'a pas su lire (ADR-021 §2 amendé par ADR-023).

    Il n'y a pas d'enveloppe à enregistrer, mais il y a une réponse : elle est persistée sous le
    type interne `system_error` avec l'extrait brut et la raison du codec, comptée dans
    `protocol_error_count` et publiée en `message.rejected`, exactement comme une enveloppe
    refusée. Le curseur, lui, ne bouge pas : une réponse illisible n'a pas d'identifiant.
    """
    records = inbound_records(rig)
    assert len(records) == 1
    record = records[0]
    assert record.message_type is MessageType.SYSTEM_ERROR
    assert record.validation_status == "invalid"
    assert record.payload == {"raw": excerpt, "reason": reason}
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1
    assert rejected[0].payload["error_code"] == UNPARSEABLE_REPLY
    assert rejected[0].payload["message_type"] is None
    conversation = rig.conversation(CONV)
    assert conversation.protocol_error_count == 1
    assert conversation.get_cursor is None
    assert conversation.last_model_response_state == "received_invalid"


async def completed_then_warning(rig: Rig) -> str:
    """Run the §12 scenario to completion, then push the reusable conversation into WARNING.

    The window is the ADR-019 §2 precondition for "one rotation instead of a failure"; the recorder
    is cleared so the caller only sees the events of the follow-up turn.
    """
    rig.script_java_scenario()
    session = await rig.run()
    assert session.status is SessionState.COMPLETED
    warning_bytes = rig.app.monitor.thresholds().warning_bytes
    rig.app.lifecycle.update_conversation(CONV, context_bytes=warning_bytes + 1_000)
    rig.recorder.clear()
    return session.session_id


#: The reply that is out of grammar in the follow-up row, used to trigger a rotation in WARNING.
def out_of_grammar_follow_up() -> dict[str, Any]:
    return resume_ack(REMOTE_1, "remote-0000", message_id="model-msg-0009")


# ================================================================================================
# 1. Enveloppe et forme du message (§12, ADR-007)
# ================================================================================================
ENVELOPE = "Enveloppe et forme du message"


@case(
    "env-array-reply",
    category=ENVELOPE,
    sends="un tableau JSON à la place de l'objet enveloppe",
    expects="rejet à l'étape enveloppe ; la forme brute est persistée sous `raw`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007 · ADR-015",
)
async def given_initial_user_request_when_model_replies_with_a_json_array_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, [1, 2, 3])

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["stage"] == "envelope"
    # the raw form is kept: it is the only trace of what the model answered
    records = inbound_records(rig)
    assert len(records) == 1
    assert records[0].validation_status == "invalid"
    assert records[0].payload == {"raw": [1, 2, 3]}
    assert records[0].message_type is MessageType.SYSTEM_ERROR  # no readable type in the reply


@case(
    "env-string-reply",
    category=ENVELOPE,
    sends="une chaîne de caractères à la place de l'objet enveloppe (codec passthrough)",
    expects="rejet à l'étape enveloppe ; le texte est persisté sous `raw`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007 · ADR-015",
)
async def given_initial_user_request_when_model_replies_with_a_bare_string_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, "I cannot produce a plan right now.")

    assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    records = inbound_records(rig)
    assert len(records) == 1
    assert records[0].payload == {"raw": "I cannot produce a plan right now."}


@case(
    "env-scalar-reply",
    category=ENVELOPE,
    sends="un nombre, puis null, à la place de l'objet enveloppe",
    expects="rejet à l'étape enveloppe ; le scalaire est persisté sous `raw`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007 · ADR-015",
)
async def given_initial_user_request_when_model_replies_with_a_scalar_then_schema_invalid() -> None:
    for scalar in (123, None):
        rig = make_rig()

        await run_rejecting(rig, scalar)

        assert_rejected_then_failed(rig, "SCHEMA_INVALID")
        records = inbound_records(rig)
        assert len(records) == 1 and records[0].payload == {"raw": scalar}


@case(
    "env-missing-type",
    category=ENVELOPE,
    sends="une enveloppe sans champ `type`",
    expects="rejet à l'étape enveloppe, `details.errors` pointe `type`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007",
)
async def given_initial_user_request_when_envelope_has_no_type_then_schema_invalid_on_the_envelope() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(type=None))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["stage"] == "envelope"
    assert failure.details["message_type"] is None
    assert [error["loc"] for error in failure.details["errors"]] == ["type"]
    # an unreadable type is persisted under the internal system_error type (§12.10)
    assert inbound_records(rig)[0].message_type is MessageType.SYSTEM_ERROR


@case(
    "env-missing-conversation-id",
    category=ENVELOPE,
    sends="une enveloppe sans `conversation_id`",
    expects="rejet à l'étape enveloppe, `details.errors` pointe `conversation_id`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-004",
)
async def given_initial_user_request_when_envelope_has_no_conversation_id_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(conversation_id=None))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["stage"] == "envelope"
    assert [error["loc"] for error in failure.details["errors"]] == ["conversation_id"]
    assert rig.conversation(CONV).get_cursor == "model-msg-0001"


@case(
    "env-missing-message-id",
    category=ENVELOPE,
    sends="une enveloppe sans `message_id`",
    expects="rejet ; le rejet reçoit un identifiant local et le curseur ne bouge pas",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-004 · ADR-017",
)
async def given_initial_user_request_when_envelope_has_no_message_id_then_schema_invalid_and_local_id_assigned() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(message_id=None))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert [error["loc"] for error in failure.details["errors"]] == ["message_id"]
    persisted = inbound_records(rig)[0]
    assert persisted.message_id.startswith("msg-")  # generated locally, never invented from nothing
    assert rig.events(EventType.MESSAGE_REJECTED)[0].payload["message_id"] is None
    assert rig.conversation(CONV).get_cursor is None  # nothing to advance the cursor to


@case(
    "env-empty-message-id",
    category=ENVELOPE,
    sends="une enveloppe dont le `message_id` est la chaîne vide",
    expects="rejet ; pydantic signale `string_too_short` sur `message_id`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-004",
)
async def given_initial_user_request_when_message_id_is_empty_then_schema_invalid() -> None:
    rig = make_rig()

    await run_rejecting(rig, plan_message(message_id=""))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["errors"] == [
        {
            "loc": "message_id",
            "type": "string_too_short",
            "msg": "String should have at least 1 character",
        }
    ]


@case(
    "env-missing-content",
    category=ENVELOPE,
    sends="une enveloppe sans `content`",
    expects="rejet à l'étape enveloppe, `details.errors` pointe `content`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007",
)
async def given_initial_user_request_when_envelope_has_no_content_then_schema_invalid() -> None:
    rig = make_rig()

    await run_rejecting(rig, plan_message(content=None))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["stage"] == "envelope"
    assert [error["loc"] for error in failure.details["errors"]] == ["content"]


@case(
    "env-content-not-an-object",
    category=ENVELOPE,
    sends="un `content` qui est une chaîne, puis une liste",
    expects="rejet à l'étape enveloppe : `content` doit être un objet",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007",
)
async def given_initial_user_request_when_content_is_not_an_object_then_schema_invalid() -> None:
    for content in ("a plan", [1, 2]):
        rig = make_rig()

        await run_rejecting(rig, plan_message(content=content))

        failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
        assert failure.details["stage"] == "envelope"
        assert failure.details["errors"] == [
            {"loc": "content", "type": "dict_type", "msg": "Input should be a valid dictionary"}
        ]
        assert inbound_records(rig)[0].payload["content"] == content


@case(
    "env-unknown-type",
    category=ENVELOPE,
    sends="un `type` qui n'existe pas (`plan`)",
    expects="rejet à l'étape enveloppe : `type` hors énumération",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§3.5 · §12 · ADR-007",
)
async def given_initial_user_request_when_type_is_unknown_then_schema_invalid_on_the_enum() -> None:
    rig = make_rig()

    await run_rejecting(rig, plan_message(type="plan"))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["message_type"] == "plan"
    assert [error["type"] for error in failure.details["errors"]] == ["enum"]
    assert rig.events(EventType.MESSAGE_REJECTED)[0].payload["message_type"] == "plan"
    assert inbound_records(rig)[0].message_type is MessageType.SYSTEM_ERROR


@case(
    "env-type-wrong-case",
    category=ENVELOPE,
    sends="un `type` dans la mauvaise casse (`FINAL_ANSWER`)",
    expects="rejet : l'énumération du §12 est sensible à la casse, aucune normalisation",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007",
)
async def given_initial_user_request_when_type_is_upper_case_then_schema_invalid() -> None:
    rig = make_rig()

    await run_rejecting(rig, plan_message(type="FINAL_ANSWER"))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["message_type"] == "FINAL_ANSWER"
    assert [error["loc"] for error in failure.details["errors"]] == ["type"]


@case(
    "env-extra-top-level-field",
    category=ENVELOPE,
    sends="une enveloppe valide plus un champ de tête inconnu (`priority`)",
    expects="rejet : l'enveloppe du §12 est fermée (`extra = forbid`)",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12 · ADR-007",
)
async def given_initial_user_request_when_envelope_carries_an_unknown_field_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(priority="high"))

    failure = assert_rejected_then_failed(rig, "SCHEMA_INVALID")
    assert failure.details["errors"] == [
        {"loc": "priority", "type": "extra_forbidden", "msg": "Extra inputs are not permitted"}
    ]


@case(
    "env-system-error-inbound",
    category=ENVELOPE,
    sends="un `system_error`, type interne que le modèle ne doit jamais émettre",
    expects="rejet avec un code dédié et `details.inbound = false`",
    code="SYSTEM_ERROR_NOT_ALLOWED_INBOUND",
    policy="échec",
    ref="§12.10 · ADR-007",
)
async def given_initial_user_request_when_model_sends_system_error_then_refused_with_dedicated_code() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(type="system_error"))

    failure = assert_rejected_then_failed(rig, "SYSTEM_ERROR_NOT_ALLOWED_INBOUND")
    assert failure.details["received"] == "system_error"
    assert failure.details["inbound"] is False
    assert failure.details["expected"] == EXPECTED_INITIAL


@case(
    "env-application-only-types",
    category=ENVELOPE,
    sends="`user_request`, `execution_result` ou `context_resume_request` (types sortants)",
    expects="rejet : ces types ne sont jamais entrants, `details.inbound = false`",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§3.5 · ADR-007",
)
async def given_initial_user_request_when_model_sends_an_application_only_type_then_refused() -> (
    None
):
    for message_type in ("user_request", "execution_result", "context_resume_request"):
        rig = make_rig()

        await run_rejecting(rig, plan_message(type=message_type))

        failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
        assert failure.details["received"] == message_type
        assert failure.details["inbound"] is False
        assert failure.details["expected"] == EXPECTED_INITIAL
        # the type is known, so the rejected record keeps it (unlike an unknown type)
        assert inbound_records(rig)[0].message_type is MessageType(message_type)


@case(
    "env-conversation-mismatch",
    category=ENVELOPE,
    sends="une enveloppe dont le `conversation_id` n'est pas celui de la conversation",
    expects="rejet ; `details` porte le reçu et l'attendu",
    code="CONVERSATION_MISMATCH",
    policy="échec",
    ref="§12 · ADR-004 · ADR-007",
)
async def given_initial_user_request_when_conversation_id_is_another_one_then_conversation_mismatch() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, plan_message(conversation_id="remote-9999"))

    failure = assert_rejected_then_failed(rig, "CONVERSATION_MISMATCH")
    assert failure.details == {
        "received": "remote-9999",
        "expected": REMOTE_1,
        "message_id": "model-msg-0001",
    }


@case(
    "env-duplicate-message-id",
    category=ENVELOPE,
    sends="un second message réutilisant un `message_id` déjà vu dans la session",
    expects="rejet ; le rejet est persisté sous un identifiant local, l'original est intact",
    code="DUPLICATE_MESSAGE_ID",
    policy="échec",
    ref="§12 · ADR-007 · ADR-017",
)
async def given_accepted_plan_when_model_reuses_its_message_id_then_duplicate_message_id() -> None:
    rig = make_rig()
    rig.script_spec_outputs()

    await run_rejecting(
        rig,
        discovery_plan(),
        discovery_plan(message_id="model-msg-0001", plan_id="plan-2"),
    )

    failure = only_failure(rig)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "DUPLICATE_MESSAGE_ID",
    )
    assert failure.details == {"message_id": "model-msg-0001"}
    assert rig.session(SESSION).status is SessionState.FAILED
    statuses = [(record.message_id, record.validation_status) for record in inbound_records(rig)]
    assert statuses[0] == ("model-msg-0001", "valid")
    assert statuses[1][1] == "invalid" and statuses[1][0].startswith("msg-")
    assert rig.events(EventType.MESSAGE_REJECTED)[0].payload["message_id"] == "model-msg-0001"


@case(
    "env-two-messages-one-get",
    category=ENVELOPE,
    sends="deux messages dans un seul GET",
    expects="rejet ; `details` nomme les deux identifiants et les deux types",
    code="UNEXPECTED_EXTRA_MESSAGE",
    policy="échec",
    ref="§12 · ADR-004 · ADR-007",
)
async def given_initial_user_request_when_one_get_carries_two_messages_then_unexpected_extra_message() -> (
    None
):
    rig = make_rig()
    first = discovery_plan()
    second = discovery_plan(message_id="model-msg-0002", plan_id="plan-2")
    rig.transport.enqueue_messages(REMOTE_1, [first, second])

    await rig.run()

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_EXTRA_MESSAGE")
    assert failure.details == {
        "expected": 1,
        "received": 2,
        "message_ids": ["model-msg-0001", "model-msg-0002"],
        "types": ["discovery_plan", "discovery_plan"],
    }
    # the whole batch is kept, not only its first message (ADR-015: persist what was received)
    persisted = inbound_records(rig)[0]
    assert persisted.payload == {"messages": [first, second]}
    assert persisted.size_bytes == size_bytes(first) + size_bytes(second)
    assert rig.conversation(CONV).get_cursor == "model-msg-0002"


@case(
    "env-no-reply-at-all",
    category=ENVELOPE,
    sends="rien du tout : le modèle ne dépose aucun message",
    expects="GET rejoué selon §7.3 puis échec en TIMEOUT, jamais en erreur de protocole",
    code="MODEL_GET_TIMEOUT",
    policy="échec après épuisement des tentatives",
    ref="§7.1 · §7.3 · ADR-004",
)
async def given_initial_user_request_when_model_never_replies_then_session_fails_on_get_timeout() -> (
    None
):
    rig = make_rig(
        make_config(retry={"max_attempts": 3, "base_delay_ms": 10}), reply_timeout_ms=1_000
    )

    session = await rig.run()

    assert session.status is SessionState.FAILED
    failures = rig.store.list_failures(session.session_id)
    assert {(f.error_type, f.error_code) for f in failures} == {
        (ErrorType.TIMEOUT_ERROR, "MODEL_GET_TIMEOUT")
    }
    assert (
        len(failures) == 3 and len(rig.transport.get_calls) == 3
    )  # retried, unlike a protocol error
    assert [d.decision for d in rig.store.list_retry_decisions(session.session_id)] == [
        "retry",
        "retry",
        "fail",
    ]
    conversation = rig.conversation(CONV)
    assert conversation.protocol_error_count == 0
    assert inbound_records(rig) == []
    assert rig.events(EventType.MESSAGE_REJECTED) == []


@case(
    "env-empty-batch",
    category=ENVELOPE,
    sends="un GET qui rend un lot vide (aucun message) au lieu d'attendre",
    expects="rejet comme réponse inutilisable ; la boucle ne plante pas",
    code="EMPTY_REPLY",
    policy="échec",
    ref="§3.12 · ADR-004 · ADR-020",
)
async def given_initial_user_request_when_get_returns_an_empty_batch_then_empty_reply() -> None:
    """``wait_for_reply`` promet au moins un message : un provider tiers (ADR-020) dont le
    long-poll rend une page vide casse ce contrat, et c'est traité comme toute réponse
    inutilisable — jamais comme une erreur de programmation."""
    rig = make_rig()
    rig.transport.enqueue_messages(REMOTE_1, [])

    await rig.run()

    failure = only_failure(rig)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "EMPTY_REPLY",
    )
    assert failure.retryable is False
    assert failure.details["expected"] == 1 and failure.details["received"] == 0
    assert rig.session(SESSION).status is SessionState.FAILED
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.FAILED
    assert conversation.protocol_error_count == 1
    # nothing was sent, so the persisted record keeps an empty batch rather than a fake message
    records = inbound_records(rig)
    assert len(records) == 1 and records[0].payload == {"messages": []}
    assert rig.app.audit.verify(SESSION).valid is True


# ================================================================================================
# 2. Séquencement des messages — la grammaire d'ADR-007 amendée par ADR-022
# ================================================================================================
SEQUENCING = "Séquencement des messages"


@case(
    "seq-execution-plan-first",
    category=SEQUENCING,
    sends="un `execution_plan` en réponse au `user_request` initial",
    expects="rejet ; `details.expected` ne liste que discovery_plan et user_response",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§14 · ADR-007 · ADR-022",
)
async def given_initial_user_request_when_model_sends_execution_plan_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, execution_plan())

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
    assert failure.details["received"] == "execution_plan"
    assert failure.details["expected"] == EXPECTED_INITIAL
    assert failure.details["inbound"] is True
    assert rig.store.list_plans(SESSION) == []  # an unexpected plan is never projected


@case(
    "seq-priority-clarification-first",
    category=SEQUENCING,
    sends="une `priority_clarification` en réponse au `user_request` initial",
    expects="rejet : la première réponse ne peut pas être une clarification",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§14 · ADR-007",
)
async def given_initial_user_request_when_model_sends_priority_clarification_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, execution_plan(message_type="priority_clarification"))

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
    assert failure.details["received"] == "priority_clarification"
    assert failure.details["expected"] == EXPECTED_INITIAL


@case(
    "seq-final-answer-first",
    category=SEQUENCING,
    sends="un `final_answer` en réponse au `user_request` initial",
    expects="rejet : conclure sans plan passe par un `user_response` (ADR-022), pas un final_answer",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§11 · §14 · ADR-007 · ADR-022",
)
async def given_initial_user_request_when_model_sends_final_answer_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, final_answer())

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
    assert failure.details["received"] == "final_answer"
    assert failure.details["expected"] == EXPECTED_INITIAL
    assert rig.session(SESSION).final_answer is None
    assert rig.events(EventType.FINAL_ANSWER_RECEIVED) == []


@case(
    "seq-context-resume-ack-first",
    category=SEQUENCING,
    sends="un `context_resume_ack` en réponse au `user_request` initial",
    expects="rejet : un ack n'existe que face à un `context_resume_request`",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§12.9 · ADR-007 · ADR-014",
)
async def given_initial_user_request_when_model_sends_context_resume_ack_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()

    await run_rejecting(rig, resume_ack(REMOTE_1, REMOTE_1, message_id="model-ack-0001"))

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
    assert failure.details["received"] == "context_resume_ack"
    assert failure.details["expected"] == EXPECTED_INITIAL
    assert rig.session(SESSION).rotations_count == 0


@case(
    "seq-user-response-first-allowed",
    category=SEQUENCING,
    sends="un `user_response` en réponse au `user_request` initial (drapeau par défaut)",
    expects="accepté : ADR-022 élargit la ligne initiale, la session se termine sans plan",
    code="— (accepté)",
    policy="accepté",
    ref="§14 · ADR-007 · ADR-022",
)
async def given_initial_user_request_when_model_answers_directly_then_accepted_by_default() -> None:
    rig = make_rig()

    session = await run_rejecting(rig, user_response())

    assert session.status is SessionState.COMPLETED
    assert rig.store.list_failures(SESSION) == []
    assert rig.events(EventType.MESSAGE_REJECTED) == []
    assert [record.message_type for record in inbound_records(rig)] == [MessageType.USER_RESPONSE]
    assert rig.conversation(CONV).protocol_error_count == 0


@case(
    "seq-user-response-first-strict",
    category=SEQUENCING,
    sends="un `user_response` initial avec `protocol.allow_direct_response = false`",
    expects="rejet ; `details.expected` retombe sur le seul `discovery_plan` du §14",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§14 · ADR-022",
)
async def given_strict_direct_response_flag_when_model_answers_directly_then_unexpected_message_type() -> (
    None
):
    base = make_config()
    rig = make_rig(
        base.model_copy(
            update={
                "protocol": ProtocolSection(allow_direct_response=False, max_correction_attempts=0)
            }
        )
    )

    await run_rejecting(rig, user_response())

    failure = assert_rejected_then_failed(rig, "UNEXPECTED_MESSAGE_TYPE")
    assert failure.details["received"] == "user_response"
    assert failure.details["expected"] == EXPECTED_INITIAL_STRICT


@case(
    "seq-discovery-plan-after-result",
    category=SEQUENCING,
    sends="un `discovery_plan` après un `execution_result`",
    expects="rejet : la découverte n'a lieu qu'au premier tour",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§14 · ADR-007",
)
async def given_execution_result_sent_when_model_sends_discovery_plan_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()
    rig.script_spec_outputs()

    await run_rejecting(
        rig,
        discovery_plan(),
        discovery_plan(message_id="model-msg-0002", plan_id="plan-2"),
    )

    failure = only_failure(rig)
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert failure.details["received"] == "discovery_plan"
    assert failure.details["expected"] == EXPECTED_AFTER_RESULT
    assert rig.session(SESSION).status is SessionState.FAILED
    assert [plan.plan_id for plan in rig.store.list_plans(SESSION)] == ["plan-0"]


@case(
    "seq-context-resume-ack-after-result",
    category=SEQUENCING,
    sends="un `context_resume_ack` après un `execution_result`",
    expects="rejet : hors rotation, l'ack n'est jamais attendu",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec",
    ref="§12.9 · ADR-007 · ADR-014",
)
async def given_execution_result_sent_when_model_sends_context_resume_ack_then_unexpected_message_type() -> (
    None
):
    rig = make_rig()
    rig.script_spec_outputs()

    await run_rejecting(
        rig,
        discovery_plan(),
        resume_ack(REMOTE_1, REMOTE_1, message_id="model-ack-0001"),
    )

    failure = only_failure(rig)
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert failure.details["received"] == "context_resume_ack"
    assert failure.details["expected"] == EXPECTED_AFTER_RESULT
    assert rig.session(SESSION).rotations_count == 0


@case(
    "seq-follow-up-row-is-wider",
    category=SEQUENCING,
    sends="une `priority_clarification` après un `user_request` de relance",
    expects="accepté : la ligne « relance » d'ADR-007 accepte les trois plans, final_answer et user_response",
    code="— (accepté)",
    policy="accepté",
    ref="§11 · §14 · ADR-007 · ADR-022",
)
async def given_follow_up_user_request_when_model_sends_priority_clarification_then_accepted() -> (
    None
):
    rig = make_rig()
    rig.script_java_scenario()
    session = await rig.run()
    sid = session.session_id
    assert session.status is SessionState.COMPLETED
    rig.recorder.clear()
    rig.reply(
        REMOTE_1,
        execution_plan(
            message_id="model-msg-0004",
            plan_id="plan-2",
            message_type="priority_clarification",
            tasks=[{"task_id": "t8", "type": "cmd", "cmd": "echo $JAVA_HOME"}],
        ),
        final_answer(message_id="model-msg-0005"),
    )
    rig.executor.script(cmd="echo $JAVA_HOME", stdout=b"/usr/lib/jvm/java-17")

    await rig.manager.continue_session(sid, "Which module should I look at first?")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert rig.store.list_failures(sid) == []
    assert rig.events(EventType.MESSAGE_REJECTED) == []
    assert [plan.plan_type.value for plan in rig.store.list_plans(sid)] == [
        "discovery_plan",
        "execution_plan",
        "priority_clarification",
    ]


@case(
    "seq-plan-instead-of-ack",
    category=SEQUENCING,
    sends="un plan au lieu du `context_resume_ack` attendu dans la conversation enfant",
    expects="rejet dans l'enfant ; `details.expected` ne liste que context_resume_ack",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec de la rotation, session FAILED (raison `rotation_failed`)",
    ref="§10 · §12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_child_replies_with_a_plan_then_rotation_fails() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, discovery_plan(REMOTE_2, message_id="model-msg-0011", plan_id="plan-7"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    assert ended.rotations_count == 1
    failures = rig.store.list_failures(sid)
    assert [f.error_code for f in failures] == [
        "UNEXPECTED_MESSAGE_TYPE",  # the follow-up reply, which triggered the rotation
        "UNEXPECTED_MESSAGE_TYPE",  # the child's reply to the resume request
    ]
    assert failures[1].details["received"] == "discovery_plan"
    assert failures[1].details["expected"] == EXPECTED_DURING_ROTATION
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "rotation_failed"
    child = rig.conversation(CHILD)
    assert child.status is ConversationState.FAILED
    assert child.protocol_error_count == 1
    assert rig.conversation(CONV).status is ConversationState.FAILED


@case(
    "seq-final-answer-instead-of-ack",
    category=SEQUENCING,
    sends="un `final_answer` au lieu du `context_resume_ack` attendu",
    expects="rejet : la conclusion n'est pas une confirmation de reprise",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec de la rotation, session FAILED",
    ref="§10 · §12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_child_replies_with_final_answer_then_rotation_fails() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, final_answer(REMOTE_2, message_id="model-msg-0011"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    assert ended.final_answer is None  # a final_answer out of grammar never concludes
    failure = rig.store.list_failures(sid)[-1]
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert failure.details["received"] == "final_answer"
    assert failure.details["expected"] == EXPECTED_DURING_ROTATION


@case(
    "seq-user-response-instead-of-ack",
    category=SEQUENCING,
    sends="un `user_response` au lieu du `context_resume_ack` attendu",
    expects="rejet : ADR-022 n'élargit pas la ligne « rotation »",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec de la rotation, session FAILED",
    ref="§10 · §12.9 · ADR-014 · ADR-022",
)
async def given_context_resume_request_sent_when_child_replies_with_user_response_then_rotation_fails() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, user_response(REMOTE_2, message_id="model-msg-0011"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    failure = rig.store.list_failures(sid)[-1]
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert failure.details["received"] == "user_response"
    assert failure.details["expected"] == EXPECTED_DURING_ROTATION


@case(
    "seq-ack-wrong-original",
    category=SEQUENCING,
    sends="un `context_resume_ack` dont l'`original_conversation_id` n'est pas celui du parent",
    expects="rejet ; `details` porte le reçu et l'attendu",
    code="ACK_WRONG_ORIGINAL",
    policy="échec de la rotation, session FAILED",
    ref="§12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_ack_names_another_conversation_then_ack_wrong_original() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, resume_ack(REMOTE_2, "remote-9999", message_id="model-ack-0002"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    failure = rig.store.list_failures(sid)[-1]
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "ACK_WRONG_ORIGINAL",
    )
    assert failure.details == {"received": "remote-9999", "expected": REMOTE_1}
    assert rig.conversation(CHILD).status is ConversationState.FAILED
    assert rig.conversation(CONV).status is ConversationState.FAILED  # the ROTATING parent too


@case(
    "seq-ack-not-acknowledged",
    category=SEQUENCING,
    sends="un `context_resume_ack` avec `acknowledged = false`",
    expects="rejet : le modèle refuse la reprise, la rotation ne peut pas se terminer",
    code="ACK_NOT_ACKNOWLEDGED",
    policy="échec de la rotation, session FAILED",
    ref="§12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_ack_is_negative_then_ack_not_acknowledged() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, resume_ack(REMOTE_2, acknowledged=False, message_id="model-ack-0002"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    failure = rig.store.list_failures(sid)[-1]
    assert failure.error_code == "ACK_NOT_ACKNOWLEDGED"
    assert failure.details == {"original_conversation_id": REMOTE_1}
    assert rig.events(EventType.ROTATION_COMPLETED) == []
    assert rig.conversation(CHILD).context_window_state is ContextWindowState.SATURATED


@case(
    "seq-rejected-ack-trail",
    category=SEQUENCING,
    sends="un `context_resume_ack` refusé pendant une rotation (trace laissée)",
    expects=(
        "le rejet est compté, publié, **persisté** comme ailleurs, et le cycle `resume` de "
        "l'enfant est clos en FAILED"
    ),
    code="ACK_NOT_ACKNOWLEDGED",
    policy="échec de la rotation, session FAILED",
    ref="§10 · §16 · ADR-014 · ADR-015",
)
async def given_rotation_rejected_on_the_ack_when_session_fails_then_reply_persisted_and_cycle_failed() -> (
    None
):
    """Le chemin de rejet du `RotationCoordinator` laisse la même trace que celui de
    l'orchestrateur : sans le `MessageRecord`, la réponse fautive du modèle serait invisible
    après coup, et un cycle `resume` laissé RUNNING derrière une session FAILED serait un état
    incohérent."""
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, resume_ack(REMOTE_2, acknowledged=False, message_id="model-ack-0002"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    child = rig.conversation(CHILD)
    assert child.protocol_error_count == 1
    assert child.get_cursor == "model-ack-0002"
    assert child.last_model_response_state == "received_invalid"
    records = inbound_records(rig, CHILD)
    assert len(records) == 1
    assert records[0].message_id == "model-ack-0002"
    assert records[0].validation_status == "invalid"
    assert records[0].message_type is MessageType.CONTEXT_RESUME_ACK
    assert child.last_inbound_message_id == "model-ack-0002"
    rejected = [
        event for event in rig.events(EventType.MESSAGE_REJECTED) if event.conversation_id == CHILD
    ]
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == "ACK_NOT_ACKNOWLEDGED"
    # the resume cycle of the child is closed, not left running behind a failed session
    child_cycles = rig.cycles(CHILD)
    assert [(cycle.cycle_type.value, cycle.status) for cycle in child_cycles] == [
        ("resume", CycleState.FAILED)
    ]
    assert rig.app.audit.verify(sid).valid is True


# ================================================================================================
# 3. Forme brute de la réponse — avant même l'adaptateur (ADR-021)
# ================================================================================================
RAW = "Forme brute de la réponse"

PROSE = "I am unable to produce a plan right now, sorry."


@case(
    "raw-no-json",
    category=RAW,
    sends="de la prose sans le moindre JSON (codec json_text)",
    expects=(
        "erreur de transport avec `reason = no_json_found` ; l'extrait brut est persisté en "
        "entrant sous `system_error` et compté (ADR-021 §2 amendé par ADR-023)"
    ),
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-021 · ADR-023",
)
async def given_json_text_codec_when_reply_has_no_json_then_unparseable_reply_with_raw_excerpt() -> (
    None
):
    rig = make_codec_rig()

    failure = await run_undecodable(rig, PROSE)

    assert failure.details["reason"] == "no_json_found"
    assert failure.details["codec"] == "json_text"
    assert failure.details["index"] == 0
    assert failure.details["excerpt"] == PROSE  # quotable back to the model
    assert failure.details["operation"] == "GET"
    assert_raw_excerpt_persisted(rig, PROSE, "no_json_found")


@case(
    "raw-json-unbalanced",
    category=RAW,
    sends="un JSON tronqué (accolade jamais refermée)",
    expects="erreur de transport avec `reason = json_unbalanced`",
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-021 · ADR-023",
)
async def given_json_text_codec_when_reply_is_truncated_then_json_unbalanced() -> None:
    rig = make_codec_rig()
    truncated = '{"type": "discovery_plan", "conversation_id": "remote-0001"'

    failure = await run_undecodable(rig, truncated)

    assert failure.details["reason"] == "json_unbalanced"
    assert failure.details["excerpt"] == truncated
    assert_raw_excerpt_persisted(rig, truncated, "json_unbalanced")


@case(
    "raw-fenced-invalid-json",
    category=RAW,
    sends="un bloc ```json``` dont le contenu n'est pas du JSON valide",
    expects="erreur de transport avec `reason = json_invalid` et l'erreur du parseur",
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-021 · ADR-023",
)
async def given_json_text_codec_when_fenced_block_is_invalid_json_then_json_invalid() -> None:
    rig = make_codec_rig()
    fenced = "```json\n{\"type\": 'discovery_plan',}\n```"

    failure = await run_undecodable(rig, fenced)

    assert failure.details["reason"] == "json_invalid"
    assert "error" in failure.details
    assert_raw_excerpt_persisted(rig, fenced, "json_invalid")


@case(
    "raw-path-not-found",
    category=RAW,
    sends="une réponse dont le `content_path` configuré n'existe pas",
    expects="erreur de transport avec `reason = path_not_found` et le chemin cherché",
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-020 · ADR-021 · ADR-023",
)
async def given_json_text_codec_with_content_path_when_path_is_absent_then_path_not_found() -> None:
    rig = make_codec_rig({"content_path": "choices[0].message.content"})

    failure = await run_undecodable(rig, {"choices": []})

    assert failure.details["reason"] == "path_not_found"
    assert failure.details["path"] == "choices[0].message.content"
    assert_raw_excerpt_persisted(rig, '{"choices":[]}', "path_not_found")


@case(
    "raw-unexpected-type",
    category=RAW,
    sends="un objet brut alors que le codec attend du texte (aucun `content_path`)",
    expects="erreur de transport avec `reason = unexpected_type` et `expected = string`",
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-021 · ADR-023",
)
async def given_json_text_codec_without_content_path_when_item_is_an_object_then_unexpected_type() -> (
    None
):
    rig = make_codec_rig()

    failure = await run_undecodable(rig, {"message": "hello"})

    assert failure.details["reason"] == "unexpected_type"
    assert failure.details["expected"] == "string"
    assert_raw_excerpt_persisted(rig, '{"message":"hello"}', "unexpected_type")


@case(
    "raw-no-envelope",
    category=RAW,
    sends="un texte dont le JSON décodé est un tableau vide",
    expects="le décorateur refuse la réponse : `reason = no_envelope`",
    code="UNPARSEABLE_REPLY",
    policy="échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer",
    ref="§3.12 · ADR-004 · ADR-021 · ADR-023",
)
async def given_json_text_codec_when_decoded_document_is_empty_then_no_envelope() -> None:
    rig = make_codec_rig()

    failure = await run_undecodable(rig, "[]")

    assert failure.details["reason"] == "no_envelope"
    assert failure.details["excerpt"] == "[]"
    assert_raw_excerpt_persisted(rig, "[]", "no_envelope")


@case(
    "raw-two-envelopes-in-one-item",
    category=RAW,
    sends="un seul élément brut qui contient un tableau de deux enveloppes",
    expects="le codec rend deux messages, l'adaptateur les refuse : la chaîne codec → adaptateur tient",
    code="UNEXPECTED_EXTRA_MESSAGE",
    policy="échec ; le lot décodé est persisté comme rejet",
    ref="§3.12 · ADR-007 · ADR-021",
)
async def given_json_text_codec_when_one_item_carries_two_envelopes_then_unexpected_extra_message() -> (
    None
):
    rig = make_codec_rig()
    first = discovery_plan()
    second = discovery_plan(message_id="model-msg-0002", plan_id="plan-2")
    rig.transport.enqueue_messages(REMOTE_1, [json.dumps([first, second])])

    session = await rig.run()

    assert session.status is SessionState.FAILED
    failure = only_failure(rig)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "UNEXPECTED_EXTRA_MESSAGE",
    )
    assert failure.origin == "ProtocolAdapter"  # not the codec: the decoding worked
    assert failure.details["message_ids"] == ["model-msg-0001", "model-msg-0002"]
    assert failure.details["types"] == ["discovery_plan", "discovery_plan"]
    # decoded, so this one *is* persisted and counted, unlike the undecodable replies above
    assert inbound_records(rig)[0].payload == {"messages": [first, second]}
    assert rig.conversation(CONV).protocol_error_count == 1


# ================================================================================================
# 4. Politique appliquée — la trace complète (§7.2 ; ADR-013, ADR-015, ADR-019 §2)
# ================================================================================================
POLICY = "Politique appliquée"


@case(
    "pol-full-trail",
    category=POLICY,
    sends="une enveloppe adressée à une autre conversation, fenêtre HEALTHY",
    expects=(
        "message.rejected publié, réponse brute persistée en `invalid`, compteur d'erreurs et "
        "octets de contexte mis à jour, FailureRecord non rejouable, un seul GET, session FAILED, "
        "chaîne d'audit toujours valide"
    ),
    code="CONVERSATION_MISMATCH",
    policy="rejet + échec de session",
    ref="§7.2 · §16 · ADR-013 · ADR-015",
)
async def given_protocol_error_in_healthy_window_when_applied_then_whole_trail_is_written_once() -> (
    None
):
    rig = make_rig()
    reply = plan_message(conversation_id="remote-9999")

    session = await run_rejecting(rig, reply)
    sid = session.session_id

    # 1. the session failed, and the failure is the protocol error itself
    assert session.status is SessionState.FAILED
    failure = only_failure(rig, sid)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "CONVERSATION_MISMATCH",
    )
    assert failure.retryable is False and failure.recoverable is False
    assert failure.origin == "ProtocolAdapter"
    assert failure.conversation_id == CONV
    assert session.last_failure_id == failure.failure_id

    # 2. the faulty reply is persisted as received, and counted in the window (ADR-013)
    messages = rig.store.list_messages(CONV)
    rejected_record = messages[-1]
    assert rejected_record.direction is MessageDirection.INBOUND
    assert rejected_record.validation_status == "invalid"
    assert rejected_record.payload == reply
    assert rejected_record.size_bytes == size_bytes(reply)
    conversation = rig.conversation(CONV)
    assert conversation.protocol_error_count == 1
    assert conversation.context_bytes == len(render_instructions(rig.config).encode("utf-8")) + sum(
        message.size_bytes for message in messages
    )

    # 3. published exactly once, with the error details
    rejected_events = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected_events) == 1
    assert rejected_events[0].payload["error_code"] == "CONVERSATION_MISMATCH"
    assert rejected_events[0].payload["details"] == failure.details
    assert rig.events(EventType.MESSAGE_INBOUND) == []

    # 4. never replayed (§7.2), the breaker is untouched, and the session ends FAILED
    assert len(rig.transport.get_calls) == 1
    assert [(d.operation, d.decision, d.delay_ms) for d in rig.store.list_retry_decisions(sid)] == [
        ("GET", "fail", None)
    ]
    assert rig.events(EventType.RETRY_SCHEDULED) == []
    assert rig.app.breaker.consecutive_failures == 0
    assert conversation.status is ConversationState.FAILED
    assert rig.cycles(CONV)[0].status is CycleState.FAILED
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "failure"
    assert rig.transport.closed == [REMOTE_1]

    # 5. the audit chain still verifies after the whole trail
    assert rig.app.audit.verify(sid).valid is True


@case(
    "pol-rotation-in-warning",
    category=POLICY,
    sends="la même faute, mais la fenêtre de contexte est déjà en WARNING",
    expects=(
        "une rotation remplace l'échec : l'enfant reçoit le message en attente retransmis et la "
        "session se termine normalement"
    ),
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="rejet + rotation puis reprise",
    ref="§10 · ADR-014 · ADR-019 §2",
)
async def given_protocol_error_in_warning_window_when_applied_then_rotation_and_normal_completion() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, resume_ack(), final_answer(REMOTE_2, message_id="model-msg-0010"))

    await rig.manager.continue_session(sid, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.COMPLETED
    assert ended.rotations_count == 1
    failure = only_failure(rig, sid)
    assert (failure.error_type, failure.error_code) == (
        ErrorType.MODEL_PROTOCOL_ERROR,
        "UNEXPECTED_MESSAGE_TYPE",
    )
    windows = [
        (event.conversation_id, event.payload["to"], event.payload.get("reason"))
        for event in rig.events(EventType.CONTEXT_WINDOW_STATE_CHANGED)
    ]
    assert windows == [
        (CONV, "WARNING", "threshold"),  # observed when the follow-up is sent
        (CONV, "SATURATED", "unusable_reply"),  # the rejected reply is read as saturation
        (CHILD, "HEALTHY", "resume_acknowledged"),
    ]
    # the pending user_request is retransmitted in the child, with the same content
    retransmitted = rig.events(EventType.MESSAGE_RETRANSMITTED)
    assert len(retransmitted) == 1 and retransmitted[0].conversation_id == CHILD
    assert rig.posted_types()[-3:] == ["user_request", "context_resume_request", "user_request"]
    assert rig.posted(-1)["content"] == rig.posted(-3)["content"]
    assert rig.posted(-1)["conversation_id"] == REMOTE_2
    assert rig.conversation(CONV).status is ConversationState.CLOSED
    assert rig.conversation(CHILD).status is ConversationState.WAITING_USER
    assert rig.app.audit.verify(sid).valid is True


@case(
    "pol-no-rotation-when-flag-off",
    category=POLICY,
    sends="la même faute en WARNING avec `context.rotate_on_unusable_reply_in_warning = false`",
    expects="aucune rotation : la politique retombe sur l'échec de session",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="rejet + échec de session",
    ref="§7.2 · ADR-019 §2",
)
async def given_rotation_on_unusable_reply_disabled_when_protocol_error_in_warning_then_session_fails() -> (
    None
):
    rig = make_rig(make_config(context={"rotate_on_unusable_reply_in_warning": False}))
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    assert ended.rotations_count == 0
    assert rig.events(EventType.ROTATION_STARTED) == []
    assert rig.conversations(sid) == [rig.conversation(CONV)]
    assert rig.conversation(CONV).context_window_state is ContextWindowState.WARNING
    assert only_failure(rig, sid).error_code == "UNEXPECTED_MESSAGE_TYPE"


@case(
    "pol-second-error-after-rotation",
    category=POLICY,
    sends="une seconde erreur de protocole dans la conversation enfant, après une rotation réussie",
    expects=(
        "aucune seconde rotation : l'ack a ramené la fenêtre de l'enfant en HEALTHY, la borne "
        "d'ADR-019 §2 est donc l'état de la fenêtre, pas un compteur"
    ),
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="rejet + échec de session",
    ref="§10 · ADR-014 · ADR-019 §2",
)
async def given_rotation_completed_when_a_second_protocol_error_arrives_then_session_fails_without_rotating_again() -> (
    None
):
    rig = make_rig()
    sid = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, resume_ack(), resume_ack(REMOTE_2, REMOTE_2, message_id="model-ack-0003"))

    await rig.manager.continue_session(sid, "follow-up")
    ended = await rig.wait(sid)

    assert ended.status is SessionState.FAILED
    assert ended.rotations_count == 1  # one rotation only
    assert [f.error_code for f in rig.store.list_failures(sid)] == [
        "UNEXPECTED_MESSAGE_TYPE",
        "UNEXPECTED_MESSAGE_TYPE",
    ]
    child = rig.conversation(CHILD)
    assert child.status is ConversationState.FAILED
    assert child.context_window_state is ContextWindowState.HEALTHY
    assert child.protocol_error_count == 1
    assert rig.conversation(CONV).status is ConversationState.CLOSED  # rotated away, not failed
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "failure"
    assert rig.app.audit.verify(sid).valid is True


@case(
    "pol-never-retried",
    category=POLICY,
    sends="une erreur de protocole alors que `[retry] max_attempts` vaut 8",
    expects="un seul GET et une seule décision `fail` : renvoyer le même message ne changerait rien",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="rejet + échec de session, sans rejeu",
    ref="§7.1 · §7.2 · §7.3",
)
async def given_high_retry_budget_when_protocol_error_received_then_no_retry_at_all() -> None:
    rig = make_rig(make_config(retry={"max_attempts": 8, "base_delay_ms": 10}))

    session = await run_rejecting(rig, final_answer())

    assert session.status is SessionState.FAILED
    assert len(rig.transport.get_calls) == 1
    decisions = rig.store.list_retry_decisions(session.session_id)
    assert [(d.operation, d.decision, d.delay_ms) for d in decisions] == [("GET", "fail", None)]
    assert decisions[0].max_attempts == 8  # the budget was there, the policy declined to use it
    assert rig.events(EventType.RETRY_SCHEDULED) == []
    assert only_failure(rig, session.session_id).attempt == 1
