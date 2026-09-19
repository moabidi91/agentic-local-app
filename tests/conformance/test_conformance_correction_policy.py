"""Politique de correction (ADR-023) : ce que l'application fait **après** avoir classé une faute.

Les autres familles de la batterie épinglent la *classification* d'une réponse inutilisable — le
code d'erreur, ce qui est persisté, ce qui est publié — et tournent avec
``protocol.max_correction_attempts = 0`` pour l'isoler (`conformance/harness.py`). Cette famille-ci
épingle la suite : au lieu de s'arrêter à la première faute, l'application cite au modèle ce qu'elle
a refusé, lui rappelle ce qu'elle attend, et relit — au plus ``max_correction_attempts`` fois de
suite, sans consommer ni cycle ni plan, et en remettant le compteur à zéro dès qu'une réponse est
valide. C'est la borne qui protège l'application, pas l'abandon immédiat.

Tout tourne sur le banc de la phase 9 : orchestrateur, adaptateur, politique d'échec, persistance
et audit réels ; seuls le réseau, le shell, l'horloge et les identifiants sont des doubles.
"""

from __future__ import annotations

from typing import Any

import pytest

from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.states import MessageDirection, MessageType, SessionState
from conformance.harness import make_rig
from conformance.registry import case
from integration.phase9_rig import (
    REMOTE_1,
    Rig,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    resume_ack,
)

pytestmark = [pytest.mark.conformance]

CORRECTION = "Politique de correction"
CONV = "conv-0001"
SESSION = "sess-0001"


# ------------------------------------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------------------------------------
def rig_with(attempts: int) -> Rig:
    """Le banc de la batterie avec un budget de corrections explicite (ADR-023)."""
    return make_rig(make_config(protocol={"max_correction_attempts": attempts}))


def stray(index: int) -> dict[str, Any]:
    """Un ``context_resume_ack`` hors de toute rotation : le type qu'aucune ligne de la table
    n'accepte en dehors d'une reprise, donc une faute quel que soit le message précédent."""
    return resume_ack(REMOTE_1, REMOTE_1, message_id=f"stray-{index:04d}")


def corrections(rig: Rig) -> list[Any]:
    return [
        message
        for message in rig.store.list_messages(CONV)
        if message.message_type is MessageType.PROTOCOL_CORRECTION_REQUEST
    ]


def rejected(rig: Rig) -> list[Any]:
    return [
        message
        for message in rig.store.list_messages(CONV)
        if message.direction is MessageDirection.INBOUND and message.validation_status == "invalid"
    ]


# ================================================================================================
# La correction, et sa borne
# ================================================================================================
@case(
    "corr-one-fault-then-valid",
    category=CORRECTION,
    sends="une réponse hors grammaire, puis la bonne après le rappel",
    expects="rappel du protocole envoyé, relecture contre la même attente, session menée à son terme",
    code="accepté après correction",
    policy="correction puis poursuite normale",
    ref="ADR-023 · ADR-007",
)
async def given_one_unusable_reply_when_corrected_then_the_session_completes() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, stray(1), discovery_plan(), execution_plan(), final_answer())

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert len(corrections(rig)) == 1 and len(rejected(rig)) == 1
    assert rig.posted_types() == [
        "user_request",
        "protocol_correction_request",
        "execution_result",
        "execution_result",
    ]
    assert rig.app.audit.verify(SESSION).valid is True


@case(
    "corr-no-cycle-no-plan",
    category=CORRECTION,
    sends="une faute au milieu d'une session dont le budget est serré",
    expects="la correction ne consomme ni cycle ni plan : seuls les vrais tours sont comptés",
    code="accepté après correction",
    policy="correction hors budget de cycles et de plans",
    ref="ADR-023 · §2.8 · ADR-012",
)
async def given_a_correction_when_budgets_are_read_then_no_cycle_and_no_plan_is_consumed() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, stray(1), discovery_plan(), execution_plan(), final_answer())

    session = await rig.run()

    # trois tours protocolaires (user_request, deux execution_result) et deux plans exécutés
    assert session.consumed_cycles == 3 and session.consumed_plans == 2
    assert len(corrections(rig)) == 1


@case(
    "corr-counter-resets",
    category=CORRECTION,
    sends="une faute, une réponse valide, puis une nouvelle faute",
    expects="le compteur est consécutif : la réponse valide rend tout le budget",
    code="accepté après correction",
    policy="correction, remise à zéro, correction",
    ref="ADR-023",
)
async def given_a_valid_reply_between_two_faults_when_counted_then_the_budget_is_restored() -> None:
    rig = rig_with(1)  # un seul essai : sans remise à zéro la seconde faute serait fatale
    rig.script_spec_outputs()
    rig.reply(
        REMOTE_1,
        stray(1),
        discovery_plan(),
        stray(2),
        execution_plan(),
        final_answer(),
    )

    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert len(corrections(rig)) == 2


@case(
    "corr-exhausted",
    category=CORRECTION,
    sends="une réponse inutilisable de plus que la borne configurée",
    expects="échec sur la dernière erreur, ses détails disant combien de corrections ont été tentées",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="corrections épuisées puis échec",
    ref="ADR-023 · §7.2",
)
async def given_more_faults_than_the_bound_when_exhausted_then_the_session_fails() -> None:
    rig = rig_with(2)
    rig.reply(REMOTE_1, stray(1), stray(2), stray(3))

    session = await rig.run()

    assert session.status is SessionState.FAILED
    assert len(corrections(rig)) == 2  # deux rappels, puis la troisième faute est fatale
    failure = rig.store.list_failures(SESSION)[-1]
    assert failure.error_code == "UNEXPECTED_MESSAGE_TYPE"
    assert failure.details["corrections_attempted"] == 2
    assert failure.details["max_correction_attempts"] == 2
    assert failure.details["unusable_replies"] == 3
    assert rig.app.audit.verify(SESSION).valid is True


@case(
    "corr-disabled",
    category=CORRECTION,
    sends="une réponse hors grammaire, la politique de correction étant désactivée",
    expects="échec immédiat : le réglage à zéro rétablit la conduite d'avant ADR-023",
    code="UNEXPECTED_MESSAGE_TYPE",
    policy="échec dès la première faute",
    ref="ADR-023 · §7.2",
)
async def given_the_policy_disabled_when_a_reply_is_unusable_then_the_session_fails_at_once() -> (
    None
):
    rig = rig_with(0)
    rig.reply(REMOTE_1, stray(1))

    session = await rig.run()

    assert session.status is SessionState.FAILED
    assert corrections(rig) == []
    # la réponse fautive reste persistée et comptée : seule la suite change
    assert len(rejected(rig)) == 1
    assert rig.conversation(CONV).protocol_error_count == 1


@case(
    "corr-request-content",
    category=CORRECTION,
    sends="une réponse hors grammaire, et on lit ce que l'application renvoie au modèle",
    expects="le rappel cite l'erreur, les types attendus et un exemple minimal valide",
    code="protocol_correction_request",
    policy="correction",
    ref="ADR-023 · §12",
)
async def given_a_correction_when_read_then_it_carries_the_fault_and_a_valid_example() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, stray(1), discovery_plan(), execution_plan(), final_answer())

    await rig.run()

    content = corrections(rig)[0].payload["content"]
    assert content["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    assert content["rejected_message_id"] == "stray-0001"
    assert content["expected_types"] == ["discovery_plan", "user_response"]
    assert content["attempt"] == 1 and content["max_attempts"] == 5
    assert content["reminder"] and content["errors"]
    # l'exemple embarqué est lui-même un message valide du type attendu
    assert content["example"]["type"] in content["expected_types"]
    assert content["example"]["conversation_id"] == REMOTE_1


@case(
    "corr-event-published",
    category=CORRECTION,
    sends="une réponse hors grammaire, et on lit le flux d'événements",
    expects="`correction.requested` est publié et audité à côté de `message.rejected`",
    code="correction.requested",
    policy="correction",
    ref="ADR-023 · ADR-015 · ADR-018",
)
async def given_a_correction_when_published_then_the_event_is_audited() -> None:
    rig = rig_with(5)
    rig.script_spec_outputs()
    rig.reply(REMOTE_1, stray(1), discovery_plan(), execution_plan(), final_answer())

    await rig.run()

    (event,) = rig.events(EventType.CORRECTION_REQUESTED)
    assert event.payload["error_code"] == "UNEXPECTED_MESSAGE_TYPE"
    assert event.payload["attempt"] == 1 and event.payload["max_attempts"] == 5
    assert len(rig.events(EventType.MESSAGE_REJECTED)) == 1
    assert rig.app.audit.verify(SESSION).valid is True
