"""Batterie de conformité protocolaire — plans impossibles, valeurs hors domaine, contenus de fin.

Ce module est la seconde moitié de la batterie. Le message est bien formé, arrive au bon moment,
et pourtant il est inexploitable : un plan sans tâche, une dépendance qui n'existe pas, un cycle,
un budget de sortie absurde, une conclusion incomplète. Chaque test rejoue la faute sur la vraie
boucle (``ProtocolOrchestrator``, ``ProtocolAdapter``, ``PlanRunner``, ``PayloadGuard``,
persistance et audit réels ; seuls le réseau, le shell, l'horloge et les identifiants sont des
doubles) et vérifie le code d'erreur, les ``details``, ce qui est persisté et ce qu'il advient de
la session. Chaque test porte un ``@case`` qui alimente ``docs/reports/conformance-protocole.md``.

Ordre de lecture (cinq familles, de la forme du plan à ce que le modèle en fait) :

1. **Structure du plan** — la charpente : ``tasks``, ``plan_id``, ``objective``,
   ``execution_policy``, unicité des identifiants dans la session (ADR-019 §1), champs inconnus,
   borne du ``state_summary`` (ADR-005).
2. **Dépendances et exécution** — le graphe et la politique : dépendance inconnue, dépendance sur
   soi, cycles, dépendance en avant selon la politique, workers, drapeaux d'arrêt (ADR-009).
3. **Valeurs des tâches** — hors domaine ou hors bornes : `cmd` vide, type inconnu, budgets et
   timeouts refusés, **ramenés au plafond** ou remplacés par un défaut (ADR-008, ADR-010), et
   toute la famille ``chunk_request`` (ADR-011, ADR-019 §1).
4. **Contenus de conclusion** — ``final_answer`` (§12.7), ``user_response`` (ADR-022) et
   ``context_resume_ack`` (§12.9) : ce qui est refusé, et ce qui est accepté **verbatim**.
5. **Réponses licites mais inattendues** — la frontière entre « faux » et « inhabituel mais
   légal » : un plan sans commande, un plan qui échoue sans faire échouer la session, un modèle
   qui se répète, un modèle qui conclut tout de suite.

Pour un cas « accepté et normalisé », l'assertion qui compte est la **valeur appliquée** sur
l'enregistrement (``TaskRecord.max_output_bytes_applied``, ``timeout_ms_applied``,
``PlanRecord.max_parallel_workers``), pas l'absence d'erreur. Les écarts constatés sont portés par
le verdict du ``@case`` correspondant, jamais corrigés ici.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from agentic_local_app.domain.canonical import size_bytes
from agentic_local_app.domain.errors import ErrorType
from agentic_local_app.domain.events import EventType
from agentic_local_app.domain.models import FailureRecord, SessionRecord
from agentic_local_app.domain.states import (
    ConversationState,
    MessageDirection,
    MessageType,
    PlanState,
    PlanType,
    SessionState,
    TaskState,
)
from agentic_local_app.protocol.messages import StateSummary
from conformance.registry import case
from integration.phase9_rig import (
    OUT_POM,
    QUESTION_BODY,
    REMOTE_1,
    REMOTE_2,
    Rig,
    chunk_task,
    cmd_task,
    discovery_plan,
    execution_plan,
    final_answer,
    make_config,
    make_rig,
    resume_ack,
    user_response,
)

pytestmark = [pytest.mark.conformance]

#: The identifiers the sequential generator hands out first (the rig is deterministic, ADR-017).
SESSION = "sess-0001"
CONV = "conv-0001"
CHILD = "conv-0002"


# ================================================================================================
# helpers
# ================================================================================================
def plan_message(**content: Any) -> dict[str, Any]:
    """The §12.2 ``discovery_plan`` with ``content`` fields replaced (``None`` removes the field).

    The envelope is always the well-formed one of the rig: this module only breaks the content.
    """
    message = discovery_plan(tasks=[cmd_task("t1")])
    body: dict[str, Any] = message["content"]
    for key, value in content.items():
        if value is None:
            body.pop(key, None)
        else:
            body[key] = value
    return message


def sequential_plan(tasks: list[dict[str, Any]], **overrides: Any) -> dict[str, Any]:
    """An ``execution_plan`` with one worker, for the second turn of a session."""
    overrides.setdefault("execution_policy", "sequential")
    overrides.setdefault("max_parallel_workers", None)
    return execution_plan(tasks=tasks, **overrides)


def ack_message(content: dict[str, Any], *, message_id: str = "model-ack-0002") -> dict[str, Any]:
    """A ``context_resume_ack`` of the child conversation with a hand-written ``content``."""
    return {
        "type": "context_resume_ack",
        "conversation_id": REMOTE_2,
        "message_id": message_id,
        "content": content,
    }


def state_summary(findings: str) -> dict[str, Any]:
    """An ADR-005 ``state_summary`` whose size is driven by the length of ``findings``."""
    return {
        "environment": {"os": "Linux"},
        "findings": [findings],
        "current_state": "confirming",
        "next_expected_step": "conclude",
    }


def summary_size(summary: dict[str, Any]) -> int:
    """The size the adapter measures: the canonical dump of the validated model (ADR-005)."""
    return size_bytes(StateSummary.model_validate(summary).model_dump(mode="json"))


def summary_of_exactly(limit: int) -> dict[str, Any]:
    """A ``state_summary`` measuring exactly ``limit`` bytes (the bound itself is accepted)."""
    padding = limit - summary_size(state_summary(""))
    assert padding > 0
    return state_summary("f" * padding)


def inbound_records(rig: Rig, conversation_id: str = CONV) -> list[Any]:
    return [
        message
        for message in rig.store.list_messages(conversation_id)
        if message.direction is MessageDirection.INBOUND
    ]


def only_failure(rig: Rig, session_id: str = SESSION) -> FailureRecord:
    failures = rig.store.list_failures(session_id)
    assert len(failures) == 1, [f.error_code for f in failures]
    return failures[0]


async def run_rejected(rig: Rig, *messages: dict[str, Any]) -> FailureRecord:
    """Queue one reply batch per message, run the session, and assert the common rejection trail.

    The last message is the faulty one: whatever came before it was accepted. Returns the single
    ``FailureRecord`` so the caller can assert on its code and its ``details``.
    """
    for message in messages:
        rig.transport.enqueue_messages(REMOTE_1, [message])
    session = await rig.run()

    assert session.status is SessionState.FAILED
    failure = only_failure(rig)
    assert failure.error_type is ErrorType.MODEL_PROTOCOL_ERROR
    assert failure.origin == "ProtocolAdapter"
    assert failure.retryable is False and failure.recoverable is False
    rejected = rig.events(EventType.MESSAGE_REJECTED)
    assert len(rejected) == 1 and rejected[0].payload["error_code"] == failure.error_code
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.FAILED
    assert conversation.protocol_error_count == 1
    assert rig.app.audit.verify(SESSION).valid is True
    return failure


async def run_accepted(rig: Rig, *messages: dict[str, Any]) -> SessionRecord:
    """Queue one reply batch per message, run the session, and assert nothing was rejected."""
    for message in messages:
        rig.transport.enqueue_messages(REMOTE_1, [message])
    session = await rig.run()

    assert session.status is SessionState.COMPLETED
    assert rig.store.list_failures(SESSION) == []
    assert rig.events(EventType.MESSAGE_REJECTED) == []
    assert rig.app.audit.verify(SESSION).valid is True
    return session


async def schema_errors(rig: Rig, *messages: dict[str, Any]) -> list[dict[str, str]]:
    """Run the messages, assert the last one was refused at the content stage, return the errors."""
    failure = await run_rejected(rig, *messages)
    assert failure.error_code == "SCHEMA_INVALID"
    assert failure.details["stage"] == "content"
    errors: list[dict[str, str]] = failure.details["errors"]
    return errors


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


def out_of_grammar_follow_up() -> dict[str, Any]:
    """The reply that is out of grammar in the follow-up row, used to trigger a rotation."""
    return resume_ack(REMOTE_1, "remote-0000", message_id="model-msg-0009")


async def rotated_then(rig: Rig, *child_messages: dict[str, Any]) -> SessionRecord:
    """Complete a turn, rotate on an unusable reply (ADR-019 §2), then play ``child_messages``.

    The child conversation answers the ``context_resume_request`` and the retransmitted follow-up
    ``user_request``; the returned session is the terminal one.
    """
    session_id = await completed_then_warning(rig)
    rig.reply(REMOTE_1, out_of_grammar_follow_up())
    rig.reply(REMOTE_2, *child_messages)

    await rig.manager.continue_session(session_id, "Is JAVA_HOME pointing at the JDK 17?")
    ended = await rig.wait(session_id)

    assert ended.rotations_count == 1
    return ended


async def rotation_ack_rejected(rig: Rig, ack: dict[str, Any]) -> FailureRecord:
    """A child that answers the resume request with an unusable ack: the rotation fails."""
    ended = await rotated_then(rig, ack)

    assert ended.status is SessionState.FAILED
    assert rig.conversation(CHILD).status is ConversationState.FAILED
    failures = rig.store.list_failures(ended.session_id)
    assert [f.error_type for f in failures] == [ErrorType.MODEL_PROTOCOL_ERROR] * 2
    assert failures[0].error_code == "UNEXPECTED_MESSAGE_TYPE"  # the reply that rotated
    assert rig.events(EventType.SESSION_STATE_CHANGED)[-1].payload["reason"] == "rotation_failed"
    return failures[-1]


# ================================================================================================
# 1. Structure du plan — la charpente du message (§12.2, ADR-005, ADR-007, ADR-019 §1)
# ================================================================================================
STRUCTURE = "Structure du plan"


@case(
    "plan-tasks-empty",
    category=STRUCTURE,
    sends="un plan dont `tasks` est une liste vide",
    expects="rejet à l'étape content : un plan porte au moins une tâche",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_plan_carries_no_task_then_schema_invalid_on_tasks() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(tasks=[]))

    assert errors == [
        {
            "loc": "content.tasks",
            "type": "too_short",
            "msg": "List should have at least 1 item after validation, not 0",
        }
    ]
    assert rig.store.list_plans(SESSION) == []  # nothing is projected from a refused plan


@case(
    "plan-tasks-missing",
    category=STRUCTURE,
    sends="un plan sans champ `tasks`",
    expects="rejet : `tasks` est obligatoire, l'absence n'est pas lue comme une liste vide",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_plan_has_no_tasks_field_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(tasks=None))

    assert errors == [{"loc": "content.tasks", "type": "missing", "msg": "Field required"}]


@case(
    "plan-tasks-not-a-list",
    category=STRUCTURE,
    sends="un plan dont `tasks` est un objet, puis une chaîne",
    expects="rejet : `tasks` est une liste, aucune tolérance pour une tâche unique",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_tasks_is_not_a_list_then_schema_invalid() -> None:
    for tasks in ({"task_id": "t1", "type": "cmd", "cmd": "run t1"}, "t1"):
        rig = make_rig()

        errors = await schema_errors(rig, plan_message(tasks=tasks))

        assert errors == [
            {"loc": "content.tasks", "type": "list_type", "msg": "Input should be a valid list"}
        ]


@case(
    "plan-task-not-an-object",
    category=STRUCTURE,
    sends="un plan dont la première tâche est une chaîne au lieu d'un objet",
    expects="rejet ; `details.errors[].loc` désigne l'indice fautif (`content.tasks.0`)",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_a_task_is_not_an_object_then_schema_invalid_at_its_index() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(tasks=["t1"]))

    assert errors == [
        {
            "loc": "content.tasks.0",
            "type": "model_type",
            "msg": "Input should be a valid dictionary or instance of TaskMessage",
        }
    ]


@case(
    "plan-missing-plan-id",
    category=STRUCTURE,
    sends="un plan sans `plan_id`",
    expects="rejet : sans identifiant, le plan ne peut être ni projeté ni référencé",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_plan_has_no_plan_id_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(plan_id=None))

    assert errors == [{"loc": "content.plan_id", "type": "missing", "msg": "Field required"}]


@case(
    "plan-empty-plan-id",
    category=STRUCTURE,
    sends="un plan dont le `plan_id` est la chaîne vide",
    expects="rejet : la chaîne vide n'est pas un identifiant",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007 · ADR-017",
)
async def given_initial_user_request_when_plan_id_is_empty_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(plan_id=""))

    assert errors == [
        {
            "loc": "content.plan_id",
            "type": "string_too_short",
            "msg": "String should have at least 1 character",
        }
    ]


@case(
    "plan-duplicate-plan-id",
    category=STRUCTURE,
    sends="un second plan réutilisant le `plan_id` du premier",
    expects="rejet ; le premier plan reste seul projeté",
    code="DUPLICATE_PLAN_ID",
    policy="échec",
    ref="§12.2 · ADR-007 · ADR-019 §1",
)
async def given_accepted_plan_when_model_reuses_its_plan_id_then_duplicate_plan_id() -> None:
    rig = make_rig()

    failure = await run_rejected(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        sequential_plan([cmd_task("t2")], plan_id="plan-0"),
    )

    assert failure.error_code == "DUPLICATE_PLAN_ID"
    assert failure.details == {"plan_id": "plan-0"}
    assert [plan.plan_id for plan in rig.store.list_plans(SESSION)] == ["plan-0"]
    assert [task.task_id for task in rig.tasks(SESSION)] == ["t1"]


@case(
    "plan-duplicate-plan-id-after-rotation",
    category=STRUCTURE,
    sends="un `plan_id` déjà utilisé, mais dans la conversation enfant née d'une rotation",
    expects="rejet quand même : la portée d'unicité est la **session**, pas la conversation",
    code="DUPLICATE_PLAN_ID",
    policy="échec de session (la rotation avait déjà servi)",
    ref="§12.2 · ADR-014 · ADR-019 §1",
)
async def given_rotated_conversation_when_model_reuses_a_plan_id_of_the_parent_then_duplicate_plan_id() -> (
    None
):
    rig = make_rig()

    ended = await rotated_then(
        rig,
        resume_ack(),
        discovery_plan(
            REMOTE_2, message_id="model-msg-0011", plan_id="plan-0", tasks=[cmd_task("t9")]
        ),
    )

    assert ended.status is SessionState.FAILED
    failure = rig.store.list_failures(ended.session_id)[-1]
    assert failure.error_code == "DUPLICATE_PLAN_ID"
    assert failure.details == {"plan_id": "plan-0"}
    assert failure.conversation_id == CHILD
    # the parent's two plans are the only ones ever projected
    assert [plan.plan_id for plan in rig.store.list_plans(ended.session_id)] == ["plan-0", "plan-1"]
    assert rig.conversation(CHILD).protocol_error_count == 1


@case(
    "plan-missing-objective",
    category=STRUCTURE,
    sends="un plan sans `objective`",
    expects="rejet : l'objectif est ce qui rend le plan auditable",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_plan_has_no_objective_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(objective=None))

    assert errors == [{"loc": "content.objective", "type": "missing", "msg": "Field required"}]


@case(
    "plan-missing-execution-policy",
    category=STRUCTURE,
    sends="un plan sans `execution_policy`",
    expects="rejet : aucune politique par défaut n'est supposée",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · §8.2 · ADR-007",
)
async def given_initial_user_request_when_plan_has_no_execution_policy_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(execution_policy=None))

    assert errors == [
        {"loc": "content.execution_policy", "type": "missing", "msg": "Field required"}
    ]


@case(
    "plan-unknown-execution-policy",
    category=STRUCTURE,
    sends="une `execution_policy` inventée (`best_effort`)",
    expects="rejet : l'énumération du §12.2 n'a que `sequential` et `parallel`",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · §8.2 · ADR-007",
)
async def given_initial_user_request_when_execution_policy_is_unknown_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(execution_policy="best_effort"))

    assert errors == [
        {
            "loc": "content.execution_policy",
            "type": "enum",
            "msg": "Input should be 'sequential' or 'parallel'",
        }
    ]


@case(
    "plan-duplicate-task-id-in-plan",
    category=STRUCTURE,
    sends="deux tâches portant le même `task_id` dans un seul plan",
    expects="rejet ; `details.scope` vaut `plan`",
    code="DUPLICATE_TASK_ID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_two_tasks_share_an_id_then_duplicate_task_id_scope_plan() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig, plan_message(tasks=[cmd_task("t1"), cmd_task("t1", "echo again")])
    )

    assert failure.error_code == "DUPLICATE_TASK_ID"
    assert failure.details == {"task_id": "t1", "plan_id": "plan-0", "scope": "plan"}


@case(
    "plan-duplicate-task-id-in-session",
    category=STRUCTURE,
    sends="un `task_id` déjà utilisé par un plan précédent de la session",
    expects="rejet ; `details.scope` vaut `session` : les identifiants ne sont pas recyclables",
    code="DUPLICATE_TASK_ID",
    policy="échec",
    ref="§12.2 · ADR-007 · ADR-019 §1",
)
async def given_accepted_plan_when_model_reuses_a_task_id_then_duplicate_task_id_scope_session() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig, discovery_plan(tasks=[cmd_task("t1")]), sequential_plan([cmd_task("t1")])
    )

    assert failure.error_code == "DUPLICATE_TASK_ID"
    assert failure.details == {"task_id": "t1", "plan_id": "plan-1", "scope": "session"}
    assert [task.task_id for task in rig.tasks(SESSION)] == ["t1"]
    assert rig.task(SESSION, "t1").plan_id == "plan-0"  # still the first one


@case(
    "plan-extra-field",
    category=STRUCTURE,
    sends="un plan valide plus un champ inconnu (`priority`)",
    expects="rejet : le contenu d'un plan est fermé (`extra = forbid`), `loc` nomme le champ",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_plan_carries_an_unknown_field_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(priority="high"))

    assert errors == [
        {
            "loc": "content.priority",
            "type": "extra_forbidden",
            "msg": "Extra inputs are not permitted",
        }
    ]


@case(
    "task-extra-field",
    category=STRUCTURE,
    sends="une tâche valide plus un champ inconnu (`shell`)",
    expects="rejet ; `loc` descend jusqu'à la tâche fautive (`content.tasks.0.shell`)",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-003 · ADR-007",
)
async def given_initial_user_request_when_a_task_carries_an_unknown_field_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(tasks=[cmd_task("t1", shell="powershell")]))

    assert errors == [
        {
            "loc": "content.tasks.0.shell",
            "type": "extra_forbidden",
            "msg": "Extra inputs are not permitted",
        }
    ]


@case(
    "plan-state-summary-too-large",
    category=STRUCTURE,
    sends="un `state_summary` plus gros que `payload.max_state_summary_bytes`",
    expects="rejet ; `details` porte la taille mesurée et la borne",
    code="STATE_SUMMARY_TOO_LARGE",
    policy="échec",
    ref="§12.2 · ADR-005 · ADR-010",
)
async def given_initial_user_request_when_state_summary_exceeds_the_bound_then_state_summary_too_large() -> (
    None
):
    rig = make_rig()
    limit = rig.config.payload.max_state_summary_bytes
    summary = state_summary("f" * (limit + 500))

    failure = await run_rejected(rig, plan_message(state_summary=summary))

    assert failure.error_code == "STATE_SUMMARY_TOO_LARGE"
    assert failure.details == {
        "size_bytes": summary_size(summary),
        "max_bytes": limit,
        "plan_id": "plan-0",
    }
    assert failure.details["size_bytes"] > limit
    assert rig.store.list_plans(SESSION) == []


@case(
    "plan-state-summary-at-the-bound",
    category=STRUCTURE,
    sends="un `state_summary` mesurant exactement `payload.max_state_summary_bytes`",
    expects="accepté : la borne elle-même passe, et le résumé est stocké verbatim sur le PlanRecord",
    code="accepté",
    policy="accepté",
    ref="§12.2 · ADR-005 · ADR-010",
)
async def given_initial_user_request_when_state_summary_sits_on_the_bound_then_accepted_and_stored() -> (
    None
):
    rig = make_rig()
    limit = rig.config.payload.max_state_summary_bytes
    summary = summary_of_exactly(limit)
    assert summary_size(summary) == limit

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")], state_summary=summary),
        final_answer(message_id="model-msg-0002"),
    )

    stored = rig.plan(session.session_id, "plan-0").state_summary
    assert stored == StateSummary.model_validate(summary).model_dump(mode="json")
    assert stored is not None and stored["findings"] == summary["findings"]


# ================================================================================================
# 2. Dépendances et politique d'exécution (§8.2, §8.3 ; ADR-007, ADR-009)
# ================================================================================================
DEPENDENCIES = "Dépendances et exécution"


@case(
    "dep-unknown",
    category=DEPENDENCIES,
    sends="une tâche dépendant d'un `task_id` qui n'est pas dans le plan",
    expects="rejet ; `details` nomme la tâche et la dépendance introuvable",
    code="UNKNOWN_DEPENDENCY",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_initial_user_request_when_a_dependency_is_not_in_the_plan_then_unknown_dependency() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig, plan_message(tasks=[cmd_task("t1"), cmd_task("t2", depends_on=["t0"])])
    )

    assert failure.error_code == "UNKNOWN_DEPENDENCY"
    assert failure.details == {"task_id": "t2", "dependency": "t0", "plan_id": "plan-0"}


@case(
    "dep-self",
    category=DEPENDENCIES,
    sends="une tâche qui se déclare dépendante d'elle-même",
    expects="rejet avec un code dédié, avant même la recherche de cycle",
    code="SELF_DEPENDENCY",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_initial_user_request_when_a_task_depends_on_itself_then_self_dependency() -> None:
    rig = make_rig()

    failure = await run_rejected(rig, plan_message(tasks=[cmd_task("t1", depends_on=["t1"])]))

    assert failure.error_code == "SELF_DEPENDENCY"
    assert failure.details == {"task_id": "t1", "plan_id": "plan-0"}


@case(
    "dep-cycle-two-tasks",
    category=DEPENDENCIES,
    sends="deux tâches qui dépendent l'une de l'autre",
    expects="rejet ; `details.cycle` est le chemin **fermé** du cycle",
    code="DEPENDENCY_CYCLE",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_initial_user_request_when_two_tasks_depend_on_each_other_then_dependency_cycle() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig,
        plan_message(
            execution_policy="parallel",
            max_parallel_workers=2,
            tasks=[cmd_task("t1", depends_on=["t2"]), cmd_task("t2", depends_on=["t1"])],
        ),
    )

    assert failure.error_code == "DEPENDENCY_CYCLE"
    assert failure.details == {"cycle": ["t1", "t2", "t1"], "plan_id": "plan-0"}


@case(
    "dep-cycle-three-tasks",
    category=DEPENDENCIES,
    sends="un cycle de trois tâches (t1 → t2 → t3 → t1)",
    expects="rejet ; `details.cycle` reconstitue le chemin entier, pas seulement la dernière arête",
    code="DEPENDENCY_CYCLE",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_initial_user_request_when_three_tasks_form_a_cycle_then_whole_path_reported() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig,
        plan_message(
            execution_policy="parallel",
            max_parallel_workers=2,
            tasks=[
                cmd_task("t1", depends_on=["t2"]),
                cmd_task("t2", depends_on=["t3"]),
                cmd_task("t3", depends_on=["t1"]),
            ],
        ),
    )

    assert failure.error_code == "DEPENDENCY_CYCLE"
    assert failure.details == {"cycle": ["t1", "t2", "t3", "t1"], "plan_id": "plan-0"}


@case(
    "dep-forward-in-sequential",
    category=DEPENDENCIES,
    sends="en `sequential`, une tâche dépendant d'une tâche déclarée **après** elle",
    expects="rejet : en séquentiel l'ordre de déclaration est l'ordre d'exécution",
    code="FORWARD_DEPENDENCY_IN_SEQUENTIAL",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_sequential_plan_when_a_dependency_is_declared_later_then_forward_dependency() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig,
        plan_message(
            execution_policy="sequential",
            tasks=[cmd_task("t1", depends_on=["t2"]), cmd_task("t2")],
        ),
    )

    assert failure.error_code == "FORWARD_DEPENDENCY_IN_SEQUENTIAL"
    assert failure.details == {
        "task_id": "t1",
        "dependency": "t2",
        "plan_id": "plan-0",
        "execution_policy": "sequential",
    }


@case(
    "dep-forward-in-parallel-accepted",
    category=DEPENDENCIES,
    sends="la même déclaration en `parallel`",
    expects=(
        "accepté : l'ordre vient du graphe, pas de la déclaration — la dépendance s'exécute en "
        "premier et les deux tâches terminent"
    ),
    code="accepté",
    policy="accepté",
    ref="§8.2 · ADR-007 · ADR-017",
)
async def given_parallel_plan_when_a_dependency_is_declared_later_then_graph_order_wins() -> None:
    rig = make_rig()

    session = await run_accepted(
        rig,
        plan_message(
            execution_policy="parallel",
            max_parallel_workers=2,
            tasks=[cmd_task("t1", depends_on=["t2"]), cmd_task("t2")],
        ),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    assert rig.plan(sid, "plan-0").status is PlanState.COMPLETED
    assert rig.task(sid, "t1").status is TaskState.COMPLETED
    assert rig.task(sid, "t2").status is TaskState.COMPLETED
    assert rig.task(sid, "t1").order_index == 0  # declared first, executed last
    assert rig.executor.spawn_order == ["t2", "t1"]  # the graph, not the declaration order


@case(
    "workers-non-positive",
    category=DEPENDENCIES,
    sends="`max_parallel_workers` à 0, puis négatif",
    expects="rejet par le schéma : au moins un worker",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§8.2 · ADR-007",
)
async def given_parallel_plan_when_workers_is_not_positive_then_schema_invalid() -> None:
    for workers in (0, -2):
        rig = make_rig()

        errors = await schema_errors(
            rig, plan_message(execution_policy="parallel", max_parallel_workers=workers)
        )

        assert errors == [
            {
                "loc": "content.max_parallel_workers",
                "type": "greater_than_equal",
                "msg": "Input should be greater than or equal to 1",
            }
        ]


@case(
    "workers-ignored-in-sequential",
    category=DEPENDENCIES,
    sends="`max_parallel_workers = 4` sur un plan `sequential`",
    expects=(
        "accepté avec l'avertissement d'audit `WORKERS_IGNORED_IN_SEQUENTIAL` ; le PlanRecord "
        "porte un seul worker et les tâches s'exécutent l'une après l'autre"
    ),
    code="accepté + avertissement",
    policy="accepté + avertissement",
    ref="§8.2 · ADR-009",
)
async def given_sequential_plan_with_workers_when_received_then_one_worker_applied_and_warning() -> (
    None
):
    rig = make_rig()

    session = await run_accepted(
        rig,
        plan_message(execution_policy="sequential", max_parallel_workers=4, tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )

    plan = rig.plan(session.session_id, "plan-0")
    assert plan.max_parallel_workers == 1  # the applied value, not the declared one
    warnings = rig.events(EventType.AUDIT_WARNING)
    assert [event.payload["code"] for event in warnings] == ["WORKERS_IGNORED_IN_SEQUENTIAL"]
    assert warnings[0].payload == {
        "code": "WORKERS_IGNORED_IN_SEQUENTIAL",
        "entity": "plan",
        "id": "plan-0",
        "details": {},
    }
    assert rig.executor.max_active == 1


@case(
    "workers-default-in-parallel",
    category=DEPENDENCIES,
    sends="un plan `parallel` sans `max_parallel_workers`",
    expects=(
        "accepté avec l'avertissement `DEFAULT_WORKERS_APPLIED` ; le PlanRecord porte la valeur "
        "appliquée (1), aucune parallélisation implicite"
    ),
    code="accepté + avertissement",
    policy="accepté + avertissement",
    ref="§8.2 · ADR-009",
)
async def given_parallel_plan_without_workers_when_received_then_default_applied_and_warning() -> (
    None
):
    rig = make_rig()

    session = await run_accepted(
        rig,
        plan_message(
            execution_policy="parallel", tasks=[cmd_task("t1"), cmd_task("t2")], plan_id="plan-0"
        ),
        final_answer(message_id="model-msg-0002"),
    )

    plan = rig.plan(session.session_id, "plan-0")
    assert plan.max_parallel_workers == 1
    assert [event.payload["code"] for event in rig.events(EventType.AUDIT_WARNING)] == [
        "DEFAULT_WORKERS_APPLIED"
    ]
    assert rig.events(EventType.PLAN_RECEIVED)[0].payload["max_parallel_workers"] == 1
    assert rig.executor.max_active == 1


@case(
    "flags-contradictory",
    category=DEPENDENCIES,
    sends="une tâche à la fois `critical` et `continue_on_error`",
    expects=(
        "accepté avec `CONTRADICTORY_FLAGS:<task_id>` ; la règle effective d'ADR-009 est un **ou** : "
        "`stops_plan_on_failure` est vrai et l'échec arrête bien le plan"
    ),
    code="accepté + avertissement",
    policy="accepté + avertissement",
    ref="§8.3 · ADR-009",
)
async def given_contradictory_flags_when_the_task_fails_then_plan_stops_despite_continue_on_error() -> (
    None
):
    rig = make_rig()
    rig.executor.script(task_id="t1", stderr=b"boom", exit_code=1)

    session = await run_accepted(
        rig,
        discovery_plan(
            tasks=[cmd_task("t1", critical=True, continue_on_error=True), cmd_task("t2")]
        ),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    warnings = rig.events(EventType.AUDIT_WARNING)
    assert [event.payload["code"] for event in warnings] == ["CONTRADICTORY_FLAGS:t1"]
    assert warnings[0].payload["details"] == {"task_id": "t1"}
    task = rig.task(sid, "t1")
    assert (task.critical, task.continue_on_error) == (True, True)
    assert task.stops_plan_on_failure is True  # the "or" of ADR-009 §2 wins
    assert task.status is TaskState.FAILED
    plan = rig.plan(sid, "plan-0")
    assert plan.status is PlanState.STOPPED_ON_FAILURE
    assert plan.stop_reason == "critical_task_failed:t1"  # `critical` only qualifies the label
    assert rig.task(sid, "t2").status is TaskState.SKIPPED
    assert rig.task(sid, "t2").reason == "plan_stopped:critical_task_failed:t1"


@case(
    "stop-plan-on-success",
    category=DEPENDENCIES,
    sends="une tâche `stop_plan_on_success` qui réussit, suivie d'une autre tâche",
    expects=(
        "le plan est court-circuité : statut `short_circuited_on_success`, la suite est `SKIPPED`, "
        "et l'`execution_result` le rapporte au modèle"
    ),
    code="accepté",
    policy="accepté (plan court-circuité)",
    ref="§8.3 · ADR-009",
)
async def given_plan_with_stop_on_success_when_the_task_succeeds_then_plan_short_circuited() -> (
    None
):
    rig = make_rig()

    session = await run_accepted(
        rig,
        discovery_plan(
            tasks=[cmd_task("t1", stop_plan_on_success=True), cmd_task("t2")],
        ),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    plan = rig.plan(sid, "plan-0")
    assert plan.status is PlanState.SHORT_CIRCUITED_ON_SUCCESS
    assert plan.stop_reason == "stop_plan_on_success:t1"
    assert rig.task(sid, "t1").status is TaskState.COMPLETED
    assert rig.task(sid, "t2").status is TaskState.SKIPPED
    assert len(rig.executor.calls) == 1  # the second command never ran
    result = rig.posted(1)["content"]
    assert result["status"] == "short_circuited_on_success"
    assert result["stop_reason"] == "stop_plan_on_success:t1"
    assert result["skipped_tasks"] == [
        {"task_id": "t2", "reason": "plan_stopped:stop_plan_on_success:t1"}
    ]


# ================================================================================================
# 3. Tâches : valeurs hors domaine et normalisation (ADR-008, ADR-010, ADR-011, ADR-019 §1)
# ================================================================================================
VALUES = "Valeurs des tâches"


@case(
    "task-cmd-blank",
    category=VALUES,
    sends="une tâche `cmd` dont la commande est vide, puis faite d'espaces",
    expects="rejet : une tâche `cmd` exige une commande non vide, l'espace ne compte pas",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-003 · ADR-007",
)
async def given_initial_user_request_when_cmd_is_blank_then_schema_invalid_on_the_task() -> None:
    for cmd in ("", "   "):
        rig = make_rig()

        errors = await schema_errors(
            rig, plan_message(tasks=[{"task_id": "t1", "type": "cmd", "cmd": cmd}])
        )

        assert errors == [
            {
                "loc": "content.tasks.0",
                "type": "value_error",
                "msg": "Value error, a cmd task requires a non-empty cmd",
            }
        ]


@case(
    "task-cmd-null",
    category=VALUES,
    sends="une tâche `cmd` dont `cmd` vaut `null`",
    expects="rejet : le type par défaut est `cmd`, et une tâche `cmd` sans commande n'existe pas",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-007",
)
async def given_initial_user_request_when_cmd_is_null_then_schema_invalid_on_the_task() -> None:
    rig = make_rig()

    errors = await schema_errors(
        rig, plan_message(tasks=[{"task_id": "t1", "type": "cmd", "cmd": None}])
    )

    assert errors == [
        {
            "loc": "content.tasks.0",
            "type": "value_error",
            "msg": "Value error, a cmd task requires a non-empty cmd",
        }
    ]


@case(
    "task-cmd-with-chunk-fields",
    category=VALUES,
    sends="une tâche `cmd` portant en plus les champs d'un `chunk_request`",
    expects="rejet : les deux formes de tâche sont disjointes, aucun mélange n'est toléré",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.6 · ADR-011",
)
async def given_initial_user_request_when_a_cmd_task_carries_chunk_fields_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(
        rig,
        plan_message(
            tasks=[cmd_task("t1", ref_task_id="t0", byte_offset=0, max_bytes=16)],
        ),
    )

    assert errors == [
        {
            "loc": "content.tasks.0",
            "type": "value_error",
            "msg": "Value error, chunk fields are not allowed on a cmd task",
        }
    ]


@case(
    "task-unknown-type",
    category=VALUES,
    sends="une tâche de type inconnu (`http_request`)",
    expects="rejet : seuls `cmd` et `chunk_request` existent, l'application n'invente pas d'action",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · §12.6 · ADR-007",
)
async def given_initial_user_request_when_task_type_is_unknown_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(
        rig, plan_message(tasks=[{"task_id": "t1", "type": "http_request", "cmd": "GET /"}])
    )

    assert errors == [
        {
            "loc": "content.tasks.0.type",
            "type": "enum",
            "msg": "Input should be 'cmd' or 'chunk_request'",
        }
    ]


@case(
    "task-max-output-non-positive",
    category=VALUES,
    sends="`max_output_bytes` à 0, puis négatif",
    expects="rejet par le schéma (`gt = 0`) : un budget nul n'est pas une troncature",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-010",
)
async def given_initial_user_request_when_max_output_bytes_is_not_positive_then_schema_invalid() -> (
    None
):
    for budget in (0, -1):
        rig = make_rig()

        errors = await schema_errors(
            rig, plan_message(tasks=[cmd_task("t1", max_output_bytes=budget)])
        )

        assert errors == [
            {
                "loc": "content.tasks.0.max_output_bytes",
                "type": "greater_than",
                "msg": "Input should be greater than 0",
            }
        ]


@case(
    "task-max-output-clamped",
    category=VALUES,
    sends="`max_output_bytes` très au-dessus de `payload.hard_max_output_bytes`",
    expects=(
        "accepté et **ramené** au plafond : `max_output_bytes_applied` vaut le plafond tandis que "
        "la valeur déclarée reste lisible sur la TaskRecord"
    ),
    code="accepté",
    policy="accepté (valeur normalisée)",
    ref="§2.5 · ADR-010",
)
async def given_task_budget_above_the_hard_cap_when_projected_then_cap_applied_and_declaration_kept() -> (
    None
):
    rig = make_rig()
    declared = 10_000_000

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1", max_output_bytes=declared)]),
        final_answer(message_id="model-msg-0002"),
    )

    task = rig.task(session.session_id, "t1")
    assert task.max_output_bytes == declared
    assert task.max_output_bytes_applied == rig.config.payload.hard_max_output_bytes
    assert task.max_output_bytes_applied < declared
    assert rig.posted(1)["content"]["results"][0]["task_id"] == "t1"


@case(
    "task-timeout-zero",
    category=VALUES,
    sends="`timeout_ms = 0`",
    expects="rejet par le schéma (`gt = 0`) : zéro n'est pas « pas de limite »",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.2 · ADR-008",
)
async def given_initial_user_request_when_timeout_is_zero_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, plan_message(tasks=[cmd_task("t1", timeout_ms=0)]))

    assert errors == [
        {
            "loc": "content.tasks.0.timeout_ms",
            "type": "greater_than",
            "msg": "Input should be greater than 0",
        }
    ]


@case(
    "task-timeout-clamped",
    category=VALUES,
    sends="`timeout_ms` au-dessus de `execution.max_task_timeout_ms`",
    expects=(
        "accepté et ramené au plafond : `timeout_ms_applied` vaut le plafond, et c'est ce délai "
        "que reçoit l'exécuteur"
    ),
    code="accepté",
    policy="accepté (valeur normalisée)",
    ref="ADR-008 §1",
)
async def given_task_timeout_above_the_cap_when_projected_then_cap_applied_and_used_by_the_executor() -> (
    None
):
    rig = make_rig()
    declared = 3_600_000

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1", timeout_ms=declared)]),
        final_answer(message_id="model-msg-0002"),
    )

    cap = rig.config.execution.max_task_timeout_ms
    task = rig.task(session.session_id, "t1")
    assert task.timeout_ms == declared
    assert task.timeout_ms_applied == cap
    assert [call.timeout_ms for call in rig.executor.calls] == [cap]


@case(
    "task-timeout-default",
    category=VALUES,
    sends="une tâche sans `timeout_ms`",
    expects=(
        "accepté : le défaut de configuration s'applique, la déclaration reste vide et l'exécuteur "
        "reçoit `execution.default_task_timeout_ms`"
    ),
    code="accepté",
    policy="accepté (défaut appliqué)",
    ref="ADR-008 §1",
)
async def given_task_without_timeout_when_projected_then_configured_default_applied() -> None:
    rig = make_rig()

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )

    default = rig.config.execution.default_task_timeout_ms
    task = rig.task(session.session_id, "t1")
    assert task.timeout_ms is None
    assert task.timeout_ms_applied == default
    assert [call.timeout_ms for call in rig.executor.calls] == [default]


@case(
    "task-values-coerced-from-strings",
    category=VALUES,
    sends="une tâche dont le budget, le timeout et les drapeaux arrivent en chaînes ou en entiers",
    expects=(
        'accepté et **converti** sans avertissement : `"2048"` devient un budget, `"1000"` un '
        'timeout, `"yes"`/`0` des drapeaux, et ce sont ces valeurs qui pilotent l\'exécution'
    ),
    code="accepté",
    policy="accepté (valeurs converties)",
    ref="§12.2 · ADR-008 · ADR-009 · ADR-010",
    verdict="à surveiller",
    note=(
        "Les contenus sont validés en mode **laxiste** (pydantic par défaut) : un entier accepte "
        'une chaîne numérique (`"2048"`) et un flottant entier (`2048.0`), un booléen accepte '
        '`"yes"`, `"no"`, `"on"`, `0`, `1`. La conversion est silencieuse et sert ensuite de '
        "base aux valeurs appliquées : plafonnement ADR-010, timeout passé au shell (ADR-008), "
        "règle d'arrêt ADR-009 — un `continue_on_error: 0` décide donc de l'arrêt du plan. Rien "
        "n'est incohérent ici (les valeurs obtenues sont celles que le modèle voulait) et les "
        "identifiants, eux, restent strictement des chaînes, mais la frontière du protocole est "
        "plus floue que ce que le §12 laisse entendre. À trancher : soit valider les contenus en "
        "mode strict (`strict=True` sur `ProtocolModel`), soit documenter explicitement la "
        "tolérance dans `PROTOCOL_INSTRUCTIONS.md` (ADR-004). Même cause que "
        "`user-response-expects-reply-coerced`."
    ),
)
async def given_task_values_sent_as_strings_when_projected_then_coerced_and_applied() -> None:
    rig = make_rig()
    task_as_text: dict[str, Any] = {
        "task_id": "t1",
        "type": "cmd",
        "cmd": "run t1",
        "max_output_bytes": "2048",
        "timeout_ms": "1000",
        "critical": "yes",
        "continue_on_error": 0,
    }

    session = await run_accepted(
        rig,
        plan_message(tasks=[task_as_text]),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    task = rig.task(sid, "t1")
    assert (task.max_output_bytes, task.max_output_bytes_applied) == (2_048, 2_048)
    assert (task.timeout_ms, task.timeout_ms_applied) == (1_000, 1_000)
    assert (task.critical, task.continue_on_error) == (True, False)
    assert task.stops_plan_on_failure is True  # derived from the coerced flags (ADR-009 §2)
    assert [call.timeout_ms for call in rig.executor.calls] == [1_000]  # the shell got the string
    assert rig.events(EventType.AUDIT_WARNING) == []  # converted silently, nothing is reported


@case(
    "plan-default-output-budget",
    category=VALUES,
    sends="un plan portant `default_max_output_bytes`, avec une tâche qui déclare et une qui non",
    expects=(
        "précédence tâche > plan > configuration, lisible sur chaque `max_output_bytes_applied` : "
        "le défaut du plan ne touche que les tâches muettes et ne survit pas au plan suivant"
    ),
    code="accepté",
    policy="accepté (valeur normalisée)",
    ref="§2.5 · ADR-010",
)
async def given_plan_default_output_budget_when_projected_then_task_then_plan_then_config_wins() -> (
    None
):
    rig = make_rig()
    plan_default = 4_096
    config_default = rig.config.payload.default_max_output_bytes
    assert plan_default != config_default  # otherwise the precedence would not be observable

    session = await run_accepted(
        rig,
        plan_message(
            default_max_output_bytes=plan_default,
            tasks=[cmd_task("t1", max_output_bytes=256), cmd_task("t2")],
        ),
        sequential_plan([cmd_task("t3")]),
        final_answer(),
    )
    sid = session.session_id

    assert rig.plan(sid, "plan-0").default_max_output_bytes == plan_default
    assert rig.task(sid, "t1").max_output_bytes_applied == 256  # the task wins over the plan
    assert rig.task(sid, "t2").max_output_bytes is None
    assert rig.task(sid, "t2").max_output_bytes_applied == plan_default  # the plan wins over config
    assert rig.plan(sid, "plan-1").default_max_output_bytes is None
    assert rig.task(sid, "t3").max_output_bytes_applied == config_default  # nothing declared


@case(
    "chunk-missing-ref-task-id",
    category=VALUES,
    sends="un `chunk_request` sans `ref_task_id`",
    expects="rejet : sans référence, il n'y a rien à relire",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.6 · ADR-011",
)
async def given_initial_user_request_when_chunk_request_has_no_reference_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(
        rig,
        plan_message(
            tasks=[{"task_id": "c1", "type": "chunk_request", "byte_offset": 0, "max_bytes": 16}]
        ),
    )

    assert errors == [
        {
            "loc": "content.tasks.0",
            "type": "value_error",
            "msg": "Value error, a chunk_request task requires ref_task_id",
        }
    ]


@case(
    "chunk-missing-range",
    category=VALUES,
    sends="un `chunk_request` sans `byte_offset`, puis sans `max_bytes`",
    expects="rejet : la plage est obligatoire, aucune valeur implicite",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.6 · ADR-011",
)
async def given_initial_user_request_when_chunk_request_has_no_range_then_schema_invalid() -> None:
    incomplete: list[dict[str, Any]] = [
        {"task_id": "c1", "type": "chunk_request", "ref_task_id": "t0", "max_bytes": 16},
        {"task_id": "c1", "type": "chunk_request", "ref_task_id": "t0", "byte_offset": 0},
    ]
    for task in incomplete:
        rig = make_rig()

        errors = await schema_errors(rig, plan_message(tasks=[task]))

        assert errors == [
            {
                "loc": "content.tasks.0",
                "type": "value_error",
                "msg": "Value error, a chunk_request task requires byte_offset and max_bytes",
            }
        ]


@case(
    "chunk-negative-offset",
    category=VALUES,
    sends="un `chunk_request` dont le `byte_offset` est négatif",
    expects="rejet par le schéma (`ge = 0`) : une plage part du début du flux, jamais d'avant",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.6 · ADR-011",
)
async def given_initial_user_request_when_chunk_offset_is_negative_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(
        rig, plan_message(tasks=[chunk_task("c1", "t0", offset=-1, max_bytes=16)])
    )

    assert errors == [
        {
            "loc": "content.tasks.0.byte_offset",
            "type": "greater_than_equal",
            "msg": "Input should be greater than or equal to 0",
        }
    ]


@case(
    "chunk-unknown-stream",
    category=VALUES,
    sends="un `chunk_request` dont le `stream` est inconnu (`stdlog`)",
    expects="rejet : deux flux existent, et l'absence vaut `stdout` — pas une invention",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.6 · ADR-011",
)
async def given_initial_user_request_when_chunk_stream_is_unknown_then_schema_invalid() -> None:
    rig = make_rig()
    task = chunk_task("c1", "t0") | {"stream": "stdlog"}

    errors = await schema_errors(rig, plan_message(tasks=[task]))

    assert errors == [
        {
            "loc": "content.tasks.0.stream",
            "type": "enum",
            "msg": "Input should be 'stdout' or 'stderr'",
        }
    ]


@case(
    "chunk-ref-unknown",
    category=VALUES,
    sends="un `chunk_request` vers un `task_id` qui n'existe nulle part dans la session",
    expects="rejet au niveau protocole : l'adaptateur connaît toutes les tâches de la session",
    code="CHUNK_REF_UNKNOWN",
    policy="échec",
    ref="§12.6 · ADR-011 · ADR-019 §1",
)
async def given_accepted_plan_when_chunk_references_an_unknown_task_then_chunk_ref_unknown() -> (
    None
):
    rig = make_rig()

    failure = await run_rejected(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        sequential_plan([chunk_task("c1", "t-nowhere")]),
    )

    assert failure.error_code == "CHUNK_REF_UNKNOWN"
    assert failure.details == {"task_id": "c1", "ref_task_id": "t-nowhere", "plan_id": "plan-1"}


@case(
    "chunk-ref-without-stored-output",
    category=VALUES,
    sends="un `chunk_request` vers une tâche connue mais dont aucune sortie n'a été stockée",
    expects=(
        "**accepté** par le protocole (la tâche existe) puis `FAILED` à l'exécution avec "
        "`CHUNK_REF_NOT_FOUND` : les deux niveaux d'ADR-019 §1 se voient"
    ),
    code="accepté puis CHUNK_REF_NOT_FOUND (tâche)",
    policy="accepté ; la tâche échoue, le plan s'arrête, la session continue",
    ref="ADR-008 §5 · ADR-011 · ADR-019 §1",
)
async def given_chunk_referencing_a_task_without_blob_when_executed_then_task_failed_not_rejected() -> (
    None
):
    rig = make_rig()
    rig.executor.script(task_id="t1", stdout=b"0123456789")

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        sequential_plan([chunk_task("c1", "t1", offset=0, max_bytes=4)]),
        execution_plan(
            message_id="model-msg-0004",
            plan_id="plan-2",
            tasks=[chunk_task("c2", "c1", offset=0, max_bytes=4)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
        final_answer(message_id="model-msg-0005"),
    )
    sid = session.session_id

    assert rig.task(sid, "c1").status is TaskState.COMPLETED  # t1 has a blob
    failed = rig.task(sid, "c2")  # c1, a chunk_request, has none
    assert failed.status is TaskState.FAILED
    assert failed.reason == "CHUNK_REF_NOT_FOUND"
    assert rig.plan(sid, "plan-2").status is PlanState.STOPPED_ON_FAILURE
    assert rig.store.list_failures(sid) == []  # never a protocol error (ADR-008 §5)


@case(
    "chunk-ref-previous-conversation",
    category=VALUES,
    sends="un `chunk_request` vers une tâche d'une **conversation précédente**, après rotation",
    expects="accepté et servi : les blobs de la session survivent à la rotation",
    code="accepté",
    policy="accepté",
    ref="ADR-011 · ADR-014 · ADR-019 §1",
)
async def given_rotated_conversation_when_chunk_references_a_task_of_the_parent_then_served() -> (
    None
):
    rig = make_rig()

    ended = await rotated_then(
        rig,
        resume_ack(),
        execution_plan(
            REMOTE_2,
            message_id="model-msg-0011",
            plan_id="plan-7",
            tasks=[chunk_task("c1", "t4", offset=0, max_bytes=16)],
            execution_policy="sequential",
            max_parallel_workers=None,
        ),
        final_answer(REMOTE_2, message_id="model-msg-0012"),
    )
    sid = ended.session_id

    assert ended.status is SessionState.COMPLETED
    chunk = rig.task(sid, "c1")
    assert chunk.status is TaskState.COMPLETED
    assert chunk.conversation_id == CHILD
    assert rig.task(sid, "t4").conversation_id == CONV  # the referenced task is in the parent
    served = rig.posted(-1)["content"]["results"][0]
    assert served["data"] == OUT_POM[:16].decode()
    assert served["range"] == [0, 16]
    assert served["total"] == len(OUT_POM)


@case(
    "chunk-max-bytes-clamped",
    category=VALUES,
    sends="un `chunk_request` valide dont `max_bytes` dépasse `payload.hard_max_output_bytes`",
    expects="accepté et ramené au plafond : `TaskRecord.max_bytes` porte la valeur appliquée",
    code="accepté",
    policy="accepté (valeur normalisée)",
    ref="ADR-010 · ADR-011",
)
async def given_chunk_request_above_the_hard_cap_when_projected_then_max_bytes_clamped() -> None:
    rig = make_rig()
    rig.executor.script(task_id="t1", stdout=b"0123456789")

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        sequential_plan([chunk_task("c1", "t1", offset=0, max_bytes=10_000_000)]),
        final_answer(),
    )
    sid = session.session_id

    cap = rig.config.payload.hard_max_output_bytes
    chunk = rig.task(sid, "c1")
    assert chunk.max_bytes == cap
    assert chunk.max_output_bytes_applied == cap
    assert chunk.timeout_ms_applied is None  # a local read has no timeout (ADR-008 §5)
    assert chunk.status is TaskState.COMPLETED
    served = rig.posted(2)["content"]["results"][0]
    assert served["data"] == "0123456789"
    assert served["eof"] is True


# ================================================================================================
# 4. Contenus de conclusion — final_answer, user_response, context_resume_ack (§12.7, §12.9, ADR-022)
# ================================================================================================
CONCLUSIONS = "Contenus de conclusion"


@case(
    "final-missing-status",
    category=CONCLUSIONS,
    sends="un `final_answer` sans `status`",
    expects="rejet : la conclusion dit toujours dans quel état elle laisse l'enquête",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.7 · ADR-007",
)
async def given_execution_result_sent_when_final_answer_has_no_status_then_schema_invalid() -> None:
    rig = make_rig()
    answer = final_answer(message_id="model-msg-0002")
    answer["content"].pop("status")

    errors = await schema_errors(rig, discovery_plan(tasks=[cmd_task("t1")]), answer)

    assert errors == [{"loc": "content.status", "type": "missing", "msg": "Field required"}]
    assert rig.session(SESSION).final_answer is None


@case(
    "final-missing-diagnosis",
    category=CONCLUSIONS,
    sends="un `final_answer` sans `diagnosis`",
    expects="rejet : une conclusion sans diagnostic n'est pas une réponse",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.7 · ADR-007",
)
async def given_execution_result_sent_when_final_answer_has_no_diagnosis_then_schema_invalid() -> (
    None
):
    rig = make_rig()
    answer = final_answer(message_id="model-msg-0002")
    answer["content"].pop("diagnosis")

    errors = await schema_errors(rig, discovery_plan(tasks=[cmd_task("t1")]), answer)

    assert errors == [{"loc": "content.diagnosis", "type": "missing", "msg": "Field required"}]
    assert rig.events(EventType.FINAL_ANSWER_RECEIVED) == []


@case(
    "final-evidence-not-a-list",
    category=CONCLUSIONS,
    sends="un `final_answer` dont `evidence` est une chaîne au lieu d'une liste",
    expects="rejet : la preuve est une liste d'éléments, pas un paragraphe",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="§12.7 · ADR-007",
)
async def given_execution_result_sent_when_evidence_is_a_string_then_schema_invalid() -> None:
    rig = make_rig()
    answer = final_answer(message_id="model-msg-0002")
    answer["content"]["evidence"] = "java -version shows 17, pom targets 21"

    errors = await schema_errors(rig, discovery_plan(tasks=[cmd_task("t1")]), answer)

    assert errors == [
        {"loc": "content.evidence", "type": "list_type", "msg": "Input should be a valid list"}
    ]


@case(
    "final-extra-fields",
    category=CONCLUSIONS,
    sends="un `final_answer` portant des champs inconnus (`confidence`, `references`)",
    expects=(
        "accepté : `FinalAnswerContent` est ouvert (`extra = allow`) et les champs supplémentaires "
        "survivent dans `SessionRecord.final_answer`"
    ),
    code="accepté",
    policy="accepté",
    ref="§12.7 · ADR-007",
)
async def given_execution_result_sent_when_final_answer_carries_extra_fields_then_accepted_and_kept() -> (
    None
):
    rig = make_rig()
    answer = final_answer(message_id="model-msg-0002")
    answer["content"]["confidence"] = 0.9
    answer["content"]["references"] = ["https://example.invalid/jdk"]

    session = await run_accepted(rig, discovery_plan(tasks=[cmd_task("t1")]), answer)

    stored = session.final_answer
    assert stored is not None
    assert stored["confidence"] == 0.9
    assert stored["references"] == ["https://example.invalid/jdk"]
    assert stored == answer["content"]  # verbatim, extras included
    assert rig.events(EventType.FINAL_ANSWER_RECEIVED)[0].payload["status"] == "completed"


@case(
    "final-empty-evidence",
    category=CONCLUSIONS,
    sends="un `final_answer` dont `evidence` est une liste vide",
    expects="accepté : conclure sans preuve citée est pauvre, pas invalide",
    code="accepté",
    policy="accepté",
    ref="§12.7 · ADR-007",
)
async def given_execution_result_sent_when_evidence_is_empty_then_accepted() -> None:
    rig = make_rig()

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002", evidence=False),
    )

    assert session.final_answer is not None
    assert session.final_answer["evidence"] == []
    assert rig.conversation(CONV).status is ConversationState.WAITING_USER


@case(
    "user-response-unknown-format",
    category=CONCLUSIONS,
    sends="un `user_response` dont le `format` est inconnu (`html`)",
    expects="rejet : trois formats de rendu existent, l'interface n'en devine pas un quatrième",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="ADR-022 §1",
)
async def given_initial_user_request_when_user_response_format_is_unknown_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, user_response(format="html"))

    assert errors == [
        {
            "loc": "content.format",
            "type": "literal_error",
            "msg": "Input should be 'text', 'markdown' or 'json'",
        }
    ]


@case(
    "user-response-empty-body",
    category=CONCLUSIONS,
    sends="un `user_response` au corps vide",
    expects="rejet : répondre à l'utilisateur sans rien lui dire n'est pas une réponse",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="ADR-022 §1",
)
async def given_initial_user_request_when_user_response_body_is_empty_then_schema_invalid() -> None:
    rig = make_rig()

    errors = await schema_errors(rig, user_response(body=""))

    assert errors == [
        {
            "loc": "content.body",
            "type": "string_too_short",
            "msg": "String should have at least 1 character",
        }
    ]


@case(
    "user-response-too-large",
    category=CONCLUSIONS,
    sends="un `user_response` dont le corps dépasse `payload.max_message_bytes`",
    expects="rejet ; `details` porte la taille du corps, la borne et le message fautif",
    code="USER_RESPONSE_TOO_LARGE",
    policy="échec",
    ref="ADR-010 · ADR-022 §1",
)
async def given_initial_user_request_when_user_response_body_is_too_large_then_user_response_too_large() -> (
    None
):
    # the bound is lowered rather than the body inflated; ADR-019 §4 keeps the sections coherent
    rig = make_rig(
        make_config(
            payload={
                "max_message_bytes": 2_048,
                "hard_max_output_bytes": 1_024,
                "default_max_output_bytes": 512,
            },
            context={"summary_budget_bytes": 1_024},
        )
    )

    failure = await run_rejected(rig, user_response(body="x" * 4_096))

    assert failure.error_code == "USER_RESPONSE_TOO_LARGE"
    assert failure.details == {
        "size_bytes": 4_096,
        "max_bytes": 2_048,
        "message_id": "model-msg-0001",
    }
    assert rig.config.payload.max_message_bytes == 2_048


@case(
    "user-response-expects-reply-not-a-bool",
    category=CONCLUSIONS,
    sends="un `user_response` dont `expects_reply` vaut la chaîne `sometimes`",
    expects="rejet : `expects_reply` décide de la suite du tour, il doit être un booléen",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="ADR-022 §1 · ADR-022 §4",
)
async def given_initial_user_request_when_expects_reply_is_not_a_boolean_then_schema_invalid() -> (
    None
):
    rig = make_rig()
    message = user_response()
    message["content"]["expects_reply"] = "sometimes"

    errors = await schema_errors(rig, message)

    assert errors == [
        {
            "loc": "content.expects_reply",
            "type": "bool_parsing",
            "msg": "Input should be a valid boolean, unable to interpret input",
        }
    ]


@case(
    "user-response-expects-reply-coerced",
    category=CONCLUSIONS,
    sends="un `user_response` dont `expects_reply` vaut la chaîne `yes`",
    expects="accepté et **converti** en `true` par la coercition laxiste de pydantic",
    code="accepté",
    policy="accepté (valeur convertie)",
    ref="ADR-022 §1 · ADR-022 §4",
    verdict="à surveiller",
    note=(
        "Même cause que `task-values-coerced-from-strings` (validation laxiste), mais sur le "
        "drapeau qui décide de la suite du tour : `expects_reply` garde la conversation "
        "réutilisable sous `auto_close_on_final_answer` (ADR-022 §4) et il est ici dérivé d'une "
        'chaîne. `"yes"`, `"on"`, `"1"` donnent `true` ; `"sometimes"` reste refusé. Même '
        "arbitrage à rendre : validation stricte des contenus, ou tolérance documentée."
    ),
)
async def given_initial_user_request_when_expects_reply_is_the_string_yes_then_coerced_to_true() -> (
    None
):
    rig = make_rig()
    message = user_response()
    message["content"]["expects_reply"] = "yes"

    session = await run_accepted(rig, message)

    received = rig.events(EventType.USER_RESPONSE_RECEIVED)
    assert len(received) == 1 and received[0].payload["expects_reply"] is True
    assert session.status is SessionState.COMPLETED
    assert rig.conversation(CONV).status is ConversationState.WAITING_USER


@case(
    "user-response-unknown-status",
    category=CONCLUSIONS,
    sends="un `user_response` dont le `status` est hors domaine (`done`)",
    expects="rejet : trois statuts existent (`completed`, `partial`, `failed`)",
    code="SCHEMA_INVALID",
    policy="échec",
    ref="ADR-022 §1",
)
async def given_initial_user_request_when_user_response_status_is_unknown_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    errors = await schema_errors(rig, user_response(status="done"))

    assert errors == [
        {
            "loc": "content.status",
            "type": "literal_error",
            "msg": "Input should be 'completed', 'partial' or 'failed'",
        }
    ]


@case(
    "user-response-json-body-not-json",
    category=CONCLUSIONS,
    sends='un `user_response` déclaré `format = "json"` dont le corps n\'est pas du JSON',
    expects=(
        "**accepté** : le corps est opaque, jamais analysé ; il est persisté verbatim et `format` "
        "ne sert qu'au rendu côté utilisateur"
    ),
    code="accepté",
    policy="accepté",
    ref="ADR-022 §1",
)
async def given_initial_user_request_when_json_user_response_body_is_not_json_then_stored_verbatim() -> (
    None
):
    rig = make_rig()
    body = "{ this is not JSON at all: it never parses, "

    session = await run_accepted(rig, user_response(format="json", body=body))

    assert session.status is SessionState.COMPLETED
    record = inbound_records(rig)[0]
    assert record.message_type is MessageType.USER_RESPONSE
    assert record.payload["content"]["body"] == body  # byte for byte, never re-serialised
    assert record.payload["content"]["format"] == "json"
    with pytest.raises(json.JSONDecodeError):
        json.loads(body)  # the application never tried
    assert rig.events(EventType.USER_RESPONSE_RECEIVED)[0].payload["format"] == "json"


@case(
    "user-response-envelope-look-alike",
    category=CONCLUSIONS,
    sends="un `user_response` dont le corps contient une enveloppe de protocole en texte",
    expects=(
        "accepté verbatim : rien du corps n'est réinjecté dans le protocole, aucun plan n'est "
        "créé, aucun message n'est posté"
    ),
    code="accepté",
    policy="accepté",
    ref="ADR-022 §1 · ADR-022 §5",
)
async def given_initial_user_request_when_user_response_body_looks_like_an_envelope_then_never_reentered() -> (
    None
):
    rig = make_rig()
    look_alike = json.dumps(
        {
            "type": "discovery_plan",
            "conversation_id": REMOTE_1,
            "message_id": "model-msg-0042",
            "content": {
                "plan_id": "plan-shadow",
                "objective": "run something",
                "execution_policy": "sequential",
                "tasks": [{"task_id": "t99", "type": "cmd", "cmd": "rm -rf /"}],
            },
        }
    )

    session = await run_accepted(rig, user_response(format="json", body=look_alike))

    assert session.status is SessionState.COMPLETED
    assert rig.store.list_plans(session.session_id) == []
    assert rig.tasks(session.session_id) == []
    assert rig.executor.calls == []
    assert rig.posted_types() == ["user_request"]
    assert inbound_records(rig)[0].payload["content"]["body"] == look_alike


@case(
    "ack-missing-acknowledged",
    category=CONCLUSIONS,
    sends="un `context_resume_ack` sans champ `acknowledged`",
    expects="rejet à l'étape content dans la conversation enfant ; la rotation échoue",
    code="SCHEMA_INVALID",
    policy="échec de la rotation, session FAILED",
    ref="§12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_ack_has_no_acknowledged_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    failure = await rotation_ack_rejected(rig, ack_message({"original_conversation_id": REMOTE_1}))

    assert failure.error_code == "SCHEMA_INVALID"
    assert failure.details["stage"] == "content"
    assert failure.details["message_type"] == "context_resume_ack"
    assert failure.details["errors"] == [
        {"loc": "content.acknowledged", "type": "missing", "msg": "Field required"}
    ]


@case(
    "ack-acknowledged-wrong-type",
    category=CONCLUSIONS,
    sends="un `context_resume_ack` dont `acknowledged` est une chaîne non booléenne",
    expects="rejet : l'accusé de reprise est un oui ou un non, pas une nuance",
    code="SCHEMA_INVALID",
    policy="échec de la rotation, session FAILED",
    ref="§12.9 · ADR-014",
)
async def given_context_resume_request_sent_when_acknowledged_is_not_a_boolean_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    failure = await rotation_ack_rejected(
        rig, ack_message({"original_conversation_id": REMOTE_1, "acknowledged": "as you wish"})
    )

    assert failure.error_code == "SCHEMA_INVALID"
    assert failure.details["errors"] == [
        {
            "loc": "content.acknowledged",
            "type": "bool_parsing",
            "msg": "Input should be a valid boolean, unable to interpret input",
        }
    ]


@case(
    "ack-extra-field",
    category=CONCLUSIONS,
    sends="un `context_resume_ack` valide plus un champ inconnu (`summary_ok`)",
    expects="rejet : le contenu de l'accusé est fermé comme les autres contenus du §12",
    code="SCHEMA_INVALID",
    policy="échec de la rotation, session FAILED",
    ref="§12.9 · ADR-007 · ADR-014",
)
async def given_context_resume_request_sent_when_ack_carries_an_unknown_field_then_schema_invalid() -> (
    None
):
    rig = make_rig()

    failure = await rotation_ack_rejected(
        rig,
        ack_message(
            {
                "original_conversation_id": REMOTE_1,
                "acknowledged": True,
                "summary_ok": True,
            }
        ),
    )

    assert failure.error_code == "SCHEMA_INVALID"
    assert failure.details["errors"] == [
        {
            "loc": "content.summary_ok",
            "type": "extra_forbidden",
            "msg": "Extra inputs are not permitted",
        }
    ]


# ================================================================================================
# 5. Réponses licites mais inattendues — la frontière du « faux » (§11, §14 ; ADR-009, ADR-022)
# ================================================================================================
UNUSUAL = "Réponses licites mais inattendues"


@case(
    "licit-plan-of-chunks-only",
    category=UNUSUAL,
    sends="un plan dont toutes les tâches sont des `chunk_request` (aucune commande)",
    expects="accepté : le plan s'exécute sans lancer un seul processus et produit un execution_result",
    code="accepté",
    policy="accepté",
    ref="§12.6 · ADR-011",
)
async def given_stored_output_when_plan_holds_only_chunk_requests_then_result_produced_without_any_command() -> (
    None
):
    rig = make_rig()
    rig.executor.script(task_id="t1", stdout=b"0123456789ABCDEF")

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        sequential_plan(
            [
                chunk_task("c1", "t1", offset=0, max_bytes=8),
                chunk_task("c2", "t1", offset=8, max_bytes=8),
            ]
        ),
        final_answer(),
    )
    sid = session.session_id

    assert len(rig.executor.calls) == 1  # only the discovery command ever ran
    assert rig.plan(sid, "plan-1").status is PlanState.COMPLETED
    assert [rig.task(sid, t).status for t in ("c1", "c2")] == [TaskState.COMPLETED] * 2
    assert rig.posted_types() == ["user_request", "execution_result", "execution_result"]
    served = rig.posted(2)["content"]["results"]
    assert [item["data"] for item in served] == ["01234567", "89ABCDEF"]
    assert [item["range"] for item in served] == [[0, 8], [8, 16]]


@case(
    "licit-plan-fails-session-continues",
    category=UNUSUAL,
    sends="un plan d'une seule commande qui sort en code non nul",
    expects=(
        "le **plan** échoue (`stopped_on_failure`) et l'`execution_result` le dit ; la session, "
        "elle, continue : un échec de commande n'est pas une faute de protocole"
    ),
    code="accepté",
    policy="accepté (plan arrêté, protocole intact)",
    ref="§8.3 · ADR-008 §3 · ADR-009",
)
async def given_failing_command_when_plan_stops_then_session_is_not_failed() -> None:
    rig = make_rig()
    rig.executor.script(task_id="t1", stderr=b"[ERROR] invalid target release: 21", exit_code=1)

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    assert session.status is SessionState.COMPLETED  # not FAILED
    task = rig.task(sid, "t1")
    assert task.status is TaskState.FAILED and task.exit_code == 1
    plan = rig.plan(sid, "plan-0")
    assert plan.status is PlanState.STOPPED_ON_FAILURE
    assert plan.stop_reason == "task_failed:t1"  # no flag declared: ADR-009 §1 default
    result = rig.posted(1)["content"]
    assert result["status"] == "stopped_on_failure"
    assert result["results"][0]["exit_code"] == 1
    assert result["results"][0]["stderr"] == "[ERROR] invalid target release: 21"
    assert rig.conversation(CONV).protocol_error_count == 0


@case(
    "licit-same-plan-new-ids",
    category=UNUSUAL,
    sends="deux fois le même contenu de plan, avec un `plan_id` et des `task_id` neufs",
    expects="accepté : l'unicité porte sur les identifiants, jamais sur le contenu",
    code="accepté",
    policy="accepté",
    ref="§12.2 · ADR-007 · ADR-019 §1",
)
async def given_accepted_plan_when_model_repeats_it_with_new_ids_then_accepted() -> None:
    rig = make_rig()
    objective = "Discover execution environment and build context"

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1", "uname -a")]),
        sequential_plan([cmd_task("t2", "uname -a")], objective=objective),
        final_answer(),
    )
    sid = session.session_id

    plans = rig.store.list_plans(sid)
    assert [plan.plan_id for plan in plans] == ["plan-0", "plan-1"]
    assert {plan.objective for plan in plans} == {objective}
    assert [task.cmd for task in rig.tasks(sid)] == ["uname -a", "uname -a"]
    assert [call.cmd for call in rig.executor.calls] == ["uname -a", "uname -a"]


@case(
    "licit-final-answer-after-discovery",
    category=UNUSUAL,
    sends="un `final_answer` dès le résultat du plan de découverte",
    expects="accepté : rien n'oblige le modèle à un second plan, la session se termine COMPLETED",
    code="accepté",
    policy="accepté",
    ref="§11 · §14 · ADR-007",
)
async def given_discovery_result_sent_when_model_concludes_immediately_then_session_completed() -> (
    None
):
    rig = make_rig()

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        final_answer(message_id="model-msg-0002"),
    )
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert [plan.plan_type for plan in rig.store.list_plans(sid)] == [PlanType.DISCOVERY_PLAN]
    assert session.consumed_plans == 1
    assert session.final_answer is not None
    assert rig.posted_types() == ["user_request", "execution_result"]
    assert rig.conversation(CONV).status is ConversationState.WAITING_USER  # reusable


@case(
    "licit-user-response-mid-investigation",
    category=UNUSUAL,
    sends="un `user_response` après un `execution_result`, au milieu d'une enquête",
    expects=(
        "accepté : le tour est conclu sans `final_answer`, la session est COMPLETED et la "
        "conversation reste réutilisable pour la réponse de l'utilisateur"
    ),
    code="accepté",
    policy="accepté",
    ref="§11 · ADR-022 §2 · ADR-022 §3",
)
async def given_execution_result_sent_when_model_answers_the_user_then_turn_concluded_without_final_answer() -> (
    None
):
    rig = make_rig()

    session = await run_accepted(
        rig,
        discovery_plan(tasks=[cmd_task("t1")]),
        user_response(message_id="model-msg-0002", body=QUESTION_BODY, expects_reply=True),
    )
    sid = session.session_id

    assert session.status is SessionState.COMPLETED
    assert session.final_answer is None  # a user_response is never written there (ADR-022 §3)
    assert rig.events(EventType.FINAL_ANSWER_RECEIVED) == []
    received = rig.events(EventType.USER_RESPONSE_RECEIVED)
    assert len(received) == 1
    assert received[0].payload["expects_reply"] is True
    assert received[0].payload["body_bytes"] == len(QUESTION_BODY.encode("utf-8"))
    conversation = rig.conversation(CONV)
    assert conversation.status is ConversationState.WAITING_USER
    assert conversation.final_answer_received is True  # the turn is concluded (ADR-007 table)
    assert [plan.plan_id for plan in rig.store.list_plans(sid)] == ["plan-0"]
