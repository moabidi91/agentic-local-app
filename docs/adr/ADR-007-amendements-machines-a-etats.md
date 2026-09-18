# ADR-007 — Amendements des machines à états et table des messages attendus

**Statut** : accepté (2026-09-18)

## Contexte

En confrontant §5 (machines à états) à §10 et §14 (rotation), §8.4 et §9 (interruption), §3.5 (types de messages) et §2.7/§11 (finalisation), plusieurs transitions nécessaires manquent, une transition est inexploitable telle quelle, le cycle n'a pas de machine à états, et la table « quel type de message le modèle a-t-il le droit d'envoyer maintenant ? », que la phase 2 doit tester, n'est pas écrite.

## Décision

### Conversation (§5.1 amendé)

| Transition | Origine | Statut |
|---|---|---|
| NEW → ACTIVE | spec | conservée |
| ACTIVE → WAITING_MODEL_RESPONSE | spec | conservée |
| WAITING_MODEL_RESPONSE → RUNNING_PLAN | spec | conservée |
| WAITING_MODEL_RESPONSE → COMPLETED | spec | conservée |
| **WAITING_MODEL_RESPONSE → ROTATING** | §14 déclenche la rotation sur erreur de GET, donc depuis cet état | **ajoutée** |
| RUNNING_PLAN → WAITING_MODEL_RESPONSE | spec | conservée |
| RUNNING_PLAN → ROTATING | spec | conservée |
| ROTATING → WAITING_MODEL_RESPONSE | spec | **réinterprétée** : c'est la conversation *enfant* qui entre en WAITING_MODEL_RESPONSE (NEW → ACTIVE → WAITING_MODEL_RESPONSE) ; sur l'enregistrement *parent* cette transition n'existe pas |
| **ROTATING → CLOSED** | le parent doit finir dans un état terminal une fois l'ACK reçu (`closure_reason = rotated`) | **ajoutée** |
| ROTATING → FAILED | ROTATION_FAILED (§2.6) | couverte par ANY → FAILED |
| ANY_ACTIVE_STATE → INTERRUPTED | spec (ACTIVE, WAITING_MODEL_RESPONSE, RUNNING_PLAN, ROTATING) | conservée ; INTERRUPTED devient **terminal** (ADR-006) |
| INTERRUPTED → READY, READY → ACTIVE | spec | **déplacées** vers la machine à états de session (ADR-006) |
| ANY → FAILED | spec | précisée : depuis tout état non terminal |
| COMPLETED → WAITING_USER, WAITING_USER → WAITING_MODEL_RESPONSE, COMPLETED → CLOSED | spec | conservées |

États terminaux : `CLOSED`, `FAILED`, `INTERRUPTED`.

### Session (nouvelle, ADR-006)

`READY → RUNNING` (user_request) · `RUNNING → COMPLETED` (final_answer) · `COMPLETED → RUNNING` (message de suivi, conversation réutilisable) · `RUNNING → INTERRUPTING` (interruption) · `INTERRUPTING → READY` (nettoyage persisté) · `RUNNING → FAILED`. Terminaux : `FAILED`, et `COMPLETED` lorsque `auto_close_on_final_answer = true`.

### Plan (§5.2 amendé)

Ajouts : **PENDING → FAILED** (budget dépassé ou plan invalide découvert après persistance, §14 « Persist active plan → Check session budget ») et **PENDING → INTERRUPTED** (interruption entre la réception et le démarrage).

### Tâche (§5.3)

Inchangée. `TIMED_OUT` est terminal et compte comme un échec pour les conditions d'arrêt (ADR-008).

### Cycle (nouvelle)

`RUNNING → COMPLETED` · `RUNNING → FAILED` · `RUNNING → INTERRUPTED`. Un cycle démarre quand le message sortant est persisté (avant le POST) et se termine quand l'`execution_result` du plan reçu est persisté, ou quand un `final_answer` / `context_resume_ack` est traité. `retry_count` compte les retries de transport du cycle.

### Fenêtre de contexte (§5.4)

Inchangée, portée par chaque conversation. La conversation enfant est créée avec l'état hérité `SATURATED` et passe à `HEALTHY` à la réception du `context_resume_ack`, ce qui réalise littéralement la transition `SATURATED → HEALTHY` de la spec.

### Table des messages attendus (ProtocolAdapter)

| Dernier message sortant | Types entrants autorisés |
|---|---|
| `user_request` (premier de la conversation) | `discovery_plan` |
| `user_request` (suivi, conversation réutilisée après COMPLETED) | `discovery_plan`, `execution_plan`, `priority_clarification`, `final_answer` |
| `execution_result` | `execution_plan`, `priority_clarification`, `final_answer` |
| `context_resume_request` | `context_resume_ack` |

Exactement **un** message est attendu par tour ; tout message supplémentaire lu par le même GET est une `MODEL_PROTOCOL_ERROR` (`UNEXPECTED_EXTRA_MESSAGE`). Un type hors table → `MODEL_PROTOCOL_ERROR` (`UNEXPECTED_MESSAGE_TYPE`).

Validation structurelle d'un plan, au-delà du schéma JSON : `conversation_id` égal à la conversation courante ; `message_id` et `plan_id` jamais vus ; `task_id` uniques ; `depends_on` référence des tâches du plan, sans cycle, et en mode `sequential` uniquement des tâches antérieures ; `max_parallel_workers ≥ 1` en mode `parallel` ; `chunk_request.ref_task_id` désigne une tâche dont la sortie brute est stockée ; `max_output_bytes`, `timeout_ms`, `max_bytes` strictement positifs.

### `system_error`

Objet **interne** : il n'est jamais envoyé au modèle. Il est persisté (`FailureRecord`), audité, exposé par l'API et la CLI (ADR-002). Le type reste dans l'énumération des messages pour la sérialisation et l'audit.

### `chunk_request`

Type de **tâche** (§12.6), pas type de message : il n'apparaît que dans `tasks[]` d'un `execution_plan`.

## Conséquences

- Les tables de transitions vivent dans `domain/transitions.py` sous forme de données (dictionnaire état → états autorisés) et sont les **seules** sources utilisées par `ConversationLifecycleManager` et `PlanRunner` ; les tests de phase 1 les parcourent exhaustivement (toute paire non listée doit être rejetée).
- La table des messages attendus vit dans `protocol/adapter.py` (`EXPECTED_INBOUND`) et est testée exhaustivement en phase 2.
- Les diagrammes de référence : `docs/architecture/01-state-machines.md`.
