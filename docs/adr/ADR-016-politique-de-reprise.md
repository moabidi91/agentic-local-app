# ADR-016 — Politique de reprise après crash : pid persistés, interruption de cause `restart`

**Statut** : accepté (2026-09-18)

## Contexte

§3.18 et §7.5 donnent trois règles (tâche RUNNING → INTERRUPTED sans ré-exécution ; plan RUNNING reconstruit depuis les tâches ; POST sans GET → refaire le GET d'abord) puis « ré-entrer dans un état déterministe », sans dire lequel. Deux trous concrets : les sous-processus lancés avant le crash peuvent **survivre** au processus de l'application (orphelins), et rien ne dit si la conversation reprend ou s'arrête après la reconstruction du plan.

## Décision

1. **Traçabilité des processus** : quand une tâche passe `RUNNING`, la `TaskRecord` reçoit `pid` et `process_group_id` (POSIX) ou `pid` (Windows) **avant** que la commande ne soit considérée lancée (l'exécuteur renvoie le pid dès le spawn, l'attente du résultat vient ensuite). Au redémarrage, `RecoveryCoordinator` termine les orphelins encore vivants avec la mécanique en deux temps d'ADR-003 (vérification préalable que le pid correspond bien à un processus démarré après `started_at`, pour ne jamais tuer un processus réattribué).
2. **Politique par entité**, appliquée dans cet ordre au démarrage, chaque étape persistée et auditée (`RECOVERY_*`) :

| Constat | Action |
|---|---|
| Tâche `RUNNING` | orphelin terminé → tâche `INTERRUPTED` (`reason = restart`), jamais ré-exécutée |
| Tâche `PENDING` / `WAITING_DEPENDENCY` d'un plan `RUNNING` | `INTERRUPTED` (`reason = restart`) |
| Plan `RUNNING` ou `PENDING` | compteurs recalculés depuis les tâches → plan `INTERRUPTED` (`stop_reason = restart`) |
| Cycle `RUNNING` | `INTERRUPTED` |
| Conversation en `RUNNING_PLAN`, `ACTIVE`, `ROTATING` | `INTERRUPTED` (`reason = restart`) — même chemin que l'interruption utilisateur (ADR-006), sans rien envoyer au modèle |
| Conversation en `WAITING_MODEL_RESPONSE` avec message sortant persisté (« POST envoyé, pas de GET ») | **d'abord** un GET avec le curseur persisté ; si une réponse valide est là, la boucle reprend normalement (`RUNNING_PLAN` ou `COMPLETED`) ; sinon la politique d'échec de §7 s'applique (retries bornés puis `FAILED`) |
| Conversation en `WAITING_MODEL_RESPONSE` avec message sortant persisté mais POST non confirmé | le POST est **rejoué** (idempotent par `message_id`, ADR-004) puis GET |
| Session `RUNNING` / `INTERRUPTING` dont la conversation a fini `INTERRUPTED` | `READY` |
| Session `RUNNING` dont la conversation a repris | reste `RUNNING` |

3. Les tâches déjà `COMPLETED` ne sont jamais rejouées (§17.4) : rien ne relance un plan côté application ; si le modèle le souhaite, il le fera dans un nouveau plan à partir de l'`execution_result`… qui, pour un plan interrompu, n'existe pas. Après un redémarrage qui a interrompu un plan, la session est donc `READY` et l'utilisateur relance une demande (nouvelle conversation, ADR-006) ; le `RecoveryReport` (liste des entités touchées) est exposé par la CLI et l'API.
4. Le point d'entrée `agentic-app run` / `serve` exécute **toujours** `RecoveryCoordinator.recover()` avant d'accepter une demande ; sur un store vide c'est un no-op audité.

## Conséquences

- Schéma `TaskRecord` : `pid`, `process_group_id`, `stdout_ref`/`stderr_ref` peuvent être nuls pour une tâche interrompue au redémarrage.
- Tests phase 9 : `given_store_with_running_task_when_recovery_runs_then_task_interrupted_and_not_reexecuted`, `given_posted_message_without_reply_when_recovery_runs_then_get_retried_first`, `given_completed_tasks_when_recovery_runs_then_none_reexecuted` — l'exécuteur factice compte les appels.
- `PlatformAdapter.terminate_orphan(pid, started_at)` (ADR-003) est testé avec un double de table de processus.
