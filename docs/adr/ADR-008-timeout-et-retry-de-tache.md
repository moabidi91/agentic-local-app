# ADR-008 — `timeout_ms` par tâche, aucun retry automatique de tâche

**Statut** : accepté (2026-09-18)

## Contexte

`CommandExecutor` doit « appliquer un timeout par tâche » (§3.8) et une tâche peut finir `TIMED_OUT` (§5.3), mais aucun champ de timeout n'existe dans le schéma de tâche (§12). Par ailleurs `TIMEOUT_ERROR` est rejouable (§7.1) et `TaskRecord.attempt_count` existe (§16) : lus ensemble, ils pourraient laisser croire qu'une commande qui dépasse son temps est relancée automatiquement — ce qui, pour `mvn clean install` ou toute commande non idempotente, serait dangereux et contraire à l'esprit « le modèle décide, l'application exécute ».

## Décision

1. Le schéma de tâche `cmd` gagne un champ optionnel **`timeout_ms`**. Trois réglages côté application : `default_task_timeout_ms` (défaut 60 000) appliqué quand le champ est absent, `max_task_timeout_ms` (défaut 900 000) qui plafonne la valeur déclarée (une valeur supérieure est **ramenée** au plafond, pas rejetée, et le plafonnement est noté dans le résultat : `timeout_ms_applied`), et les délais de drain (ADR-003).
2. **Aucune tâche shell n'est relancée automatiquement** en v1. `attempt_count` vaut toujours 1 ; le champ est conservé pour compatibilité de schéma et pour une éventuelle politique future. Si le modèle veut réessayer, il émet une nouvelle tâche dans son prochain plan.
3. La retryabilité de §7.1 (`NETWORK_ERROR`, `TIMEOUT_ERROR`, `RATE_LIMIT_ERROR`, `SYSTEM_ERROR` transitoire) ne concerne que les **appels de transport** (POST/GET). Un dépassement de temps d'une commande n'est pas une `TIMEOUT_ERROR` du point de vue du `FailureManager` : c'est un résultat de tâche `TIMED_OUT`, rapporté au modèle dans l'`execution_result` avec `exit_code = null`, la sortie capturée jusqu'à la terminaison, et `timed_out: true`.
4. Pour les **conditions d'arrêt** (§8.3), `TIMED_OUT` est traité exactement comme `FAILED`. Une erreur de l'exécuteur lui-même (interpréteur introuvable, impossible de lancer le processus) donne une tâche `FAILED` avec `exit_code = null` et un `FailureRecord` de type `TASK_EXECUTION_ERROR` (`SPAWN_FAILED`).
5. Une `chunk_request` n'a pas de timeout (lecture locale) ; un `ref_task_id` inconnu ou une plage hors bornes donne une tâche `FAILED` (`CHUNK_REF_NOT_FOUND`, `CHUNK_RANGE_INVALID`), jamais une erreur de protocole : le plan continue selon ses drapeaux d'arrêt.

## Conséquences

- Schéma : `Task.timeout_ms: int | None`, résultat : `timed_out: bool`, `timeout_ms_applied: int`.
- Tests phase 4 : `given_task_running_when_timeout_exceeded_then_task_marked_timed_out` (double d'exécuteur qui « dort »), `given_declared_timeout_above_cap_when_task_runs_then_cap_applied_and_reported`.
- Tests phase 5 : `given_timed_out_task_with_stop_on_failure_when_plan_runs_then_plan_stopped_on_failure`.
- Instructions du protocole (ADR-004) : le modèle est informé du défaut et du plafond pour dimensionner ses commandes.
