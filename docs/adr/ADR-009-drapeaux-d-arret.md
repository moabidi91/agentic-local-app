# ADR-009 — Drapeaux d'arrêt : règle effective, valeurs par défaut, rôle de `critical`

**Statut** : accepté (2026-09-18)

## Contexte

Trois drapeaux produisent le même effet à l'échec d'une tâche : `critical = true`, `stop_plan_on_failure = true`, `continue_on_error = false` (§8.3, combinés par un **ou** logique). Les exemples de la spec les déclarent toujours de façon cohérente, mais rien ne dit ce qui se passe quand un champ est absent (la tâche `chunk_request` de §12.6 n'en a aucun), ni si `critical` change autre chose que l'arrêt du plan.

## Décision

1. **Valeurs par défaut quand un champ est absent** : `critical = false`, `continue_on_error = false`, `stop_plan_on_failure = false`, `stop_plan_on_success = false`. Conséquence directe : **un échec arrête le plan sauf si le modèle a explicitement écrit `continue_on_error: true`**. C'est le comportement le plus sûr et il est cohérent avec tous les exemples de la spec (les tâches tolérantes y portent toutes `continue_on_error: true`).
2. **Règle effective**, calculée une fois à la validation du plan et persistée sur la `TaskRecord` :

```
stops_plan_on_failure = critical or stop_plan_on_failure or not continue_on_error
stops_plan_on_success = stop_plan_on_success
```

   Une combinaison contradictoire (`critical: true` avec `continue_on_error: true`) n'est pas une erreur : le **ou** l'emporte, le plan s'arrête, et le validateur émet un avertissement d'audit `CONTRADICTORY_FLAGS` pour la traçabilité.
3. **Rôle de `critical`** : il n'ajoute aucune règle d'arrêt supplémentaire. Il qualifie le `stop_reason` et le statut rapporté :

| Cause de l'arrêt | `plan.status` | `stop_reason` |
|---|---|---|
| échec d'une tâche `critical` | STOPPED_ON_FAILURE | `critical_task_failed:<task_id>` |
| échec avec `stop_plan_on_failure` | STOPPED_ON_FAILURE | `stop_plan_on_failure:<task_id>` |
| échec sans `continue_on_error` | STOPPED_ON_FAILURE | `task_failed:<task_id>` |
| succès avec `stop_plan_on_success` | SHORT_CIRCUITED_ON_SUCCESS | `stop_plan_on_success:<task_id>` |
| interruption utilisateur | INTERRUPTED | `user_interrupt` |
| redémarrage (ADR-016) | INTERRUPTED | `restart` |
| budget dépassé entre deux tâches (ADR-012) | FAILED | `budget_exceeded:<max_cycles|max_plans|max_total_duration_ms>` |

   Quand plusieurs conditions sont vraies pour la même tâche, la première ligne applicable de ce tableau donne le libellé.
4. `execution_result.content.status` est le statut du plan en minuscules : `completed`, `stopped_on_failure`, `short_circuited_on_success`, `failed`. Un plan interrompu ne produit pas d'`execution_result` (§8.4).
5. Une tâche dont une dépendance (`depends_on`) n'a pas terminé en `COMPLETED` passe en `SKIPPED` avec `skip_reason = dependency_failed:<dep_id>` (ou `dependency_skipped:<dep_id>`), et ce **même si** le plan continue (la dépendance ayant `continue_on_error: true`). Les listes `skipped_tasks`, `cancelled_tasks`, `interrupted_tasks` de l'`execution_result` contiennent des objets `{ "task_id", "reason" }` et non de simples identifiants.

## Conséquences

- Tests phase 5 : une table paramétrée couvrant les 16 combinaisons des quatre drapeaux × {succès, échec}, plus les cas « champ absent ».
- `ResultCollector` produit les objets `{task_id, reason}` ; le schéma §12.5 est étendu en conséquence (ajout compatible : les listes vides restent vides).
- Instructions du protocole (ADR-004) : les défauts sont annoncés au modèle.
