# ADR-012 — Budget de session porté par un `SessionRecord`, au-delà des rotations

**Statut** : accepté (2026-09-18) — amendé par [ADR-019](ADR-019-consolidation-vague-1.md)

## Contexte

Le budget (`max_cycles`, `max_plans`, `max_total_duration_ms`, §2.8) est stocké dans `ConversationRecord.session_budget_json` (§16). Une rotation crée une nouvelle conversation : si les compteurs consommés ne la suivent pas, chaque rotation **remet le budget à zéro** et la boucle infinie que le budget devait empêcher redevient possible. La spec parle par ailleurs de « session » (budget, `auto_close_on_final_answer`) sans jamais en définir l'enregistrement, et demande d'appliquer le budget « avant l'exécution du plan » (§17.1) sans dire ce qu'il advient d'un plan qui dure au-delà de l'échéance.

## Décision

1. Nouvel enregistrement **`SessionRecord`** (persistance, §16 étendu) :

```
session_id, status (ADR-006), goal, user_message, auto_close_on_final_answer,
budget_max_cycles, budget_max_plans, budget_max_total_duration_ms,
consumed_cycles, consumed_plans, started_at, ended_at, current_conversation_id,
interrupted_at, created_at, updated_at
```

   `ConversationRecord` gagne `session_id` et garde `session_budget_json` comme **copie de lecture** (snapshot du budget au moment de la création, pour l'audit) ; la vérité des compteurs est le `SessionRecord`.

2. **Comptage** : `consumed_cycles` s'incrémente à l'ouverture d'un cycle (message sortant persisté) ; `consumed_plans` à la persistance d'un plan reçu ; la durée est `now - started_at`. Les cycles de type `resume` (rotation) **comptent** : une rotation coûte un cycle, ce qui borne aussi le nombre de rotations.

3. **Points de contrôle** :
   - avant de traiter un message entrant du modèle (cycle → `max_cycles`) ;
   - après persistance d'un plan, avant son démarrage (`max_plans`, `max_total_duration_ms`) — plan `PENDING → FAILED` (ADR-007) ;
   - **entre deux tâches** d'un plan (`max_total_duration_ms`) : une tâche déjà lancée n'est jamais tuée pour cause de budget (déterminisme, pas de commande coupée en deux), mais aucune nouvelle tâche ne démarre après l'échéance ; le plan finit `FAILED` avec `stop_reason = budget_exceeded:max_total_duration_ms`, les tâches restantes `SKIPPED` (`reason = budget_exceeded`).

4. Dépassement → `BUDGET_EXCEEDED` (non rejouable, §7.2) : conversation `FAILED`, session `FAILED`, `FailureRecord` détaillant la borne atteinte et les valeurs consommées ; rien n'est envoyé au modèle (la conversation distante est fermée en best effort, ADR-006).

5. L'`ExecutionTracker` expose `session_budget` avec les trois limites et les trois valeurs consommées (§4.1, critère d'acceptation 12).

## Conséquences

- Phase 1 : machine à états de session ; phase 3 : `SessionRecord` ; phase 9 : `given_budget_max_plans_reached_when_plan_received_then_session_failed_with_budget_exceeded`, `given_deadline_passed_between_tasks_when_next_task_due_then_plan_failed_and_remaining_skipped`.
- ADR-006 (nouvelle conversation après interruption) et ADR-014 (rotation) s'appuient sur ce record pour conserver les compteurs.
