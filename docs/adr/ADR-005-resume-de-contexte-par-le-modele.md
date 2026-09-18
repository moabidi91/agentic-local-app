# ADR-005 — Résumé de contexte : `state_summary` porté par le modèle

**Statut** : accepté (2026-09-18)

## Contexte

Le `ContextReducer` doit construire « un résumé structuré compact » qui « préserve uniquement les découvertes critiques et l'état d'exécution courant » (§3.11), et l'exemple §12.8 contient des `key_findings` comme « pom.xml targets Java 21 » ou `current_state: "Java version mismatch confirmed as root cause"`. Ce sont des **interprétations** de sorties de commandes ; or l'application « ne traduit pas sémantiquement » (§1) et n'a aucun moyen déterministe de les produire. L'alternative — demander un résumé au modèle au moment de la rotation — est fragile : la rotation se produit précisément quand le contexte du modèle déborde.

## Décision

1. Chaque plan émis par le modèle (`discovery_plan`, `execution_plan`, `priority_clarification`) porte un champ **`state_summary`** dans `content`, écrit par le modèle, borné par `max_state_summary_bytes` (défaut 4 096 octets, annoncé dans les instructions du protocole) :

```json
"state_summary": {
  "environment": { "os": "Windows 11 x64", "shell": "powershell", "cwd": "C:\\work\\proj" },
  "findings": [ "Java runtime = 17.0.12", "pom.xml targets Java 21" ],
  "current_state": "Version mismatch suspected, confirming Maven runtime",
  "next_expected_step": "Confirm with mvn -version then conclude"
}
```

   Un plan sans `state_summary` est accepté (champ optionnel) : le dernier résumé connu est conservé. Un `state_summary` qui dépasse la borne est une `MODEL_PROTOCOL_ERROR`.

2. Le **`ContextReducer` n'interprète rien**. Il assemble, dans un ordre fixe et une forme canonique, uniquement des données déjà structurées :

| Section du résumé | Source | Nature |
|---|---|---|
| `goal`, `user_message` | `user_request` initial | copie |
| `environment`, `findings`, `current_state`, `next_expected_step` | dernier `state_summary` reçu | copie verbatim |
| `plan_ledger` | store | pour chaque plan : `plan_id`, `plan_type`, `objective`, `status`, `stop_reason`, et par tâche : `task_id`, `cmd`, `status`, `exit_code`, `truncated`, `original_size_bytes` |
| `pending_outputs` | store | références des sorties tronquées encore récupérables par `chunk_request` (`task_id`, `stream`, `total_bytes`) |
| `budget` | `SessionRecord` (ADR-012) | compteurs consommés / limites |

   Le `context_summary` de §12.8 est produit à partir de ces sections ; ses champs `environment`, `completed_plans[].key_findings` et `current_state` proviennent du `state_summary` du modèle, jamais d'une lecture des sorties par l'application.

3. Si le résumé assemblé dépasse `context_summary_budget_bytes`, le `ContextReducer` applique une **réduction déterministe par paliers** avant d'échouer : (a) retirer les `cmd` du `plan_ledger`, (b) ne garder que les plans non terminés et le dernier plan terminé, (c) retirer `pending_outputs`. Si le résumé dépasse encore le budget après (c) → `ROTATION_FAILED`, explicite (§2.6). Le palier appliqué est enregistré dans le `ContextSummaryRecord`.

## Conséquences

- Schéma de plan étendu (`protocol/messages.py`) ; les instructions du protocole (ADR-004) demandent au modèle de tenir `state_summary` à jour à chaque plan.
- Tests phase 8 : `given_last_state_summary_when_summary_built_then_findings_copied_verbatim`, `given_ledger_exceeding_budget_when_reduction_applied_then_steps_recorded`, `given_summary_exceeding_budget_after_all_steps_when_built_then_rotation_failed`.
- Le résumé est **reproductible** : deux constructions à partir du même store produisent le même JSON canonique (clé triée, séparateurs fixes), ce qui est testé.
