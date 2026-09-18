# ADR-010 — Limites de payload : par tâche, par plan, plafond application, plafond message

**Statut** : accepté (2026-09-18)

## Contexte

§2.5 dit que le modèle déclare `max_output_bytes` « dans les métadonnées du plan », alors que tous les exemples le portent **par tâche**. Rien ne protège l'application elle-même si le modèle déclare une valeur énorme (§17.2 : « les payloads surdimensionnés ne doivent jamais être envoyés tels quels »), et rien ne borne la taille totale d'un `execution_result` (un plan de cinquante tâches à 32 Ko chacune fait 1,6 Mo). Enfin, §14 propose de *rotater* quand « payload ou contexte trop gros », or changer de conversation ne réduit pas la taille d'un résultat unique.

## Décision

Quatre bornes, du plus fin au plus large :

| Borne | Où | Défaut | Rôle |
|---|---|---|---|
| `task.max_output_bytes` | tâche (modèle) | — | budget déclaré pour **cette** tâche |
| `plan.default_max_output_bytes` | plan (modèle, optionnel) | — | valeur appliquée aux tâches qui ne déclarent rien |
| `payload.default_max_output_bytes` | config app | 8 192 | appliqué si ni la tâche ni le plan ne déclarent rien |
| `payload.hard_max_output_bytes` | config app | 262 144 | plafond absolu : une déclaration supérieure est **ramenée** au plafond (noté `max_output_bytes_applied` dans le résultat) |
| `payload.max_message_bytes` | config app | 1 048 576 | taille maximale d'un message sortant sérialisé |

Règle d'application par tâche : `effective = min(task.max_output_bytes ?? plan.default ?? app.default, hard_max)`.

**Plafond message** : après construction de l'`execution_result`, si sa sérialisation dépasse `max_message_bytes`, le `PayloadGuard` re-tronque de façon déterministe : il prend la tâche dont le `stdout` retenu est le plus long, divise son budget par deux (en conservant la fin, ADR-011), et recommence jusqu'à tenir dans la borne ; `stderr` n'est réduit qu'en dernier recours, une fois tous les `stdout` ramenés à zéro. Chaque re-troncature est visible dans le résultat (`truncated: true`, plages), et **rien n'est perdu** : la sortie brute complète reste dans le blob, récupérable par `chunk_request`. Le cas « un seul message trop gros » n'est donc **jamais** une cause de rotation ; la rotation ne traite que l'accumulation de contexte (ADR-013).

Le `state_summary` (ADR-005) et le résumé de rotation ont leurs propres bornes (`max_state_summary_bytes`, `context_summary_budget_bytes`).

## Conséquences

- `PayloadGuard.apply(raw, budget)` est une fonction pure testée exhaustivement (phase 4) ; `PayloadGuard.fit_message(result, max_message_bytes)` idem.
- Le résultat de tâche porte `max_output_bytes_applied` en plus de `truncated` et `original_size_bytes` (§2.5).
- Instructions du protocole (ADR-004) : les défauts et plafonds sont annoncés au modèle, qui sait ainsi qu'au-delà de `hard_max_output_bytes` il devra paginer par `chunk_request`.
