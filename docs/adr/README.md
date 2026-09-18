# Architecture Decision Records

Chaque ADR fixe un point que la spec v1.1 laissait ambigu, contradictoire ou muet. La spec n'est jamais modifiée en place : elle reste la source de vérité, et les ADR disent comment on la lit et où on la complète.

Format : **Contexte** (ce que dit la spec, où ça coince) · **Décision** · **Conséquences** (code, tests, protocole) · **Statut**.

| # | Titre | Touche | Statut |
|---|---|---|---|
| [ADR-001](ADR-001-stack-technique.md) | Stack technique : Python, asyncio, pydantic, SQLite, pytest | tout | accepté |
| [ADR-002](ADR-002-interfaces-utilisateur.md) | Interfaces : CLI + API HTTP locale | §3.1, §4 | accepté |
| [ADR-003](ADR-003-plateformes-cibles.md) | Plateformes : Windows + Linux/macOS, couche plateforme | §2.4, §3.8, §17.5 | accepté |
| [ADR-004](ADR-004-contrat-de-transport.md) | Contrat de transport : endpoints configurables, jeton, user id, mock | §2.1, §3.12 | accepté |
| [ADR-005](ADR-005-resume-de-contexte-par-le-modele.md) | Résumé de contexte : `state_summary` porté par le modèle | §2.6, §3.11, §12.8 | accepté |
| [ADR-006](ADR-006-interruption-nouvelle-conversation.md) | Interruption : INTERRUPTED terminal, nouvelle conversation ensuite | §2.9, §5.1, §9 | accepté |
| [ADR-007](ADR-007-amendements-machines-a-etats.md) | Amendements des machines à états et table protocolaire | §5, §14, §3.5 | accepté |
| [ADR-008](ADR-008-timeout-et-retry-de-tache.md) | `timeout_ms` par tâche, aucun retry automatique de tâche | §3.8, §7.1, §12 | accepté |
| [ADR-009](ADR-009-drapeaux-d-arret.md) | Drapeaux d'arrêt : règle effective et valeurs par défaut | §2.4, §8.3 | accepté |
| [ADR-010](ADR-010-limites-de-payload.md) | Limites de payload : par tâche, par plan, plafond app, plafond message | §2.5, §17.2 | accepté |
| [ADR-011](ADR-011-troncature-et-chunks.md) | Troncature stderr/stdout, plages, `chunk_request` avec `stream` | §2.5, §3.10, §12.6 | accepté |
| [ADR-012](ADR-012-budget-de-session.md) | Budget de session porté par un `SessionRecord` à travers les rotations | §2.8, §16, §17.1 | accepté |
| [ADR-013](ADR-013-metrique-de-saturation.md) | Métrique de saturation du contexte : octets cumulés et seuils | §5.4, §10 | accepté |
| [ADR-014](ADR-014-continuation-apres-rotation.md) | Continuation après rotation : renvoi du message sortant en attente | §10, §14 | accepté |
| [ADR-015](ADR-015-persister-avant-publier.md) | Persister avant publier : EventBus synchrone, store hors du bus | §3.20, §17.1 | accepté |
| [ADR-016](ADR-016-politique-de-reprise.md) | Politique de reprise après crash : pid persistés, interruption de cause restart | §3.18, §7.5 | accepté |
| [ADR-017](ADR-017-determinisme-des-resultats-et-identifiants.md) | Ordre déterministe des résultats, horloge et identifiants injectables | §8, §12.5, §18.3 | accepté |
| [ADR-018](ADR-018-api-pour-un-front-et-flux-live.md) | API pour un front à venir : flux live SSE, lecture rapide de l'état, `config.toml` global | §3.1, §4, §3.19 | accepté |

## Comment contester un ADR

Ouvrir une issue ou une PR qui modifie l'ADR concerné (statut `remplacé par ADR-xxx`) ; le code et les tests qui en dépendent sont listés dans la section *Conséquences* de chaque ADR.
