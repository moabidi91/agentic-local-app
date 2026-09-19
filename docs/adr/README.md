# Architecture Decision Records

Chaque ADR fixe un point que la spec v1.1 laissait ambigu, contradictoire ou muet. La spec n'est jamais modifiée en place : elle reste la source de vérité, et les ADR disent comment on la lit et où on la complète.

Format : **Contexte** (ce que dit la spec, où ça coince) · **Décision** · **Conséquences** (code, tests, protocole) · **Statut**.

| # | Titre | Touche | Statut |
|---|---|---|---|
| [ADR-001](ADR-001-stack-technique.md) | Stack technique : Python, asyncio, pydantic, SQLite, pytest | tout | accepté |
| [ADR-002](ADR-002-interfaces-utilisateur.md) | Interfaces : CLI + API HTTP locale | §3.1, §4 | accepté |
| [ADR-003](ADR-003-plateformes-cibles.md) | Plateformes : Windows + Linux/macOS, couche plateforme | §2.4, §3.8, §17.5 | accepté (§3 amendé par ADR-030) |
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
| [ADR-019](ADR-019-consolidation-vague-1.md) | Arbitrages de consolidation après la vague 1 (chunk ref, rotation en WARNING, transitions, bornes, coût de rotation, disjoncteur, noms, lancement, SQLite FULL, savepoints) | ADR-003/004/005/007/008/010/011/012/013/014 | accepté |
| [ADR-020](ADR-020-transport-enfichable.md) | Transport enfichable : providers choisis par configuration (registre, base template method, `templated_http`) | §3.12, ADR-004 | accepté |
| [ADR-021](ADR-021-codec-de-messages-par-modele.md) | Codec de messages par modèle : forme brute des réponses (texte, chat completion, appel d'outil) convertie par configuration, décorateur transparent, `UNPARSEABLE_REPLY` | §3.12, §3.5, ADR-004, ADR-020 | accepté |
| [ADR-022](ADR-022-reponse-utilisateur.md) | `user_response` : le modèle répond directement à l'utilisateur (corps opaque borné, question avec `expects_reply`, drapeau `protocol.allow_direct_response` sur le premier message, chemin du `final_answer`, `GET /responses` / `/reply`, `agentic-app reply`) | §3.5, §11, §12, §14, ADR-007 | accepté |
| [ADR-023](ADR-023-politique-de-correction.md) | Politique de correction : `protocol_correction_request` (rappel du protocole, types attendus, exemple minimal), compteur de réponses inutilisables consécutives, `protocol.max_correction_attempts`, ordre face à la rotation, `correction.requested` / `corrections_total` / `GET /corrections` | §7.2, §14, §3.5, ADR-007, ADR-019, ADR-021 | accepté |
| [ADR-024](ADR-024-profils-de-modele.md) | Profils de modèle nommés `[models]` (un profil = une `TransportSection` complète, `models.active` choisi au démarrage, `[transport]` = profil implicite `default`, `requires_credentials`) et identité machine (`identity.py`, ordre de résolution et `source`) | §3.12, §4, ADR-018, ADR-020, ADR-021 | accepté |
| [ADR-025](ADR-025-pause-sur-erreur-d-authentification.md) | Pause sur erreur d'authentification : `SessionState.PAUSED` non terminal, un `401` suspend la session au lieu de l'échouer, jeton fourni par `POST /credentials` puis reprise du message en attente, `paused_reason`, borne `max_resume_attempts` | §7.1, §7.2, ADR-007, ADR-016, ADR-024 | accepté |
| [ADR-026](ADR-026-espace-de-travail-et-fichiers-temporaires.md) | Espace de travail par session et fichiers temporaires : section `[scratch]`, dossier par session, `working_space` fourni par l'utilisateur jamais supprimé, variables `AGENTIC_SCRATCH_DIR` / `AGENTIC_WORKING_SPACE` / `AGENTIC_SESSION_ID`, inventaire et politique de nettoyage | §3.4, ADR-003, ADR-018 | accepté |
| [ADR-027](ADR-027-champs-d-identifiants-par-modele.md) | Champs d'identifiants déclarés par le profil (`credential_fields`, `secret` fermé par défaut, champ implicite `access_token`), `GET /models` sans `env`, `POST /credentials` en objet plat avec l'alias `token`, section `[skills]` et `GET /skills`, `skills` / `effort` tracés dans `session.created` sans être transmis au modèle | §3.12, §4, ADR-024, ADR-025 | accepté |
| [ADR-028](ADR-028-ouverture-de-session-sans-message.md) | Ouverture de session sans message : `goal` et `user_message` facultatifs mais appariés (`400 GOAL_REQUIRED` / `USER_MESSAGE_REQUIRED`), session `READY` sans conversation ni cycle, premier message qui ouvre la première conversation et devient le but, `user_id` porté par la session et aligné sur `GET /whoami`, `agentic-app open`, origines CORS du front (1420) | §5, §16, ADR-006, ADR-016, ADR-018, ADR-024 | accepté |
| [ADR-029](ADR-029-echec-d-outil-comme-verdict.md) | L'échec d'un outil est un verdict : troncature qui garantit une part à chaque flux (amende ADR-011 §1), `[execution] verdict_programs` dont l'échec n'arrête pas le plan sauf consigne explicite du modèle (précise ADR-009 §1), `default_continue_on_error` au niveau du plan, champs `execution` et `failure_is_verdict` du résultat de tâche | §2.5, §8.3, §12.5, ADR-009, ADR-010, ADR-011, ADR-017 | accepté |
| [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) | Le shell de la machine détecté, annoncé et traduit : `DetectedShell` / `ExecutionEnvironment` (dialectes `posix` / `powershell` / `cmd` / `unknown`, `which` injecté), code de sortie natif préservé sur Windows (`-EncodedCommand` + `exit $LASTEXITCODE`), environnement annoncé dans les instructions et dictionnaire fermé entre dialectes appliqué seulement quand il est exact, tracé des deux côtés (`[execution] translate_commands`, champ `translation`, `agentic-app shell show` / `shell rules`) | §3.8, §17.5, ADR-003 §3, ADR-029 | accepté |

## Comment contester un ADR

Ouvrir une issue ou une PR qui modifie l'ADR concerné (statut `remplacé par ADR-xxx`) ; le code et les tests qui en dépendent sont listés dans la section *Conséquences* de chaque ADR.
