# ADR-019 — Arbitrages de consolidation après la vague 1 (phases 1, 2, 3, 4, 7, 10)

**Statut** : accepté (2026-09-18) — amende ADR-003, ADR-004, ADR-005, ADR-007, ADR-008, ADR-010, ADR-011, ADR-012, ADR-013, ADR-014 ; §2 amendé par [ADR-023](ADR-023-politique-de-correction.md) (la rotation sur réponse inutilisable devient le repli, après les corrections)

## Contexte

La mise en œuvre parallèle des phases 1, 2, 3, 4, 7 et 10 et la rédaction des documents d'architecture ont fait remonter dix points où les ADR se contredisaient entre eux, contredisaient la spec, ou laissaient un trou. Chaque point est tranché ici ; les ADR concernés portent désormais la mention « amendé par ADR-019 » et le code a été aligné dans la même livraison.

## Décisions

### 1. `chunk_request` vers une tâche inconnue : deux niveaux (ADR-007 vs ADR-008/011)

- **Analyse (ProtocolAdapter)** : `ref_task_id` qui ne désigne **aucune tâche déclarée dans la session** → `MODEL_PROTOCOL_ERROR` / `CHUNK_REF_UNKNOWN`. L'orchestrateur passe à `parse_inbound` l'ensemble de **tous** les `task_id` connus de la session (`known_task_ids`), pas seulement ceux dont une sortie est stockée.
- **Exécution (PayloadGuard.serve_chunk)** : tâche connue mais sans blob (sautée, interrompue, `chunk_request` elle-même) → tâche `FAILED` / `CHUNK_REF_NOT_FOUND` ; plage hors bornes → `FAILED` / `CHUNK_RANGE_INVALID`. Le plan continue selon ses drapeaux (ADR-009).
- Le PlanRunner persiste **un blob par flux pour chaque tâche `cmd` exécutée, même vide**, pour que `chunk_request` sur un flux vide donne `CHUNK_RANGE_INVALID` (plage) et non `CHUNK_REF_NOT_FOUND`.

### 2. Réponses inutilisables et rotation (ADR-013 §3 vs §7.2 / §14)

La règle « `SATURATED` après N erreurs de protocole » est **retirée** : §14 impose l'arrêt dès la première `MODEL_PROTOCOL_ERROR`, et le `FailureManager` décide `fail`. Elle est remplacée par une règle déterministe qui réalise le « replies missing or unusable due to context accumulation » de §10 : **si la fenêtre de contexte est en `WARNING` (≥ 70 % du budget) au moment où survient une `MODEL_PROTOCOL_ERROR` ou l'épuisement des retries d'un `MODEL_GET_TIMEOUT`, l'orchestrateur rotate une fois au lieu d'échouer** ; en `HEALTHY`, la politique de §7 s'applique telle quelle. Clé de configuration : `context.rotate_on_unusable_reply_in_warning` (défaut `true`), qui remplace `protocol_errors_before_rotation`. `ConversationRecord.protocol_error_count` reste tenu pour l'observabilité.

**Amendé par [ADR-023](ADR-023-politique-de-correction.md)** : cette règle n'est plus la première réaction à une réponse inutilisable, c'est le **repli**. Tant que la fenêtre n'est pas `SATURATED`, l'application envoie d'abord un `protocol_correction_request` et relit contre la même attente, au plus `protocol.max_correction_attempts` fois d'affilée ; la rotation décrite ici ne s'applique qu'une fois ce budget épuisé (et une fenêtre déjà saturée l'emporte tout de suite, une correction ne pouvant pas tenir dans un contexte plein). L'épuisement des retries d'un `MODEL_GET_TIMEOUT`, qui n'est pas une faute du modèle, garde la règle telle quelle, sans correction.

### 3. Machines à états : deux transitions supplémentaires (ADR-007)

- `INTERRUPTING → FAILED` (session) : un échec de persistance pendant le nettoyage d'interruption ne peut pas laisser la session en `INTERRUPTING` — déjà présent dans `SESSION_TRANSITIONS`, désormais documenté.
- `WAITING_USER → ROTATING` (conversation) : un `user_request` de suivi projeté au-delà du budget de contexte (ADR-013 §3) doit pouvoir rotater depuis `WAITING_USER`.

### 4. Cohérence des bornes de taille (ADR-010 vs ADR-013)

Un message maximal doit toujours pouvoir être envoyé dans une conversation neuve, et une sortie de tâche doit tenir dans un message. Nouveaux défauts : `payload.hard_max_output_bytes = 131072`, `payload.max_message_bytes = 196608`, `context.budget_bytes = 400000`. Invariants **validés au chargement de la configuration** (`ConfigError` sinon) : `hard_max_output_bytes ≤ max_message_bytes`, `max_message_bytes × 2 ≤ context.budget_bytes`, `context.summary_budget_bytes ≤ max_message_bytes`.

### 5. Coût d'une rotation (ADR-012 / ADR-014)

Une rotation consomme **exactement un cycle** : le cycle de type `resume` (`context_resume_request` → `context_resume_ack`). La **retransmission** du message en attente appartient au cycle d'origine de ce message (elle en est la suite), elle n'ouvre pas de nouveau cycle. `max_rotations_per_session` borne en plus le nombre de rotations.

### 6. Disjoncteur consulté avant chaque appel distant (§7.4)

Le `FailureManager` consulte `breaker.allow()` au moment de décider un retry (phase 7) ; en complément, l'**orchestrateur** consulte `breaker.allow()` **avant chaque appel** init/POST/GET et, s'il est refusé, n'appelle pas le transport : il attend `open_duration_ms` (une seule fois, borné par le budget de durée) puis réévalue ; refus persistant → `NETWORK_ERROR` / `CIRCUIT_OPEN`, non rejouable, conversation `FAILED`. Un appel réussi appelle `FailureManager.note_success()`.

### 7. Noms de configuration : `config.toml` fait foi

`transport.token_env` (et non `token`), `transport.close_url = ""` (et non `null`), `context.summary_budget_bytes` (et non `context_summary_budget_bytes`). Les ADR-004/005/010 se lisent avec ces noms.

### 8. Recette de lancement des commandes (ADR-003)

`SubprocessCommandExecutor` lance `create_subprocess_exec(<shell>, "-c", <cmd>)` (POSIX) et `create_subprocess_exec("powershell", "-NoProfile", "-NonInteractive", "-Command", <cmd>)` ou `("cmd", "/c", <cmd>)` (Windows), plutôt que `create_subprocess_shell(..., executable=)` : bash lancé avec `argv[0] = sh` bascule en mode POSIX et change le comportement des commandes. La commande elle-même n'est jamais réécrite (§1).

### 9. Durabilité SQLite (ADR-001 / ADR-015)

`PRAGMA synchronous = FULL` (et non `NORMAL`) : un checkpoint stable doit survivre à une coupure de courant, pas seulement à un crash du processus. Le volume de données rend le coût négligeable.

### 10. Sémantique des transactions imbriquées du store mémoire (ADR-001)

`InMemoryConversationStore.transaction()` a désormais la sémantique **savepoint** exacte de SQLite : chaque niveau prend un instantané, une exception qui s'échappe d'un niveau ne restaure que ce niveau ; une exception interne attrapée par le bloc externe perd les écritures internes et conserve les externes. L'unicité d'`event_id` est globale (toutes sessions), et toute opération après `close()` échoue.

## Conséquences

- Code aligné : `domain/transitions.py` (§3), `config.py` + `config.toml` (§2, §4), `persistence/memory.py` (§10), `persistence/sqlite_store.py` (§9), tests des phases 0/2/3/4 ajustés.
- À appliquer par les phases suivantes : §1 et §6 par le PlanRunner et l'orchestrateur (phases 5, 9) ; §2 par l'orchestrateur (phase 9) ; §5 par le comptage de cycles (phase 9).
- Les documents d'architecture 01, 02, 05, 06 sont mis à jour pour retirer la règle du compteur d'erreurs de protocole.
