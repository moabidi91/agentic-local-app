# ADR-018 — API pensée pour un front à venir : flux live, lecture rapide de l'état, configuration globale

**Statut** : accepté (2026-09-18)

## Contexte

En v1 l'application n'est utilisable qu'en console, mais une application front viendra l'exploiter. Elle devra voir **en direct** ce qui se passe (pour suivre et déboguer), retrouver **vite** les sessions, plans et tâches en cours, et lire les sorties. Par ailleurs tout ce qui est externe ou paramétrable (endpoints du modèle, jeton, identifiant utilisateur, timeouts, limites…) doit se configurer **une seule fois**, en un seul endroit.

## Décision

### Configuration globale unique : `config.toml`

Un seul fichier, chargé au démarrage par `agentic_local_app.config.load_config()`, validé par un modèle pydantic `AppConfig` (valeurs par défaut sûres, erreurs de configuration explicites au démarrage). Recherche : `--config <chemin>` de la CLI, sinon variable `AGENTIC_APP_CONFIG`, sinon `./config.toml`, sinon les défauts. Les **secrets ne sont pas dans le fichier** : `transport.token_env` nomme la variable d'environnement qui porte le jeton (défaut `AGENTIC_TRANSPORT_TOKEN`), et un fichier `.env` local (ignoré par git) peut la fournir. Toute valeur est surchargeable par variable d'environnement `AGENTIC__<section>__<clé>` (double underscore), utile pour la CI et les conteneurs.

Sections : `app` (nom, `data_dir`, niveau de log), `transport` (ADR-004), `execution` (shell, cwd, timeouts, drains, ADR-003/008), `payload` (ADR-010), `protocol` (réponse directe du modèle, ADR-022), `context` (ADR-013), `budget` (défauts de session, ADR-012), `retry` et `circuit_breaker` (§7), `api` (hôte, port, CORS pour le front, taille des pages), `cli`, `telemetry`. Le fichier `config.toml` du dépôt est **le** modèle commenté ; la CLI `agentic-app config show` affiche la configuration effective (jeton masqué) et `agentic-app config validate` la vérifie.

### API HTTP locale conçue pour le front

Principes : ressources REST pour l'état, **un flux SSE pour le direct**, pagination partout, identifiants stables, aucun état côté serveur autre que le store. Base : `http://127.0.0.1:8765/api/v1`.

| Méthode | Route | Rôle |
|---|---|---|
| POST | `/sessions` | crée et démarre une session |
| GET | `/sessions?status=running,ready&limit=…&cursor=…` | liste paginée, filtrable |
| GET | `/sessions/{sid}` | `SessionRecord` + conversation courante |
| POST | `/sessions/{sid}/interrupt` | interruption ; répond quand READY |
| POST | `/sessions/{sid}/messages` | message de suivi (conversation réutilisable, §11), ou réponse à une question du modèle (ADR-022) |
| GET | `/sessions/{sid}/snapshot` | snapshot complet `ExecutionTracker` (§4.1) — **une seule requête pour tout afficher** |
| GET | `/sessions/{sid}/responses` · `/sessions/{sid}/reply` | les réponses directes du modèle (`user_response`, ADR-022) ; la dernière réponse concluante, `final_answer` ou `user_response` |
| GET | `/sessions/{sid}/conversations` · `/conversations/{cid}` | chaîne de conversations (rotations) |
| GET | `/sessions/{sid}/plans` · `/plans/{pid}` | plans avec compteurs, tâches incluses à la demande (`?include=tasks`) |
| GET | `/sessions/{sid}/tasks?status=running` | tâches, filtrables par statut/plan — **la vue « en cours » du front** |
| GET | `/tasks/{tid}` | détail d'une tâche (tous les champs §4.1) |
| GET | `/tasks/{tid}/output?stream=stdout&offset=0&max_bytes=65536` | lecture par plage de la sortie brute (même moteur que `chunk_request`) |
| GET | `/sessions/{sid}/messages?direction=in,out` | messages protocolaires échangés (debug) |
| GET | `/sessions/{sid}/failures` | `FailureRecord` |
| GET | `/sessions/{sid}/audit?after=…` | chaîne d'audit paginée + `GET /sessions/{sid}/audit/verify` |
| GET | `/sessions/{sid}/events` **(SSE)** | **flux live** : chaque événement du bus, `id` = `event_id` d'audit, reprise avec `Last-Event-ID` |
| GET | `/events` **(SSE)** | flux live toutes sessions (tableau de bord) |
| GET | `/tasks/{tid}/output/live` **(SSE)** | sortie d'une tâche **pendant** qu'elle tourne (événements `task.output`) |
| GET | `/metrics` | métriques `TelemetryService` (format texte type Prometheus) |
| GET | `/health` · `/config` | santé, configuration effective (jeton masqué) |

### Flux live : ce qui circule

Le flux SSE rejoue les événements de l'`EventBus` (ADR-015) sérialisés en JSON canonique : `event_id`, `event_type`, `timestamp`, `session_id`, `conversation_id`, `cycle_id`, `plan_id`, `task_id`, `payload`. Le catalogue est dans `docs/architecture/08-observability.md`. Pour le direct, l'exécuteur publie en plus des événements **`task.output`** (`stream`, `offset`, `data`), émis par tranches d'au plus `execution.live_output_chunk_bytes` (défaut 4 096) et au plus toutes les `execution.live_output_interval_ms` (défaut 250 ms) par tâche — c'est une **copie** de ce qui est écrit dans le blob, jamais une source : le blob reste la vérité, le flux est un confort de suivi. Ces événements ne sont **pas** audités (volume), à la différence de tous les autres.

La CLI consomme exactement le même flux (elle est un client de l'API) : ce qui est visible en console l'est pour le front, sans exception.

## Conséquences

- `config.py` + `config.toml` livrés dès la phase 1 (la configuration est une dépendance de tous les composants) ; chaque ADR ultérieur cite ses clés.
- `EventBus` accepte des abonnés asynchrones (files SSE) sans casser la livraison synchrone : l'abonné SSE ne fait qu'empiler dans une `asyncio.Queue` bornée (abandon des clients trop lents, jamais de blocage du cœur).
- `SubprocessCommandExecutor` lit stdout/stderr par morceaux (nécessaire de toute façon pour le blob) et publie `task.output` ; `FakeCommandExecutor` peut simuler des tranches.
- Phase 9 : tests d'intégration de l'API (client de test FastAPI) dont un test SSE : `given_running_task_when_client_subscribes_to_events_then_task_output_chunks_streamed_in_order`.
