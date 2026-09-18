# ADR-002 — Interfaces utilisateur : CLI + API HTTP locale

**Statut** : accepté (2026-09-18)

## Contexte

La spec décrit un `ConversationManager` « point d'entrée des demandes et des interruptions » (§3.1) et exige que l'état soit observable à tout instant (§4), mais ne dit pas par quel canal l'utilisateur envoie une demande, interrompt, ou consulte le snapshot.

## Décision

Deux interfaces minces, sans aucune logique métier, qui appellent toutes deux le même `ConversationManager` :

1. **CLI interactive** (`agentic-app run`) : lance une session, affiche le snapshot en direct (état conversation / cycle / plan / tâches), et **Ctrl-C déclenche l'interruption** (pas l'arrêt du processus). Un second Ctrl-C pendant le drain force la sortie. `agentic-app status` lit le snapshot d'une session en cours via l'API.
2. **API HTTP locale** (`agentic-app serve`, FastAPI, liée à `127.0.0.1`) :

| Méthode | Route | Effet |
|---|---|---|
| `POST` | `/sessions` | crée une session (goal, user_message, session_budget, auto_close_on_final_answer) et démarre la boucle |
| `POST` | `/sessions/{id}/interrupt` | interruption immédiate ; répond quand READY est atteint (borné par `interrupt_drain_timeout_ms`) |
| `GET` | `/sessions/{id}/snapshot` | snapshot `ExecutionTracker` (tous les champs du §4.1) |
| `GET` | `/sessions/{id}/audit` | événements d'audit (pagination) |
| `GET` | `/sessions/{id}/tasks/{task_id}/output?stream=stdout&offset=0&max_bytes=…` | lecture d'une sortie brute par plage (même service que `chunk_request`) |
| `GET` | `/health` | état du processus, breaker, store |

Les erreurs normalisées (§6) sont renvoyées telles quelles (mêmes attributs) ; `system_error` n'est jamais envoyé au modèle (ADR-007), il est exposé ici.

## Conséquences

- `interfaces/cli.py` et `interfaces/http_api.py` ne contiennent que du mapping ; ils sont testés par des tests d'intégration légers en phase 9.
- Le gestionnaire de signal de la CLI ne fait qu'appeler `ConversationManager.interrupt()` ; toute la logique d'interruption reste dans `InterruptionHandler` (§3.4).
- Sous Windows, Ctrl-C est reçu comme `KeyboardInterrupt` / `SIGINT` par asyncio ; la CLI l'intercepte via `signal.signal` (POSIX) ou la boucle `ProactorEventLoop` (Windows), voir ADR-003.
