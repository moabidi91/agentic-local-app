# ADR-001 — Stack technique

**Statut** : accepté (2026-09-18)

## Contexte

La spec ne fixe ni langage ni outillage. Elle impose en revanche : TDD strict par phases (§18), des doubles de test injectables pour le shell, le réseau et la base (§18.3), une convention de nommage `given_…_when_…_then_…` (§18.4), de l'exécution parallèle de sous-processus avec annulation et délai de drain (§2.4, §8.3), et une persistance locale de tout l'état (§16).

## Décision

| Sujet | Choix |
|---|---|
| Langage | Python ≥ 3.11 (le conteneur de CI teste 3.11 et 3.12) |
| Concurrence | `asyncio` pour l'orchestration, le transport et l'exécution des sous-processus (`asyncio.create_subprocess_shell`) |
| Schémas & validation | `pydantic` v2 pour les messages du protocole et les records (validation stricte, sérialisation JSON canonique) |
| Persistance | SQLite (module standard `sqlite3`, mode WAL) derrière une interface `ConversationStore` ; implémentation `InMemoryConversationStore` pour les tests |
| Interface du store | **synchrone**. Les écritures sont minuscules et doivent précéder toute action (§17.1) ; un store synchrone rend l'ordre « persister puis agir » trivial à garantir et à tester. L'exécuteur et le transport sont asynchrones. |
| HTTP client | `httpx` (async, timeouts explicites, gzip) |
| API locale & mock | `fastapi` + `uvicorn` |
| CLI | `typer` + `rich` (affichage du snapshot en direct) |
| Tests | `pytest`, `pytest-asyncio` (mode auto), `pytest-timeout` (aucun test ne peut bloquer), `pytest-cov` |
| Qualité | `ruff` (lint + imports), `mypy --strict` |
| Gestion d'env | `uv` (`uv sync --extra dev`) |

Convention de nommage : pytest est configuré avec `python_functions = ["given_*", "test_*"]` pour que les fonctions de test portent **exactement** le nom imposé par la spec, sans préfixe `test_`.

Marqueurs : un marqueur `phaseN` par phase (`pytest -m phase1`) et `real_subprocess` pour les rares tests qui lancent un vrai shell (phase 4, exécutés sur les deux OS en CI, jamais dans les tests unitaires).

## Conséquences

- Arborescence `src/agentic_local_app/<couche>/…` (voir `docs/architecture/09-module-map.md`).
- Tout composant qui touche l'extérieur reçoit ses dépendances par injection (constructeur) : `CommandExecutor`, `TransportGateway`, `ConversationStore`, `Clock`, `IdGenerator` (ADR-017).
- Aucun test unitaire n'ouvre de socket, de fichier SQLite ou de processus ; les tests d'intégration (phase 9) utilisent le store SQLite sur fichier temporaire et le serveur mock (ADR-004).
