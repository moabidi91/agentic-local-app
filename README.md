# agentic-local-app

Application locale qui orchestre **un modèle distant** à travers un **protocole de messages strict** (`POST message` / `GET messages`), exécute sur la machine les commandes que le modèle planifie, et boucle jusqu'à une réponse finale — de façon **déterministe, persistée, auditable, interruptible et restart-safe**.

> Spécification de référence : [`docs/spec/SPEC-v1.1.md`](docs/spec/SPEC-v1.1.md).
> Toutes les décisions qui la précisent ou l'amendent sont tracées dans [`docs/adr`](docs/adr/README.md).

[![CI](https://github.com/moabidi91/agentic-local-app/actions/workflows/ci.yml/badge.svg)](https://github.com/moabidi91/agentic-local-app/actions/workflows/ci.yml)

---

## 1. L'idée en une image

Le modèle est le **cerveau** : il ne voit jamais la machine, il ne reçoit que des résultats de commandes. L'application est le **bras** : elle n'interprète rien, elle exécute des plans, mesure, tronque, persiste, audite, et rend compte.

```mermaid
flowchart LR
    U([Utilisateur])
    subgraph APP["agentic-local-app (machine locale)"]
        direction TB
        CLI[CLI / API HTTP locale]
        ORCH[ProtocolOrchestrator<br/>boucle protocolaire]
        RUN[PlanRunner<br/>exécution des plans]
        STORE[(ConversationStore<br/>SQLite + blobs)]
        AUDIT[(AuditLog<br/>chaîne hachée)]
    end
    M[[Modèle distant]]
    ENV[/Shell local<br/>Windows · Linux · macOS/]

    U -- "demande / interruption" --> CLI
    CLI --> ORCH
    ORCH -- "POST message" --> M
    M -- "GET messages" --> ORCH
    ORCH -- "plan" --> RUN
    RUN -- "cmd" --> ENV
    ENV -- "stdout / stderr / exit_code" --> RUN
    RUN -- "execution_result" --> ORCH
    ORCH --> STORE
    ORCH --> AUDIT
    RUN --> STORE
```

## 2. La boucle protocolaire

Le modèle doit respecter une grammaire fermée. La première réponse est **toujours** un `discovery_plan` (c'est ainsi que le modèle découvre l'OS, le shell, le répertoire courant, les versions installées — l'application n'injecte rien).

```mermaid
sequenceDiagram
    autonumber
    participant U as Utilisateur
    participant A as Application
    participant M as Modèle distant
    participant S as Shell local

    U->>A: user_request (goal, message, session_budget)
    A->>M: POST user_request
    M-->>A: GET → discovery_plan
    loop pour chaque tâche du plan
        A->>S: cmd
        S-->>A: stdout / stderr / exit_code
    end
    A->>M: POST execution_result (tronqué selon max_output_bytes)
    M-->>A: GET → execution_plan | priority_clarification
    loop tant que le modèle envoie des plans
        A->>S: cmd …
        S-->>A: …
        A->>M: POST execution_result
        M-->>A: GET → execution_plan | final_answer
    end
    M-->>A: GET → final_answer
    A-->>U: diagnostic, preuves, prochaine étape
    Note over U,A: À tout instant : interruption → SIGTERM + drain → tout marqué INTERRUPTED → READY
```

## 3. Les garanties, et le composant qui les porte

| Garantie | Mécanisme | Composant |
|---|---|---|
| Déterminisme | Machines à états explicites, transitions persistées **avant** d'être exploitées | `ConversationLifecycleManager`, `PlanRunner` |
| Contrôle de taille | `max_output_bytes` par tâche, troncature stderr-d'abord / fin-de-stdout, `chunk_request` | `PayloadGuard` |
| Contexte borné | Rotation de conversation avec résumé structuré, ACK obligatoire, échec explicite | `ContextReducer`, `ProtocolOrchestrator` |
| Boucles bornées | Budget de session (`max_cycles`, `max_plans`, `max_total_duration_ms`) | `ProtocolOrchestrator` |
| Interruption | SIGTERM + drain, tout INTERRUPTED, READY en temps borné | `InterruptionHandler` |
| Robustesse | Taxonomie d'erreurs fermée, retries bornés, circuit breaker | `FailureManager`, `RetryController`, `CircuitBreaker` |
| Restart-safe | Checkpoints + politique explicite de reprise | `RecoveryCoordinator` |
| Auditabilité | Journal append-only chaîné par hash | `AuditLog` |
| Observabilité | Snapshot cohérent à tout instant | `ExecutionTracker`, `TelemetryService` |

## 4. Architecture en couches

```mermaid
flowchart TB
    subgraph IF["interfaces"]
        CLI[cli]
        API[http_api]
    end
    subgraph ORC["orchestration"]
        CM[ConversationManager]
        PO[ProtocolOrchestrator]
        RY[RecoveryCoordinator]
    end
    subgraph SVC["services applicatifs"]
        LC[lifecycle · ConversationLifecycleManager]
        IH[interruption · InterruptionHandler]
        PA[protocol · ProtocolAdapter]
        PR[execution · PlanRunner / PayloadGuard / ResultCollector]
        CR[context · ContextReducer]
        FM[resilience · FailureManager / Retry / Breaker]
    end
    subgraph OBS["observability"]
        EB[EventBus]
        AL[AuditLog]
        TS[TelemetryService]
        ET[ExecutionTracker]
    end
    subgraph INFRA["infrastructure (injectable, doublée en test)"]
        CS[(persistence · ConversationStore)]
        CE[execution · CommandExecutor]
        TG[transport · TransportGateway]
    end
    subgraph DOM["domain (pur, sans I/O)"]
        ST[states · transitions · models · errors · events]
    end

    IF --> ORC
    ORC --> SVC
    SVC --> OBS
    SVC --> INFRA
    ORC --> INFRA
    SVC --> DOM
    INFRA --> DOM
    OBS --> DOM
```

Le détail de chaque couche et le mapping vers les modules Python est dans [`docs/architecture/09-module-map.md`](docs/architecture/09-module-map.md).

## 5. Documentation

| Dossier | Contenu |
|---|---|
| [`docs/spec/`](docs/spec/SPEC-v1.1.md) | La spécification v1.1, source de vérité |
| [`docs/architecture/`](docs/architecture/00-overview.md) | Conception détaillée : composants, machines à états, protocole, exécution, persistance, transport, rotation, interruption, observabilité, module map |
| [`docs/adr/`](docs/adr/README.md) | Architecture Decision Records : chaque arbitrage pris là où la spec était ambiguë ou muette |
| [`docs/phases/`](docs/phases/README.md) | Guides d'implémentation par phase TDD (objectif, conception, plan de tests, gate) |

## 6. Démarrage rapide

```bash
# prérequis : Python >= 3.11 et uv (https://docs.astral.sh/uv/)
uv sync --extra dev          # crée .venv et installe le projet + outils de test
uv run pytest                # lance toute la suite (unit + integration)
uv run pytest -m phase1      # seulement une phase
uv run ruff check src tests  # lint
uv run mypy                  # typage strict
```

Les tests suivent la convention imposée par la spec (§18.4) : `given_<état>_when_<action>_then_<résultat>`. pytest est configuré pour collecter directement ces fonctions (`python_functions = given_*`).

## 7. Feuille de route (ordre imposé par la spec §20)

```mermaid
flowchart LR
    P1[Phase 1<br/>Machines à états] --> P3[Phase 3<br/>Persistance]
    P3 --> P2[Phase 2<br/>Protocole]
    P2 --> P4[Phase 4<br/>Exécution de tâche]
    P4 --> P5[Phase 5<br/>Exécution de plan]
    P5 --> P6[Phase 6<br/>Interruption]
    P6 --> P7[Phase 7<br/>Transport & échecs]
    P7 --> P8[Phase 8<br/>Rotation de contexte]
    P8 --> P10[Phase 10<br/>Audit & observabilité]
    P10 --> P9[Phase 9<br/>Orchestration complète]
```

Chaque phase a une **gate** : sa suite de tests doit être entièrement verte avant d'ouvrir la suivante. L'état d'avancement est tenu dans [`docs/phases/README.md`](docs/phases/README.md).

## 8. Périmètre de la v1 — à lire avant de lancer l'application sur une vraie machine

Le modèle est un **orchestrateur de confiance** : les commandes sont exécutées **telles quelles**, sans sandbox ni contrôle de périmètre (spec §1). L'application est donc, de fait, un shell distant piloté par le modèle. Le sandboxing est explicitement reporté à une version ultérieure.
