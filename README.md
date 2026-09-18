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

Le modèle doit respecter une grammaire fermée. La première réponse est un `discovery_plan` (c'est ainsi que le modèle découvre l'OS, le shell, le répertoire courant, les versions installées — l'application n'injecte rien) — ou, quand la demande n'appelle aucune commande (une explication, une analyse, une question à poser à l'utilisateur), un `user_response` ([ADR-022](docs/adr/ADR-022-reponse-utilisateur.md) ; `protocol.allow_direct_response = false` rétablit la règle stricte de la spec). Un `user_response` conclut le tour comme un `final_answer` : son corps est opaque, borné, affiché tel quel ; `expects_reply = true` signifie que le modèle attend une réponse de l'utilisateur (`agentic-app reply <sid> "…"`).

```mermaid
sequenceDiagram
    autonumber
    participant U as Utilisateur
    participant A as Application
    participant M as Modèle distant
    participant S as Shell local

    U->>A: user_request (goal, message, session_budget)
    A->>M: POST user_request
    M-->>A: GET → discovery_plan (ou user_response : le tour se termine, ADR-022)
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
        M-->>A: GET → execution_plan | final_answer | user_response
    end
    M-->>A: GET → final_answer (ou user_response)
    A-->>U: diagnostic, preuves, prochaine étape (ou la réponse directe du modèle)
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
| [`docs/guides/`](docs/guides/README.md) | **Guides pratiques** : [prendre en main l'application](docs/guides/01-prise-en-main.md), [brancher un modèle par configuration](docs/guides/02-brancher-un-modele.md), [écrire un provider de transport](docs/guides/03-ecrire-un-provider.md), [écrire un codec de messages](docs/guides/04-ecrire-un-codec.md) |
| [`examples/`](examples/README.md) | Un provider et un codec écrits **hors de l'application** (`acme_model_plugin`), sélectionnés par chemin d'import dans [`examples/config.acme.toml`](examples/config.acme.toml), couverts par les tests |
| [`src/agentic_local_app/transport/README.md`](src/agentic_local_app/transport/README.md) | Carte du paquet transport : contrat, base HTTP, registres, codecs, invariants |

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

## 7. Utilisation

```bash
# 1. lancer un modèle simulé (rejoue le scénario Java de la spec §12, contrat ADR-004)
uv run agentic-app mock-server --host 127.0.0.1 --port 9000
#    ou le scénario « analyse » : le modèle répond directement, sans commande (ADR-022)
uv run agentic-app mock-server --scenario-name analysis

# 2. lancer une session en console (affichage en direct, Ctrl-C = interruption propre)
uv run agentic-app run "Understand the root cause of a Java build failure" \
    --message "Please debug the Java error in my project."
#    quand le modèle pose une question (user_response avec expects_reply), lui répondre :
uv run agentic-app reply <sid> "Only the service-api module fails."

# 3. ou exposer l'API locale (REST + flux SSE) pour un front ou un autre outil
uv run agentic-app serve --host 127.0.0.1 --port 8765
curl -X POST http://127.0.0.1:8765/api/v1/sessions \
     -H "Content-Type: application/json" \
     -d '{"goal":"…","user_message":"…"}'
curl -N http://127.0.0.1:8765/api/v1/events            # flux live de tous les événements
curl http://127.0.0.1:8765/api/v1/sessions/<sid>/snapshot
curl http://127.0.0.1:8765/api/v1/sessions/<sid>/reply  # la dernière réponse : final_answer ou user_response

# 4. vérifier ou afficher la configuration effective
uv run agentic-app config validate
uv run agentic-app config show

# 5. choisir l'implémentation du transport (ADR-020) : lister les providers, voir l'effectif
uv run agentic-app transport list
uv run agentic-app transport show

# 6. choisir le codec de messages (ADR-021) : lister les codecs, voir l'effectif
uv run agentic-app codec list
uv run agentic-app codec show
```

Tout ce qui est externe ou paramétrable se règle **une seule fois** dans [`config.toml`](config.toml) (endpoints du modèle, jeton via variable d'environnement, identifiant utilisateur, timeouts, drains, limites de payload, réponse directe du modèle (`[protocol]`), budgets, seuils de contexte, API). Pour brancher un vrai modèle : renseigner `[transport]` (`init_url`, `post_url`, `get_url`, `user_id`) et exporter le jeton dans la variable nommée par `token_env`. Le contrat attendu de l'endpoint est décrit dans [ADR-004](docs/adr/ADR-004-contrat-de-transport.md) ; le serveur mock en est l'implémentation de référence. Le pas-à-pas complet — les quatre requêtes, l'arbre de décision provider / codec, la vérification, le dépannage par code d'erreur — est le [guide 02](docs/guides/02-brancher-un-modele.md) ; la prise en main de l'application (installation, configuration, première session, API et flux live) est le [guide 01](docs/guides/01-prise-en-main.md).

**Choisir un provider de transport.** Le contrat `TransportGateway` est unique, mais son implémentation se choisit **par configuration** avec `transport.provider` ([ADR-020](docs/adr/ADR-020-transport-enfichable.md)) :

| `transport.provider` | Quand | Configuration |
|---|---|---|
| `generic_http` (défaut) | le modèle expose le contrat ADR-004 (le serveur mock, ou un adaptateur qui le respecte) | `init_url`, `post_url`, `get_url`, `close_url` + `close_method` (`POST` / `DELETE`), `token_env`, `user_id` |
| `templated_http` | le modèle expose une API HTTP quelconque | `[transport.options]` : pour chaque opération `init` / `post` / `get` / `close`, la méthode, l'URL, les en-têtes et le corps sous forme de gabarits (`{conversation_id}`, `{after}`, `{instructions}`, `{user_id}`, `{metadata_json}`, `{message_json}`, `{message_id}`, `{message_type}`, `{token}`, `${env:VAR}` résolu à l'appel) et les chemins de lecture des réponses (`conversation_id_path = "data.id"`, `messages_path = "items"`, `cursor_path`…) ; exemple complet commenté dans `config.toml` |
| `fake` | un lancement sans aucun réseau (démonstration, tests d'interface) | aucune |
| `paquet.module:Classe` ou un entry point `agentic_local_app.transports` | un provider écrit en dehors du dépôt (dérivant de `HttpProviderBase` ou directement de `TransportGateway`) | ses propres options, validées par son `options_model` |

```toml
[transport]
provider = "templated_http"
[transport.options]
headers = { "X-Api-Key" = "${env:MY_MODEL_KEY}" }
[transport.options.init]
url = "https://api.example.com/v1/threads"
body = { instructions = "{instructions}", user = "{user_id}", meta = "{metadata_json}" }
conversation_id_path = "data.id"
[transport.options.post]
url = "https://api.example.com/v1/threads/{conversation_id}/messages"
body = { role = "user", content = "{message_json}" }
[transport.options.get]
url = "https://api.example.com/v1/threads/{conversation_id}/messages?since={after}"
messages_path = "items"
```

`agentic-app transport show` affiche le provider effectif, sa classe et ses options avec les secrets masqués ; un provider inconnu ou des options invalides sont refusés au démarrage avec un message explicite.

**Choisir un codec.** Le provider parle l'API ; le **codec** parle le modèle ([ADR-021](docs/adr/ADR-021-codec-de-messages-par-modele.md)). Un modèle réel ne rend pas ses réponses sous forme d'enveloppes protocolaires : il rend du texte où le JSON du message est entouré de prose ou de clôtures ```` ```json ````, un objet *chat completion*, ou un appel d'outil. `transport.codec` désigne la conversion appliquée autour du transport, dans les deux sens, sans rien changer à l'orchestration :

| `transport.codec` | Quand | `[transport.codec_options]` |
|---|---|---|
| `passthrough` (défaut) | le transport rend déjà des enveloppes (contrat ADR-004, ou `templated_http` avec `message_path`) | aucune ; le transport n'est pas enveloppé |
| `json_text` | le modèle répond par du texte contenant le JSON du message | `content_path` (le texte dans un objet, ex. `choices[0].message.content` ; absent = chaînes nues), `strip_code_fences`, `extract_first_json_object`, `id_path` (`message_id` s'il manque), `conversation_id_fallback`, `outbound = "object"` ou `"text"` (le message part en JSON canonique) |
| `tool_call` | le modèle répond par un appel d'outil dont les arguments sont le message | `arguments_path`, `name_path` + `tool_name`, `id_path`, `conversation_id_fallback`, `outbound` |
| `paquet.module:Classe` ou un entry point `agentic_local_app.codecs` | un codec écrit en dehors du dépôt (classe concrète de `MessageCodec`) | ses propres options, validées par son `options_model` |

```toml
[transport]
provider = "templated_http"
codec = "json_text"
[transport.codec_options]
content_path = "choices[0].message.content"
outbound = "text"
[transport.options.post]
url = "https://api.example.com/v1/threads/{conversation_id}/chat/completions"
body = { messages = [{ role = "user", content = "{message_json}" }] }   # content reçoit le texte
```

Une réponse que le codec ne sait pas lire (pas de JSON, JSON invalide, chemin absent) est un échec `MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY`, jamais rejoué, dont le `FailureRecord` garde un extrait de la forme brute (`excerpt`) et la raison ; `agentic-app codec show` affiche le codec effectif et ses options. L'exemple complet (init, post, get, options du codec) est commenté dans `config.toml`.

Les routes de l'API sont listées dans [docs/phases/phase-09-interfaces.md](docs/phases/phase-09-interfaces.md) et [ADR-018](docs/adr/ADR-018-api-pour-un-front-et-flux-live.md).

## 8. Feuille de route (ordre imposé par la spec §20)

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

Chaque phase a une **gate** : sa suite de tests doit être entièrement verte avant d'ouvrir la suivante. L'état d'avancement est tenu dans [`docs/phases/README.md`](docs/phases/README.md). **État : les 10 phases sont livrées et vertes** (2 450 tests, couverture 97 %, Python 3.11/3.12, Linux + Windows).

## 9. Périmètre de la v1 — à lire avant de lancer l'application sur une vraie machine

Le modèle est un **orchestrateur de confiance** : les commandes sont exécutées **telles quelles**, sans sandbox ni contrôle de périmètre (spec §1). L'application est donc, de fait, un shell distant piloté par le modèle. Le sandboxing est explicitement reporté à une version ultérieure.
