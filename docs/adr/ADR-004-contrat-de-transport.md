# ADR-004 — Contrat de transport : endpoints configurables, jeton optionnel, user id, serveur mock

**Statut** : accepté (2026-09-18)

## Contexte

La spec impose de ne parler au modèle qu'à travers `POST message` / `GET messages` (§2.1) et confie le réseau au `TransportGateway` (§3.12), mais ne définit ni URL, ni authentification, ni la manière de créer ou fermer une conversation distante, ni la sémantique du GET (quand la réponse du modèle est-elle disponible ?), ni comment le modèle apprend le protocole lui-même. Décision utilisateur : **les endpoints doivent être configurables** — un endpoint pour initialiser une conversation, un endpoint POST, un endpoint GET — avec en paramètres un **jeton** (facultatif, inutile pour les tests) et un **identifiant utilisateur** ; un serveur mock sert aux tests.

## Décision

### Configuration (`transport/config.py`)

```yaml
transport:
  init_url:  "https://model.example.com/v1/conversations"                       # POST : crée la conversation
  post_url:  "https://model.example.com/v1/conversations/{conversation_id}/messages"   # POST : dépose un message
  get_url:   "https://model.example.com/v1/conversations/{conversation_id}/messages?after={after}"  # GET : lit les messages
  close_url: null            # optionnel : POST de fermeture (auto_close_on_final_answer) ; null => fermeture locale seulement
  token: null                # optionnel : envoyé en `Authorization: Bearer <token>` si présent
  user_id: "hama"            # obligatoire : envoyé en en-tête `X-User-Id` et dans le corps de l'init
  request_timeout_ms: 15000  # timeout d'un appel HTTP
  poll_interval_ms: 1000     # intervalle entre deux GET quand aucune réponse n'est encore disponible
  reply_timeout_ms: 120000   # attente maximale d'une réponse du modèle avant TIMEOUT_ERROR (MODEL_GET_TIMEOUT)
  gzip: true                 # Content-Encoding: gzip sur les corps POST (§3.12)
```

Les URL sont des gabarits ; les seuls placeholders reconnus sont `{conversation_id}` et `{after}`.

### Les trois opérations

| Opération | Requête | Réponse attendue |
|---|---|---|
| **init** | `POST init_url` — corps `{ "user_id", "instructions": <texte du protocole>, "metadata": { "session_id", "parent_conversation_id" } }` | `201 { "conversation_id": "…" }` |
| **post** | `POST post_url` — corps = le message protocolaire complet (`type`, `conversation_id`, `message_id`, `content`) | `202 { "accepted": true, "message_id": "…" }` — **idempotent** : un re-POST avec le même `message_id` renvoie le même accusé sans dupliquer |
| **get** | `GET get_url` avec `after` = dernier `message_id` connu | `200 { "messages": [ …messages du modèle… ], "cursor": "<message_id du dernier>" }` — liste vide tant que le modèle n'a pas répondu |
| **close** (option) | `POST close_url` | `200` |

Le **GET est un polling** : le `TransportGateway` répète le GET toutes les `poll_interval_ms` jusqu'à obtenir au moins un message ou atteindre `reply_timeout_ms` (→ `TIMEOUT_ERROR` / `MODEL_GET_TIMEOUT`, rejouable selon §7.1). Le curseur garantit qu'aucun message n'est lu deux fois ni sauté.

### Bootstrap du protocole

Le modèle apprend le protocole par le champ `instructions` de l'init : l'application embarque `protocol/PROTOCOL_INSTRUCTIONS.md` (grammaire des messages, schémas JSON, règle du `discovery_plan` obligatoire, `state_summary` d'ADR-005, limites de payload d'ADR-010). Ce texte est le seul contenu « prompt » que l'application possède ; il est versionné avec le code.

### Classification HTTP → taxonomie (§6)

| HTTP / situation | `error_type` |
|---|---|
| 401 | AUTHN_ERROR |
| 403 | AUTHZ_ERROR |
| 429 | RATE_LIMIT_ERROR (respecte `Retry-After` si présent) |
| 408, 504, timeout client | TIMEOUT_ERROR |
| 413, ou corps `{"error":"context_window_exceeded"}` | MODEL_CONTEXT_WINDOW_ERROR |
| erreur de connexion, 502, 503 | NETWORK_ERROR |
| autre 5xx | SYSTEM_ERROR (transitoire) |
| corps non JSON / schéma de réponse invalide | MODEL_PROTOCOL_ERROR |

### Serveur mock (`testing/mock_model_server.py`)

Application FastAPI qui implémente exactement ce contrat et joue le rôle du modèle à partir d'un **scénario** (fichier JSON : suite de réponses attendues, éventuellement conditionnées par le type du dernier message reçu). Il sert aux tests d'intégration de la phase 9, aux démonstrations manuelles (`agentic-app mock-server`), et permet d'injecter des pannes (latence, 429, 503, coupure, réponse hors protocole, `context_window_exceeded`).

## Conséquences

- `TransportGateway` (interface) : `init_conversation()`, `post_message()`, `get_messages(after)`, `close_conversation()`, `abandon()` (interruption : annule les appels en vol, §2.9). Implémentations : `HttpTransportGateway` (httpx) et `FakeTransportGateway` (réponses scriptées, aucun réseau).
- `message_id` est généré par l'application (`IdGenerator`, ADR-017) et persisté **avant** le POST, ce qui rend le retry après erreur réseau sûr.
- Le contrat est isolé dans une seule classe ; changer d'endpoint réel = changer la configuration, changer de forme de réponse = changer une méthode de mapping.
