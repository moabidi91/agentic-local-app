# ADR-020 — Transport enfichable : providers choisis par configuration

**Statut** : accepté (2026-09-18) — amende [ADR-004](ADR-004-contrat-de-transport.md)

## Contexte

ADR-004 fixe **un** contrat HTTP (init / post / get / close, corps et réponses JSON de forme imposée) et une seule implémentation, `HttpTransportGateway`. Le serveur mock le respecte, mais aucun modèle réel ne l'expose tel quel : chaque fournisseur a ses propres URL, son authentification (clé d'API en en-tête, jeton, paramètre de requête), la forme de ses corps et de ses réponses (`data.id`, `items[]`, `next_cursor`…). Brancher un tel modèle imposait de **modifier** `transport/gateway.py`, donc de retoucher le code testé du contrat pour chaque nouvelle API, à rebours du principe ouvert/fermé et de la règle du module map (« une ABC + une implémentation réelle + un double »).

Le besoin : plusieurs implémentations du même contrat abstrait `TransportGateway` doivent coexister, et le choix de l'implémentation doit se faire **par configuration seulement**, sans toucher au code existant — y compris pour une implémentation écrite en dehors du dépôt.

## Décision

### 1. Le contrat reste unique : `TransportGateway`

`transport/base.py` porte l'ABC (`init_conversation`, `post_message`, `get_messages`, `wait_for_reply`, `close_conversation`, `abandon`), ses objets valeur (`PostAck`, `GetResult`), l'`InFlightGuard` d'`abandon()` et les constantes `OP_*`. Le reste du code ne connaît que cette ABC ; `transport/gateway.py` reste une façade de compatibilité qui réexporte ces noms et `HttpTransportGateway`.

Un **provider** est une classe concrète de `TransportGateway` construite par convention `Provider(config.transport, clock, **kwargs)` (`kwargs` actuels : `transport` — un transport httpx pour les tests — et `sleep` — la fonction d'attente ; un provider ignore ceux qu'il ne connaît pas). Il peut déclarer un attribut de classe `options_model` (modèle pydantic, `None` ou absent = aucune option admise) qui valide `config.transport.options`.

### 2. Sélection par configuration : `TransportRegistry`

```toml
[transport]
provider = "generic_http"      # nom enregistré | entry point | "paquet.module:Classe"
close_method = "POST"          # generic_http : méthode de close_url (POST | DELETE)
[transport.options]            # sous-table propre au provider, validée par son options_model
```

`transport/registry.py` résout `transport.provider`, dans cet ordre : un **nom enregistré** (décorateur de classe `@TransportRegistry.register("nom")`, exécuté à l'import du paquet `transport` pour les providers intégrés), un **chemin d'import** `paquet.module:Classe` (`importlib`, aucun enregistrement nécessaire), un **entry point** du groupe `agentic_local_app.transports` (`importlib.metadata`, découverte paresseuse, chargé seulement quand il est nommé). Un nom enregistré prime toujours (aucun doublon dans `names()`). Erreurs de configuration : `TRANSPORT_PROVIDER_UNKNOWN` (avec la liste des noms disponibles), `TRANSPORT_PROVIDER_INVALID` (pas une classe concrète de `TransportGateway`, entry point cassé), `TRANSPORT_OPTIONS_INVALID` (détail pydantic `loc` / `msg` / `type`).

`TransportRegistry.create(config, clock=…, **kwargs)` valide les options puis instancie le provider ; `build_application` l'appelle quand aucun transport n'est injecté, **avant** d'ouvrir le store. Le `pyproject.toml` publie les trois providers intégrés dans le groupe d'entry points pour montrer le mécanisme.

Providers intégrés : `generic_http` (`GenericHttpProvider`, contrat ADR-004 — `HttpTransportGateway` est le **même** objet de classe, pas une sous-classe, pour que les tests `isinstance` existants restent vrais), `templated_http` (`TemplatedHttpProvider`), `fake` (`FakeTransportProvider`, le double scripté sans réseau, `reply_timeout_ms` lu dans la section).

### 3. Base « template method » : `HttpProviderBase`

`transport/http_base.py` porte tout ce qui ne dépend pas de la forme de l'API distante : client httpx injectable et timeouts, `InFlightGuard` / `abandon()`, encodage JSON canonique et gzip des corps, polling de `wait_for_reply` borné par `reply_timeout_ms` (`MODEL_GET_TIMEOUT`), contrôle des statuts attendus, table HTTP → `ErrorType` d'ADR-004 (`Retry-After`, `context_window_exceeded`), et les `details` de toute `TransportError` (`operation`, `http_status`, `url` sans secret). Un provider décrit chaque opération par un `HttpCall(method, url, headers, json, expected_statuses, parse_json=True)` et lit chaque réponse par ses points d'extension :

| Point d'extension | Rôle | Défaut |
|---|---|---|
| `headers(operation) -> dict` | en-têtes communs d'un appel | `Accept: application/json`, `X-User-Id`, `Authorization: Bearer` si jeton ; `Content-Type` / `Content-Encoding: gzip` ajoutés par la base dès qu'il y a un corps |
| `build_init(instructions, metadata) -> HttpCall` · `parse_init(status, body) -> str` | créer la conversation distante, lire son identifiant | abstrait |
| `build_post(remote_id, payload) -> HttpCall` · `parse_post(status, body, *, payload) -> PostAck` | déposer un message, lire l'accusé | abstrait |
| `build_get(remote_id, after) -> HttpCall` · `parse_get(status, body) -> GetResult` | lire les messages après le curseur | abstrait |
| `build_close(remote_id) -> HttpCall \| None` | fermer ; `None` = aucun appel distant | abstrait |
| `classify_error(operation, status, body, headers) -> TransportError` | statut hors `expected_statuses` | table ADR-004 |
| `redact_url(url) -> str` | l'`url` écrite dans `details` | jeton masqué |
| `options_model` | modèle pydantic de `transport.options` | `None` |

Un `parse_*` signale un corps hors contrat en levant `InvalidResponseError(code="INVALID_RESPONSE_BODY", **details)` ; la base le transforme en `TransportError(MODEL_PROTOCOL_ERROR, code)` et y **estampille** `operation`, `http_status` et `url`. L'erreur rendue par `classify_error` est estampillée de la même façon : un provider ne transporte jamais l'opération ni l'URL à travers ses hooks.

### 4. Provider piloté par la configuration : `templated_http`

`TemplatedHttpProvider` décrit n'importe quelle API HTTP par ses options (validées par `TemplatedOptions`, `extra = forbid`) :

```toml
[transport]
provider = "templated_http"
[transport.options]
headers = { "X-Api-Key" = "${env:MY_MODEL_KEY}" }          # commun à toutes les opérations
[transport.options.init]
method = "POST"
url = "https://api.example.com/v1/threads"
body = { instructions = "{instructions}", user = "{user_id}", meta = "{metadata_json}" }
conversation_id_path = "data.id"
expected_statuses = [200, 201]
[transport.options.post]
method = "POST"
url = "https://api.example.com/v1/threads/{conversation_id}/messages"
body = { role = "user", content = "{message_json}" }         # valeur ENTIÈRE => l'objet JSON du message
accepted_path = "ok"                                          # optionnel : absent => accepté si statut attendu
message_id_path = "id"                                        # optionnel : absent => message_id du message envoyé
[transport.options.get]
method = "GET"
url = "https://api.example.com/v1/threads/{conversation_id}/messages?since={after}"
messages_path = "items"
message_path = "payload"                                      # optionnel : le message protocolaire dans chaque élément
cursor_path = "next_cursor"                                   # optionnel : absent ou null => message_id du dernier message
[transport.options.close]                                     # optionnel : absent => fermeture locale seulement
method = "DELETE"
url = "https://api.example.com/v1/threads/{conversation_id}"
```

Règles :

- **Placeholders** (par opération, vérifiés à la validation des options) : `{user_id}` et `{token}` partout ; `{instructions}`, `{metadata_json}` à l'init ; `{conversation_id}`, `{message_json}`, `{message_id}`, `{message_type}` au post ; `{conversation_id}`, `{after}` au get ; `{conversation_id}` au close. Dans une URL les valeurs des placeholders sont percent-encodées (les valeurs `${env:…}` sont insérées telles quelles : une variable peut porter une URL de base). Substitution sur les feuilles `str` du corps (les autres types passent tels quels) ; une feuille valant exactement `"{message_json}"` ou `"{metadata_json}"` reçoit l'objet ; dans une chaîne plus longue ces deux placeholders deviennent du JSON canonique.
- **Environnement** : `${env:VAR}` partout (URL, en-têtes, feuilles de corps), résolu **au moment de l'appel**, jamais stocké ; variable absente → `ConfigError(TRANSPORT_ENV_MISSING)` avec `variable` et `operation` ; idem pour `{token}` quand la variable `token_env` est vide. Les valeurs résolues sont masquées dans les `details.url` des erreurs. Les en-têtes par défaut se limitent à `Accept` : rien d'identifiant ni d'authentifiant n'est envoyé sans que les options le disent (pas de fuite du jeton ADR-004 vers une API tierce).
- **Chemins de réponse** : notation pointée avec index de liste (`data.items[0].id`) ; chemin absent ou type inattendu → `TransportError(MODEL_PROTOCOL_ERROR, INVALID_RESPONSE_BODY)` avec `details.path` et `details.reason` (`path_not_found` / `unexpected_type`). `messages_path` doit désigner une liste d'objets, chaque élément étant un message protocolaire tel quel (ou le portant à `message_path`). `accepted_path` faux → `POST_NOT_ACCEPTED`.
- Tout le reste (timeouts, gzip, polling, table d'erreurs, `abandon()`) est hérité : une configuration `templated_http` qui reproduit ADR-004 dialogue avec le serveur mock exactement comme `generic_http` (test d'équivalence).

### 5. Observabilité et masquage

`AppConfig.masked()` masque, dans `transport.options`, toute valeur contenant `${env:` et toute valeur sous une clé ressemblant à un secret (`*key*`, `*token*`, `*secret*`, `*password*`, `*authorization*`), en plus du jeton. La CLI gagne `agentic-app transport list` (nom, classe, origine `builtin` / `entry point`) et `agentic-app transport show` (provider effectif, classe, origine, modèle d'options, options masquées ; `--json`) ; un provider inconnu ou des options invalides y sont des erreurs lisibles (code 1), comme pour `run` et `serve`.

### 6. Ajouter un provider en trois étapes

1. Écrire une classe concrète de `TransportGateway` — le plus souvent en dérivant `HttpProviderBase` et en implémentant `build_*` / `parse_*` (et, si besoin, `headers`, `classify_error`, `options_model`) ; ses tests utilisent `httpx.MockTransport`, un `FakeClock` et un `sleep` injecté, comme ceux du dépôt.
2. La rendre atteignable : par chemin d'import (`provider = "mon_paquet.transport:MonProvider"`, rien d'autre à faire), ou par un entry point `agentic_local_app.transports` dans la distribution qui la contient, ou — pour un provider intégré au dépôt — par `@TransportRegistry.register("nom")` et un import dans `transport/__init__.py`.
3. La sélectionner dans `config.toml` (`transport.provider`, `[transport.options]`) et vérifier avec `agentic-app transport show`.

## Conséquences

- Code : `transport/base.py`, `transport/http_base.py`, `transport/registry.py`, `transport/providers/{generic_http,templated_http}.py`, `transport/fake.py` (`FakeTransportProvider`), `transport/gateway.py` (façade), `config.py` (`provider`, `close_method`, `options`, masquage, surcharge d'environnement JSON pour les tables), `orchestration/wiring.py` (`TransportRegistry.create`), `interfaces/cli.py` (`transport list` / `show`), `pyproject.toml` (entry points), `config.toml`.
- Compatibilité : `HttpTransportGateway` et `FakeTransportGateway` restent importables aux mêmes chemins avec le même comportement ; la configuration existante reste valide (`provider = "generic_http"` par défaut, `options = {}`).
- Tests : `tests/unit/test_phase7_providers.py` (registre, base, generic, templated, équivalence contre le serveur mock, câblage) et la commande `transport` dans `tests/integration/test_phase9_cli.py` ; la suite de phase 7 existante couvre toujours `generic_http` sous son ancien nom.
- ADR-004 reste la description du contrat `generic_http` ; ses conséquences « une seule classe » sont amendées par le présent ADR.
- Points ouverts : un provider dont l'authentification demande un échange préalable (OAuth, signature de requête) devra surcharger `headers` ou `build_*` en Python — `templated_http` ne couvre que les en-têtes statiques et les variables d'environnement ; le curseur de `templated_http` suit la sémantique ADR-004 (`message_id` du dernier message) quand l'API n'en fournit pas.
