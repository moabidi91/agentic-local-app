# ADR-021 — Codec de messages par modèle : la forme brute des réponses convertie par configuration

**Statut** : accepté (2026-09-18) — complète [ADR-020](ADR-020-transport-enfichable.md) et [ADR-004](ADR-004-contrat-de-transport.md)

## Contexte

ADR-020 rend le transport enfichable : `templated_http` décrit n'importe quelle API HTTP (URL, en-têtes, corps, chemins de réponse). Mais le contrat de `GetResult.messages` reste celui d'ADR-004 : chaque élément est une **enveloppe protocolaire** (`type`, `conversation_id`, `message_id`, `content`) prête pour `ProtocolAdapter.parse_inbound`. Or aucun modèle réel ne rend ses réponses sous cette forme :

- un modèle de complétion rend du **texte**, où le JSON du message est entouré de prose ou de clôtures Markdown (```` ```json … ``` ````) ;
- une API de type *chat completions* rend un **objet** dont un chemin (`choices[0].message.content`) porte ce texte, et attend en entrée un texte dans `messages[].content`, pas un objet ;
- un modèle configuré avec des outils rend un **appel d'outil** dont les `arguments` (JSON en chaîne ou objet) sont le message.

Brancher un tel modèle imposait soit d'écrire un provider dédié qui mélange le dialecte HTTP et la forme du modèle, soit de retoucher l'orchestrateur. Le besoin : une **conversion optionnelle, choisie par configuration** exactement comme le provider, transparente pour l'orchestrateur, la rotation et la reprise, et dont les échecs restent exploitables par la politique de correction à venir (ADR-020, points ouverts) — il faut pouvoir citer la réponse brute au modèle.

## Décision

### 1. Un contrat : `MessageCodec`

`transport/codecs/base.py` porte l'ABC : `decode_inbound(raw_messages: list[Any]) -> list[dict]` (les éléments bruts rendus par le transport → les enveloppes protocolaires, un élément pouvant en porter plusieurs ou aucune) et `encode_outbound(payload: dict) -> Any` (l'enveloppe → ce que le transport doit poster ; défaut : identité). Un codec est **pur** (ni I/O ni horloge), construit par le registre comme `cls(options)` avec ses `codec_options` validées par son `options_model` (modèle pydantic de classe, `None` = aucune option), et se nomme dans ses erreurs (`name`).

### 2. Les échecs : `CodecError` = `MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY`

Une forme brute illisible lève une `CodecError`, sous-classe de `TransportError` (origine `TransportGateway` : le décorateur est un transport), `error_type = MODEL_PROTOCOL_ERROR`, `error_code = UNPARSEABLE_REPLY`, **non rejouable** (§7.2). Ses `details` portent toujours `codec`, l'`index` de l'élément brut, un `excerpt` (≤ 500 caractères de la forme brute : la chaîne telle quelle, sinon son JSON canonique) et une `reason`, plus `operation` / `http_status` estampillés par le décorateur. L'orchestrateur la traite comme tout échec de transport : `FailureRecord` + `failure.recorded`, décision `fail` (aucun retry, le disjoncteur n'est pas nourri), session `FAILED` — ou **une rotation** si la fenêtre est `WARNING` (ADR-019 §2, la réponse inutilisable). Rien n'est persisté comme message entrant (aucun `message.rejected`) : à la différence d'une `ProtocolError` de l'adaptateur, il n'y a pas d'enveloppe à enregistrer ; c'est l'`excerpt` du `FailureRecord` qui garde la trace de ce que le modèle a rendu.

### 3. Sélection par configuration : `CodecRegistry`

```toml
[transport]
codec = "passthrough"          # nom enregistré | entry point agentic_local_app.codecs | "paquet.module:Classe"
[transport.codec_options]      # sous-table propre au codec, validée par son options_model
```

`transport/codecs/registry.py` porte `CodecRegistry`, bâti sur la même base générique que `TransportRegistry` (`PluginRegistry`, `transport/registry.py`) : nom enregistré (`@CodecRegistry.register("nom")`) > chemin d'import > entry point du groupe `agentic_local_app.codecs`, chargé paresseusement ; classe concrète de `MessageCodec` exigée ; erreurs `CODEC_UNKNOWN` (avec les noms disponibles), `CODEC_INVALID`, `CODEC_OPTIONS_INVALID` (détail pydantic `loc` / `msg` / `type`, clé `codec` dans les détails). `CodecRegistry.create(config)` valide puis instancie. `pyproject.toml` publie les codecs intégrés dans le groupe.

### 4. Application transparente : `CodecTransport`

`transport/codecs/decorator.py` : un décorateur, lui-même `TransportGateway`, qui enveloppe n'importe quel provider et n'agit que sur les messages :

| Opération | Ce que fait le décorateur |
|---|---|
| `post_message(remote, payload)` | poste `codec.encode_outbound(payload)` ; si l'accusé du provider n'a pas de `message_id` (il a posté du texte), l'accusé reprend le `message_id` de l'enveloppe protocolaire (ADR-004 : l'accusé nomme le message envoyé) |
| `get_messages` / `wait_for_reply` | `GetResult(messages = codec.decode_inbound(result.messages), cursor, http_status)` ; le curseur du provider est gardé sauf s'il n'a pas avancé (`None` ou toujours égal à `after` : un provider lisant des éléments bruts ne peut pas dériver le curseur ADR-004), auquel cas il devient le `message_id` de la dernière enveloppe décodée |
| erreur du codec | `CodecError` estampillée `operation` / `http_status`, propagée |
| `init_conversation`, `close_conversation`, `abandon`, `aclose` | délégués tels quels |

`build_application` résout le codec **avant** le transport et le store (une mauvaise configuration n'ouvre ni client ni base), puis `apply_codec(transport, codec)` : le codec `passthrough` laisse le transport **nu** (les `isinstance(app.transport, HttpTransportGateway)` existants restent vrais), tout autre codec l'enveloppe — qu'il ait été créé par le registre ou injecté (`transport=`) ; un `codec=` injecté prime sur la configuration. L'orchestrateur, `RotationCoordinator`, `InterruptionHandler` et la reprise ne sont pas modifiés : ils voient un `TransportGateway`.

### 5. Codecs intégrés

| Codec | Options (`codec_options`) | Entrée (élément brut → enveloppes) | Sortie (`encode_outbound`) |
|---|---|---|---|
| `passthrough` (défaut) | aucune | identité : les éléments sont déjà des enveloppes | identité |
| `json_text` | `content_path` (chemin pointé avec index, absent = l'élément est une chaîne) · `strip_code_fences` (défaut `true` : contenu de la première clôture ```` ``` ````) · `extract_first_json_object` (défaut `true` : premier objet ou tableau JSON équilibré, chaînes et échappements respectés, un candidat qui n'est pas du JSON valide est sauté ; `false` = tout le texte doit être du JSON) · `id_path` (chemin dans l'élément brut d'où synthétiser `message_id` s'il manque) · `conversation_id_fallback` (défaut `true`) · `outbound` (`object` \| `text`) | texte → JSON → une enveloppe (objet) ou un tableau d'enveloppes | `object` : l'enveloppe ; `text` : `canonical_json(enveloppe)` |
| `tool_call` | `arguments_path` (défaut `arguments`) · `name_path` + `tool_name` (facultatifs : tout autre outil est refusé, `unexpected_tool`) · `id_path` · `conversation_id_fallback` · `outbound` | les `arguments` de l'appel (JSON en chaîne, strict, ou objet) → enveloppe(s) | idem |

Raisons d'`UNPARSEABLE_REPLY` : `path_not_found` (+ `path`), `unexpected_type` (+ `path`, `expected`), `no_json_found`, `json_unbalanced`, `json_invalid` (+ `error`), `missing_conversation_id`, `unexpected_tool` (+ `expected`, `received`), et `no_envelope` levée par le décorateur quand la réponse attendue par `wait_for_reply` ne porte aucune enveloppe (un tableau vide, par exemple).

Règles communes : un codec **n'invente jamais** de `conversation_id` — avec `conversation_id_fallback` une enveloppe qui n'en a pas est transmise telle quelle à l'adaptateur, qui la rejette (`SCHEMA_INVALID`, `message.rejected`, réponse persistée et comptée) ; sans l'option, le codec la refuse d'emblée (`missing_conversation_id`). Un `message_id` manquant n'est synthétisé que si `id_path` le désigne (chaîne non vide ou entier, rendu en texte) ; un tableau de plusieurs enveloppes sans `message_id` reçoit le même (c'est de toute façon un tour à plusieurs messages, refusé par ADR-007).

### 6. Exemple : une API de type chat-completions

```toml
[transport]
provider = "templated_http"
codec = "json_text"
[transport.codec_options]
content_path = "choices[0].message.content"   # chaque élément de messages_path est l'objet complet
outbound = "text"                              # le message part en JSON canonique dans content
[transport.options]
headers = { "Authorization" = "Bearer ${env:MY_MODEL_KEY}" }
[transport.options.init]
url = "https://api.example.com/v1/threads"
body = { instructions = "{instructions}", user = "{user_id}" }
conversation_id_path = "id"
[transport.options.post]
url = "https://api.example.com/v1/threads/{conversation_id}/chat/completions"
body = { messages = [{ role = "user", content = "{message_json}" }] }   # content reçoit le texte
[transport.options.get]
url = "https://api.example.com/v1/threads/{conversation_id}/completions?since={after}"
messages_path = "data"
cursor_path = "next_cursor"
```

Le texte posté est `canonical_json(enveloppe)` (la feuille `content` valant exactement `"{message_json}"` reçoit la valeur elle-même, ici la chaîne) ; la réponse `Here it is:\n```json\n{…}\n```` est lue à `content_path`, déclôturée, extraite et validée par l'adaptateur comme n'importe quelle enveloppe. Avec `outbound = "text"`, `{message_id}` et `{message_type}` sont vides dans les gabarits de `templated_http` et l'accusé prend le `message_id` de l'enveloppe (ou celui de `message_id_path`).

### 7. Observabilité

`AppConfig.masked()` masque `codec_options` comme `options` (valeurs `${env:…}`, clés `*key*` / `*token*` / `*secret*` / `*password*` / `*authorization*`). CLI : `agentic-app codec list` (nom, classe, origine), `agentic-app codec show` (codec effectif, classe, origine, modèle d'options, options masquées ; `--json`) ; `agentic-app transport show` affiche aussi le codec effectif (`codec`, `codec_class`, `codec_origin`, `codec_options_model`, `codec_options`). Un codec inconnu ou des options invalides y sont des erreurs lisibles (code 1), comme au démarrage de `run` et `serve`.

### 8. Ajouter un codec en trois étapes

1. Écrire une classe concrète de `MessageCodec` : `decode_inbound` (lever `self.error(index, raw, reason, **details)` pour tout élément illisible), `encode_outbound` si la forme de sortie n'est pas l'enveloppe, `options_model` si elle a des options, `name`. Elle est pure : ses tests n'ont besoin ni de réseau ni d'horloge ; `envelopes_of` / `complete_envelope` (`codecs/json_text.py`) factorisent « document JSON → enveloppes ».
2. La rendre atteignable : par chemin d'import (`codec = "mon_paquet.codecs:MonCodec"`, rien d'autre à faire), par un entry point `agentic_local_app.codecs`, ou — pour un codec intégré — par `@CodecRegistry.register("nom")` et un import dans `transport/codecs/__init__.py`.
3. La sélectionner dans `config.toml` (`transport.codec`, `[transport.codec_options]`) et vérifier avec `agentic-app codec show`.

## Conséquences

- Code : `transport/codecs/{base,registry,decorator,passthrough,json_text,tool_call}.py` (nouveau paquet), `transport/registry.py` (base générique `PluginRegistry`, `TransportRegistry` inchangé en surface), `transport/base.py` (`validate_options` paramétré par code et clé d'erreur), `transport/fake.py` (éléments bruts en file, payload texte accepté), `transport/providers/templated_http.py` (payload texte accepté au POST), `config.py` (`codec`, `codec_options`, masquage), `orchestration/wiring.py` (`CodecRegistry.create` + `apply_codec`, paramètre `codec=`), `interfaces/cli.py` (`codec list` / `show`, codec dans `transport show`), `pyproject.toml` (entry points), `config.toml`.
- Compatibilité : `codec = "passthrough"` par défaut, `codec_options = {}` — aucune configuration existante ne change de comportement, `Application.transport` reste le provider nu.
- Tests : `tests/unit/test_phase7_codecs.py` (contrat, codecs, extraction JSON, décorateur, registre, câblage, CLI, exemple chat-completions contre `httpx.MockTransport`) et `tests/integration/test_phase9_orchestration_codec.py` (boucle complète en texte clôturé, chat completions avec `id_path`, réponse sans JSON → `UNPARSEABLE_REPLY` épinglé, rotation sur réponse inutilisable en `WARNING` à travers le codec).
- Points ouverts :
  1. **Octets de contexte.** La fenêtre (ADR-013) compte les enveloppes **décodées**, pas la forme brute (prose, clôtures) que le modèle a réellement produite : approximation par défaut, acceptée ; un codec pourrait exposer la taille brute si l'écart devenait significatif.
  2. **Éléments non-objets dans `templated_http`.** `messages_path` exige des objets ; une API rendant une liste de chaînes nues demanderait une option de relâchement du provider (le codec, lui, accepte déjà les chaînes).
  3. **Politique de correction.** `UNPARSEABLE_REPLY` porte `excerpt` et `reason` pour qu'une politique de correction (re-solliciter le modèle en citant sa réponse) puisse naître sans changer les codecs ; elle reste à écrire (ADR-020, points ouverts).
  4. **Sortie `text` et gabarits.** `{message_id}` / `{message_type}` sont vides quand le codec poste du texte : un placeholder dédié (`{message_text}`) dans `templated_http` lèverait la limite.
