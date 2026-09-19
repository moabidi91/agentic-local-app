# ADR-027 — Champs d'identifiants déclarés par le profil de modèle, skills et niveau d'effort

**Statut** : accepté (2026-09-19) — complète [ADR-024](ADR-024-profils-de-modele.md) (`requires_credentials`, le catalogue servi aux interfaces) et [ADR-025](ADR-025-pause-sur-erreur-d-authentification.md) (`POST /credentials`, la règle du silence) ; répond aux besoins §2, §3 et §4 du contrat d'interface du front (`agentic-front/contrat-interface.md`, écarts **G-3** et **G-4** de [`docs/contracts/front-backend-v1.md`](../contracts/front-backend-v1.md)) ; sans effet sur le protocole et sur le schéma persisté

## Contexte

ADR-024 a donné au front de quoi afficher un catalogue de modèles, et ADR-025 de quoi réparer un jeton expiré sans perdre la session. Les deux reposent sur un postulat : **un modèle a besoin d'exactement une chose, un jeton**, nommée par `token_env`. `requires_credentials` est un booléen, `POST /credentials` prend `{"token": …}`, et l'écran de connexion du front dessine un champ « Access token ».

Ce postulat est faux dès le deuxième modèle réel. Le provider `templated_http` d'[ADR-020](ADR-020-transport-enfichable.md) décrit n'importe quelle API HTTP : ses en-têtes, ses URL et ses corps acceptent `${env:VAR}` n'importe où. Rien n'oblige cette API à se contenter d'un porteur — il lui faut souvent, en plus, un identifiant de conversation, un identifiant d'organisation, un espace de travail. Et le provider écrit hors du dépôt fait pire : [`examples/config.acme.toml`](../../examples/config.acme.toml) lit sa clé dans la variable nommée par `options.api_key_env`, **pas** dans `token_env`. Un utilisateur qui remplirait le champ « Access token » de ce profil renseignerait une variable que personne ne lit ; le formulaire dirait « c'est bon », et l'appel suivant échouerait quand même en 401.

Le front ne peut pas deviner ces champs. Il ne voit ni `config.toml`, ni les options du provider, ni la classe qui les lit — c'est la règle d'ADR-024 §4 et le §2 du contrat : **aucune route ne rend d'URL, d'option ni de secret**. Deviner voudrait dire, soit exposer les options du provider à l'écran de connexion (ce que l'on refuse), soit coder en dur dans le front une table « provider → champs » qui serait fausse le jour où quelqu'un branche son propre provider.

Deux besoins arrivent dans le même écran de connexion et n'ont, eux, **aucune** existence côté application :

- des **skills** (§3 du contrat d'interface) : des notes Markdown que l'utilisateur a écrites sur son poste et veut rattacher à une session. Le front demande une liste pour proposer un choix guidé plutôt qu'une saisie libre ; `listKnownSkills()` rend aujourd'hui une liste vide et le dit (`supportsKnownSkills = false`) ;
- un **niveau d'effort** (§4) : `low` / `medium` / `high`, une intention, jamais un nombre de threads.

Ces deux-là posent une question que cet ADR **ne tranche pas** : que doit en faire le modèle ? Injecter le contenu d'un fichier dans le contexte engage [ADR-010](ADR-010-limites-de-payload.md) (quelle taille ?) et [ADR-005](ADR-005-resume-de-contexte-par-le-modele.md) (quelle place dans le budget ?) ; traduire un niveau d'effort en consigne textuelle modifie ce qui part au modèle, donc le protocole de §12. Rien de tout cela ne se décide en passant.

## Décision

### 1. Les champs d'identifiants sont **déclarés par le profil**, pas devinés par le front

Un profil de modèle gagne une liste facultative `credential_fields`. Chaque entrée décrit **une entrée de formulaire** :

| Clé | Rôle |
|---|---|
| `key` | l'identifiant que l'interface renvoie dans `POST /credentials` (`access_token`, `chat_id`…) ; non vide, unique dans le profil, de la forme d'un identifiant |
| `label` | le libellé affiché au-dessus du champ |
| `placeholder` | facultatif |
| `secret` | facultatif, **vrai par défaut** |
| `env` | la variable d'environnement où la valeur est écrite ; non vide |

```toml
[[transport.credential_fields]]
key = "access_token"
label = "Jeton d'accès"
env = "CLAUDE_API_KEY"

[[transport.credential_fields]]
key = "chat_id"
label = "Identifiant de conversation"
secret = false
env = "CLAUDE_CHAT_ID"
```

**Pourquoi le profil et non le front.** Le profil est le seul endroit qui sait à la fois *ce que le provider lit* et *dans quelle variable*. Le front, lui, sait dessiner un formulaire. Déclarer les champs là où la connaissance est déjà, et ne faire traverser que ce qu'un formulaire demande, c'est la même décision qu'ADR-024 §4 pour `requires_credentials` : l'application répond à une question d'affichage, elle n'expose pas sa configuration. Le jour où quelqu'un branche un provider maison, il ajoute quatre lignes à son `config.toml` et le front dessine les bons champs sans qu'une ligne de TypeScript change — ce qu'aucune table « provider → champs » codée en dur dans le front ne pourrait offrir.

**Pourquoi `secret` vaut vrai par défaut.** `secret = false` autorise l'interface à **retenir la valeur entre deux lancements**, donc à l'écrire dans un fichier de préférences. Une déclaration incomplète, une faute de frappe, un champ ajouté à la hâte : tous ces accidents doivent aboutir au comportement prudent, celui qui ne persiste rien. Le défaut est la valeur qu'on obtient quand on n'a pas réfléchi, et un secret écrit sur le disque ne se rattrape pas. On ferme donc par défaut, et exposer une valeur demande un geste explicite (`secret = false`), écrit à côté de la valeur concernée.

**Le repli implicite, des deux côtés.** Un profil qui ne déclare **aucune** liste se comporte comme s'il en déclarait exactement une, à un champ, construite sur `token_env` :

```
{key = "access_token", label = "Access token", placeholder = "Paste an access token",
 secret = true, env = <token_env>}
```

…et aucune, quand `token_env` est vide lui aussi. C'est **mot pour mot** ce que le front fait déjà d'un `requires_credentials` nu (contrat d'interface §2 : « `true` sans `credential_fields` est traité comme un unique champ implicite Access token, secret »). Écrire la même règle des deux côtés, plutôt que de la laisser à un seul, a une conséquence précise : un front ancien parlant à une application récente, et une application ancienne parlant à un front récent, **dessinent le même formulaire**. Une configuration écrite avant cet ADR n'a rien à changer, et la route continue d'accepter `{"token": …}`.

`requires_credentials` garde son sens et reste cohérent : **vrai dès qu'au moins un champ déclaré n'a pas de valeur dans l'environnement**, à cet instant. Pour un profil sans déclaration, c'est exactement la règle d'ADR-024 §4 (« `token_env` non vide et variable vide ou absente ») ; pour un profil qui déclare trois champs dont un seul est renseigné, l'écran de connexion reste ouvert, ce qui est le comportement voulu. La valeur reste une **vue**, calculée à la lecture, jamais un champ du modèle de configuration.

### 2. `GET /models` : les champs voyagent, les variables non

Chaque entrée du catalogue gagne `credential_fields`, **toujours présent**, tableau éventuellement vide, dans l'ordre de déclaration :

```json
{"key": "chat_id", "label": "Identifiant de conversation", "placeholder": null, "secret": false}
```

**Toujours présent**, parce qu'un tableau vide est une réponse (« ce modèle ne demande rien ») alors qu'une clé absente est une question (« est-ce que cette version sait répondre ? »). L'ordre est celui du fichier : c'est l'ordre dans lequel l'auteur du profil veut voir les champs, et c'est le seul ordre qui ait un sens pour un formulaire.

**`env` n'en fait pas partie**, et aucune valeur non plus. Le nom d'une variable d'environnement n'apprend rien à un formulaire : le front renvoie des `key`, pas des variables. La règle d'ADR-024 §4 (« aucun secret : ni jeton, ni URL, ni option ») s'étend donc d'un cran — le catalogue rend ce qu'il faut dessiner et rien de plus —, et un test l'épingle dans les deux sens : ni `env`, ni valeur. `GET /config` reste à part : c'est la configuration effective, elle montre déjà `token_env` et montre aussi `env`, comme elle montre les URL ; ce n'est pas la surface de connexion.

Le reste de l'entrée ne bouge pas (`name`, `display_name`, `description`, `provider`, `codec`, `requires_credentials`, `active`), l'actif reste premier.

### 3. `POST /credentials` : un objet plat, une entrée par champ

```json
{"credentials": {"access_token": "…", "chat_id": "…"}}
```

`204` sans corps ; chaque valeur est écrite, détourée, dans la variable `env` de son champ, par `credentials.set_token` et par rien d'autre. `{"token": "…"}` reste accepté comme **alias** de `{"credentials": {"access_token": "…"}}` : la CLI le poste, le client historique aussi, et c'est exactement le champ implicite du §1.

Les refus :

| Code | Statut | Quand | Détails |
|---|---|---|---|
| `CREDENTIALS_EMPTY` | 400 | aucune entrée, ou une valeur vide / blanche | `key` quand c'est une entrée précise |
| `CREDENTIAL_FIELD_UNKNOWN` | 400 | une clé que le profil actif ne déclare pas | `key`, `expected` (les clés déclarées) |
| `CREDENTIALS_NOT_CONFIGURED` | 409 | le profil actif ne déclare aucun champ | `field`, `provider` |

**Rien n'est écrit tant que tout n'est pas accepté** : la route valide chaque entrée contre la déclaration avant d'en écrire une seule, pour qu'un formulaire refusé laisse l'environnement exactement dans l'état où il était. Une clé inconnue est **refusée, pas ignorée** : l'ignorer laisserait le front croire qu'il a signé, et l'erreur suivante serait un 401 incompréhensible trois écrans plus loin.

**Pourquoi une route à part, et pas un champ de la création de session.** Le front pourrait poster ses identifiants avec le `POST /sessions` qui suit — c'est un écran, c'est un bouton. Trois raisons de ne pas le faire.

1. **Le corps d'une création de session est journalisé, celui-ci ne l'est jamais.** `goal`, `user_message` et le budget partent dans un `SessionRecord` persisté et dans la charge de `session.created` ; un secret dans ce corps serait un secret dans la base et dans la chaîne d'audit. `POST /credentials` est la seule route de l'API dont le corps est lu **à la main**, précisément pour qu'aucune erreur de validation de pydantic ne puisse renvoyer ce qui a été posté (ADR-025 §6) — une propriété qu'on ne sait pas offrir à une route qui, elle, doit rendre des erreurs de champ détaillées.
2. **Les deux gestes n'ont pas la même durée de vie.** Les identifiants valent pour le processus ; une session est un épisode. Après un 401 (ADR-025), l'utilisateur fournit un jeton **sans** créer de session : c'est tout l'intérêt de la pause. Mêler les deux forcerait à inventer une création de session qui n'en est pas une.
3. **La séquence du front reste vérifiable.** `POST /credentials` → `GET /models` (le profil cesse de réclamer) → `POST /sessions`. Chaque étape a une réponse observable, et l'écran de connexion sait dire *laquelle* a échoué.

Seul le profil **actif** est accepté, comme aujourd'hui (ADR-025, point ouvert 4) : un processus sert un modèle (ADR-024 §2).

### 4. `GET /skills` et les deux extras de la création de session

**La lecture.** Une section `[skills]`, deux clés : `root` (le dossier parcouru, vide par défaut) et `enabled` (vrai par défaut). `GET /skills` rend `{"skills": [{"name", "path"}]}` — les fichiers `*.md` trouvés sous `root`, en descendant au plus **3 niveaux**, triés par nom, bornés à **200 entrées** ; `name` est le nom du fichier sans extension, `path` son chemin.

**La route ne rend jamais d'erreur.** Racine vide, absente, illisible, pointant sur un fichier : `200` et une liste vide. C'est l'écran de connexion qui appelle cette route ; le bloquer parce que l'utilisateur a renommé un dossier serait échanger un champ de saisie libre parfaitement utilisable contre un écran en panne. Le front dégrade déjà exactement ainsi (`listKnownSkills()` rend `[]`), et la borne suit la règle d'[ADR-026](ADR-026-espace-de-travail-et-fichiers-temporaires.md) §4 : elle porte sur ce qui est **rapporté**, pas sur ce qui est parcouru, pour que la tête de liste soit déterministe ([ADR-017](ADR-017-determinisme-des-resultats-et-identifiants.md)) et non dépendante de l'ordre du système de fichiers.

**L'écriture.** `POST /sessions` accepte deux champs facultatifs de plus : `skills` (une liste de chaînes) et `effort` (`low`, `medium` ou `high` ; `400 EFFORT_INVALID` sinon, avec la liste attendue dans les détails).

**Ils sont tracés, pas appliqués.** Ils voyagent dans la charge de l'événement `session.created` — `{"goal", "budget", "skills", "effort"}` — et s'arrêtent là. Aucun fichier n'est ouvert, aucune consigne n'est dérivée, **le modèle reçoit exactement ce qu'il recevait avant**. Les deux clés sont toujours présentes dans la charge (`[]` et `null` quand rien n'a été choisi) : une clé absente obligerait un lecteur de la trace à distinguer « pas de skill » de « version qui ne les connaissait pas ».

Aucun enregistrement ne les porte : ni colonne, ni table, ni migration, le schéma SQLite reste en version 1. C'est délibéré. **Persister un champ, c'est promettre qu'il sert.** Les tracer dans l'événement suffit à répondre à la seule question qu'on sait traiter aujourd'hui — « qu'est-ce que l'utilisateur avait demandé ? » — et la charge d'un événement est le seul endroit de l'application où l'on ait le droit d'écrire un fait sans avoir décidé de son comportement. Le jour où l'on tranchera ce que le modèle doit en faire, la forme persistée sera décidée avec ce comportement, pas avant lui.

**Ce qui reste à décider**, et pourquoi ce n'est pas ici : donner une skill au modèle, c'est choisir entre lui passer une **référence de fichier** (accessible depuis le `working_space` d'ADR-026, donc lisible par une commande) et lui **injecter le contenu** dans le contexte (donc engager ADR-010 sur la taille d'un message et ADR-005 sur le budget). Traduire un niveau d'effort, c'est ajouter une consigne au message envoyé, donc toucher §12. Deux décisions qui changent ce que le modèle voit : elles valent un ADR, pas un paramètre glissé dans celui-ci.

## Conséquences

- **Code** : `config.py` (`CredentialField`, `CredentialFieldView`, `TransportSection.credential_fields`, `declared_credential_fields`, `credential_field_views`, `requires_credentials` réécrit sur les champs, `ModelProfileView.credential_fields`, `SkillsSection` ajoutée à `AppConfig`, les trois constantes du champ implicite) ; nouveau `skills.py` (`Skill`, `list_skills`, `MAX_SKILLS`, `MAX_SKILL_DEPTH`) ; `credentials.py` (`set_token` prend la variable à écrire en `variable=`, la règle du silence inchangée) ; `interfaces/http_api.py` (`GET /skills`, `POST /credentials` réécrit sur `_posted_credentials`, `CREDENTIAL_FIELD_UNKNOWN`, `EFFORT_LEVELS` / `EFFORT_INVALID`, `skills` et `effort` sur `CreateSessionRequest` et sur `ConversationManagerLike`) ; `orchestration/conversation_manager.py` et `lifecycle/conversation_lifecycle.py` (les deux extras passés jusqu'à la charge de `session.created`, et rien de plus).
- **Configuration** : `config.toml` documente `credential_fields` sous `[transport]` et sous `[models.<nom>]`, et gagne `[skills]` (identique aux défauts du code, comme le vérifie le test du dépôt). `examples/config.acme.toml` déclare son champ, ce qui corrige un piège réel : la clé ACME est lue dans `options.api_key_env`, jamais dans `token_env`.
- **Compatibilité** : totale dans les deux sens. Une configuration sans `credential_fields` garde le champ implicite `access_token` ; `POST /credentials` accepte toujours `{"token": …}` ; `requires_credentials` répond comme avant pour tout profil qui ne déclare rien ; `agentic-app credentials` n'est pas modifiée.
- **Persistance** : aucun changement. Aucune colonne, aucune table, aucune migration ; schéma en version 1. Seule la charge de `session.created` gagne deux clés.
- **Protocole** : aucun changement. Ni les skills, ni le niveau d'effort n'atteignent le modèle.
- **Sécurité** : `env` ne traverse aucune des deux routes de connexion — ni `GET /models`, ni le détail d'une erreur de `POST /credentials`, qui nomme la `key` et le provider (`GET /config`, qui rend la configuration effective, le montre comme il montre déjà `token_env`). Aucune valeur postée n'apparaît dans une réponse, une trace, un journal ou un événement ; le test qui épinglait cette règle pour un jeton unique la couvre maintenant pour un formulaire à plusieurs champs, erreur de clé inconnue comprise.
- **Tests** : `tests/unit/test_phase11_models.py` (déclaration et validations — clé vide, clé qui n'est pas un identifiant, clés en double, `env` vide —, repli implicite dans ses trois cas, `secret` fermé par défaut, `requires_credentials` sur plusieurs champs, `[skills]` et le parcours réel d'un dossier temporaire : profondeur, tri, borne, racine absente / illisible / désactivée) ; `tests/unit/test_phase11_credentials.py` (`set_token` avec `variable=`, la règle du silence sur plusieurs variables) ; `tests/integration/test_phase9_api.py` (la charge de `/models` et l'absence d'`env`, les quatre chemins d'erreur de `/credentials`, l'alias, l'écriture multi-champs, `/skills` avec et sans racine, les deux extras dans l'événement `session.created`, `EFFORT_INVALID`).
- **Points ouverts** :
  1. **Les skills et le niveau d'effort n'ont aucun effet sur le modèle.** Ils sont acceptés, validés pour l'effort, journalisés dans `session.created`, et c'est tout. Décider de leur effet demande de trancher référence de fichier contre contenu injecté (ADR-010, ADR-005) et l'ajout d'une consigne au message (§12) : c'est l'ADR suivant.
  2. **Rien ne vérifie qu'une valeur d'identifiant est la bonne**, ni même qu'elle a la forme attendue : `requires_credentials` dit seulement qu'une variable est renseignée (ADR-024, point ouvert 2). Un `chat_id` mal collé se découvre à l'appel suivant, comme un jeton expiré.
  3. **`GET /skills` ne lit pas les fichiers** : ni titre, ni description, ni taille. Le jour où l'écran de connexion voudra montrer autre chose qu'un nom, il faudra décider ce qu'on lit d'un fichier que l'utilisateur possède, et à quel coût.
  4. **Les identifiants d'un profil inactif ne sont toujours pas acceptés** (ADR-025, point ouvert 4). Préparer un autre modèle demande un redémarrage de toute façon.
