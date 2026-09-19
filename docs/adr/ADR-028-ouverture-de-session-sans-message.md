# ADR-028 — Ouverture de session sans message, et l'utilisateur qui voyage avec la session

**Statut** : accepté (2026-09-19) — précise [ADR-006](ADR-006-interruption-nouvelle-conversation.md) (la session `READY` et la conversation enfant), [ADR-016](ADR-016-politique-de-reprise.md) (ce que la reprise au démarrage touche, et ce qu'elle ne touche pas), [ADR-018](ADR-018-api-pour-un-front-et-flux-live.md) (`POST /sessions`) et [ADR-024](ADR-024-profils-de-modele.md) §2 (l'identité machine) ; ferme l'écart **G-4** de [`docs/contracts/front-backend-v1.md`](../contracts/front-backend-v1.md) (le message d'ouverture fabriqué et l'`userId` qui n'allait nulle part) ; sans effet sur le protocole (§12) et sur le schéma persisté (version 1, aucune migration)

## Contexte

`POST /sessions` exige depuis toujours un `goal` et un `user_message`, tous deux non vides. Cette exigence venait de la CLI, où les deux sont là : `agentic-app run "<goal>" --message "<message>"` connaît le but et la première phrase avant même d'ouvrir la base.

Le front de bureau ne travaille pas dans cet ordre. Son écran de connexion demande un utilisateur, un modèle, des identifiants, un dossier de travail, des skills, un niveau d'effort — **jamais une phrase** : l'utilisateur se connecte, *puis* il écrit. Son client HTTP, tenu de fournir les deux champs, les a donc inventés (`"Desktop console session"` / `"Session opened from the desktop console."`, `openingGoal` / `openingMessage` de `HttpApiClient`). Le test d'intégration de bout en bout a montré ce que cela coûte :

- **le modèle reçoit un `user_request` que personne n'a tapé.** Il répond, donc il planifie, donc il exécute peut-être des commandes — sur un but fabriqué. C'est le pire des trois défauts : l'application ment au modèle en notre nom ;
- **le budget de session est entamé avant le premier mot.** Ce `user_request` consomme un cycle et, dès que le modèle répond par un plan, un plan ([ADR-012](ADR-012-budget-de-session.md)). Un utilisateur qui se connecte et part déjeuner a déjà dépensé ;
- **la trace d'audit est fausse.** La chaîne d'événements et la table des messages contiennent un tour utilisateur qui n'a jamais eu lieu, et rien ne le distingue d'un vrai. Une trace qui contient un fait inventé ne sert plus à rien : on ne peut plus l'opposer à qui que ce soit.

Le correctif « (a) ajouter le premier message à l'écran de connexion » que proposait le contrat déplace le problème sans le résoudre : il faudrait demander à l'utilisateur d'écrire son message *avant* de savoir si ses identifiants sont bons, et l'écran de connexion cesserait d'être un écran de connexion.

Reste le correctif (b), que le contrat écartait faute d'un état : « l'application accepte une session sans message initial, ce qui demande un état *créée, pas démarrée* qui n'existe pas aujourd'hui ». **Cet état existe.** ADR-006 §3 l'a introduit pour l'interruption : une session `READY` est une session vivante qui n'a pas de boucle, dont la conversation courante est terminée, et dont le prochain message ouvre une **nouvelle conversation enfant**. `ConversationManager.continue_session` en tient déjà la branche complète. Une session créée sans message est exactement cela, à une nuance près : elle n'a pas encore de conversation du tout, donc le parent de sa première conversation est `None`. Le code écrivait déjà `parent_conversation_id=session.current_conversation_id` — la nuance était déjà couverte.

Deux autres défauts sont sortis du même test, et ils tiennent dans la même décision parce qu'ils portent sur la même requête :

- **`GET /whoami` et les sessions ne parlent pas du même utilisateur.** La route répond l'identité machine d'ADR-024 §2 (`root`, `alice`…) ; chaque session porte `transport.user_id`, une constante de configuration (`local-user`). Les deux s'affichent côte à côte dans le front, et se contredisent. Par-dessus, `POST /sessions` n'accepte aucun `user_id` : le champ « utilisateur » de l'écran de connexion est décoratif ;
- **le serveur de développement du front est en 1420** (`vite.config.ts`, `strictPort`), pas en 5173. Les `cors_origins` par défaut ne le listaient pas : le vrai front, en développement, est refusé par le navigateur avant même d'atteindre une route.

## Décision

### 1. Le message d'ouverture devient facultatif, et c'est une **paire**

`POST /sessions` accepte trois formes, et trois seulement :

| Corps | Réponse | Ce qui se passe |
|---|---|---|
| `goal` **et** `user_message` | `201`, `status: "RUNNING"` | inchangé : session `READY → RUNNING`, première conversation `NEW → ACTIVE`, boucle lancée, `user_request` posté |
| **ni l'un ni l'autre** | `201`, `status: "READY"` | la session existe, et rien d'autre : aucune conversation, aucun cycle, rien de posté au modèle, aucun `user_request` persisté |
| un seul des deux | `400 GOAL_REQUIRED` / `400 USER_MESSAGE_REQUIRED` | refusé **avant** toute création : aucun enregistrement, aucun événement |

Un champ présent mais vide (`""`) reste un `422 VALIDATION_ERROR`, comme avant : omettre un champ et le vider sont deux affirmations différentes, et seule la première veut dire « je n'ai rien à dire pour l'instant ».

**Pourquoi une paire et non deux champs indépendants.** Un `goal` sans premier message décrirait une intention que personne n'aurait formulée ; un premier message sans `goal` obligerait l'application à en fabriquer un — c'est-à-dire à refaire, un étage plus bas, exactement ce qu'on reproche au front. Les deux moitiés n'ont de sens qu'ensemble, donc elles arrivent ensemble ou pas du tout, et le refus est explicite plutôt que silencieusement complété.

**Le premier message devient le but.** Une session ouverte sans message a un `goal` vide. Le premier `POST /sessions/{sid}/messages` le remplit avec ce que l'utilisateur vient d'écrire, **et seulement s'il est vide** : une session ouverte avec un but ne le voit jamais réécrit par un message de suivi. C'est le plus petit ajout qui préserve l'invariant dont dépend tout ce qui lit un but — le `user_request` envoyé au modèle (§12.1), le résumé de rotation ([ADR-005](ADR-005-resume-de-contexte-par-le-modele.md), [ADR-014](ADR-014-continuation-apres-rotation.md)), l'instantané d'exécution, la liste des sessions : **une session qui a démarré a un but non vide, et ce but est de l'utilisateur**. L'alternative — laisser le but vide pour toujours — aurait envoyé `goal: ""` au modèle et affiché une ligne vide dans chaque écran ; elle est honnête et inutile.

**Aucun nouvel état, aucune nouvelle transition.** Les deux machines à états ne bougent pas. `SESSION_TRANSITIONS` garde `READY → RUNNING` comme seule sortie de `READY` ; `CONVERSATION_TRANSITIONS` n'est pas touchée. Une session sans conversation n'est pas un état : c'est `current_conversation_id = None`, la valeur par défaut du champ depuis §16. Ce qui change est ce que `ConversationManager.start_session` **ne fait pas** quand il n'y a rien à ouvrir.

**Ce que cela impose aux quatre chemins qui touchent une session.** Chacun avait déjà la réponse ; la décision est de vérifier qu'aucun ne suppose une conversation :

| Chemin | Comportement sur une session sans conversation |
|---|---|
| **premier message** (`continue_session`, branche `READY`) | crée la conversation enfant avec `parent_conversation_id = None`, passe `READY → RUNNING`, lance `run_session` — le chemin d'un message après interruption, au but près |
| **interruption** (`InterruptionHandler.interrupt`) | `READY` est déjà un état inactif : rapport `nothing_to_interrupt`, `conversation_id = null`, **aucune écriture, aucun événement** |
| **lecture** (`GET /sessions/{sid}`, `/snapshot`, `GET /sessions`) | `conversation = null`, `conversations = []`, ni cycle ni plan ni tâche ; la session est listée et filtrable comme n'importe quelle autre `READY` |
| **reprise au démarrage** ([ADR-016](ADR-016-politique-de-reprise.md) §2) | **rien** : la reprise ne règle que les sessions `RUNNING` / `INTERRUPTING`. Une session `READY` n'est pas une session ouverte — il n'y a rien en vol, rien à interrompre, rien à réconcilier. Elle traverse un redémarrage octet pour octet, et son premier message ouvre sa première conversation de l'autre côté |

`resume_session` la refuse (`SESSION_NOT_RESUMABLE`) : il n'y a pas de message en attente à rejouer. C'est le bon refus — ce qu'il faut envoyer à une telle session, c'est un message, pas une reprise.

### 2. L'`user_id` voyage avec la session, et son défaut est celui de `GET /whoami`

`POST /sessions` accepte un `user_id` facultatif. Il est **passé au travers**, pas tracé : `SessionRecord` porte déjà une colonne `user_id` depuis §16, générée du modèle pydantic comme toutes les autres. Rien n'est ajouté au schéma, qui reste en version 1 sans migration — c'est le contraire du choix d'ADR-027 §4 pour `skills` / `effort`, et pour la raison exacte qui le justifiait : **persister un champ, c'est promettre qu'il sert**. Celui-là servait déjà, il était seulement rempli par la mauvaise source.

Cette mauvaise source était `transport.user_id`. C'est la valeur qu'ADR-004 destine à **l'appel au modèle** — l'en-tête `X-User-Id`, le corps de l'`init` —, pas à l'identité de la personne devant l'écran. ADR-024 §2 avait déjà donné la bonne : `identity.py` résout l'utilisateur de la machine (`$USER`, `id -un`, `%USERNAME%`, `whoami`) et ne retombe sur `transport.user_id` qu'en **dernier recours**. La décision est de faire lire cette résolution par les deux routes :

- `build_application` passe `Application.identity.user_id` au `ConversationManager` (`default_user_id`), qui l'applique à toute session créée sans `user_id` — la CLI comprise, donc `agentic-app run` cesse lui aussi de contredire `GET /whoami` ;
- dans l'API, `GET /whoami` et le défaut de `POST /sessions` lisent **la même fonction**, résolue une fois par processus. La cohérence n'est pas une convention de câblage à respecter : deux appelants, une source.

Un `user_id` vide ou blanc n'est pas un nom : il retombe sur le défaut. Un `user_id` explicitement `""` dans le corps reste un `422`, comme les deux autres champs.

**Ce qui n'est pas décidé ici.** Cet `user_id` est une **étiquette**, pas une authentification. L'API locale n'est pas authentifiée (ADR-002), elle écoute la boucle locale et sert un seul poste ; rien ne vérifie qu'un appelant est bien qui il dit, et aucune route ne filtre les sessions par utilisateur. Le jour où plusieurs utilisateurs partageraient une base, il faudra une décision entière — pas un champ.

### 3. Les origines CORS du vrai front

`http://localhost:1420` et `http://127.0.0.1:1420` s'ajoutent aux `cors_origins` par défaut, dans `config.py` et dans `config.toml`. Les entrées existantes sont conservées : `3000` (port historique du front), `5173` (port Vite par défaut) et `tauri://localhost` (la fenêtre empaquetée). Les deux orthographes de la boucle locale sont listées à chaque fois — pour un navigateur, `localhost` et `127.0.0.1` ne sont pas la même origine.

## Conséquences

- **Code** : `orchestration/conversation_manager.py` (`start_session` prend `goal` / `user_message` / `user_id` facultatifs, refuse une demi-paire, n'ouvre rien quand il n'y a rien à ouvrir ; `continue_session` remplit le but vide d'une session `READY` ; `default_user_id`) ; `orchestration/wiring.py` (l'identité machine devient le `default_user_id` de la façade) ; `interfaces/http_api.py` (`CreateSessionRequest.goal` / `user_message` / `user_id` facultatifs, `GOAL_REQUIRED` / `USER_MESSAGE_REQUIRED`, une seule résolution d'identité partagée par `GET /whoami` et `POST /sessions`) ; `interfaces/cli.py` (commande `open`) ; `config.py` (`DEFAULT_CORS_ORIGINS`).
- **Interface en ligne de commande** : `agentic-app run` est inchangée — elle exige toujours un but, et lance la boucle dans son propre processus. La nouvelle commande `agentic-app open` est le pendant de l'écran de connexion : cliente de l'API comme `reply`, `interrupt` et `resume`, elle crée une session vide (`--user-id`, `--working-space`, `--skill`, `--effort`) et affiche la commande qui lui enverra son premier message. Deux formes, deux usages : `run` quand le but est connu d'avance, `open` quand il ne l'est pas encore.
- **Configuration** : `cors_origins` gagne deux entrées dans `config.toml` comme dans les défauts du code — le test du dépôt vérifie que les deux restent identiques.
- **Compatibilité** : totale dans les deux sens. Un corps qui porte les deux champs se comporte exactement comme avant, jusqu'au statut `RUNNING` de la réponse ; un client qui n'envoie pas d'`user_id` obtient un défaut meilleur qu'avant, jamais vide. Le seul changement observable pour un client existant est que `{"goal": …}` seul, qui répondait `422`, répond maintenant `400 USER_MESSAGE_REQUIRED` — un refus plus précis pour la même requête refusée.
- **Persistance** : aucun changement. Aucune colonne, aucune table, aucune migration ; schéma en version 1. `SessionRecord.user_id` existait déjà et est simplement rempli par la bonne source ; `goal` et `user_message` restent des chaînes non nulles, vides tant que rien n'a été dit.
- **Protocole** : aucun changement. Une session sans message n'envoie rien ; quand elle envoie, elle envoie le `user_request` de §12.1 tel quel. Ce qui change est **négatif** : le modèle ne reçoit plus le message fabriqué que le front devait inventer.
- **Observabilité** : la charge de `session.created` est inchangée (`goal`, `budget`, `skills`, `effort`) — le `goal` y est vide pour une session ouverte sans message, ce qui est exactement ce qui s'est passé. L'`user_id` n'y est pas ajouté : il est sur l'enregistrement, qui est la source, et le dupliquer dans un événement inviterait les deux à diverger.
- **Tests** : `tests/integration/test_phase9_api.py` (création sans message : `READY`, sans conversation, un seul événement audité ; le premier message qui ouvre le premier cycle et devient le but ; le but conservé sur un suivi ; interruption et lecture d'une session vide ; les deux `400` ; `user_id` fourni et par défaut, cohérent avec `GET /whoami` ; les origines CORS 1420) ; `tests/integration/test_phase9_orchestration.py` (les mêmes faits contre la vraie boucle : rien d'init, rien de posté, aucune tâche de boucle créée ; le scénario §12 complet à partir d'une session vide ; la demi-paire refusée sans laisser d'enregistrement ; le défaut d'`user_id` et son dernier recours de configuration) ; `tests/integration/test_phase9_recovery.py` (une session vide traverse un redémarrage intacte et reste utilisable) ; `tests/integration/test_phase9_cli.py` (`open` : corps posté, champs de l'écran de connexion, corps vide, erreur de l'API).
- **Documentation** : [`docs/contracts/front-backend-v1.md`](../contracts/front-backend-v1.md) est remis à jour contre la réalité (§3.4, §3.3 `GET /skills`, §3.10, §6.2, §9) ; le `README.md` documente `agentic-app open` et les trois formes de `POST /sessions`.
- **Points ouverts** :
  1. **Une session vide n'a pas de date d'expiration.** Rien ne ferme une session ouverte puis abandonnée : elle reste `READY` indéfiniment, occupe une ligne et, si elle a un `working_space`, le garde lié. Le budget de session ne la borne pas non plus — il ne court qu'à partir du premier `RUNNING` (`started_at`). Décider d'un ramassage demanderait de choisir ce qu'on supprime d'une session qui n'a rien produit, et quand.
  2. **`user_id` n'est pas une authentification** (§2). Aucune route ne le vérifie ni ne filtre sur lui ; deux utilisateurs sur le même poste voient les sessions l'un de l'autre. C'est la conséquence assumée d'ADR-002 (API locale non authentifiée), pas un oubli.
  3. **Le but promu depuis le premier message n'est pas modifiable ensuite.** Une session dont le premier message était « bonjour » gardera ce but pour toute sa vie, y compris dans le résumé de rotation. Laisser l'utilisateur le corriger demande une route d'écriture sur la session, donc une décision sur ce qu'un front a le droit de réécrire d'un enregistrement déjà audité.
  4. **`skills` et `effort` restent inertes** (ADR-027, point ouvert 1). Ouvrir une session sans message ne change rien à cela : ils sont tracés à la création, et le premier message part sans eux.
