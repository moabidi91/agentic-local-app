# ADR-029 — L'échec d'un outil est un verdict : partage de la troncature, plan qui continue, résultat qui le dit

**Statut** : accepté (2026-09-19) — **amende** [ADR-011](ADR-011-troncature-et-chunks.md) §1 (l'algorithme de troncature, dont la règle 1 est remplacée) ; **précise** [ADR-009](ADR-009-drapeaux-d-arret.md) §1 (le défaut implicite « un échec arrête le plan ») et §2 (la règle effective, qui ne change pas) ; ajoute un défaut de plan à côté de celui d'[ADR-010](ADR-010-limites-de-payload.md) et deux champs de résultat dans la lignée d'[ADR-011](ADR-011-troncature-et-chunks.md) ; sans effet sur les machines à états de la spec (§5), sur la taxonomie d'erreurs ([ADR-008](ADR-008-timeout-et-retry-de-tache.md)) et sur le schéma persisté (version 1, aucune migration)

## Contexte

Quand le modèle planifie `mvn clean install` et que le projet ne compile pas, Maven sort en 1. **Une compilation qui échoue est le résultat que toute la boucle existe pour produire** : c'est le fait que le modèle a demandé, celui sur lequel il va raisonner, et la seule chose qui distingue une session utile d'une session qui tourne à vide. Ce n'est pas un incident de l'application.

L'application ne le traitait déjà pas comme un incident au sens d'[ADR-008](ADR-008-timeout-et-retry-de-tache.md) §3, et c'est le point à préserver : un code de sortie non nul ne produit **aucune** `NormalizedError`, **aucun** `FailureRecord`, ne déclenche **aucune** reprise et ne fait **pas** bouger le disjoncteur ; la session continue, la faute n'est pas protocolaire, et le plan rapporte simplement la tâche en `FAILED`. Cette partie était juste et le reste.

Trois défauts la vidaient pourtant de son sens, du plus grave au plus visible.

### 1. Les diagnostics du compilateur pouvaient être supprimés en silence

`PayloadGuard.apply` appliquait la règle 1 d'ADR-011 à la lettre : `stderr_kept = min(E, B)`, puis `stdout_kept = min(O, B - stderr_kept)`. La priorité donnée à stderr venait de §2.5 (« `stderr` est toujours préservé en entier ») et se défend pour un outil Unix ordinaire, qui écrit ses erreurs sur stderr et ses données sur stdout.

Les outils de compilation JVM font l'inverse. Maven écrit **tout** son journal sur stdout, `[ERROR] … cannot find symbol` et `BUILD FAILURE` compris ; stderr ne reçoit que le bruit de la JVM — avertissements de dépréciation, options obsolètes, messages de l'agent. Avec le budget par défaut de 8 192 octets, un stderr bruyant de 8,6 Ko donnait donc :

```
stderr_kept = min(8640, 8192) = 8192      stdout_kept = min(3294, 8192 - 8192) = 0
```

Le modèle recevait **8 Ko d'avertissements de la JVM et pas une ligne du verdict**, avec `stdout_range = [3294, 3294]` pour seule trace. Rien ne le signalait autrement : les octets étaient bien stockés, le `chunk_request` aurait pu les relire — encore aurait-il fallu que le modèle sache qu'il lui manquait quelque chose d'important, alors que le message qu'il venait de recevoir ne contenait aucune erreur de compilation. C'est le pire des trois défauts, parce qu'il est **silencieux** : il ne produit pas une mauvaise réponse, il produit une réponse plausible fondée sur rien.

### 2. Le reste du plan était jeté

`continue_on_error` vaut `false` par défaut (ADR-009 §1), donc un plan de diagnostic parfaitement raisonnable — *compiler · lire le fichier fautif · vérifier la version du JDK* — se réduisait à sa première tâche : les deux autres partaient en `SKIPPED` et le modèle recevait **un code de sortie au lieu d'un diagnostic**. Pour obtenir le comportement voulu, le modèle devait écrire sur chaque tâche une double négation (`continue_on_error: true`, « continue même si ça se passe mal ») alors que ce qu'il voulait dire est : « cette commande est censée pouvoir refuser, c'est même la question que je pose ».

Le défaut d'ADR-009 §1 reste le bon pour une commande ordinaire — `cd`, `test -f`, un script maison : si elle rate, la suite du plan est bâtie sur du sable. Il est faux pour un outil dont **le refus est la réponse**.

### 3. Le modèle n'avait pas de quoi distinguer un verdict d'une non-exécution

Une erreur de compilation et un binaire absent arrivaient tous deux comme `status: "failed"`. Seuls `reason: "SPAWN_FAILED"` et `exit_code: null` les séparaient — et `PROTOCOL_INSTRUCTIONS.md` **ne documentait pas `reason` sur un résultat de tâche**. Un modèle qui lit `status: "failed"`, `stdout: ""` et `exit_code: null` peut raisonnablement conclure « la compilation ne produit aucune sortie » alors que la vraie phrase est « `mvn` n'existe pas sur cette machine ». Les deux mènent à des plans opposés.

## Décision

### 1. La troncature garantit une part à chaque flux (amende ADR-011 §1)

La règle 1 d'ADR-011 (« `stderr_kept = min(E, B)` », c'est-à-dire *stderr d'abord, jusqu'à tout le budget*) est **remplacée**. Soit `B` le budget effectif (ADR-010), `E` la taille de stderr, `O` celle de stdout :

1. chaque flux garde au moins `min(taille du flux, B // 2)` octets ;
2. ce qu'un flux ne consomme pas de sa part est **donné à l'autre** — stderr servi en premier, ce qui est tout ce qui subsiste de la priorité d'ADR-011 ;
3. dans chaque flux, c'est la **fin** qui est conservée (inchangé, ADR-011 règle 2) ;
4. `truncated`, `original_size_bytes`, `stdout_total`, `stderr_total` et les plages d'octets `[début, fin)` sont inchangés, et `stdout_kept + stderr_kept <= B` reste vrai **par construction** (`min(O, B//2) + min(E, B//2) <= B`, et la redistribution ne dépasse jamais le reliquat).

Sur le cas de Maven ci-dessus (`B = 8192`, `E = 8640`, `O = 3294`) : stdout est sous sa moitié, il passe **entier** (3 294 octets, `stdout_range = [0, 3294]`), stderr prend les 4 898 octets restants. Le verdict arrive.

Ce que la nouvelle règle coûte : un stderr plus petit que le budget mais plus grand que sa moitié, face à un stdout volumineux, est maintenant coupé alors qu'il passait entier (`E = 6`, `O = 20`, `B = 10` : 5 et 5, contre 6 et 4 auparavant). C'est le prix d'une garantie symétrique, et il est bien moindre que le défaut qu'il supprime : perdre la fin d'un flux est visible dans les plages, perdre un flux entier ne l'est pas.

**Ce qui n'est pas touché** : le blob brut, non tronqué, reste stocké pour toute la vie de la session et relisible par `chunk_request` (ADR-011) ; le plafond de message et le rabotage de `fit_message` (ADR-010) ; le décodage UTF-8 avec remplacement (ADR-003).

### 2. Les outils dont l'échec est un verdict (précise ADR-009 §1)

`[execution] verdict_programs` liste les programmes dont un code de sortie non nul est un **résultat à interpréter** : compilateurs, outils de compilation, lanceurs de tests, linteurs.

**La règle.** Quand la commande d'une tâche invoque l'un d'eux **et que cette commande a tourné** — elle a rendu un code de sortie, donc ni échec de lancement ni dépassement de délai —, un code non nul **n'arrête pas le plan**. Deux limites, qui sont la décision elle-même :

- **une consigne explicite du modèle l'emporte toujours.** `critical: true` ou `stop_plan_on_failure: true` sur cette tâche arrêtent le plan comme avant, avec le même `stop_reason`. Ce qui est neutralisé est le défaut *implicite* (`not continue_on_error`) — et lui seul : le modèle qui a dit « arrête-toi là » est obéi, le modèle qui n'a rien dit reçoit son diagnostic ;
- **la règle effective d'ADR-009 §2 n'est pas modifiée.** `stops_plan_on_failure = critical or stop_plan_on_failure or not continue_on_error` est toujours calculée à la validation et persistée telle quelle sur la `TaskRecord` : une tâche `mvn` sans drapeau porte bien `stops_plan_on_failure = true`. La lecture du verdict se fait **à l'exécution**, dans `PlanRunner._stop_condition`, parce qu'elle a besoin d'un fait que la validation ne connaît pas encore : la commande a-t-elle tourné ?

**Ce que la règle ne change pas** : la tâche reste `FAILED` (aucun nouvel état), elle continue de compter dans `failed_task_count`, et ses dépendants passent en `SKIPPED` (`dependency_failed:<id>`) exactement comme le prescrit ADR-009 §5 — « même si le plan continue ». Un plan dont toutes les tâches se terminent finit `completed` avec des tâches en échec dedans : c'est déjà ce qui se passait avec `continue_on_error: true`.

**La reconnaissance.** Elle porte sur le **programme réellement invoqué**, lu dans la ligne de commande (`domain/commands.py`, pur, sans I/O) :

- les affectations de tête sont ignorées (`JAVA_HOME=/opt/jdk21 mvn test` → `mvn`) ;
- le premier mot restant est réduit à son nom : chemin retiré (`/usr/bin/mvn`, `./mvnw`), extension retirée (`.exe`, `.cmd`, `.bat`, `.ps1`, `.sh` — donc `gradlew.bat` = `gradlew`), casse repliée ;
- une entrée peut nommer **une sous-commande** quand elle compte : `npm run` et `npm test` sont des entrées, `npm` seul n'en est pas une, parce que `npm install` n'est pas une compilation et que `npm build` n'existe pas. La sous-commande comparée est le premier argument qui n'est pas une option, donc `npm --silent run build` est reconnu.

**La liste par défaut**, cinq familles, entièrement configurable (ajouter les outils de la maison, ou vider la liste pour revenir au comportement d'avant cet ADR) :

| Famille | Entrées |
|---|---|
| Compilations JVM et wrappers | `mvn`, `mvnw`, `gradle`, `gradlew`, `ant`, `sbt`, `javac` |
| Node et TypeScript | `npm run`, `npm test`, `yarn`, `pnpm`, `npx`, `tsc`, `eslint` |
| Rust, .NET, Go | `cargo`, `dotnet`, `go` |
| C / C++ et famille make | `make`, `cmake`, `ninja`, `gcc`, `g++`, `clang`, `clang++` |
| Python | `pytest`, `tox`, `ruff`, `mypy`, `flake8`, `pylint` |

### 3. `default_continue_on_error`, le défaut porté par le plan

`PlanContent` accepte `default_continue_on_error`, résolu **exactement** comme `default_max_output_bytes` (ADR-010) : `tâche ?? plan ?? false`. La valeur résolue est écrite sur la `TaskRecord` dans le champ `continue_on_error` qui existe déjà, et la règle effective d'ADR-009 §2 tourne dessus **verbatim**. Un plan de diagnostic déclare donc son intention une fois au lieu de cinq doubles négations, et une tâche qui déclare sa propre valeur l'emporte toujours sur le défaut du plan.

L'avertissement d'audit `CONTRADICTORY_FLAGS` (ADR-009 §2) est lu sur la valeur **résolue** : `critical: true` dans un plan `default_continue_on_error: true` est la même contradiction que `critical: true` avec `continue_on_error: true` sur la tâche, et mérite la même trace. Pour un plan qui ne déclare pas de défaut, le comportement est inchangé au caractère près.

### 4. Le résultat dit sans ambiguïté ce qu'il est advenu de la commande

Deux champs s'ajoutent, sans toucher un seul champ existant.

**`execution`**, présent sur chaque résultat de tâche, quatre valeurs :

| Valeur | Ce qui s'est passé | Ce que le modèle en fait |
|---|---|---|
| `ran` | la commande a tourné jusqu'au bout ; `exit_code` est **sa** réponse, nulle ou non | lire `stdout` et `stderr` : c'est la preuve |
| `not_started` | aucune commande n'a tourné : elle n'a pas pu être lancée (`reason: "SPAWN_FAILED"`) ou le plan ne l'a jamais atteinte ; `exit_code` est `null` | **le seul cas où il n'y a rien à lire** : ne pas interpréter la sortie vide |
| `timed_out` | lancée puis tuée à son délai ; sortie partielle, pas de code | replanifier avec un `timeout_ms` plus grand, ou une commande plus étroite |
| `stopped` | lancée puis terminée par l'application (arrêt du plan, interruption utilisateur) ; sortie partielle, pas de code | rien n'est anormal, la commande n'a simplement pas fini |

Le même champ est porté par les objets des listes `skipped_tasks` / `cancelled_tasks` / `interrupted_tasks` (`not_started` pour une tâche sautée, `stopped` pour une tâche annulée ou interrompue), pour que tout le message parle **un seul vocabulaire** : c'est aussi ce qui donne à `stopped` un endroit où exister, une tâche annulée n'apparaissant jamais dans `results` (ADR-009 §5). Un `chunk_request` vaut toujours `ran` : la lecture est locale, elle a lieu, servie ou refusée.

**`failure_is_verdict`**, présent **et à `true` uniquement** dans le cas de §2 : tâche `failed`, commande qui a tourné, programme reconnu. Absent partout ailleurs — un booléen toujours présent aurait grossi chaque message d'une information vraie une fois sur cinquante. C'est la phrase que le modèle doit lire : *ceci est une réponse à analyser, pas une panne de l'application*.

**L'ordre d'ADR-017 est intact** : `results` suit l'ordre de déclaration des tâches, les listes de références aussi, et rien de ce qui précède ne dépend de l'ordre d'achèvement.

**`PROTOCOL_INSTRUCTIONS.md`** documente les quatre valeurs d'`execution`, `failure_is_verdict`, le nouveau partage de la troncature, `default_continue_on_error`, et **nomme la liste des programmes reconnus telle qu'elle est configurée** (un déploiement qui l'a vidée l'annonce comme telle). C'est la réponse au troisième défaut : le modèle n'a plus à deviner ce que `status: "failed"` recouvre.

## Conséquences

- **Code** : `domain/commands.py` (nouveau : `invoked`, `normalise_program_entry`, `VerdictPrograms` — pur, aucune I/O) ; `config.py` (`DEFAULT_VERDICT_PROGRAMS`, `ExecutionSection.verdict_programs` et sa normalisation) ; `execution/payload_guard.py` (`apply`) ; `execution/result_collector.py` (`execution`, `failure_is_verdict`, recogniser injecté) ; `execution/plan_runner.py` (`_stop_condition`, `_failure_is_verdict`) ; `protocol/messages.py` (`TaskExecution`, `TaskResult.execution` / `.failure_is_verdict`, `TaskRef.execution`, `PlanContent.default_continue_on_error`) ; `protocol/adapter.py` (`_resolve_continue_on_error`, le rendu de la règle des verdicts dans les instructions).
- **Configuration** : une seule clé, `[execution] verdict_programs`, documentée famille par famille dans `config.toml` ; les défauts du fichier et du code restent identiques (test du dépôt). Un opérateur ajoute ses outils ou vide la liste ; une entrée est normalisée (chemin, extension, casse) et une entrée de plus de deux mots est une erreur de configuration.
- **Protocole** : extension **additive** dans les deux sens. `default_continue_on_error` est facultatif et son absence redonne le comportement d'ADR-009 §1 ; `execution` et `failure_is_verdict` sont des champs de plus dans un message que le modèle lit, jamais qu'il écrit. Les exemples de §12.5 restent valides.
- **Persistance** : **aucun changement**. Aucune colonne, aucune table, aucune migration ; schéma en version 1. La valeur résolue de `continue_on_error` va dans le champ qui existait déjà, et `execution` / `failure_is_verdict` sont **dérivés** à la construction du message (`exit_code`, `timed_out`, `reason`, `status`, `cmd`) plutôt que persistés : ils ne disent rien que l'enregistrement ne dise déjà, et les dupliquer inviterait les deux à diverger.
- **Ce qui n'est explicitement pas changé** : aucun nouvel état de tâche ni de plan, les machines à états de la spec (§5) sont intactes ; la taxonomie d'erreurs d'ADR-008 est intacte — un code non nul ne produit toujours ni `NormalizedError`, ni `FailureRecord`, ni reprise, ni tic de disjoncteur, et un `SPAWN_FAILED` en produit toujours un ; ADR-009 §2 (la règle effective) et §5 (les dépendants sautés) tournent mot pour mot comme avant ; le stockage des blobs et le `chunk_request` d'ADR-011 sont inchangés ; l'ordre déterministe d'ADR-017 est inchangé.
- **Tests** : phase 0 (la liste par défaut, la normalisation d'une entrée, la liste vidée, l'entrée à trois mots refusée) ; phase 2 (la table `tâche ?? plan ?? false`, la contradiction lue sur la valeur résolue, les instructions qui nomment la liste configurée ou annoncent la règle éteinte) ; phase 4 (la table de troncature récrite autour de la garantie par flux, plus le cas Maven réaliste — stderr bruyant, `[ERROR]` et `BUILD FAILURE` sur stdout — qui échouerait sous l'ancienne règle ; `invoked` sur quinze formes de ligne de commande ; `VerdictPrograms` par famille ; `execution` et `failure_is_verdict` sur chaque forme de résultat) ; phase 5 (le plan qui continue et rend les trois résultats, `critical` / `stop_plan_on_failure` qui l'emportent, l'échec de lancement et le dépassement de délai qui arrêtent toujours, le programme non reconnu inchangé, la liste vidée, les dépendants toujours sautés, la table des six combinaisons de `default_continue_on_error`) ; batterie de conformité (`licit-build-tool-verdict-continues`, `licit-build-tool-verdict-explicit-stop`). **La table exhaustive des 32 combinaisons de drapeaux de la phase 5 passe sans être modifiée** : elle porte sur des commandes ordinaires, et c'est la vérification que le défaut d'ADR-009 §1 n'a pas bougé.
- **Documentation** : `PROTOCOL_INSTRUCTIONS.md` (§2.2, §3.1, §3.2, §3.3, §4, §5), `config.toml`, la carte des modules et le `README.md`.

## Points ouverts

1. **Ce que la reconnaissance ne peut pas voir.** Elle lit une ligne de commande, pas un processus. Lui échappent : un programme derrière une variable de shell (`$BUILD_TOOL install`, `"${MAVEN}" verify`) ; un **tube**, dont le code de sortie est celui de la *dernière* commande — `mvn install | tail -80` sort en 0 même quand Maven échoue, et l'application voit alors un succès, pas un verdict (sans `pipefail`, elle a raison) ; un script maison (`./build.sh`, `make.py`) qui appelle l'outil sans en porter le nom ; un `env mvn test`, un `nice`, un `timeout 300 mvn test`, où le premier mot est un lanceur. Aucun de ces cas ne produit un mauvais verdict : il produit l'**absence** de verdict, c'est-à-dire le comportement d'avant cet ADR — le plan s'arrête, ce qui est le défaut sûr. Aller plus loin demanderait de lire la commande comme un shell la lit, donc d'écrire un analyseur de shell par plateforme : une décision entière, pas une liste.
2. **Le même raisonnement vaut au-delà des compilations.** Un linteur qui trouve une faute, un lanceur de tests qui rapporte un test rouge, un vérificateur de types qui refuse un fichier : leur code de sortie non nul est tout autant un verdict, et la liste par défaut les inclut déjà (`pytest`, `tox`, `ruff`, `mypy`, `flake8`, `pylint`, `eslint`, `tsc`). Reste à décider s'il faut aller plus loin et **distinguer les familles** — « échoue parce qu'il a trouvé quelque chose » (code 1) d'« échoue parce qu'il n'a pas pu tourner » (code 2, 4, 127…), une convention que chaque outil interprète à sa façon. Tant que ce n'est pas tranché, tout code non nul d'un programme reconnu est un verdict, ce qui est le choix le plus simple à expliquer au modèle.
3. **La part garantie est fixée à la moitié.** `B // 2` par flux est un choix rond, pas une mesure : un partage configurable (ou proportionnel aux tailles réelles) serait plus fin, au prix d'une règle que le modèle ne pourrait plus refaire de tête. Les plages d'octets restant exactes, le modèle sait toujours ce qui lui manque et peut le relire par `chunk_request`.
4. **Rien ne dit au modèle qu'un outil a été reconnu *avant* qu'il ne planifie.** Les instructions nomment la liste, mais l'application ne valide pas les commandes d'un plan à l'avance et ne signale pas « cette tâche sera lue comme un verdict » au moment de l'accepter. Le modèle l'apprend dans le résultat. Le lui dire plus tôt supposerait une réponse à l'acceptation d'un plan, qui n'existe pas dans le protocole.
