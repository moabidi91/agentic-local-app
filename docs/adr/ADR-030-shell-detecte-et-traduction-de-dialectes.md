# ADR-030 — Le shell de la machine : détecté, annoncé, et un dictionnaire entre dialectes

**Statut** : accepté (2026-09-19) — **amende** [ADR-003](ADR-003-plateformes-cibles.md) §3 sur deux points et deux seulement : l'environnement d'exécution (OS, dialecte du shell, répertoire de travail) **entre désormais dans les instructions** envoyées au modèle, et une commande peut être **réécrite** dans un cas étroit, borné et tracé ; corrige le défaut de code de sortie documenté depuis l'origine dans l'en-tête d'`execution/platform.py`, devenu un défaut de correction depuis qu'[ADR-029](ADR-029-echec-d-outil-comme-verdict.md) §2 fait du code de sortie un **verdict** ; ajoute un champ de résultat dans la lignée d'ADR-029 §4 et une clé de configuration ; **sans effet sur le schéma persisté** (version 1, aucune colonne, aucune migration : la trace vit dans le journal d'audit et le champ de résultat est **dérivé**, exactement comme ADR-029 §4)

## Contexte

Le modèle écrit des commandes de shell à l'aveugle.

ADR-003 §3 le voulait ainsi, et pour une bonne raison : « le modèle continue de découvrir l'environnement par son `discovery_plan`, et c'est à lui d'émettre des commandes valides pour l'OS qu'il découvre (`Get-ChildItem` plutôt que `ls`, par exemple). On ne réécrit jamais une commande. » L'application ne devait rien injecter, rien interpréter, rien deviner : elle exécute, mesure, rapporte. C'est la ligne directrice de tout le projet et elle reste juste.

Elle a un coût que l'usage a rendu visible. **Le premier plan d'une conversation est écrit avant toute découverte.** Le `discovery_plan` de la spec §12.2 est lui-même une commande de shell (`uname -a && echo $SHELL`) : sur une machine Windows en PowerShell, il échoue — `uname` n'existe pas, `&&` n'est pas un opérateur de Windows PowerShell 5.1, et le modèle reçoit un `SPAWN_FAILED` ou une erreur d'analyse à la place de la découverte qu'il demandait. Le tour suivant sert à réparer le tour précédent. Sur la machine de développement principale, qui est sous Windows, la boucle commence donc systématiquement par un aller-retour perdu, et rien dans le protocole ne dit au modèle pourquoi.

Trois défauts, du plus profond au plus visible.

### 1. L'application ne sait pas elle-même quel shell elle lance

`PosixPlatformAdapter.default_shell()` cherchait `bash` puis `sh` ; `WindowsPlatformAdapter.default_shell()` rendait la chaîne `"powershell"`, en dur. Nulle part un objet ne disait « cette machine parle PowerShell » : la question ne se posait qu'au moment de construire l'`argv`, sous forme d'un `if "powershell" in name` enfoui dans `build_launch`. Il n'y avait donc rien à annoncer, rien sur quoi décider, et rien à interroger : ni la CLI ni l'API ne savaient répondre à « quel shell vas-tu utiliser ? ». Les cas réels que cette absence ignore sont pourtant ordinaires : PowerShell 7 (`pwsh`) coexiste avec Windows PowerShell 5.1 (`powershell`) sur la même machine, `pwsh` tourne aussi sur Linux et macOS, et Git bash tourne sur Windows.

### 2. Sur Windows, `exit_code` mentait — et ADR-029 en a fait un défaut de correction

L'en-tête d'`execution/platform.py` le documentait depuis l'origine, en le qualifiant de curiosité :

> Known Windows PowerShell limitation: `powershell -Command <cmd>` reports exit code 1 for any native command that exited with a code other than 0 or 1.

`powershell -Command` ne rend pas le code de sortie du programme qu'il lance : il rend **son** code à lui, 0 en cas de succès, 1 sinon. Un `mvn` qui sort en 1 et un `javac` qui sort en 2, un binaire absent qui sort en 127, un test qui sort en 5 : tous arrivent à l'application en **1**.

Tant qu'« un code non nul = un échec », la confusion était sans conséquence pratique. ADR-029 §2 a changé cela : le code de sortie d'un programme reconnu est maintenant **le résultat que la boucle existe pour produire**, celui que le modèle lit et sur lequel il raisonne, celui qui décide si le plan continue. Une valeur fausse à cet endroit n'est plus une curiosité, c'est une **donnée corrompue présentée comme une preuve**. Et elle est corrompue en silence : rien, dans le message, ne distingue un vrai 1 d'un 1 fabriqué par l'interpréteur.

### 3. Rien ne dit au modèle où il est, alors que c'est l'information la moins chère du système

L'application connaît son OS, son interpréteur et son `cwd` avant même d'ouvrir la conversation. Le modèle, lui, les découvre par échec. ADR-003 §3 rangeait ces deux réglages hors du protocole au nom du principe « l'application n'injecte aucune configuration d'environnement » (spec §17.5) — mais ce principe protège contre l'injection de **connaissances que l'application n'a pas** (ce qui est installé, ce que contient le projet, ce que veut l'utilisateur). Le nom du shell qu'elle s'apprête elle-même à lancer n'en fait pas partie : ce n'est pas une hypothèse sur la machine, c'est un fait sur l'application.

## Décision

### 1. Le shell est détecté, et la réponse est un objet

`domain/shell.py` (pur sauf la sonde, injectable) répond à « quel interpréteur va recevoir les commandes, et pourquoi celui-là ».

`DetectedShell` porte quatre champs : `program` (l'exécutable lancé), `name` (son nom nu, chemin et extension retirés, en minuscules), `dialect` et `source`.

| `dialect` | Interpréteurs reconnus |
|---|---|
| `posix` | `bash`, `sh`, `zsh`, `dash`, `ash`, `ksh` |
| `powershell` | `powershell` (Windows PowerShell 5.1), `pwsh` (PowerShell 7, y compris sur Linux et macOS) |
| `cmd` | `cmd` |
| `unknown` | tout le reste — un opérateur peut épingler n'importe quoi, et l'application ne lui invente pas un dialecte |

| `source` | Ce qui a décidé |
|---|---|
| `configured` | `[execution] shell` nomme un interpréteur : il est pris **tel quel**, sans aucune recherche. L'opérateur n'est jamais contredit, même quand le nom est inconnu |
| `detected` | trouvé sur le `PATH` par `shutil.which`, dans l'ordre des candidats de la plateforme : `bash`, `zsh`, `sh` hors Windows ; `pwsh` puis `powershell` sur Windows |
| `default` | aucun candidat trouvé : `/bin/sh` hors Windows, `powershell` sur Windows — exactement ce que lançaient les adaptateurs avant cet ADR |

Trois propriétés tiennent : la détection **ne lève jamais** (un `PATH` illisible rend `default`), elle **n'appelle jamais rien** (seul `which` est consulté, jamais l'interpréteur lui-même), et `which` est **injecté** — aucun test de la suite ne dépend de la machine qui l'exécute.

`ExecutionEnvironment` réunit les trois faits annoncés au §3 : `operating_system` (`win32` → `Windows`, `darwin` → `macOS`, `linux` → `Linux`, sinon la valeur brute), le `DetectedShell`, et `cwd` rendu absolu **lexicalement** (`os.path.abspath`, jamais `Path.resolve` : l'annonce ne doit dépendre ni de ce qui existe sur le disque à cet instant, ni d'un lien symbolique).

`PlatformAdapter.detect_shell()` et `.environment()` exposent le tout, et `agentic-app shell show` l'imprime.

### 2. Le code de sortie survit à PowerShell

`build_launch` remonte de la plateforme **au dialecte** : ce qui décide de la forme du lancement est le shell détecté, pas le système d'exploitation. PowerShell 7 épinglé sur Linux est donc lancé comme PowerShell, et Git bash épinglé sur Windows comme un shell POSIX.

Pour le dialecte `powershell`, l'interpréteur reçoit un **script** :

```
<la commande du modèle, mot pour mot>
if (Test-Path -LiteralPath variable:\LASTEXITCODE) { exit $LASTEXITCODE }
```

et ce script voyage **encodé en Base64 d'UTF-16LE** :

```
<powershell> -NoProfile -NonInteractive -EncodedCommand <base64>
```

Deux décisions, et il faut les séparer.

**L'épilogue** rend le code de sortie. `$LASTEXITCODE` est la seule variable de PowerShell qui contient le code d'un programme natif ; `exit $LASTEXITCODE` le fait sortir de l'interpréteur. Le `Test-Path` qui l'entoure est essentiel : `$LASTEXITCODE` **n'existe pas** tant qu'aucun programme natif n'a tourné. Une commande faite uniquement de cmdlets (`Get-ChildItem`) n'atteint donc pas le `exit` et garde le code de PowerShell lui-même, c'est-à-dire le comportement d'avant l'ADR pour les commandes qui n'en souffraient pas. Une erreur bloquante interrompt le script avant l'épilogue et sort en 1, comme avant.

**L'encodage** rend le passage sûr. La commande vient du modèle et peut contenir des guillemets simples et doubles, des `$`, des accents graves, des points-virgules, des sauts de ligne. Les faire traverser une ligne de commande Windows en les échappant demande d'empiler les règles de citation de trois couches (`CreateProcess`, l'analyseur d'arguments, l'analyseur de PowerShell) : c'est exactement le genre de code qui marche sur les exemples et corrompt les cas réels. `-EncodedCommand` supprime le problème plutôt que de le traiter — le script est une suite d'octets, il n'y a plus rien à échapper.

**Ce que ce lancement ne couvre pas**, et qui doit être lu comme faisant partie de la décision :

- **le code rendu est celui du dernier programme natif du script, pas celui de la dernière instruction.** `foo.exe; Write-Host ok` sort avec le code de `foo.exe`, là où `foo; echo ok` en bash sort avec celui d'`echo`. C'est la convention retenue par les intégrations usuelles de PowerShell, et c'est celle qui sert le modèle : le code d'un outil de compilation ne doit pas être effacé par la commande d'affichage qui le suit ;
- **une commande sans aucun programme natif garde le code de PowerShell** (0 ou 1). L'application ne peut pas mieux faire : un cmdlet en échec n'a pas de code de sortie ;
- **la taille.** Le Base64 d'UTF-16LE pèse environ 2,7 fois le nombre de caractères de la commande, et Windows plafonne une ligne de commande à 32 767 caractères. Une commande de plus de ~12 000 caractères ne peut donc pas être lancée sous cette forme ; elle rend un `SPAWN_FAILED` clair au lieu d'être tronquée. Aucune commande raisonnable n'en approche, et le plafond est documenté plutôt que contourné ;
- **la décoration de stderr reste.** PowerShell enveloppe la sortie d'erreur d'un programme natif dans un enregistrement d'erreur formaté ; le contenu réel est dedans, mais il est entouré de texte. C'est une limitation d'affichage, pas de correction : rien n'est perdu, et le modèle lit le message d'origine dans la décoration ;
- **`cmd /c` et les shells POSIX ne changent pas.** Ils reçoivent la commande verbatim comme avant. `cmd` a la même limitation de code de sortie pour ses commandes internes, et aucune forme sûre n'existe pour la corriger ; c'est un point ouvert.

La construction est **vérifiable partout** : `powershell_script` et `encode_powershell_command` sont deux fonctions pures, et les tests décodent le Base64 pour comparer au script attendu. Aucun Windows n'est nécessaire, ce qui est bien, puisque `mypy --platform win32` est une porte du dépôt mais qu'aucune machine Windows ne l'est.

### 3. L'environnement est **annoncé** — l'amendement d'ADR-003 §3

Les instructions envoyées au modèle (`PROTOCOL_INSTRUCTIONS.md`, §3.6) portent désormais, à côté des limites d'octets et de délais déjà rendues depuis la configuration :

| | |
|---|---|
| Operating system | **Windows** |
| Shell | `C:\Program Files\PowerShell\7\pwsh.exe` (found on this machine) |
| Shell dialect | **powershell** |
| Working directory | `C:\work\project` |

suivies d'une ligne de conseil propre au dialecte (« `Get-ChildItem` rather than `ls -la`, `$env:JAVA_HOME` rather than `$JAVA_HOME` »).

**C'est un amendement d'ADR-003 §3**, qui rangeait `shell` et `cwd` hors des messages du protocole. Il est délibérément étroit, et les limites sont la décision :

- **trois faits, pas un de plus** : OS, dialecte (avec l'exécutable et la façon dont il a été choisi) et répertoire de travail. Rien sur les outils installés, leurs versions, le contenu du projet, les variables d'environnement ;
- **le modèle reste celui qui émet les commandes.** L'application ne propose aucune commande, n'en valide aucune à l'avance, n'en suggère aucune ;
- **le `discovery_plan` reste la façon d'apprendre tout le reste**, et le texte des instructions le dit explicitement à cet endroit précis ;
- **c'est un fait sur l'application, pas une hypothèse sur la machine.** L'application annonce l'interpréteur qu'elle va lancer elle-même. Spec §17.5 (« l'application n'injecte aucune configuration d'environnement, le modèle la découvre ») garde tout son sens pour ce que l'application ne sait pas.

Le gain est disproportionné au coût : un modèle qui lit « powershell » écrit du PowerShell, et l'aller-retour perdu du premier plan disparaît. C'est la correction la moins chère des quatre, et de loin.

### 4. Le dictionnaire entre dialectes — la vraie brèche dans ADR-003 §3

ADR-003 §3 écrit « on ne réécrit jamais une commande ». Cet ADR réécrit une commande. **C'est le point qui demande la justification la plus forte du document, et il est encadré par quatre garanties, chacune vérifiable.**

Le dictionnaire (`domain/dialects.py`) est consulté **uniquement** quand la commande est écrite dans l'autre dialecte que le shell détecté. Il est piloté par `[execution] translate_commands` (par défaut `true`) et sa table entière est imprimable par `agentic-app shell rules`.

#### Garantie 1 — jamais silencieux

Dès que le dictionnaire est consulté, la décision est **inscrite dans le journal d'audit** — l'événement `task.state_changed` de la transition `RUNNING` porte `cmd_executed`, `translation_rules` et `translated_to`, ou le motif quand rien n'a été réécrit — et **rendue au modèle** dans le champ `translation` du résultat de tâche :

```json
{
  "status": "translated",
  "from_dialect": "posix",
  "to_dialect": "powershell",
  "original_cmd": "head -n 20 build.log",
  "executed_cmd": "Get-Content build.log -TotalCount 20",
  "rules": ["head-lines"]
}
```

Ce journal est **chaîné par hachage** (ADR-017), écrit une fois et jamais réécrit : c'est lui, et non une colonne, qui prouve ce qui a réellement tourné, et il le prouve mieux qu'une colonne puisqu'il est infalsifiable. L'événement est publié avec la transition qu'il accompagne, donc après sa persistance et avant que la commande ne soit lancée : l'ordre « persister, publier, agir » d'[ADR-015](ADR-015-persister-avant-publier.md) est respecté à la lettre.

Le champ `translation` du résultat, lui, est **dérivé** à la construction du message — le traducteur est une fonction pure de la ligne de commande, et la ligne de commande est stockée telle quelle. C'est le choix d'ADR-029 §4 pour `execution` et `failure_is_verdict`, pour la même raison : deux endroits qui disent la même chose finissent par se contredire, et **aucun schéma ne bouge**. Le champ est **absent** quand le dictionnaire n'a pas été consulté, ce qui est le cas ordinaire (`mvn clean install` est neutre) : un champ toujours présent aurait grossi chaque message d'une information vraie une fois sur cinquante, exactement le raisonnement d'ADR-029 §4 sur `failure_is_verdict`.

#### Garantie 2 — seulement ce dont le dictionnaire est sûr

Ce qu'il ne sait pas traduire **exactement** passe **tel quel**, et le résultat dit pourquoi :

```json
{
  "status": "unchanged",
  "from_dialect": "posix", "to_dialect": "powershell",
  "original_cmd": "rm -rf build", "executed_cmd": "rm -rf build",
  "rules": [],
  "reason": "no rule maps `rm`: the dictionary translates only commands that read, never one that creates, moves or deletes"
}
```

Le modèle lit la phrase et se corrige lui-même. **Une traduction seulement plausible est pire que pas de traduction** : le modèle raisonnerait alors sur une commande qu'il n'a pas écrite et qu'il ne voit pas, ce qui est précisément le défaut silencieux qu'ADR-029 §1 a passé un ADR entier à supprimer ailleurs.

La traduction est **tout ou rien** : si un seul segment d'une commande résiste, rien n'est réécrit. Une commande à moitié traduite serait un troisième dialecte que personne ne parle.

#### Garantie 3 — rien qui écrive

C'est la forme retenue pour « refuser plutôt que deviner sur tout ce qui est destructeur », et elle est plus forte qu'une liste noire :

> **Toutes les règles traduisent des commandes qui LISENT.** Lister un répertoire, afficher un fichier, afficher le répertoire courant, lire une variable d'environnement, localiser un programme. **Aucune règle ne crée, ne déplace, n'écrase ni ne supprime quoi que ce soit.**

Il n'y a donc pas de mauvaise suppression possible : il n'y a aucune suppression. `rm`, `mv`, `cp`, `mkdir`, `touch`, `chmod`, `Remove-Item`, `Set-Content`, `Out-File` et leurs semblables sont **reconnus** — pour que le refus soit expliqué plutôt que muet — et jamais traduits. `mkdir` a été écarté après hésitation : `New-Item -ItemType Directory -Force` est presque `mkdir -p`, et « presque » est exactement le mot qui disqualifie une règle ici.

#### Garantie 4 — bornée et inspectable

Le critère d'admission d'une règle, énoncé une fois :

> **Une règle est admise quand la commande traduite pose la même question sur le même objet, et ne peut pas agir sur un autre objet que celui nommé.** Le *format* de la sortie peut différer — il diffère de toute façon entre deux shells, et le modèle lit du texte — mais la cible, l'effet et le mode d'échec doivent être identiques.

**La table**, seize règles, huit par sens :

| Règle | POSIX | PowerShell |
|---|---|---|
| `list-directory` | `ls [-a\|-A\|-l] [PATH]` | `Get-ChildItem [-Force] [PATH]` |
| `print-file` | `cat FILE` | `Get-Content FILE` |
| `head-lines` | `head [-n N] FILE` | `Get-Content FILE -TotalCount N` |
| `tail-lines` | `tail [-n N] FILE` | `Get-Content FILE -Tail N` |
| `print-working-directory` | `pwd` | `Get-Location` |
| `print-text` | `echo ARG` | `Write-Output ARG` |
| `locate-program` | `which NAME` | `Get-Command NAME` (`command -v NAME` au retour) |
| `list-environment` | `env` / `printenv` | `Get-ChildItem Env:` |
| `environment-variable` | `$NAME` / `${NAME}` | `$env:NAME` |

Les deux sens ne sont pas exactement symétriques, et c'est voulu : au retour, le dictionnaire ne reconnaît que des noms **non ambigus** (`Get-ChildItem`, `gci`, `Get-Content`, `gc`, `Get-Location`, `gl`, `Write-Output`, `Get-Command`). Les alias PowerShell qui portent un nom POSIX (`ls`, `cat`, `pwd`, `echo`) sont volontairement absents de ce sens : `ls -la` envoyé à bash est déjà correct, et le traduire serait absurde.

Les bornes d'arguments sont étroites **pour une raison** : `Get-ChildItem a b` ne liste pas deux répertoires, il lie `b` à un autre paramètre ; `Write-Output a b` est une erreur de paramètre. `ls` et `cat` sont donc limités à **un** chemin, `echo` à **un** argument. Une règle qui lit « presque juste » est celle qui fait le plus de dégâts, parce qu'elle réussit.

**La syntaxe des variables** est convertie dans les deux sens, sauf pour les noms que le shell ou le système possède lui-même : `$HOME`, `$PWD`, `$SHELL`, `$USER`, `$UID`, `$IFS`, `$RANDOM`… au départ, `$env:USERPROFILE`, `$env:APPDATA`, `$env:WINDIR`, `$env:COMSPEC`… au retour. Raison unique et symétrique : Windows ne définit pas `HOME`, donc `$env:HOME` **lirait vide** là où `$HOME` lisait le répertoire de l'utilisateur, et réciproquement. Un `$` que le dictionnaire ne sait pas lire (`$1`, `$?`, `$PSVersionTable`) refuse la commande entière.

**Ce qui est refusé d'office**, avant même de consulter une règle, parce que les deux dialectes ne le lisent pas pareil : tubes, redirections, opérateurs de chaînage (`&&`, `||`, `&`), jokers (`*`, `?`, `[`), substitutions (`` ` ``, `$(…)`), blocs, échappements par barre oblique inverse, commentaires, sauts de ligne, préfixes `NAME=valeur`. Seul `;` sépare, parce qu'il sépare exactement pareil des deux côtés.

`&&` mérite un mot, parce que c'est le refus le plus coûteux : il est partout dans les commandes écrites par un modèle. Le traduire en `;` changerait la sémantique (la seconde commande s'exécuterait même après un échec) — inacceptable. PowerShell 7 possède bien `&&`, mais le nom du programme ne dit pas la version : `pwsh` est PowerShell 6 **ou** 7, et `&&` est une erreur d'analyse en 6. Deviner la version pour gagner un opérateur est exactement ce que la garantie 2 interdit. Le modèle, lui, est prévenu par l'annonce du §3 et écrit `;`.

**Les programmes reconnus et délibérément non traduits** portent chacun sa raison, rendue au modèle :

| Raison | Programmes (extraits) |
|---|---|
| le dictionnaire ne traduit que ce qui lit | `rm`, `rmdir`, `mv`, `cp`, `ln`, `mkdir`, `touch`, `chmod`, `dd`, `tee`, `kill` · `Remove-Item`, `Move-Item`, `Copy-Item`, `New-Item`, `Set-Content`, `Out-File`, `Stop-Process` |
| langage de motifs, de champs ou d'expressions différent | `grep`, `sed`, `awk`, `find`, `cut`, `tr`, `sort`, `uniq`, `xargs`, `wc` · `Select-String`, `Where-Object`, `ForEach-Object`, `Sort-Object`, `Select-Object` |
| répond par son code de sortie, et les codes ne s'accordent pas | `test` · `Test-Path` |
| aucune commande qui pose exactement la même question | `uname`, `df`, `du`, `ps`, `stat`, `date`, `export`, `source` · `Get-Process`, `Get-Service`, `Get-Date`, `Get-CimInstance` |

Tout programme absent de ces listes (`mvn`, `java`, `python`, `./gradlew`) est **neutre** : le dictionnaire n'est pas consulté, rien n'est persisté, aucun champ n'apparaît.

`translate_commands = false` supprime le mécanisme entier : aucune commande n'est jamais réécrite, aucun résultat ne porte `translation`, et les instructions l'annoncent au modèle.

## Conséquences

- **Code** : `domain/shell.py` (nouveau : `ShellDialect`, `ShellSource`, `DetectedShell`, `ExecutionEnvironment`, `classify_shell`, `detect_shell`, `describe_environment`) ; `domain/dialects.py` (nouveau : `TranslationRule`, `RefusedProgram`, `CommandTranslation`, `ShellTranslator`, `TRANSLATION_RULES`, `REFUSED_PROGRAMS`, `describe_dictionary`) ; `execution/platform.py` (détection déléguée, `detect_shell` / `environment`, `build_launch` remonté dans la classe de base et piloté par le dialecte, `powershell_script` / `encode_powershell_command`, `LaunchSpec.dialect`, `default_translator`) ; `execution/plan_runner.py` (le dictionnaire consulté au lancement, les champs écrits avec la transition, la charge utile d'audit) ; `execution/result_collector.py` (`translation`) ; `protocol/messages.py` (`TaskTranslation`, `TaskResult.translation`) ; `protocol/adapter.py` (`render_instructions(config, environment=…)`, l'annonce et la règle de traduction) ; `config.py` (`translate_commands`) ; `interfaces/cli.py` (`shell show`, `shell rules`).
- **Configuration** : une seule clé nouvelle, `[execution] translate_commands`. `[execution] shell` ne change pas de sens, seulement de documentation : vide = **détecté** (et non plus « défaut plateforme »), renseigné = pris tel quel.
- **Protocole** : extension **additive** dans le sens application → modèle. `translation` est un champ de plus dans un message que le modèle lit, jamais qu'il écrit ; l'annonce est du texte dans les instructions, hors des messages typés. Les exemples de §12.5 restent valides et rien de ce que le modèle envoie ne change.
- **Persistance** : **aucun changement**. Aucune colonne, aucune table, aucune migration ; schéma en version **1**, et toute base existante s'ouvre comme avant. La trace durable de ce qui a tourné est l'événement `task.state_changed` de la transition `RUNNING`, dont la charge utile est du JSON dans le journal d'audit chaîné par hachage — un journal n'a pas de schéma à faire évoluer, et il est déjà l'endroit où l'on va chercher « qu'est-ce qui s'est passé ». Le champ `translation` du résultat est **dérivé** de `cmd` au moment de construire le message, exactement comme `execution` et `failure_is_verdict` (ADR-029 §4) : le traducteur est pur, la commande est stockée verbatim, donc lui reposer la question rend la réponse sur laquelle le runner a agi.
- **Ce qui n'est explicitement pas changé** : aucun nouvel état de tâche ni de plan ; la taxonomie d'erreurs d'ADR-008 ; la règle de verdict d'ADR-029 §2, qui devient simplement **exacte** sur Windows ; la troncature d'ADR-011/ADR-029 §1 ; l'ordre déterministe d'ADR-017 ; la terminaison en deux temps d'ADR-003 §2 ; l'encodage en octets bruts d'ADR-003 §4 ; `cmd /c` et les shells POSIX, dont le lancement est identique au caractère près.
- **Tests** : phase 0 (la clé de configuration et son défaut, la parité `config.toml` / code) ; phase 2 (l'annonce rendue pour chaque dialecte et chaque source, la règle de traduction allumée et éteinte, les instructions qui ne diffèrent que par l'annonce d'une machine à l'autre) ; phase 4 (la détection sur les deux plateformes avec un `which` injecté, le shell épinglé jamais sondé, le `PATH` cassé, la classification par nom, le script PowerShell et son aller-retour Base64 sur sept commandes hostiles, `cmd` et les shells POSIX inchangés, `pwsh` épinglé sur POSIX également encodé, chaque règle du dictionnaire dans les deux sens, vingt-deux formes de refus avec leur motif, la commande neutre, la traduction éteinte, le champ du résultat dérivé — deux constructions du même enregistrement donnant le même objet, un collecteur sans traducteur n'en inventant aucun) ; phase 5 (la traduction exécutée, auditée et rendue ; la commande non traduisible exécutée verbatim avec son motif ; la traduction éteinte ; le `chunk_request` jamais concerné) ; phase 9 (`shell show`, `shell rules`, leurs formes `--json` ; et de bout en bout, sur le câblage complet : la commande traduite reçue par l'exécuteur, l'événement d'audit **relu depuis la base** dans une chaîne valide, le `translation` du message réellement posté, et l'absence de tout champ de traduction sur `TaskRecord`). La batterie de conformité n'est pas touchée : elle porte sur ce que le modèle **envoie**, et rien n'y change.
- **Documentation** : `PROTOCOL_INSTRUCTIONS.md` (§2.2 pour `translation`, §3.6 pour l'annonce), `config.toml`, `docs/architecture/03-execution-model.md`, `docs/architecture/02-protocol.md`, `docs/architecture/04-persistence-and-audit.md`, `docs/architecture/09-module-map.md`, la ligne de statut d'ADR-003 et `docs/adr/README.md`.

## Points ouverts

1. **Ce qu'un dictionnaire de ce genre ne peut fondamentalement pas faire.** La liste est courte et vaut d'être lue comme une limite de principe, pas comme un travail à finir.
   - **Les tubes.** `ls -la | grep foo` n'est pas une commande, c'est une composition d'objets. En POSIX, du texte circule ; en PowerShell, des objets .NET. `Get-ChildItem | Select-String foo` ne cherche pas dans les mêmes données que `ls -la | grep foo` : il cherche dans la représentation par défaut des objets, pas dans la sortie formatée. Traduire un tube demanderait de modéliser ce qui circule, c'est-à-dire de comprendre la commande. Refusé, et ce refus est définitif.
   - **La citation.** Les deux shells ont trois niveaux de citation et deux caractères d'échappement différents (`\` contre `` ` ``). Toute règle qui devrait *récrire* l'intérieur d'un argument cité sortirait du domaine où l'exactitude est vérifiable. Le dictionnaire ne touche donc jamais à l'intérieur d'un argument, sauf pour la syntaxe de variable, hors apostrophes simples.
   - **Les jokers.** En POSIX le shell développe `*.xml` **avant** de lancer le programme, et échoue ou passe le motif littéral s'il ne correspond à rien ; en PowerShell le motif est passé au cmdlet, qui le développe lui-même avec ses propres règles. Deux commandes qui se ressemblent, deux comportements différents sur les cas limites (aucun fichier, fichier commençant par un point, sensibilité à la casse). Refusés.
   - **Les codes de sortie.** `test -f x` répond 0 ou 1 ; `Test-Path x` affiche `True` et sort en 0. Une commande POSIX qui *répond par son code* n'a pas d'équivalent PowerShell qui réponde pareil. C'est d'autant plus sensible depuis ADR-029, et c'est pourquoi `test` est refusé nommément. À l'inverse, `which java` → `Get-Command java` **est** traduit alors que l'échec d'un `Get-Command` n'est pas un code de sortie non nul : c'est le seul endroit où la règle du code de sortie est assouplie, parce que la question posée (« où est ce programme ? ») et sa réponse sur stdout sont identiques, et parce que c'est une commande de découverte dont le modèle a besoin au premier tour. À réexaminer si un cas réel montre que le code importe.
2. **`cmd` n'a pas de dictionnaire, et son code de sortie n'est pas corrigé.** Le dialecte est détecté et annoncé, mais aucune table ne mène vers lui ni n'en part : une commande d'un autre dialecte passe telle quelle, sans même un motif (le dictionnaire n'a rien à en dire). `cmd /c` souffre par ailleurs du même défaut de code de sortie que PowerShell pour ses commandes internes, sans forme de contournement aussi sûre que `-EncodedCommand`. Un opérateur qui épingle `cmd` accepte ces deux limites ; elles sont documentées dans `config.toml`.
3. **La version de PowerShell n'est pas connue.** `powershell` est 5.1, mais `pwsh` est 6 ou 7, et leurs grammaires diffèrent (`&&`, `||`, les opérateurs de chaînage de pipeline). La distinguer demanderait de **lancer** l'interpréteur au démarrage pour lire `$PSVersionTable`, ce qui casserait la propriété « la détection n'appelle jamais rien » et ajouterait une latence et un mode d'échec au démarrage. Tant que ce n'est pas tranché, le dictionnaire vise le plus petit dénominateur.
4. **L'annonce n'est pas rejouée après coup.** Elle est rendue une fois, dans les instructions envoyées à l'initialisation de la conversation. Si l'opérateur change `[execution] shell` pendant qu'une session tourne, le modèle continue de croire ce qu'on lui a dit ; une rotation de conversation renvoie les instructions et corrige l'écart. Rien ne le signale explicitement.
5. **Ce que coûte la dérivation, et pourquoi elle ne coûte rien ici.** Un champ dérivé peut, par construction, être recalculé un jour à partir d'une configuration différente : changer `[execution] translate_commands`, ou changer de shell, et redemander le résultat d'une tâche donnerait une autre réponse. Cela ne peut pas se produire, pour quatre raisons qui tiennent ensemble et qu'il faut lire comme faisant partie de la décision.
   - **Un seul appelant, au moment même de l'exécution.** `ResultCollector.build` n'est appelé qu'à un endroit : le `PlanRunner`, à la fin du run qui vient d'exécuter les commandes, dans le même processus, quelques millisecondes après.
   - **Le même objet, pas la même configuration.** Le runner passe au collecteur **l'instance de `ShellTranslator` qu'il a lui-même consultée** pour décider quoi lancer, et non une relecture de `config.toml`. Même une modification du fichier en cours de session ne peut pas désynchroniser les deux.
   - **Un `execution_result` n'est jamais reconstruit.** Une fois bâti, il est persisté verbatim comme `MessageRecord` ; une rotation ré-enveloppe l'**objet** déjà construit (`ProtocolOrchestrator._build`), une reprise après crash rejoue la charge utile stockée (§7.5), et un plan interrompu n'a pas de résultat du tout (§8.4). Aucun chemin ne relit une `TaskRecord` pour refabriquer un résultat.
   - **Et la preuve ne dépend d'aucune dérivation.** Ce qui a réellement tourné est dans l'événement d'audit, écrit une fois, chaîné, jamais recalculé.

   Un changement de configuration n'atteint donc que les runs qui commencent après lui — la règle qui vaut déjà pour `verdict_programs` et pour toutes les clés d'`[execution]`.
6. **Le modèle n'apprend la traduction qu'après coup.** Comme pour la reconnaissance des verdicts (ADR-029, point ouvert 4), l'application ne valide pas les commandes d'un plan à l'avance et ne dit pas « cette tâche sera traduite » au moment de l'accepter. Le modèle l'apprend dans le résultat. Le lui dire plus tôt supposerait une réponse à l'acceptation d'un plan, qui n'existe pas dans le protocole.
