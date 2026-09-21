# ADR-032 — Un programme que le shell n'a pas pu lancer n'est jamais un verdict : 126 et 127, un `reason` qui le dit, PowerShell aligné

**Statut** : accepté (2026-09-21) — **précise** [ADR-029](ADR-029-echec-d-outil-comme-verdict.md) §2 (un verdict exige la preuve que le programme a tourné) et §4 (le `reason` d'un résultat de tâche, dérivé comme `execution` et `failure_is_verdict`), **clôt** son point ouvert 2 pour les codes qui appartiennent au shell, et ajoute `rustc` à sa liste par défaut ; **amende** [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) §2 (le script remis à PowerShell gagne un prologue et un épilogue explicite) ; **corrige** le contrat d'[ADR-031](ADR-031-contrat-du-modele-regles-dictionnaire-exemples.md), dont le tableau `execution` rangeait un programme introuvable sous `not_started` ; **sans effet sur le schéma persisté** (version 1, aucune colonne, aucune migration)

## Contexte

Le défaut n'a été trouvé ni par une relecture ni par un test unitaire, mais par la boucle réelle. `tests/integration/test_phase9_compile_feedback_loop.py` fait tourner l'application entière sur la configuration de démonstration — transport HTTP, `SubprocessCommandExecutor`, shell détecté, vrais compilateurs — face à un modèle double qui calcule chaque réponse à partir du message reçu. Son cas de contrôle « compilateur mal orthographié » (`gccc -c main.c -o main.o`) devait vérifier qu'un programme inconnu arrive au modèle comme le contrat le promet. Il a épinglé autre chose.

### 1. À travers un shell, un programme introuvable n'est pas une commande qui n'a pas démarré

Toute commande tourne sous la forme `<shell> -c <cmd>` (ADR-003, ADR-030 §2). Quand le **programme** n'existe pas, le **shell**, lui, démarre parfaitement : il cherche le programme, écrit son message sur stderr et sort en **127** — en **126** quand le fichier existe mais ne peut pas être exécuté. C'est la convention de POSIX.1 (*Shell Command Language*, 2.8.2 « Exit Status for Commands »), et bash comme dash la suivent :

```
$ bash -c '/opt/no-such-jdk/bin/javac Main.java'   →  bash: line 1: /opt/no-such-jdk/bin/javac: No such file or directory   (127)
$ bash -c 'gccc -c main.c'                         →  bash: line 1: gccc: command not found                             (127)
$ bash -c './javac'          (fichier en 0644)     →  bash: line 1: ./javac: Permission denied                          (126)
```

L'exécuteur voit donc un processus lancé, un pid, une sortie et un code : le résultat arrivait avec `execution: "ran"`, `exit_code: 127` et **aucun `reason`**. Aucun `FailureRecord` n'était écrit, et c'est juste : l'échec de lancement (`SPAWN_FAILED`) n'existe que lorsque le shell lui-même ne peut pas partir, ou que le répertoire de travail n'existe pas.

### 2. Pour un programme reconnu, c'était un verdict — faux

ADR-029 §2 fait du code non nul d'un programme de `[execution] verdict_programs` un **verdict** : le plan continue, et le résultat porte `failure_is_verdict: true`, la phrase qui dit au modèle « ceci est la réponse du compilateur, analyse-la ». Or la reconnaissance lit la ligne de commande, et `/opt/no-such-jdk/bin/javac Main.java` se lit `javac` ; `mvn clean install` sur une machine sans Maven se lit `mvn`. Les deux revenaient avec `failure_is_verdict: true` et le plan **continuait** — la lecture du dossier, les vérifications suivantes, bâties sur une compilation qui n'avait jamais eu lieu —, et le modèle recevait l'ordre d'analyser la réponse d'un compilateur qui n'avait jamais tourné. C'est le défaut silencieux qu'ADR-029 §1 a combattu ailleurs : une réponse plausible fondée sur rien.

### 3. Le contrat du modèle mentait

`PROTOCOL_INSTRUCTIONS.md` (le tableau `execution` de la section 2.3, reconduit par ADR-031) rangeait « programme inconnu, non exécutable, mauvais répertoire de travail » sous `not_started` avec `reason: SPAWN_FAILED`. Le raisonnement venait du troisième défaut du contexte d'ADR-029, pour qui « seuls `reason: "SPAWN_FAILED"` et `exit_code: null` » séparaient un binaire absent d'un verdict — vrai pour `create_subprocess_exec("mvn", …)`, faux dès qu'un shell s'intercale, c'est-à-dire toujours. Un modèle qui suivait le contrat attendait un `not_started` qui n'arrivait jamais, et lisait à la place un code 127 présenté comme la réponse d'un outil.

### 4. Sous PowerShell, c'était pire

Une commande inconnue y lève une `CommandNotFoundException`, écrit une erreur, et le processus sort en **1** — exactement le code d'une compilation qui échoue. Aucun code ne distinguait « javac a refusé le fichier » de « javac n'existe pas ». Et c'est au mieux : d'après la règle documentée de `-Command`, l'épilogue d'ADR-030, dernière instruction du script, pouvait même ramener ce code à 0 (§3).

### 5. Et `rustc` manquait

La famille Rust de la liste par défaut ne portait que `cargo`, alors que `gcc` et `javac` y figurent pour leurs langages. Le cas Rust du test de boucle devait ajouter `rustc` par surcharge, comme un opérateur l'aurait fait.

## Décision

### 1. Un verdict exige la preuve que le programme a tourné

**La règle.** Sous un shell POSIX, les codes **126** et **127** sont la réponse conventionnelle **du shell** — « trouvé mais pas exécutable », « introuvable » — et ne sont **jamais** un verdict, quel que soit le programme. Le plan suit alors ADR-009 **exactement** comme pour tout autre échec : il s'arrête (`task_failed:<id>`), sauf si le modèle a écrit `continue_on_error: true` (ou le défaut de plan d'ADR-029 §3) ; `critical` et `stop_plan_on_failure` produisent leurs libellés habituels. stderr, qui porte le message du shell, arrive au modèle comme toute sortie : la troncature d'ADR-029 §1 lui garantit la moitié du budget.

**La table, par dialecte** (`domain/shell.py`, `NOT_RUN_EXIT_CODES`) :

| Dialecte | `127` | `126` | `9009` |
|---|---|---|---|
| `posix` | `COMMAND_NOT_FOUND` | `COMMAND_NOT_EXECUTABLE` | réponse du programme |
| `powershell` | `COMMAND_NOT_FOUND` | `COMMAND_NOT_EXECUTABLE` | réponse du programme |
| `cmd` | réponse du programme | réponse du programme | `COMMAND_NOT_FOUND` |
| `unknown` | `COMMAND_NOT_FOUND` | `COMMAND_NOT_EXECUTABLE` | réponse du programme |

PowerShell y répond comme un shell POSIX parce que le script de §3 le lui fait dire ; `cmd` est traité au §4. Un interpréteur `unknown` est lu avec la convention POSIX : il est lancé comme un shell POSIX (`<shell> -c <cmd>`, ADR-030 §2), les exemples du contrat lui prêtent déjà l'orthographe POSIX (ADR-031 §5), et c'est le côté sûr d'une présomption — au pire un verdict est retenu, jamais un verdict n'est inventé.

**Un seul endroit, un seul dialecte.** `command_not_run_reason(dialect, exit_code)` est une fonction pure ; `VerdictPrograms.is_verdict(cmd, exit_code, *, timed_out, dialect)` la consulte et la prend en paramètre **obligatoire**, si bien qu'aucun appelant ne peut l'oublier. Le `PlanRunner` (condition d'arrêt) et le `ResultCollector` (champ `failure_is_verdict`) l'appellent avec le même dialecte : celui vers lequel est tourné le `ShellTranslator` du runner, c'est-à-dire le shell que l'exécuteur lance — l'argument d'ADR-030 §5 tient mot pour mot : un seul appelant, au moment même de l'exécution, avec l'objet même qui a servi à décider quoi lancer.

**Ce qui ne change pas.** La tâche reste `FAILED` et son code de sortie est rapporté tel quel. Aucun `FailureRecord`, aucune reprise, aucun tic de disjoncteur : ADR-008 §3 tient toujours — un code de sortie est une réponse, ici celle du shell — et ce n'est pas un `SPAWN_FAILED`, puisque le shell a démarré.

### 2. `execution` reste `ran`, et `reason` le dit

**Pourquoi pas `not_started`.** Le shell a tourné : il a un pid, une durée, une sortie, et cette sortie est la preuve dont le modèle a besoin — quel programme, quel chemin. ADR-029 §4 définit `not_started` comme « le seul cas où il n'y a rien à lire » ; ranger là un résultat dont stderr dit précisément ce qui manque mentirait dans l'autre sens. `not_started` garde donc son sens exact : le shell lui-même n'a pas pu démarrer, ou le répertoire de travail est invalide.

**Le `reason`.** Le résultat de tâche porte `reason: "COMMAND_NOT_FOUND"` pour 127 (9009 sous `cmd`) et `reason: "COMMAND_NOT_EXECUTABLE"` pour 126. Il est **dérivé à la construction du message**, du code de sortie et du dialecte, exactement comme `execution` et `failure_is_verdict` (ADR-029 §4) et `translation` (ADR-030 §4) : aucune colonne, la `TaskRecord` garde `reason = None`, et un `reason` stocké (`SPAWN_FAILED`, un défaut de l'exécuteur) n'est jamais remplacé.

**Une présomption, et pourquoi elle est acceptable.** 126 et 127 sont une **convention du shell**, pas une promesse du programme : un script d'emballage dont la dernière commande est introuvable, un programme qui sort en 127 de lui-même, portent le même code. Le `reason` est alors une présomption, et c'est pourquoi elle ne joue **que dans un sens** : elle rend l'application plus prudente — pas de verdict, le plan s'arrête, le modèle lit stderr — et jamais moins. Aucun code ne devient un verdict à cause de cet ADR. Le contrat le dit au modèle en toutes lettres : « `reason` is a presumption and `stderr` the evidence ».

### 3. PowerShell dit la même chose

Le script que reçoit PowerShell (toujours en Base64 d'UTF-16LE par `-EncodedCommand`, ADR-030 §2) se lit désormais, décodé :

```
trap [System.Management.Automation.CommandNotFoundException] { $global:LASTEXITCODE = 127 }; trap [System.Management.Automation.ApplicationFailedException] { $global:LASTEXITCODE = 126 }; trap [System.Management.Automation.PSSecurityException] { $global:LASTEXITCODE = 126 }
<la commande du modèle, mot pour mot>
$commandSucceeded = $?
if (Test-Path -LiteralPath variable:\LASTEXITCODE) { exit $LASTEXITCODE }
if (-not $commandSucceeded) { exit 1 }
```

**Le prologue** traduit en codes les erreurs par lesquelles PowerShell dit qu'il n'a pas pu lancer un programme, **par type d'exception et non par message** — la lecture ne dépend donc pas de la langue de la machine :

| Exception | Ce qu'elle signifie | Code |
|---|---|---|
| `CommandNotFoundException` | un nom ou un chemin que PowerShell ne résout pas | 127 |
| `ApplicationFailedException` | « Program '…' failed to run » : le fichier est trouvé mais le système refuse de le démarrer (pas une application valide, accès refusé, élévation requise) | 126 |
| `PSSecurityException` | la politique d'exécution interdit le script — le cas classique de `npm.ps1 cannot be loaded because running scripts is disabled on this system`, alors que `npm run` est un programme reconnu | 126 |

126 peut donc être distingué, et il l'est : deux types d'exception, sans ambiguïté avec l'échec d'un programme qui a tourné.

Chaque trap écrit son code dans `$global:LASTEXITCODE`, là où un programme natif écrit le sien, et ne se termine ni par `continue` ni par `break` : PowerShell écrit **son propre message** d'erreur sur stderr, puis reprend à l'instruction suivante, comme un shell POSIX continue après « command not found ». La règle d'ADR-030 (« le code rendu est celui du dernier programme natif du script ») s'étend naturellement : un programme que PowerShell n'a pas pu lancer compte comme un programme natif qui a répondu 127 ou 126. `mvnn -v; Get-ChildItem` sort en 127 ; `mvnn -v; javac -version` sort avec le code de `javac`.

**L'épilogue** garde les trois garanties d'ADR-030 : le vrai code natif propagé (`exit $LASTEXITCODE`), le transport par `-EncodedCommand` (inchangé), et le code de PowerShell lui-même pour une commande faite uniquement de cmdlets. Cette dernière est désormais **explicite**. D'après la règle documentée de `-Command` — le processus sort en 0 ou en 1 selon `$?` après la dernière instruction exécutée —, l'épilogue d'ADR-030, placé en dernier, répondait à la place de la commande : son `Test-Path` réussissait, et un `Get-Content absent.txt` risquait de sortir en 0. `$?` est maintenant lu sur la ligne qui suit immédiatement la commande, et le script sort en 1 quand il était faux ; une commande réussie sans programme natif se termine sans `exit`, donc en 0.

**Ce que cela coûte**, à lire comme faisant partie de la décision :

- **les positions** : PowerShell compte la ligne du prologue quand il situe une erreur, et une commande d'une ligne y est la ligne 2 ;
- **la taille** : l'enveloppe passe de 75 à 412 caractères ; avec le plafond Windows de 32 767 caractères et le Base64 d'UTF-16LE (×8/3), la plus longue commande lançable passe d'environ 12 180 à environ 11 840 caractères — au-delà, toujours le `SPAWN_FAILED` clair d'ADR-030 §2 ;
- **la portée des traps** : un trap du script attrape aussi ces erreurs quand elles sont levées, et non traitées, **dans une fonction ou un `.ps1`** que la commande appelle ; PowerShell reprend alors après l'instruction du script qui a fait l'appel, si bien que la fin de cette fonction ou de ce script est sautée là où, sans trap, elle se serait exécutée ;
- **un trap du modèle** déclaré pour les mêmes types entre en concurrence avec les nôtres.

**Non vérifié sur une vraie machine Windows.** `pwsh` n'est pas installé dans l'environnement de développement de ce dépôt, et aucune machine Windows ne fait partie des portes. La construction est prouvée comme ADR-030 l'a fait, par des fonctions pures : les tests décodent le Base64, relisent chaque trap (type et code), vérifient que la commande est intacte sur sa ligne entre le prologue et l'épilogue, que `$?` est lu juste après elle, et que les codes produits par le prologue sont exactement ceux que `NOT_RUN_EXIT_CODES` relit pour PowerShell. Le **comportement** — la portée des traps typés sur ces erreurs, la forme du message écrit sur stderr, la valeur de `$?` après une instruction rattrapée, sous Windows PowerShell 5.1 comme sous PowerShell 7 — reste à vérifier ; la liste de contrôle est au point ouvert 1.

### 4. `cmd` : 9009 est lu comme « introuvable », le lancement ne change pas

`cmd /c` sort en **9009** pour « '…' is not recognized as an internal or external command, operable program or batch file. » C'est son code à lui, et il est bien connu (« exited with code 9009 »).

**Décision : 9009 est lu comme 127, et seulement lu.** Sous le dialecte `cmd`, un code 9009 porte `reason: "COMMAND_NOT_FOUND"` et n'est jamais un verdict ; `exit_code` reste **9009** dans le résultat, parce que l'application rapporte ce que le processus a rendu, et le contrat rendu pour une machine `cmd` nomme 9009 (le texte de §5 est calculé depuis la même table). Trois raisons : 9009 est bien plus spécifique que 127 — aucun compilateur, aucun outil de construction ne le rend de lui-même —, la lecture ne joue que dans le sens prudent, et elle ne demande **rien** au lancement.

**Pourquoi pas de réécriture en 127.** Il faudrait envelopper la commande dans `cmd` (`… & if errorlevel 9009 exit /b 127`), ce qu'ADR-030 §2 a écarté pour de bonnes raisons : `%ERRORLEVEL%` est développé à l'analyse de toute la ligne, l'expansion retardée (`/v:on`) changerait le sens de chaque `!` de la commande du modèle, `&` la ferait réanalyser, et `cmd` n'a pas d'équivalent de `-EncodedCommand`. Lire le code est sûr ; le réécrire ne l'est pas.

**126 n'est pas traité sous `cmd`** : aucun code de `cmd` ne distingue de façon fiable « trouvé mais impossible à lancer » d'un échec ordinaire ; un tel code reste la réponse du programme (point ouvert 3).

### 5. Le contrat corrigé

`PROTOCOL_INSTRUCTIONS.md` est corrigé là où il mentait, et nulle part ailleurs :

- **le tableau `execution`** : `ran` dit que `exit_code` est la réponse **du programme** ; une ligne nouvelle, « `ran`, with `reason` `COMMAND_NOT_FOUND` or `COMMAND_NOT_EXECUTABLE` », dit que le shell a tourné mais n'a pas trouvé ou pas pu lancer le programme, que le code est celui du shell et **jamais un verdict**, et que stderr nomme le programme ; `not_started` ne couvre plus que le shell qui n'a pas pu démarrer et le répertoire de travail invalide (`SPAWN_FAILED`) — « Nothing reached your program » ;
- **le dictionnaire** : `exit_code` (la réponse du programme, ou celle du shell quand `reason` le dit), `failure_is_verdict` (« Never together with `reason` »), `reason` (les deux valeurs et leurs codes, `SPAWN_FAILED` réservé au shell qui ne démarre pas) ;
- **la section 5.1** : la règle des verdicts nomme l'exception, et un paragraphe la développe, présomption comprise, suivi d'un exemple complet — `N1`, un plan qui, en réponse à `A3`, cherche le JDK 21 là où il n'est pas installé, et `N2`, son résultat : `javac` y est un programme reconnu, et pourtant `t12` ne porte aucun `failure_is_verdict`, le plan est arrêté et `t13` sautée ; puis la leçon : corriger le nom ou le chemin, ou sonder avec `continue_on_error: true`.

Les codes cités sont **rendus pour la machine** depuis `NOT_RUN_EXIT_CODES` : 127 et 126 pour `posix`, `powershell` et `unknown`, 9009 pour `cmd`. Dans l'exemple, la commande vient de `EXAMPLE_COMMANDS` (clé `missing_compiler`) et la réponse du shell — code, message, durée — d'une table propre à l'adaptateur, écrite **indépendamment** de `NOT_RUN_EXIT_CODES` pour que le test prouve l'une par l'autre.

Les deux tests anti-dérive d'ADR-031 ont forcé le reste, et on les a laissés faire :

- **le rejeu** reconstruit `N2` avec le vrai `ResultCollector`, sous les quatorze rendus ; son aide ne recopie plus sur l'enregistrement un `reason` affiché à côté d'un code de sortie — il doit être **dérivé**, et c'est ce que la reconstruction prouve ;
- **un test nouveau** fait tourner `N1` par le vrai `PlanRunner`, sur un `FakeCommandExecutor` qui répond comme le shell de la machine, et compare l'`execution_result` entier à `N2`, arrêt du plan et tâche sautée compris ;
- les deux tests « deux machines » neutralisent la ligne qui est la réponse du shell. Le principe d'ADR-031 §5 — seules les chaînes de commande varient d'un dialecte à l'autre — gagne **exactement** cette exception : la réponse d'un shell à un programme qu'il ne peut pas lancer ne peut pas être la même d'un shell à l'autre.

Le texte rendu passe de 55 544 à 58 913 octets sur une machine POSIX, de 55 687 à 59 263 sur une machine PowerShell (58 172 pour `cmd`) : il reste sous la borne de 60 Kio (61 440 octets) d'ADR-031 §7, et à 14,8 % du budget de contexte par défaut.

### 6. `rustc` dans la liste par défaut

La famille Rust porte désormais `cargo` **et** `rustc`, comme `gcc` et `javac` figurent à côté de `make` et de `mvn` : un compilateur appelé directement rend un verdict autant que l'outil qui l'orchestre. Le commentaire de la famille dans `config.toml` le dit, et le test de parité fichier / code le vérifie. La surcharge du cas Rust du test de boucle disparaît ; ce test vérifie maintenant que la liste par défaut reconnaît chacun de ses compilateurs.

### 7. Le point ouvert 2 d'ADR-029 est clos

ADR-029 laissait ouverte la distinction entre « échoue parce qu'il a trouvé quelque chose » (code 1) et « échoue parce qu'il n'a pas pu tourner » (code 2, 4, **127**…). La question est tranchée pour les codes qui **appartiennent au shell** : 126 et 127 (9009 sous `cmd`) ne sont jamais un verdict. Les codes qui appartiennent à l'**outil** — 2 pour l'erreur d'usage de bien des outils, 5 pour pytest quand il ne collecte aucun test, 128 + n pour un signal — restent des verdicts, délibérément : chaque outil a sa convention, et les interpréter demanderait une table par outil que rien ne justifie aujourd'hui (point ouvert 4).

## Conséquences

- **Code** : `domain/shell.py` (`COMMAND_NOT_FOUND`, `COMMAND_NOT_EXECUTABLE`, `NOT_RUN_EXIT_CODES`, `command_not_run_reason`) ; `domain/commands.py` (`VerdictPrograms.is_verdict(…, dialect=…)`, paramètre obligatoire) ; `execution/result_collector.py` (propriété `dialect`, `reason` dérivé) ; `execution/plan_runner.py` (propriété `shell_dialect`, lue sur le traducteur au moment de décider ; `_failure_is_verdict`) ; `execution/platform.py` (`POWERSHELL_NOT_RUN_TRAPS`, `POWERSHELL_NOT_RUN_PROLOGUE`, `POWERSHELL_EXIT_CODE_EPILOGUE` réécrit, `powershell_script`) ; `protocol/adapter.py` (la règle des verdicts, `EXAMPLE_COMMANDS["missing_compiler"]`, la réponse d'exemple de chaque shell, les valeurs rendues `not_run_codes` et `example_not_run_result`) ; `protocol/PROTOCOL_INSTRUCTIONS.md` ; `config.py` (`rustc`).
- **Configuration** : aucune clé nouvelle. `rustc` entre dans `[execution] verdict_programs` par défaut, dans `config.toml` comme dans le code ; le commentaire du fichier dit aussi qu'un programme que le shell n'a pas trouvé n'a pas tourné.
- **Protocole** : extension **additive** dans le sens application → modèle : `reason` prend deux valeurs de plus, sur un résultat que le modèle lit et n'écrit jamais ; `execution` et ses quatre valeurs sont inchangés. Le seul changement de comportement est celui qui est voulu : un programme reconnu que le shell n'a pas pu lancer ne porte plus `failure_is_verdict` et ne fait plus continuer le plan. Rien de ce que le modèle envoie ne change.
- **Persistance** : **aucun changement**. Schéma en version 1, aucune colonne, aucune table, aucune migration ; le `reason` est dérivé, la `TaskRecord` garde `reason = None`, et toute base existante se relit comme avant.
- **Ce qui n'est explicitement pas changé** : la taxonomie d'ADR-008 (126 et 127 ne produisent ni `NormalizedError`, ni `FailureRecord`, ni reprise, ni tic de disjoncteur) ; `SPAWN_FAILED` et son `FailureRecord` ; la règle effective d'ADR-009 §2 et le saut des dépendants de §5 ; la troncature d'ADR-029 §1 ; le lancement verbatim des shells POSIX et de `cmd /c` ; le transport `-EncodedCommand` ; le dictionnaire entre dialectes ; l'ordre d'ADR-017.
- **Tests** : phase 0 (`rustc` et les compilateurs appelés directement dans la liste par défaut) ; phase 2 (le contrat : `N1`/`N2` rejoués, reconstruits par le collecteur et joués par le vrai runner sous les quatorze rendus ; les codes nommés, exactement ceux du shell de la machine ; les deux tests « deux machines ») ; phase 4 (la table par dialecte, codes des autres dialectes compris ; le verdict retenu ; le `reason` dérivé dans chaque dialecte, jamais écrit sur l'enregistrement ; un `reason` stocké jamais remplacé ; le collecteur par défaut lu en POSIX ; le prologue PowerShell décodé, ses traps et leurs codes relus par le domaine, l'ordre de l'épilogue ; avec le vrai shell : 127 pour un nom et pour un chemin, 126 pour un fichier en 0644) ; phase 5 (le runner qui s'arrête dans chaque dialecte, `continue_on_error` qui le fait continuer, 127 sous `cmd` qui reste un verdict) ; batterie de conformité (`licit-build-tool-not-found-is-no-verdict`, rapport régénéré : 124 cas) ; phase 9, la boucle de compilation (le cas `gccc` porte `reason: "COMMAND_NOT_FOUND"` ; un cas nouveau, `/opt/no-such-jdk/bin/javac Main.java` : aucun verdict, plan arrêté, `reason`, message du shell parvenu au modèle, et le modèle qui se rattrape avec le `javac` du `PATH` ; le cas Rust sans surcharge). **Mutations** : retirer la règle de `is_verdict` fait échouer le cas `/opt/no-such-jdk` de la boucle — le plan repart, la liste tourne — et 24 tests du contrat ; ajouter un code erroné à la table de `cmd` en fait échouer trois.
- **Documentation** : `PROTOCOL_INSTRUCTIONS.md` (§2.3, §5.1), `config.toml`, `docs/architecture/02-protocol.md`, `03-execution-model.md` et `09-module-map.md`, le `README.md`, les lignes de statut d'ADR-029 et d'ADR-030, `docs/adr/README.md`.

## Points ouverts

1. **La vérification sur Windows.** À faire sur la machine de développement principale, sous Windows PowerShell 5.1 et sous PowerShell 7, par `agentic-app` ou directement avec le script décodé : (a) `nonexistent-xyz` sort en 127, le message de PowerShell sur stderr ; (b) un `.ps1` bloqué par la politique d'exécution sort en 126 ; (c) un `.exe` qui n'est pas une application valide sort en 126 ; (d) `Get-Content absent.txt` sort en 1 et `Get-ChildItem` en 0 ; (e) `mvnn -v; javac -version` sort avec le code de `javac` ; (f) sous `cmd`, `cmd /c nonexistent` sort en 9009. Le jour où un exécuteur Windows rejoint les portes, ces six cas deviennent des tests `real_subprocess`.
2. **La présomption.** Un programme qui sort de lui-même en 126 ou 127 — le plus souvent un script d'emballage dont la dernière commande est introuvable — perd son verdict et arrête le plan. C'est accepté parce que le `reason` reste vrai au sens large (une commande n'a pas été trouvée) et que stderr nomme laquelle ; si un cas réel montre un outil qui utilise ces codes pour un vrai verdict, il demandera sa propre décision — une exception nommée dans la configuration, par exemple —, pas un assouplissement de la règle.
3. **`cmd` et 126.** Aucun code fiable ne dit « trouvé mais impossible à lancer » : un tel échec reste lu comme la réponse du programme, verdict compris s'il est reconnu. Un opérateur qui épingle `cmd` accepte cette limite, à côté de celles qu'ADR-030 documente déjà.
4. **Les codes propres aux outils.** 2 (erreur d'usage), 5 (pytest : aucun test collecté), 128 + n (un compilateur tué par un signal, 139 pour `SIGSEGV`) restent des verdicts. Les distinguer demanderait une table par outil ; tant qu'aucun cas réel ne l'exige, la règle reste celle qu'ADR-029 sait expliquer au modèle.
5. **La portée des traps PowerShell** (§3) : une erreur de ce type non traitée dans une fonction ou un script appelé interrompt la fin de cette fonction ou de ce script. Aucune voie simple ne l'évite : un trap placé plus haut ne sait pas reprendre à l'intérieur de la fonction appelée, et renoncer aux traps pour une lecture après coup de `$Error` perdrait l'ordre entre les programmes qui ont tourné et ceux qui n'ont pas pu — celui qui fait le code rendu. Tant qu'aucun cas réel ne le demande, la portée est documentée plutôt que contournée.
