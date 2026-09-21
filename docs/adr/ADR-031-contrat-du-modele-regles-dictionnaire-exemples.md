# ADR-031 — Le contrat du modèle : les règles d'abord et en dernier, un dictionnaire des champs, un échange complet dans le dialecte de la machine, des exemples prouvés par le code

**Statut** : accepté (2026-09-21) — **précise** [ADR-004](ADR-004-contrat-de-transport.md) (le texte des instructions, « seul contenu *prompt* de l'application ») et réorganise, sans en retirer une règle ni une valeur rendue, ce qu'y avaient ajouté [ADR-005](ADR-005-resume-de-contexte-par-le-modele.md), [ADR-007](ADR-007-amendements-machines-a-etats.md), [ADR-009](ADR-009-drapeaux-d-arret.md), [ADR-010](ADR-010-limites-de-payload.md), [ADR-011](ADR-011-troncature-et-chunks.md), [ADR-014](ADR-014-continuation-apres-rotation.md), [ADR-022](ADR-022-reponse-utilisateur.md), [ADR-023](ADR-023-politique-de-correction.md), [ADR-029](ADR-029-echec-d-outil-comme-verdict.md) et [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) ; **tranche** les deux constats « à surveiller » du rapport de conformité (la validation laxiste des types) ; **corrige** le choix de l'exemple d'une correction, tel qu'ADR-023 §1 l'avait décidé ; **sans effet sur le schéma persisté** (version 1, aucune colonne, aucune migration)

## Contexte

Le texte que reçoit le modèle à l'ouverture de chaque conversation — `protocol/PROTOCOL_INSTRUCTIONS.md`, rendu par `render_instructions` — est tout le contrat entre l'application et lui. Il n'y a pas d'autre canal : ce que le texte ne dit pas, le modèle le devine, et ce qu'il devine mal est refusé.

Ce texte a grandi ADR après ADR, chacun ajoutant sa section. Il en résultait onze sections et 38 Ko rendus, dont les règles étaient dispersées dans la prose, et des exemples partiels. Le propriétaire demande quatre choses : que le protocole soit expliqué **champ par champ** et la façon de répondre **exactement** ; qu'un **échange complet**, découverte comprise, montre le protocole en action ; que **chaque champ** ait une entrée de dictionnaire ou un exemple (quoi envoyer, quel type JSON, quelles valeurs) ; que le texte **insiste sans ambiguïté** : le protocole se suit exactement, sans modification ni écart.

La relecture du texte existant contre le code a montré que l'insistance seule ne suffisait pas : le texte et le code divergeaient par endroits, sans qu'aucun test ne le voie.

- **Des exemples inexacts.** L'arbre de la grammaire omettait `final_answer` après le premier `execution_result`, que la table d'ADR-007 accepte (et que la batterie vérifie : `licit-final-answer-after-discovery`). Une tâche sautée portait la raison `task_failed:t6`, alors que le runner écrit `plan_stopped:task_failed:t6`. Le texte annonçait `exit_code: null`, alors que l'application, qui sérialise avec `exclude_none`, **omet** le champ. `state_summary` « accepte des clés supplémentaires », mais la rotation n'en copie que quatre (`STATE_SUMMARY_SECTIONS`) : les autres disparaissent sans bruit.
- **Des commandes dans le mauvais dialecte.** ADR-030 annonce au modèle le shell de la machine, puis tous les exemples du texte étaient en POSIX (`uname -a && echo $SHELL`, `grep`, `sed -n`, `tail`) — y compris sur la machine de développement principale, qui est en PowerShell. Un modèle recopie un exemple avant de lire une règle.
- **Une frontière plus floue que le texte.** Les contenus étaient validés en mode laxiste : `"2048"` pour un entier et `"yes"` pour un booléen étaient acceptés puis **convertis** en silence. Le rapport de conformité le signalait depuis sa création (`task-values-coerced-from-strings`, `user-response-expects-reply-coerced`), avec un arbitrage laissé ouvert : « soit valider en mode strict, soit documenter la tolérance dans `PROTOCOL_INSTRUCTIONS.md` ». Un contrat qui dit « un nombre est un nombre » ne peut pas coexister avec un adaptateur qui lit `"2048"` comme 2048.
- **Des tests qui ne voyaient pas la dérive.** Les exemples étaient validés contre leur modèle de contenu, chacun isolément : jamais dans la situation où le texte les place (après quel message, avec quels identifiants déjà pris), jamais contre les composants qui produisent réellement les messages de l'application. Et rien ne reliait les listes de champs de la prose aux modèles pydantic.

## Décision

### 1. Un contrat qui se lit de haut en bas

| § | Contenu |
|---|---|
| 1 | **Le contrat** : dix règles numérotées, une ligne chacune ; ce qui arrive quand l'une est enfreinte ; la table « ce que vous pouvez envoyer, et quand » |
| 2 | **Messages et champs** : l'enveloppe, puis une table par type envoyé (`discovery_plan` / `execution_plan` / `priority_clarification`, tâche, `state_summary`, `final_answer`, `user_response`, `context_resume_ack`) et par type reçu (`user_request`, `session_budget`, `execution_result`, résultat de tâche, référence de tâche, `translation`, `context_resume_request`, `context_summary`, `protocol_correction_request`, codes d'erreur) |
| 3 | **Un échange complet**, message par message (§5 ci-dessous) |
| 4 | **Les erreurs courantes** : douze messages faux, chacun avec le code qu'il reçoit et la forme juste |
| 5 à 11 | Tout ce que le texte fixait déjà, resserré : règles d'arrêt et défauts (dont la règle de verdict d'ADR-029), dépendances, `priority_clarification`, identifiants ; la machine (annonce d'ADR-030, dictionnaire de traduction) ; limites, troncature et `chunk_request` ; `state_summary` et rotation ; conclure (`final_answer` ou `user_response`) ; la correction ; le budget de session |
| 12 | **Avant chaque message** : les dix règles, de nouveau, en liste de contrôle |

Le texte reste un gabarit rendu depuis la configuration ; toutes les valeurs rendues auparavant le sont encore (limites d'octets et de délais, types de la première réponse, politique de correction, programmes reconnus, environnement, règle de traduction), et quelques-unes s'y ajoutent (§5).

### 2. Les règles d'abord, et les mêmes en dernier

Les dix règles sont écrites en termes RFC 2119 (MUST / MUST NOT), une par ligne : un seul message par réponse, une seule enveloppe JSON et rien d'autre ; les noms de champs exacts ; aucun champ non listé (sauf dans le `content` d'un `final_answer`, seule exception) ; les valeurs énumérées exactes, casse comprise ; les types JSON exacts (un entier sans guillemets, un booléen `true` ou `false`, une liste même d'un élément) ; le `conversation_id` du dernier message reçu ; des `message_id`, `plan_id` et `task_id` neufs pour toute la session ; seulement un type permis à cet instant ; jamais un type que seule l'application envoie ; aucune prose hors de l'enveloppe.

Juste après, le texte dit ce qui arrive : le message est **refusé**, rien n'en est exécuté ni montré à l'utilisateur, un `protocol_correction_request` suit, et au-delà de **N** refus consécutifs la session échoue — N est la valeur rendue de `protocol.max_correction_attempts`, et `0` rend la phrase « the first refused reply ends the session ».

Les mêmes dix points reviennent **à la fin** (§12), formulés en questions à se poser avant d'envoyer. Un texte de 55 Ko est lu inégalement : son début et sa fin retiennent l'attention mieux que son milieu. Les règles sont ce qui doit survivre à cette perte ; elles sont donc aux deux endroits, dans le même ordre, et un test vérifie qu'elles sont dix de part et d'autre.

La règle 1 ne contredit aucun codec d'ADR-021 : avec `tool_call`, l'enveloppe est l'argument d'**un** appel de l'outil, ce que la règle dit explicitement ; avec `json_text`, une prose autour du JSON est tolérée par le codec — le texte l'interdit quand même, et dit honnêtement ce qui arrive selon la lecture (refus `UNPARSEABLE_REPLY` ou `SCHEMA_INVALID`, ou prose jetée sans être lue).

### 3. Le dictionnaire des champs

Chaque type que le modèle **envoie** a sa table : champ, type JSON, obligatoire ou non (avec le défaut), valeurs permises ou contraintes, exemple de valeur, signification. Chaque type qu'il **reçoit** a une table plus compacte (champ, type, signification) — il les lit, il ne les construit pas — mais chaque champ qu'il peut rencontrer y figure, y compris les clés de `context_summary` et la table des codes d'erreur d'une correction, qui remplace l'ancienne section « What gets rejected ».

Le dictionnaire corrige au passage les inexactitudes relevées : `exit_code` **absent** quand la commande n'a pas fini, raisons réelles des tâches sautées (`plan_stopped:<stop_reason>`, `dependency_failed:<id>`, `budget_exceeded`, `user_interrupt`), clés de `state_summary` limitées aux quatre que la rotation transporte (la permission d'ajouter des clés est retirée : elles étaient acceptées mais perdues à la première rotation), `CHUNK_REF_UNKNOWN` (refus du message) distingué de `CHUNK_REF_NOT_FOUND` (échec de la tâche).

### 4. Un nombre est un nombre : la frontière devient stricte sur les types

`ProtocolAdapter._validate_content` valide désormais chaque contenu **deux fois** :

1. la validation pydantic par défaut, dont les erreurs gardent leur libellé habituel ;
2. le même contenu **en JSON et en mode strict** (`model_validate_json(canonical_json(content), strict=True)`). Sur tout ce que la première passe accepte, la seconde ne peut objecter qu'à une valeur que la première aurait **convertie** : un entier écrit `"600000"` ou `600000.0`, un booléen écrit `"true"`, `"yes"` ou `1`.

Les deux passes rapportent ensemble — la seconde n'ajoute que les champs que la première n'a pas déjà nommés — pour qu'une seule correction liste toutes les fautes. Le mode strict est appliqué au JSON et non aux objets Python parce qu'en mode Python strict une énumération n'accepte que ses instances : `"sequential"` y serait refusé.

C'est la réponse à l'arbitrage laissé ouvert par le rapport de conformité, dans le sens de la demande du propriétaire. Les deux cas deviennent des refus (`task-values-sent-as-strings`, `user-response-expects-reply-as-string`) et le rapport ne compte plus aucun constat « à surveiller ». L'enveloppe n'est pas concernée : ses champs sont des chaînes et une énumération, que la validation par défaut ne convertit pas. Les messages déjà persistés ne sont pas revalidés par cette voie : `ConversationManager.user_responses` relit les `user_response` stockés avec la validation par défaut, donc une base existante se relit comme avant.

### 5. Un échange complet, rendu dans le dialecte de la machine

La section 3 est une session réelle, chaque message une enveloppe entière, chacun précédé d'une ligne qui dit pourquoi c'est le bon message à ce moment :

- **A** — `user_request` → `discovery_plan` (trois lectures indépendantes en `parallel`) → son `execution_result` → un `execution_plan` (un build Maven et la lecture du `pom.xml` qu'il met en cause, avec `default_continue_on_error`) → son `execution_result`, où le build échoue avec `failure_is_verdict: true` et une sortie tronquée dont la plage est expliquée au §7 → `final_answer` ;
- **B** — une demande qui n'appelle aucune commande, en suivi, et son `user_response` ;
- **C** — un message refusé (`expects_reply: "yes"`), le `protocol_correction_request` qu'il reçoit, et le message corrigé.

D'autres blocs montrent les messages hors de ce fil, chacun dans la section qui en parle et **en situation** : `P1` (une `priority_clarification` à la place de `A4`), `K1` (un `chunk_request` qui relit le début du log de `A5`), `R1` / `R2` (la rotation qui aurait remplacé `A5`, et l'accusé de reprise), et les douze erreurs `M1` à `M12`.

Chaque bloc porte son **étiquette dans sa ligne d'ouverture** : `` ```json A4 ``, `` ```json M5 after A5 refused DUPLICATE_MESSAGE_ID ``, `` ```json fragment translation ``. Le modèle la lit comme une légende ; le test la lit comme la situation du bloc.

**Les commandes des exemples sont rendues dans le dialecte annoncé** (ADR-030 §3). `adapter.EXAMPLE_COMMANDS` donne, par dialecte, les trois commandes qui varient : lister le projet (`ls -A` / `Get-ChildItem -Force -Name` / `dir /a /b`), lire les réglages du compilateur (`grep` / `(Get-Content pom.xml) -match` / `findstr`), lire `JAVA_HOME` (`echo "$JAVA_HOME"` / `$env:JAVA_HOME` / `echo %JAVA_HOME%`). Les autres commandes (`java -version`, `mvn -version`, `mvn -B clean install`) s'écrivent pareil partout. **La variation se limite aux chaînes de commande** : elles sont choisies pour que leur sortie soit identique d'un dialecte à l'autre (des noms seuls, des lignes seules, une valeur), si bien que les résultats des exemples ne changent pas. Un dialecte `unknown` reçoit l'orthographe POSIX, la plus courante parmi les interpréteurs non reconnus. L'exemple de l'objet `translation` est calculé par le dictionnaire d'ADR-030 lui-même, dans le sens qu'emprunterait cette machine.

Trois autres valeurs des exemples viennent de la configuration, pour qu'aucun exemple ne contredise le déploiement : les délais et budgets appliqués (`timeout_ms_applied`, `max_output_bytes_applied` des tâches qui n'en déclarent pas), la présence de `failure_is_verdict` sur le build (seulement si `mvn` est dans `execution.verdict_programs`), et `max_attempts` de la correction (la valeur configurée, ou le défaut quand la politique est éteinte — la ligne qui introduit l'exemple le dit alors).

### 6. Deux tests qui rendent la dérive impossible

`tests/unit/test_phase2_protocol_contract.py` porte les deux garanties.

**Le rejeu des exemples.** Chaque bloc du texte **rendu** — pour un shell POSIX, PowerShell, `cmd` et inconnu, sous six configurations — est rejoué dans l'état que le texte lui donne (`after <étiquette>`, sinon le bloc précédent de la même série) : un message que le modèle envoie passe par le vrai `ProtocolAdapter.parse_inbound` et doit être **accepté** ; un message marqué `refused CODE` doit être refusé **avec ce code-là**, et le texte qui le précède doit nommer ce code ; un message que le modèle reçoit doit valider contre son modèle **sous la forme exacte que l'application sérialise**. Puis les messages de l'application sont **reconstruits par les vrais composants** et comparés au texte : les résultats `A3` et `A5` par le `ResultCollector`, après la vraie troncature (`PayloadGuard`) et la vraie projection du plan (`plan_to_records`) ; le résumé de `R1` par le `ContextReducer` sur un magasin en mémoire ; la correction `C3` par `build_protocol_correction_request` (le `reminder`, abrégé dans le texte, doit être un préfixe du vrai). La prose enveloppée de `M3` est lue par les trois codecs, et chacun doit faire ce que le texte dit.

**Le dictionnaire contre les modèles.** Chaque table de la section 2 doit lister **exactement** les champs de son modèle pydantic, avec le bon type JSON, et pour les messages envoyés le caractère obligatoire, le défaut et les valeurs permises des énumérations. La table de `context_summary` doit lister les clés qu'écrit le réducteur, et celle des codes d'erreur tous les codes que l'adaptateur peut rendre. Ajouter un champ à un modèle, changer un défaut, renommer une clé du résumé : un test échoue tant que le contrat n'a pas suivi.

Onze mutations, appliquées une à une pendant l'écriture (seconde passe stricte retirée, champ inventé dans un exemple, champ ajouté à `TaskMessage`, code de refus erroné, budget appliqué faux, clé du résumé renommée, identifiant réutilisé, champ de verdict supprimé, défaut modifié dans un modèle, règle retirée de la liste finale, commande POSIX dans la table PowerShell), ont toutes été détectées.

**Ce que les tests ont trouvé en écrivant le texte.** L'exemple d'une correction n'était jamais celui du type tenté. ADR-023 §1 décide que l'exemple montre « le type que le modèle a tenté quand ce type est parmi les attendus » ; le code ne lisait ce type que dans `details["received"]`, qui n'existe que pour `UNEXPECTED_MESSAGE_TYPE` — le seul cas où le type tenté n'est, par définition, **pas** attendu. La branche ne pouvait donc jamais s'exécuter, et un `user_response` mal formé recevait en exemple un `discovery_plan` portant `uname -a`, sur n'importe quelle machine. `_correction_example` lit désormais aussi `details["message_type"]`, que porte tout refus de schéma au stade du contenu ; l'exemple de `C3` est celui que l'application envoie réellement.

### 7. La taille, et pourquoi elle compte deux fois

Les instructions entrent dans `context_bytes` (ADR-013) et sont **renvoyées à chaque rotation** : chaque octet se paie une fois par conversation. Le texte rendu passe de **38 391** à **55 544 octets** sur une machine POSIX et de 38 465 à 55 687 sur une machine PowerShell (54 860 pour `cmd`), soit **13,9 %** du budget par défaut de 400 000 octets, contre 9,6 %. Le seuil `WARNING` (70 %, 280 000 octets) reste cinq fois plus loin que le texte lui-même.

La borne retenue est **60 Kio (61 440 octets)** pour chaque dialecte sous la configuration par défaut ; un test la vérifie, un autre qu'une conversation qui s'ouvre sous la configuration par défaut est `HEALTHY` (évaluation du moniteur, et de bout en bout sur l'orchestrateur réel). Pour tenir dans cette borne, la prose a été coupée avant les exemples et les règles : les tables remplacent les paragraphes, les objets JSON des résultats tiennent sur une ligne, les erreurs courantes sont des messages d'une ligne.

### 8. Pourquoi le texte reste en anglais

Le texte est lu par le modèle, pas par l'équipe ; le vocabulaire du protocole — types, champs, valeurs, codes d'erreur — est anglais, et un contrat qui exige ces mots à l'identique les montre dans leur langue, sans traduction intermédiaire à réconcilier. Le texte existant était anglais, les exemples de la spécification (§12) aussi. Les ADR, le rapport de conformité et la documentation restent en français.

### 9. `agentic-app protocol show`

La commande, voisine de `transport show`, `codec show` et `shell show`, imprime le texte **exactement** tel que la configuration effective et l'environnement détecté le rendent (`agentic-app protocol show > contrat.md` donne l'octet près). `--dialect posix|powershell|cmd` le rend comme si `[execution] shell` épinglait l'interpréteur de ce dialecte (`bash`, `pwsh`, `cmd`) ; `--json` ajoute l'environnement, la taille en octets, le budget de contexte et la part qu'en prend le texte.

## Conséquences

- **Code** : `protocol/PROTOCOL_INSTRUCTIONS.md` (réécrit) ; `protocol/adapter.py` (`EXAMPLE_COMMANDS`, l'exemple `translation` calculé par le dictionnaire, les valeurs d'exemple rendues — commandes, verdict, `max_attempts`, introduction de la correction —, les textes rendus de la politique de refus et de correction, la double validation de `_validate_content`, `_correction_example` qui lit `message_type`) ; `interfaces/cli.py` (`protocol show`, `ContractDialect`).
- **Configuration** : aucune clé nouvelle.
- **Protocole** : un seul changement de comportement, voulu : un entier ou un booléen écrit sous une autre forme JSON est **refusé** (`SCHEMA_INVALID`, `int_type` / `bool_type`) au lieu d'être converti. Avec la politique de correction par défaut, le modèle reçoit le champ fautif et se corrige ; avec `max_correction_attempts = 0`, la session échoue comme pour toute autre faute. Aucun type de message, aucun champ, aucune ligne de la table d'ADR-007 ne change. L'exemple d'une correction devient, pour une faute de schéma, celui du type tenté.
- **Persistance** : **aucun changement**. Schéma en version 1, aucune colonne, aucune table, aucune migration ; les messages déjà stockés se relisent comme avant.
- **Tests** : `tests/unit/test_phase2_protocol_contract.py` (nouveau : le rejeu sur quatorze rendus, la reconstruction des résultats, du résumé et de la correction, le dictionnaire, les règles d'abord et en dernier, la politique rendue, le dialecte, la taille, la fenêtre `HEALTHY`) ; `tests/unit/test_phase2_protocol.py` (les tests qui épinglaient l'ancien texte épinglent le nouveau sans rien perdre de ce qu'ils gardaient : l'annonce est lue en §6, deux machines ne diffèrent que par l'annonce **et** les commandes rendues, les lignes de la table portent leur nouveau libellé, les exemples marqués `refused` sont écartés des tests qui valident les exemples acceptés — ils sont prouvés refusés ailleurs — ; deux tests nouveaux pour le choix de l'exemple d'une correction) ; `tests/conformance/test_conformance_plan_and_values.py` (les deux cas de coercition deviennent des refus, rapport régénéré) ; `tests/integration/test_phase9_orchestration.py` (la session qui s'ouvre `HEALTHY` avec le contrat complet) ; `tests/integration/test_phase9_cli.py` (`protocol show`, ses options et ses erreurs).
- **Documentation** : `docs/architecture/02-protocol.md` (§7, les sections du texte), `docs/reports/conformance-protocole.md` (régénéré : 123 cas, 123 conformes), la ligne de cet ADR dans `docs/adr/README.md`.

## Points ouverts

1. **L'exemple d'une correction pour un plan reste en POSIX.** Les exemples minimaux d'ADR-023 (`uname -a`, `echo $JAVA_HOME`, `mvn -version`) sont écrits une fois, dans l'adaptateur, qui ne connaît pas le dialecte de la machine. Le texte du contrat n'en montre plus aucun (l'exemple de `C3` est un `user_response`), mais un modèle sur PowerShell qui rate la forme d'un plan reçoit encore un exemple POSIX. Deux voies : passer l'environnement à l'adaptateur, ou choisir des commandes neutres. À trancher avec ADR-023.
2. **`final_answer.status` n'est pas validé.** Le contrat en donne trois valeurs, mais le contenu d'un `final_answer` est ouvert (§12.7) et `status` y est une chaîne libre : `"COMPLETED"` passe. Le fermer toucherait à la lecture du §12.7 et mérite son propre ADR ; en attendant, le texte ne prétend pas qu'un tel message soit refusé, et l'erreur courante `M1` illustre la casse sur un `user_response`, dont le statut est bien validé.
3. **La prose autour du JSON dépend du codec.** `json_text` la tolère et la jette ; `tool_call` et `passthrough` refusent. Le texte l'interdit partout et décrit les deux issues plutôt que de promettre un refus qu'un déploiement ne ferait pas.
4. **La taille relative dépend du budget.** 55 Ko font 14 % du budget par défaut, mais 55 % d'un budget de 100 000 octets configuré pour un modèle à petite fenêtre. Une variante compacte (sans la section des erreurs courantes, par exemple) reste possible si un tel profil apparaît ; elle devrait passer les mêmes deux tests.
5. **Le rejeu couvre les configurations qu'il énumère.** Une configuration extrême pourrait rendre un exemple inexact sans qu'un test le voie : un `hard_max_output_bytes` inférieur à 1 024 ramènerait le budget appliqué de `t4` sous ce que montre `A5`. Les six configurations rejouées (défaut, première réponse stricte, corrections éteintes, verdicts éteints, traduction éteinte, limites personnalisées) sont celles que la suite connaît.
6. **Les identifiants du modèle.** Le contrat recommande un préfixe propre (`model-001`) pour ne jamais croiser ceux de l'application (`msg-<uuid>`) ; rien ne l'impose, et une collision reste un `DUPLICATE_MESSAGE_ID` ordinaire.
