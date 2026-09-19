# ADR-003 — Plateformes cibles : Windows + Linux/macOS

**Statut** : accepté (2026-09-18) — amendé par [ADR-019](ADR-019-consolidation-vague-1.md) ; §3 **amendé par** [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) sur deux points : l'environnement d'exécution (OS, dialecte du shell, `cwd`) est désormais **annoncé** au modèle dans les instructions, et une commande peut être **réécrite** dans un cas étroit, borné et tracé (dictionnaire entre dialectes) ; le reste de §3 — le modèle émet les commandes, le `discovery_plan` lui apprend tout le reste — est inchangé

## Contexte

La spec est écrite en vocabulaire POSIX : SIGTERM (§2.4, §8.4), `uname -a && echo $SHELL` dans le `discovery_plan` d'exemple (§12.2). Elle affirme aussi que « l'application n'injecte aucune configuration d'environnement, le modèle la découvre » (§17.5). Or la machine de développement et d'exploitation principale est sous **Windows**, et pour lancer une commande il faut bien choisir un interpréteur et un répertoire de travail : l'application possède donc, de fait, deux réglages d'environnement.

## Décision

1. **Cibles** : Windows (x64) et POSIX (Linux, macOS). La CI exécute la suite sur `ubuntu-latest` et `windows-latest`.
2. **Couche plateforme** dans `execution/platform.py`, sélectionnée automatiquement (`sys.platform`) et injectable en test :

| | POSIX | Windows |
|---|---|---|
| Lancement | `asyncio.create_subprocess_shell(cmd, executable=<shell>)` dans un nouveau groupe de processus (`start_new_session=True`) | `create_subprocess_shell` avec `creationflags=CREATE_NEW_PROCESS_GROUP` ; interpréteur `powershell.exe -NoProfile -Command` par défaut, `cmd.exe /c` en option — le lancement PowerShell est remplacé par `-EncodedCommand` et un épilogue `exit $LASTEXITCODE` par [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) §2 |
| Annulation (stop condition, interruption) | `SIGTERM` au groupe, attente `drain_timeout_ms`, puis `SIGKILL` au groupe | `CTRL_BREAK_EVENT` au groupe, attente `drain_timeout_ms`, puis `TerminateProcess` (+ `taskkill /T /F` sur l'arbre) |
| Shell par défaut | `/bin/bash` s'il existe, sinon `/bin/sh` | PowerShell |

> [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) §1 étend la détection de ce shell par défaut (`bash`, `zsh`, `sh` hors Windows ; `pwsh` puis `powershell` sur Windows) et l'expose comme un objet — programme, dialecte, origine de la décision — au lieu d'une simple chaîne.

3. **Les deux seuls réglages d'environnement de l'application** sont `shell` (l'interpréteur qui reçoit `cmd`) et `cwd` (répertoire de travail des commandes). Ils vivent dans la configuration de l'application, jamais dans les messages du protocole : le modèle continue de découvrir l'environnement par son `discovery_plan`, et c'est à lui d'émettre des commandes valides pour l'OS qu'il découvre (`Get-ChildItem` plutôt que `ls`, par exemple). On ne réécrit jamais une commande (§1).

   > **Amendé par [ADR-030](ADR-030-shell-detecte-et-traduction-de-dialectes.md) §3 et §4.** Deux points de ce paragraphe changent, et deux seulement. (a) L'OS, le dialecte du shell et le `cwd` sont désormais **annoncés** au modèle dans les instructions d'initialisation : c'est un fait sur l'application — l'interpréteur qu'elle s'apprête à lancer — et non une hypothèse sur la machine, et le `discovery_plan` reste la façon d'apprendre tout le reste. (b) « On ne réécrit jamais une commande » devient : une commande écrite dans l'autre dialecte que le shell détecté peut être réécrite, **uniquement** quand un dictionnaire fermé la traduit exactement, jamais pour une commande qui écrit, et toujours en persistant les deux formes, en les traçant dans l'audit et en les rendant au modèle. Tout le reste de ce paragraphe tient : le modèle reste celui qui émet les commandes, et l'application ne lui en propose aucune.
4. **Encodage** : stdout/stderr sont capturés en octets bruts et stockés tels quels dans les blobs ; le décodage (UTF-8 avec remplacement) n'a lieu qu'au moment de construire l'`execution_result`. Les plages de `chunk_request` sont donc exprimées en **octets**, jamais en caractères.

## Conséquences

- `SubprocessCommandExecutor` délègue lancement et terminaison à un objet `PlatformAdapter` ; les tests unitaires utilisent `FakeCommandExecutor` (aucun processus), les tests marqués `real_subprocess` valident le vrai comportement sur les deux OS (commande courte, commande qui échoue, commande qui dort au-delà du timeout, commande qui ignore la terminaison douce).
- `interrupt_drain_timeout_ms` et `cancel_drain_timeout_ms` sont deux réglages distincts (interruption utilisateur vs stop condition), avec la même mécanique de terminaison en deux temps.
- Le README avertit que le `discovery_plan` d'exemple de la spec suppose un shell POSIX.
