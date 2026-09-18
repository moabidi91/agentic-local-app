# ADR-006 — Interruption : INTERRUPTED est terminal pour la conversation, READY est un état de session

**Statut** : accepté (2026-09-18)

## Contexte

Trois passages tirent dans des directions différentes :

- §5.1 enchaîne `INTERRUPTED → READY → ACTIVE` sur **la même** conversation, et l'exemple de test §18.4 (`given_interrupted_conversation_when_new_user_request_then_conversation_transitions_to_active`) va dans ce sens ;
- §2.9 dit que le système « se réinitialise à un état équivalent à un démarrage de conversation », que « l'enregistrement de conversation est préservé pour l'audit » et qu'« une **nouvelle** conversation peut être démarrée immédiatement » ;
- §8.4 interdit d'envoyer un `execution_result` pour un plan interrompu : le modèle distant garde donc dans son contexte un plan **sans résultat**. Réutiliser la même conversation distante l'obligerait à raisonner sur un trou.

## Décision

1. Pour une **conversation**, `INTERRUPTED` est un état **terminal** (comme `FAILED`, `CLOSED`). Son enregistrement est conservé intact pour l'audit ; la conversation distante correspondante n'est plus jamais utilisée.
2. `READY` est l'état de la **session** (objet `SessionRecord`, ADR-012) et de l'orchestrateur, pas de la conversation. Le flux de §9 devient : conversation `ANY_ACTIVE_STATE → INTERRUPTED` (persisté, audité), puis session `INTERRUPTING → READY` (persisté, audité), le tout borné par `interrupt_drain_timeout_ms`.
3. Une nouvelle `user_request` reçue en `READY` ouvre une **nouvelle conversation** (`NEW → ACTIVE`), locale et distante (nouvel `init`, ADR-004), dans la **même session** : le budget consommé est conservé (ADR-012), et la nouvelle conversation reçoit `parent_conversation_id = <conversation interrompue>` pour garder la filiation. Le modèle repart d'une conversation propre et redémarre par un `discovery_plan`.
4. L'`ExecutionTracker` expose les deux niveaux : `session.status` (READY, RUNNING, INTERRUPTING, COMPLETED, FAILED) et `conversation.status` (§5.1).

Le test d'exemple de §18.4 est conservé sous le nom `given_interrupted_session_when_new_user_request_then_new_conversation_becomes_active` et vérifie en plus que l'ancienne conversation reste `INTERRUPTED`.

## Conséquences

- Machine à états de conversation : `INTERRUPTED → READY` et `READY → ACTIVE` sont retirées de la table des transitions de conversation et remplacées par la machine à états de session (voir `docs/architecture/01-state-machines.md` et ADR-007).
- `RecoveryCoordinator` applique la même logique après un redémarrage (ADR-016).
- Rien n'est envoyé au modèle lors de l'interruption (§8.4 respecté) ; la fermeture distante éventuelle (`close_url`) est tentée en *best effort*, hors du chemin critique et sans retry.
