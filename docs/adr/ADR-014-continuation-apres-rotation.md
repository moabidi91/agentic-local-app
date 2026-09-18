# ADR-014 — Continuation après rotation : renvoi du message sortant en attente

**Statut** : accepté (2026-09-18) — amendé par [ADR-019](ADR-019-consolidation-vague-1.md)

## Contexte

Après le `context_resume_ack`, le diagramme d'activité (§14) refait simplement un GET et attend que le modèle enchaîne spontanément sur un plan. Cela suppose que le modèle puisse émettre deux messages de suite sans sollicitation, ce que le contrat POST/GET ne garantit pas, et l'ack de §12.9 ne porte aucun plan. De plus la spec ne dit pas ce que devient le message qui était en cours d'envoi ou en attente de réponse quand la rotation s'est déclenchée.

## Décision

Une rotation se déclenche toujours **avec un message sortant en attente** `M` (ADR-013) : soit `M` est prêt et son POST projeté dépasserait le budget (`RUNNING_PLAN → ROTATING`, `M` = `execution_result`), soit `M` a été envoyé et le GET a échoué pour cause de contexte (`WAITING_MODEL_RESPONSE → ROTATING`, `M` = `execution_result` ou `user_request`).

Séquence de rotation :

1. conversation parent → `ROTATING` ; `ContextReducer` construit le résumé (ADR-005) ; échec → `ROTATION_FAILED`, parent `FAILED`, session `FAILED` ;
2. `init` d'une conversation distante ; `ConversationRecord` enfant (`parent_conversation_id`, `context_window_state = SATURATED`, `session_id` identique) ; `ContextSummaryRecord` ; cycle de type `resume` ;
3. POST `context_resume_request` dans l'enfant, GET jusqu'au `context_resume_ack` (seul type autorisé, ADR-007) ; enfant `SATURATED → HEALTHY` ; parent `ROTATING → CLOSED` (`closure_reason = rotated`) ;
4. **l'application renvoie `M` dans l'enfant**, avec un **nouveau `message_id`** et le `conversation_id` de l'enfant, puis GET : la table des messages attendus est celle de `M` (`execution_result` → plan ou `final_answer` ; `user_request` initial → `discovery_plan`). Le contenu de `M` est identique à l'original (même `plan_id`, mêmes résultats), ce qui garde l'invariant « un plan ⇒ exactement un `execution_result` » (§19.9) — il s'agit d'une **retransmission**, tracée en audit (`RETRANSMITTED_AFTER_ROTATION`, avec les deux `message_id`).

Le `context_resume_request` contient en plus du résumé le champ `pending_message_type` pour que le modèle sache ce qui va suivre ; il n'a **rien à faire** d'autre qu'acquitter.

Si l'ack n'arrive pas (timeout, erreur de protocole), la politique de §7 s'applique dans la conversation enfant ; une interruption pendant la rotation marque parent **et** enfant `INTERRUPTED`.

## Conséquences

- `ProtocolOrchestrator.rotate(pending_message)` encapsule la séquence ; testée en phase 8 avec `FakeTransportGateway` : `given_context_saturated_when_rotation_completes_then_pending_result_retransmitted_in_child`, `given_rotation_when_ack_missing_then_failure_policy_applies_in_child`.
- Schéma : `context_resume_request.content.pending_message_type`.
- Le diagramme §14 est amendé dans `docs/architecture/06-context-rotation.md` (le retour `ZA → N` devient `ZA → retransmit M → G`).
