<!-- Fichier généré par tools/protocol_conformance_report.py — ne pas éditer à la main. -->

# Rapport de conformité protocolaire — comportement de l'application face à un modèle qui se trompe

**Généré le 2026-09-19** à partir de la batterie `tests/conformance` (121 cas, tous exécutés à chaque `pytest`).

## 1. Ce que ce rapport mesure

Le modèle distant est la seule source de messages entrants, et rien ne garantit qu'il respecte le protocole : il peut répondre à côté de la grammaire, renvoyer une enveloppe incomplète, un plan impossible, une valeur hors domaine, ou du texte qui n'est pas un message du tout. Ce rapport répond à une question précise : **pour chacune de ces fautes, que fait l'application ?** — quelle erreur elle produit, ce qu'elle persiste, et ce qu'il advient de la session.

Chaque ligne des tableaux est un test exécutable de `tests/conformance` : la boucle protocolaire, l'adaptateur, la politique d'échec, la persistance et l'audit sont les vrais composants ; seuls le réseau, le shell, l'horloge et le générateur d'identifiants sont des doubles. Le rapport est régénéré depuis les tests (`uv run python tools/protocol_conformance_report.py`), il ne peut donc pas décrire un comportement que la suite ne vérifie pas.

## 2. Les politiques en vigueur

Cinq comportements possibles, et un seul est choisi selon la nature de la faute :

1. **Rejet + correction.** C'est la première réponse de l'application à une faute (ADR-023). La réponse fautive est **persistée** (`validation_status = "invalid"`, événement `message.rejected`), comptée dans la fenêtre de contexte (elle est dans le contexte du modèle, ADR-013) et dans `protocol_error_count`, et un `FailureRecord` est écrit ; puis l'application POSTe un `protocol_correction_request` qui cite les erreurs de validation exactes, les types valides à cet instant, un rappel de leur forme et un exemple minimal valide, et relit contre la **même** attente. Une erreur de protocole n'est toujours pas rejouée (spec §7.2) — renvoyer le même message ne changerait rien : une correction est un message neuf, pas une nouvelle tentative. Elle n'ouvre aucun cycle et ne consomme aucun plan, seul le budget de durée continue de courir, et au plus `protocol.max_correction_attempts` réponses inutilisables d'affilée sont tolérées (5 par défaut) ; toute réponse valide remet le compteur à zéro.
2. **Rejet + rotation.** Une fois les corrections épuisées — ou tout de suite, sans corriger, quand la fenêtre est déjà `SATURATED`, un rappel envoyé dans un contexte plein ne pouvant pas recevoir de réponse —, si la fenêtre n'est pas `HEALTHY` et que `context.rotate_on_unusable_reply_in_warning` est vrai (défaut), la faute est lue comme un signe de saturation : l'application ouvre une conversation enfant, lui envoie un résumé, retransmet le message en attente et **continue** (ADR-019 §2, ADR-014), l'enfant repartant avec un budget de corrections neuf.
3. **Rejet + échec de session.** Ce qui reste quand aucune rotation n'est possible : fenêtre `HEALTHY`, ou `context.rotate_on_unusable_reply_in_warning` à faux — auquel cas même une fenêtre saturée échoue au lieu de rotationner. La session passe en `FAILED` sur la **dernière** erreur, dont les `details` disent combien de corrections ont été tentées.
4. **Acceptation avec avertissement.** Une valeur licite mais contradictoire ou implicite (drapeaux contradictoires, workers en séquentiel, budget de sortie supérieur au plafond) est acceptée, normalisée, et l'écart est enregistré comme avertissement sur le message entrant — le modèle n'est pas puni pour une imprécision sans conséquence (ADR-009, ADR-010).
5. **Acceptation.** Le message est conforme : il est persisté, le cycle avance.

Une réponse que le **codec** ne sait même pas lire (pas d'enveloppe du tout : du texte nu, un JSON invalide, un chemin absent) ne passe pas par l'adaptateur : c'est une `MODEL_PROTOCOL_ERROR / UNPARSEABLE_REPLY` levée au niveau du transport, avec un extrait brut de la réponse (ADR-021). Même politique (1, 2 ou 3) et, depuis ADR-023, même trace : faute d'enveloppe, l'enregistrement entrant garde ce que le codec a pu citer — l'extrait et la raison — sous le type interne `system_error`.

**La batterie, elle, tourne avec `protocol.max_correction_attempts = 0`** (`tests/conformance/harness.py`). Chaque cas épingle la **classification** d'une faute : le code d'erreur, ce qui est persisté, ce qui est publié. La correction est orthogonale à cette classification — elle décide de ce que l'application fait *ensuite* —, et la désactiver isole donc ce que la matrice mesure : une faute, un verdict, cas par cas, avec « rejet puis échec » vrai ligne à ligne. La boucle de correction se vérifie dans ses propres cas, qui fixent la borne explicitement.

## 3. Ce que la campagne a corrigé

La batterie a été écrite contre l'application telle qu'elle était, sans rien changer d'abord. Trois défauts sont apparus et ont été corrigés dans la foulée ; les cas correspondants vérifient désormais le comportement corrigé, et échoueraient si l'ancien revenait :

1. **Une réponse qui n'est pas un objet JSON faisait planter la boucle** (`env-array-reply`, `env-string-reply`, `env-scalar-reply`). `_persist_rejected` construisait le payload du rejet avec `dict(reply.messages[0])` : un tableau, une chaîne nue ou un nombre levaient `TypeError` / `ValueError` **avant** toute persistance. Le résultat était un `SYSTEM_ERROR / UNHANDLED_EXCEPTION` remontant jusqu'à l'appelant, sans `message.rejected`, sans trace de ce que le modèle avait envoyé, et `protocol_error_count` inchangé — la faute la plus banale d'un modèle bavard était aussi la moins bien traitée. La forme brute est maintenant conservée telle quelle (`{"raw": …}`) et le rejet suit la politique 1 (`SCHEMA_INVALID`).
2. **Un lot vide rendu par un GET faisait planter la boucle** (`env-empty-batch`). `wait_for_reply` promet au moins un message ; `parse_inbound([])` levait donc un `ValueError` de programmation. Un provider tiers (ADR-020) dont le long-poll rend une page vide tombait dessus. C'est désormais une `MODEL_PROTOCOL_ERROR / EMPTY_REPLY`, traitée comme toute réponse inutilisable.
3. **Le rejet de l'ACK d'une rotation ne laissait pas de trace** (`seq-rejected-ack-trail`). Le `RotationCoordinator` comptait l'erreur et publiait `message.rejected`, mais n'écrivait aucun `MessageRecord` — la réponse fautive était invisible après coup, contrairement à tous les autres rejets — et le cycle `resume` de la conversation enfant restait `RUNNING` derrière une session `FAILED`. Le rejet est maintenant persisté comme ailleurs et le cycle clos en `FAILED` avec son `cycle.ended`.

## 4. Vue d'ensemble

| Famille de cas | Cas | ✅ conforme | ⚠️ à surveiller | ❌ écart |
|---|---|---|---|---|
| Politique de correction | 7 | 7 | 0 | 0 |
| Enveloppe et forme du message | 19 | 19 | 0 | 0 |
| Séquencement des messages | 15 | 15 | 0 | 0 |
| Forme brute de la réponse | 7 | 7 | 0 | 0 |
| Politique appliquée | 5 | 5 | 0 | 0 |
| Structure du plan | 17 | 17 | 0 | 0 |
| Dépendances et exécution | 11 | 11 | 0 | 0 |
| Valeurs des tâches | 19 | 18 | 1 | 0 |
| Contenus de conclusion | 16 | 15 | 1 | 0 |
| Réponses licites mais inattendues | 5 | 5 | 0 | 0 |

**Total : 121 cas — 119 conformes, 2 à surveiller, 0 écarts.**

## 5. La matrice

### Politique de correction

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `corr-one-fault-then-valid` | une réponse hors grammaire, puis la bonne après le rappel | rappel du protocole envoyé, relecture contre la même attente, session menée à son terme | `accepté après correction` | correction puis poursuite normale | ADR-023 · ADR-007 | ✅ |
| `corr-no-cycle-no-plan` | une faute au milieu d'une session dont le budget est serré | la correction ne consomme ni cycle ni plan : seuls les vrais tours sont comptés | `accepté après correction` | correction hors budget de cycles et de plans | ADR-023 · §2.8 · ADR-012 | ✅ |
| `corr-counter-resets` | une faute, une réponse valide, puis une nouvelle faute | le compteur est consécutif : la réponse valide rend tout le budget | `accepté après correction` | correction, remise à zéro, correction | ADR-023 | ✅ |
| `corr-exhausted` | une réponse inutilisable de plus que la borne configurée | échec sur la dernière erreur, ses détails disant combien de corrections ont été tentées | `UNEXPECTED_MESSAGE_TYPE` | corrections épuisées puis échec | ADR-023 · §7.2 | ✅ |
| `corr-disabled` | une réponse hors grammaire, la politique de correction étant désactivée | échec immédiat : le réglage à zéro rétablit la conduite d'avant ADR-023 | `UNEXPECTED_MESSAGE_TYPE` | échec dès la première faute | ADR-023 · §7.2 | ✅ |
| `corr-request-content` | une réponse hors grammaire, et on lit ce que l'application renvoie au modèle | le rappel cite l'erreur, les types attendus et un exemple minimal valide | `protocol_correction_request` | correction | ADR-023 · §12 | ✅ |
| `corr-event-published` | une réponse hors grammaire, et on lit le flux d'événements | `correction.requested` est publié et audité à côté de `message.rejected` | `correction.requested` | correction | ADR-023 · ADR-015 · ADR-018 | ✅ |
### Enveloppe et forme du message

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `env-array-reply` | un tableau JSON à la place de l'objet enveloppe | rejet à l'étape enveloppe ; la forme brute est persistée sous `raw` | `SCHEMA_INVALID` | échec | §12 · ADR-007 · ADR-015 | ✅ |
| `env-string-reply` | une chaîne de caractères à la place de l'objet enveloppe (codec passthrough) | rejet à l'étape enveloppe ; le texte est persisté sous `raw` | `SCHEMA_INVALID` | échec | §12 · ADR-007 · ADR-015 | ✅ |
| `env-scalar-reply` | un nombre, puis null, à la place de l'objet enveloppe | rejet à l'étape enveloppe ; le scalaire est persisté sous `raw` | `SCHEMA_INVALID` | échec | §12 · ADR-007 · ADR-015 | ✅ |
| `env-missing-type` | une enveloppe sans champ `type` | rejet à l'étape enveloppe, `details.errors` pointe `type` | `SCHEMA_INVALID` | échec | §12 · ADR-007 | ✅ |
| `env-missing-conversation-id` | une enveloppe sans `conversation_id` | rejet à l'étape enveloppe, `details.errors` pointe `conversation_id` | `SCHEMA_INVALID` | échec | §12 · ADR-004 | ✅ |
| `env-missing-message-id` | une enveloppe sans `message_id` | rejet ; le rejet reçoit un identifiant local et le curseur ne bouge pas | `SCHEMA_INVALID` | échec | §12 · ADR-004 · ADR-017 | ✅ |
| `env-empty-message-id` | une enveloppe dont le `message_id` est la chaîne vide | rejet ; pydantic signale `string_too_short` sur `message_id` | `SCHEMA_INVALID` | échec | §12 · ADR-004 | ✅ |
| `env-missing-content` | une enveloppe sans `content` | rejet à l'étape enveloppe, `details.errors` pointe `content` | `SCHEMA_INVALID` | échec | §12 · ADR-007 | ✅ |
| `env-content-not-an-object` | un `content` qui est une chaîne, puis une liste | rejet à l'étape enveloppe : `content` doit être un objet | `SCHEMA_INVALID` | échec | §12 · ADR-007 | ✅ |
| `env-unknown-type` | un `type` qui n'existe pas (`plan`) | rejet à l'étape enveloppe : `type` hors énumération | `SCHEMA_INVALID` | échec | §3.5 · §12 · ADR-007 | ✅ |
| `env-type-wrong-case` | un `type` dans la mauvaise casse (`FINAL_ANSWER`) | rejet : l'énumération du §12 est sensible à la casse, aucune normalisation | `SCHEMA_INVALID` | échec | §12 · ADR-007 | ✅ |
| `env-extra-top-level-field` | une enveloppe valide plus un champ de tête inconnu (`priority`) | rejet : l'enveloppe du §12 est fermée (`extra = forbid`) | `SCHEMA_INVALID` | échec | §12 · ADR-007 | ✅ |
| `env-system-error-inbound` | un `system_error`, type interne que le modèle ne doit jamais émettre | rejet avec un code dédié et `details.inbound = false` | `SYSTEM_ERROR_NOT_ALLOWED_INBOUND` | échec | §12.10 · ADR-007 | ✅ |
| `env-application-only-types` | `user_request`, `execution_result` ou `context_resume_request` (types sortants) | rejet : ces types ne sont jamais entrants, `details.inbound = false` | `UNEXPECTED_MESSAGE_TYPE` | échec | §3.5 · ADR-007 | ✅ |
| `env-conversation-mismatch` | une enveloppe dont le `conversation_id` n'est pas celui de la conversation | rejet ; `details` porte le reçu et l'attendu | `CONVERSATION_MISMATCH` | échec | §12 · ADR-004 · ADR-007 | ✅ |
| `env-duplicate-message-id` | un second message réutilisant un `message_id` déjà vu dans la session | rejet ; le rejet est persisté sous un identifiant local, l'original est intact | `DUPLICATE_MESSAGE_ID` | échec | §12 · ADR-007 · ADR-017 | ✅ |
| `env-two-messages-one-get` | deux messages dans un seul GET | rejet ; `details` nomme les deux identifiants et les deux types | `UNEXPECTED_EXTRA_MESSAGE` | échec | §12 · ADR-004 · ADR-007 | ✅ |
| `env-no-reply-at-all` | rien du tout : le modèle ne dépose aucun message | GET rejoué selon §7.3 puis échec en TIMEOUT, jamais en erreur de protocole | `MODEL_GET_TIMEOUT` | échec après épuisement des tentatives | §7.1 · §7.3 · ADR-004 | ✅ |
| `env-empty-batch` | un GET qui rend un lot vide (aucun message) au lieu d'attendre | rejet comme réponse inutilisable ; la boucle ne plante pas | `EMPTY_REPLY` | échec | §3.12 · ADR-004 · ADR-020 | ✅ |
### Séquencement des messages

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `seq-execution-plan-first` | un `execution_plan` en réponse au `user_request` initial | rejet ; `details.expected` ne liste que discovery_plan et user_response | `UNEXPECTED_MESSAGE_TYPE` | échec | §14 · ADR-007 · ADR-022 | ✅ |
| `seq-priority-clarification-first` | une `priority_clarification` en réponse au `user_request` initial | rejet : la première réponse ne peut pas être une clarification | `UNEXPECTED_MESSAGE_TYPE` | échec | §14 · ADR-007 | ✅ |
| `seq-final-answer-first` | un `final_answer` en réponse au `user_request` initial | rejet : conclure sans plan passe par un `user_response` (ADR-022), pas un final_answer | `UNEXPECTED_MESSAGE_TYPE` | échec | §11 · §14 · ADR-007 · ADR-022 | ✅ |
| `seq-context-resume-ack-first` | un `context_resume_ack` en réponse au `user_request` initial | rejet : un ack n'existe que face à un `context_resume_request` | `UNEXPECTED_MESSAGE_TYPE` | échec | §12.9 · ADR-007 · ADR-014 | ✅ |
| `seq-user-response-first-allowed` | un `user_response` en réponse au `user_request` initial (drapeau par défaut) | accepté : ADR-022 élargit la ligne initiale, la session se termine sans plan | `— (accepté)` | accepté | §14 · ADR-007 · ADR-022 | ✅ |
| `seq-user-response-first-strict` | un `user_response` initial avec `protocol.allow_direct_response = false` | rejet ; `details.expected` retombe sur le seul `discovery_plan` du §14 | `UNEXPECTED_MESSAGE_TYPE` | échec | §14 · ADR-022 | ✅ |
| `seq-discovery-plan-after-result` | un `discovery_plan` après un `execution_result` | rejet : la découverte n'a lieu qu'au premier tour | `UNEXPECTED_MESSAGE_TYPE` | échec | §14 · ADR-007 | ✅ |
| `seq-context-resume-ack-after-result` | un `context_resume_ack` après un `execution_result` | rejet : hors rotation, l'ack n'est jamais attendu | `UNEXPECTED_MESSAGE_TYPE` | échec | §12.9 · ADR-007 · ADR-014 | ✅ |
| `seq-follow-up-row-is-wider` | une `priority_clarification` après un `user_request` de relance | accepté : la ligne « relance » d'ADR-007 accepte les trois plans, final_answer et user_response | `— (accepté)` | accepté | §11 · §14 · ADR-007 · ADR-022 | ✅ |
| `seq-plan-instead-of-ack` | un plan au lieu du `context_resume_ack` attendu dans la conversation enfant | rejet dans l'enfant ; `details.expected` ne liste que context_resume_ack | `UNEXPECTED_MESSAGE_TYPE` | échec de la rotation, session FAILED (raison `rotation_failed`) | §10 · §12.9 · ADR-014 | ✅ |
| `seq-final-answer-instead-of-ack` | un `final_answer` au lieu du `context_resume_ack` attendu | rejet : la conclusion n'est pas une confirmation de reprise | `UNEXPECTED_MESSAGE_TYPE` | échec de la rotation, session FAILED | §10 · §12.9 · ADR-014 | ✅ |
| `seq-user-response-instead-of-ack` | un `user_response` au lieu du `context_resume_ack` attendu | rejet : ADR-022 n'élargit pas la ligne « rotation » | `UNEXPECTED_MESSAGE_TYPE` | échec de la rotation, session FAILED | §10 · §12.9 · ADR-014 · ADR-022 | ✅ |
| `seq-ack-wrong-original` | un `context_resume_ack` dont l'`original_conversation_id` n'est pas celui du parent | rejet ; `details` porte le reçu et l'attendu | `ACK_WRONG_ORIGINAL` | échec de la rotation, session FAILED | §12.9 · ADR-014 | ✅ |
| `seq-ack-not-acknowledged` | un `context_resume_ack` avec `acknowledged = false` | rejet : le modèle refuse la reprise, la rotation ne peut pas se terminer | `ACK_NOT_ACKNOWLEDGED` | échec de la rotation, session FAILED | §12.9 · ADR-014 | ✅ |
| `seq-rejected-ack-trail` | un `context_resume_ack` refusé pendant une rotation (trace laissée) | le rejet est compté, publié, **persisté** comme ailleurs, et le cycle `resume` de l'enfant est clos en FAILED | `ACK_NOT_ACKNOWLEDGED` | échec de la rotation, session FAILED | §10 · §16 · ADR-014 · ADR-015 | ✅ |
### Forme brute de la réponse

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `raw-no-json` | de la prose sans le moindre JSON (codec json_text) | erreur de transport avec `reason = no_json_found` ; l'extrait brut est persisté en entrant sous `system_error` et compté (ADR-021 §2 amendé par ADR-023) | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-021 · ADR-023 | ✅ |
| `raw-json-unbalanced` | un JSON tronqué (accolade jamais refermée) | erreur de transport avec `reason = json_unbalanced` | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-021 · ADR-023 | ✅ |
| `raw-fenced-invalid-json` | un bloc ```json``` dont le contenu n'est pas du JSON valide | erreur de transport avec `reason = json_invalid` et l'erreur du parseur | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-021 · ADR-023 | ✅ |
| `raw-path-not-found` | une réponse dont le `content_path` configuré n'existe pas | erreur de transport avec `reason = path_not_found` et le chemin cherché | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-020 · ADR-021 · ADR-023 | ✅ |
| `raw-unexpected-type` | un objet brut alors que le codec attend du texte (aucun `content_path`) | erreur de transport avec `reason = unexpected_type` et `expected = string` | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-021 · ADR-023 | ✅ |
| `raw-no-envelope` | un texte dont le JSON décodé est un tableau vide | le décorateur refuse la réponse : `reason = no_envelope` | `UNPARSEABLE_REPLY` | échec ; l'extrait brut est persisté, aucune enveloppe à enregistrer | §3.12 · ADR-004 · ADR-021 · ADR-023 | ✅ |
| `raw-two-envelopes-in-one-item` | un seul élément brut qui contient un tableau de deux enveloppes | le codec rend deux messages, l'adaptateur les refuse : la chaîne codec → adaptateur tient | `UNEXPECTED_EXTRA_MESSAGE` | échec ; le lot décodé est persisté comme rejet | §3.12 · ADR-007 · ADR-021 | ✅ |
### Politique appliquée

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `pol-full-trail` | une enveloppe adressée à une autre conversation, fenêtre HEALTHY | message.rejected publié, réponse brute persistée en `invalid`, compteur d'erreurs et octets de contexte mis à jour, FailureRecord non rejouable, un seul GET, session FAILED, chaîne d'audit toujours valide | `CONVERSATION_MISMATCH` | rejet + échec de session | §7.2 · §16 · ADR-013 · ADR-015 | ✅ |
| `pol-rotation-in-warning` | la même faute, mais la fenêtre de contexte est déjà en WARNING | une rotation remplace l'échec : l'enfant reçoit le message en attente retransmis et la session se termine normalement | `UNEXPECTED_MESSAGE_TYPE` | rejet + rotation puis reprise | §10 · ADR-014 · ADR-019 §2 | ✅ |
| `pol-no-rotation-when-flag-off` | la même faute en WARNING avec `context.rotate_on_unusable_reply_in_warning = false` | aucune rotation : la politique retombe sur l'échec de session | `UNEXPECTED_MESSAGE_TYPE` | rejet + échec de session | §7.2 · ADR-019 §2 | ✅ |
| `pol-second-error-after-rotation` | une seconde erreur de protocole dans la conversation enfant, après une rotation réussie | aucune seconde rotation : l'ack a ramené la fenêtre de l'enfant en HEALTHY, la borne d'ADR-019 §2 est donc l'état de la fenêtre, pas un compteur | `UNEXPECTED_MESSAGE_TYPE` | rejet + échec de session | §10 · ADR-014 · ADR-019 §2 | ✅ |
| `pol-never-retried` | une erreur de protocole alors que `[retry] max_attempts` vaut 8 | un seul GET et une seule décision `fail` : renvoyer le même message ne changerait rien | `UNEXPECTED_MESSAGE_TYPE` | rejet + échec de session, sans rejeu | §7.1 · §7.2 · §7.3 | ✅ |
### Structure du plan

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `plan-tasks-empty` | un plan dont `tasks` est une liste vide | rejet à l'étape content : un plan porte au moins une tâche | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-tasks-missing` | un plan sans champ `tasks` | rejet : `tasks` est obligatoire, l'absence n'est pas lue comme une liste vide | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-tasks-not-a-list` | un plan dont `tasks` est un objet, puis une chaîne | rejet : `tasks` est une liste, aucune tolérance pour une tâche unique | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-task-not-an-object` | un plan dont la première tâche est une chaîne au lieu d'un objet | rejet ; `details.errors[].loc` désigne l'indice fautif (`content.tasks.0`) | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-missing-plan-id` | un plan sans `plan_id` | rejet : sans identifiant, le plan ne peut être ni projeté ni référencé | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-empty-plan-id` | un plan dont le `plan_id` est la chaîne vide | rejet : la chaîne vide n'est pas un identifiant | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 · ADR-017 | ✅ |
| `plan-duplicate-plan-id` | un second plan réutilisant le `plan_id` du premier | rejet ; le premier plan reste seul projeté | `DUPLICATE_PLAN_ID` | échec | §12.2 · ADR-007 · ADR-019 §1 | ✅ |
| `plan-duplicate-plan-id-after-rotation` | un `plan_id` déjà utilisé, mais dans la conversation enfant née d'une rotation | rejet quand même : la portée d'unicité est la **session**, pas la conversation | `DUPLICATE_PLAN_ID` | échec de session (la rotation avait déjà servi) | §12.2 · ADR-014 · ADR-019 §1 | ✅ |
| `plan-missing-objective` | un plan sans `objective` | rejet : l'objectif est ce qui rend le plan auditable | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-missing-execution-policy` | un plan sans `execution_policy` | rejet : aucune politique par défaut n'est supposée | `SCHEMA_INVALID` | échec | §12.2 · §8.2 · ADR-007 | ✅ |
| `plan-unknown-execution-policy` | une `execution_policy` inventée (`best_effort`) | rejet : l'énumération du §12.2 n'a que `sequential` et `parallel` | `SCHEMA_INVALID` | échec | §12.2 · §8.2 · ADR-007 | ✅ |
| `plan-duplicate-task-id-in-plan` | deux tâches portant le même `task_id` dans un seul plan | rejet ; `details.scope` vaut `plan` | `DUPLICATE_TASK_ID` | échec | §12.2 · ADR-007 | ✅ |
| `plan-duplicate-task-id-in-session` | un `task_id` déjà utilisé par un plan précédent de la session | rejet ; `details.scope` vaut `session` : les identifiants ne sont pas recyclables | `DUPLICATE_TASK_ID` | échec | §12.2 · ADR-007 · ADR-019 §1 | ✅ |
| `plan-extra-field` | un plan valide plus un champ inconnu (`priority`) | rejet : le contenu d'un plan est fermé (`extra = forbid`), `loc` nomme le champ | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `task-extra-field` | une tâche valide plus un champ inconnu (`shell`) | rejet ; `loc` descend jusqu'à la tâche fautive (`content.tasks.0.shell`) | `SCHEMA_INVALID` | échec | §12.2 · ADR-003 · ADR-007 | ✅ |
| `plan-state-summary-too-large` | un `state_summary` plus gros que `payload.max_state_summary_bytes` | rejet ; `details` porte la taille mesurée et la borne | `STATE_SUMMARY_TOO_LARGE` | échec | §12.2 · ADR-005 · ADR-010 | ✅ |
| `plan-state-summary-at-the-bound` | un `state_summary` mesurant exactement `payload.max_state_summary_bytes` | accepté : la borne elle-même passe, et le résumé est stocké verbatim sur le PlanRecord | `accepté` | accepté | §12.2 · ADR-005 · ADR-010 | ✅ |
### Dépendances et exécution

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `dep-unknown` | une tâche dépendant d'un `task_id` qui n'est pas dans le plan | rejet ; `details` nomme la tâche et la dépendance introuvable | `UNKNOWN_DEPENDENCY` | échec | §8.2 · ADR-007 | ✅ |
| `dep-self` | une tâche qui se déclare dépendante d'elle-même | rejet avec un code dédié, avant même la recherche de cycle | `SELF_DEPENDENCY` | échec | §8.2 · ADR-007 | ✅ |
| `dep-cycle-two-tasks` | deux tâches qui dépendent l'une de l'autre | rejet ; `details.cycle` est le chemin **fermé** du cycle | `DEPENDENCY_CYCLE` | échec | §8.2 · ADR-007 | ✅ |
| `dep-cycle-three-tasks` | un cycle de trois tâches (t1 → t2 → t3 → t1) | rejet ; `details.cycle` reconstitue le chemin entier, pas seulement la dernière arête | `DEPENDENCY_CYCLE` | échec | §8.2 · ADR-007 | ✅ |
| `dep-forward-in-sequential` | en `sequential`, une tâche dépendant d'une tâche déclarée **après** elle | rejet : en séquentiel l'ordre de déclaration est l'ordre d'exécution | `FORWARD_DEPENDENCY_IN_SEQUENTIAL` | échec | §8.2 · ADR-007 | ✅ |
| `dep-forward-in-parallel-accepted` | la même déclaration en `parallel` | accepté : l'ordre vient du graphe, pas de la déclaration — la dépendance s'exécute en premier et les deux tâches terminent | `accepté` | accepté | §8.2 · ADR-007 · ADR-017 | ✅ |
| `workers-non-positive` | `max_parallel_workers` à 0, puis négatif | rejet par le schéma : au moins un worker | `SCHEMA_INVALID` | échec | §8.2 · ADR-007 | ✅ |
| `workers-ignored-in-sequential` | `max_parallel_workers = 4` sur un plan `sequential` | accepté avec l'avertissement d'audit `WORKERS_IGNORED_IN_SEQUENTIAL` ; le PlanRecord porte un seul worker et les tâches s'exécutent l'une après l'autre | `accepté + avertissement` | accepté + avertissement | §8.2 · ADR-009 | ✅ |
| `workers-default-in-parallel` | un plan `parallel` sans `max_parallel_workers` | accepté avec l'avertissement `DEFAULT_WORKERS_APPLIED` ; le PlanRecord porte la valeur appliquée (1), aucune parallélisation implicite | `accepté + avertissement` | accepté + avertissement | §8.2 · ADR-009 | ✅ |
| `flags-contradictory` | une tâche à la fois `critical` et `continue_on_error` | accepté avec `CONTRADICTORY_FLAGS:<task_id>` ; la règle effective d'ADR-009 est un **ou** : `stops_plan_on_failure` est vrai et l'échec arrête bien le plan | `accepté + avertissement` | accepté + avertissement | §8.3 · ADR-009 | ✅ |
| `stop-plan-on-success` | une tâche `stop_plan_on_success` qui réussit, suivie d'une autre tâche | le plan est court-circuité : statut `short_circuited_on_success`, la suite est `SKIPPED`, et l'`execution_result` le rapporte au modèle | `accepté` | accepté (plan court-circuité) | §8.3 · ADR-009 | ✅ |
### Valeurs des tâches

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `task-cmd-blank` | une tâche `cmd` dont la commande est vide, puis faite d'espaces | rejet : une tâche `cmd` exige une commande non vide, l'espace ne compte pas | `SCHEMA_INVALID` | échec | §12.2 · ADR-003 · ADR-007 | ✅ |
| `task-cmd-null` | une tâche `cmd` dont `cmd` vaut `null` | rejet : le type par défaut est `cmd`, et une tâche `cmd` sans commande n'existe pas | `SCHEMA_INVALID` | échec | §12.2 · ADR-007 | ✅ |
| `task-cmd-with-chunk-fields` | une tâche `cmd` portant en plus les champs d'un `chunk_request` | rejet : les deux formes de tâche sont disjointes, aucun mélange n'est toléré | `SCHEMA_INVALID` | échec | §12.6 · ADR-011 | ✅ |
| `task-unknown-type` | une tâche de type inconnu (`http_request`) | rejet : seuls `cmd` et `chunk_request` existent, l'application n'invente pas d'action | `SCHEMA_INVALID` | échec | §12.2 · §12.6 · ADR-007 | ✅ |
| `task-max-output-non-positive` | `max_output_bytes` à 0, puis négatif | rejet par le schéma (`gt = 0`) : un budget nul n'est pas une troncature | `SCHEMA_INVALID` | échec | §12.2 · ADR-010 | ✅ |
| `task-max-output-clamped` | `max_output_bytes` très au-dessus de `payload.hard_max_output_bytes` | accepté et **ramené** au plafond : `max_output_bytes_applied` vaut le plafond tandis que la valeur déclarée reste lisible sur la TaskRecord | `accepté` | accepté (valeur normalisée) | §2.5 · ADR-010 | ✅ |
| `task-timeout-zero` | `timeout_ms = 0` | rejet par le schéma (`gt = 0`) : zéro n'est pas « pas de limite » | `SCHEMA_INVALID` | échec | §12.2 · ADR-008 | ✅ |
| `task-timeout-clamped` | `timeout_ms` au-dessus de `execution.max_task_timeout_ms` | accepté et ramené au plafond : `timeout_ms_applied` vaut le plafond, et c'est ce délai que reçoit l'exécuteur | `accepté` | accepté (valeur normalisée) | ADR-008 §1 | ✅ |
| `task-timeout-default` | une tâche sans `timeout_ms` | accepté : le défaut de configuration s'applique, la déclaration reste vide et l'exécuteur reçoit `execution.default_task_timeout_ms` | `accepté` | accepté (défaut appliqué) | ADR-008 §1 | ✅ |
| `task-values-coerced-from-strings` | une tâche dont le budget, le timeout et les drapeaux arrivent en chaînes ou en entiers | accepté et **converti** sans avertissement : `"2048"` devient un budget, `"1000"` un timeout, `"yes"`/`0` des drapeaux, et ce sont ces valeurs qui pilotent l'exécution | `accepté` | accepté (valeurs converties) | §12.2 · ADR-008 · ADR-009 · ADR-010 | ⚠️ |
| `plan-default-output-budget` | un plan portant `default_max_output_bytes`, avec une tâche qui déclare et une qui non | précédence tâche > plan > configuration, lisible sur chaque `max_output_bytes_applied` : le défaut du plan ne touche que les tâches muettes et ne survit pas au plan suivant | `accepté` | accepté (valeur normalisée) | §2.5 · ADR-010 | ✅ |
| `chunk-missing-ref-task-id` | un `chunk_request` sans `ref_task_id` | rejet : sans référence, il n'y a rien à relire | `SCHEMA_INVALID` | échec | §12.6 · ADR-011 | ✅ |
| `chunk-missing-range` | un `chunk_request` sans `byte_offset`, puis sans `max_bytes` | rejet : la plage est obligatoire, aucune valeur implicite | `SCHEMA_INVALID` | échec | §12.6 · ADR-011 | ✅ |
| `chunk-negative-offset` | un `chunk_request` dont le `byte_offset` est négatif | rejet par le schéma (`ge = 0`) : une plage part du début du flux, jamais d'avant | `SCHEMA_INVALID` | échec | §12.6 · ADR-011 | ✅ |
| `chunk-unknown-stream` | un `chunk_request` dont le `stream` est inconnu (`stdlog`) | rejet : deux flux existent, et l'absence vaut `stdout` — pas une invention | `SCHEMA_INVALID` | échec | §12.6 · ADR-011 | ✅ |
| `chunk-ref-unknown` | un `chunk_request` vers un `task_id` qui n'existe nulle part dans la session | rejet au niveau protocole : l'adaptateur connaît toutes les tâches de la session | `CHUNK_REF_UNKNOWN` | échec | §12.6 · ADR-011 · ADR-019 §1 | ✅ |
| `chunk-ref-without-stored-output` | un `chunk_request` vers une tâche connue mais dont aucune sortie n'a été stockée | **accepté** par le protocole (la tâche existe) puis `FAILED` à l'exécution avec `CHUNK_REF_NOT_FOUND` : les deux niveaux d'ADR-019 §1 se voient | `accepté puis CHUNK_REF_NOT_FOUND (tâche)` | accepté ; la tâche échoue, le plan s'arrête, la session continue | ADR-008 §5 · ADR-011 · ADR-019 §1 | ✅ |
| `chunk-ref-previous-conversation` | un `chunk_request` vers une tâche d'une **conversation précédente**, après rotation | accepté et servi : les blobs de la session survivent à la rotation | `accepté` | accepté | ADR-011 · ADR-014 · ADR-019 §1 | ✅ |
| `chunk-max-bytes-clamped` | un `chunk_request` valide dont `max_bytes` dépasse `payload.hard_max_output_bytes` | accepté et ramené au plafond : `TaskRecord.max_bytes` porte la valeur appliquée | `accepté` | accepté (valeur normalisée) | ADR-010 · ADR-011 | ✅ |
### Contenus de conclusion

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `final-missing-status` | un `final_answer` sans `status` | rejet : la conclusion dit toujours dans quel état elle laisse l'enquête | `SCHEMA_INVALID` | échec | §12.7 · ADR-007 | ✅ |
| `final-missing-diagnosis` | un `final_answer` sans `diagnosis` | rejet : une conclusion sans diagnostic n'est pas une réponse | `SCHEMA_INVALID` | échec | §12.7 · ADR-007 | ✅ |
| `final-evidence-not-a-list` | un `final_answer` dont `evidence` est une chaîne au lieu d'une liste | rejet : la preuve est une liste d'éléments, pas un paragraphe | `SCHEMA_INVALID` | échec | §12.7 · ADR-007 | ✅ |
| `final-extra-fields` | un `final_answer` portant des champs inconnus (`confidence`, `references`) | accepté : `FinalAnswerContent` est ouvert (`extra = allow`) et les champs supplémentaires survivent dans `SessionRecord.final_answer` | `accepté` | accepté | §12.7 · ADR-007 | ✅ |
| `final-empty-evidence` | un `final_answer` dont `evidence` est une liste vide | accepté : conclure sans preuve citée est pauvre, pas invalide | `accepté` | accepté | §12.7 · ADR-007 | ✅ |
| `user-response-unknown-format` | un `user_response` dont le `format` est inconnu (`html`) | rejet : trois formats de rendu existent, l'interface n'en devine pas un quatrième | `SCHEMA_INVALID` | échec | ADR-022 §1 | ✅ |
| `user-response-empty-body` | un `user_response` au corps vide | rejet : répondre à l'utilisateur sans rien lui dire n'est pas une réponse | `SCHEMA_INVALID` | échec | ADR-022 §1 | ✅ |
| `user-response-too-large` | un `user_response` dont le corps dépasse `payload.max_message_bytes` | rejet ; `details` porte la taille du corps, la borne et le message fautif | `USER_RESPONSE_TOO_LARGE` | échec | ADR-010 · ADR-022 §1 | ✅ |
| `user-response-expects-reply-not-a-bool` | un `user_response` dont `expects_reply` vaut la chaîne `sometimes` | rejet : `expects_reply` décide de la suite du tour, il doit être un booléen | `SCHEMA_INVALID` | échec | ADR-022 §1 · ADR-022 §4 | ✅ |
| `user-response-expects-reply-coerced` | un `user_response` dont `expects_reply` vaut la chaîne `yes` | accepté et **converti** en `true` par la coercition laxiste de pydantic | `accepté` | accepté (valeur convertie) | ADR-022 §1 · ADR-022 §4 | ⚠️ |
| `user-response-unknown-status` | un `user_response` dont le `status` est hors domaine (`done`) | rejet : trois statuts existent (`completed`, `partial`, `failed`) | `SCHEMA_INVALID` | échec | ADR-022 §1 | ✅ |
| `user-response-json-body-not-json` | un `user_response` déclaré `format = "json"` dont le corps n'est pas du JSON | **accepté** : le corps est opaque, jamais analysé ; il est persisté verbatim et `format` ne sert qu'au rendu côté utilisateur | `accepté` | accepté | ADR-022 §1 | ✅ |
| `user-response-envelope-look-alike` | un `user_response` dont le corps contient une enveloppe de protocole en texte | accepté verbatim : rien du corps n'est réinjecté dans le protocole, aucun plan n'est créé, aucun message n'est posté | `accepté` | accepté | ADR-022 §1 · ADR-022 §5 | ✅ |
| `ack-missing-acknowledged` | un `context_resume_ack` sans champ `acknowledged` | rejet à l'étape content dans la conversation enfant ; la rotation échoue | `SCHEMA_INVALID` | échec de la rotation, session FAILED | §12.9 · ADR-014 | ✅ |
| `ack-acknowledged-wrong-type` | un `context_resume_ack` dont `acknowledged` est une chaîne non booléenne | rejet : l'accusé de reprise est un oui ou un non, pas une nuance | `SCHEMA_INVALID` | échec de la rotation, session FAILED | §12.9 · ADR-014 | ✅ |
| `ack-extra-field` | un `context_resume_ack` valide plus un champ inconnu (`summary_ok`) | rejet : le contenu de l'accusé est fermé comme les autres contenus du §12 | `SCHEMA_INVALID` | échec de la rotation, session FAILED | §12.9 · ADR-007 · ADR-014 | ✅ |
### Réponses licites mais inattendues

| Cas | Ce que le modèle envoie | Ce que fait l'application | Code | Suite | Règle |  |
|---|---|---|---|---|---|---|
| `licit-plan-of-chunks-only` | un plan dont toutes les tâches sont des `chunk_request` (aucune commande) | accepté : le plan s'exécute sans lancer un seul processus et produit un execution_result | `accepté` | accepté | §12.6 · ADR-011 | ✅ |
| `licit-plan-fails-session-continues` | un plan d'une seule commande qui sort en code non nul | le **plan** échoue (`stopped_on_failure`) et l'`execution_result` le dit ; la session, elle, continue : un échec de commande n'est pas une faute de protocole | `accepté` | accepté (plan arrêté, protocole intact) | §8.3 · ADR-008 §3 · ADR-009 | ✅ |
| `licit-same-plan-new-ids` | deux fois le même contenu de plan, avec un `plan_id` et des `task_id` neufs | accepté : l'unicité porte sur les identifiants, jamais sur le contenu | `accepté` | accepté | §12.2 · ADR-007 · ADR-019 §1 | ✅ |
| `licit-final-answer-after-discovery` | un `final_answer` dès le résultat du plan de découverte | accepté : rien n'oblige le modèle à un second plan, la session se termine COMPLETED | `accepté` | accepté | §11 · §14 · ADR-007 | ✅ |
| `licit-user-response-mid-investigation` | un `user_response` après un `execution_result`, au milieu d'une enquête | accepté : le tour est conclu sans `final_answer`, la session est COMPLETED et la conversation reste réutilisable pour la réponse de l'utilisateur | `accepté` | accepté | §11 · ADR-022 §2 · ADR-022 §3 | ✅ |

## 6. Constats et recommandations

- ⚠️ **`task-values-coerced-from-strings`** (Valeurs des tâches) — Les contenus sont validés en mode **laxiste** (pydantic par défaut) : un entier accepte une chaîne numérique (`"2048"`) et un flottant entier (`2048.0`), un booléen accepte `"yes"`, `"no"`, `"on"`, `0`, `1`. La conversion est silencieuse et sert ensuite de base aux valeurs appliquées : plafonnement ADR-010, timeout passé au shell (ADR-008), règle d'arrêt ADR-009 — un `continue_on_error: 0` décide donc de l'arrêt du plan. Rien n'est incohérent ici (les valeurs obtenues sont celles que le modèle voulait) et les identifiants, eux, restent strictement des chaînes, mais la frontière du protocole est plus floue que ce que le §12 laisse entendre. À trancher : soit valider les contenus en mode strict (`strict=True` sur `ProtocolModel`), soit documenter explicitement la tolérance dans `PROTOCOL_INSTRUCTIONS.md` (ADR-004). Même cause que `user-response-expects-reply-coerced`.
- ⚠️ **`user-response-expects-reply-coerced`** (Contenus de conclusion) — Même cause que `task-values-coerced-from-strings` (validation laxiste), mais sur le drapeau qui décide de la suite du tour : `expects_reply` garde la conversation réutilisable sous `auto_close_on_final_answer` (ADR-022 §4) et il est ici dérivé d'une chaîne. `"yes"`, `"on"`, `"1"` donnent `true` ; `"sometimes"` reste refusé. Même arbitrage à rendre : validation stricte des contenus, ou tolérance documentée.

## 7. Comment rejouer la batterie

```bash
uv run pytest -m conformance -q                     # la batterie seule
uv run pytest -q                                    # toute la suite, batterie comprise
uv run python tools/protocol_conformance_report.py  # régénérer ce rapport
```

Ajouter un cas : écrire le test dans `tests/conformance/`, le décorer avec `@case(...)` (identifiant, famille, ce qui est envoyé, ce que fait l'application, code, suite, règle, verdict), puis régénérer le rapport. Un identifiant en double est une erreur, et un verdict autre que « conforme » exige une note : la matrice et la suite ne peuvent pas diverger.
