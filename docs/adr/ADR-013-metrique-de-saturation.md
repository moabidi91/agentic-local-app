# ADR-013 — Métrique de saturation du contexte : octets cumulés et seuils

**Statut** : accepté (2026-09-18)

## Contexte

La fenêtre de contexte passe `HEALTHY → WARNING → SATURATED` (§5.4) et la rotation se déclenche « quand le contexte est saturé, quand l'échange devient dangereux, quand les réponses manquent à cause de l'accumulation, quand les seuils configurés sont dépassés » (§10). Aucune grandeur mesurable n'est nommée. L'application ne connaît pas le tokenizer du modèle ; elle connaît en revanche exactement ce qu'elle a envoyé et reçu.

## Décision

1. **Grandeur mesurée** : `context_bytes` = somme des tailles (octets, JSON sérialisé, avant gzip) de tous les messages **envoyés et reçus** dans la conversation courante, plus la taille des `instructions` de l'init (ADR-004). Elle est persistée sur la `ConversationRecord` (`context_bytes`) et mise à jour à chaque POST accepté et à chaque GET valide.
2. **Budget et seuils** (config) : `context.budget_bytes` (défaut 400 000), `context.warning_ratio` 0,70, `context.saturation_ratio` 0,90.
3. **Transitions** :
   - `HEALTHY → WARNING` quand `context_bytes ≥ warning_ratio × budget` (événement d'audit, métrique) ;
   - `WARNING → SATURATED` quand `context_bytes ≥ saturation_ratio × budget`, **ou** quand le prochain message sortant *projeté* porterait `context_bytes` au-delà de `budget` (contrôle fait avant chaque POST : le message n'est pas envoyé, la rotation a lieu d'abord — c'est le « payload exchange becomes unsafe » de §10) ;
   - saut direct `HEALTHY|WARNING → SATURATED` sur `MODEL_CONTEXT_WINDOW_ERROR` retourné par le transport (413 ou erreur explicite, ADR-004), ou sur une `MODEL_PROTOCOL_ERROR` répétée `context.protocol_errors_before_rotation` fois (défaut 2) dans la même conversation — c'est le « replies missing or unusable due to context accumulation » de §10 ;
   - `SATURATED → HEALTHY` sur la conversation **enfant** à la réception du `context_resume_ack` (ADR-007).
4. La rotation n'est **jamais** déclenchée par la taille d'un message isolé (ADR-010) ni par un plan en cours : elle intervient uniquement aux frontières de cycle (avant un POST, ou après un GET en erreur), ce qui correspond aux transitions `RUNNING_PLAN → ROTATING` (résultat prêt, POST projeté trop gros) et `WAITING_MODEL_RESPONSE → ROTATING` (GET en erreur de contexte).
5. Une conversation ne peut être rotatée que `context.max_rotations_per_session` fois (défaut 5) ; au-delà → `ROTATION_FAILED`. Combiné au coût d'un cycle par rotation (ADR-012), cela exclut toute boucle silencieuse (§2.6).

## Conséquences

- `context/window.py` : `ContextWindowMonitor` pur (entrées : compteur, tailles, erreurs ; sortie : état) testé en phase 8 avec des valeurs de budget minuscules.
- L'`ExecutionTracker` expose `context_window_state`, `context_bytes`, `context_budget_bytes`, `rotations_count`.
- Le budget par défaut est volontairement prudent ; il se règle par modèle dans la configuration.
