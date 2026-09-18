# ADR-015 — Persister avant publier : EventBus synchrone, store hors du chemin critique du bus

**Statut** : accepté (2026-09-18)

## Contexte

§17.1 exige que « toutes les transitions soient persistées **avant** d'être exploitées » et qu'il n'y ait « aucun état critique uniquement en mémoire ». §3.20 fait pourtant du `ConversationStore` un **abonné de l'EventBus** au même titre que l'audit et la télémétrie : si la persistance de l'état passait par le bus, l'ordre « persister puis agir » dépendrait de l'ordre de dispatch et ne serait plus garanti par construction. Par ailleurs la chaîne de hash de l'audit (§3.16) exige un ordre total des événements.

## Décision

1. **Ordre imposé pour toute transition** : `valider → persister (store) → publier (bus) → agir`. Le composant propriétaire de la transition (`ConversationLifecycleManager` pour la conversation et la session, `PlanRunner` pour plan et tâches) écrit dans le store **directement**, puis publie l'événement. Une `PERSISTENCE_ERROR` empêche la transition : l'état en mémoire n'est pas modifié et l'événement n'est pas publié.
2. L'**EventBus est synchrone et en process** : `publish()` appelle les abonnés dans l'ordre d'inscription, dans le thread appelant, et ne rend la main qu'une fois tous les abonnés notifiés. C'est ce qui donne à la fois l'« ordre de livraison garanti » de §3.20 et la cohérence immédiate du snapshot (§4 : « à tout instant »). Un abonné qui lève une exception est isolé : l'erreur est journalisée et transformée en `SYSTEM_ERROR` non bloquante, sauf pour l'`AuditLog` dont l'échec est une `PERSISTENCE_ERROR` (l'audit fait partie de l'état critique).
3. Le `ConversationStore` reste abonné au bus **uniquement** pour des données dérivées non critiques (référence du dernier événement d'audit sur la conversation, `last_model_response_state`), jamais pour les transitions d'état.
4. Ordre d'inscription des abonnés, fixé au démarrage : `AuditLog` → `ExecutionTracker` → `TelemetryService` → `ConversationStore (dérivé)`. Ainsi l'événement est chaîné dans l'audit avant que le snapshot ne le reflète.
5. Un **checkpoint stable** (§3.6) est l'état du store après une transition complète ; SQLite en mode WAL avec une transaction par transition suffit : il n'y a pas de table de checkpoints séparée, `RecoveryCoordinator` relit simplement l'état persisté (ADR-016).

## Conséquences

- Signature type : `lifecycle.transition(conversation_id, to_state, *, reason)` → persiste, publie `ConversationStateChanged`, retourne le record mis à jour.
- Tests phase 1 : `given_store_failing_when_transition_attempted_then_state_unchanged_and_no_event_published` ; phase 10 : `given_two_transitions_when_published_then_audit_order_equals_publication_order`.
- Les tests unitaires utilisent un `RecordingEventBus` (liste des événements publiés) et l'`InMemoryConversationStore`.
