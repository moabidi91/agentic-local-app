# ADR-017 — Ordre déterministe des résultats, horloge et identifiants injectables

**Statut** : accepté (2026-09-18)

## Contexte

L'exécution parallèle est par nature non déterministe dans l'ordre d'achèvement des tâches. Si l'`execution_result` listait les résultats dans l'ordre d'arrivée, deux exécutions du même plan produiraient des payloads différents, et les tests ne pourraient pas comparer des messages entiers. De même, des identifiants tirés au hasard et une horloge système rendent les scénarios non rejouables, alors que §18.3 exige des tests isolés et §17.3 une reconstruction complète depuis l'audit.

## Décision

1. **Ordre des résultats** : `results[]`, `skipped_tasks[]`, `cancelled_tasks[]`, `interrupted_tasks[]` sont triés dans l'**ordre de déclaration des tâches dans le plan**, quel que soit l'ordre d'achèvement. L'ordre réel est conservé par `started_at` / `ended_at` sur chaque résultat et dans l'audit.
2. **Sérialisation canonique** : tout message sortant et tout payload d'audit sont sérialisés avec `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`. Le hash de la chaîne d'audit (§3.16) est `sha256(previous_event_hash + canonical(event_sans_hash))`, avec un hash « genèse » constant (`"0" * 64`) pour le premier événement d'une session.
3. **Horloge injectable** : `Clock` (`now() -> datetime UTC`, `monotonic_ms() -> int`). Implémentations : `SystemClock` et `FakeClock` (avance manuelle). Toute mesure de durée (timeouts, drain, budget, backoff) passe par `monotonic_ms()` ; toute estampille persistée par `now()`.
4. **Identifiants injectables** : `IdGenerator` avec `conversation_id()`, `message_id()`, `cycle_id()`, `event_id()`, `blob_id()`, `session_id()`. `UuidIdGenerator` en production ; `SequentialIdGenerator` en test (`conv-0001`, `msg-0001`, …) pour des messages attendus comparables à l'octet près. Les `plan_id` et `task_id` viennent du modèle et ne sont jamais réécrits.
5. **Backoff déterministe** : `RetryController` calcule `min(base × 2^attempt, cap)` sans gigue par défaut ; la gigue est une option (`jitter_ratio`) désactivée dans les tests, et la séquence de délais calculée est persistée avec la décision de retry (§7.3).

## Conséquences

- Tests : comparaison de messages complets (`assert built == expected_json`) ; scénarios de `FakeTransportGateway` écrits avec les identifiants séquentiels.
- `AuditLog.verify()` recalcule toute la chaîne et signale la première rupture ; testé en phase 10 sur une chaîne altérée.
- Aucun composant n'appelle `datetime.now()`, `time.time()` ou `uuid4()` directement (règle vérifiée par un test d'inspection du code source en phase 10).
