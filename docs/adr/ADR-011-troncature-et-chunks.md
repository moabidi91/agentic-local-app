# ADR-011 — Troncature stderr/stdout, plages d'octets, `chunk_request` avec `stream`

**Statut** : accepté (2026-09-18)

## Contexte

§2.5 : « `stderr` est toujours préservé en entier (priorité 1) ; la fin de `stdout` est préservée plutôt que le début (priorité 2) ». La première règle est impossible à tenir si `stderr` seul dépasse `max_output_bytes`. L'exemple §12.6 demande ensuite `byte_offset: 16384` pour la tâche `t4` (budget 16 384) comme si le modèle avait reçu le **début** de la sortie — ce qui contredit la seconde règle. Enfin `chunk_request` ne dit pas quel flux il vise, et le résultat tronqué n'indique pas quelle plage le modèle a effectivement reçue.

## Décision

### Algorithme de troncature (fonction pure, `PayloadGuard.apply`)

Soit `B` le budget effectif (ADR-010), `E` la taille de stderr, `O` la taille de stdout :

1. `stderr_kept = min(E, B)` octets, **fin** de stderr conservée (les messages d'erreur utiles sont en général les derniers).
2. `stdout_kept = min(O, B - stderr_kept)` octets, **fin** de stdout conservée.
3. `truncated = (stderr_kept < E) or (stdout_kept < O)` ; `original_size_bytes = E + O` ; et par flux : `stdout_total`, `stderr_total`.
4. La coupe se fait sur une frontière d'octets ; le décodage UTF-8 ultérieur utilise `errors="replace"` (ADR-003), et la première ligne conservée peut donc être partielle — la plage exacte est fournie pour lever toute ambiguïté.

Quand `E ≥ B`, stdout n'est pas transmis du tout (`stdout_kept = 0`) : stderr garde la priorité, conformément à l'intention de la spec, mais de façon réalisable.

### Résultat de tâche (extension compatible de §12.5)

```json
{
  "task_id": "t4",
  "status": "completed",
  "exit_code": 0,
  "stdout": "…fin de la sortie…",
  "stderr": "",
  "truncated": true,
  "original_size_bytes": 48211,
  "stdout_total": 48211,
  "stderr_total": 0,
  "stdout_range": [31827, 48211],
  "stderr_range": [0, 0],
  "max_output_bytes_applied": 16384
}
```

`*_range` est l'intervalle `[début, fin)` en octets du flux tel que reçu par le modèle. Ici le modèle sait qu'il lui manque `[0, 31827)` de stdout.

### `chunk_request` (tâche)

```json
{ "task_id": "t-chunk-1", "type": "chunk_request", "ref_task_id": "t4", "stream": "stdout", "byte_offset": 0, "max_bytes": 16384 }
```

- `stream` ∈ {`stdout`, `stderr`}, défaut `stdout` (rend l'exemple §12.6 valide).
- `max_bytes` est plafonné par `hard_max_output_bytes` (ADR-010), puis par le budget effectif de la tâche `chunk_request` si elle en déclare un.
- Résultat : `{ "task_id", "status": "completed", "ref_task_id", "stream", "range": [o, o+n), "total": T, "eof": o+n >= T, "data": "…" }`.
- `ref_task_id` inconnu → tâche `FAILED` (`CHUNK_REF_NOT_FOUND`) ; `byte_offset ≥ total` → `FAILED` (`CHUNK_RANGE_INVALID`) ; jamais une erreur de protocole (ADR-008). Les blobs de toutes les conversations d'une même session restent lisibles après rotation.

### Rétention des blobs

Les blobs sont conservés tant que la session existe ; une purge `agentic-app purge --older-than` est fournie mais rien n'est supprimé automatiquement en v1 (l'auditabilité prime).

## Conséquences

- Tests phase 4 : table paramétrée (E, O, B) couvrant `E ≥ B`, `E + O ≤ B`, `O` seul trop grand, flux vides ; propriété vérifiée : `stdout_kept + stderr_kept ≤ B` et concaténation des plages = flux d'origine.
- Tests phase 3 : lecture par plage dans le store (`read_blob_range`).
- Les exemples §12.5/§12.6 de la spec restent valides tels quels (champs ajoutés optionnels, `stream` par défaut).
