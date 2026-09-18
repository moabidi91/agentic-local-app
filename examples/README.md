# Exemples

## `acme_model_plugin/` — un provider et un codec écrits hors de l'application

Ce que fait une équipe qui branche son propre modèle quand la configuration seule ne suffit pas : une classe pour le dialecte HTTP de l'API (`provider.py`, [guide 03](../docs/guides/03-ecrire-un-provider.md)) et une classe pour la forme des réponses du modèle (`codec.py`, [guide 04](../docs/guides/04-ecrire-un-codec.md)). Rien n'est enregistré dans `agentic_local_app` : [`config.acme.toml`](config.acme.toml) les désigne par leur chemin d'import.

L'API fictive « ACME Threads » : clé d'API en en-tête `X-Api-Key` (lue dans une variable d'environnement dont le nom est une option), URL construites à partir d'un espace de travail, `POST /workspaces/{ws}/threads` → `{"thread": {"id"}}`, `POST /threads/{id}/events` → `{"event": {"id"}}`, `GET /threads/{id}/events?after=&limit=` → `{"events": [{"kind", "payload"}…], "next"}` (seuls les événements `kind = "message"` portent un message), `DELETE /threads/{id}` → `204`, et un `429 {"error": {"code": "throttled", "retry_in_ms"}}`. Le modèle derrière répond en **fragments** (`{"chunks": [{"delta": "…"}…]}`) qu'il faut recoller avant de lire le JSON du message.

```bash
export ACME_API_KEY=...                       # PowerShell : $env:ACME_API_KEY = "..."
PYTHONPATH=examples uv run agentic-app --config examples/config.acme.toml transport show
PYTHONPATH=examples uv run agentic-app --config examples/config.acme.toml codec show
PYTHONPATH=examples uv run agentic-app --config examples/config.acme.toml config validate
```

Sans `PYTHONPATH=examples`, l'application répond `TRANSPORT_PROVIDER_UNKNOWN` avec l'`ImportError` : c'est le comportement attendu pour un chemin d'import qui n'est pas importable. Un paquet installé dans l'environnement (ou publié avec des entry points `agentic_local_app.transports` / `agentic_local_app.codecs`) n'a pas besoin de `PYTHONPATH`.

Le code est vérifié comme celui de l'application (`ruff`, `mypy --strict`) et couvert par `tests/unit/test_phase7_examples_plugin.py` (options, les quatre requêtes, erreurs, codec, chargement de la configuration) et `tests/integration/test_phase9_examples_plugin.py` (une session complète jusqu'à la `final_answer` contre une API ACME scriptée, une réponse illisible → `UNPARSEABLE_REPLY`, le polling à travers des événements sans message).
