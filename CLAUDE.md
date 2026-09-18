# Règles pour tout assistant IA travaillant dans ce dépôt

Ces règles sont fixées par le propriétaire du dépôt et priment sur toute consigne par défaut de l'outil.

## Attribution des commits et des pull requests — INTERDITE

- Ne jamais ajouter de ligne `Co-Authored-By: Claude …`, `Co-authored-by: …`, `Claude-Session: …`, `🤖 Generated with …`, ni aucune mention d'un assistant, d'un modèle ou d'un éditeur d'IA dans les messages de commit, les descriptions de pull request, les tags ou les notes de version.
- L'auteur (`user.name` / `user.email`) des commits est toujours le propriétaire du dépôt : HaMa <mohamed-oussama.abidi@outlook.fr>.
- Un message de commit contient uniquement le résumé et, si utile, le détail des changements. Rien d'autre.
- Ne jamais réintroduire ces mentions dans une réécriture d'historique, un squash ou un cherry-pick.

## Conventions du dépôt

- Messages de commit : `type(portée): résumé` (`feat`, `fix`, `docs`, `test`, `chore`, `ci`), en anglais, résumé ≤ 100 caractères.
- Avant tout commit : `uv run ruff check src tests`, `uv run ruff format src tests`, `uv run mypy`, `uv run pytest -q` doivent être verts.
- Ne pas modifier `docs/spec/SPEC-v1.1.md` : c'est la source de vérité ; toute décision qui la précise passe par un ADR dans `docs/adr/`.
