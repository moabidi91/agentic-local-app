"""Render the protocol conformance report from the battery of ``tests/conformance``.

Usage (from the repository root)::

    uv run python tools/protocol_conformance_report.py            # writes docs/reports/conformance-protocole.md
    uv run python tools/protocol_conformance_report.py --check    # exit 1 when the file is stale

The report is **generated**: every table comes from the cases the tests register
(:mod:`tests.conformance.registry`), so a case that is not tested cannot appear in the report, and
a test whose behaviour changes without its case being updated shows up in review as a diff of this
file. The prose around the tables lives in this script.
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs" / "reports" / "conformance-protocole.md"
PACKAGE = "conformance"

VERDICT_MARK = {"conforme": "✅", "à surveiller": "⚠️", "écart": "❌"}


def _load_cases() -> list[object]:
    """Import every conformance module (which registers its cases) and return the registry."""
    sys.path.insert(0, str(ROOT / "tests"))
    registry = importlib.import_module(f"{PACKAGE}.registry")
    registry.reset_registry()
    package = importlib.import_module(PACKAGE)
    for module in sorted(
        info.name
        for info in pkgutil.iter_modules(package.__path__)
        if info.name.startswith("test_")
    ):
        importlib.import_module(f"{PACKAGE}.{module}")
    return list(registry.CASES)


def _table(rows: list[list[str]], header: list[str]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(cell.replace("|", "\\|") for cell in row) + " |" for row in rows]
    return "\n".join(lines)


def render(cases: list) -> str:  # type: ignore[type-arg]
    total = len(cases)
    by_verdict = Counter(case.verdict for case in cases)
    by_category: dict[str, list] = {}  # type: ignore[type-arg]
    for case in cases:
        by_category.setdefault(case.category, []).append(case)

    summary = _table(
        [
            [
                category,
                str(len(items)),
                str(sum(1 for item in items if item.verdict == "conforme")),
                str(sum(1 for item in items if item.verdict == "à surveiller")),
                str(sum(1 for item in items if item.verdict == "écart")),
            ]
            for category, items in by_category.items()
        ],
        ["Famille de cas", "Cas", "✅ conforme", "⚠️ à surveiller", "❌ écart"],
    )

    sections = []
    for category, items in by_category.items():
        rows = [
            [
                f"`{item.id}`",
                item.sends,
                item.expects,
                f"`{item.code}`",
                item.policy,
                item.ref,
                VERDICT_MARK[item.verdict],
            ]
            for item in items
        ]
        sections.append(
            f"### {category}\n\n"
            + _table(
                rows,
                [
                    "Cas",
                    "Ce que le modèle envoie",
                    "Ce que fait l'application",
                    "Code",
                    "Suite",
                    "Règle",
                    "",
                ],
            )
        )

    flagged = [case for case in cases if case.verdict != "conforme"]
    if flagged:
        findings = "\n".join(
            f"- {VERDICT_MARK[case.verdict]} **`{case.id}`** ({case.category}) — {case.note}"
            for case in flagged
        )
    else:
        findings = "Aucun écart : tous les cas de la matrice se comportent comme la spécification et les ADR le prescrivent."

    generated = datetime.now(UTC).strftime("%Y-%m-%d")
    return f"""<!-- Fichier généré par tools/protocol_conformance_report.py — ne pas éditer à la main. -->

# Rapport de conformité protocolaire — comportement de l'application face à un modèle qui se trompe

**Généré le {generated}** à partir de la batterie `tests/conformance` ({total} cas, tous exécutés à chaque `pytest`).

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

1. **Une réponse qui n'est pas un objet JSON faisait planter la boucle** (`env-array-reply`, `env-string-reply`, `env-scalar-reply`). `_persist_rejected` construisait le payload du rejet avec `dict(reply.messages[0])` : un tableau, une chaîne nue ou un nombre levaient `TypeError` / `ValueError` **avant** toute persistance. Le résultat était un `SYSTEM_ERROR / UNHANDLED_EXCEPTION` remontant jusqu'à l'appelant, sans `message.rejected`, sans trace de ce que le modèle avait envoyé, et `protocol_error_count` inchangé — la faute la plus banale d'un modèle bavard était aussi la moins bien traitée. La forme brute est maintenant conservée telle quelle (`{{"raw": …}}`) et le rejet suit la politique 1 (`SCHEMA_INVALID`).
2. **Un lot vide rendu par un GET faisait planter la boucle** (`env-empty-batch`). `wait_for_reply` promet au moins un message ; `parse_inbound([])` levait donc un `ValueError` de programmation. Un provider tiers (ADR-020) dont le long-poll rend une page vide tombait dessus. C'est désormais une `MODEL_PROTOCOL_ERROR / EMPTY_REPLY`, traitée comme toute réponse inutilisable.
3. **Le rejet de l'ACK d'une rotation ne laissait pas de trace** (`seq-rejected-ack-trail`). Le `RotationCoordinator` comptait l'erreur et publiait `message.rejected`, mais n'écrivait aucun `MessageRecord` — la réponse fautive était invisible après coup, contrairement à tous les autres rejets — et le cycle `resume` de la conversation enfant restait `RUNNING` derrière une session `FAILED`. Le rejet est maintenant persisté comme ailleurs et le cycle clos en `FAILED` avec son `cycle.ended`.

## 4. Vue d'ensemble

{summary}

**Total : {total} cas — {by_verdict.get("conforme", 0)} conformes, {by_verdict.get("à surveiller", 0)} à surveiller, {by_verdict.get("écart", 0)} écarts.**

## 5. La matrice

{chr(10).join(sections)}

## 6. Constats et recommandations

{findings}

## 7. Comment rejouer la batterie

```bash
uv run pytest -m conformance -q                     # la batterie seule
uv run pytest -q                                    # toute la suite, batterie comprise
uv run python tools/protocol_conformance_report.py  # régénérer ce rapport
```

Ajouter un cas : écrire le test dans `tests/conformance/`, le décorer avec `@case(...)` (identifiant, famille, ce qui est envoyé, ce que fait l'application, code, suite, règle, verdict), puis régénérer le rapport. Un identifiant en double est une erreur, et un verdict autre que « conforme » exige une note : la matrice et la suite ne peuvent pas diverger.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="exit 1 when the report is stale")
    args = parser.parse_args()

    content = render(_load_cases())
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    if args.check:
        current = REPORT.read_text(encoding="utf-8") if REPORT.exists() else ""

        # the generation date changes every day: compare everything but that line
        def strip_date(text: str) -> str:
            return "\n".join(
                line for line in text.splitlines() if not line.startswith("**Généré le ")
            )

        if strip_date(current) != strip_date(content):
            print(f"{REPORT.relative_to(ROOT)} is stale: regenerate it", file=sys.stderr)
            return 1
        print(f"{REPORT.relative_to(ROOT)} is up to date")
        return 0
    REPORT.write_text(content, encoding="utf-8")
    print(f"wrote {REPORT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
