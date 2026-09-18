"""The registry behind the protocol conformance battery (``tests/conformance``).

Every conformance test describes **one** thing a model can send — a malformed envelope, a message
out of sequence, a plan with impossible values, an unexpected value in a field — and asserts what
the application does with it. The :func:`case` decorator records that description next to the test
so the report generator (``tools/protocol_conformance_report.py``) can render the full matrix
without re-reading the assertions: what is sent, what the application answers, what happens to the
session, the rule it comes from, and whether the observed behaviour is what the specification and
the ADRs intend.

The decorator does not change the test: it only registers metadata and returns the function. A
duplicate identifier is an error, so the matrix and the suite cannot drift apart.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

__all__ = ["CASES", "Case", "Verdict", "case", "cases_by_category", "reset_registry"]

F = TypeVar("F", bound=Callable[..., Any])

#: How the observed behaviour compares to what the spec and the ADRs prescribe.
Verdict = Literal["conforme", "écart", "à surveiller"]


@dataclass(frozen=True)
class Case:
    """One line of the conformance matrix."""

    id: str
    category: str
    #: What the model sends (or what the transport hands back), in one short sentence.
    sends: str
    #: What the application does with it, in one short sentence.
    expects: str
    #: The ``error_code`` (``ProtocolError`` / ``TransportError``) or the accepted outcome.
    code: str
    #: What becomes of the session: "échec", "rejet puis rotation", "accepté", "accepté + avertissement"…
    policy: str
    #: The rule this case checks: spec paragraph, ADR, or both.
    ref: str
    verdict: Verdict = "conforme"
    #: Only for a verdict other than "conforme": what is off and what should be done.
    note: str = ""
    #: Set by :func:`case`: the qualified name of the test function.
    test: str = field(default="", compare=False)


CASES: list[Case] = []


def reset_registry() -> None:
    """Empty the registry (the report generator imports the modules once; tests may re-import)."""
    CASES.clear()


def case(
    id: str,
    *,
    category: str,
    sends: str,
    expects: str,
    code: str,
    policy: str,
    ref: str,
    verdict: Verdict = "conforme",
    note: str = "",
) -> Callable[[F], F]:
    """Register the conformance case a test function checks, and return the function unchanged."""

    def decorator(function: F) -> F:
        if any(existing.id == id for existing in CASES):
            raise ValueError(f"duplicate conformance case id: {id!r}")
        if verdict != "conforme" and not note:
            raise ValueError(f"case {id!r}: a verdict other than 'conforme' requires a note")
        CASES.append(
            Case(
                id=id,
                category=category,
                sends=sends,
                expects=expects,
                code=code,
                policy=policy,
                ref=ref,
                verdict=verdict,
                note=note,
                test=f"{function.__module__}.{function.__qualname__}",
            )
        )
        function.__conformance_case__ = id  # type: ignore[attr-defined]
        return function

    return decorator


def cases_by_category() -> dict[str, list[Case]]:
    """The registered cases grouped by category, categories and cases in registration order."""
    grouped: dict[str, list[Case]] = {}
    for registered in CASES:
        grouped.setdefault(registered.category, []).append(registered)
    return grouped
