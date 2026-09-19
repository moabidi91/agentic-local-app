"""List the reusable notes ("skills") a session can be started with (ADR-027 §4).

A skill is a markdown file the user wrote somewhere on this machine. The sign-in screen of the
desktop front offers them as a guided choice instead of a free text field, so the application only
needs to **name** them: ``GET /skills`` walks ``[skills] root`` and answers ``{name, path}`` per
file. Nothing is opened, nothing is read, nothing is sent to the model — what a session does with
the skills it was given is undecided (ADR-027, point ouvert).

The listing **never fails**: a root that is unset, missing, unreadable or not a directory answers
an empty list, exactly like a root that holds no markdown file. A sign-in screen must not be
blocked by a folder the user renamed, so there is nothing here for an interface to handle.

The walk is bounded on purpose: :data:`MAX_SKILL_DEPTH` levels below the root and
:data:`MAX_SKILLS` entries reported. Like the inventory of ADR-026, the bound applies to what is
**reported**, not to what is walked: everything is collected and sorted before being cut, so the
head of the list is deterministic (ADR-017) rather than dependent on the order the filesystem
happened to give.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from agentic_local_app.config import SkillsSection

__all__ = ["MAX_SKILLS", "MAX_SKILL_DEPTH", "SKILL_SUFFIX", "Skill", "list_skills"]

#: Extension of a skill file; a skill is a note, not a program.
SKILL_SUFFIX = ".md"
#: How far below ``[skills] root`` the walk goes (a file directly in the root is at depth 1).
MAX_SKILL_DEPTH = 3
#: How many skills are reported at most.
MAX_SKILLS = 200


class Skill(BaseModel):
    """One listed skill: ``name`` is the file stem, ``path`` its absolute path as a string."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    path: str


def list_skills(section: SkillsSection) -> list[Skill]:
    """Every markdown file under ``section.root``, sorted by name, bounded by :data:`MAX_SKILLS`.

    Returns an empty list — never raises — when the section is disabled, when ``root`` is unset,
    and whenever the filesystem refuses to answer.
    """
    if not section.enabled:
        return []
    root = section.root.strip()
    if not root:
        return []
    base = Path(os.path.abspath(os.path.expanduser(root)))
    found = _walk(base)
    found.sort(key=lambda skill: (skill.name, skill.path))
    return found[:MAX_SKILLS]


def _walk(base: Path) -> list[Skill]:
    """Breadth of the tree down to :data:`MAX_SKILL_DEPTH`; anything unreadable is skipped."""
    skills: list[Skill] = []
    pending: list[tuple[Path, int]] = [(base, 1)]
    while pending:
        directory, depth = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue  # the root is gone, or a folder is not ours to read: it simply has no skill
        for entry in entries:
            try:
                if entry.is_dir():
                    if depth < MAX_SKILL_DEPTH:
                        pending.append((Path(entry.path), depth + 1))
                elif entry.is_file() and entry.name.lower().endswith(SKILL_SUFFIX):
                    skills.append(Skill(name=Path(entry.path).stem, path=entry.path))
            except OSError:
                continue
    return skills
