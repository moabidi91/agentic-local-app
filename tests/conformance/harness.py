"""Harnais de la batterie de conformité : le banc de la phase 9, avec la politique de correction
d'ADR-023 **désactivée** par défaut.

Chaque cas de `tests/conformance` épingle la **classification** d'une faute : le code d'erreur, ce
qui est persisté, ce qui est publié. La politique de correction (ADR-023) est orthogonale à cette
classification : elle décide ce que l'application fait *ensuite*. Exécuter la batterie avec
`protocol.max_correction_attempts = 0` isole donc ce qui est mesuré — une faute, un verdict — et
garde les assertions lisibles (« rejet puis échec » reste vrai cas par cas).

La boucle de correction elle-même a sa propre famille de cas, qui fixe `max_correction_attempts`
explicitement (`test_conformance_correction_policy.py`).
"""

from __future__ import annotations

from typing import Any

from agentic_local_app.config import AppConfig
from integration.phase9_rig import Rig
from integration.phase9_rig import make_config as _rig_make_config
from integration.phase9_rig import make_rig as _rig_make_rig

__all__ = ["CORRECTION_OFF", "make_config", "make_rig"]

#: Le réglage par défaut de la batterie : la première faute termine la session (comportement
#: d'avant ADR-023), ce que chaque cas assert cas par cas.
CORRECTION_OFF: dict[str, Any] = {"max_correction_attempts": 0}


def make_config(*args: Any, **overrides: Any) -> AppConfig:
    """`phase9_rig.make_config` avec `protocol.max_correction_attempts = 0` par défaut."""
    protocol: dict[str, Any] = {**CORRECTION_OFF, **(overrides.pop("protocol", None) or {})}
    return _rig_make_config(*args, protocol=protocol, **overrides)


def make_rig(config: AppConfig | None = None, **kwargs: Any) -> Rig:
    """`phase9_rig.make_rig` sur la configuration de :func:`make_config` par défaut."""
    return _rig_make_rig(config if config is not None else make_config(), **kwargs)
