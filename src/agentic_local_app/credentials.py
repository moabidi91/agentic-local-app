"""Set, clear and probe the credentials of a model profile (ADR-004, ADR-024, ADR-025, ADR-027).

The token is **never** part of the configuration file (ADR-004): ``transport.token_env`` names an
environment variable and the transport reads that variable at call time. This module is the only
place allowed to write it, so that an interface asking the user for a new token after a 401
(ADR-025) has one obvious, testable way to hand it over.

**The value is never journalled.** Nothing here logs it, returns it, puts it in an event payload or
in an error detail; the module holds no copy of it and exposes no reader. Only the **name** of the
variable travels — it is already public (it is written in ``config.toml`` and shown by
``ModelProfileView``) — and :func:`has_token` answers about a token's presence, never its content.
That rule is the point of the module and it is pinned by a test.

Every function takes the ``TransportSection`` of the profile to act on (``config.transport`` is the
active one, ADR-024) and an optional ``environ``, which defaults to ``os.environ`` — inject a plain
dictionary to change nothing in the process.

A profile may need more than a token (ADR-027 §1): ``config.declared_credential_fields`` says which
variables it declares, and :func:`set_token` takes the one to write as ``variable=``. The rule of
silence is the same for every one of them.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping

from agentic_local_app.config import TransportSection
from agentic_local_app.domain.errors import ConfigError

__all__ = [
    "CREDENTIALS_EMPTY",
    "CREDENTIALS_NOT_CONFIGURED",
    "clear_token",
    "has_token",
    "set_token",
    "token_variable",
]

#: ``error_code`` of a token that is empty, or blank once stripped.
CREDENTIALS_EMPTY = "CREDENTIALS_EMPTY"
#: ``error_code`` of a profile that names no environment variable to hold a token.
CREDENTIALS_NOT_CONFIGURED = "CREDENTIALS_NOT_CONFIGURED"


def token_variable(section: TransportSection) -> str:
    """The name of the variable holding the token of ``section``.

    :raises ConfigError: ``CREDENTIALS_NOT_CONFIGURED`` when the profile names none — it takes no
        token at all, so there is nothing to write or to clear.
    """
    variable = section.token_env.strip()
    if not variable:
        raise ConfigError(CREDENTIALS_NOT_CONFIGURED, field="token_env", provider=section.provider)
    return variable


def set_token(
    section: TransportSection,
    value: str,
    environ: MutableMapping[str, str] | None = None,
    *,
    variable: str | None = None,
) -> None:
    """Write ``value``, stripped, to ``variable`` — by default the one named by ``token_env``.

    ``variable`` is the ``env`` of one declared credential field (ADR-027 §1): a profile may need
    more than a token, and each posted value goes to its own environment variable. Left out, the
    profile's ``token_env`` is used, which is exactly the implicit ``access_token`` field.

    :raises ConfigError: ``CREDENTIALS_NOT_CONFIGURED`` when no variable can be named,
        ``CREDENTIALS_EMPTY`` when ``value`` is empty or blank. Neither error carries the value.
    """
    target = _target_variable(section, variable)
    token = value.strip()
    if not token:
        raise ConfigError(CREDENTIALS_EMPTY, variable=target)
    _environ(environ)[target] = token


def clear_token(section: TransportSection, environ: MutableMapping[str, str] | None = None) -> None:
    """Remove the variable named by ``section.token_env``; absent is not an error.

    :raises ConfigError: ``CREDENTIALS_NOT_CONFIGURED`` when the profile names no variable.
    """
    _environ(environ).pop(token_variable(section), None)


def has_token(section: TransportSection, environ: MutableMapping[str, str] | None = None) -> bool:
    """Whether a non-blank token is available for ``section`` **right now**.

    ``False`` for a profile that names no variable: it holds no token — it needs none either, which
    is what ``config.requires_credentials`` answers.
    """
    variable = section.token_env.strip()
    if not variable:
        return False
    return bool(_environ(environ).get(variable, "").strip())


def _target_variable(section: TransportSection, variable: str | None) -> str:
    """The variable to write: an explicit credential field's ``env``, or the profile's token."""
    if variable is None:
        return token_variable(section)
    named = variable.strip()
    if not named:
        raise ConfigError(CREDENTIALS_NOT_CONFIGURED, field="env", provider=section.provider)
    return named


def _environ(environ: MutableMapping[str, str] | None) -> MutableMapping[str, str]:
    return os.environ if environ is None else environ
