"""ADR-025 — the credentials module: handing a new token to a paused session.

``credentials.py`` is the one place allowed to write the environment variable a model profile names
(``transport.token_env``, ADR-004 / ADR-024). It is a leaf: configuration in, environment out, no
component of the application involved — hence the ``phase0`` marker.

The rule that matters as much as the behaviour is the **silence**: the value never comes back, is
never logged, never lands in an event payload or in an error detail. The last section of this file
pins it by walking everything the module can be made to emit.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import pytest

from agentic_local_app.config import CredentialField, TransportSection, requires_credentials
from agentic_local_app.credentials import (
    CREDENTIALS_EMPTY,
    CREDENTIALS_NOT_CONFIGURED,
    clear_token,
    has_token,
    set_token,
    token_variable,
)
from agentic_local_app.domain.errors import ConfigError, ErrorType, Severity

pytestmark = pytest.mark.phase0

VARIABLE = "PHASE11_CREDENTIALS_TOKEN"
SECRET = "sk-live-0123456789"


def _section(**overrides: Any) -> TransportSection:
    return TransportSection(token_env=VARIABLE, **overrides)


# ================================================================================================
# set_token
# ================================================================================================
def given_profile_with_token_env_when_token_set_then_variable_written() -> None:
    environ: dict[str, str] = {}

    set_token(_section(), SECRET, environ)

    assert environ == {VARIABLE: SECRET}


def given_token_with_surrounding_blanks_when_set_then_stored_stripped() -> None:
    environ: dict[str, str] = {}

    set_token(_section(), f"  \t{SECRET}\n ", environ)

    assert environ[VARIABLE] == SECRET


def given_variable_already_set_when_token_set_then_replaced() -> None:
    environ = {VARIABLE: "expired-token"}

    set_token(_section(), SECRET, environ)

    assert environ[VARIABLE] == SECRET


@pytest.mark.parametrize("value", ["", " ", "\n\t "])
def given_blank_token_when_set_then_credentials_empty_and_nothing_written(value: str) -> None:
    environ: dict[str, str] = {}

    with pytest.raises(ConfigError) as raised:
        set_token(_section(), value, environ)

    assert raised.value.error.error_code == CREDENTIALS_EMPTY
    assert raised.value.error.error_type is ErrorType.SYSTEM_ERROR
    assert raised.value.error.severity is Severity.CRITICAL
    assert raised.value.error.details == {"variable": VARIABLE}
    assert environ == {}


@pytest.mark.parametrize("token_env", ["", "   "])
def given_profile_without_token_env_when_token_set_then_credentials_not_configured(
    token_env: str,
) -> None:
    environ: dict[str, str] = {}

    with pytest.raises(ConfigError) as raised:
        set_token(TransportSection(token_env=token_env), SECRET, environ)

    assert raised.value.error.error_code == CREDENTIALS_NOT_CONFIGURED
    assert raised.value.error.details == {"field": "token_env", "provider": "generic_http"}
    assert environ == {}


def given_no_environ_given_when_token_set_then_process_environment_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(VARIABLE, raising=False)
    section = _section()

    set_token(section, SECRET)

    try:
        assert os.environ[VARIABLE] == SECRET
        assert section.token == SECRET  # what the transport reads at call time (ADR-004)
    finally:
        os.environ.pop(VARIABLE, None)


# ================================================================================================
# clear_token / has_token
# ================================================================================================
def given_variable_set_when_token_cleared_then_variable_removed() -> None:
    environ = {VARIABLE: SECRET, "OTHER": "kept"}

    clear_token(_section(), environ)

    assert environ == {"OTHER": "kept"}


def given_variable_absent_when_token_cleared_then_nothing_happens() -> None:
    environ: dict[str, str] = {}

    clear_token(_section(), environ)

    assert environ == {}


def given_profile_without_token_env_when_token_cleared_then_credentials_not_configured() -> None:
    with pytest.raises(ConfigError) as raised:
        clear_token(TransportSection(token_env=""), {})

    assert raised.value.error.error_code == CREDENTIALS_NOT_CONFIGURED


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({VARIABLE: SECRET}, True),
        ({VARIABLE: f"  {SECRET}  "}, True),
        ({VARIABLE: ""}, False),
        ({VARIABLE: "   "}, False),
        ({}, False),
    ],
)
def given_environment_when_token_probed_then_presence_reported(
    environ: dict[str, str], expected: bool
) -> None:
    assert has_token(_section(), environ) is expected


def given_profile_without_token_env_when_token_probed_then_false_and_no_credentials_needed() -> (
    None
):
    """A profile that names no variable holds no token — and needs none (ADR-024)."""
    section = TransportSection(token_env="")
    assert has_token(section, {}) is False
    assert requires_credentials(section, {}) is False


def given_paused_session_flow_when_token_provided_then_profile_stops_requiring_credentials() -> (
    None
):
    """The sequence the interface runs after a 401: probe, set, probe again (ADR-025)."""
    section = _section()
    environ: dict[str, str] = {}
    assert has_token(section, environ) is False
    assert requires_credentials(section, environ) is True

    set_token(section, SECRET, environ)

    assert has_token(section, environ) is True
    assert requires_credentials(section, environ) is False


def given_token_env_name_when_read_then_stripped_and_refused_when_absent() -> None:
    assert token_variable(TransportSection(token_env=f"  {VARIABLE}  ")) == VARIABLE
    with pytest.raises(ConfigError):
        token_variable(TransportSection(token_env=" "))


# ================================================================================================
# the value is never journalled (ADR-025)
# ================================================================================================
def given_token_operations_when_performed_then_nothing_logs_the_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    section = _section()
    environ: dict[str, str] = {}

    with caplog.at_level(logging.DEBUG):
        set_token(section, SECRET, environ)
        assert has_token(section, environ) is True
        clear_token(section, environ)
        with pytest.raises(ConfigError):
            set_token(section, "", environ)

    assert caplog.records == []


def given_refusals_when_rendered_then_no_error_carries_the_value() -> None:
    """Neither refusal quotes the token, in its details, its message or its JSON form."""
    environ: dict[str, str] = {}
    for value, section in ((SECRET, TransportSection(token_env="")), ("", _section())):
        with pytest.raises(ConfigError) as raised:
            set_token(section, value, environ)
        error = raised.value.error
        rendered = f"{error.model_dump_json()} {raised.value}"
        assert SECRET not in rendered
        assert set(error.details) <= {"variable", "field", "provider"}


def given_module_when_inspected_then_it_exposes_no_reader_of_the_value() -> None:
    """Nothing in the module returns the token: the transport reads it from the environment."""
    from agentic_local_app import credentials

    assert set(credentials.__all__) == {
        "CREDENTIALS_EMPTY",
        "CREDENTIALS_NOT_CONFIGURED",
        "clear_token",
        "has_token",
        "set_token",
        "token_variable",
    }
    assert set_token(_section(), SECRET, {}) is None
    assert clear_token(_section(), {}) is None
    assert has_token(_section(), {VARIABLE: SECRET}) is True  # a bool, not the value


# ================================================================================================
# several declared fields (ADR-027 §1): one value, one variable
# ================================================================================================
CHAT_VARIABLE = "PHASE11_CREDENTIALS_CHAT"
CHAT_ID = "chat-42"

MULTI_FIELD = TransportSection(
    token_env=VARIABLE,
    credential_fields=[
        CredentialField(key="access_token", label="Access token", env=VARIABLE),
        CredentialField(key="chat_id", label="Chat id", secret=False, env=CHAT_VARIABLE),
    ],
)


def given_a_field_variable_when_token_set_then_that_variable_is_written() -> None:
    """``variable=`` is the ``env`` of one declared field; ``token_env`` is only the default."""
    environ: dict[str, str] = {}

    set_token(MULTI_FIELD, CHAT_ID, environ, variable=CHAT_VARIABLE)

    assert environ == {CHAT_VARIABLE: CHAT_ID}


def given_every_field_when_written_then_each_value_lands_in_its_own_variable() -> None:
    environ: dict[str, str] = {}
    assert requires_credentials(MULTI_FIELD, environ) is True

    set_token(MULTI_FIELD, f"  {SECRET} ", environ, variable=VARIABLE)
    assert requires_credentials(MULTI_FIELD, environ) is True  # one field still empty
    set_token(MULTI_FIELD, CHAT_ID, environ, variable=CHAT_VARIABLE)

    assert environ == {VARIABLE: SECRET, CHAT_VARIABLE: CHAT_ID}
    assert requires_credentials(MULTI_FIELD, environ) is False


@pytest.mark.parametrize("value", ["", "   "])
def given_a_blank_value_for_a_field_when_set_then_credentials_empty_and_nothing_written(
    value: str,
) -> None:
    environ: dict[str, str] = {}

    with pytest.raises(ConfigError) as raised:
        set_token(MULTI_FIELD, value, environ, variable=CHAT_VARIABLE)

    assert raised.value.error.error_code == CREDENTIALS_EMPTY
    assert raised.value.error.details == {"variable": CHAT_VARIABLE}
    assert environ == {}


@pytest.mark.parametrize("variable", ["", "   "])
def given_a_blank_variable_when_token_set_then_credentials_not_configured(variable: str) -> None:
    environ: dict[str, str] = {}

    with pytest.raises(ConfigError) as raised:
        set_token(MULTI_FIELD, SECRET, environ, variable=variable)

    assert raised.value.error.error_code == CREDENTIALS_NOT_CONFIGURED
    assert raised.value.error.details == {"field": "env", "provider": "generic_http"}
    assert environ == {}


def given_several_values_when_written_then_no_value_is_logged_or_rendered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The rule of silence holds field by field, not only for the token (ADR-027 §3)."""
    environ: dict[str, str] = {}

    with caplog.at_level(logging.DEBUG):
        set_token(MULTI_FIELD, SECRET, environ, variable=VARIABLE)
        set_token(MULTI_FIELD, CHAT_ID, environ, variable=CHAT_VARIABLE)
        with pytest.raises(ConfigError) as raised:
            set_token(MULTI_FIELD, "  ", environ, variable=CHAT_VARIABLE)

    rendered = f"{raised.value.error.model_dump_json()} {raised.value} {caplog.text}"
    assert SECRET not in rendered and CHAT_ID not in rendered
