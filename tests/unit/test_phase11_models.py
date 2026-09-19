"""ADR-024 — named model profiles and machine identity (the start-up contract of the front).

Two things a user interface needs before anything else happens: **which models exist** (name,
presentation, whether a token is missing) and **who the user is** on this machine. Both are decided
once, at load time and at wiring time: the model of a process never changes (changing model means
restarting with another ``models.active``), and the identity is resolved with an injected
environment and an injected command runner, so nothing here spawns a process or reads the real
machine unless a test says so.

Marked ``phase0``: profiles are configuration (``config.py``), identity is a leaf module with no
dependency on the rest of the application.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from agentic_local_app import identity as identity_module
from agentic_local_app import skills as skills_module
from agentic_local_app.config import (
    DEFAULT_CREDENTIAL_KEY,
    DEFAULT_CREDENTIAL_LABEL,
    DEFAULT_CREDENTIAL_PLACEHOLDER,
    DEFAULT_MODEL_PROFILE,
    AppConfig,
    AppSection,
    CredentialField,
    CredentialFieldView,
    ModelProfileView,
    SkillsSection,
    TransportSection,
    credential_field_views,
    declared_credential_fields,
    load_config,
    requires_credentials,
)
from agentic_local_app.domain.clock import FakeClock
from agentic_local_app.domain.dialects import ShellTranslator
from agentic_local_app.domain.errors import ConfigError
from agentic_local_app.domain.ids import SequentialIdGenerator
from agentic_local_app.domain.shell import ShellDialect
from agentic_local_app.identity import (
    SOURCE_CONFIG,
    SOURCE_UNKNOWN,
    UNKNOWN_USER,
    UserIdentity,
    current_user,
    run_identity_command,
)
from agentic_local_app.orchestration import Application, build_application
from agentic_local_app.persistence.memory import InMemoryConversationStore
from agentic_local_app.skills import MAX_SKILLS, Skill, list_skills
from agentic_local_app.testing.fake_executor import FakeCommandExecutor

pytestmark = pytest.mark.phase0

TOKEN_VARIABLE = "PHASE11_MODEL_TOKEN"

TWO_PROFILES = """
[transport]
user_id = "from-transport"

[models]
active = "mock"

[models.mock]
provider = "fake"
codec = "passthrough"
init_url = "http://127.0.0.1:9000/v1/conversations"
token_env = ""
display_name = "Mock local"
description = "Built-in test server"

[models.claude]
provider = "templated_http"
codec = "json_text"
token_env = "PHASE11_MODEL_TOKEN"
display_name = "Claude (chat completions)"
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def _load(tmp_path: Path, text: str, **environ: str) -> AppConfig:
    return load_config(_write(tmp_path, text), environ=dict(environ), load_env_file=False)


def _runner_answering(answer: str | None) -> Any:
    def runner(command: list[str]) -> str | None:
        return answer

    return runner


def _host() -> str | None:
    return "workstation"


# ================================================================================================
# 1. profiles in the configuration
# ================================================================================================
def given_no_models_section_when_config_loaded_then_transport_is_the_implicit_active_profile(
    tmp_path: Path,
) -> None:
    """Backward compatibility: a configuration written before ADR-024 keeps working unchanged."""
    config = _load(tmp_path, '[transport]\nuser_id = "solo"\n')
    assert config.models.active == DEFAULT_MODEL_PROFILE
    assert list(config.models.profiles) == [DEFAULT_MODEL_PROFILE]
    assert config.models.profiles[DEFAULT_MODEL_PROFILE] is config.transport
    assert config.transport.user_id == "solo"
    assert config.active_transport is config.transport


def given_models_section_when_config_loaded_then_every_profile_validated_like_transport(
    tmp_path: Path,
) -> None:
    config = _load(tmp_path, TWO_PROFILES)
    assert sorted(config.models.profiles) == ["claude", "mock"]
    assert isinstance(config.models.profiles["claude"], TransportSection)
    claude = config.models.profiles["claude"]
    assert (claude.provider, claude.codec, claude.token_env) == (
        "templated_http",
        "json_text",
        TOKEN_VARIABLE,
    )
    # a profile inherits nothing from [transport]: it is complete, on the defaults of the section
    assert claude.user_id == TransportSection().user_id != "from-transport"


def given_active_profile_when_config_loaded_then_transport_resolves_to_that_profile(
    tmp_path: Path,
) -> None:
    """The resolution every other module relies on: ``config.transport`` *is* the active profile."""
    config = _load(tmp_path, TWO_PROFILES)
    assert config.transport is config.models.profiles["mock"]
    assert config.active_transport is config.transport
    assert (config.transport.provider, config.transport.codec) == ("fake", "passthrough")


def given_profile_with_invalid_url_when_config_loaded_then_config_invalid_at_its_location(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, '[models]\nactive = "x"\n[models.x]\nget_url = "http://h/{after}"\n')
    assert exc.value.error.error_code == "CONFIG_INVALID"
    locations = [error["loc"] for error in exc.value.error.details["errors"]]
    assert ("models", "profiles", "x", "get_url") in locations


def given_unknown_active_profile_when_config_loaded_then_refused_with_available_names(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, TWO_PROFILES.replace('active = "mock"', 'active = "gpt"'))
    assert exc.value.error.error_code == "MODEL_PROFILE_UNKNOWN"
    assert exc.value.error.details == {"model": "gpt", "available": ["claude", "mock"]}


def given_blank_active_profile_when_config_loaded_then_refused(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, '[models]\nactive = "  "\n[models.mock]\nprovider = "fake"\n')
    assert exc.value.error.error_code == "CONFIG_INVALID"


def given_environment_override_when_config_loaded_then_named_profile_becomes_active(
    tmp_path: Path,
) -> None:
    """``AGENTIC__MODELS__ACTIVE`` is the start-up switch: one model per process (ADR-024)."""
    config = _load(tmp_path, TWO_PROFILES, AGENTIC__MODELS__ACTIVE="claude")
    assert config.models.active == "claude"
    assert config.transport.provider == "templated_http"
    assert config.transport.token_env == TOKEN_VARIABLE


def given_environment_override_without_models_section_when_config_loaded_then_only_default_exists(
    tmp_path: Path,
) -> None:
    assert (
        _load(tmp_path, "[transport]\n", AGENTIC__MODELS__ACTIVE="default").models.active
        == DEFAULT_MODEL_PROFILE
    )
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, "[transport]\n", AGENTIC__MODELS__ACTIVE="claude")
    assert exc.value.error.details == {"model": "claude", "available": [DEFAULT_MODEL_PROFILE]}


def given_profile_options_when_config_masked_then_masked_like_transport_options(
    tmp_path: Path,
) -> None:
    config = _load(
        tmp_path,
        TWO_PROFILES
        + """
[models.claude.options]
workspace = "demo"
api_key = "${env:PHASE11_MODEL_TOKEN}"
[models.claude.options.headers]
Authorization = "Bearer ${env:PHASE11_MODEL_TOKEN}"
[models.claude.codec_options]
content_path = "choices[0].message.content"
""",
    )
    masked = config.masked()["models"]["profiles"]["claude"]
    assert masked["options"] == {
        "workspace": "demo",
        "api_key": "***",
        "headers": {"Authorization": "***"},
    }
    assert masked["codec_options"] == {"content_path": "choices[0].message.content"}
    assert TOKEN_VARIABLE not in str(masked["options"])


# ================================================================================================
# 2. the catalogue served to the interfaces (ModelProfileView)
# ================================================================================================
def given_token_variable_unset_when_views_built_then_profile_requires_credentials(
    tmp_path: Path,
) -> None:
    views = {view.name: view for view in _load(tmp_path, TWO_PROFILES).profile_views(environ={})}
    assert views["claude"].requires_credentials is True
    assert views["claude"].display_name == "Claude (chat completions)"
    assert views["mock"].requires_credentials is False  # token_env = "" : no token at all
    assert views["mock"].description == "Built-in test server"


def given_token_variable_set_when_views_built_then_profile_does_not_require_credentials(
    tmp_path: Path,
) -> None:
    config = _load(tmp_path, TWO_PROFILES)
    environ = {TOKEN_VARIABLE: "s3cr3t"}
    views = {view.name: view for view in config.profile_views(environ)}
    assert views["claude"].requires_credentials is False
    # a variable set to blank is not a credential
    assert requires_credentials(config.models.profiles["claude"], {TOKEN_VARIABLE: "  "}) is True


def given_several_profiles_when_views_built_then_active_first_then_sorted_by_name(
    tmp_path: Path,
) -> None:
    config = _load(tmp_path, TWO_PROFILES + '\n[models.alpha]\nprovider = "fake"\n')
    views = config.profile_views(environ={})
    assert [view.name for view in views] == ["mock", "alpha", "claude"]
    assert [view.active for view in views] == [True, False, False]
    assert isinstance(views[0], ModelProfileView)
    assert views[0].provider == "fake" and views[0].codec == "passthrough"


def given_no_models_section_when_views_built_then_one_active_default_entry(tmp_path: Path) -> None:
    views = _load(tmp_path, "[transport]\n").profile_views(environ={})
    assert [(view.name, view.active, view.display_name) for view in views] == [
        (DEFAULT_MODEL_PROFILE, True, None)
    ]


# ================================================================================================
# 3. machine identity — each resolution step in isolation (no process, no real environment)
# ================================================================================================
def given_user_variable_when_identity_resolved_then_read_from_the_environment() -> None:
    who = current_user(
        {"USER": "alice", "LOGNAME": "ignored"}, _runner_answering(None), hostname=_host
    )
    assert who == UserIdentity(user_id="alice", source="env:USER", host="workstation")


def given_no_user_variable_when_identity_resolved_then_logname_answers() -> None:
    who = current_user({"LOGNAME": "bob"}, _runner_answering("never"), hostname=_host)
    assert (who.user_id, who.source) == ("bob", "env:LOGNAME")


def given_no_environment_variable_when_identity_resolved_then_the_command_answers() -> None:
    seen: list[list[str]] = []

    def runner(command: list[str]) -> str | None:
        seen.append(command)
        return "  carol\n"

    who = current_user({}, runner, hostname=_host)
    assert (who.user_id, who.source) == ("carol", "cmd:id")
    assert seen == [["id", "-un"]]


def given_windows_platform_when_identity_resolved_then_username_is_the_first_step() -> None:
    who = current_user(
        {"USERNAME": "dana", "USER": "ignored"},
        _runner_answering("never"),
        platform="win32",
        hostname=_host,
    )
    assert (who.user_id, who.source) == ("dana", "env:USERNAME")


def given_windows_whoami_with_domain_when_identity_resolved_then_only_the_user_is_kept() -> None:
    seen: list[list[str]] = []

    def runner(command: list[str]) -> str | None:
        seen.append(command)
        return "CORP\\erin\r\n"

    who = current_user({}, runner, platform="win32", hostname=_host)
    assert (who.user_id, who.source) == ("erin", "cmd:whoami")
    assert seen == [["whoami"]]


def given_runner_that_fails_when_identity_resolved_then_configured_user_id_is_the_fallback() -> (
    None
):
    who = current_user({}, _runner_answering(None), fallback_user_id=" local-user ", hostname=_host)
    assert who == UserIdentity(user_id="local-user", source=SOURCE_CONFIG, host="workstation")


def given_runner_answering_blank_when_identity_resolved_then_the_step_is_skipped() -> None:
    who = current_user({"USER": "   "}, _runner_answering(" \n "), fallback_user_id="cfg")
    assert (who.user_id, who.source) == ("cfg", SOURCE_CONFIG)


def given_nothing_at_all_when_identity_resolved_then_unknown_is_the_floor() -> None:
    who = current_user({}, _runner_answering(None), fallback_user_id="   ", hostname=_host)
    assert (who.user_id, who.source) == (UNKNOWN_USER, SOURCE_UNKNOWN)


def given_hostname_resolver_without_answer_when_identity_resolved_then_host_is_none() -> None:
    who = current_user({"USER": "alice"}, _runner_answering(None), hostname=lambda: None)
    assert who.host is None and who.user_id == "alice"


def given_identity_when_built_then_frozen() -> None:
    who = UserIdentity(user_id="alice", source="env:USER")
    with pytest.raises(ValueError):
        who.user_id = "mallory"  # type: ignore[misc]


# ---------------------------------------------------------------- the default runner ---------
def given_unknown_command_when_default_runner_used_then_none_returned() -> None:
    assert run_identity_command(["phase11-no-such-command"]) is None


def given_command_exiting_non_zero_when_default_runner_used_then_none_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    completed = subprocess.CompletedProcess(args=["id", "-un"], returncode=1, stdout="noise")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: completed)
    assert run_identity_command(["id", "-un"]) is None


def given_command_timing_out_when_default_runner_used_then_none_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*args: Any, **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(cmd="whoami", timeout=1.0)

    monkeypatch.setattr(subprocess, "run", boom)
    assert run_identity_command(["whoami"]) is None


def given_no_injection_at_all_when_identity_resolved_then_this_machine_answers() -> None:
    """The production path, on the real machine: always an answer, always a named source."""
    who = current_user()
    assert who.user_id and who.source
    assert who.source.startswith(("env:", "cmd:")) or who.source in {SOURCE_CONFIG, SOURCE_UNKNOWN}


# ================================================================================================
# 4. wiring — what the interfaces read on the Application (ADR-024 §3)
# ================================================================================================
def _build(config: AppConfig, **kwargs: Any) -> Application:
    clock = FakeClock()
    return build_application(
        config,
        store=InMemoryConversationStore(),
        executor=FakeCommandExecutor(clock),
        clock=clock,
        ids=SequentialIdGenerator(),
        run_recovery=False,
        translator=ShellTranslator(ShellDialect.POSIX),  # scripted machine, ADR-030
        **kwargs,
    )


def given_profiles_when_application_built_then_catalogue_exposed_with_the_active_one_first(
    tmp_path: Path,
) -> None:
    config = _load(tmp_path, TWO_PROFILES).model_copy(
        update={"app": AppSection(data_dir=str(tmp_path / "data"))}
    )
    app = _build(config, identity=UserIdentity(user_id="alice", source="test"))
    try:
        assert [(view.name, view.active) for view in app.models] == [
            ("mock", True),
            ("claude", False),
        ]
        assert app.identity == UserIdentity(user_id="alice", source="test")
    finally:
        app.close()


def given_no_identity_injected_when_application_built_then_configured_user_id_is_the_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for variable in ("USER", "LOGNAME", "USERNAME"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(identity_module, "run_identity_command", _runner_answering(None))
    config = AppConfig(
        app=AppSection(data_dir=str(tmp_path / "data")),
        transport=TransportSection(provider="fake", user_id="wired-user"),
    )
    app = _build(config)
    try:
        assert (app.identity.user_id, app.identity.source) == ("wired-user", SOURCE_CONFIG)
        assert [view.name for view in app.models] == [DEFAULT_MODEL_PROFILE]
        assert app.models[0].active is True
    finally:
        app.close()


# ================================================================================================
# 5. credential fields declared by a profile (ADR-027 §1)
# ================================================================================================
TWO_FIELDS = """
[transport]
token_env = "PHASE11_MODEL_TOKEN"

[[transport.credential_fields]]
key = "access_token"
label = "Jeton d'accès"
placeholder = "acme-…"
env = "PHASE11_FIELD_TOKEN"

[[transport.credential_fields]]
key = "chat_id"
label = "Identifiant de conversation"
secret = false
env = "PHASE11_FIELD_CHAT"
"""

FIELD_TOKEN = "PHASE11_FIELD_TOKEN"
FIELD_CHAT = "PHASE11_FIELD_CHAT"


def given_declared_fields_when_config_loaded_then_kept_in_declaration_order(
    tmp_path: Path,
) -> None:
    fields = _load(tmp_path, TWO_FIELDS).transport.credential_fields
    assert fields is not None
    assert [field.key for field in fields] == ["access_token", "chat_id"]
    assert fields[0] == CredentialField(
        key="access_token",
        label="Jeton d'accès",
        placeholder="acme-…",
        secret=True,  # not declared: fails closed
        env=FIELD_TOKEN,
    )
    assert fields[1].placeholder is None and fields[1].secret is False


def given_a_field_without_secret_when_built_then_it_defaults_to_true() -> None:
    """ADR-027 §1: ``secret`` decides whether an interface may keep the value on disk."""
    assert CredentialField(key="k", label="K", env="V").secret is True


@pytest.mark.parametrize(
    ("declaration", "location"),
    [
        ('key = ""\nlabel = "K"\nenv = "V"', "key"),
        ('key = "  "\nlabel = "K"\nenv = "V"', "key"),
        ('key = "chat id"\nlabel = "K"\nenv = "V"', "key"),
        ('key = "chat-id"\nlabel = "K"\nenv = "V"', "key"),
        ('key = "2fa"\nlabel = "K"\nenv = "V"', "key"),
        ('key = "k"\nlabel = "K"\nenv = ""', "env"),
        ('key = "k"\nlabel = "K"\nenv = "   "', "env"),
    ],
)
def given_an_invalid_credential_field_when_config_loaded_then_refused_at_its_location(
    tmp_path: Path, declaration: str, location: str
) -> None:
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, f"[[transport.credential_fields]]\n{declaration}\n")
    assert exc.value.error.error_code == "CONFIG_INVALID"
    locations = [error["loc"] for error in exc.value.error.details["errors"]]
    assert ("transport", "credential_fields", 0, location) in locations


def given_a_missing_env_when_config_loaded_then_refused(tmp_path: Path) -> None:
    """``env`` is what the application acts on: a field without it would write nowhere."""
    with pytest.raises(ConfigError) as exc:
        _load(tmp_path, '[[transport.credential_fields]]\nkey = "k"\nlabel = "K"\n')
    assert exc.value.error.error_code == "CONFIG_INVALID"


def given_two_fields_sharing_a_key_when_config_loaded_then_refused(tmp_path: Path) -> None:
    """The posted object is keyed by ``key``: a duplicate would make one value win silently."""
    with pytest.raises(ConfigError) as exc:
        _load(
            tmp_path,
            '[[transport.credential_fields]]\nkey = "k"\nlabel = "A"\nenv = "A"\n'
            '[[transport.credential_fields]]\nkey = "k"\nlabel = "B"\nenv = "B"\n',
        )
    assert exc.value.error.error_code == "CONFIG_INVALID"
    assert any(
        "declared twice" in str(error.get("msg", "")) for error in exc.value.error.details["errors"]
    )


def given_a_profile_without_declaration_when_fields_read_then_implicit_access_token(
    tmp_path: Path,
) -> None:
    """ADR-027 §1: the fallback a front already applies to a bare ``requires_credentials``."""
    section = _load(tmp_path, '[transport]\ntoken_env = "PHASE11_MODEL_TOKEN"\n').transport
    assert section.credential_fields is None
    assert declared_credential_fields(section) == [
        CredentialField(
            key=DEFAULT_CREDENTIAL_KEY,
            label=DEFAULT_CREDENTIAL_LABEL,
            placeholder=DEFAULT_CREDENTIAL_PLACEHOLDER,
            secret=True,
            env=TOKEN_VARIABLE,
        )
    ]


def given_a_profile_without_token_env_when_fields_read_then_empty(tmp_path: Path) -> None:
    section = _load(tmp_path, '[transport]\ntoken_env = ""\n').transport
    assert declared_credential_fields(section) == []


def given_an_explicitly_empty_declaration_when_fields_read_then_empty() -> None:
    """An empty list is a declaration ("this profile needs nothing"), not an absence."""
    section = TransportSection(token_env=TOKEN_VARIABLE, credential_fields=[])
    assert declared_credential_fields(section) == []
    assert requires_credentials(section, {}) is False


def given_declared_fields_when_read_then_they_replace_token_env(tmp_path: Path) -> None:
    """The point of the declaration: ``token_env`` is not what this provider reads."""
    section = _load(tmp_path, TWO_FIELDS).transport
    assert section.token_env == TOKEN_VARIABLE
    assert [field.env for field in declared_credential_fields(section)] == [
        FIELD_TOKEN,
        FIELD_CHAT,
    ]


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        ({}, True),
        ({FIELD_TOKEN: "s3cr3t"}, True),  # one field left empty is enough
        ({FIELD_CHAT: "42"}, True),
        ({FIELD_TOKEN: "s3cr3t", FIELD_CHAT: "  "}, True),  # blank is not a value
        ({FIELD_TOKEN: "s3cr3t", FIELD_CHAT: "42"}, False),
    ],
)
def given_several_fields_when_credentials_probed_then_true_until_all_are_provided(
    tmp_path: Path, environ: dict[str, str], expected: bool
) -> None:
    assert requires_credentials(_load(tmp_path, TWO_FIELDS).transport, environ) is expected


def given_declared_fields_when_views_built_then_env_is_dropped(tmp_path: Path) -> None:
    """ADR-027 §2: an interface gets what it draws, never the variable behind it."""
    section = _load(tmp_path, TWO_FIELDS).transport
    views = credential_field_views(section)
    assert views == [
        CredentialFieldView(
            key="access_token", label="Jeton d'accès", placeholder="acme-…", secret=True
        ),
        CredentialFieldView(
            key="chat_id", label="Identifiant de conversation", placeholder=None, secret=False
        ),
    ]
    assert set(views[0].model_dump()) == {"key", "label", "placeholder", "secret"}


def given_declared_fields_when_catalogue_built_then_the_entry_carries_them_without_env(
    tmp_path: Path,
) -> None:
    view = _load(tmp_path, TWO_FIELDS).profile_views(environ={})[0]
    assert [field.key for field in view.credential_fields] == ["access_token", "chat_id"]
    dumped = view.model_dump(mode="json")
    assert "env" not in str(dumped)
    assert FIELD_TOKEN not in str(dumped) and FIELD_CHAT not in str(dumped)


def given_no_declaration_when_catalogue_built_then_the_implicit_field_is_listed(
    tmp_path: Path,
) -> None:
    views = {view.name: view for view in _load(tmp_path, TWO_PROFILES).profile_views(environ={})}
    assert [field.key for field in views["claude"].credential_fields] == [DEFAULT_CREDENTIAL_KEY]
    assert views["claude"].credential_fields[0].secret is True
    assert views["mock"].credential_fields == []  # token_env = "": nothing to render


# ================================================================================================
# 6. skills: the [skills] section and what GET /skills lists (ADR-027 §4)
# ================================================================================================
def _skill_tree(root: Path) -> None:
    """A root with two levels of notes, one too deep, and files that are not skills."""
    (root / "deploy.md").write_text("deploy", encoding="utf-8")
    (root / "review.md").write_text("review", encoding="utf-8")
    (root / "notes.txt").write_text("not a skill", encoding="utf-8")
    (root / "java").mkdir()
    (root / "java" / "build.md").write_text("build", encoding="utf-8")
    (root / "java" / "deep").mkdir()
    (root / "java" / "deep" / "archive.md").write_text("archive", encoding="utf-8")
    (root / "java" / "deep" / "deeper").mkdir()
    (root / "java" / "deep" / "deeper" / "too-far.md").write_text("too far", encoding="utf-8")


def given_no_skills_section_when_config_loaded_then_enabled_without_a_root() -> None:
    assert AppConfig().skills == SkillsSection(enabled=True, root="")


def given_a_root_when_skills_listed_then_markdown_files_sorted_by_name(tmp_path: Path) -> None:
    _skill_tree(tmp_path)
    listed = list_skills(SkillsSection(root=str(tmp_path)))
    assert [skill.name for skill in listed] == ["archive", "build", "deploy", "review"]
    assert Skill(name="build", path=str(tmp_path / "java" / "build.md")) in listed


def given_a_file_deeper_than_the_bound_when_skills_listed_then_it_is_left_out(
    tmp_path: Path,
) -> None:
    _skill_tree(tmp_path)
    assert "too-far" not in [skill.name for skill in list_skills(SkillsSection(root=str(tmp_path)))]


def given_more_notes_than_the_bound_when_skills_listed_then_the_head_is_reported(
    tmp_path: Path,
) -> None:
    """Like the inventory of ADR-026: everything is sorted, then cut, so the head is stable."""
    for index in range(MAX_SKILLS + 25):
        (tmp_path / f"skill-{index:04d}.md").write_text("x", encoding="utf-8")
    listed = list_skills(SkillsSection(root=str(tmp_path)))
    assert len(listed) == MAX_SKILLS
    assert listed[0].name == "skill-0000" and listed[-1].name == f"skill-{MAX_SKILLS - 1:04d}"


def given_a_relative_root_when_skills_listed_then_paths_are_absolute(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "deploy.md").write_text("deploy", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    listed = list_skills(SkillsSection(root="."))
    assert [Path(skill.path).is_absolute() for skill in listed] == [True]


@pytest.mark.parametrize(
    "section",
    [
        SkillsSection(),  # no root at all
        SkillsSection(root="   "),
        SkillsSection(root="./does-not-exist-anywhere"),
    ],
)
def given_an_unusable_root_when_skills_listed_then_empty_and_no_error(
    section: SkillsSection,
) -> None:
    """The sign-in screen degrades into a free text field; it never shows an error."""
    assert list_skills(section) == []


def given_a_root_that_is_a_file_when_skills_listed_then_empty(tmp_path: Path) -> None:
    note = tmp_path / "deploy.md"
    note.write_text("deploy", encoding="utf-8")
    assert list_skills(SkillsSection(root=str(note))) == []


def given_an_unreadable_root_when_skills_listed_then_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _skill_tree(tmp_path)

    def refuse(_: Any) -> Any:
        raise PermissionError("nope")

    monkeypatch.setattr(skills_module.os, "scandir", refuse)
    assert list_skills(SkillsSection(root=str(tmp_path))) == []


def given_skills_disabled_when_listed_then_empty_without_walking(tmp_path: Path) -> None:
    _skill_tree(tmp_path)
    assert list_skills(SkillsSection(enabled=False, root=str(tmp_path))) == []
