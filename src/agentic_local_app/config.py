"""Global configuration (ADR-018): one ``config.toml`` for everything external or tunable.

Resolution order of the file: ``--config`` (CLI) > ``AGENTIC_APP_CONFIG`` env var > ``./config.toml``
> built-in defaults. Every value can be overridden by an environment variable named
``AGENTIC__<SECTION>__<KEY>`` (double underscores), e.g. ``AGENTIC__TRANSPORT__USER_ID=alice``;
lists and tables are given as JSON (``AGENTIC__TRANSPORT__OPTIONS='{"init": {...}}'``).

Secrets never live in the file: ``transport.token_env`` names the environment variable holding the
model API token (an optional ``.env`` file is loaded into the environment first, without overriding
variables already set).

ADR-024 adds named model profiles: ``[models]`` declares one complete transport section per model
and ``models.active`` names the one this process runs with. The model is chosen **once**, at
start-up; changing model means restarting the application with another active profile.

ADR-027 adds ``credential_fields`` to a profile: the inputs a user interface has to render before
that model can be called (a token, and whatever else a templated provider needs — a chat id, an
organisation slug). Each field names the environment variable its value is written to; a profile
that declares none falls back to one implicit ``access_token`` field built from ``token_env``.
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping, MutableMapping
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from agentic_local_app.domain.commands import normalise_program_entry
from agentic_local_app.domain.errors import ConfigError

ENV_CONFIG_PATH = "AGENTIC_APP_CONFIG"
ENV_PREFIX = "AGENTIC__"
DEFAULT_CONFIG_FILENAME = "config.toml"
#: The default transport provider (ADR-020): the ADR-004 contract over httpx.
DEFAULT_TRANSPORT_PROVIDER = "generic_http"
#: The default message codec (ADR-021): identity, the transport already yields protocol envelopes.
DEFAULT_CODEC = "passthrough"
#: Marker of an environment reference inside a provider option (``${env:VAR}``, ADR-020).
ENV_REFERENCE_MARKER = "${env:"
#: Name of the implicit model profile built from ``[transport]`` when there is no ``[models]``
#: (ADR-024).
DEFAULT_MODEL_PROFILE = "default"
#: Key of the implicit credential field a profile falls back to when it declares none (ADR-027 §1).
DEFAULT_CREDENTIAL_KEY = "access_token"
#: Label of that implicit field — the same words the front shows when it has no declaration.
DEFAULT_CREDENTIAL_LABEL = "Access token"
#: Placeholder of that implicit field.
DEFAULT_CREDENTIAL_PLACEHOLDER = "Paste an access token"
#: Option keys whose values are always masked by :meth:`AppConfig.masked` (substring match).
SECRET_KEY_MARKERS: tuple[str, ...] = ("key", "token", "secret", "password", "authorization")
MASK = "***"

#: ADR-029 §2 — the programs whose non-zero exit is a **result to interpret**, not a task that went
#: wrong: compilers, build tools, test runners and linters answer by their exit code. Grouped by
#: family, in the order ``config.toml`` documents them. Entries are either a program (``mvn``) or a
#: program and the sub-command that matters (``npm run``: ``npm install`` is not a build).
DEFAULT_VERDICT_PROGRAMS: tuple[str, ...] = (
    # JVM builds and their wrappers
    "mvn",
    "mvnw",
    "gradle",
    "gradlew",
    "ant",
    "sbt",
    "javac",
    # Node and TypeScript
    "npm run",
    "npm test",
    "yarn",
    "pnpm",
    "npx",
    "tsc",
    "eslint",
    # Rust (cargo, and the compiler called directly), .NET, Go
    "cargo",
    "rustc",
    "dotnet",
    "go",
    # C / C++ and the make family
    "make",
    "cmake",
    "ninja",
    "gcc",
    "g++",
    "clang",
    "clang++",
    # Python
    "pytest",
    "tox",
    "ruff",
    "mypy",
    "flake8",
    "pylint",
)


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AppSection(_Section):
    name: str = "agentic-local-app"
    data_dir: str = "./data"
    log_level: str = "INFO"
    env_file: str = ".env"


class CredentialField(_Section):
    """ADR-027 §1 — one input a user interface renders for a model profile.

    ``key`` is the identifier the interface sends back in the ``credentials`` object of
    ``POST /credentials``; ``label`` and ``placeholder`` are presentation; ``secret`` says whether
    the value may be remembered between launches (**default ``true``: fail closed**); ``env`` is
    the environment variable the value is written to, which is the only field the application acts
    on. ``env`` never leaves the process: the catalogue served to an interface drops it.
    """

    key: str
    label: str
    placeholder: str | None = None
    secret: bool = True
    env: str

    @field_validator("key")
    @classmethod
    def _key_is_an_identifier(cls, value: str) -> str:
        """A key travels as a JSON object key and as a form field name: keep it plain."""
        value = value.strip()
        if not value:
            raise ValueError("must name a credential field")
        if not value.isidentifier():
            raise ValueError("must be shaped like an identifier (letters, digits, underscores)")
        return value

    @field_validator("env")
    @classmethod
    def _env_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must name an environment variable")
        return value


class TransportSection(_Section):
    """ADR-004 (endpoints, token, user id, timeouts), ADR-020 (pluggable provider) and ADR-021
    (message codec per model).

    URL templates accept ``{conversation_id}`` and ``{after}`` placeholders. ``provider`` selects
    the implementation by configuration only: a registered name (``generic_http``,
    ``templated_http``, ``fake``), an entry point of the ``agentic_local_app.transports`` group or
    an import path ``package.module:ClassName``. ``options`` is the provider-specific sub-table,
    validated by the provider itself (``options_model``); ``close_method`` is the HTTP method of
    ``close_url`` for ``generic_http``. ``codec`` selects, the same way (registered name,
    ``agentic_local_app.codecs`` entry point, import path), the codec converting the raw shape of
    the model's replies into protocol envelopes and back (``passthrough`` = identity, the
    transport is used bare); ``codec_options`` is its own sub-table.

    The same section describes a **named model profile** of ``[models]`` (ADR-024); ``display_name``
    and ``description`` are presentation only — nothing in the application reads them, they travel
    to the user interfaces through :class:`ModelProfileView`.

    ``credential_fields`` (ADR-027 §1) declares what the profile needs to be called: one entry per
    input an interface renders. Left out (``None``, the default), the profile falls back to one
    implicit ``access_token`` field built from ``token_env`` — see
    :func:`declared_credential_fields`. An explicitly empty list means the profile needs nothing.
    """

    init_url: str = "http://127.0.0.1:9000/v1/conversations"
    post_url: str = "http://127.0.0.1:9000/v1/conversations/{conversation_id}/messages"
    get_url: str = "http://127.0.0.1:9000/v1/conversations/{conversation_id}/messages?after={after}"
    close_url: str = ""
    token_env: str = "AGENTIC_TRANSPORT_TOKEN"
    user_id: str = "local-user"
    request_timeout_ms: int = Field(default=15_000, gt=0)
    poll_interval_ms: int = Field(default=1_000, gt=0)
    reply_timeout_ms: int = Field(default=120_000, gt=0)
    gzip: bool = True
    verify_tls: bool = True
    # ADR-020
    provider: str = DEFAULT_TRANSPORT_PROVIDER
    close_method: Literal["POST", "DELETE"] = "POST"
    options: dict[str, Any] = Field(default_factory=dict)
    # ADR-021
    codec: str = DEFAULT_CODEC
    codec_options: dict[str, Any] = Field(default_factory=dict)
    # ADR-024: presentation of the profile in a model catalogue, never read by the application
    display_name: str | None = None
    description: str | None = None
    # ADR-027: the inputs an interface renders before this profile can be called
    credential_fields: list[CredentialField] | None = None

    @field_validator("credential_fields")
    @classmethod
    def _credential_keys_unique(
        cls, value: list[CredentialField] | None
    ) -> list[CredentialField] | None:
        """Two fields sharing a key would make the posted object ambiguous."""
        if value is None:
            return None
        seen: set[str] = set()
        duplicates: set[str] = set()
        for field in value:
            if field.key in seen:
                duplicates.add(field.key)
            seen.add(field.key)
        if duplicates:
            raise ValueError(f"credential field key(s) declared twice: {sorted(duplicates)}")
        return value

    @field_validator("provider")
    @classmethod
    def _provider_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must name a transport provider")
        return value

    @field_validator("codec")
    @classmethod
    def _codec_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must name a message codec")
        return value

    @field_validator("post_url", "get_url")
    @classmethod
    def _must_have_conversation_placeholder(cls, value: str) -> str:
        if "{conversation_id}" not in value:
            raise ValueError("must contain the {conversation_id} placeholder")
        return value

    @field_validator("get_url")
    @classmethod
    def _must_have_after_placeholder(cls, value: str) -> str:
        if "{after}" not in value:
            raise ValueError("must contain the {after} placeholder")
        return value

    @property
    def token(self) -> str | None:
        """The bearer token, read from the environment at call time (never persisted)."""
        value = os.environ.get(self.token_env, "").strip()
        return value or None


class ModelsSection(_Section):
    """ADR-024 — the named model profiles and the one this process runs with.

    ``active`` names a profile; every other key of ``[models]`` is a profile, validated exactly
    like ``[transport]``::

        [models]
        active = "mock"

        [models.mock]
        provider = "generic_http"
        codec = "passthrough"

        [models.claude]
        provider = "templated_http"
        codec = "json_text"
        token_env = "CLAUDE_API_KEY"
        display_name = "Claude (chat completions)"

    A profile is **complete**: it inherits nothing from ``[transport]``, only the defaults of
    :class:`TransportSection`. Without ``[models]``, ``[transport]`` is the implicit profile named
    ``default`` and it is the active one (:data:`DEFAULT_MODEL_PROFILE`).
    """

    active: str = DEFAULT_MODEL_PROFILE
    profiles: dict[str, TransportSection] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _gather_profiles(cls, data: Any) -> Any:
        """``[models.<name>]`` sub-tables are profiles; ``active`` is the only reserved key."""
        if not isinstance(data, dict):
            return data
        profiles: dict[str, Any] = dict(data.get("profiles") or {})
        profiles.update({k: v for k, v in data.items() if k not in {"active", "profiles"}})
        gathered: dict[str, Any] = {"profiles": profiles}
        if "active" in data:
            gathered["active"] = data["active"]
        return gathered

    @field_validator("active")
    @classmethod
    def _active_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must name a model profile")
        return value


class CredentialFieldView(_Section):
    """What the interfaces show of one credential field (ADR-027 §2): everything but ``env``.

    The name of the environment variable is an implementation detail of this process and stays
    here; an interface only ever needs the key it sends back, and what to draw around the input.
    """

    key: str
    label: str
    placeholder: str | None
    secret: bool


class ModelProfileView(_Section):
    """What the interfaces show of one profile (ADR-024 §3): a catalogue entry, no secret.

    ``requires_credentials`` is computed at read time, never stored: at least one declared field
    has no value in the environment right now. ``credential_fields`` is **always present** — a
    possibly empty list, in declaration order (ADR-027 §2) — so an interface never has to guess
    what to render from a bare boolean.
    """

    name: str
    display_name: str | None
    description: str | None
    provider: str
    codec: str
    requires_credentials: bool
    credential_fields: list[CredentialFieldView]
    active: bool


def declared_credential_fields(section: TransportSection) -> list[CredentialField]:
    """ADR-027 §1 — the fields of a profile, declared or implicit, in declaration order.

    A profile that declares no list falls back to exactly one implicit ``access_token`` field
    built from ``token_env``, and to nothing at all when it names no variable either. That is
    what a front does with ``requires_credentials`` alone, so both ends degrade identically.
    """
    if section.credential_fields is not None:
        return list(section.credential_fields)
    variable = section.token_env.strip()
    if not variable:
        return []
    return [
        CredentialField(
            key=DEFAULT_CREDENTIAL_KEY,
            label=DEFAULT_CREDENTIAL_LABEL,
            placeholder=DEFAULT_CREDENTIAL_PLACEHOLDER,
            secret=True,
            env=variable,
        )
    ]


def credential_field_views(section: TransportSection) -> list[CredentialFieldView]:
    """The same fields as :func:`declared_credential_fields`, stripped of ``env`` (ADR-027 §2)."""
    return [
        CredentialFieldView(
            key=field.key, label=field.label, placeholder=field.placeholder, secret=field.secret
        )
        for field in declared_credential_fields(section)
    ]


def requires_credentials(
    section: TransportSection, environ: Mapping[str, str] | None = None
) -> bool:
    """ADR-024 / ADR-027: at least one declared field has no value in the environment (yet).

    Unchanged for a profile that declares no field: it falls back to the implicit ``access_token``
    field of ``token_env``, so the answer is still "``token_env`` named and that variable empty".
    """
    env: Mapping[str, str] = os.environ if environ is None else environ
    return any(not env.get(field.env, "").strip() for field in declared_credential_fields(section))


class ExecutionSection(_Section):
    """ADR-003 (platform), ADR-008 (timeouts), ADR-018 (live output), ADR-029 (verdict programs),
    ADR-030 (shell detection and dialect translation).

    ``verdict_programs`` lists the programs whose non-zero exit is a **result to interpret** rather
    than a task that went wrong (ADR-029 §2). Each entry is a program (``mvn``) or a program and
    the sub-command that matters (``npm run``); paths, extensions and case are normalised away, so
    ``/usr/bin/MVN`` and ``mvn.cmd`` are the same entry. An empty list turns the rule off.

    ``translate_commands`` turns the dialect dictionary of ADR-030 §4 on or off. Left on, a command
    written for the other dialect than the detected shell is rewritten **only** when the dictionary
    maps it exactly, and both forms are persisted and reported. Turned off, every command reaches
    the shell exactly as the model wrote it, which is the behaviour that predates the ADR.
    """

    shell: str = ""  # "" = detected (bash/zsh/sh on POSIX, pwsh/powershell on Windows)
    cwd: str = "."
    translate_commands: bool = True
    default_task_timeout_ms: int = Field(default=60_000, gt=0)
    max_task_timeout_ms: int = Field(default=900_000, gt=0)
    cancel_drain_timeout_ms: int = Field(default=5_000, gt=0)
    interrupt_drain_timeout_ms: int = Field(default=5_000, gt=0)
    live_output_chunk_bytes: int = Field(default=4_096, gt=0)
    live_output_interval_ms: int = Field(default=250, ge=0)
    verdict_programs: list[str] = Field(default_factory=lambda: list(DEFAULT_VERDICT_PROGRAMS))

    @field_validator("verdict_programs")
    @classmethod
    def _normalise_verdict_programs(cls, value: list[str]) -> list[str]:
        """Normalise every entry and drop the duplicates the normalisation creates, in order."""
        unique: dict[str, None] = {}
        for entry in value:
            unique[normalise_program_entry(entry)] = None
        return list(unique)


def _as_absolute(value: str) -> Path:
    """A configured directory as an absolute, normalised path — without touching the filesystem.

    ``os.path.abspath`` resolves ``.``/``..`` and the current directory lexically; ``Path.resolve``
    is deliberately avoided, it would follow symbolic links and depend on what exists right now.
    """
    return Path(os.path.abspath(os.path.expanduser(value)))


class ScratchSection(_Section):
    """ADR-026: the working space offered to the model's commands, and its fate.

    ``root`` is the parent of the per-session folders (``<root>/<session_id>``), created lazily and
    narrowed to the owner on POSIX. ``policy`` decides what happens to a folder **the application
    generated** when its session ends — a ``working_space`` handed over by the user is never deleted
    nor archived, whatever the policy. ``keep_on_failure`` overrides ``delete`` and ``archive`` for
    a session that failed, because that is when the files are worth looking at.
    ``max_inventory_entries`` bounds what an inventory reports, not what it walks.

    ``enabled = false`` restores the behaviour that predates the ADR: no folder is created and no
    variable is exported.
    """

    enabled: bool = True
    root: str = "./data/scratch"
    policy: Literal["delete", "keep", "archive"] = "delete"
    archive_root: str = "./data/scratch-archive"
    keep_on_failure: bool = True
    max_inventory_entries: int = Field(default=200, gt=0)

    @field_validator("root", "archive_root")
    @classmethod
    def _directory_not_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must name a directory")
        return value

    @model_validator(mode="after")
    def _archive_outside_root(self) -> ScratchSection:
        """``archive_root`` inside ``root`` would make an archive look like a session folder."""
        root, archive = _as_absolute(self.root), _as_absolute(self.archive_root)
        if archive == root or archive.is_relative_to(root) or root.is_relative_to(archive):
            raise ValueError("scratch.archive_root must be outside scratch.root")
        return self


class SkillsSection(_Section):
    """ADR-027 §4: the reusable notes a user interface offers to attach to a session.

    A skill is a markdown file the user wrote somewhere on this machine; the application only ever
    **lists** them (``GET /skills``) so that the interface can propose a choice instead of a free
    text field. ``root`` is the folder that is walked (``""`` = unset, nothing is walked) and
    ``enabled = false`` turns the listing off without touching the path.

    Nothing here is ever read, opened or sent to the model: what a session does with the skills it
    was given is undecided (ADR-027, point ouvert).
    """

    enabled: bool = True
    root: str = ""


class PayloadSection(_Section):
    """ADR-010 / ADR-005."""

    default_max_output_bytes: int = Field(default=8_192, gt=0)
    hard_max_output_bytes: int = Field(default=131_072, gt=0)
    max_message_bytes: int = Field(default=196_608, gt=0)
    max_state_summary_bytes: int = Field(default=4_096, gt=0)


class ProtocolSection(_Section):
    """ADR-022 (the model's direct answers to the user) and ADR-023 (the correction policy).

    ``allow_direct_response`` lets the model answer the **initial** ``user_request`` of a
    conversation with a ``user_response`` (an analysis, an explanation or a question that needs no
    command) instead of the mandatory ``discovery_plan`` of spec §14; after an ``execution_result``
    or a follow-up ``user_request`` a ``user_response`` is always accepted. ``false`` keeps the
    strict grammar of the specification.

    ``max_correction_attempts`` bounds the correction policy of ADR-023: on an unusable reply the
    application sends a ``protocol_correction_request`` and reads again, at most that many times in
    a row for one conversation, before applying the previous policy (rotation in ``WARNING``,
    otherwise session ``FAILED``). ``0`` disables the policy and restores the behaviour that
    predates ADR-023: the first unusable reply ends the session.
    """

    allow_direct_response: bool = True
    max_correction_attempts: int = Field(default=5, ge=0)


class ContextSection(_Section):
    """ADR-013 / ADR-005."""

    budget_bytes: int = Field(default=400_000, gt=0)
    warning_ratio: float = Field(default=0.70, gt=0, lt=1)
    saturation_ratio: float = Field(default=0.90, gt=0, le=1)
    # ADR-019: in WARNING, an unusable reply (protocol error, exhausted GET timeout) rotates once
    rotate_on_unusable_reply_in_warning: bool = True
    max_rotations_per_session: int = Field(default=5, ge=0)
    summary_budget_bytes: int = Field(default=32_768, gt=0)


class BudgetSection(_Section):
    """Defaults of §2.8 applied when the user request does not specify a budget (ADR-012)."""

    default_max_cycles: int = Field(default=20, gt=0)
    default_max_plans: int = Field(default=10, gt=0)
    default_max_total_duration_ms: int = Field(default=300_000, gt=0)
    auto_close_on_final_answer: bool = False


class RetrySection(_Section):
    """§7.3 bounded exponential backoff (ADR-017: no jitter by default)."""

    max_attempts: int = Field(default=4, ge=1)
    base_delay_ms: int = Field(default=500, ge=0)
    max_delay_ms: int = Field(default=8_000, ge=0)
    jitter_ratio: float = Field(default=0.0, ge=0, le=1)


class CircuitBreakerSection(_Section):
    """§7.4."""

    failure_threshold: int = Field(default=5, ge=1)
    open_duration_ms: int = Field(default=30_000, gt=0)
    half_open_max_calls: int = Field(default=1, ge=1)


#: Origins allowed by default (ADR-018): the Vite dev server of the desktop front, which pins its
#: port to 1420 (``vite.config.ts``, ``strictPort``), the default Vite port, a packaged Tauri
#: desktop application, plus the historical front port. Both spellings of the loopback are listed:
#: a browser treats ``localhost`` and ``127.0.0.1`` as two different origins.
DEFAULT_CORS_ORIGINS: tuple[str, ...] = (
    "http://localhost:1420",
    "http://127.0.0.1:1420",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "tauri://localhost",
)


class ApiSection(_Section):
    """ADR-018 local HTTP API.

    ``allow_destructive_admin`` opens the one route that destroys data,
    ``POST /admin/reset-database``; it is ``false`` by default and the route then answers
    ``403 ADMIN_DISABLED`` without touching anything.
    """

    host: str = "127.0.0.1"
    port: int = Field(default=8765, gt=0, lt=65536)
    cors_origins: list[str] = Field(default_factory=lambda: list(DEFAULT_CORS_ORIGINS))
    page_size: int = Field(default=100, gt=0)
    sse_queue_size: int = Field(default=1_000, gt=0)
    allow_destructive_admin: bool = False


class CliSection(_Section):
    refresh_interval_ms: int = Field(default=250, gt=0)


class TelemetrySection(_Section):
    enabled: bool = True


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    app: AppSection = Field(default_factory=AppSection)
    transport: TransportSection = Field(default_factory=TransportSection)
    models: ModelsSection = Field(default_factory=ModelsSection)
    execution: ExecutionSection = Field(default_factory=ExecutionSection)
    scratch: ScratchSection = Field(default_factory=ScratchSection)
    skills: SkillsSection = Field(default_factory=SkillsSection)
    payload: PayloadSection = Field(default_factory=PayloadSection)
    protocol: ProtocolSection = Field(default_factory=ProtocolSection)
    context: ContextSection = Field(default_factory=ContextSection)
    budget: BudgetSection = Field(default_factory=BudgetSection)
    retry: RetrySection = Field(default_factory=RetrySection)
    circuit_breaker: CircuitBreakerSection = Field(default_factory=CircuitBreakerSection)
    api: ApiSection = Field(default_factory=ApiSection)
    cli: CliSection = Field(default_factory=CliSection)
    telemetry: TelemetrySection = Field(default_factory=TelemetrySection)

    @model_validator(mode="after")
    def _resolve_active_model(self) -> AppConfig:
        """ADR-024: the active profile **becomes** ``transport``, once, at load time.

        Without ``[models]`` the section itself is the implicit profile named ``default``, so the
        catalogue always has at least one entry and nothing that reads ``config.transport`` changes.
        With ``[models]``, the selected profile is copied over ``transport``: one model per process,
        chosen at start-up — changing model means restarting with another ``models.active``.

        The two fields are rebound through :func:`object.__setattr__`: the model is frozen, and
        pydantic ignores a validator that returns another instance when the model is built by
        ``__init__``. Nothing is re-validated, both values are already validated sections.
        """
        models = self.models
        if not models.profiles:
            models = models.model_copy(update={"profiles": {DEFAULT_MODEL_PROFILE: self.transport}})
            object.__setattr__(self, "models", models)
        profile = models.profiles.get(models.active)
        if profile is None:
            raise ConfigError(
                "MODEL_PROFILE_UNKNOWN", model=models.active, available=sorted(models.profiles)
            )
        if profile is not self.transport:
            object.__setattr__(self, "transport", profile)
        return self

    @model_validator(mode="after")
    def _cross_section_bounds(self) -> AppConfig:
        """ADR-019: a maximal message must always fit in a fresh conversation, and a single task
        output must fit in a message."""
        if self.payload.hard_max_output_bytes > self.payload.max_message_bytes:
            raise ValueError("payload.hard_max_output_bytes must be <= payload.max_message_bytes")
        if self.payload.max_message_bytes * 2 > self.context.budget_bytes:
            raise ValueError("payload.max_message_bytes must be <= context.budget_bytes / 2")
        if self.context.summary_budget_bytes > self.payload.max_message_bytes:
            raise ValueError("context.summary_budget_bytes must be <= payload.max_message_bytes")
        return self

    @property
    def active_transport(self) -> TransportSection:
        """The transport of the active model profile (ADR-024).

        After validation this **is** :attr:`transport`: ``_resolve_active_model`` copies the active
        profile over the section, so every existing reader of ``config.transport`` already runs with
        the selected model.
        """
        return self.transport

    def profile_views(self, environ: Mapping[str, str] | None = None) -> list[ModelProfileView]:
        """Model catalogue for the interfaces (ADR-024 §3): active profile first, then by name."""
        views = [
            ModelProfileView(
                name=name,
                display_name=profile.display_name,
                description=profile.description,
                provider=profile.provider,
                codec=profile.codec,
                requires_credentials=requires_credentials(profile, environ),
                credential_fields=credential_field_views(profile),
                active=name == self.models.active,
            )
            for name, profile in self.models.profiles.items()
        ]
        return sorted(views, key=lambda view: (not view.active, view.name))

    def masked(self) -> dict[str, Any]:
        """Effective configuration as a dict with the token masked (for ``config show`` / ``/config``).

        Provider options (ADR-020) and codec options (ADR-021) are masked with a simple guard:
        any value containing an environment reference (``${env:...}``) and any value under a key
        that looks secret (``*key*``, ``*token*``, ``*secret*``, ``*password*``,
        ``*authorization*``) becomes ``***``. The options of **every** model profile (ADR-024) are
        masked the same way, not only those of the active one.
        """
        data = self.model_dump()
        data["transport"]["token"] = MASK if self.transport.token else None
        data["transport"]["options"] = mask_options(self.transport.options)
        data["transport"]["codec_options"] = mask_options(self.transport.codec_options)
        for name, profile in self.models.profiles.items():
            dumped = data["models"]["profiles"][name]
            dumped["options"] = mask_options(profile.options)
            dumped["codec_options"] = mask_options(profile.codec_options)
        return data


def _looks_secret(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in SECRET_KEY_MARKERS)


def mask_options(value: Any, *, under_secret_key: bool = False) -> Any:
    """A deep copy of provider options with the secret-looking leaves replaced by ``***``."""
    if isinstance(value, dict):
        return {
            str(key): mask_options(
                item, under_secret_key=under_secret_key or _looks_secret(str(key))
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [mask_options(item, under_secret_key=under_secret_key) for item in value]
    if isinstance(value, str) and ENV_REFERENCE_MARKER in value:
        return MASK
    if under_secret_key and value is not None:
        return MASK
    return value


# ------------------------------------------------------------------------------------------------
# Loading
# ------------------------------------------------------------------------------------------------
def load_dotenv(
    path: str | os.PathLike[str], environ: MutableMapping[str, str] | None = None
) -> int:
    """Minimal ``.env`` loader (``KEY=VALUE`` lines, ``#`` comments). Never overrides existing vars.

    Returns the number of variables set. A missing file is not an error.
    """
    env = os.environ if environ is None else environ
    p = Path(path)
    if not p.is_file():
        return 0
    count = 0
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in env:
            env[key] = value
            count += 1
    return count


def _apply_env_overrides(data: dict[str, Any], environ: MutableMapping[str, str]) -> dict[str, Any]:
    """``AGENTIC__SECTION__KEY=value`` -> data[section][key] = value (string; pydantic coerces)."""
    for name, value in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        parts = name[len(ENV_PREFIX) :].lower().split("__")
        if len(parts) != 2 or not all(parts):
            raise ConfigError("INVALID_ENV_OVERRIDE", variable=name)
        section, key = parts
        parsed: Any = value
        if value.startswith(("[", "{")) or value.lower() in {"true", "false"}:
            import json

            try:
                parsed = json.loads(value.lower() if value.lower() in {"true", "false"} else value)
            except json.JSONDecodeError as exc:
                raise ConfigError("INVALID_ENV_OVERRIDE", variable=name, error=str(exc)) from exc
        data.setdefault(section, {})[key] = parsed
    return data


def resolve_config_path(
    explicit: str | os.PathLike[str] | None, environ: MutableMapping[str, str] | None = None
) -> Path | None:
    env = os.environ if environ is None else environ
    if explicit is not None:
        return Path(explicit)
    if env.get(ENV_CONFIG_PATH):
        return Path(env[ENV_CONFIG_PATH])
    default = Path.cwd() / DEFAULT_CONFIG_FILENAME
    return default if default.is_file() else None


def load_config(
    path: str | os.PathLike[str] | None = None,
    *,
    environ: MutableMapping[str, str] | None = None,
    load_env_file: bool = True,
) -> AppConfig:
    """Load, override and validate the configuration. Raises :class:`ConfigError` on any problem."""
    env: MutableMapping[str, str] = os.environ if environ is None else environ
    resolved = resolve_config_path(path, env)
    data: dict[str, Any] = {}
    if resolved is not None:
        if not resolved.is_file():
            raise ConfigError("CONFIG_FILE_NOT_FOUND", path=str(resolved))
        try:
            data = tomllib.loads(resolved.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(
                "CONFIG_FILE_INVALID_TOML", path=str(resolved), error=str(exc)
            ) from exc
    data = _apply_env_overrides(data, env)
    try:
        config = AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError("CONFIG_INVALID", errors=exc.errors(include_url=False)) from exc
    if load_env_file:
        load_dotenv(config.app.env_file, env)
    return config
