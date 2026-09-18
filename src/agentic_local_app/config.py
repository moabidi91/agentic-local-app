"""Global configuration (ADR-018): one ``config.toml`` for everything external or tunable.

Resolution order of the file: ``--config`` (CLI) > ``AGENTIC_APP_CONFIG`` env var > ``./config.toml``
> built-in defaults. Every value can be overridden by an environment variable named
``AGENTIC__<SECTION>__<KEY>`` (double underscores), e.g. ``AGENTIC__TRANSPORT__USER_ID=alice``;
lists and tables are given as JSON (``AGENTIC__TRANSPORT__OPTIONS='{"init": {...}}'``).

Secrets never live in the file: ``transport.token_env`` names the environment variable holding the
model API token (an optional ``.env`` file is loaded into the environment first, without overriding
variables already set).
"""

from __future__ import annotations

import os
import tomllib
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

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
#: Option keys whose values are always masked by :meth:`AppConfig.masked` (substring match).
SECRET_KEY_MARKERS: tuple[str, ...] = ("key", "token", "secret", "password", "authorization")
MASK = "***"


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AppSection(_Section):
    name: str = "agentic-local-app"
    data_dir: str = "./data"
    log_level: str = "INFO"
    env_file: str = ".env"


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


class ExecutionSection(_Section):
    """ADR-003 (platform), ADR-008 (timeouts), ADR-018 (live output)."""

    shell: str = ""  # "" = platform default (bash/sh on POSIX, PowerShell on Windows)
    cwd: str = "."
    default_task_timeout_ms: int = Field(default=60_000, gt=0)
    max_task_timeout_ms: int = Field(default=900_000, gt=0)
    cancel_drain_timeout_ms: int = Field(default=5_000, gt=0)
    interrupt_drain_timeout_ms: int = Field(default=5_000, gt=0)
    live_output_chunk_bytes: int = Field(default=4_096, gt=0)
    live_output_interval_ms: int = Field(default=250, ge=0)


class PayloadSection(_Section):
    """ADR-010 / ADR-005."""

    default_max_output_bytes: int = Field(default=8_192, gt=0)
    hard_max_output_bytes: int = Field(default=131_072, gt=0)
    max_message_bytes: int = Field(default=196_608, gt=0)
    max_state_summary_bytes: int = Field(default=4_096, gt=0)


class ProtocolSection(_Section):
    """ADR-022: the model's direct answers to the user.

    ``allow_direct_response`` lets the model answer the **initial** ``user_request`` of a
    conversation with a ``user_response`` (an analysis, an explanation or a question that needs no
    command) instead of the mandatory ``discovery_plan`` of spec §14; after an ``execution_result``
    or a follow-up ``user_request`` a ``user_response`` is always accepted. ``false`` keeps the
    strict grammar of the specification. The correction policy of ADR-023 will live here too.
    """

    allow_direct_response: bool = True


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


class ApiSection(_Section):
    """ADR-018 local HTTP API."""

    host: str = "127.0.0.1"
    port: int = Field(default=8765, gt=0, lt=65536)
    cors_origins: list[str] = Field(default_factory=lambda: ["http://localhost:3000"])
    page_size: int = Field(default=100, gt=0)
    sse_queue_size: int = Field(default=1_000, gt=0)


class CliSection(_Section):
    refresh_interval_ms: int = Field(default=250, gt=0)


class TelemetrySection(_Section):
    enabled: bool = True


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    app: AppSection = Field(default_factory=AppSection)
    transport: TransportSection = Field(default_factory=TransportSection)
    execution: ExecutionSection = Field(default_factory=ExecutionSection)
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

    def masked(self) -> dict[str, Any]:
        """Effective configuration as a dict with the token masked (for ``config show`` / ``/config``).

        Provider options (ADR-020) and codec options (ADR-021) are masked with a simple guard:
        any value containing an environment reference (``${env:...}``) and any value under a key
        that looks secret (``*key*``, ``*token*``, ``*secret*``, ``*password*``,
        ``*authorization*``) becomes ``***``.
        """
        data = self.model_dump()
        data["transport"]["token"] = MASK if self.transport.token else None
        data["transport"]["options"] = mask_options(self.transport.options)
        data["transport"]["codec_options"] = mask_options(self.transport.codec_options)
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
