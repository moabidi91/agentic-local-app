"""``TransportRegistry`` — the providers the application can be configured with (ADR-020) — and
the generic :class:`PluginRegistry` it is built on, shared with the message codecs of ADR-021
(:class:`~agentic_local_app.transport.codecs.registry.CodecRegistry`).

A plugin specification (``transport.provider``, ``transport.codec``) is resolved, in this order,
as:

1. a **registered name** — the built-in plugins register themselves with the ``register`` class
   decorator when their package is imported (``generic_http``, ``templated_http``, ``fake`` for the
   providers; ``passthrough``, ``json_text``... for the codecs); a registered name always wins;
2. an **import path** ``package.module:ClassName`` (any installed module, no registration needed:
   the open/closed way of adding a plugin);
3. an **entry point** of the registry's group (``agentic_local_app.transports``,
   ``agentic_local_app.codecs``), discovered lazily through ``importlib.metadata`` and loaded only
   when named.

Every resolution checks that the result is a concrete subclass of the registry's base class
(``TransportGateway``, ``MessageCodec``); ``validate`` checks the plugin's options against its
``options_model``. Errors are ``ConfigError`` with the registry's own codes: for the providers
``TRANSPORT_PROVIDER_UNKNOWN`` (with the available names), ``TRANSPORT_PROVIDER_INVALID`` (not a
usable ``TransportGateway``) and ``TRANSPORT_OPTIONS_INVALID``; their details name the plugin under
the registry's ``kind`` key (``provider``, ``codec``).
"""

from __future__ import annotations

import importlib
import importlib.metadata
import inspect
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, cast

from pydantic import BaseModel

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ConfigError
from agentic_local_app.transport.base import (
    OPTIONS_INVALID_CODE,
    TransportGateway,
    validate_options,
)

__all__ = [
    "ENTRY_POINT_GROUP",
    "ORIGIN_BUILTIN",
    "ORIGIN_ENTRY_POINT",
    "ORIGIN_IMPORT_PATH",
    "PluginInfo",
    "PluginRegistry",
    "ProviderInfo",
    "TransportRegistry",
    "qualified_name",
]

#: The ``importlib.metadata`` entry point group a distribution uses to publish a provider.
ENTRY_POINT_GROUP = "agentic_local_app.transports"
ORIGIN_BUILTIN = "builtin"
ORIGIN_ENTRY_POINT = "entry point"
ORIGIN_IMPORT_PATH = "import path"
_IMPORT_PATH_SEPARATOR = ":"

P = TypeVar("P")
C = TypeVar("C")


@dataclass(frozen=True)
class PluginInfo:
    """What ``agentic-app transport list`` / ``codec list`` / ``show`` display about a plugin."""

    name: str
    origin: str
    qualified_name: str  # ``package.module:ClassName``


#: Historical name of :class:`PluginInfo` (ADR-020): the same class.
ProviderInfo = PluginInfo


def qualified_name(plugin: type[Any]) -> str:
    return f"{plugin.__module__}{_IMPORT_PATH_SEPARATOR}{plugin.__qualname__}"


class PluginRegistry(Generic[P]):
    """Process-wide table of the named plugins of one kind plus the lazy entry-point / import-path
    lookups. A subclass sets the class attributes below; each subclass owns its own table."""

    #: The abstract base every plugin must concretely derive from.
    base: ClassVar[type[Any]]
    #: What the plugin is called in error details and messages (``provider``, ``codec``).
    kind: ClassVar[str]
    #: The ``importlib.metadata`` entry point group of this kind of plugin.
    entry_point_group: ClassVar[str]
    #: ``ConfigError`` codes: unknown specification, unusable class, invalid options.
    unknown_code: ClassVar[str]
    invalid_code: ClassVar[str]
    options_code: ClassVar[str]

    _registered: ClassVar[dict[str, type[Any]]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._registered = {}

    # ------------------------------------------------------------------ registration ------
    @classmethod
    def register(cls, name: str) -> Callable[[type[C]], type[C]]:
        """Class decorator registering a concrete subclass of ``base`` under ``name``; the class
        is returned unchanged."""

        def decorator(plugin: type[C]) -> type[C]:
            problem = cls._unusable_reason(plugin)
            if problem is not None:
                raise TypeError(f"cannot register {plugin!r} as {name!r}: {problem}")
            existing = cls._registered.get(name)
            if existing is not None and existing is not plugin:
                raise ValueError(
                    f"{cls.kind} {name!r} is already registered ({qualified_name(existing)})"
                )
            cls._registered[name] = plugin
            return plugin

        return decorator

    @classmethod
    def registered(cls) -> dict[str, type[P]]:
        """A copy of the name -> class table of the registered plugins."""
        return dict(cast(dict[str, type[P]], cls._registered))

    # ------------------------------------------------------------------ discovery ---------
    @classmethod
    def names(cls) -> list[str]:
        """Every selectable name, sorted: registered names and entry points (no duplicate)."""
        return sorted(set(cls._registered) | {point.name for point in cls._entry_points()})

    @classmethod
    def list_plugins(cls) -> list[PluginInfo]:
        """The registered plugins and the entry points (not loaded), sorted by name."""
        infos = {
            name: PluginInfo(name, ORIGIN_BUILTIN, qualified_name(plugin))
            for name, plugin in cls._registered.items()
        }
        for point in cls._entry_points():
            if point.name not in infos:
                infos[point.name] = PluginInfo(point.name, ORIGIN_ENTRY_POINT, point.value)
        return [infos[name] for name in sorted(infos)]

    @classmethod
    def resolve(cls, spec: str) -> type[P]:
        """The plugin class for ``spec``: a registered name, an import path or an entry point."""
        return cls.describe(spec)[1]

    @classmethod
    def describe(cls, spec: str) -> tuple[PluginInfo, type[P]]:
        """The plugin class for ``spec`` together with where it comes from."""
        spec = spec.strip()
        registered = cls._registered.get(spec)
        if registered is not None:
            return PluginInfo(spec, ORIGIN_BUILTIN, qualified_name(registered)), registered
        if _IMPORT_PATH_SEPARATOR in spec:
            plugin = cls._import(spec)
            return PluginInfo(spec, ORIGIN_IMPORT_PATH, qualified_name(plugin)), plugin
        for point in cls._entry_points():
            if point.name == spec:
                plugin = cls._load_entry_point(point)
                return PluginInfo(spec, ORIGIN_ENTRY_POINT, qualified_name(plugin)), plugin
        raise ConfigError(cls.unknown_code, **{cls.kind: spec}, available=cls.names())

    # ------------------------------------------------------------------ options -----------
    @classmethod
    def validate(cls, plugin: type[Any], options: Mapping[str, Any]) -> BaseModel | None:
        """The options validated against the plugin's ``options_model`` (``ConfigError`` with the
        registry's ``options_code`` otherwise); ``None`` when the plugin takes no options."""
        return validate_options(plugin, options, code=cls.options_code, kind=cls.kind)

    # ------------------------------------------------------------------ internals ---------
    @classmethod
    def _entry_points(cls) -> Iterable[importlib.metadata.EntryPoint]:
        return importlib.metadata.entry_points(group=cls.entry_point_group)

    @classmethod
    def _import(cls, spec: str) -> type[P]:
        module_name, _, attribute = spec.partition(_IMPORT_PATH_SEPARATOR)
        try:
            module = importlib.import_module(module_name)
            candidate = getattr(module, attribute)
        except (ImportError, AttributeError) as exc:
            raise ConfigError(
                cls.unknown_code,
                **{cls.kind: spec},
                available=cls.names(),
                error=f"{type(exc).__name__}: {exc}",
            ) from exc
        return cls._checked(candidate, spec)

    @classmethod
    def _load_entry_point(cls, point: importlib.metadata.EntryPoint) -> type[P]:
        try:
            candidate = point.load()
        except (ImportError, AttributeError) as exc:
            raise ConfigError(
                cls.invalid_code,
                **{cls.kind: point.name},
                reason="entry_point_load_failed",
                entry_point=point.value,
                error=f"{type(exc).__name__}: {exc}",
            ) from exc
        return cls._checked(candidate, point.name)

    @classmethod
    def _unusable_reason(cls, candidate: Any) -> str | None:
        """Why ``candidate`` cannot serve as a plugin, or ``None`` when it can."""
        if not inspect.isclass(candidate):
            return "not a class"
        if not issubclass(candidate, cls.base):
            return f"not a {cls.base.__name__} subclass"
        if inspect.isabstract(candidate):
            return "abstract class"
        return None

    @classmethod
    def _checked(cls, candidate: Any, spec: str) -> type[P]:
        problem = cls._unusable_reason(candidate)
        if problem is not None:
            raise ConfigError(
                cls.invalid_code, **{cls.kind: spec}, reason=problem, resolved=repr(candidate)
            )
        return cast(type[P], candidate)


class TransportRegistry(PluginRegistry[TransportGateway]):
    """The transport providers (ADR-020): ``transport.provider`` and ``transport.options``."""

    base = TransportGateway
    kind = "provider"
    entry_point_group = ENTRY_POINT_GROUP
    unknown_code = "TRANSPORT_PROVIDER_UNKNOWN"
    invalid_code = "TRANSPORT_PROVIDER_INVALID"
    options_code = OPTIONS_INVALID_CODE

    @classmethod
    def list_providers(cls) -> list[PluginInfo]:
        """The registered providers and the entry points (not loaded), sorted by name."""
        return cls.list_plugins()

    @classmethod
    def create(cls, config: AppConfig, *, clock: Clock, **kwargs: Any) -> TransportGateway:
        """Instantiate the configured provider after validating its options."""
        provider = cls.resolve(config.transport.provider)
        cls.validate(provider, config.transport.options)
        factory = cast(Callable[..., TransportGateway], provider)
        return factory(config.transport, clock, **kwargs)
