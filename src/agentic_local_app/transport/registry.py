"""``TransportRegistry`` — the providers the application can be configured with (ADR-020).

``transport.provider`` is resolved, in this order, as:

1. a **registered name** — the built-in providers register themselves with the
   :meth:`TransportRegistry.register` class decorator when the transport package is imported
   (``generic_http``, ``templated_http``, ``fake``); a registered name always wins;
2. an **import path** ``package.module:ClassName`` (any installed module, no registration needed:
   the open/closed way of adding a provider);
3. an **entry point** of the ``agentic_local_app.transports`` group, discovered lazily through
   ``importlib.metadata`` and loaded only when named.

Every resolution checks that the result is a concrete subclass of ``TransportGateway``;
:meth:`TransportRegistry.create` then validates ``transport.options`` against the provider's
``options_model`` and instantiates ``provider(config.transport, clock, **kwargs)``. Errors are
``ConfigError``: ``TRANSPORT_PROVIDER_UNKNOWN`` (with the available names),
``TRANSPORT_PROVIDER_INVALID`` (not a usable ``TransportGateway``), ``TRANSPORT_OPTIONS_INVALID``.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import inspect
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from agentic_local_app.config import AppConfig
from agentic_local_app.domain.clock import Clock
from agentic_local_app.domain.errors import ConfigError
from agentic_local_app.transport.base import TransportGateway, validate_options

__all__ = [
    "ENTRY_POINT_GROUP",
    "ORIGIN_BUILTIN",
    "ORIGIN_ENTRY_POINT",
    "ORIGIN_IMPORT_PATH",
    "ProviderInfo",
    "TransportRegistry",
]

#: The ``importlib.metadata`` entry point group a distribution uses to publish a provider.
ENTRY_POINT_GROUP = "agentic_local_app.transports"
ORIGIN_BUILTIN = "builtin"
ORIGIN_ENTRY_POINT = "entry point"
ORIGIN_IMPORT_PATH = "import path"
_IMPORT_PATH_SEPARATOR = ":"

G = TypeVar("G", bound=TransportGateway)


@dataclass(frozen=True)
class ProviderInfo:
    """What ``agentic-app transport list`` / ``show`` display about a provider."""

    name: str
    origin: str
    qualified_name: str  # ``package.module:ClassName``


def qualified_name(provider: type[Any]) -> str:
    return f"{provider.__module__}{_IMPORT_PATH_SEPARATOR}{provider.__qualname__}"


class TransportRegistry:
    """Process-wide table of the named providers plus the lazy entry-point / import-path lookups."""

    _registered: dict[str, type[TransportGateway]] = {}

    # ------------------------------------------------------------------ registration ------
    @classmethod
    def register(cls, name: str) -> Callable[[type[G]], type[G]]:
        """Class decorator registering a concrete ``TransportGateway`` under ``name``."""

        def decorator(provider: type[G]) -> type[G]:
            problem = _unusable_reason(provider)
            if problem is not None:
                raise TypeError(f"cannot register {provider!r} as {name!r}: {problem}")
            existing = cls._registered.get(name)
            if existing is not None and existing is not provider:
                raise ValueError(
                    f"transport provider {name!r} is already registered "
                    f"({qualified_name(existing)})"
                )
            cls._registered[name] = provider
            return provider

        return decorator

    @classmethod
    def registered(cls) -> dict[str, type[TransportGateway]]:
        """A copy of the name -> class table of the registered providers."""
        return dict(cls._registered)

    # ------------------------------------------------------------------ discovery ---------
    @classmethod
    def names(cls) -> list[str]:
        """Every selectable name, sorted: registered names and entry points (no duplicate)."""
        return sorted(set(cls._registered) | {point.name for point in cls._entry_points()})

    @classmethod
    def list_providers(cls) -> list[ProviderInfo]:
        """The registered providers and the entry points (not loaded), sorted by name."""
        infos = {
            name: ProviderInfo(name, ORIGIN_BUILTIN, qualified_name(provider))
            for name, provider in cls._registered.items()
        }
        for point in cls._entry_points():
            if point.name not in infos:
                infos[point.name] = ProviderInfo(point.name, ORIGIN_ENTRY_POINT, point.value)
        return [infos[name] for name in sorted(infos)]

    @classmethod
    def resolve(cls, spec: str) -> type[TransportGateway]:
        """The provider class for ``spec``: a registered name, an import path or an entry point."""
        return cls.describe(spec)[1]

    @classmethod
    def describe(cls, spec: str) -> tuple[ProviderInfo, type[TransportGateway]]:
        """The provider class for ``spec`` together with where it comes from."""
        spec = spec.strip()
        registered = cls._registered.get(spec)
        if registered is not None:
            return ProviderInfo(spec, ORIGIN_BUILTIN, qualified_name(registered)), registered
        if _IMPORT_PATH_SEPARATOR in spec:
            provider = cls._import(spec)
            return ProviderInfo(spec, ORIGIN_IMPORT_PATH, qualified_name(provider)), provider
        for point in cls._entry_points():
            if point.name == spec:
                provider = cls._load_entry_point(point)
                return ProviderInfo(spec, ORIGIN_ENTRY_POINT, qualified_name(provider)), provider
        raise ConfigError("TRANSPORT_PROVIDER_UNKNOWN", provider=spec, available=cls.names())

    # ------------------------------------------------------------------ construction ------
    @classmethod
    def create(cls, config: AppConfig, *, clock: Clock, **kwargs: Any) -> TransportGateway:
        """Instantiate the configured provider after validating its options."""
        provider = cls.resolve(config.transport.provider)
        validate_options(provider, config.transport.options)
        factory = cast(Callable[..., TransportGateway], provider)
        return factory(config.transport, clock, **kwargs)

    # ------------------------------------------------------------------ internals ---------
    @staticmethod
    def _entry_points() -> Iterable[importlib.metadata.EntryPoint]:
        return importlib.metadata.entry_points(group=ENTRY_POINT_GROUP)

    @classmethod
    def _import(cls, spec: str) -> type[TransportGateway]:
        module_name, _, attribute = spec.partition(_IMPORT_PATH_SEPARATOR)
        try:
            module = importlib.import_module(module_name)
            candidate = getattr(module, attribute)
        except (ImportError, AttributeError) as exc:
            raise ConfigError(
                "TRANSPORT_PROVIDER_UNKNOWN",
                provider=spec,
                available=cls.names(),
                error=f"{type(exc).__name__}: {exc}",
            ) from exc
        return _checked(candidate, spec)

    @staticmethod
    def _load_entry_point(point: importlib.metadata.EntryPoint) -> type[TransportGateway]:
        try:
            candidate = point.load()
        except (ImportError, AttributeError) as exc:
            raise ConfigError(
                "TRANSPORT_PROVIDER_INVALID",
                provider=point.name,
                reason="entry_point_load_failed",
                entry_point=point.value,
                error=f"{type(exc).__name__}: {exc}",
            ) from exc
        return _checked(candidate, point.name)


def _unusable_reason(candidate: Any) -> str | None:
    """Why ``candidate`` cannot serve as a provider, or ``None`` when it can."""
    if not inspect.isclass(candidate):
        return "not a class"
    if not issubclass(candidate, TransportGateway):
        return "not a TransportGateway subclass"
    if inspect.isabstract(candidate):
        return "abstract class"
    return None


def _checked(candidate: Any, spec: str) -> type[TransportGateway]:
    problem = _unusable_reason(candidate)
    if problem is not None:
        raise ConfigError(
            "TRANSPORT_PROVIDER_INVALID", provider=spec, reason=problem, resolved=repr(candidate)
        )
    return cast(type[TransportGateway], candidate)
