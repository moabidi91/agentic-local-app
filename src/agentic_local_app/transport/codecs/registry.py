"""``CodecRegistry`` — the message codecs the application can be configured with (ADR-021).

The same mechanism as the transport providers (ADR-020, :class:`~agentic_local_app.transport.registry.PluginRegistry`):
``transport.codec`` is a registered name (``passthrough``, ``json_text``...), an import path
``package.module:ClassName`` or an entry point of the ``agentic_local_app.codecs`` group;
``transport.codec_options`` is validated by the codec's ``options_model``. Errors are
``ConfigError``: ``CODEC_UNKNOWN`` (with the available names), ``CODEC_INVALID`` (not a usable
``MessageCodec``), ``CODEC_OPTIONS_INVALID`` (pydantic ``loc`` / ``msg`` / ``type``).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from agentic_local_app.config import AppConfig
from agentic_local_app.transport.codecs.base import MessageCodec
from agentic_local_app.transport.registry import PluginInfo, PluginRegistry

__all__ = ["CODEC_ENTRY_POINT_GROUP", "CodecRegistry"]

#: The ``importlib.metadata`` entry point group a distribution uses to publish a codec.
CODEC_ENTRY_POINT_GROUP = "agentic_local_app.codecs"


class CodecRegistry(PluginRegistry[MessageCodec]):
    """The message codecs (ADR-021): ``transport.codec`` and ``transport.codec_options``."""

    base = MessageCodec
    kind = "codec"
    entry_point_group = CODEC_ENTRY_POINT_GROUP
    unknown_code = "CODEC_UNKNOWN"
    invalid_code = "CODEC_INVALID"
    options_code = "CODEC_OPTIONS_INVALID"

    @classmethod
    def list_codecs(cls) -> list[PluginInfo]:
        """The registered codecs and the entry points (not loaded), sorted by name."""
        return cls.list_plugins()

    @classmethod
    def create(cls, config: AppConfig) -> MessageCodec:
        """Instantiate the configured codec with its validated options."""
        codec = cls.resolve(config.transport.codec)
        options = cls.validate(codec, config.transport.codec_options)
        factory = cast(Callable[..., MessageCodec], codec)
        return factory(options)
