"""Transport layer (§3.12, ADR-004, ADR-020): the only place that talks to the remote model.

``TransportGateway`` (:mod:`~agentic_local_app.transport.base`) is the boundary; the
implementations are **providers** chosen by ``transport.provider`` through the
:class:`~agentic_local_app.transport.registry.TransportRegistry`:

- ``generic_http`` — :class:`~agentic_local_app.transport.providers.generic_http.GenericHttpProvider`,
  the ADR-004 contract (``HttpTransportGateway`` is its historical name);
- ``templated_http`` — :class:`~agentic_local_app.transport.providers.templated_http.TemplatedHttpProvider`,
  any HTTP API described by ``transport.options``;
- ``fake`` — :class:`~agentic_local_app.transport.fake.FakeTransportProvider`, the scripted double;
- any ``package.module:ClassName`` or entry point of the ``agentic_local_app.transports`` group.

HTTP providers share :class:`~agentic_local_app.transport.http_base.HttpProviderBase` (httpx
client, gzip, polling GET, error table, in-flight abandonment). Importing this package registers
the built-in providers.
"""

from agentic_local_app.transport.base import (
    OP_CLOSE,
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    InFlightGuard,
    PostAck,
    TransportGateway,
)
from agentic_local_app.transport.fake import FakeTransportGateway, FakeTransportProvider
from agentic_local_app.transport.http_base import HttpCall, HttpProviderBase, InvalidResponseError
from agentic_local_app.transport.providers.generic_http import (
    GenericHttpProvider,
    HttpTransportGateway,
)
from agentic_local_app.transport.providers.templated_http import TemplatedHttpProvider
from agentic_local_app.transport.registry import ProviderInfo, TransportRegistry

__all__ = [
    "OP_CLOSE",
    "OP_GET",
    "OP_INIT",
    "OP_POST",
    "FakeTransportGateway",
    "FakeTransportProvider",
    "GenericHttpProvider",
    "GetResult",
    "HttpCall",
    "HttpProviderBase",
    "HttpTransportGateway",
    "InFlightGuard",
    "InvalidResponseError",
    "PostAck",
    "ProviderInfo",
    "TemplatedHttpProvider",
    "TransportGateway",
    "TransportRegistry",
]
