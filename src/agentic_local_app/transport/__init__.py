"""Transport layer (§3.12, ADR-004): the only place that talks to the remote model.

``TransportGateway`` is the boundary (ABC); ``HttpTransportGateway`` is the real implementation
(httpx, configurable endpoints, gzip, polling GET, in-flight abandonment); ``FakeTransportGateway``
is the scripted double used by every other phase.
"""

from agentic_local_app.transport.fake import FakeTransportGateway
from agentic_local_app.transport.gateway import (
    OP_CLOSE,
    OP_GET,
    OP_INIT,
    OP_POST,
    GetResult,
    HttpTransportGateway,
    InFlightGuard,
    PostAck,
    TransportGateway,
)

__all__ = [
    "OP_CLOSE",
    "OP_GET",
    "OP_INIT",
    "OP_POST",
    "FakeTransportGateway",
    "GetResult",
    "HttpTransportGateway",
    "InFlightGuard",
    "PostAck",
    "TransportGateway",
]
