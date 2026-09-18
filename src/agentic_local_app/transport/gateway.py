"""``transport.gateway`` — the historical public surface of the transport layer (§3.12, ADR-004).

Since ADR-020 the contract lives in :mod:`agentic_local_app.transport.base` (``TransportGateway``,
``PostAck``, ``GetResult``, ``InFlightGuard``, ``OP_*``) and the ADR-004 implementation is the
``generic_http`` provider (:mod:`agentic_local_app.transport.providers.generic_http`), of which
``HttpTransportGateway`` is the historical name. Both keep being importable from here with the
same behaviour; new code imports from the modules that own the names.
"""

from __future__ import annotations

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
from agentic_local_app.transport.providers.generic_http import HttpTransportGateway

__all__ = [
    "OP_CLOSE",
    "OP_GET",
    "OP_INIT",
    "OP_POST",
    "GetResult",
    "HttpTransportGateway",
    "InFlightGuard",
    "PostAck",
    "TransportGateway",
]
