"""Built-in transport providers (ADR-020), each registered under its name in the
:class:`~agentic_local_app.transport.registry.TransportRegistry` when its module is imported:

- :mod:`~agentic_local_app.transport.providers.generic_http` — ``generic_http``: the ADR-004
  contract (``HttpTransportGateway`` is its compatibility name);
- :mod:`~agentic_local_app.transport.providers.templated_http` — ``templated_http``: any HTTP API
  described by ``transport.options`` (URL, headers and body templates, response paths).

The package itself imports nothing: :mod:`agentic_local_app.transport` imports both modules so
that the names are registered as soon as the transport package is.
"""
