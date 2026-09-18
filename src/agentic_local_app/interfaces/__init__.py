"""User interfaces (ADR-002, ADR-018): the local HTTP API (REST + SSE) and the ``agentic-app`` CLI.

Both are thin mappings over the ``ConversationManager`` façade (§3.1) and contain no business
logic. Nothing here is imported by the rest of the package (module map rule 2):

- :mod:`agentic_local_app.interfaces.http_api` — ``create_app(manager)``: FastAPI application under
  ``/api/v1`` (sessions, snapshot, plans, tasks, output ranges, messages, failures, audit, metrics,
  health, config) and the SSE routes;
- :mod:`agentic_local_app.interfaces.sse` — ``SseBroker``: the non-critical bus subscriber that
  fans events out to bounded per-client queues (ADR-015), with ``Last-Event-ID`` resume;
- :mod:`agentic_local_app.interfaces.cli` — the typer application (``run``, ``serve``, ``status``,
  ``sessions``, ``interrupt``, ``config show|validate``, ``mock-server``, ``audit verify``,
  ``version``); ``main`` is the ``agentic-app`` console script.
"""
