"""``agentic-app`` — the console interface (ADR-002, ADR-018).

Commands: ``run`` (a session in-process, live display, **Ctrl-C = interruption**), ``serve`` (the
HTTP API), ``status`` / ``sessions`` / ``interrupt`` / ``audit verify`` (clients of the API),
``config show`` / ``config validate``, ``transport list`` / ``transport show`` (the pluggable
transport providers of ADR-020), ``mock-server`` (the scripted model), ``version``.

Everything external is injectable through :class:`CliDependencies` (``ctx.obj``): the application
factory (``orchestration.wiring.build_application``, imported lazily so that this module never
couples to the orchestration package at import time), the server runners and the HTTP transport
of the API client. The tests drive every command through ``typer.testing.CliRunner`` without a
process, a port or a network call.

Ctrl-C in ``run``: the first one turns into ``manager.interrupt(session_id)`` — either as the
``KeyboardInterrupt`` raised at the await point or as the ``CancelledError`` that ``asyncio.run``
injects on SIGINT (which is then *uncancelled* so the interruption can run to ``READY``); the
second one, raised by ``asyncio.run`` itself, ends the process (exit code 2). Exit codes: 0 final
answer, 1 failure or error, 2 interrupted.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Protocol, cast

import httpx
import typer
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from agentic_local_app import __version__
from agentic_local_app.config import AppConfig, load_config
from agentic_local_app.domain.errors import AppError, ConfigError, NormalizedError
from agentic_local_app.domain.events import Event, EventType
from agentic_local_app.domain.models import SessionBudget, SessionRecord
from agentic_local_app.domain.states import SessionState
from agentic_local_app.interfaces.http_api import API_PREFIX, ConversationManagerLike, create_app
from agentic_local_app.interruption.handler import InterruptionReport
from agentic_local_app.testing.mock_model_server import run_mock_server
from agentic_local_app.transport.base import validate_options
from agentic_local_app.transport.registry import TransportRegistry

__all__ = [
    "CLI_SUBSCRIBER_NAME",
    "EXIT_FAILED",
    "EXIT_INTERRUPTED",
    "EXIT_OK",
    "ApplicationLike",
    "CliDependencies",
    "app",
    "main",
    "render_snapshot",
]

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_INTERRUPTED = 2
#: Name of the bus subscriber that feeds the live display of ``run``.
CLI_SUBSCRIBER_NAME = "cli"
_EVENT_TAIL = 8
_OUTPUT_TAIL = 12
_TERMINAL_SESSION_STATES = frozenset({SessionState.COMPLETED, SessionState.FAILED})
_ACTIVE_SESSION_STATES = frozenset({SessionState.RUNNING, SessionState.INTERRUPTING})


class ApplicationLike(Protocol):
    """What ``orchestration.wiring.build_application(config)`` returns: it carries the façade."""

    @property
    def manager(self) -> ConversationManagerLike: ...


ServerRunner = Callable[..., None]


@dataclass
class CliDependencies:
    """Injection points (``ctx.obj``); ``None`` means the production default."""

    build_application: Callable[[AppConfig], ApplicationLike] | None = None
    server_runner: ServerRunner | None = None
    mock_server_runner: ServerRunner | None = None
    transport: httpx.BaseTransport | None = None
    config_path: Path | None = None


app = typer.Typer(
    name="agentic-app",
    help="Local agentic application: run sessions, serve the API, inspect the state.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)
config_app = typer.Typer(help="Show or validate the effective configuration (config.toml).")
audit_app = typer.Typer(help="Audit chain tools.")
transport_app = typer.Typer(
    help="Transport providers: list the available ones, show the effective one."
)
app.add_typer(config_app, name="config")
app.add_typer(audit_app, name="audit")
app.add_typer(transport_app, name="transport")

ConfigOption = Annotated[
    Path | None, typer.Option("--config", help="Path of config.toml (else AGENTIC_APP_CONFIG).")
]
ApiUrlOption = Annotated[
    str | None, typer.Option("--api-url", help="Base URL of the API (default: from the config).")
]
JsonOption = Annotated[bool, typer.Option("--json", help="Machine-readable JSON output.")]


@app.callback()
def _root(ctx: typer.Context, config: ConfigOption = None) -> None:
    deps = ctx.ensure_object(CliDependencies)
    if config is not None:
        deps.config_path = config


# ================================================================================================
# shared helpers
# ================================================================================================
def _deps(ctx: typer.Context) -> CliDependencies:
    return ctx.ensure_object(CliDependencies)


def _console() -> Console:
    return Console()


def _fail(message: str, code: int = EXIT_FAILED) -> typer.Exit:
    typer.echo(message, err=True)
    return typer.Exit(code)


def _config_error_lines(exc: ConfigError) -> list[str]:
    """Readable lines for a ``ConfigError`` (``CONFIG_INVALID`` lists every field problem)."""
    error = exc.error
    lines = [f"Configuration error {error.error_code}"]
    details = dict(error.details)
    problems = details.pop("errors", None)
    for key, value in details.items():
        lines.append(f"  {key}: {value}")
    if isinstance(problems, list):
        for problem in problems:
            if isinstance(problem, dict):
                location = ".".join(str(part) for part in problem.get("loc", ())) or "<root>"
                lines.append(f"  {location}: {problem.get('msg', problem.get('type', '?'))}")
            else:
                lines.append(f"  {problem}")
    return lines


def _load(path: Path | None) -> AppConfig:
    try:
        return load_config(path)
    except ConfigError as exc:
        for line in _config_error_lines(exc):
            typer.echo(line, err=True)
        raise typer.Exit(EXIT_FAILED) from None


def _config_path(ctx: typer.Context, config: Path | None) -> Path | None:
    return config if config is not None else _deps(ctx).config_path


#: Where the production application factory lives (imported lazily, ADR-002 / module map rule 2).
WIRING_MODULE = "agentic_local_app.orchestration.wiring"
WIRING_FACTORY = "build_application"


def _build_application(deps: CliDependencies, config: AppConfig) -> ApplicationLike:
    factory = deps.build_application
    if factory is None:
        try:
            wiring = importlib.import_module(WIRING_MODULE)
            factory = cast(Callable[[AppConfig], ApplicationLike], getattr(wiring, WIRING_FACTORY))
        except (ImportError, AttributeError) as exc:  # pragma: no cover - not deployed
            raise _fail(f"The orchestration package is not available: {exc}") from None
    try:
        return factory(config)
    except ConfigError as exc:  # e.g. an unknown transport provider (ADR-020)
        for line in _config_error_lines(exc):
            typer.echo(line, err=True)
        raise typer.Exit(EXIT_FAILED) from None


def _api_base(config: AppConfig, api_url: str | None) -> str:
    root = api_url or f"http://{config.api.host}:{config.api.port}"
    return root.rstrip("/") + API_PREFIX


def _api_call(deps: CliDependencies, base_url: str, method: str, path: str, **kwargs: Any) -> Any:
    """One request to the API; a normalized error or a transport failure ends the command."""
    try:
        with httpx.Client(base_url=base_url, transport=deps.transport, timeout=30.0) as client:
            response = client.request(method, path, **kwargs)
    except httpx.HTTPError as exc:
        raise _fail(f"Cannot reach the API at {base_url}: {exc}") from None
    if response.status_code >= 400:
        raise _fail(_describe_http_error(response)) from None
    return response.json()


def _describe_http_error(response: httpx.Response) -> str:
    try:
        error = response.json().get("error", {})
    except ValueError:
        error = {}
    code = error.get("error_code", f"HTTP_{response.status_code}")
    message = error.get("details", {}).get("message") if isinstance(error, dict) else None
    return f"API error {response.status_code} {code}" + (f": {message}" if message else "")


def _print_error(error: NormalizedError) -> None:
    typer.echo(
        f"Error {error.error_type.value}/{error.error_code} ({error.origin}): "
        f"{json.dumps(error.details, sort_keys=True, default=str)}",
        err=True,
    )


def _value(value: Any) -> str:
    return "-" if value is None else str(value)


# ================================================================================================
# rendering (shared by ``run`` and ``status``; works on the JSON form of a RuntimeSnapshot)
# ================================================================================================
def _row(label: str, *cells: Any) -> tuple[str, Text]:
    """A grid row whose value is plain text (never interpreted as rich markup)."""
    return label, Text("".join(str(cell) for cell in cells))


def render_snapshot(
    snapshot: dict[str, Any],
    *,
    events: Sequence[str] = (),
    output: Sequence[str] = (),
) -> RenderableType:
    """A rich renderable of a snapshot (``RuntimeSnapshot.model_dump(mode="json")``)."""
    session = snapshot.get("session", {})
    conversation = snapshot.get("conversation") or {}
    budget = session.get("session_budget", {})
    cycle = snapshot.get("cycle") or {}
    plan = snapshot.get("plan") or {}
    interaction = snapshot.get("model_interaction", {})
    header = Table.grid(padding=(0, 2))
    header.add_column(style="bold")
    header.add_column()
    header.add_row(*_row("Session", f"{session.get('session_id')}  [{session.get('status')}]"))
    header.add_row(*_row("Goal", _value(session.get("goal"))))
    header.add_row(
        *_row(
            "Budget",
            f"cycles {budget.get('consumed_cycles')}/{budget.get('max_cycles')}  ",
            f"plans {budget.get('consumed_plans')}/{budget.get('max_plans')}  ",
            f"duration {budget.get('consumed_duration_ms')}/{budget.get('max_total_duration_ms')}",
            f" ms  rotations {session.get('rotations_count')}",
        )
    )
    header.add_row(
        *_row(
            "Conversation",
            f"{_value(conversation.get('conversation_id'))}  ",
            f"[{_value(conversation.get('status'))}]  ",
            f"context {_value(conversation.get('context_window_state'))} ",
            f"({_value(conversation.get('context_bytes'))} bytes)  ",
            f"model response {_value(conversation.get('last_model_response_state'))}",
        )
    )
    header.add_row(
        *_row(
            "Cycle",
            f"{_value(cycle.get('cycle_id'))}  {_value(cycle.get('cycle_type'))}  ",
            f"[{_value(cycle.get('status'))}]",
        )
    )
    header.add_row(
        *_row(
            "Plan",
            f"{_value(plan.get('plan_id'))}  {_value(plan.get('plan_type'))}  ",
            f"[{_value(plan.get('status'))}]  ",
            f"tasks {plan.get('completed_task_count', 0)}/{plan.get('task_count', 0)} done, ",
            f"{plan.get('failed_task_count', 0)} failed, ",
            f"{plan.get('skipped_task_count', 0)} skipped",
            f"  stop: {plan['stop_reason']}" if plan.get("stop_reason") else "",
        )
    )
    header.add_row(
        *_row(
            "Model",
            f"out {_value(interaction.get('last_outbound_message_type'))} ",
            f"(POST {_value(interaction.get('last_post_status'))})  ",
            f"in {_value(interaction.get('last_inbound_message_type'))} ",
            f"(GET {_value(interaction.get('last_get_status'))}, ",
            f"{_value(interaction.get('last_protocol_validation_status'))})",
        )
    )
    header.add_row(
        *_row(
            "Last event",
            f"{_value(snapshot.get('last_event_type'))} ",
            f"#{_value(snapshot.get('last_event_sequence'))}",
            f"  at {_value(snapshot.get('snapshot_at'))}",
        )
    )
    parts: list[RenderableType] = [Panel(header, title="agentic-app", title_align="left")]

    tasks = snapshot.get("tasks") or []
    if tasks:
        running = set(snapshot.get("running_task_ids") or [])
        table = Table(title="Tasks of the current plan", title_justify="left", expand=True)
        table.add_column("task", no_wrap=True)
        table.add_column("status", no_wrap=True)
        table.add_column("exit", justify="right", no_wrap=True)
        table.add_column("ms", justify="right", no_wrap=True)
        table.add_column("cmd", overflow="fold", ratio=3)
        for task in tasks:
            marker = "> " if task.get("task_id") in running else "  "
            table.add_row(
                Text(f"{marker}{task.get('task_id')}"),
                Text(_value(task.get("status"))),
                Text(_value(task.get("exit_code"))),
                Text(_value(task.get("duration_ms"))),
                Text(_value(task.get("cmd"))),
            )
        parts.append(table)
    if events:
        parts.append(Panel(Text("\n".join(events)), title="Last events", title_align="left"))
    if output:
        parts.append(Panel(Text("\n".join(output)), title="Live output", title_align="left"))
    return Group(*parts)


def _render_report(report: dict[str, Any]) -> RenderableType:
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()
    for key in (
        "session_id",
        "session_status",
        "reason",
        "duration_ms",
        "within_timeout",
        "loop_drained",
        "nothing_to_interrupt",
        "interrupted_task_ids",
        "plan_id",
        "cycle_id",
        "conversation_id",
    ):
        value = report.get(key)
        grid.add_row(*_row(key, ", ".join(value) if isinstance(value, list) else _value(value)))
    return Panel(grid, title="Interruption report", title_align="left")


class _EventTail:
    """Bus subscriber of ``run``: the last events and output lines for the live display."""

    def __init__(self) -> None:
        self.events: deque[str] = deque(maxlen=_EVENT_TAIL)
        self.output: deque[str] = deque(maxlen=_OUTPUT_TAIL)

    def __call__(self, event: Event) -> None:
        if event.event_type is EventType.TASK_OUTPUT:
            data = event.payload.get("data")
            if isinstance(data, str):
                prefix = f"[{event.task_id}:{event.payload.get('stream', '?')}] "
                self.output.extend(prefix + line for line in data.splitlines() if line.strip())
            return
        summary = ", ".join(
            f"{key}={value}"
            for key, value in event.payload.items()
            if key in ("from", "to", "reason", "message_type", "plan_type", "status", "limit")
        )
        target = event.task_id or event.plan_id or event.cycle_id or event.conversation_id or ""
        self.events.append(f"{event.event_type.value} {target} {summary}".rstrip())


# ================================================================================================
# run
# ================================================================================================
@dataclass
class _RunOutcome:
    session: SessionRecord
    exit_code: int
    interruption: InterruptionReport | None = None
    error: NormalizedError | None = None
    snapshot: dict[str, Any] = field(default_factory=dict)


def _uncancel() -> None:
    """After catching the ``CancelledError`` of a SIGINT, let the task await again (3.11+)."""
    task = asyncio.current_task()
    if task is not None:
        task.uncancel()


async def _run_session(
    manager: ConversationManagerLike,
    config: AppConfig,
    *,
    goal: str,
    message: str,
    budget: SessionBudget | None,
    auto_close: bool | None,
    json_output: bool,
    console: Console,
) -> _RunOutcome | NormalizedError:
    tail = _EventTail()
    manager.bus.subscribe(tail, name=CLI_SUBSCRIBER_NAME)
    try:
        try:
            session = await manager.start_session(
                goal=goal, user_message=message, budget=budget, auto_close=auto_close
            )
        except AppError as exc:
            return exc.error
        sid = session.session_id
        interruption: InterruptionReport | None = None
        error: NormalizedError | None = None
        seen_active = False
        live = Live(console=console, auto_refresh=False, transient=False)
        if not json_output:
            live.start()
        try:
            while True:
                if not json_output:
                    live.update(_live_view(manager, sid, tail), refresh=True)
                if session.status in _TERMINAL_SESSION_STATES:
                    break
                if session.status is SessionState.READY and (seen_active or interruption):
                    break
                seen_active = seen_active or session.status in _ACTIVE_SESSION_STATES
                try:
                    session = await manager.wait(sid, timeout_ms=config.cli.refresh_interval_ms)
                except TimeoutError:
                    # still running: refresh the record and redraw (the façade raises on timeout)
                    session = manager.get_session(sid) or session
                except (asyncio.CancelledError, KeyboardInterrupt):
                    _uncancel()
                    if not json_output:
                        console.print(
                            "Ctrl-C: interrupting the session (Ctrl-C again to force exit)..."
                        )
                    interruption = await manager.interrupt(sid)
                    session = manager.get_session(sid) or session
                except AppError as exc:
                    error = exc.error
                    session = manager.get_session(sid) or session
                    break
        finally:
            if not json_output:
                live.stop()
                if not console.is_terminal:
                    console.line()
        snapshot = manager.snapshot(sid).model_dump(mode="json")
        if session.status is SessionState.COMPLETED and error is None:
            code = EXIT_OK
        elif interruption is not None or session.status is SessionState.READY:
            code = EXIT_INTERRUPTED
        else:
            code = EXIT_FAILED
        return _RunOutcome(session, code, interruption, error, snapshot)
    finally:
        manager.bus.unsubscribe(CLI_SUBSCRIBER_NAME)
        await manager.shutdown()


def _live_view(manager: ConversationManagerLike, sid: str, tail: _EventTail) -> RenderableType:
    snapshot = manager.snapshot(sid).model_dump(mode="json")
    return render_snapshot(snapshot, events=list(tail.events), output=list(tail.output))


def _report_dict(report: InterruptionReport | None) -> dict[str, Any] | None:
    if report is None:
        return None
    return {
        "session_id": report.session_id,
        "reason": report.reason,
        "requested_at": report.requested_at.isoformat(),
        "completed_at": report.completed_at.isoformat(),
        "duration_ms": report.duration_ms,
        "within_timeout": report.within_timeout,
        "loop_drained": report.loop_drained,
        "nothing_to_interrupt": report.nothing_to_interrupt,
        "interrupted_task_ids": list(report.interrupted_task_ids),
        "plan_id": report.plan_id,
        "cycle_id": report.cycle_id,
        "conversation_id": report.conversation_id,
        "session_status": report.session_status.value,
    }


def _print_outcome(
    console: Console, manager: ConversationManagerLike, outcome: _RunOutcome, json_output: bool
) -> None:
    session = outcome.session
    final_answer = manager.final_answer(session.session_id)
    if json_output:
        document = {
            "session_id": session.session_id,
            "status": session.status.value,
            "exit_code": outcome.exit_code,
            "final_answer": final_answer,
            "interruption": _report_dict(outcome.interruption),
            "error": outcome.error.model_dump(mode="json") if outcome.error else None,
            "snapshot": outcome.snapshot,
        }
        typer.echo(json.dumps(document, indent=2, sort_keys=True, default=str))
        return
    if outcome.interruption is not None:
        console.print(_render_report(_report_dict(outcome.interruption) or {}))
    if outcome.error is not None:
        _print_error(outcome.error)
    status = session.status.value
    if final_answer is not None:
        console.print(
            Panel(
                json.dumps(final_answer, indent=2, ensure_ascii=False, default=str),
                title=f"Final answer — session {session.session_id} [{status}]",
                title_align="left",
            )
        )
    else:
        console.print(f"Session {session.session_id} ended [{status}] without a final answer.")


def _budget(
    config: AppConfig, cycles: int | None, plans: int | None, duration_ms: int | None
) -> SessionBudget | None:
    if cycles is None and plans is None and duration_ms is None:
        return None
    return SessionBudget(
        max_cycles=cycles if cycles is not None else config.budget.default_max_cycles,
        max_plans=plans if plans is not None else config.budget.default_max_plans,
        max_total_duration_ms=(
            duration_ms if duration_ms is not None else config.budget.default_max_total_duration_ms
        ),
    )


@app.command()
def run(
    ctx: typer.Context,
    goal: Annotated[str, typer.Argument(help="Goal of the session.")],
    message: Annotated[
        str | None, typer.Option("--message", "-m", help="First user message (default: the goal).")
    ] = None,
    config: ConfigOption = None,
    budget_cycles: Annotated[int | None, typer.Option("--budget-cycles", min=1)] = None,
    budget_plans: Annotated[int | None, typer.Option("--budget-plans", min=1)] = None,
    budget_duration_ms: Annotated[int | None, typer.Option("--budget-duration-ms", min=1)] = None,
    auto_close: Annotated[
        bool, typer.Option("--auto-close", help="Close the conversation on the final answer.")
    ] = False,
    json_output: JsonOption = False,
) -> None:
    """Start a session, follow it live; Ctrl-C interrupts it (twice: forced exit)."""
    deps = _deps(ctx)
    cfg = _load(_config_path(ctx, config))
    application = _build_application(deps, cfg)
    console = _console()
    try:
        result = asyncio.run(
            _run_session(
                application.manager,
                cfg,
                goal=goal,
                message=message if message is not None else goal,
                budget=_budget(cfg, budget_cycles, budget_plans, budget_duration_ms),
                auto_close=True if auto_close else None,
                json_output=json_output,
                console=console,
            )
        )
    except KeyboardInterrupt:
        typer.echo("Forced exit (second Ctrl-C).", err=True)
        raise typer.Exit(EXIT_INTERRUPTED) from None
    if isinstance(result, NormalizedError):
        _print_error(result)
        raise typer.Exit(EXIT_FAILED)
    _print_outcome(console, application.manager, result, json_output)
    raise typer.Exit(result.exit_code)


# ================================================================================================
# serve · mock-server
# ================================================================================================
def _run_uvicorn(asgi_app: Any, *, host: str, port: int, log_level: str = "info") -> None:
    import uvicorn

    uvicorn.run(asgi_app, host=host, port=port, log_level=log_level)


@app.command()
def serve(
    ctx: typer.Context,
    config: ConfigOption = None,
    host: Annotated[str | None, typer.Option("--host")] = None,
    port: Annotated[int | None, typer.Option("--port", min=1, max=65535)] = None,
) -> None:
    """Serve the local HTTP API (REST + SSE) with uvicorn."""
    deps = _deps(ctx)
    cfg = _load(_config_path(ctx, config))
    application = _build_application(deps, cfg)
    api = create_app(application.manager)
    bind_host = host or cfg.api.host
    bind_port = port or cfg.api.port
    typer.echo(f"Serving the API on http://{bind_host}:{bind_port}{API_PREFIX} (Ctrl-C to stop)")
    runner = deps.server_runner
    if runner is None:
        _run_uvicorn(api, host=bind_host, port=bind_port, log_level=cfg.app.log_level.lower())
    else:
        runner(api, host=bind_host, port=bind_port)


@app.command("mock-server")
def mock_server(
    ctx: typer.Context,
    scenario: Annotated[
        Path | None, typer.Option("--scenario", help="Scenario JSON (default: Java debug loop).")
    ] = None,
    host: Annotated[str, typer.Option("--host")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", min=1, max=65535)] = 9000,
) -> None:
    """Serve the mock model (a scripted scenario) for local runs and demos."""
    deps = _deps(ctx)
    typer.echo(
        f"Mock model server on http://{host}:{port} "
        f"(scenario: {scenario if scenario is not None else 'default java debug'})"
    )
    run_mock_server(host, port, scenario, runner=deps.mock_server_runner)


# ================================================================================================
# API clients: status · sessions · interrupt · audit verify
# ================================================================================================
@app.command()
def status(
    ctx: typer.Context,
    session_id: Annotated[str, typer.Argument()],
    api_url: ApiUrlOption = None,
    json_output: JsonOption = False,
) -> None:
    """Show the runtime snapshot (§4.1) of a session through the API."""
    deps = _deps(ctx)
    base = _api_base(_load(deps.config_path), api_url)
    snapshot = _api_call(deps, base, "GET", f"/sessions/{session_id}/snapshot")
    if json_output:
        typer.echo(json.dumps(snapshot, indent=2, sort_keys=True))
        return
    _console().print(render_snapshot(snapshot))


@app.command()
def sessions(
    ctx: typer.Context,
    api_url: ApiUrlOption = None,
    status_filter: Annotated[
        str | None, typer.Option("--status", help="Comma-separated states (running,ready...).")
    ] = None,
    limit: Annotated[int | None, typer.Option("--limit", min=1)] = None,
    json_output: JsonOption = False,
) -> None:
    """List the sessions known to the API."""
    deps = _deps(ctx)
    base = _api_base(_load(deps.config_path), api_url)
    params: dict[str, Any] = {}
    if status_filter:
        params["status"] = status_filter
    if limit is not None:
        params["limit"] = limit
    page = _api_call(deps, base, "GET", "/sessions", params=params)
    if json_output:
        typer.echo(json.dumps(page, indent=2, sort_keys=True))
        return
    items = page.get("items", [])
    if not items:
        typer.echo("No session.")
        return
    table = Table(expand=True)
    table.add_column("session", no_wrap=True)
    table.add_column("status", no_wrap=True)
    table.add_column("goal", overflow="fold", ratio=3)
    table.add_column("conversation", no_wrap=True)
    table.add_column("created", no_wrap=True)
    for item in items:
        table.add_row(
            _value(item.get("session_id")),
            _value(item.get("status")),
            _value(item.get("goal")),
            _value(item.get("current_conversation_id")),
            _value(item.get("created_at")),
        )
    _console().print(table)


@app.command()
def interrupt(
    ctx: typer.Context,
    session_id: Annotated[str, typer.Argument()],
    api_url: ApiUrlOption = None,
    json_output: JsonOption = False,
) -> None:
    """Interrupt a session through the API (answers once the session is READY)."""
    deps = _deps(ctx)
    base = _api_base(_load(deps.config_path), api_url)
    report = _api_call(deps, base, "POST", f"/sessions/{session_id}/interrupt")
    if json_output:
        typer.echo(json.dumps(report, indent=2, sort_keys=True))
        return
    _console().print(_render_report(report))


@audit_app.command("verify")
def audit_verify(
    ctx: typer.Context,
    session_id: Annotated[str, typer.Argument()],
    api_url: ApiUrlOption = None,
    json_output: JsonOption = False,
) -> None:
    """Verify the hash chain of a session's audit trail (exit 1 when broken)."""
    deps = _deps(ctx)
    base = _api_base(_load(deps.config_path), api_url)
    verification = _api_call(deps, base, "GET", f"/sessions/{session_id}/audit/verify")
    if json_output:
        typer.echo(json.dumps(verification, indent=2, sort_keys=True))
    elif verification.get("valid"):
        typer.echo(
            f"Audit chain of {session_id} valid: {verification.get('checked')} events checked "
            f"(verified at {verification.get('verified_at')})."
        )
    else:
        typer.echo(
            f"Audit chain of {session_id} BROKEN at sequence "
            f"{verification.get('first_broken_sequence')}: {verification.get('reason')} "
            f"({verification.get('checked')} events intact before it).",
            err=True,
        )
    if not verification.get("valid"):
        raise typer.Exit(EXIT_FAILED)


# ================================================================================================
# config show · config validate · version
# ================================================================================================
@config_app.command("show")
def config_show(ctx: typer.Context, config: ConfigOption = None) -> None:
    """Print the effective configuration as JSON, token masked."""
    cfg = _load(_config_path(ctx, config))
    typer.echo(json.dumps(cfg.masked(), indent=2, sort_keys=True, default=str))


@config_app.command("validate")
def config_validate(ctx: typer.Context, config: ConfigOption = None) -> None:
    """Load and validate the configuration; exit 1 with readable errors when invalid."""
    path = _config_path(ctx, config)
    _load(path)
    source = str(path) if path is not None else "AGENTIC_APP_CONFIG / ./config.toml / defaults"
    typer.echo(f"Configuration valid ({source}).")


# ================================================================================================
# transport list · transport show (ADR-020)
# ================================================================================================
@transport_app.command("list")
def transport_list(json_output: JsonOption = False) -> None:
    """List the selectable transport providers (name, class, origin)."""
    rows = [
        {"name": info.name, "class": info.qualified_name, "origin": info.origin}
        for info in TransportRegistry.list_providers()
    ]
    if json_output:
        typer.echo(json.dumps(rows, indent=2, sort_keys=True))
        return
    columns = ("name", "class", "origin")
    widths = {key: max(len(key), *(len(row[key]) for row in rows)) for key in columns}
    typer.echo("  ".join(key.upper().ljust(widths[key]) for key in columns).rstrip())
    for row in rows:
        typer.echo("  ".join(row[key].ljust(widths[key]) for key in columns).rstrip())


@transport_app.command("show")
def transport_show(
    ctx: typer.Context, config: ConfigOption = None, json_output: JsonOption = False
) -> None:
    """Show the effective transport provider and its options (secrets masked)."""
    cfg = _load(_config_path(ctx, config))
    try:
        info, provider = TransportRegistry.describe(cfg.transport.provider)
        validate_options(provider, cfg.transport.options)
    except ConfigError as exc:
        for line in _config_error_lines(exc):
            typer.echo(line, err=True)
        raise typer.Exit(EXIT_FAILED) from None
    options_model = getattr(provider, "options_model", None)
    masked = cfg.masked()["transport"]
    document = {
        "provider": cfg.transport.provider,
        "class": info.qualified_name,
        "origin": info.origin,
        "options_model": options_model.__name__ if options_model is not None else None,
        "options": masked["options"],
        "transport": masked,
    }
    if json_output:
        typer.echo(json.dumps(document, indent=2, sort_keys=True, default=str))
        return
    for key in ("provider", "class", "origin", "options_model"):
        typer.echo(f"{key}: {_value(document[key])}")
    typer.echo(f"options: {json.dumps(document['options'], indent=2, sort_keys=True)}")


@app.command()
def version() -> None:
    """Print the version."""
    typer.echo(f"agentic-app {__version__}")


def main() -> None:
    """Console script entry point (``agentic-app``)."""
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
