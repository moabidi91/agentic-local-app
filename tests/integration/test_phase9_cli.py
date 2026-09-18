"""Phase 9b — the ``agentic-app`` CLI (ADR-002, ADR-018) through ``typer.testing.CliRunner``.

No process is spawned and no port is opened: ``serve`` and ``mock-server`` receive an injected
runner, ``run`` receives an injected application factory returning the ``FakeConversationManager``
double, and the API-client commands (``status``, ``sessions``, ``interrupt``, ``audit verify``)
talk to an ``httpx.MockTransport``. Ctrl-C is simulated by a ``KeyboardInterrupt`` injected in the
façade double (ADR-002: Ctrl-C = interruption, not process exit).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from agentic_local_app import __version__
from agentic_local_app.config import ApiSection, AppConfig, TransportSection
from agentic_local_app.domain.models import SessionBudget
from agentic_local_app.domain.states import SessionState
from agentic_local_app.interfaces.cli import (
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    CliDependencies,
    app,
    main,
)
from integration.fake_manager import FakeConversationManager

pytestmark = pytest.mark.phase9

TOKEN_ENV = "AGENTIC_TRANSPORT_TOKEN_PHASE9_CLI"


# ================================================================================================
# harness
# ================================================================================================
class _Application:
    """What ``build_application`` returns: something carrying the façade."""

    def __init__(self, manager: FakeConversationManager) -> None:
        self.manager = manager


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(
        "\n".join(
            [
                "[app]",
                'name = "phase9-cli"',
                f'data_dir = "{(tmp_path / "data").as_posix()}"',
                "[transport]",
                f'token_env = "{TOKEN_ENV}"',
                "[api]",
                'host = "127.0.0.9"',
                "port = 9765",
                "[cli]",
                "refresh_interval_ms = 1",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def fake(config_file: Path) -> FakeConversationManager:
    return FakeConversationManager(
        AppConfig(
            api=ApiSection(host="127.0.0.9", port=9765),
            transport=TransportSection(token_env=TOKEN_ENV),
        )
    )


def _deps(fake: FakeConversationManager, **extra: Any) -> CliDependencies:
    captured: dict[str, Any] = {}

    def build_application(config: AppConfig) -> _Application:
        captured["config"] = config
        return _Application(fake)

    deps = CliDependencies(build_application=build_application, **extra)
    deps.captured = captured  # type: ignore[attr-defined]
    return deps


def _mock_api(
    routes: dict[tuple[str, str], Callable[[httpx.Request], httpx.Response] | tuple[int, Any]],
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    """An httpx transport answering ``(method, path)`` with a fixed ``(status, json)`` or a callable."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        route = routes.get((request.method, request.url.path))
        if route is None:
            return httpx.Response(404, json={"error": {"error_code": "HTTP_404"}})
        if callable(route):
            return route(request)
        status, body = route
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handler), seen


def _snapshot_payload(sid: str = "sess-0001") -> dict[str, Any]:
    fake = FakeConversationManager()
    import asyncio

    asyncio.run(fake.start_session(goal="fix the build", user_message="m"))
    fake.add_plan(
        sid,
        "plan-1",
        [{"task_id": "t1", "cmd": "mvn test", "status": "RUNNING"}, {"task_id": "t2"}],
    )
    return fake.snapshot(sid).model_dump(mode="json")


# ================================================================================================
# config show / validate · version
# ================================================================================================
def given_config_with_token_in_environment_when_config_show_then_token_masked(
    runner: CliRunner, config_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(TOKEN_ENV, "s3cr3t-token")
    result = runner.invoke(app, ["config", "show", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    assert "s3cr3t-token" not in result.output
    shown = json.loads(result.stdout)
    assert shown["transport"]["token"] == "***"
    assert shown["transport"]["token_env"] == TOKEN_ENV
    assert shown["app"]["name"] == "phase9-cli"
    assert shown["api"]["port"] == 9765


def given_global_config_option_when_config_show_then_same_file_used(
    runner: CliRunner, config_file: Path
) -> None:
    result = runner.invoke(app, ["--config", str(config_file), "config", "show"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["app"]["name"] == "phase9-cli"


def given_no_config_file_when_config_show_then_defaults_shown(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("AGENTIC_APP_CONFIG", raising=False)
    result = runner.invoke(app, ["config", "show"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["api"]["port"] == 8765


def given_invalid_config_file_when_config_validate_then_exit_1_and_readable_errors(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text('[api]\nport = 99999\n[transport]\npost_url = "http://x/no-placeholder"\n')
    result = runner.invoke(app, ["config", "validate", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "CONFIG_INVALID" in result.output
    assert "api.port" in result.output
    assert "transport.post_url" in result.output


def given_missing_config_file_when_config_validate_then_exit_1_with_path(
    runner: CliRunner, tmp_path: Path
) -> None:
    missing = tmp_path / "missing.toml"
    result = runner.invoke(app, ["config", "validate", "--config", str(missing)])
    assert result.exit_code == 1
    assert "CONFIG_FILE_NOT_FOUND" in result.output and "missing.toml" in result.output


def given_valid_config_file_when_config_validate_then_exit_0(
    runner: CliRunner, config_file: Path
) -> None:
    result = runner.invoke(app, ["config", "validate", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    assert "valid" in result.output.lower()
    assert str(config_file) in result.output


def given_cli_when_version_then_prints_package_version(runner: CliRunner) -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.output.strip() == f"agentic-app {__version__}"


def given_cli_when_help_then_every_command_listed(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for command in ("run", "serve", "status", "sessions", "interrupt", "config", "mock-server"):
        assert command in result.output
    assert "audit" in result.output
    assert callable(main)


# ================================================================================================
# serve · mock-server
# ================================================================================================
def given_injected_server_runner_when_serve_then_api_app_served_on_config_host_and_port(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    served: dict[str, Any] = {}

    def server_runner(asgi_app: Any, *, host: str, port: int) -> None:
        served.update(app=asgi_app, host=host, port=port)

    deps = _deps(fake, server_runner=server_runner)
    result = runner.invoke(app, ["serve", "--config", str(config_file)], obj=deps)
    assert result.exit_code == 0, result.output
    assert (served["host"], served["port"]) == ("127.0.0.9", 9765)
    assert served["app"].title.startswith("agentic")
    assert deps.captured["config"].app.name == "phase9-cli"  # type: ignore[attr-defined]
    assert "http://127.0.0.9:9765/api/v1" in result.output


def given_host_and_port_options_when_serve_then_they_override_the_config(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    served: dict[str, Any] = {}

    def server_runner(asgi_app: Any, *, host: str, port: int) -> None:
        served.update(host=host, port=port)

    result = runner.invoke(
        app,
        ["serve", "--config", str(config_file), "--host", "0.0.0.0", "--port", "1234"],
        obj=_deps(fake, server_runner=server_runner),
    )
    assert result.exit_code == 0, result.output
    assert (served["host"], served["port"]) == ("0.0.0.0", 1234)


def given_invalid_config_when_serve_then_exit_1_without_starting(
    runner: CliRunner, tmp_path: Path, fake: FakeConversationManager
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text("[api]\nport = 0\n")
    started: list[Any] = []
    result = runner.invoke(
        app,
        ["serve", "--config", str(path)],
        obj=_deps(fake, server_runner=lambda *a, **k: started.append(a)),
    )
    assert result.exit_code == 1
    assert started == [] and "CONFIG_INVALID" in result.output


def given_injected_mock_runner_when_mock_server_then_called_with_scenario_host_and_port(
    runner: CliRunner, tmp_path: Path
) -> None:
    scenario = tmp_path / "scenario.json"
    scenario.write_text(json.dumps({"steps": [{"on": "user_request", "respond": []}]}))
    calls: list[dict[str, Any]] = []

    def mock_runner(asgi_app: Any, *, host: str, port: int, **kwargs: Any) -> None:
        calls.append({"app": asgi_app, "host": host, "port": port})

    result = runner.invoke(
        app,
        ["mock-server", "--scenario", str(scenario), "--host", "127.0.0.2", "--port", "9100"],
        obj=CliDependencies(mock_server_runner=mock_runner),
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert (calls[0]["host"], calls[0]["port"]) == ("127.0.0.2", 9100)
    assert calls[0]["app"].title  # a FastAPI app
    assert "127.0.0.2:9100" in result.output


def given_no_scenario_when_mock_server_then_default_java_scenario_served(
    runner: CliRunner,
) -> None:
    calls: list[dict[str, Any]] = []
    result = runner.invoke(
        app,
        ["mock-server"],
        obj=CliDependencies(
            mock_server_runner=lambda a, *, host, port, **k: calls.append({"h": host, "p": port})
        ),
    )
    assert result.exit_code == 0, result.output
    assert calls == [{"h": "127.0.0.1", "p": 9000}]


# ================================================================================================
# API client commands: status · sessions · interrupt · audit verify
# ================================================================================================
def given_mock_api_when_status_then_snapshot_rendered(runner: CliRunner) -> None:
    snapshot = _snapshot_payload()
    transport, seen = _mock_api({("GET", "/api/v1/sessions/sess-0001/snapshot"): (200, snapshot)})
    result = runner.invoke(
        app,
        ["status", "sess-0001", "--api-url", "http://api.test:1"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert str(seen[0].url) == "http://api.test:1/api/v1/sessions/sess-0001/snapshot"
    assert "sess-0001" in result.output and "RUNNING" in result.output
    assert "fix the build" in result.output
    assert "plan-1" in result.output and "t1" in result.output and "mvn test" in result.output


def given_mock_api_when_status_with_json_then_raw_snapshot_printed(runner: CliRunner) -> None:
    snapshot = _snapshot_payload()
    transport, _ = _mock_api({("GET", "/api/v1/sessions/sess-0001/snapshot"): (200, snapshot)})
    result = runner.invoke(
        app,
        ["status", "sess-0001", "--json", "--api-url", "http://api.test:1"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == snapshot


def given_unknown_session_on_api_when_status_then_exit_1_with_error(runner: CliRunner) -> None:
    error = {
        "error": {
            "error_type": "SYSTEM_ERROR",
            "error_code": "NOT_FOUND",
            "severity": "low",
            "origin": "http_api",
            "retryable": False,
            "recoverable": True,
            "attempt": 1,
            "max_attempts": 1,
            "details": {"message": "unknown session: sess-0009"},
        }
    }
    transport, _ = _mock_api({("GET", "/api/v1/sessions/sess-0009/snapshot"): (404, error)})
    result = runner.invoke(
        app,
        ["status", "sess-0009", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 1
    assert "NOT_FOUND" in result.output and "sess-0009" in result.output


def given_unreachable_api_when_status_then_exit_1_with_connection_error(runner: CliRunner) -> None:
    def failing(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    result = runner.invoke(
        app,
        ["status", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=httpx.MockTransport(failing)),
    )
    assert result.exit_code == 1
    assert "connection refused" in result.output


def given_mock_api_when_sessions_then_table_with_every_session(runner: CliRunner) -> None:
    fake = FakeConversationManager()
    import asyncio

    asyncio.run(fake.start_session(goal="first goal", user_message="m"))
    asyncio.run(fake.start_session(goal="second goal", user_message="m"))
    fake.complete("sess-0001")
    items = [s.model_dump(mode="json") for s in fake.list_sessions()]
    transport, seen = _mock_api(
        {("GET", "/api/v1/sessions"): (200, {"items": items, "limit": 100, "offset": 0})}
    )
    result = runner.invoke(
        app,
        ["sessions", "--api-url", "http://api.test", "--status", "running,completed"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert seen[0].url.params["status"] == "running,completed"
    assert "sess-0001" in result.output and "sess-0002" in result.output
    assert "COMPLETED" in result.output and "RUNNING" in result.output
    assert "first goal" in result.output


def given_mock_api_when_interrupt_then_report_printed(runner: CliRunner) -> None:
    report = {
        "session_id": "sess-0001",
        "reason": "user_interrupt",
        "requested_at": "2026-01-01T00:00:00Z",
        "completed_at": "2026-01-01T00:00:00Z",
        "duration_ms": 12,
        "within_timeout": True,
        "loop_drained": True,
        "nothing_to_interrupt": False,
        "interrupted_task_ids": ["t2", "t3"],
        "plan_id": "plan-1",
        "cycle_id": "cyc-0001",
        "conversation_id": "conv-0001",
        "session_status": "READY",
    }
    transport, seen = _mock_api({("POST", "/api/v1/sessions/sess-0001/interrupt"): (200, report)})
    result = runner.invoke(
        app,
        ["interrupt", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert seen[0].method == "POST"
    assert "READY" in result.output and "t2" in result.output and "12" in result.output


def given_mock_api_when_audit_verify_then_valid_chain_reported(runner: CliRunner) -> None:
    verification = {
        "valid": True,
        "checked": 42,
        "first_broken_sequence": None,
        "reason": None,
        "verified_at": "2026-01-01T00:00:00Z",
    }
    transport, seen = _mock_api(
        {("GET", "/api/v1/sessions/sess-0001/audit/verify"): (200, verification)}
    )
    result = runner.invoke(
        app,
        ["audit", "verify", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert "valid" in result.output.lower() and "42" in result.output


def given_mock_api_when_audit_verify_finds_a_break_then_exit_1(runner: CliRunner) -> None:
    verification = {
        "valid": False,
        "checked": 4,
        "first_broken_sequence": 5,
        "reason": "HASH_MISMATCH",
        "verified_at": "2026-01-01T00:00:00Z",
    }
    transport, _ = _mock_api(
        {("GET", "/api/v1/sessions/sess-0001/audit/verify"): (200, verification)}
    )
    result = runner.invoke(
        app,
        ["audit", "verify", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 1
    assert "HASH_MISMATCH" in result.output and "5" in result.output


def given_config_api_section_when_client_command_without_api_url_then_config_url_used(
    runner: CliRunner, config_file: Path
) -> None:
    transport, seen = _mock_api(
        {("GET", "/api/v1/sessions"): (200, {"items": [], "limit": 100, "offset": 0})}
    )
    result = runner.invoke(
        app,
        ["--config", str(config_file), "sessions"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert str(seen[0].url).startswith("http://127.0.0.9:9765/api/v1/sessions")
    assert "no session" in result.output.lower()


# ================================================================================================
# run
# ================================================================================================
def given_session_completing_immediately_when_run_then_final_answer_shown_and_exit_0(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(
        sid, {"status": "success", "summary": "Build fixed: missing dependency added"}
    )
    result = runner.invoke(
        app,
        ["run", "fix the build", "--message", "mvn fails", "--config", str(config_file)],
        obj=_deps(fake),
    )
    assert result.exit_code == EXIT_OK == 0, result.output
    assert fake.calls[0] == ("start_session", "fix the build", "mvn fails", None, None)
    assert fake.shutdown_called
    assert "Build fixed: missing dependency added" in result.output
    assert "COMPLETED" in result.output
    assert "sess-0001" in result.output


def given_goal_without_message_when_run_then_goal_used_as_first_message(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(sid)
    result = runner.invoke(
        app, ["run", "just do it", "--config", str(config_file)], obj=_deps(fake)
    )
    assert result.exit_code == 0, result.output
    assert fake.calls[0] == ("start_session", "just do it", "just do it", None, None)


def given_budget_and_auto_close_options_when_run_then_forwarded_to_the_manager(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(sid)
    result = runner.invoke(
        app,
        [
            "run",
            "goal",
            "--config",
            str(config_file),
            "--budget-cycles",
            "3",
            "--budget-plans",
            "2",
            "--budget-duration-ms",
            "5000",
            "--auto-close",
        ],
        obj=_deps(fake),
    )
    assert result.exit_code == 0, result.output
    budget = SessionBudget(max_cycles=3, max_plans=2, max_total_duration_ms=5000)
    assert fake.calls[0] == ("start_session", "goal", "goal", budget, True)


def given_partial_budget_options_when_run_then_config_defaults_fill_the_rest(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(sid)
    result = runner.invoke(
        app,
        ["run", "goal", "--config", str(config_file), "--budget-cycles", "3"],
        obj=_deps(fake),
    )
    assert result.exit_code == 0, result.output
    assert fake.calls[0][3] == SessionBudget(
        max_cycles=3, max_plans=10, max_total_duration_ms=300_000
    )


def given_session_failing_when_run_then_failure_shown_and_exit_1(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.fail(sid, "budget_exceeded")
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == EXIT_FAILED == 1, result.output
    assert "FAILED" in result.output
    assert fake.shutdown_called


def given_session_progressing_over_several_waits_when_run_then_polls_until_terminal(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.wait_script.append(lambda m, sid: m.add_cycle(sid))
    fake.wait_script.append(lambda m, sid: m.add_plan(sid, "plan-1", [{"task_id": "t1"}]))
    fake.wait_script.append(lambda m, sid: m.complete(sid, {"status": "success", "summary": "ok"}))
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == 0, result.output
    waits = [c for c in fake.calls if c[0] == "wait"]
    assert len(waits) >= 3
    assert waits[0] == ("wait", "sess-0001", 1)  # cli.refresh_interval_ms of the config file
    assert "plan-1" in result.output


def given_keyboard_interrupt_during_wait_when_run_then_manager_interrupted_and_exit_2(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.add_plan(
        sid, "plan-1", [{"task_id": "t1", "status": "RUNNING"}, {"task_id": "t2"}]
    )
    fake.wait_raises.append(KeyboardInterrupt())
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == EXIT_INTERRUPTED == 2, result.output
    assert ("interrupt", "sess-0001") in fake.calls
    assert fake.require_session("sess-0001").status is SessionState.READY
    assert "interrupt" in result.output.lower()
    assert "t1" in result.output and "t2" in result.output  # the interrupted tasks of the report
    assert "READY" in result.output
    assert fake.shutdown_called


def given_second_keyboard_interrupt_during_the_interruption_when_run_then_forced_exit_2(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.wait_raises.append(KeyboardInterrupt())
    fake.interrupt_raises = KeyboardInterrupt()
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == EXIT_INTERRUPTED == 2, result.output
    assert ("interrupt", "sess-0001") in fake.calls
    assert "forced" in result.output.lower()


def given_run_with_json_when_completed_then_machine_readable_result(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(sid, {"status": "success", "summary": "done"})
    result = runner.invoke(
        app, ["run", "goal", "--json", "--config", str(config_file)], obj=_deps(fake)
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["session_id"] == "sess-0001"
    assert document["status"] == "COMPLETED"
    assert document["final_answer"] == {"status": "success", "summary": "done"}
    assert document["exit_code"] == 0
    assert document["interruption"] is None
    assert document["snapshot"] == fake.snapshot("sess-0001").model_dump(mode="json")


def given_manager_raising_app_error_at_start_when_run_then_normalized_error_and_exit_1(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    from agentic_local_app.domain.errors import PersistenceError

    fake.start_raises = PersistenceError("DISK_FULL", path="/data")
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == 1
    assert "PERSISTENCE_ERROR" in result.output and "DISK_FULL" in result.output
    assert fake.shutdown_called


def given_invalid_config_when_run_then_exit_1_before_building_the_application(
    runner: CliRunner, tmp_path: Path, fake: FakeConversationManager
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text("[budget]\ndefault_max_cycles = -1\n")
    deps = _deps(fake)
    result = runner.invoke(app, ["run", "goal", "--config", str(path)], obj=deps)
    assert result.exit_code == 1
    assert "CONFIG_INVALID" in result.output
    assert "config" not in deps.captured  # type: ignore[attr-defined]
    assert fake.calls == []


# ================================================================================================
# transport list · transport show (ADR-020)
# ================================================================================================
KEY_ENV = "AGENTIC_PHASE9_CLI_MODEL_KEY"
GENERIC_CLASS = "agentic_local_app.transport.providers.generic_http:GenericHttpProvider"
TEMPLATED_CLASS = "agentic_local_app.transport.providers.templated_http:TemplatedHttpProvider"
FAKE_CLASS = "agentic_local_app.transport.fake:FakeTransportProvider"


def _templated_config(tmp_path: Path, provider: str = "templated_http") -> Path:
    path = tmp_path / "templated.toml"
    path.write_text(
        "\n".join(
            [
                "[transport]",
                f'provider = "{provider}"',
                f'token_env = "{TOKEN_ENV}"',
                "[transport.options]",
                f'headers = {{ "X-Api-Key" = "${{env:{KEY_ENV}}}", "Accept" = "application/json" }}',
                "[transport.options.init]",
                'url = "https://api.example.com/v1/threads"',
                'body = { instructions = "{instructions}", api_secret = "literal" }',
                'conversation_id_path = "data.id"',
                "[transport.options.post]",
                'url = "https://api.example.com/v1/threads/{conversation_id}/messages"',
                'body = { content = "{message_json}" }',
                "[transport.options.get]",
                'url = "https://api.example.com/v1/threads/{conversation_id}/messages?since={after}"',
                'messages_path = "items"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def given_cli_when_transport_list_then_builtin_providers_with_class_and_origin(
    runner: CliRunner,
) -> None:
    result = runner.invoke(app, ["transport", "list"])
    assert result.exit_code == 0, result.output
    rows = {line.split()[0]: line for line in result.output.splitlines() if line.strip()}
    assert "generic_http" in rows and GENERIC_CLASS in rows["generic_http"]
    assert "templated_http" in rows and TEMPLATED_CLASS in rows["templated_http"]
    assert "fake" in rows and FAKE_CLASS in rows["fake"]
    assert all("builtin" in rows[name] for name in ("generic_http", "templated_http", "fake"))


def given_cli_when_transport_list_json_then_machine_readable_sorted_rows(
    runner: CliRunner,
) -> None:
    result = runner.invoke(app, ["transport", "list", "--json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)
    assert [row["name"] for row in rows] == sorted(row["name"] for row in rows)
    by_name = {row["name"]: row for row in rows}
    assert by_name["generic_http"] == {
        "name": "generic_http",
        "class": GENERIC_CLASS,
        "origin": "builtin",
    }
    assert by_name["templated_http"]["class"] == TEMPLATED_CLASS
    assert by_name["fake"]["class"] == FAKE_CLASS


def given_default_config_when_transport_show_then_generic_http_without_options(
    runner: CliRunner, config_file: Path
) -> None:
    result = runner.invoke(app, ["transport", "show", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    assert "provider: generic_http" in result.output
    assert f"class: {GENERIC_CLASS}" in result.output
    assert "origin: builtin" in result.output
    assert "options: {}" in result.output


def given_templated_config_with_secrets_when_transport_show_then_options_masked(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(KEY_ENV, "k-secret")
    monkeypatch.setenv(TOKEN_ENV, "t-secret")
    result = runner.invoke(app, ["transport", "show", "--config", str(_templated_config(tmp_path))])
    assert result.exit_code == 0, result.output
    assert "k-secret" not in result.output and "t-secret" not in result.output
    assert "provider: templated_http" in result.output
    assert f"class: {TEMPLATED_CLASS}" in result.output
    assert "options_model: TemplatedOptions" in result.output
    assert '"X-Api-Key": "***"' in result.output
    assert '"api_secret": "***"' in result.output
    assert '"instructions": "{instructions}"' in result.output


def given_templated_config_when_transport_show_json_then_document_with_masked_options(
    runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(KEY_ENV, "k-secret")
    monkeypatch.setenv(TOKEN_ENV, "t-secret")
    result = runner.invoke(
        app, ["transport", "show", "--json", "--config", str(_templated_config(tmp_path))]
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["provider"] == "templated_http"
    assert document["class"] == TEMPLATED_CLASS
    assert document["origin"] == "builtin"
    assert document["options_model"] == "TemplatedOptions"
    assert document["options"]["headers"] == {"X-Api-Key": "***", "Accept": "application/json"}
    assert document["options"]["init"]["body"] == {
        "instructions": "{instructions}",
        "api_secret": "***",
    }
    assert document["transport"]["token"] == "***"
    assert document["transport"]["options"] == document["options"]
    assert "k-secret" not in result.stdout and "t-secret" not in result.stdout


def given_unknown_provider_when_transport_show_then_exit_1_with_available_names(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = _templated_config(tmp_path, provider="carrier_pigeon")
    result = runner.invoke(app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "TRANSPORT_PROVIDER_UNKNOWN" in result.output
    assert "carrier_pigeon" in result.output and "generic_http" in result.output


def given_options_for_a_provider_without_options_when_transport_show_then_exit_1_options_invalid(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = _templated_config(tmp_path, provider="generic_http")
    result = runner.invoke(app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "TRANSPORT_OPTIONS_INVALID" in result.output
    assert "GenericHttpProvider" in result.output


def given_invalid_templated_options_when_transport_show_then_exit_1_with_location(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text(
        '[transport]\nprovider = "templated_http"\n[transport.options.init]\nurl = "http://x"\n',
        encoding="utf-8",
    )
    result = runner.invoke(app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "TRANSPORT_OPTIONS_INVALID" in result.output
    assert "init.conversation_id_path" in result.output
    assert "post" in result.output and "get" in result.output


def given_invalid_config_file_when_transport_show_then_exit_1_config_invalid(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "bad.toml"
    path.write_text('[transport]\nclose_method = "PUT"\n', encoding="utf-8")
    result = runner.invoke(app, ["transport", "show", "--config", str(path)])
    assert result.exit_code == 1, result.output
    assert "CONFIG_INVALID" in result.output and "transport.close_method" in result.output


def given_cli_when_help_then_transport_command_listed(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0 and "transport" in result.output
    sub = runner.invoke(app, ["transport", "--help"])
    assert sub.exit_code == 0 and "list" in sub.output and "show" in sub.output


def given_unknown_transport_provider_when_run_with_real_factory_then_exit_1_before_any_session(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        "\n".join(
            [
                "[app]",
                f'data_dir = "{(tmp_path / "data").as_posix()}"',
                "[transport]",
                'provider = "carrier_pigeon"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["run", "goal", "--config", str(path)], obj=CliDependencies())
    assert result.exit_code == 1, result.output
    assert "TRANSPORT_PROVIDER_UNKNOWN" in result.output and "carrier_pigeon" in result.output
    assert not (tmp_path / "data").exists()  # the store was never opened
