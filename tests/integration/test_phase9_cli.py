"""Phase 9b — the ``agentic-app`` CLI (ADR-002, ADR-018) through ``typer.testing.CliRunner``.

No process is spawned and no port is opened: ``serve`` and ``mock-server`` receive an injected
runner, ``run`` receives an injected application factory returning the ``FakeConversationManager``
double, and the API-client commands (``open``, ``status``, ``sessions``, ``interrupt``, ``reply``,
``audit verify``) talk to an ``httpx.MockTransport``. Ctrl-C is simulated by a ``KeyboardInterrupt``
injected in the façade double (ADR-002: Ctrl-C = interruption, not process exit).
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
from agentic_local_app.identity import UserIdentity
from agentic_local_app.interfaces.cli import (
    EXIT_FAILED,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_PAUSED,
    CliDependencies,
    app,
    main,
)
from agentic_local_app.testing.mock_model_server import (
    default_analysis_scenario,
    default_java_debug_scenario,
)
from integration.fake_manager import FakeConversationManager

pytestmark = pytest.mark.phase9

TOKEN_ENV = "AGENTIC_TRANSPORT_TOKEN_PHASE9_CLI"


# ================================================================================================
# harness
# ================================================================================================
class _Application:
    """What ``build_application`` returns: the façade and the machine identity (ADR-024 §6)."""

    def __init__(self, manager: FakeConversationManager) -> None:
        self.manager = manager
        self.identity = UserIdentity(user_id="tester", source="config", host="workstation")


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
    for command in (
        "run",
        "serve",
        "open",
        "status",
        "sessions",
        "interrupt",
        "reply",
        "config",
        "mock-server",
    ):
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
    assert served["app"].state.manager is fake
    assert deps.captured["config"].app.name == "phase9-cli"  # type: ignore[attr-defined]
    assert "http://127.0.0.9:9765/api/v1" in result.output


def given_wired_identity_when_serve_then_the_api_answers_whoami_with_it(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    """ADR-024 §6: the identity is resolved once, at wiring time; the API serves that one."""
    import asyncio

    served: dict[str, Any] = {}

    def server_runner(asgi_app: Any, *, host: str, port: int) -> None:
        served["app"] = asgi_app

    result = runner.invoke(
        app,
        ["serve", "--config", str(config_file)],
        obj=_deps(fake, server_runner=server_runner),
    )
    assert result.exit_code == 0, result.output

    async def ask() -> Any:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=served["app"]), base_url="http://testserver"
        ) as client:
            return (await client.get("/api/v1/whoami")).json()

    assert asyncio.run(ask()) == {
        "user_id": "tester",
        "source": "config",
        "host": "workstation",
    }


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
            mock_server_runner=lambda a, *, host, port, **k: calls.append(
                {"h": host, "p": port, "app": a}
            )
        ),
    )
    assert result.exit_code == 0, result.output
    assert [(c["h"], c["p"]) for c in calls] == [("127.0.0.1", 9000)]
    assert calls[0]["app"].state.engine.scenario == default_java_debug_scenario()
    assert "built-in java" in result.output


def given_scenario_name_analysis_when_mock_server_then_analysis_scenario_served(
    runner: CliRunner,
) -> None:
    calls: list[Any] = []
    result = runner.invoke(
        app,
        ["mock-server", "--scenario-name", "analysis", "--port", "9001"],
        obj=CliDependencies(mock_server_runner=lambda a, *, host, port, **k: calls.append(a)),
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    scenario = calls[0].state.engine.scenario
    assert scenario == default_analysis_scenario()
    assert [m["type"] for s in scenario.steps for m in s.respond] == ["user_response"]
    assert "built-in analysis" in result.output and "9001" in result.output


def given_unknown_scenario_name_when_mock_server_then_exit_1_without_serving(
    runner: CliRunner,
) -> None:
    calls: list[Any] = []
    result = runner.invoke(
        app,
        ["mock-server", "--scenario-name", "nope"],
        obj=CliDependencies(mock_server_runner=lambda a, **k: calls.append(a)),
    )
    assert result.exit_code == 1
    assert calls == []
    assert "nope" in result.output and "analysis" in result.output and "java" in result.output


def given_scenario_path_and_name_when_mock_server_then_exit_1_mutually_exclusive(
    runner: CliRunner, tmp_path: Path
) -> None:
    scenario = tmp_path / "scenario.json"
    scenario.write_text(json.dumps({"steps": []}))
    calls: list[Any] = []
    result = runner.invoke(
        app,
        ["mock-server", "--scenario", str(scenario), "--scenario-name", "analysis"],
        obj=CliDependencies(mock_server_runner=lambda a, **k: calls.append(a)),
    )
    assert result.exit_code == 1
    assert calls == [] and "mutually exclusive" in result.output


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


def _session_payload(sid: str = "sess-0001", status: str = "RUNNING") -> dict[str, Any]:
    fake = FakeConversationManager()
    import asyncio

    asyncio.run(fake.start_session(goal="explain the error", user_message="m"))
    body = fake.require_session(sid).model_dump(mode="json")
    body["status"] = status
    return body


def _empty_session_payload(sid: str = "sess-0001") -> dict[str, Any]:
    """ADR-028: what ``POST /sessions`` answers with no opening message — READY, no conversation."""
    fake = FakeConversationManager()
    import asyncio

    asyncio.run(fake.start_session(user_id="alice"))
    return fake.require_session(sid).model_dump(mode="json")


def given_mock_api_when_open_then_empty_session_created_and_first_message_hinted(
    runner: CliRunner,
) -> None:
    payload = _empty_session_payload()
    transport, seen = _mock_api({("POST", "/api/v1/sessions"): (201, payload)})
    result = runner.invoke(
        app,
        ["open", "--user-id", "alice", "--api-url", "http://api.test:1"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert str(seen[0].url) == "http://api.test:1/api/v1/sessions"
    assert json.loads(seen[0].content) == {"user_id": "alice"}
    assert "sess-0001" in result.output and "READY" in result.output
    assert "alice" in result.output
    assert 'agentic-app reply sess-0001 "..."' in result.output


def given_open_with_the_sign_in_fields_when_invoked_then_all_of_them_travel(
    runner: CliRunner, tmp_path: Path
) -> None:
    transport, seen = _mock_api({("POST", "/api/v1/sessions"): (201, _empty_session_payload())})
    result = runner.invoke(
        app,
        [
            "open",
            "--working-space",
            str(tmp_path),
            "--skill",
            "deploy",
            "--skill",
            "review",
            "--effort",
            "high",
            "--json",
            "--api-url",
            "http://api.test",
        ],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert json.loads(seen[0].content) == {
        "working_space": str(tmp_path),
        "skills": ["deploy", "review"],
        "effort": "high",
    }
    assert json.loads(result.stdout)["status"] == "READY"


def given_open_without_any_option_when_invoked_then_an_empty_body_is_posted(
    runner: CliRunner,
) -> None:
    """Nothing is invented: no goal, no message, not even a user id (the API knows who that is)."""
    transport, seen = _mock_api({("POST", "/api/v1/sessions"): (201, _empty_session_payload())})
    result = runner.invoke(
        app, ["open", "--api-url", "http://api.test"], obj=CliDependencies(transport=transport)
    )
    assert result.exit_code == 0, result.output
    assert json.loads(seen[0].content) == {}


def given_an_effort_refused_by_the_api_when_open_then_exit_1_with_the_code(
    runner: CliRunner,
) -> None:
    error = {
        "error": {
            "error_type": "SYSTEM_ERROR",
            "error_code": "EFFORT_INVALID",
            "severity": "low",
            "origin": "http_api",
            "retryable": False,
            "recoverable": True,
            "attempt": 1,
            "max_attempts": 1,
            "details": {"message": "unknown effort level", "effort": "extreme"},
        }
    }
    transport, _ = _mock_api({("POST", "/api/v1/sessions"): (400, error)})
    result = runner.invoke(
        app,
        ["open", "--effort", "extreme", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 1
    assert "EFFORT_INVALID" in result.output


def given_mock_api_when_reply_then_follow_up_posted_and_session_printed(runner: CliRunner) -> None:
    transport, seen = _mock_api(
        {("POST", "/api/v1/sessions/sess-0001/messages"): (202, _session_payload())}
    )
    result = runner.invoke(
        app,
        ["reply", "sess-0001", "Only service-api fails.", "--api-url", "http://api.test:1"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert seen[0].method == "POST"
    assert str(seen[0].url) == "http://api.test:1/api/v1/sessions/sess-0001/messages"
    assert json.loads(seen[0].content) == {"user_message": "Only service-api fails."}
    assert "sess-0001" in result.output and "RUNNING" in result.output
    assert "conv-0001" in result.output
    assert "agentic-app status sess-0001" in result.output


def given_mock_api_when_reply_with_json_then_session_record_printed(runner: CliRunner) -> None:
    payload = _session_payload()
    transport, _ = _mock_api({("POST", "/api/v1/sessions/sess-0001/messages"): (202, payload)})
    result = runner.invoke(
        app,
        ["reply", "sess-0001", "all of it", "--json", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == payload


def given_non_reusable_session_on_api_when_reply_then_exit_1_with_conflict(
    runner: CliRunner,
) -> None:
    error = {
        "error": {
            "error_type": "SYSTEM_ERROR",
            "error_code": "CONFLICT",
            "severity": "low",
            "origin": "http_api",
            "retryable": False,
            "recoverable": True,
            "attempt": 1,
            "max_attempts": 1,
            "details": {"message": "session sess-0001 was closed after its final answer"},
        }
    }
    transport, _ = _mock_api({("POST", "/api/v1/sessions/sess-0001/messages"): (409, error)})
    result = runner.invoke(
        app,
        ["reply", "sess-0001", "again", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == 1
    assert "CONFLICT" in result.output and "closed after its final answer" in result.output


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


def given_session_answered_by_user_response_when_run_then_body_shown_and_exit_0(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.respond(
        sid, "## Analysis\n\nThe target release is wrong.", format="markdown"
    )
    result = runner.invoke(
        app,
        [
            "run",
            "explain the error",
            "--message",
            "what does it mean?",
            "--config",
            str(config_file),
        ],
        obj=_deps(fake),
    )
    assert result.exit_code == EXIT_OK, result.output
    assert "The target release is wrong." in result.output
    assert "Model response" in result.output
    assert "markdown" in result.output and "completed" in result.output
    assert "COMPLETED" in result.output and "sess-0001" in result.output
    assert "waiting for your answer" not in result.output
    assert "without a final answer" not in result.output
    assert fake.shutdown_called


def given_session_answered_by_question_when_run_then_reply_hint_shown(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.respond(sid, "Which module fails?", expects_reply=True)
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == EXIT_OK, result.output
    assert "Which module fails?" in result.output
    assert "waiting for your answer" in result.output
    assert 'agentic-app reply sess-0001 "..."' in result.output


def given_run_with_json_when_answered_by_user_response_then_last_reply_in_document(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.respond(sid, "Which module fails?", expects_reply=True)
    result = runner.invoke(
        app, ["run", "goal", "--json", "--config", str(config_file)], obj=_deps(fake)
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["status"] == "COMPLETED" and document["final_answer"] is None
    assert document["last_reply"] == fake.last_reply("sess-0001")
    assert document["last_reply"]["type"] == "user_response"
    assert document["last_reply"]["content"]["expects_reply"] is True
    assert document["last_reply"]["content"]["body"] == "Which module fails?"


def given_run_with_json_when_completed_by_final_answer_then_last_reply_reflects_it(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.complete(sid, {"status": "success", "summary": "done"})
    result = runner.invoke(
        app, ["run", "goal", "--json", "--config", str(config_file)], obj=_deps(fake)
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["final_answer"] == {"status": "success", "summary": "done"}
    # the double's ``complete`` writes no message record: nothing to read back from the table
    assert document["last_reply"] is None


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


# ================================================================================================
# shell show · shell rules (ADR-030)
# ================================================================================================
def given_pinned_shell_when_shell_show_then_the_detected_environment_is_printed(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "shell.toml"
    path.write_text('[execution]\nshell = "pwsh"\ncwd = "."\n', encoding="utf-8")
    result = runner.invoke(app, ["shell", "show", "--config", str(path), "--json"])
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["shell"] == "pwsh" and document["shell_name"] == "pwsh"
    assert document["dialect"] == "powershell" and document["source"] == "configured"
    assert document["translate_commands"] is True and document["translation_enabled"] is True
    assert document["translates_from"] == "posix"
    assert Path(document["cwd"]).is_absolute()


def given_translation_disabled_when_shell_show_then_reported_as_off(
    runner: CliRunner, tmp_path: Path
) -> None:
    path = tmp_path / "shell.toml"
    path.write_text('[execution]\nshell = "cmd"\ntranslate_commands = false\n', encoding="utf-8")
    result = runner.invoke(app, ["shell", "show", "--config", str(path)])
    assert result.exit_code == 0, result.output
    assert "dialect: cmd" in result.output
    assert "translate_commands: False" in result.output
    assert "translation_enabled: False" in result.output
    assert "translates_from: -" in result.output


def given_cli_when_shell_rules_then_the_whole_dictionary_and_its_refusals_are_listed(
    runner: CliRunner,
) -> None:
    result = runner.invoke(app, ["shell", "rules"])
    assert result.exit_code == 0, result.output
    assert "Get-ChildItem [-Force] [PATH]" in result.output
    assert "environment-variable" in result.output
    assert "Recognised and deliberately NOT translated:" in result.output
    assert "only commands that read" in result.output


def given_cli_when_shell_rules_json_then_machine_readable_rules_and_refusals(
    runner: CliRunner,
) -> None:
    result = runner.invoke(app, ["shell", "rules", "--json"])
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert {row["rule"] for row in document["rules"]} >= {"list-directory", "print-file"}
    assert all(row["from"] != row["to"] for row in document["rules"])
    assert {entry["program"] for entry in document["refused"]} >= {"rm", "grep", "remove-item"}


def given_cli_when_help_then_shell_command_listed(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0 and "shell" in result.output
    sub = runner.invoke(app, ["shell", "--help"])
    assert sub.exit_code == 0 and "show" in sub.output and "rules" in sub.output


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


# ================================================================================================
# credentials · resume (ADR-025, clients of the API like status / interrupt)
# ================================================================================================
def _no_content(_: httpx.Request) -> httpx.Response:
    return httpx.Response(204)


def given_token_on_stdin_when_credentials_then_posted_without_ever_being_printed(
    runner: CliRunner, config_file: Path
) -> None:
    transport, seen = _mock_api({("POST", "/api/v1/credentials"): _no_content})
    result = runner.invoke(
        app,
        ["--config", str(config_file), "credentials"],
        obj=CliDependencies(transport=transport),
        input="s3cr3t-token\n",
    )
    assert result.exit_code == EXIT_OK, result.output
    assert str(seen[0].url) == "http://127.0.0.9:9765/api/v1/credentials"
    assert json.loads(seen[0].content)["token"].strip() == "s3cr3t-token"
    assert "s3cr3t-token" not in result.output  # the value is never echoed back at the user
    assert TOKEN_ENV in result.output  # only the name of the variable, which is public
    assert "agentic-app resume" in result.output


def given_from_env_when_credentials_then_the_variable_is_read_not_an_argument(
    runner: CliRunner, config_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SOME_VENDOR_KEY", "k-from-env")
    transport, seen = _mock_api({("POST", "/api/v1/credentials"): _no_content})
    result = runner.invoke(
        app,
        ["--config", str(config_file), "credentials", "--from-env", "SOME_VENDOR_KEY"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == EXIT_OK, result.output
    assert json.loads(seen[0].content) == {"token": "k-from-env"}
    assert "k-from-env" not in result.output


def given_unset_variable_or_empty_stdin_when_credentials_then_exit_1_without_a_call(
    runner: CliRunner, config_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ABSENT_VENDOR_KEY", raising=False)
    transport, seen = _mock_api({("POST", "/api/v1/credentials"): _no_content})
    missing = runner.invoke(
        app,
        ["--config", str(config_file), "credentials", "--from-env", "ABSENT_VENDOR_KEY"],
        obj=CliDependencies(transport=transport),
    )
    assert missing.exit_code == EXIT_FAILED
    assert "ABSENT_VENDOR_KEY" in missing.output
    blank = runner.invoke(
        app,
        ["--config", str(config_file), "credentials"],
        obj=CliDependencies(transport=transport),
        input="   \n",
    )
    assert blank.exit_code == EXIT_FAILED
    assert "empty" in blank.output.lower()
    assert seen == []


def given_api_refusing_the_token_when_credentials_then_exit_1_with_the_error_code(
    runner: CliRunner, config_file: Path
) -> None:
    error = {
        "error": {
            "error_code": "CREDENTIALS_NOT_CONFIGURED",
            "details": {"message": "the active model profile takes no token"},
        }
    }
    transport, _ = _mock_api({("POST", "/api/v1/credentials"): (409, error)})
    result = runner.invoke(
        app,
        ["--config", str(config_file), "credentials"],
        obj=CliDependencies(transport=transport),
        input="s3cr3t-token\n",
    )
    assert result.exit_code == EXIT_FAILED
    assert "CREDENTIALS_NOT_CONFIGURED" in result.output
    assert "s3cr3t-token" not in result.output


def given_mock_api_when_resume_then_session_posted_and_status_printed(runner: CliRunner) -> None:
    payload = _session_payload()
    transport, seen = _mock_api({("POST", "/api/v1/sessions/sess-0001/resume"): (200, payload)})
    result = runner.invoke(
        app,
        ["resume", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == EXIT_OK, result.output
    assert seen[0].method == "POST"
    assert str(seen[0].url) == "http://api.test/api/v1/sessions/sess-0001/resume"
    assert "sess-0001" in result.output and "RUNNING" in result.output
    assert "agentic-app status sess-0001" in result.output


def given_session_that_is_not_paused_when_resume_then_exit_1_with_the_conflict(
    runner: CliRunner,
) -> None:
    error = {
        "error": {
            "error_code": "SESSION_NOT_RESUMABLE",
            "details": {"message": "session sess-0001 is not resumable (COMPLETED)"},
        }
    }
    transport, _ = _mock_api({("POST", "/api/v1/sessions/sess-0001/resume"): (409, error)})
    result = runner.invoke(
        app,
        ["resume", "sess-0001", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == EXIT_FAILED
    assert "SESSION_NOT_RESUMABLE" in result.output
    assert "not resumable" in result.output


def given_mock_api_when_resume_with_json_then_session_record_printed(runner: CliRunner) -> None:
    payload = _session_payload()
    transport, _ = _mock_api({("POST", "/api/v1/sessions/sess-0001/resume"): (200, payload)})
    result = runner.invoke(
        app,
        ["resume", "sess-0001", "--json", "--api-url", "http://api.test"],
        obj=CliDependencies(transport=transport),
    )
    assert result.exit_code == EXIT_OK, result.output
    assert json.loads(result.stdout) == payload


# ================================================================================================
# run on a paused session (ADR-025): it stops, it says why, and it does not spin
# ================================================================================================
def given_session_paused_during_a_wait_when_run_then_bounded_polling_and_exit_3(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    """The regression test of the spin: a PAUSED session ends no state ``run`` used to know, so
    the wait loop turned on itself for ever (measured: ~570 000 ``wait`` calls in two seconds)."""
    fake.wait_script.append(lambda m, sid: m.pause(sid, operation="POST"))
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    waits = [call for call in fake.calls if call[0] == "wait"]
    assert len(waits) <= 3, f"{len(waits)} wait calls: the run is spinning on a paused session"
    assert result.exit_code == EXIT_PAUSED == 3, result.output
    assert fake.require_session("sess-0001").status is SessionState.PAUSED
    assert fake.shutdown_called


def given_session_paused_when_run_then_reason_and_the_two_commands_are_printed(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.pause(sid, operation="GET")
    result = runner.invoke(app, ["run", "goal", "--config", str(config_file)], obj=_deps(fake))
    assert result.exit_code == EXIT_PAUSED, result.output
    assert len([call for call in fake.calls if call[0] == "wait"]) == 0
    output = " ".join(result.output.split())  # rich wraps the panel to the terminal width
    assert "paused" in output.lower()
    assert "credentials_required" in output
    assert "HTTP_401" in output and "AUTHN_ERROR" in output and "GET" in output
    assert "agentic-app credentials" in output
    assert "agentic-app resume sess-0001" in output


def given_run_with_json_when_session_paused_then_reason_in_the_document(
    runner: CliRunner, config_file: Path, fake: FakeConversationManager
) -> None:
    fake.on_start = lambda m, sid: m.pause(sid)
    result = runner.invoke(
        app, ["run", "goal", "--json", "--config", str(config_file)], obj=_deps(fake)
    )
    assert result.exit_code == EXIT_PAUSED, result.output
    document = json.loads(result.stdout)
    assert document["status"] == "PAUSED"
    assert document["exit_code"] == EXIT_PAUSED
    assert document["final_answer"] is None
    assert document["paused_reason"] == fake.paused_reason("sess-0001")
    assert document["paused_reason"]["reason"] == "credentials_required"
    assert document["paused_reason"]["operation"] == "POST"


def given_cli_when_help_then_credentials_and_resume_are_listed(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, result.output
    for command in ("credentials", "resume"):
        assert command in result.output
    run_help = runner.invoke(app, ["run", "--help"])
    assert "3 paused" in " ".join(run_help.output.split())
    credentials_help = " ".join(runner.invoke(app, ["credentials", "--help"]).output.split())
    assert "--from-env" in credentials_help
    assert "standard input" in credentials_help
