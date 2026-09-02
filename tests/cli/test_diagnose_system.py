"""Unit and integration tests for src.cli.diagnose_system and SystemDiagnosticsEngine."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock
import pytest

from src.api.live_stream_dto import CircuitState as LiveCircuitState
from src.api.routers.live_stream import live_relay_breaker
from src.cli.diagnose_system import build_arg_parser, main
from src.diagnostics.dto import DiagnosticSeverity, SubsystemType
from src.diagnostics.engine import SystemDiagnosticsEngine


def test_build_arg_parser() -> None:
    """Test argument parser with various subsystem options and flags."""
    parser = build_arg_parser()
    args = parser.parse_args(["--subsystem", "scheduler", "--fix", "--json"])
    assert args.subsystem == "scheduler"
    assert args.fix is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Test main CLI execution with JSON output format."""
    exit_code = main(["--json"])
    captured = capsys.readouterr()

    assert exit_code in (0, 1)
    json_str = captured.out[captured.out.find("{") :]
    data = json.loads(json_str)
    assert "overall_status" in data
    assert "total_checks" in data
    assert len(data["checks"]) > 0


def test_diagnose_subsystem_filtering(capsys: pytest.CaptureFixture[str]) -> None:
    """Test that specifying --subsystem scheduler only executes scheduler checks."""
    exit_code = main(["--subsystem", "scheduler", "--json"])
    captured = capsys.readouterr()

    assert exit_code in (0, 1)
    json_str = captured.out[captured.out.find("{") :]
    data = json.loads(json_str)
    for check in data["checks"]:
        assert check["subsystem"] == SubsystemType.SCHEDULER.value


def test_diagnose_database_connection_failure_handled_gracefully() -> None:
    """Test that unexpected connection failures (e.g. DNS socket.gaierror) do not crash diagnose."""
    failing_engine = MagicMock()
    failing_engine.connect.side_effect = OSError("Connection refused or DNS resolution failed")

    engine = SystemDiagnosticsEngine(engine=failing_engine)
    checks = engine.diagnose_database()

    assert len(checks) == 1
    assert checks[0].name == "db_connectivity"
    assert checks[0].severity == DiagnosticSeverity.CRITICAL
    assert checks[0].status == "FAIL"
    assert "Connection refused" in checks[0].message
    assert checks[0].remediation_hint is not None


def test_auto_heal_live_relay_breaker() -> None:
    """Test that auto_heal resets tripped live relay stream circuit breakers."""
    asyncio.run(live_relay_breaker.trip("Test trip for auto_heal"))
    assert live_relay_breaker.state == LiveCircuitState.OPEN

    engine = SystemDiagnosticsEngine()
    healed = engine.auto_heal("crawler")

    assert any("live relay stream circuit breaker" in msg for msg in healed)
    assert live_relay_breaker.state == LiveCircuitState.CLOSED
