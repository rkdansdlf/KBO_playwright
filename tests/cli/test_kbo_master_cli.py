"""Unit tests for src.cli.kbo master CLI router."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.kbo import build_master_parser, main

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_build_master_parser_subcommands() -> None:
    parser = build_master_parser()
    # Test workflow subcommand parsing
    args = parser.parse_args(["workflow", "--workflow", "daily_sync", "--dry-run"])
    assert args.command == "workflow"
    assert args.workflow == "daily_sync"
    assert args.dry_run is True

    # Test config subcommand parsing
    args = parser.parse_args(["config", "--env", "local", "--strict"])
    assert args.command == "config"
    assert args.env == "local"
    assert args.strict is True

    # Test rag subcommand parsing
    args = parser.parse_args(["rag", "query", "KIA 김도영", "--top-k", "3", "--json"])
    assert args.command == "rag"
    assert args.rag_command == "query"
    assert args.query == "KIA 김도영"
    assert args.top_k == 3
    assert args.json is True

    args = parser.parse_args(["rag", "evaluate", "--strict", "--limit", "10"])
    assert args.command == "rag"
    assert args.rag_command == "evaluate"
    assert args.strict is True
    assert args.limit == 10

    # Test simulate subcommand parsing
    args = parser.parse_args(["simulate", "--home-team", "KIA", "--innings", "5", "--speed", "10"])
    assert args.command == "simulate"
    assert args.home_team == "KIA"
    assert args.innings == 5
    assert args.speed == 10.0

    # Test drift subcommand parsing
    args = parser.parse_args(["drift", "--dialect", "oracle", "--apply", "--strict"])
    assert args.command in {"drift", "detect-drift"}
    assert args.dialect == "oracle"
    assert args.apply is True
    assert args.strict is True

    # Test serve subcommand parsing
    args = parser.parse_args(["serve", "--host", "0.0.0.0", "--port", "9000", "--reload"])
    assert args.command == "serve"
    assert args.host == "0.0.0.0"
    assert args.port == 9000
    assert args.reload is True


def test_master_cli_no_args_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main([])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Unified KBO Playwright Data & Analytics Platform Master CLI." in captured.out


def test_master_cli_route_workflow_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["workflow", "--workflow", "daily_sync", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_stages"] == 6
    assert data["overall_status"] == "SUCCESS"


def test_master_cli_route_detect_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["detect", "--sensitivity", "medium", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert "total_metrics_evaluated" in data
    assert data["overall_status"] == "HEALTHY"


def test_master_cli_route_config_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["config", "--env", "local", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert "is_valid" in data


def test_master_cli_route_sync_dry_run_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import io
    import sqlite3
    import sys

    db_file = tmp_path / "sync_test.db"
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE game (game_id TEXT PRIMARY KEY, updated_at TEXT)")
    conn.commit()
    conn.close()

    buf = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buf)

    exit_code = main(["sync", "--source-url", f"sqlite:///{db_file}", "--dry-run", "--json"])

    assert exit_code == 0
    raw_output = buf.getvalue()
    json_start = raw_output.find("{")
    assert json_start != -1
    data = json.loads(raw_output[json_start:])
    assert "started_at" in data
    assert "completed_at" in data
    assert "tables_synced" in data
    assert "rows_synced" in data
