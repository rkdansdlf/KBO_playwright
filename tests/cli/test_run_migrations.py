"""Unit tests for src.cli.run_migrations."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.run_migrations import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--dialect", "oracle", "--dry-run", "--json"])
    assert args.dialect == "oracle"
    assert args.dry_run is True
    assert args.json is True


def test_main_cli_execution_status_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--dialect", "oracle", "--status", "--json", "--db-url", "sqlite:///:memory:"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["dialect"] == "oracle"
    assert "total_available" in data
    assert data["total_available"] >= 50
