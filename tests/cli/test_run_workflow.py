"""Unit tests for src.cli.run_workflow."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.run_workflow import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--workflow", "daily_sync", "--date", "20260401", "--dry-run", "--json"])
    assert args.workflow == "daily_sync"
    assert args.date == "20260401"
    assert args.dry_run is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--workflow", "daily_sync", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_stages"] == 6
    assert data["overall_status"] == "SUCCESS"
