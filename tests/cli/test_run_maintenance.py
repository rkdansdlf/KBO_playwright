"""Unit tests for src.cli.run_maintenance."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.run_maintenance import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--task", "pa_audit", "--apply", "--json"])
    assert args.task == "pa_audit"
    assert args.apply is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--task", "checkpoint", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert "total_tasks" in data
    assert data["total_tasks"] == 1
