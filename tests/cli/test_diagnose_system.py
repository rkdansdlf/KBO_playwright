"""Unit tests for src.cli.diagnose_system."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.diagnose_system import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--subsystem", "scheduler", "--fix", "--json"])
    assert args.subsystem == "scheduler"
    assert args.fix is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--json"])
    captured = capsys.readouterr()

    assert exit_code in (0, 1)
    data = json.loads(captured.out)
    assert "overall_status" in data
    assert "total_checks" in data
    assert len(data["checks"]) > 0
