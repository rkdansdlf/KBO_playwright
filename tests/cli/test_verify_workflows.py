"""Unit tests for src.cli.verify_workflows."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.verify_workflows import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--strict", "--json"])
    assert args.strict is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert "total_workflows" in data
    assert data["total_workflows"] >= 10
    assert data["failed_workflows"] == 0
