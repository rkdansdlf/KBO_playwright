"""Unit tests for src.cli.validate_config."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.validate_config import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--env", "ci", "--strict", "--json"])
    assert args.env == "ci"
    assert args.strict is True
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--env", "local", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert "is_valid" in data
