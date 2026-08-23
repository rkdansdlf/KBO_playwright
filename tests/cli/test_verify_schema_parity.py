"""Unit tests for src.cli.verify_schema_parity."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.verify_schema_parity import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--strict", "--json", "--db-url", "sqlite:///:memory:"])
    assert args.strict is True
    assert args.json is True
    assert args.db_url == "sqlite:///:memory:"


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    # Running against current sqlite db
    main(["--json"])
    captured = capsys.readouterr()

    data = json.loads(captured.out)
    assert "total_tables" in data
    assert data["total_tables"] >= 50
