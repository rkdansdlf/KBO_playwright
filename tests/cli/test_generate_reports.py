"""Unit tests for src.cli.generate_reports."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.generate_reports import build_arg_parser, main

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--category", "quality", "--format", "json"])
    assert args.category == "quality"
    assert args.format == "json"


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--category", "executive", "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code in (0, 1)
    data = json.loads(captured.out)
    assert data["category"] == "executive_dashboard"
    assert "sections" in data


def test_main_cli_execution_file_output(tmp_path: Path) -> None:
    out_file = tmp_path / "report.md"
    exit_code = main(["--category", "gap", "--format", "markdown", "--output", str(out_file)])

    assert exit_code in (0, 1)
    assert out_file.exists()
    content = out_file.read_text(encoding="utf-8")
    assert "Completeness & Gap Analysis" in content
