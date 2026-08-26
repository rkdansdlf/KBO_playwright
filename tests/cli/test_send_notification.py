"""Unit tests for src.cli.send_notification."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from src.cli.send_notification import build_arg_parser, main

if TYPE_CHECKING:
    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--channel", "telegram", "--title", "T", "--body", "B", "--dry-run"])
    assert args.channel == "telegram"
    assert args.title == "T"
    assert args.body == "B"
    assert args.dry_run is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["--channel", "console", "--title", "CLI Test", "--body", "Body text", "--dry-run", "--json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_messages"] == 1
    assert data["sent_count"] == 1
