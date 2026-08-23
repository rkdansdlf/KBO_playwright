"""Unit tests for src.cli.seed_synthetic_data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from src.cli.seed_synthetic_data import build_arg_parser, main
from src.models.base import Base

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_build_arg_parser() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(["--season", "2026", "--games-per-team", "3", "--json"])
    assert args.season == 2026
    assert args.games_per_team == 3
    assert args.json is True


def test_main_cli_execution_json(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    db_file = tmp_path / "test_synth.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    exit_code = main(["--season", "2026", "--games-per-team", "1", "--json", "--db-url", db_url])
    captured = capsys.readouterr()

    assert exit_code == 0
    data = json.loads(captured.out)
    assert data["total_games"] >= 1
    assert data["total_players"] >= 10
