"""Subprocess contract tests for text relay CLI commands.

Verifies:
1. All canonical CLIs and wrappers respond to --help with exit code 0.
2. All canonical CLIs and wrappers reject invalid arguments with exit code 2.
3. Compatibility wrappers properly forward business arguments (--season, --dry-run, --json)
   with precise semantic assertions against an isolated test database.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import Game, GameEvent, GameValidationMetrics

VENV_PYTHON = sys.executable

CANONICAL_MODULES = [
    "src.cli.collection.crawl_text_relay",
    "src.cli.sync.load_text_relay",
    "src.cli.backfill.rebuild_relay_events",
    "src.cli.collection.seed_relay_validation_metrics",
]

WRAPPER_MODULES = [
    "src.cli.crawl_text_relay",
    "src.cli.load_text_relay",
    "src.cli.rebuild_relay_events",
    "src.cli.seed_relay_validation_metrics",
]


@pytest.fixture
def initialized_test_db(tmp_path: Path) -> str:
    """Create an isolated test SQLite DB with schema and test games."""
    db_file = tmp_path / "test_cli_runtime.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    with Session() as s:
        # 2098 game & event
        s.add(
            Game(
                game_id="20980401LGSS0",
                game_date=date(2098, 4, 1),
                home_team="SS",
                away_team="LG",
                game_status="COMPLETED",
            )
        )
        s.add(
            GameEvent(
                game_id="20980401LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                description="홍길동 안타",
            )
        )
        # 2099 game & event
        s.add(
            Game(
                game_id="20990401LGSS0",
                game_date=date(2099, 4, 1),
                home_team="SS",
                away_team="LG",
                game_status="COMPLETED",
            )
        )
        s.add(
            GameEvent(
                game_id="20990401LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                description="이순신 홈런",
            )
        )
        s.commit()

    return db_url


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_help_contract(module: str) -> None:
    """Verify that --help exits with code 0."""
    cmd = [VENV_PYTHON, "-m", module, "--help"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0, f"{module} --help failed with code {result.returncode}"
    assert "show this help message and exit" in result.stdout.lower()


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_invalid_option_contract(module: str) -> None:
    """Verify that invalid arguments exit with code 2 (argparse error)."""
    cmd = [VENV_PYTHON, "-m", module, "--totally-invalid-option-xyz"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 2, f"{module} invalid option did not exit with 2: {result.returncode}"
    assert "unrecognized arguments" in result.stderr.lower() or "error:" in result.stderr.lower()


def test_wrapper_argument_forwarding_seed_dry_run(initialized_test_db: str) -> None:
    """Verify that seed wrapper forwards --season, --dry-run, --json with strict scoping."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.seed_relay_validation_metrics",
        "--season",
        "2099",
        "--dry-run",
        "--json",
    ]
    env = {**os.environ, "DATABASE_URL": initialized_test_db}
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0, f"Command failed: {result.stderr}"

    # Verify JSON output structure and precise scoping
    data = json.loads(result.stdout.strip())
    assert data["requested_season"] == 2099
    assert data["dry_run"] is True
    assert data["total_games"] == 1  # Exactly 1 game for 2099 (2098 excluded!)
    assert data["committed_inserts"] == 0

    # Verify DB state: zero rows committed in game_validation_metrics
    engine = create_engine(initialized_test_db)
    Session = sessionmaker(bind=engine)
    with Session() as s:
        metric_count = s.query(GameValidationMetrics).count()
        assert metric_count == 0


def test_wrapper_argument_forwarding_rebuild(initialized_test_db: str) -> None:
    """Verify that rebuild wrapper forwards --season, --dry-run, and --json properly."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.rebuild_relay_events",
        "--season",
        "2099",
        "--dry-run",
        "--json",
    ]
    env = {**os.environ, "DATABASE_URL": initialized_test_db}
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0, f"Command failed: {result.stderr}"

    data = json.loads(result.stdout.strip())
    assert data["requested_seasons"] == [2099]
    assert data["dry_run"] is True
    assert data["apply"] is False
    assert data["total_games"] == 1
    assert data["report_rows"][0]["game_id"] == "20990401LGSS0"
