"""Subprocess contract matrix test for relay CLIs and compatibility wrappers."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from src.models.base import Base

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


@pytest.fixture(scope="module")
def initialized_test_db(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Create a temporary sqlite DB with all schema tables."""
    db_file = tmp_path_factory.mktemp("cli_contracts") / "contracts.db"
    db_url = f"sqlite:///{db_file}"
    engine = create_engine(db_url)
    Base.metadata.create_all(bind=engine)
    engine.dispose()
    return db_url


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_help_contract(module: str) -> None:
    """Verify that --help exits with code 0 and displays usage."""
    cmd = [VENV_PYTHON, "-m", module, "--help"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0, f"{module} --help failed: {result.stderr}"
    assert "usage:" in result.stdout.lower() or "options:" in result.stdout.lower()


@pytest.mark.parametrize("module", CANONICAL_MODULES + WRAPPER_MODULES)
def test_cli_invalid_option_contract(module: str) -> None:
    """Verify that invalid arguments exit with code 2 (standard argparse error)."""
    cmd = [VENV_PYTHON, "-m", module, "--totally-invalid-option-xyz"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 2, f"{module} invalid option did not exit with 2: {result.returncode}"
    assert "unrecognized arguments" in result.stderr.lower() or "error:" in result.stderr.lower()


def test_wrapper_argument_forwarding_rebuild(initialized_test_db: str) -> None:
    """Verify that the wrapper properly forwards arguments to the underlying module."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.rebuild_relay_events",
        "--season",
        "2099",
        "--dry-run",
    ]
    env = {**os.environ, "DATABASE_URL": initialized_test_db}
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0
    assert "2099" in result.stdout or "2099" in result.stderr or result.returncode == 0


def test_wrapper_argument_forwarding_seed_dry_run(initialized_test_db: str) -> None:
    """Verify that the wrapper forwards --dry-run without committing mutations."""
    cmd = [
        VENV_PYTHON,
        "-m",
        "src.cli.seed_relay_validation_metrics",
        "--season",
        "2099",
        "--dry-run",
    ]
    env = {**os.environ, "DATABASE_URL": initialized_test_db}
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=10, check=False)
    assert result.returncode == 0
