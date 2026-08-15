"""Unit tests for calculate_projections CLI."""

from __future__ import annotations

import json

import pytest

from src.cli.calculate_projections import calculate_projections_batch, main
from src.db.engine import init_db


@pytest.fixture(autouse=True)
def _setup_db():
    init_db()


def test_calculate_projections_batch_dry_run() -> None:
    """Projections batch run in dry-run mode should calculate without errors."""
    summary = calculate_projections_batch(2026, dry_run=True, limit=5)
    assert summary.target_season == 2026
    assert summary.dry_run is True
    assert summary.persisted_count == 0


def test_calculate_projections_cli_json(capsys) -> None:
    """CLI should support --json output format."""
    exit_code = main(["--season", "2026", "--dry-run", "--limit", "2", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["target_season"] == 2026
    assert data["dry_run"] is True
