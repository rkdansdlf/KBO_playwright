from __future__ import annotations

import pytest

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _configure_models_snippet() -> str:
    return (
        "from sqlalchemy.orm import configure_mappers; "
        "from src.models.game import Game; "
        "configure_mappers(); print('ok')"
    )


@pytest.mark.slow
def test_model_registry_configures_fa_contract_relationships() -> None:
    result = subprocess.run(
        [sys.executable, "-c", _configure_models_snippet()],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
    assert "InvalidRequestError" not in result.stderr


def test_model_registry_includes_oci_migration_dependencies() -> None:
    snippet = (
        "import src.models; "
        "from src.models.base import Base; "
        "expected = {'crawl_runs', 'team_franchises', 'team_history', "
        "'stat_rankings', 'team_season_batting', 'team_season_pitching'}; "
        "missing = expected - set(Base.metadata.tables); "
        "assert not missing, missing"
    )
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
