"""Unit tests for audit_historical_lake CLI."""

from __future__ import annotations

import json

import pytest

from src.cli.audit_historical_lake import audit_historical_lake, main
from src.db.engine import init_db


@pytest.fixture(autouse=True)
def _setup_db():
    init_db()


def test_audit_historical_lake_returns_rows() -> None:
    """Audit should return rows for 1982 to 1985."""
    rows = audit_historical_lake(1982, 1985)
    assert len(rows) == 4
    assert rows[0].season == 1982
    assert rows[0].status in ("PARTIAL", "VERIFIED_COMPLETE", "EMPTY")


def test_audit_historical_lake_cli_json(capsys) -> None:
    """CLI should support --json format."""
    exit_code = main(["--start-year", "1982", "--end-year", "1983", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert len(data) == 2
    assert data[0]["season"] == 1982
