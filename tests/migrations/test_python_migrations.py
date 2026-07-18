from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[2]


def _load_migration(relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_deletion_anomaly_migration_dry_run_delegates_to_repair(monkeypatch, tmp_path: Path) -> None:
    migration = _load_migration("migrations/sqlite/005_deletion_anomaly_integrity.py")
    repair = MagicMock(return_value=[SimpleNamespace(status="planned", name="check", row_count=0)])
    monkeypatch.setattr(migration, "repair", repair)
    db_path = tmp_path / "migration.db"
    monkeypatch.setattr(sys, "argv", ["migration", "--db-path", str(db_path), "--dry-run"])

    migration.main()

    repair.assert_called_once_with(db_path, apply=False, schema=True)


def test_fielding_float_migration_preserves_data_and_is_idempotent() -> None:
    migration = _load_migration("migrations/sqlite/032_fix_team_season_fielding_float_columns.py")
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE team_season_fielding (
            id INTEGER PRIMARY KEY,
            def_innings INTEGER,
            fielding_pct INTEGER,
            range_factor_per_game INTEGER
        );
        CREATE TABLE team_season_baserunning (
            id INTEGER PRIMARY KEY,
            sb_success_rate INTEGER
        );
        INSERT INTO team_season_fielding VALUES (1, 123, 98, 4);
        INSERT INTO team_season_baserunning VALUES (1, 75);
        """,
    )

    migration.upgrade(connection)
    first_rows = connection.execute("SELECT * FROM team_season_fielding").fetchall()
    first_types = {row[1]: row[2].upper() for row in connection.execute("PRAGMA table_info(team_season_fielding)")}
    first_running_types = {
        row[1]: row[2].upper() for row in connection.execute("PRAGMA table_info(team_season_baserunning)")
    }

    migration.upgrade(connection)

    assert first_rows == [(1, 123, 98, 4)]
    assert connection.execute("SELECT * FROM team_season_fielding").fetchall() == first_rows
    assert first_types["def_innings"] == "REAL"
    assert first_types["fielding_pct"] == "REAL"
    assert first_types["range_factor_per_game"] == "REAL"
    assert first_running_types["sb_success_rate"] == "REAL"
