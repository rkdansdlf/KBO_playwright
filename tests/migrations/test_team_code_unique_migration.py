from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION_PATH = ROOT / "migrations/sqlite/047_add_team_code_to_player_season_unique.py"
SPEC = importlib.util.spec_from_file_location("team_code_migration", MIGRATION_PATH)
assert SPEC is not None
assert SPEC.loader is not None
migration = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(migration)


def _create_tables(connection: sqlite3.Connection) -> None:
    for table in ("player_season_batting", "player_season_pitching"):
        connection.execute(
            f"""
            CREATE TABLE {table} (
                id INTEGER PRIMARY KEY,
                player_id INTEGER NOT NULL,
                season INTEGER NOT NULL,
                league TEXT NOT NULL,
                level TEXT NOT NULL,
                team_code TEXT,
                value INTEGER,
                CONSTRAINT uq_{table} UNIQUE (player_id, season, league, level)
            )
            """,
        )
        connection.execute(f"CREATE INDEX idx_{table}_player ON {table} (player_id, season)")


def test_migration_preserves_rows_and_allows_team_splits() -> None:
    with sqlite3.connect(":memory:") as connection:
        _create_tables(connection)
        connection.execute(
            "INSERT INTO player_season_batting VALUES (1, 100, 2021, 'REGULAR', 'KBO1', 'LG', 10)",
        )
        connection.execute(
            "INSERT INTO player_season_pitching VALUES (1, 100, 2021, 'REGULAR', 'KBO1', 'LG', 10)",
        )

        migration.upgrade(connection)
        migration.upgrade(connection)

        connection.execute(
            "INSERT INTO player_season_batting VALUES (2, 100, 2021, 'REGULAR', 'KBO1', 'KH', 8)",
        )
        connection.execute(
            "INSERT INTO player_season_pitching VALUES (2, 100, 2021, 'REGULAR', 'KBO1', 'KH', 8)",
        )

        assert connection.execute("SELECT COUNT(*) FROM player_season_batting").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM player_season_pitching").fetchone()[0] == 2
        assert (
            "team_code"
            in connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'player_season_batting'",
            ).fetchone()[0]
        )
