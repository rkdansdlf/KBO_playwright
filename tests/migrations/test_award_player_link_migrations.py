from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/054_add_award_player_id.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/048_add_award_player_id.sql"


def test_sqlite_award_migration_adds_player_link_columns():
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(
            """
            CREATE TABLE awards (
                id INTEGER PRIMARY KEY,
                year INTEGER NOT NULL,
                award_type VARCHAR(50) NOT NULL,
                category VARCHAR(50),
                player_name VARCHAR(100) NOT NULL,
                team_name VARCHAR(50) NOT NULL
            );
            """,
        )
        connection.executescript(SQLITE_MIGRATION.read_text(encoding="utf-8"))

        columns = {row[1] for row in connection.execute("PRAGMA table_info(awards)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(awards)")}

    assert {"player_id", "team_code"} <= columns
    assert "idx_award_player_id" in indexes


def test_postgres_award_migration_uses_if_not_exists_columns() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS player_id INTEGER" in sql
    assert "ADD COLUMN IF NOT EXISTS team_code VARCHAR(20)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_award_player_id" in sql


def test_postgres_award_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert sql.count("ADD COLUMN IF NOT EXISTS") == 2
    statements = [line.strip() for line in sql.splitlines() if line.strip() and not line.strip().startswith("--")]
    assert all(statement.startswith(("ALTER", "CREATE")) for statement in statements)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
