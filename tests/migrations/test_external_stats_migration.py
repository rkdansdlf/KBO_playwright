"""Migration checks for provider-specific season statistics."""

from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/056_external_season_stats.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/050_external_season_stats.sql"


def test_sqlite_external_stats_migration_is_idempotent() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE player_basic (player_id INTEGER PRIMARY KEY)")
        connection.executescript(sql)
        connection.executescript(sql)
        columns = {row[1] for row in connection.execute("PRAGMA table_info(external_season_stats)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(external_season_stats)")}

    assert {"source_record_key", "provider", "metrics", "player_id", "resolution_status"} <= columns
    assert "idx_external_stats_provider_season" in indexes
    assert "idx_external_stats_player" in indexes


def test_postgres_external_stats_migration_is_idempotent_sql() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS external_season_stats" in sql
    assert "source_record_key VARCHAR(64) NOT NULL UNIQUE" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_external_stats_resolution" in sql
    assert "DROP" not in sql
