"""Migration tests for quarantine/audit/projection tables (sqlite 055, postgresql 049)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/055_quality_gate_and_projection_tables.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/049_quality_gate_and_projection_tables.sql"

EXPECTED_TABLES = {"quarantined_records", "correction_audit_trail", "player_projections"}


def test_sqlite_migration_creates_all_three_tables_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables >= EXPECTED_TABLES

        columns = {row[1] for row in connection.execute("PRAGMA table_info(quarantined_records)")}
        assert {
            "game_id",
            "entity_type",
            "rule_id",
            "severity",
            "failure_reason",
            "raw_payload",
            "status",
            "retry_count",
        } <= columns

        indexes = {row[1] for row in connection.execute("PRAGMA index_list(quarantined_records)")}
        assert "ix_quarantined_records_status" in indexes


def test_sqlite_projection_table_has_unique_constraint() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(player_projections)")}
    assert "uq_player_projection" in sql
    assert any("autoindex" in name for name in indexes)


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    for table in EXPECTED_TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "CREATE INDEX IF NOT EXISTS ix_player_projections_player_id" in sql
    assert "CONSTRAINT uq_player_projection UNIQUE" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
