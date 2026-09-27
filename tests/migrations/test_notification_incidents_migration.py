"""Migration tests for the notification incident ledger (sqlite 061, postgresql 056, oracle 075)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/061_notification_incidents.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/056_notification_incidents.sql"
ORACLE_MIGRATION = ROOT / "migrations/oracle/075_notification_incidents.sql"

EXPECTED_COLUMNS = {
    "incident_key",
    "source",
    "component",
    "severity",
    "state",
    "title",
    "message",
    "details_hash",
    "occurrence_count",
    "notification_count",
    "first_opened_at",
    "last_seen_at",
    "last_notified_at",
    "resolved_at",
    "metadata",
    "created_at",
    "updated_at",
}

EXPECTED_INDEXES = {
    "idx_notification_incidents_state_severity",
    "idx_notification_incidents_source_last_seen",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(notification_incidents)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(notification_incidents)")}

    assert "notification_incidents" in tables
    assert columns >= EXPECTED_COLUMNS
    assert indexes >= EXPECTED_INDEXES


def test_sqlite_incident_key_is_unique() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        insert = (
            "INSERT INTO notification_incidents "
            "(incident_key, source, severity, state, first_opened_at, last_seen_at) "
            "VALUES (?, 'data_integrity', 'ERROR', 'OPEN', '2026-09-25T04:45:00', '2026-09-25T04:45:00')"
        )
        connection.execute(insert, ("data_integrity:game_stats:20260925",))
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(insert, ("data_integrity:game_stats:20260925",))


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS notification_incidents" in sql
    assert "incident_key VARCHAR(255) NOT NULL UNIQUE" in sql
    for index in EXPECTED_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    assert "DROP" not in sql


def test_oracle_migration_is_idempotent_syntax() -> None:
    sql = ORACLE_MIGRATION.read_text(encoding="utf-8")
    assert "user_tables" in sql
    assert "user_indexes" in sql
    assert "CREATE TABLE NOTIFICATION_INCIDENTS" in sql
    assert "UQ_NOTIFICATION_INCIDENTS_KEY UNIQUE (INCIDENT_KEY)" in sql
    for index in ("IDX_NOTIFICATION_INCIDENTS_STATE_SEV", "IDX_NOTIFICATION_INCIDENTS_SRC_SEEN"):
        assert index in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
