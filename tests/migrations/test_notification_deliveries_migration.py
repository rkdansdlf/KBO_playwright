"""Migration tests for the delivery audit ledger (sqlite 064, postgresql 059, oracle 076)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/064_notification_deliveries.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/059_notification_deliveries.sql"
ORACLE_MIGRATION = ROOT / "migrations/oracle/076_notification_deliveries.sql"

EXPECTED_COLUMNS = {
    "incident_id",
    "notification_type",
    "batch_id",
    "channel",
    "destination",
    "status",
    "attempt_count",
    "dispatched_at",
    "completed_at",
    "latency_ms",
    "error_code",
    "error_message",
    "created_at",
    "updated_at",
}

EXPECTED_INDEXES = {
    "idx_notification_deliveries_incident",
    "idx_notification_deliveries_channel_status",
    "idx_notification_deliveries_batch",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(notification_deliveries)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(notification_deliveries)")}

    assert "notification_deliveries" in tables
    assert columns >= EXPECTED_COLUMNS
    assert indexes >= EXPECTED_INDEXES


def test_sqlite_incident_id_is_soft_reference_without_fk() -> None:
    """Delivery rows must outlive incident pruning, so there is no FK."""
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    assert "FOREIGN KEY" not in sql.upper()
    assert "REFERENCES" not in sql.upper()


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS notification_deliveries" in sql
    for index in EXPECTED_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    assert "DROP" not in sql


def test_oracle_migration_is_idempotent_syntax() -> None:
    sql = ORACLE_MIGRATION.read_text(encoding="utf-8")
    assert "user_tables" in sql
    assert "user_indexes" in sql
    assert "CREATE TABLE NOTIFICATION_DELIVERIES" in sql
    for index in (
        "IDX_NOTIFICATION_DELIVERIES_INCIDENT",
        "IDX_NOTIFICATION_DELIVERIES_CHAN_STATUS",
        "IDX_NOTIFICATION_DELIVERIES_BATCH",
    ):
        assert index in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
