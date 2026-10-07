"""Migration tests for parking fee kinds (sqlite 065, postgresql 060, oracle 077)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/065_parking_fee_kinds.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/060_parking_fee_kinds.sql"
ORACLE_MIGRATION = ROOT / "migrations/oracle/077_parking_fee_kinds.sql"

EXPECTED_COLUMNS = {
    "parking_lot_id",
    "fee_kind",
    "amount_krw",
    "source_url",
    "created_at",
    "updated_at",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE parking_lots (id INTEGER PRIMARY KEY)")
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(parking_fee_kinds)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(parking_fee_kinds)")}

    assert "parking_fee_kinds" in tables
    assert columns >= EXPECTED_COLUMNS
    assert "idx_parking_fee_kinds_lot" in indexes


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS parking_fee_kinds" in sql
    assert "UNIQUE(parking_lot_id, fee_kind)" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_parking_fee_kinds_lot" in sql
    assert "DROP" not in sql


def test_oracle_migration_is_idempotent_syntax() -> None:
    sql = ORACLE_MIGRATION.read_text(encoding="utf-8")
    assert "user_tables" in sql
    assert "user_indexes" in sql
    assert "CREATE TABLE PARKING_FEE_KINDS" in sql
    assert "CONSTRAINT UQ_PARKING_FEE_KIND UNIQUE (PARKING_LOT_ID, FEE_KIND)" in sql
    assert "IDX_PARKING_FEE_KINDS_LOT" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
