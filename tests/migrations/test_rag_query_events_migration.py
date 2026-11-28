"""Migration tests for the L3 query ledger (sqlite 071, postgresql 070)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.models import RagQueryEvent

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/071_rag_query_events.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/070_rag_query_events.sql"

EXPECTED_COLUMNS = {
    "id",
    "created_at",
    "query_hash",
    "query_chars",
    "route",
    "intent",
    "outcome",
    "source_table",
    "result_count",
    "structured_candidates",
    "fallback_used",
    "latency_ms",
}

EXPECTED_INDEXES = {
    "idx_rag_query_events_created_at",
    "idx_rag_query_events_outcome_created_at",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(rag_query_events)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(rag_query_events)")}

    assert "rag_query_events" in tables
    assert columns >= EXPECTED_COLUMNS
    assert indexes >= EXPECTED_INDEXES


def test_postgres_migration_is_idempotent_and_has_no_raw_query_column() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS rag_query_events" in sql
    for index in EXPECTED_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    for column in EXPECTED_COLUMNS:
        assert column in sql
    # The whole point of the table is that the question is never stored.
    assert "query_text" not in sql
    assert "raw_query" not in sql


def test_orm_columns_match_the_migration_contract() -> None:
    columns = {column.name for column in RagQueryEvent.__table__.columns}
    assert columns == EXPECTED_COLUMNS
    orm_indexes = {index.name for index in RagQueryEvent.__table__.indexes}
    assert orm_indexes >= EXPECTED_INDEXES
