"""Migration tests for the DLQ (status, updated_at) recovery index (sqlite 063, postgresql 058)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_TABLE = ROOT / "migrations/sqlite/062_crawl_dead_letters.sql"
SQLITE_INDEX = ROOT / "migrations/sqlite/063_crawl_dead_letters_status_updated_index.sql"
POSTGRES_INDEX = ROOT / "migrations/postgresql/058_crawl_dead_letters_status_updated_index.sql"

INDEX_NAME = "idx_crawl_dead_letters_status_updated"


def test_sqlite_status_updated_index_is_idempotent() -> None:
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(SQLITE_TABLE.read_text(encoding="utf-8"))
        connection.executescript(SQLITE_INDEX.read_text(encoding="utf-8"))
        connection.executescript(SQLITE_INDEX.read_text(encoding="utf-8"))
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(crawl_dead_letters)")}
    assert INDEX_NAME in indexes


def test_postgres_status_updated_index_is_idempotent_syntax() -> None:
    sql = POSTGRES_INDEX.read_text(encoding="utf-8")
    assert f"CREATE INDEX IF NOT EXISTS {INDEX_NAME}" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
