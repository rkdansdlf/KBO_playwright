"""Migration tests for the crawl execution run ledger (sqlite 060, postgresql 055)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/060_crawl_execution_runs.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/055_crawl_execution_runs.sql"

EXPECTED_COLUMNS = {
    "run_id",
    "crawler",
    "target_type",
    "target_id",
    "season",
    "game_id",
    "status",
    "attempt",
    "started_at",
    "finished_at",
    "records_read",
    "records_written",
    "records_failed",
    "error_code",
    "error_message",
    "checkpoint",
    "source_url",
    "parser_version",
    "snapshot_id",
    "evidence_id",
    "parent_run_id",
    "replay_of_run_id",
    "created_at",
    "updated_at",
}

EXPECTED_INDEXES = {
    "idx_crawl_execution_runs_crawler",
    "idx_crawl_execution_runs_status",
    "idx_crawl_execution_runs_game",
    "idx_crawl_execution_runs_parent",
    "idx_crawl_execution_runs_replay_of",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(crawl_execution_runs)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(crawl_execution_runs)")}

    assert "crawl_execution_runs" in tables
    assert columns >= EXPECTED_COLUMNS
    assert indexes >= EXPECTED_INDEXES


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS crawl_execution_runs" in sql
    assert "run_id VARCHAR(36) NOT NULL UNIQUE" in sql
    for index in EXPECTED_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
