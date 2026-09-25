"""Migration tests for the dead letter queue (sqlite 062, postgresql 057)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SQLITE_MIGRATION = ROOT / "migrations/sqlite/062_crawl_dead_letters.sql"
POSTGRES_MIGRATION = ROOT / "migrations/postgresql/057_crawl_dead_letters.sql"

EXPECTED_COLUMNS = {
    "dlq_id",
    "original_run_id",
    "crawler",
    "target_type",
    "target_id",
    "season",
    "game_id",
    "failure_stage",
    "error_code",
    "error_message",
    "error_type",
    "source_url",
    "payload_ref",
    "snapshot_id",
    "evidence_id",
    "retry_count",
    "max_retries",
    "next_retry_at",
    "status",
    "replay_run_id",
    "resolved_at",
    "created_at",
    "updated_at",
}

EXPECTED_INDEXES = {
    "idx_crawl_dead_letters_status_retry",
    "idx_crawl_dead_letters_original_run",
    "idx_crawl_dead_letters_crawler_status",
    "idx_crawl_dead_letters_error_code",
    "idx_crawl_dead_letters_next_retry",
}


def test_sqlite_migration_creates_table_idempotently() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.executescript(sql)

        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in connection.execute("PRAGMA table_info(crawl_dead_letters)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(crawl_dead_letters)")}

    assert "crawl_dead_letters" in tables
    assert EXPECTED_COLUMNS.issubset(columns)
    assert EXPECTED_INDEXES.issubset(indexes)
    assert "uq_crawl_dead_letters_incident" in sql


def test_sqlite_incident_unique_absorbs_same_run_only() -> None:
    sql = SQLITE_MIGRATION.read_text(encoding="utf-8")
    row = (
        "one",
        "run-a",
        "awards",
        "award_history",
        "kbo_awards_wikipedia",
        "fetch",
        "FETCH_TIMEOUT",
    )
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(sql)
        connection.execute(
            "INSERT INTO crawl_dead_letters "
            "(dlq_id, original_run_id, crawler, target_type, target_id, failure_stage, error_code) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            row,
        )
        # Same source within the same original run is absorbed by the unique key.
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO crawl_dead_letters "
                "(dlq_id, original_run_id, crawler, target_type, target_id, failure_stage, error_code) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("two", "run-a", "awards", "award_history", "kbo_awards_wikipedia", "fetch", "FETCH_HTTP_ERROR"),
            )
        # A later failure from a different run is a new incident.
        connection.execute(
            "INSERT INTO crawl_dead_letters "
            "(dlq_id, original_run_id, crawler, target_type, target_id, failure_stage, error_code) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("three", "run-b", "awards", "award_history", "kbo_awards_wikipedia", "fetch", "FETCH_TIMEOUT"),
        )
        count = connection.execute("SELECT COUNT(*) FROM crawl_dead_letters").fetchone()[0]
    assert count == 2


def test_postgres_migration_is_idempotent_syntax() -> None:
    sql = POSTGRES_MIGRATION.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS crawl_dead_letters" in sql
    assert "CONSTRAINT uq_crawl_dead_letters_incident" in sql
    for index in EXPECTED_INDEXES:
        assert f"CREATE INDEX IF NOT EXISTS {index}" in sql
    assert "DROP" not in sql


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
