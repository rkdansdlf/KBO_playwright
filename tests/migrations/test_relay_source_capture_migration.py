from __future__ import annotations

import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations/sqlite/053_relay_source_capture_columns.sql"


def test_relay_source_capture_migration_adds_legacy_columns():
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(
            """
            CREATE TABLE game_validation_metrics (id INTEGER PRIMARY KEY, validation_status VARCHAR(32));
            CREATE TABLE raw_source_snapshots (id INTEGER PRIMARY KEY, fetched_at DATETIME);
            """,
        )
        connection.executescript(MIGRATION.read_text(encoding="utf-8"))

        metric_columns = {row[1] for row in connection.execute("PRAGMA table_info(game_validation_metrics)")}
        snapshot_columns = {row[1] for row in connection.execute("PRAGMA table_info(raw_source_snapshots)")}

    assert "payload_hash_full" in metric_columns
    assert {"source_url", "content_type", "raw_size", "capture_metadata"} <= snapshot_columns
