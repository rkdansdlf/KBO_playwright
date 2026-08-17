"""Tests for the SQLite RAG index consistency migration."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path


def _load_migration():
    """Load the numbered SQLite migration module."""
    path = Path(__file__).parents[2] / "migrations" / "sqlite" / "057_rag_index_consistency.py"
    spec = importlib.util.spec_from_file_location("rag_index_consistency_migration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sqlite_rag_index_migration_is_idempotent() -> None:
    """Add identity columns while preserving safe rerun behavior."""
    migration = _load_migration()
    with sqlite3.connect(":memory:") as connection:
        connection.execute(
            "CREATE TABLE rag_chunks (id INTEGER PRIMARY KEY, source_table TEXT NOT NULL, source_row_id TEXT NOT NULL)"
        )
        migration.upgrade(connection)
        migration.upgrade(connection)
        columns = {row[1] for row in connection.execute("PRAGMA table_info(rag_chunks)")}
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(rag_chunks)")}

    assert {"content_hash", "index_version", "index_status", "indexed_at"} <= columns
    assert {
        "idx_rag_chunks_content_hash",
        "idx_rag_chunks_index_version",
        "idx_rag_chunks_index_status",
    } <= indexes


def test_oracle_rag_index_migration_handles_equivalent_orm_indexes() -> None:
    """Allow Oracle's duplicate-column-index error during baseline reconciliation."""
    sql = Path("migrations/oracle/059_rag_index_consistency.sql").read_text(encoding="utf-8").upper()

    assert "SQLCODE != -1408" in sql
