# ruff: noqa: INP001
"""Add canonical identity columns to an existing SQLite RAG table."""

from __future__ import annotations

from typing import Any

_COLUMNS = {
    "content_hash": "VARCHAR(64)",
    "index_version": "VARCHAR(64)",
    "index_status": "VARCHAR(24) NOT NULL DEFAULT 'ACTIVE'",
    "indexed_at": "DATETIME",
}


def upgrade(connection: Any) -> None:
    """Add missing RAG identity columns and indexes idempotently."""
    columns = {row[1] for row in connection.execute("PRAGMA table_info(rag_chunks)")}
    for name, definition in _COLUMNS.items():
        if name not in columns:
            connection.execute(f"ALTER TABLE rag_chunks ADD COLUMN {name} {definition}")
    connection.execute("UPDATE rag_chunks SET index_status = 'ACTIVE' WHERE index_status IS NULL")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_rag_chunks_content_hash ON rag_chunks(content_hash)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_rag_chunks_index_version ON rag_chunks(index_version)")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_rag_chunks_index_status ON rag_chunks(index_status)")
    connection.commit()
