# ruff: noqa: INP001
"""Add source identity uniqueness to an existing SQLite RAG table."""

from __future__ import annotations

from typing import Any

_INDEX_NAME = "uq_rag_chunks_source_identity"


def upgrade(connection: Any) -> None:
    """Deduplicate rows by source identity and create a unique index idempotently."""
    connection.execute(
        "DELETE FROM rag_chunks WHERE id NOT IN (SELECT MIN(id) FROM rag_chunks GROUP BY source_table, source_row_id)"
    )
    connection.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {_INDEX_NAME} ON rag_chunks(source_table, source_row_id)")
    connection.commit()
