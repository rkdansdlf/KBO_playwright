# ruff: noqa: INP001
"""Add embedding_vector JSON column to SQLite rag_chunks table for cross-dialect compatibility."""

from __future__ import annotations

from typing import Any


def upgrade(connection: Any) -> None:
    """Ensure embedding_vector column exists on rag_chunks table idempotently."""
    cursor = connection.cursor() if hasattr(connection, "cursor") else connection
    cursor.execute("PRAGMA table_info(rag_chunks)")
    columns = [row[1] for row in cursor.fetchall()]

    if "embedding_vector" not in columns:
        cursor.execute("ALTER TABLE rag_chunks ADD COLUMN embedding_vector JSON")

    # Mirror existing embedding column if present
    if "embedding" in columns:
        cursor.execute(
            "UPDATE rag_chunks SET embedding_vector = embedding "
            "WHERE embedding_vector IS NULL AND embedding IS NOT NULL"
        )

    if hasattr(connection, "commit"):
        connection.commit()
