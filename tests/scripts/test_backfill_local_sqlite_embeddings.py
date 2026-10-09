"""Tests for filling the local database's RAG vectors.

The trap this exists for is small and silent: the dev database writes "no
vector" as the four-character string ``null``, which every ``IS NOT NULL`` test
in the codebase reads as embedded. A backfill that trusts those tests finds
nothing to do, and a reader that trusts them reports full coverage.
"""

from __future__ import annotations

import sqlite3

import pytest
from scripts.maintenance.backfill_local_sqlite_embeddings import (
    _load_pending,
    needs_embedding,
    vector_json,
)

from src.constants import RAG_EMBEDDING_DIMENSION


class TestNeedsEmbedding:
    """Pin which stored values count as "no vector"."""

    @pytest.mark.parametrize("value", [None, "null", "NULL", " [] ", "", "  "])
    def test_placeholders_count_as_missing(self, value: object) -> None:
        """Treat SQL NULL and the text placeholders as absent."""
        assert needs_embedding(value) is True

    def test_a_real_vector_counts_as_present(self) -> None:
        """Leave a row that already carries a vector alone."""
        assert needs_embedding("[0.5, 0.25]") is False


class TestVectorJson:
    """Pin the guard that keeps an unusable vector out of the column."""

    def test_serializes_a_full_length_vector(self) -> None:
        """Write the vector as a JSON array of the agreed dimension."""
        payload = vector_json([0.5] * RAG_EMBEDDING_DIMENSION)
        assert payload.startswith("[0.5,")
        assert payload.endswith("]")

    def test_refuses_a_short_vector(self) -> None:
        """Refuse a vector the rest of the index could not compare against."""
        with pytest.raises(ValueError, match="expected 1536 dimensions"):
            vector_json([0.1, 0.2])

    def test_refuses_a_zero_vector(self) -> None:
        """Refuse the value a failed provider call returns instead of raising."""
        with pytest.raises(ValueError, match="zero vector"):
            vector_json([0.0] * RAG_EMBEDDING_DIMENSION)


class TestLoadPending:
    """Pin that the pending set is read the way the database stores it."""

    def test_finds_rows_holding_the_literal_null(self) -> None:
        """Catch the rows an IS NOT NULL query would skip."""
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE rag_chunks (id INTEGER, title TEXT, content TEXT, embedding_vector TEXT, embedding TEXT)"
        )
        connection.execute("INSERT INTO rag_chunks VALUES (1, 'a', 'text', 'null', 'null')")
        connection.execute("INSERT INTO rag_chunks VALUES (2, 'b', 'text', '[0.5]', '[0.5]')")
        connection.execute("INSERT INTO rag_chunks VALUES (3, 'c', 'text', NULL, 'null')")

        pending = _load_pending(connection)
        connection.close()

        assert [row[0] for row in pending] == [1, 3]
