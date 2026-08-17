"""Tests for dual-index update and delete propagation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.rag_index_propagation import (
    propagate_index_delete,
    propagate_index_update,
    publish_index_batch,
)


def test_update_keeps_rows_non_retrievable_until_both_indexes_are_written() -> None:
    """Write a pending pair before publishing the final ACTIVE state."""
    primary = MagicMock()
    vector = MagicMock()
    primary.scalar.return_value = None
    vector.scalar.return_value = None
    chunk = {
        "source_table": "game",
        "source_row_id": "g1",
        "title": "Updated game",
        "content": "KIA가 승리했다.",
        "meta": {"document_type": "game_result"},
    }

    with (
        patch("src.services.rag_index_propagation.RagChunkRepository") as sparse_cls,
        patch("src.services.rag_index_propagation.VectorSearchRepository") as vector_cls,
    ):
        result = propagate_index_update(primary, vector, chunk, [1.0, 0.0], index_version="rag-v2")

    assert result.primary_status == "ACTIVE"
    assert result.vector_status == "ACTIVE"
    sparse_calls = sparse_cls.return_value.upsert_chunks.call_args_list
    vector_calls = vector_cls.return_value.upsert_chunk.call_args_list
    assert [call.args[1][0]["index_status"] for call in sparse_calls] == ["PENDING", "ACTIVE"]
    assert [call.args[1]["index_status"] for call in vector_calls] == ["PENDING", "ACTIVE"]
    assert primary.commit.call_count == 1
    assert vector.commit.call_count == 2


def test_delete_marks_both_rows_before_optional_purge() -> None:
    """Remove an indexed source only after both stores carry the delete state."""
    primary = MagicMock()
    vector = MagicMock()
    primary_row = SimpleNamespace(index_status="ACTIVE")
    vector_row = SimpleNamespace(index_status="ACTIVE")

    with patch(
        "src.services.rag_index_propagation._row",
        side_effect=[primary_row, vector_row, primary_row, vector_row],
    ):
        result = propagate_index_delete(primary, vector, "game", "g1", purge=True)

    assert result.operation == "purge"
    assert result.primary_status == "DELETED"
    assert result.vector_status == "DELETED"
    assert primary_row.index_status == "DELETED"
    assert vector_row.index_status == "DELETED"
    primary.delete.assert_called_once_with(primary_row)
    vector.delete.assert_called_once_with(vector_row)


def test_bulk_publish_surfaces_active_sparse_failure_after_vector_commit() -> None:
    """Keep the recovery boundary visible when the final sparse publish fails."""
    primary = MagicMock()
    vector = MagicMock()
    primary.get_bind.return_value.dialect.name = "postgresql"
    vector.get_bind.return_value.dialect.name = "postgresql"
    payload = {"source_table": "game", "source_row_id": "g1", "title": "Game", "content": "내용"}
    events: list[str] = []
    fail_active_sparse = True

    def sparse_upsert(_session, _payloads, status):
        events.append(f"sparse:{status}")
        if status == "ACTIVE" and fail_active_sparse:
            raise RuntimeError("injected sparse active publish failure")

    def vector_upsert(_session, _payloads, status):
        events.append(f"vector:{status}")

    primary.commit.side_effect = lambda: events.append("sparse:commit")
    vector.commit.side_effect = lambda: events.append("vector:commit")
    with (
        patch("src.services.rag_index_propagation._reject_purged_batch"),
        patch("src.services.rag_index_propagation._bulk_upsert_sparse", side_effect=sparse_upsert),
        patch("src.services.rag_index_propagation._bulk_upsert_vector", side_effect=vector_upsert),
    ):
        with pytest.raises(RuntimeError, match="injected sparse active publish failure"):
            publish_index_batch(primary, vector, [payload])

        assert events == [
            "sparse:PENDING",
            "vector:PENDING",
            "vector:commit",
            "vector:ACTIVE",
            "vector:commit",
            "sparse:ACTIVE",
        ]

        fail_active_sparse = False
        assert publish_index_batch(primary, vector, [payload]) == 1

    assert events[-4:] == ["vector:ACTIVE", "vector:commit", "sparse:ACTIVE", "sparse:commit"]
    primary.commit.assert_called_once()
