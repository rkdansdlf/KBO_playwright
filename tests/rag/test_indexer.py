"""Unit tests for src.rag.indexer.knowledge_indexer."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.rag.indexer.knowledge_indexer import KnowledgeIndexer


def test_knowledge_indexer_methods_mocked() -> None:
    mock_session = MagicMock()
    indexer = KnowledgeIndexer(mock_session)

    # Mock rag repository
    mock_rag_repo = MagicMock()
    mock_rag_repo.upsert_chunks.return_value = 5
    indexer.rag_repo = mock_rag_repo

    # Mock DB query executions
    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    counts = indexer.index_all()
    assert isinstance(counts, dict)
    assert "press_releases" in counts
    assert "milestones" in counts
    assert "futures_schedule" in counts
    assert "player_splits" in counts
