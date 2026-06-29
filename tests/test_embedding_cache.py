"""Unit tests for the local SQLite EmbeddingCache system in EmbeddingService."""

from __future__ import annotations

from unittest import mock

import pytest

from src.db.engine import Engine, SessionLocal
from src.models.embedding_cache import EmbeddingCache
from src.services.embedding_service import EmbeddingService

pytestmark = pytest.mark.integration


def test_embedding_cache_mechanism():
    Engine.dispose()
    EmbeddingCache.__table__.create(bind=Engine, checkfirst=True)

    # 1. Initialize the service and mock API key
    svc = EmbeddingService()
    svc.api_key = "sk-or-v1-mock-key-for-cache-testing"

    # We mock the OpenRouter network fetch method
    mock_vector = [0.1] * 1536  # text-embedding-3-small default
    mock_fetch = mock.MagicMock(return_value=[mock_vector])

    test_text = "이것은 캐시 테스트용 고유 텍스트 문구입니다."

    # Clean cache database entry for this test to ensure clean run
    model_name = "openai/text-embedding-3-small"
    text_hash = svc._compute_hash(test_text)

    with SessionLocal() as session:
        existing = session.get(EmbeddingCache, (text_hash, model_name))
        if existing:
            session.delete(existing)
            session.commit()

    with mock.patch.object(svc, "_fetch_openrouter_embeddings", mock_fetch):
        # First call: Should trigger API fetch
        emb1 = svc.get_embeddings_batch([test_text])
        assert len(emb1) == 1
        assert len(emb1[0]) == 256  # Should be post-processed to 256
        assert mock_fetch.call_count == 1

        # Verify L2 normalized value check
        import math

        norm = math.sqrt(sum(x * x for x in emb1[0]))
        assert abs(norm - 1.0) < 1e-5

        # Second call: Should read from SQLite cache instead of calling API
        emb2 = svc.get_embeddings_batch([test_text])
        assert len(emb2) == 1
        assert len(emb2[0]) == 256
        assert emb2[0] == emb1[0]
        assert mock_fetch.call_count == 1  # Mock API call count must remain 1

    # Cleanup the test cache entry
    with SessionLocal() as session:
        existing = session.get(EmbeddingCache, (text_hash, model_name))
        if existing:
            session.delete(existing)
            session.commit()
