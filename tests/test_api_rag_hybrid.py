"""API contract tests for hybrid RAG search and Q&A responses."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import app
from src.services.hybrid_retriever import HybridSearchResult

client = TestClient(app)
AUTH_HEADERS = {"X-API-Key": os.getenv("REST_API_KEY", "")}


def _result() -> HybridSearchResult:
    """Return a representative hybrid result."""
    return HybridSearchResult(
        chunk_id="press_release:100",
        title="올스타전 일정 발표",
        content="2026 KBO 올스타전 일정이 발표되었습니다.",
        source_url="https://example.com/notice/100",
        category="press_release",
        vector_rank=1,
        bm25_rank=2,
        rrf_score=0.0325,
        meta={"notice_id": "100"},
    )


def test_hybrid_search_keeps_source_metadata() -> None:
    """Test that the response model preserves URL and document type fields."""
    with (
        patch("src.api.routers.rag.get_db_session") as get_db,
        patch("src.api.routers.rag.HybridRetriever.retrieve", return_value=[_result()]),
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        response = client.post(
            "/api/v1/rag/hybrid-search",
            headers=AUTH_HEADERS,
            json={"query": "올스타전 일정", "top_k": 1},
        )

    assert response.status_code == 200
    item = response.json()["results"][0]
    assert item["source_url"] == "https://example.com/notice/100"
    assert item["category"] == "press_release"


def test_rag_ask_uses_hybrid_results_and_structured_sources() -> None:
    """Test that Q&A returns highlighted snippets and source metadata."""
    with (
        patch("src.api.routers.rag.get_db_session") as get_db,
        patch("src.api.routers.rag.HybridRetriever.retrieve", return_value=[_result()]),
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        response = client.post(
            "/api/v1/rag/ask",
            headers=AUTH_HEADERS,
            json={"query": "올스타전 일정", "top_k": 1},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["chunk_count"] == 1
    assert data["sources"][0]["source_url"] == "https://example.com/notice/100"
    assert data["sources"][0]["document_type"] == "press_release"
    assert "**올스타전**" in data["sources"][0]["snippet"]
