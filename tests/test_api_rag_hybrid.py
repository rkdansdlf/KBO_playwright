"""API contract tests for hybrid RAG search and Q&A responses."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.api.app import app
from src.services.kbo_entity_resolver import ResolvedKboEntities
from src.services.hybrid_retriever import HybridSearchResult
from src.services.query_router import QueryIntent, QueryPlan, RetrievalRoute
from src.services.structured_retriever import StructuredSearchResult
from src.utils.kbo_entity_extractor import ExtractedKboEntities

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


def test_rag_ask_routes_stat_question_to_structured_retrieval() -> None:
    """Expose a structured route and analysis trace for authoritative facts."""
    plan = QueryPlan(
        query="2024년 김도영 홈런 수",
        intent=QueryIntent.STAT_QUERY,
        route=RetrievalRoute.STRUCTURED,
        entities=ResolvedKboEntities(
            ExtractedKboEntities(player_name="김도영", season_year=2024),
            player_id="100",
        ),
        stat_type="home_runs",
        filters={"player_id": "100", "season_year": 2024},
    )
    structured = StructuredSearchResult(
        chunk_id="structured:player_season_batting:1",
        title="2024년 김도영 타격 기록",
        content="2024년 김도영 구조화 타격 기록: 홈런: 38",
        source_table="player_season_batting",
        source_row_id="1",
        category="structured_stat",
        score=1.0,
        meta={"player_id": "100", "season_year": 2024},
    )
    with (
        patch("src.api.routers.rag.get_db_session") as get_db,
        patch("src.api.routers.rag.QueryRouter.plan", return_value=plan),
        patch("src.api.routers.rag.StructuredRetriever.retrieve", return_value=[structured]),
    ):
        get_db.return_value.__enter__.return_value = MagicMock()
        response = client.post(
            "/api/v1/rag/ask",
            headers=AUTH_HEADERS,
            json={"query": "2024년 김도영 홈런 수", "top_k": 1},
        )

    assert response.status_code == 200
    data = response.json()
    assert data["analysis"]["route"] == "STRUCTURED"
    assert data["retrieval"]["mode"] == "structured"
    assert data["chunks"][0]["category"] == "structured_stat"
    assert data["chunks"][0]["provenance"]["type"] == "structured_db"
