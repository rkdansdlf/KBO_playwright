"""Unit tests for Hybrid Retriever."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.services.hybrid_retriever import BM25_RRF_WEIGHT, HybridRetriever, HybridSearchResult


def test_hybrid_retriever_bm25_only() -> None:
    """Test hybrid retrieval when pgvector is not available."""
    mock_session = MagicMock()
    retriever = HybridRetriever(mock_session)

    # Mock BM25 search
    with (
        patch.object(
            retriever.bm25_engine,
            "search",
            return_value=[
                {
                    "chunk_id": "chunk-1",
                    "title": "KBO 올스타전 공지",
                    "content": "2026 KBO 올스타전 일정 및 투표 안내",
                    "source_url": "https://koreabaseball.com/1",
                    "category": "press_release",
                    "score": 5.0,
                },
                {
                    "chunk_id": "chunk-2",
                    "title": "KIA 타이거즈 우승 기록",
                    "content": "KIA 타이거즈 한국시리즈 우승",
                    "source_url": "https://koreabaseball.com/2",
                    "category": "milestone",
                    "score": 3.0,
                },
            ],
        ),
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=False),
    ):
        results = retriever.retrieve("올스타전 일정", top_k=2)

        assert len(results) == 2
        assert isinstance(results[0], HybridSearchResult)
        assert results[0].chunk_id == "chunk-1"
        assert results[0].bm25_rank == 1
        assert results[0].vector_rank is None
        assert results[0].rrf_score > results[1].rrf_score
        assert retriever.last_trace["mode"] == "hybrid"
        assert retriever.last_trace["latency_ms"]["total"] >= 0

        res_dict = results[0].to_dict()
        assert res_dict["title"] == "KBO 올스타전 공지"
        assert res_dict["score"] > 0


def test_hybrid_retriever_fuses_dense_and_sparse_by_source_identity() -> None:
    """Test that matching source rows receive both RRF rank contributions."""
    retriever = HybridRetriever(MagicMock())
    sparse = {
        "chunk_id": "rag_chunks:player_basic:78224",
        "source_table": "player_basic",
        "source_row_id": "78224",
        "title": "김도영 선수 프로필",
        "content": "KIA 김도영 선수",
        "category": "player_profile",
        "meta": {"player_id": "78224"},
    }
    dense = {
        "id": 99,
        "source_table": "player_basic",
        "source_row_id": "78224",
        "title": "김도영 선수 프로필",
        "content": "KIA 김도영 선수",
        "document_type": "player_profile",
        "source_url": "https://example.com/player/78224",
        "meta": {"player_id": "78224"},
    }

    with (
        patch.object(retriever.bm25_engine, "search", return_value=[sparse]) as sparse_search,
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=True),
        patch("src.services.embedding_service.EmbeddingService") as embedding_cls,
        patch("src.repositories.vector_search_repository.VectorSearchRepository") as repo_cls,
    ):
        embedding_cls.return_value.get_embedding.return_value = [0.1]
        repo_cls.return_value.search_by_cosine.return_value = [dense]
        results = retriever.retrieve(
            "김도영 선수",
            top_k=1,
            filters={"team_id": "KIA", "game_date": "2026-08-20"},
        )

    assert len(results) == 1
    assert results[0].chunk_id == "player_basic:78224"
    assert results[0].bm25_rank == 1
    assert results[0].vector_rank == 1
    assert results[0].rrf_score == pytest.approx(
        BM25_RRF_WEIGHT / (retriever.k + 1) + retriever.dense_weight / (retriever.k + 1)
    )
    assert results[0].source_url == "https://example.com/player/78224"
    sparse_search.assert_called_once()
    assert sparse_search.call_args.kwargs["filters"]["team_id"] == "KIA"
    assert repo_cls.return_value.search_by_cosine.call_args.kwargs["player_id"] is None
    assert repo_cls.return_value.search_by_cosine.call_args.kwargs["game_date"] == "2026-08-20"


def test_hybrid_retriever_keeps_cross_source_rows_distinct() -> None:
    """Test that equal row IDs from different source tables do not collide."""
    retriever = HybridRetriever(MagicMock())
    with (
        patch.object(
            retriever.bm25_engine,
            "search",
            return_value=[
                {"chunk_id": "game:1", "source_table": "game", "source_row_id": "1", "content": "game"},
            ],
        ),
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=True),
        patch("src.services.embedding_service.EmbeddingService") as embedding_cls,
        patch("src.repositories.vector_search_repository.VectorSearchRepository") as repo_cls,
    ):
        embedding_cls.return_value.get_embedding.return_value = [0.1]
        repo_cls.return_value.search_by_cosine.return_value = [
            {"id": 1, "source_table": "player_basic", "source_row_id": "1", "content": "player"}
        ]
        results = retriever.retrieve("기록", top_k=5)

    assert {result.chunk_id for result in results} == {"game:1", "player_basic:1"}


def test_hybrid_retriever_passes_extracted_metadata_to_sparse_fallback() -> None:
    """Test that pgvector fallback still receives KBO entity filters."""
    retriever = HybridRetriever(MagicMock())
    with (
        patch.object(retriever.bm25_engine, "search", return_value=[]) as sparse_search,
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=False),
    ):
        retriever.retrieve("잠실 야구장 주차장 요금", top_k=3)

    filters = sparse_search.call_args.kwargs["filters"]
    assert filters["stadium"] == "잠실"
    assert filters["document_type"] == "stadium_facility"
    assert "player_name" not in filters


def test_hybrid_retriever_does_not_filter_on_unresolved_player_candidate() -> None:
    """Avoid treating ordinary query words as strict player-name filters."""
    retriever = HybridRetriever(MagicMock())
    with (
        patch.object(retriever.bm25_engine, "search", return_value=[]) as sparse_search,
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=False),
    ):
        retriever.retrieve("한화와 KT의 2026년 경기 결과", top_k=3)

    filters = sparse_search.call_args.kwargs["filters"]
    assert filters["team_id"] == "KT"
    assert "player_name" not in filters


def test_hybrid_retriever_omits_unreliable_game_team_and_season_filters() -> None:
    """Do not filter games by fields that game chunks do not populate reliably."""
    retriever = HybridRetriever(MagicMock())
    with (
        patch.object(retriever.bm25_engine, "search", return_value=[]) as sparse_search,
        patch("src.services.hybrid_retriever.is_pgvector_available", return_value=False),
    ):
        retriever.retrieve(
            "2026년 한화와 KIA 경기 결과",
            top_k=3,
            filters={"source_table": "game"},
        )

    filters = sparse_search.call_args.kwargs["filters"]
    assert "team_id" not in filters
    assert "season_year" not in filters
