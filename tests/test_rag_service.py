"""RAG 서비스 유닛 테스트."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestRagResult:
    """RagResult 데이터 클래스 테스트."""

    def test_to_dict_returns_all_fields(self) -> None:
        from src.services.rag_service import RagResult

        result = RagResult(
            title="테스트 제목",
            content="테스트 내용",
            score=0.92,
            source_table="player_basic",
            source_row_id="12345",
            team_id="KIA",
            player_id="12345",
            season_year=2025,
            meta={"position": "투수"},
        )
        d = result.to_dict()

        assert d["title"] == "테스트 제목"
        assert d["content"] == "테스트 내용"
        assert d["score"] == 0.92
        assert d["source_table"] == "player_basic"
        assert d["team_id"] == "KIA"
        assert d["season_year"] == 2025
        assert d["meta"] == {"position": "투수"}

    def test_to_dict_defaults(self) -> None:
        from src.services.rag_service import RagResult

        result = RagResult(
            title=None,
            content="내용",
            score=0.5,
            source_table="game",
            source_row_id="20250601KIA0",
        )
        d = result.to_dict()

        assert d["title"] is None
        assert d["team_id"] is None
        assert d["player_id"] is None
        assert d["season_year"] is None
        assert d["meta"] == {}


class TestRagService:
    """RagService 검색 로직 테스트."""

    @patch("src.services.rag_service.is_pgvector_available", return_value=False)
    def test_search_raises_when_pgvector_unavailable(self, _mock: MagicMock) -> None:
        from src.services.rag_service import RagService

        service = RagService()
        with pytest.raises(RuntimeError, match="pgvector"):
            service.search("테스트 쿼리")

    @patch("src.services.rag_service.is_pgvector_available", return_value=True)
    @patch("src.services.rag_service.EmbeddingService")
    @patch("src.services.rag_service.VectorSearchRepository")
    def test_search_returns_results(
        self,
        mock_repo_cls: MagicMock,
        mock_embed_cls: MagicMock,
        _mock_pg: MagicMock,
    ) -> None:
        from src.services.rag_service import RagService

        mock_embed_inst = mock_embed_cls.return_value
        mock_embed_inst.get_embedding.return_value = [0.1] * 256

        mock_repo_inst = mock_repo_cls.return_value
        mock_repo_inst.search_by_cosine.return_value = [
            {
                "id": 1,
                "title": "류현진 (한화)",
                "content": "선수: 류현진, 팀: 한화, 포지션: 투수",
                "source_table": "player_basic",
                "source_row_id": "99999",
                "team_id": "HH",
                "player_id": "99999",
                "season_year": None,
                "score": 0.95,
                "meta": {},
            }
        ]

        service = RagService()
        results, timings = service.search("류현진 선수 정보", top_k=5)

        assert len(results) == 1
        assert results[0].title == "류현진 (한화)"
        assert results[0].score == 0.95
        assert results[0].source_table == "player_basic"
        assert "embedding_ms" in timings
        assert "search_ms" in timings

        mock_embed_inst.get_embedding.assert_called_once_with("류현진 선수 정보")
        mock_repo_inst.search_by_cosine.assert_called_once_with(
            query_vector=[0.1] * 256,
            top_k=5,
            team_id=None,
            season_year=None,
            source_table=None,
            league_type_code=None,
            document_type=None,
            game_date=None,
        )

    @patch("src.services.rag_service.is_pgvector_available", return_value=True)
    @patch("src.services.rag_service.EmbeddingService")
    @patch("src.services.rag_service.VectorSearchRepository")
    def test_search_with_filters(
        self,
        mock_repo_cls: MagicMock,
        mock_embed_cls: MagicMock,
        _mock_pg: MagicMock,
    ) -> None:
        from src.services.rag_service import RagService

        mock_embed_inst = mock_embed_cls.return_value
        mock_embed_inst.get_embedding.return_value = [0.0] * 256

        mock_repo_inst = mock_repo_cls.return_value
        mock_repo_inst.search_by_cosine.return_value = []

        service = RagService()
        results, _ = service.search(
            "KIA 선수",
            top_k=3,
            filters={"team_id": "KIA", "season_year": 2025},
        )

        assert results == []
        mock_repo_inst.search_by_cosine.assert_called_once_with(
            query_vector=[0.0] * 256,
            top_k=3,
            team_id="KIA",
            season_year=2025,
            source_table=None,
            league_type_code=None,
            document_type=None,
            game_date=None,
        )

    @patch("src.services.rag_service.is_pgvector_available", return_value=True)
    @patch("src.services.rag_service.EmbeddingService")
    @patch("src.services.rag_service.VectorSearchRepository")
    def test_search_returns_empty_on_no_results(
        self,
        mock_repo_cls: MagicMock,
        mock_embed_cls: MagicMock,
        _mock_pg: MagicMock,
    ) -> None:
        from src.services.rag_service import RagService

        mock_embed_inst = mock_embed_cls.return_value
        mock_embed_inst.get_embedding.return_value = [0.0] * 256

        mock_repo_inst = mock_repo_cls.return_value
        mock_repo_inst.search_by_cosine.return_value = []

        service = RagService()
        results, timings = service.search("없는 내용 쿼리")

        assert results == []
        assert timings["embedding_ms"] >= 0
        assert timings["search_ms"] >= 0
