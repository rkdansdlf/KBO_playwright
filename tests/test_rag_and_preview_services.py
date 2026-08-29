"""Tests for RagSearchEngine and GamePreviewGenerator services."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import oracle, postgresql
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.services.game_preview_generator import GamePreviewGenerator
from src.services.rag_search_engine import (
    RagSearchEngine,
    _query_season_year,
    _resolved_search_filters,
    _search_keywords,
)

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


def test_rag_search_engine(db_session: Session) -> None:
    """Test RAG search engine keyword lookup and Q&A synthesis."""
    db_session.add(
        RagChunk(
            source_table="press_release",
            source_row_id="https://example.com/1",
            title="KBO 올스타전 개최 발표",
            content="2026 KBO 올스타전이 잠실구장에서 열립니다.",
            meta={"category": "보도자료"},
        )
    )
    db_session.add(
        RagChunk(
            source_table="player_milestone",
            source_row_id="https://example.com/2",
            title="최형우 1600타점 달성 임박",
            content="최형우 선수가 1600타점 달성까지 2타점 남았습니다.",
            meta={"category": "대기록"},
        )
    )
    db_session.commit()

    engine = RagSearchEngine(db_session)
    results = engine.search("올스타전")
    assert len(results) == 1
    assert "올스타전" in results[0]["title"]

    qa = engine.answer_question("최형우 타점")
    assert qa["chunk_count"] == 1
    assert "https://example.com/2" in qa["sources"]


def test_rag_search_engine_applies_game_date_metadata_filter(db_session: Session) -> None:
    """Apply game-date filters on the portable sparse-search path."""
    db_session.add_all(
        [
            RagChunk(
                source_table="game_play_by_play",
                source_row_id="target",
                title="김현수 타석 결과",
                content="2015년 경기 타석 결과",
                meta={"game_date": "2015-10-19", "document_type": "game_play_by_play"},
            ),
            RagChunk(
                source_table="game_play_by_play",
                source_row_id="other",
                title="김현수 타석 결과",
                content="2015년 경기 타석 결과",
                meta={"game_date": "2015-10-18", "document_type": "game_play_by_play"},
            ),
        ]
    )
    db_session.commit()

    results = RagSearchEngine(db_session).search(
        "김현수 타석 결과",
        filters={"source_table": "game_play_by_play", "game_date": "2015-10-19"},
    )

    assert [result["chunk_id"] for result in results] == ["game_play_by_play:target"]


def test_rag_search_engine_extracts_season_from_query(db_session: Session) -> None:
    """Use an explicit year in a query to scope historical retrieval."""
    db_session.add_all(
        [
            RagChunk(
                source_table="game",
                source_row_id="19880402OBLT0",
                title="1988-04-02 OB vs LT",
                content="1988-04-02 KBO 경기: OB 4 - LT 0",
                season_year=1988,
            ),
            RagChunk(
                source_table="player_basic",
                source_row_id="ob-lt-profile",
                title="OB LT 선수 프로필",
                content="OB와 LT 선수 정보",
            ),
        ],
    )
    db_session.commit()

    results = RagSearchEngine(db_session).search("1988-04-02 OB 4 LT 0")

    assert results[0]["chunk_id"] == "game:19880402OBLT0"


def test_query_season_year_rejects_out_of_range_years() -> None:
    """Accept only years covered by the KBO season contract."""
    assert _query_season_year("1988-04-02") == 1988
    assert _query_season_year("2101 season") is None


def test_game_date_filter_does_not_add_redundant_season_filter() -> None:
    filters = {"source_table": "game", "game_date": "2026-07-21"}

    assert _resolved_search_filters("2026년 7월 21일 경기", filters) == filters


def test_date_in_query_adds_game_date_for_date_scoped_source() -> None:
    filters = {"source_table": "game"}

    assert _resolved_search_filters("2026년 7월 21일 경기", filters) == {
        "source_table": "game",
        "game_date": "2026-07-21",
    }


def test_non_date_scoped_source_keeps_season_year_inference() -> None:
    filters = {"source_table": "player_movements"}

    assert _resolved_search_filters("2018년 12월 트레이드 내용", filters) == {
        "source_table": "player_movements",
        "season_year": 2018,
    }


def test_postgresql_search_uses_bounded_tsvector_candidates() -> None:
    """Use the indexed PostgreSQL lexical path instead of loading every match."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    session.execute.return_value.scalars.return_value.all.return_value = [
        RagChunk(
            id=1,
            source_table="press_release",
            source_row_id="1",
            title="올스타전",
            content="올스타전 개최",
        )
    ]

    RagSearchEngine(session).search("올스타전", top_k=5)

    statement = session.execute.call_args_list[0].args[0]
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "to_tsvector" in compiled
    assert "LIMIT" in compiled


def test_oracle_search_uses_bounded_candidates_without_vectors(monkeypatch) -> None:
    """Bound Oracle sparse candidates and avoid transferring dense embeddings."""
    monkeypatch.setenv("RAG_ORACLE_SPARSE_MODE", "legacy")
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    session.execute.return_value.scalars.return_value.all.return_value = []

    RagSearchEngine(session).search("올스타전", top_k=5)

    statement = session.execute.call_args_list[0].args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "FETCH FIRST" in compiled
    assert "instr" in compiled.lower()
    assert "CASE" in compiled
    assert "embedding_vector" not in compiled


def test_oracle_search_can_skip_candidate_sorting(monkeypatch) -> None:
    """Support the faster sparse candidate path used by Oracle hybrid retrieval."""
    monkeypatch.setenv("RAG_ORACLE_SPARSE_MODE", "legacy")
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    session.execute.return_value.scalars.return_value.all.return_value = []

    RagSearchEngine(session).search("올스타전", top_k=5, oracle_ranked_candidates=False)

    statement = session.execute.call_args_list[0].args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "CASE" not in compiled
    assert "FETCH FIRST" in compiled


def test_oracle_term_mode_uses_postings_table(monkeypatch) -> None:
    """Select the feature-flagged postings path without touching CLOB search."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    session.scalar.return_value = 1
    session.execute.return_value.scalars.return_value.all.return_value = []
    monkeypatch.setenv("RAG_ORACLE_SPARSE_MODE", "terms")

    RagSearchEngine(session).search("OPS", top_k=5)

    statement = session.execute.call_args_list[0].args[0]
    compiled = str(statement.compile(dialect=oracle.dialect()))
    assert "rag_chunk_terms" in compiled.lower()
    assert "lower(rag_chunks.content)" not in compiled.lower()


def test_oracle_sparse_mode_defaults_to_terms(monkeypatch) -> None:
    """Route Oracle keyword search through the postings index without env setup."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="oracle"))
    monkeypatch.delenv("RAG_ORACLE_SPARSE_MODE", raising=False)

    with patch(
        "src.repositories.oracle_sparse_search_repository.OracleSparseSearchRepository.search_candidates",
        return_value=[],
    ) as repo_search:
        RagSearchEngine(session).search("OPS", top_k=5)

    repo_search.assert_called_once()


def test_postgresql_search_applies_game_date_metadata_filter() -> None:
    """Apply exact game-date filters before ranking sparse candidates."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    session.execute.return_value.scalars.return_value.all.return_value = []

    RagSearchEngine(session).search(
        "김현수 타석 결과",
        top_k=5,
        filters={"source_table": "game_play_by_play", "game_date": "2015-10-19"},
    )

    statement = session.execute.call_args_list[0].args[0]
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "rag_chunks.meta" in compiled
    compiled_params = statement.compile(dialect=postgresql.dialect()).params
    assert "game_play_by_play" in compiled_params.values()
    assert "2015-10-19" in compiled_params.values()


def test_search_keywords_remove_korean_particles() -> None:
    """Normalize attached Korean particles before lexical retrieval."""
    assert _search_keywords("김도영의 2026시즌 KT와") == ["김도영", "2026시즌", "KT"]


def test_search_keywords_split_punctuation_like_sparse_index() -> None:
    """Keep query tokenization aligned with the Oracle postings builder."""
    assert _search_keywords("1988-04-02 OB와 LT") == ["1988", "04", "02", "OB", "LT"]


def test_game_preview_generator(db_session: Session) -> None:
    """Test game preview generator combining milestones, notices, and RAG context."""
    db_session.add(
        PlayerMilestone(
            season=2026,
            player_id="P1",
            player_name="김도영",
            team_code="KIA",
            milestone_category="30-30 클럽",
            current_val=29,
            target_val=30,
            remaining_val=1,
            is_achieved=False,
        )
    )
    db_session.add(
        KboPressRelease(
            notice_id="N100",
            published_date=date(2026, 8, 9),
            category="공시",
            title="KBO 선수 등록 공시",
            source_url="https://example.com/notice",
        )
    )
    db_session.commit()

    generator = GamePreviewGenerator(db_session)
    preview = generator.generate_preview(away_team="LG", home_team="KIA", season=2026)

    assert preview["matchup"] == "LG vs KIA"
    assert len(preview["milestone_alerts"]) == 1
    assert preview["milestone_alerts"][0]["player_name"] == "김도영"
    assert len(preview["recent_notices"]) == 1
    assert "김도영" in preview["markdown_report"]
    assert "KBO 선수 등록 공시" in preview["markdown_report"]
