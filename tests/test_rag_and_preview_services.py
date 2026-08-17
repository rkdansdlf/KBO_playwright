"""Tests for RagSearchEngine and GamePreviewGenerator services."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.rag_chunk import RagChunk
from src.services.game_preview_generator import GamePreviewGenerator
from src.services.rag_search_engine import RagSearchEngine

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


def test_postgresql_search_uses_bounded_tsvector_candidates() -> None:
    """Use the indexed PostgreSQL lexical path instead of loading every match."""
    session = MagicMock()
    session.get_bind.return_value = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    session.execute.return_value.scalars.return_value.all.return_value = []

    RagSearchEngine(session).search("올스타전", top_k=5)

    statement = session.execute.call_args.args[0]
    compiled = str(statement.compile(dialect=postgresql.dialect()))
    assert "to_tsvector" in compiled
    assert "LIMIT" in compiled


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
