"""Tests for HybridRetriever and hybrid search API."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.rag_chunk import RagChunk
from src.services.hybrid_retriever import HybridRetriever

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


def test_hybrid_retriever(db_session: Session) -> None:
    """Test HybridRetriever functionality and RRF ranking."""
    db_session.add(
        RagChunk(
            source_table="press_release",
            source_row_id="https://example.com/1",
            title="KBO 올스타전 개최 발표",
            content="2026 KBO 올스타전이 잠실구장에서 개최됩니다.",
            meta={"category": "공시"},
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

    retriever = HybridRetriever(db_session)
    results = retriever.retrieve(query="올스타전", top_k=5)

    assert len(results) == 1
    assert "올스타전" in results[0].title
    assert results[0].rrf_score > 0
