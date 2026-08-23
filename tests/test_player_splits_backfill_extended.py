"""Tests for player splits backfill crawler and CLI."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

import src.models
from src.crawlers.player_splits_crawler import PlayerSplitsCrawler
from src.models.base import Base
from src.repositories.player_splits_repository import PlayerSplitsRepository
from src.services.rag_indexer import RagKnowledgeIndexer


class FakeEmbeddingService:
    """Offline provider returning a fixed-size vector per text."""

    def get_embeddings_batch(self, texts: list[str]) -> list[list[float]]:
        return [[0.1, 0.2, 0.3] for _ in texts]


if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def db_session() -> Generator[Session, None, None]:
    """In-memory SQLite session fixture."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    yield session
    session.close()


@pytest.mark.asyncio
async def test_crawl_all_splits_and_reindex(db_session: Session) -> None:
    """Test crawl_all_splits and RAG knowledge reindexing."""

    async def _mock_splits(season: int = 2026, split_type: str = "scoring_position"):
        return [
            {
                "season": season,
                "player_id": f"P1_{split_type}",
                "player_name": "김도영",
                "team_code": "KIA",
                "split_type": split_type,
                "split_key": split_type,
                "avg": 0.375,
                "ops": 1.050,
            }
        ]

    crawler = PlayerSplitsCrawler()
    with patch.object(crawler, "crawl_player_splits", side_effect=_mock_splits):
        results = await crawler.crawl_all_splits(season=2026)
        assert len(results) == 3  # 3 categories x 1 record

        repo = PlayerSplitsRepository(db_session)
        for item in results:
            repo.save_splits_entry(item)

        indexer = RagKnowledgeIndexer(db_session, embedding_service=FakeEmbeddingService())
        count = indexer.index_player_splits(season=2026)
        assert count == 3
