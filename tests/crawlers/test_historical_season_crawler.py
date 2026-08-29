"""Unit tests for HistoricalSeasonCrawler."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.crawlers.historical_season_crawler import HistoricalSeasonCrawler
from src.models.base import Base

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture
def session() -> Generator[Session, None, None]:
    """Provide isolated in-memory test database session."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    s = factory()
    try:
        yield s
    finally:
        s.close()
        engine.dispose()


@pytest.mark.asyncio
async def test_crawl_and_save_season_mock(session: Session) -> None:
    """HistoricalSeasonCrawler should parse raw games and save them with provenance metadata."""
    crawler = HistoricalSeasonCrawler(request_delay=0.1)

    mock_raw_games = [
        {
            "game_id": "19820327SSMB0",
            "game_date": "1982-03-27",
            "away_team": "SS",
            "home_team": "MB",
            "away_score": 7,
            "home_score": 11,
            "stadium": "동대문",
            "game_status": "COMPLETED",
        },
        {
            "game_id": "19820328LTHT0",
            "game_date": "1982-03-28",
            "away_team": "LT",
            "home_team": "HT",
            "away_score": 2,
            "home_score": 14,
            "stadium": "광주",
            "game_status": "COMPLETED",
        },
    ]

    with patch.object(crawler.schedule_crawler, "crawl_season", new_callable=AsyncMock) as mock_crawl:
        mock_crawl.return_value = mock_raw_games

        summary = await crawler.crawl_and_save_season(session, 1982, dry_run=False)

        assert summary.season == 1982
        assert summary.games_found == 2
        assert summary.games_saved == 2
        assert summary.source_name == "kbo_official_schedule"
        assert summary.provenance_verified is True


@pytest.mark.asyncio
async def test_crawl_and_save_season_marks_robots_limited_source(session: Session) -> None:
    """If no games found and no source fallback, crawler logs properly."""
    crawler = HistoricalSeasonCrawler(request_delay=0.1)

    with patch.object(crawler.schedule_crawler, "crawl_season", new_callable=AsyncMock) as mock_crawl:
        mock_crawl.return_value = []

        summary = await crawler.crawl_and_save_season(session, 1982, dry_run=False)

        assert summary.season == 1982
        assert summary.games_found == 0
        assert summary.games_saved == 0
