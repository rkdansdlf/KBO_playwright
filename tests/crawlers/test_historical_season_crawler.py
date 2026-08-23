"""Unit tests for HistoricalSeasonCrawler."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.historical_season_crawler import HistoricalSeasonCrawler
from src.db.engine import SessionLocal, init_db


@pytest.fixture(autouse=True)
def _setup_db():
    init_db()


@pytest.mark.asyncio
async def test_crawl_and_save_season_mock() -> None:
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

        with SessionLocal() as session:
            summary = await crawler.crawl_and_save_season(session, 1982, dry_run=False)

            assert summary.season == 1982
            assert summary.games_found == 2
            assert summary.games_saved == 2
            assert summary.source_name == "kbo_official_schedule"
            assert summary.provenance_verified is True


@pytest.mark.asyncio
async def test_crawl_and_save_season_marks_robots_limited_source() -> None:
    crawler = HistoricalSeasonCrawler(request_delay=0.1)

    with (
        patch.object(crawler.schedule_crawler, "crawl_season", new_callable=AsyncMock, return_value=[]),
        patch.object(crawler.schedule_crawler, "get_last_failure_reason", return_value="kbo_robots_blocked"),
    ):
        with SessionLocal() as session:
            summary = await crawler.crawl_and_save_season(session, 1982, dry_run=False)

    assert summary.games_found == 0
    assert summary.games_saved == 0
    assert summary.source_name == "kbo_official_schedule:kbo_robots_blocked"
    assert summary.provenance_verified is False
