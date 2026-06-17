from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark
from sqlalchemy.exc import SQLAlchemyError

from src.crawlers.award_crawler import AwardCrawler


@pytest.fixture
def crawler():
    return AwardCrawler()


@pytest.fixture
def mock_page():
    page = AsyncMock()
    page.evaluate = AsyncMock()
    return page


class TestCrawlPlayerPrize:
    @mark.asyncio
    async def test_returns_parsed_mvp_and_rookie(self, crawler, mock_page):
        mock_page.evaluate.return_value = [
            {"year": 2024, "award_type": "MVP", "category": None, "player_name": "Kim", "team_name": "LG"},
            {
                "year": 2024,
                "award_type": "Rookie of the Year",
                "category": None,
                "player_name": "Park",
                "team_name": "SS",
            },
        ]
        result = await crawler.crawl_player_prize(mock_page)
        assert len(result) == 2
        assert result[0]["award_type"] == "MVP"
        assert result[1]["award_type"] == "Rookie of the Year"

    @mark.asyncio
    async def test_empty_when_page_returns_empty(self, crawler, mock_page):
        mock_page.evaluate.return_value = []
        result = await crawler.crawl_player_prize(mock_page)
        assert result == []


class TestCrawlGoldenGlove:
    @mark.asyncio
    async def test_returns_golden_glove_entries(self, crawler, mock_page):
        mock_page.evaluate.return_value = [
            {"year": 2024, "award_type": "Golden Glove", "category": "P", "player_name": "A", "team_name": "LG"},
            {"year": 2024, "award_type": "Golden Glove", "category": "C", "player_name": "B", "team_name": "SS"},
        ]
        result = await crawler.crawl_golden_glove(mock_page)
        assert len(result) == 2
        assert result[0]["category"] == "P"
        assert result[1]["category"] == "C"


class TestCrawlDefensePrize:
    @mark.asyncio
    async def test_returns_defense_prize_entries(self, crawler, mock_page):
        mock_page.evaluate.return_value = [
            {"year": 2024, "award_type": "Defense Prize", "category": "LF", "player_name": "C", "team_name": "NC"},
        ]
        result = await crawler.crawl_defense_prize(mock_page)
        assert len(result) == 1
        assert result[0]["award_type"] == "Defense Prize"


class TestCrawlSeriesPrize:
    @mark.asyncio
    async def test_returns_series_prize_entries(self, crawler, mock_page):
        mock_page.evaluate.return_value = [
            {"year": 2024, "award_type": "All-Star MVP", "category": None, "player_name": "D", "team_name": "LG"},
            {"year": 2024, "award_type": "Korean Series MVP", "category": None, "player_name": "E", "team_name": "SS"},
        ]
        result = await crawler.crawl_series_prize(mock_page)
        assert len(result) == 2
        assert result[0]["award_type"] == "All-Star MVP"
        assert result[1]["award_type"] == "Korean Series MVP"


class TestSaveToDb:
    @patch("src.crawlers.award_crawler.SessionLocal")
    @patch("src.crawlers.award_crawler.AwardRepository")
    def test_saves_data(self, mock_repo_cls, mock_session_local, crawler):
        mock_session = MagicMock()
        mock_session_local.return_value = mock_session
        mock_repo = MagicMock()
        mock_repo_cls.return_value = mock_repo

        data = [{"year": 2024, "award_type": "MVP", "player_name": "Kim", "team_name": "LG"}]
        crawler.save_to_db(data)

        mock_repo.save_award.assert_called_once_with(data[0])
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

    @patch("src.crawlers.award_crawler.SessionLocal")
    @patch("src.crawlers.award_crawler.AwardRepository")
    def test_handles_duplicate_gracefully(self, mock_repo_cls, mock_session_local, crawler):
        mock_session = MagicMock()
        mock_session_local.return_value = mock_session
        mock_repo = MagicMock()
        mock_repo_cls.return_value = mock_repo
        mock_repo.save_award.side_effect = [None, SQLAlchemyError("duplicate")]

        data = [
            {"year": 2024, "award_type": "MVP", "player_name": "Kim", "team_name": "LG"},
            {"year": 2024, "award_type": "MVP", "player_name": "Kim", "team_name": "LG"},
        ]
        crawler.save_to_db(data)

        assert mock_repo.save_award.call_count == 2


class TestRun:
    @mark.asyncio
    @patch("src.crawlers.award_crawler.async_playwright")
    async def test_crawls_all_types_by_default(self, mock_async_playwright, crawler):
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=[])
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_async_playwright.return_value.__aenter__ = AsyncMock(return_value=mock_pw)
        mock_async_playwright.return_value.__aexit__ = AsyncMock()

        await crawler.run(award_types=["player_prize"], save=False)

        assert mock_page.evaluate.called
        mock_browser.close.assert_awaited_once()
