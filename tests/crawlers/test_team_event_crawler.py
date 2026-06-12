from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import mark

from src.crawlers.team_event_crawler import TEAM_NEWS_SOURCES, TeamEventCrawler


@pytest.fixture
def crawler():
    return TeamEventCrawler(days_back=30)


class TestCrawlTeam:
    @mark.asyncio
    async def test_fetches_and_parses_events(self, crawler):
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><div>event</div></html>"
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__ = AsyncMock()

        with patch.object(crawler, "_raw_pages", []), \
             patch("src.crawlers.team_event_crawler.throttle.wait", AsyncMock()), \
             patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse:
            mock_parse.return_value = [
                {"team_id": "LG", "title": "Event 1", "source_url": "https://lg.com/1"},
                {"team_id": "LG", "title": "Event 2", "source_url": "https://lg.com/2"},
            ]
            config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

            with patch("httpx.AsyncClient", return_value=mock_client):
                result = await crawler._crawl_team("LG", config)

        assert len(result) == 2
        assert result[0]["title"] == "Event 1"

    @mark.asyncio
    async def test_deduplicates_events(self, crawler):
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html>data</html>"
        mock_client.get.return_value = mock_response
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__ = AsyncMock()

        with patch.object(crawler, "_raw_pages", []), \
             patch("src.crawlers.team_event_crawler.throttle.wait", AsyncMock()), \
             patch("src.crawlers.team_event_crawler.parse_team_events") as mock_parse:
            mock_parse.return_value = [
                {"team_id": "LG", "title": "Same Event", "source_url": "https://lg.com/1"},
                {"team_id": "LG", "title": "Same Event", "source_url": "https://lg.com/1"},
            ]
            config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

            with patch("httpx.AsyncClient", return_value=mock_client):
                result = await crawler._crawl_team("LG", config)

        assert len(result) == 1

    @mark.asyncio
    async def test_handles_http_error_gracefully(self, crawler):
        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("timeout")
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__ = AsyncMock()

        config = {"url": "https://lg.com/page={page}", "link_prefix": "https://lg.com"}

        with patch("httpx.AsyncClient", return_value=mock_client), \
             patch("src.crawlers.team_event_crawler.throttle.wait", AsyncMock()):
            result = await crawler._crawl_team("LG", config)

        assert result == []


class TestRun:
    @mark.asyncio
    async def test_iterates_all_teams(self, crawler):
        with patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])):
            result = await crawler.run(save=False)
        assert len(result) == len(TEAM_NEWS_SOURCES)

    @mark.asyncio
    async def test_filters_by_team(self, crawler):
        with patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])):
            result = await crawler.run(save=False, team_filter="LG")
        assert len(result) == 1

    @mark.asyncio
    async def test_saves_to_db(self, crawler):
        with patch.object(crawler, "_crawl_team", AsyncMock(return_value=[{"title": "event"}])), \
             patch.object(crawler, "_save_to_db") as mock_save:
            await crawler.run(save=True)
        mock_save.assert_called_once()


class TestSaveToDb:
    def test_saves_events_and_snapshots(self, crawler):
        with patch("src.crawlers.team_event_crawler.SessionLocal") as mock_sl, \
             patch("src.crawlers.team_event_crawler.save_raw_snapshots") as mock_snap, \
             patch("src.crawlers.team_event_crawler.TeamEventRepository") as mock_repo_cls:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_repo = MagicMock()
            mock_repo_cls.return_value = mock_repo
            mock_snap.return_value = 2

            crawler._raw_pages = [{"url": "test", "html": "<html/>"}]
            crawler._save_to_db([{"team_id": "LG", "title": "Event"}])

            mock_repo.save.assert_called_once()
            mock_session.commit.assert_called_once()
            assert crawler._raw_pages == []
