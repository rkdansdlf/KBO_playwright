from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.retire.listing import RetiredPlayerListingCrawler


@pytest.fixture
def crawler():
    return RetiredPlayerListingCrawler(request_delay=0.01)


class TestRetiredPlayerListingCrawler:
    def test_extract_ids_empty(self, crawler):
        result = crawler._extract_ids({})
        assert result == set()

    def test_extract_ids_with_hitters(self, crawler):
        data = {
            "hitters": [{"player_id": "10001"}, {"player_id": "10002"}],
            "pitchers": [],
        }
        result = crawler._extract_ids(data)
        assert result == {"10001", "10002"}

    def test_extract_ids_with_both(self, crawler):
        data = {
            "hitters": [{"player_id": "10001"}],
            "pitchers": [{"player_id": "20001"}, {"player_id": "20002"}],
        }
        result = crawler._extract_ids(data)
        assert result == {"10001", "20001", "20002"}

    def test_extract_ids_skips_missing_player_id(self, crawler):
        data = {
            "hitters": [{"player_id": None}, {"name": "foo"}],
            "pitchers": [],
        }
        result = crawler._extract_ids(data)
        assert result == set()

    def test_extract_ids_with_extra_keys(self, crawler):
        data = {
            "hitters": [{"player_id": "10001", "name": "Kim", "team": "LG"}],
            "pitchers": [{"player_id": "20001"}],
        }
        result = crawler._extract_ids(data)
        assert result == {"10001", "20001"}

    @pytest.mark.asyncio
    async def test_determine_inactive_player_ids_validates_range(self, crawler):
        with pytest.raises(ValueError, match="start_year must be <= end_year"):
            await crawler.determine_inactive_player_ids(start_year=2025, end_year=2020, active_year=2025)

    @pytest.mark.asyncio
    async def test_collect_player_ids_for_year_uses_pool(self, crawler):
        """Verify pool is created and released for a single year."""
        mock_page = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value={})

        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock(return_value=mock_page)
        mock_pool.start = AsyncMock()
        mock_pool.close = AsyncMock()

        with (
            patch.object(crawler, "_crawl_record_page_ids_with_teams", new=AsyncMock(return_value={})),
        ):
            crawler.pool = mock_pool
            result = await crawler.collect_player_ids_for_year(2025)

            assert result == {}

    @pytest.mark.asyncio
    async def test_collect_historical_player_ids_empty(self, crawler):
        with patch.object(crawler, "collect_player_ids_for_year", new=AsyncMock(return_value={})):
            result = await crawler.collect_historical_player_ids([])
            assert result == {}

    @pytest.mark.asyncio
    async def test_collect_historical_player_ids_multiple_years(self, crawler):
        year_data = {
            2020: {"10001": "Kim", "10002": "Lee"},
            2021: {"10002": "Lee", "10003": "Park"},
        }

        async def mock_collect(year):
            return year_data.get(year, {})

        with patch.object(crawler, "collect_player_ids_for_year", new=mock_collect):
            result = await crawler.collect_historical_player_ids([2020, 2021])
            assert result == {"10001": "Kim", "10002": "Lee", "10003": "Park"}

    @pytest.mark.asyncio
    async def test_determine_inactive_player_ids_basic(self, crawler):
        async def mock_collect_historical(seasons):
            return {"10001": "Kim", "10002": "Lee", "10003": "Park"}

        async def mock_collect_active(year):
            return {"10001": "Kim", "10003": "Park"}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_collect_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_collect_active),
        ):
            result = await crawler.determine_inactive_player_ids(
                start_year=2020,
                end_year=2023,
                active_year=2025,
            )
            assert result == {"10002"}

    @pytest.mark.asyncio
    async def test_determine_inactive_player_ids_all_active(self, crawler):
        async def mock_collect_historical(seasons):
            return {"10001": "Kim", "10002": "Lee"}

        async def mock_collect_active(year):
            return {"10001": "Kim", "10002": "Lee"}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_collect_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_collect_active),
        ):
            result = await crawler.determine_inactive_player_ids(
                start_year=2020,
                end_year=2023,
                active_year=2025,
            )
            assert result == set()

    @pytest.mark.asyncio
    async def test_determine_inactive_player_ids_all_inactive(self, crawler):
        async def mock_collect_historical(seasons):
            return {"10001": "Kim", "10002": "Lee"}

        async def mock_collect_active(year):
            return {}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_collect_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_collect_active),
        ):
            result = await crawler.determine_inactive_player_ids(
                start_year=2020,
                end_year=2023,
                active_year=2025,
            )
            assert result == {"10001", "10002"}
