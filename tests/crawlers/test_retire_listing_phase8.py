from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.retire.listing import RetiredPlayerListingCrawler


@pytest.fixture
def crawler():
    return RetiredPlayerListingCrawler(request_delay=0.01)


class TestExtractIdsEdgeCases:
    def test_empty_hitters_empty_pitchers(self, crawler):
        result = crawler._extract_ids({"hitters": [], "pitchers": []})
        assert result == set()

    def test_missing_hitters_key(self, crawler):
        result = crawler._extract_ids({"pitchers": [{"player_id": "20001"}]})
        assert result == {"20001"}

    def test_missing_pitchers_key(self, crawler):
        result = crawler._extract_ids({"hitters": [{"player_id": "10001"}]})
        assert result == {"10001"}

    def test_empty_dict(self, crawler):
        result = crawler._extract_ids({})
        assert result == set()

    def test_duplicate_ids(self, crawler):
        data = {
            "hitters": [{"player_id": "10001"}],
            "pitchers": [{"player_id": "10001"}],
        }
        result = crawler._extract_ids(data)
        assert result == {"10001"}

    def test_mixed_valid_invalid(self, crawler):
        data = {
            "hitters": [{"player_id": "10001"}, {"player_id": None}, {"name": "no_id"}],
            "pitchers": [{"player_id": "20001"}, {"player_id": ""}],
        }
        result = crawler._extract_ids(data)
        assert result == {"10001", "20001"}


class TestCollectHistoricalPlayerIds:
    @pytest.mark.asyncio
    async def test_single_season(self, crawler):
        with patch.object(crawler, "collect_player_ids_for_year", new=AsyncMock(return_value={"10001": "Kim"})):
            result = await crawler.collect_historical_player_ids([2025])
            assert result == {"10001": "Kim"}

    @pytest.mark.asyncio
    async def test_overlapping_players(self, crawler):
        year_data = {
            2020: {"10001": "Kim", "10002": "Lee"},
            2021: {"10002": "Lee", "10003": "Park"},
            2022: {"10001": "Kim", "10003": "Park", "10004": "Choi"},
        }

        async def mock_collect(year):
            return year_data.get(year, {})

        with patch.object(crawler, "collect_player_ids_for_year", new=mock_collect):
            result = await crawler.collect_historical_player_ids([2020, 2021, 2022])
            assert result == {"10001": "Kim", "10002": "Lee", "10003": "Park", "10004": "Choi"}

    @pytest.mark.asyncio
    async def test_exception_in_year_returns_empty(self, crawler):
        async def mock_collect(year):
            if year == 2021:
                raise RuntimeError("Network error")
            return {"10001": "Kim"}

        with patch.object(crawler, "collect_player_ids_for_year", new=mock_collect):
            result = await crawler.collect_historical_player_ids([2020, 2021, 2022])
            assert "10001" in result


class TestDetermineInactivePlayerIds:
    @pytest.mark.asyncio
    async def test_empty_historical(self, crawler):
        async def mock_historical(seasons):
            return {}

        async def mock_active(year):
            return {"10001": "Kim"}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_active),
        ):
            result = await crawler.determine_inactive_player_ids(2020, 2023, 2025)
            assert result == set()

    @pytest.mark.asyncio
    async def test_empty_active(self, crawler):
        async def mock_historical(seasons):
            return {"10001": "Kim", "10002": "Lee"}

        async def mock_active(year):
            return {}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_active),
        ):
            result = await crawler.determine_inactive_player_ids(2020, 2023, 2025)
            assert result == {"10001", "10002"}

    @pytest.mark.asyncio
    async def test_partial_overlap(self, crawler):
        async def mock_historical(seasons):
            return {"10001": "Kim", "10002": "Lee", "10003": "Park"}

        async def mock_active(year):
            return {"10001": "Kim", "10003": "Park", "10004": "Choi"}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_active),
        ):
            result = await crawler.determine_inactive_player_ids(2020, 2023, 2025)
            assert result == {"10002"}

    @pytest.mark.asyncio
    async def test_same_start_end_year(self, crawler):
        async def mock_historical(seasons):
            return {"10001": "Kim"}

        async def mock_active(year):
            return {}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_active),
        ):
            result = await crawler.determine_inactive_player_ids(2020, 2020, 2025)
            assert result == {"10001"}

    @pytest.mark.asyncio
    async def test_historical_ids_filtered_for_empty_strings(self, crawler):
        async def mock_historical(seasons):
            return {"": "Empty", "10001": "Kim"}

        async def mock_active(year):
            return {"10001": "Kim"}

        with (
            patch.object(crawler, "collect_historical_player_ids", new=mock_historical),
            patch.object(crawler, "collect_player_ids_for_year", new=mock_active),
        ):
            result = await crawler.determine_inactive_player_ids(2020, 2023, 2025)
            assert "" not in result
            assert result == set()
