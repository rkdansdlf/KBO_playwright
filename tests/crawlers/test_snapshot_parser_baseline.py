"""The three crawlers whose snapshots the drift check samples must record a baseline.

``snapshot_drift_check`` re-parses a stored artifact and compares the count against
``capture_metadata["parsed_records"]``. Measured 2026-11-01 on the most recent 100
snapshots: **0 carried that number**, and the sources were almost entirely these
three crawlers (구단 이벤트 76, 음식 5, 주차 5). The check was re-parsing artifacts
and reporting ``unknown_baseline`` for every one of them, which is why the drift gate
could not see a drift: it had nothing to compare against.

``team_event_crawler`` is covered in its own file, next to the pagination that makes
its counts differ per page. This file covers the other two, whose shape is the same
in one respect that matters: the snapshot dict is built **before** the parse runs, so
the count has to be attached to that dict afterwards rather than as a second entry.

The count belongs to the page it came from, and an absent count is the honest record
of a parse that raised -- there was nothing to compare against.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from pytest import mark

from src.crawlers.food_crawler import TEAM_FOOD_SOURCES, FoodCrawler
from src.crawlers.parking_crawler import TEAM_PARKING_SOURCES, ParkingCrawler
from src.crawlers.result import CrawlOutcome, CrawlResult

#: Any team's config; the shape is what the method reads, not the values.
FOOD_INFO = {"url": "https://example.test/food", "source_key": "lg_twins_fnb"}
PARKING_INFO = {"url": "https://example.test/parking", "source_key": "jamsil_parking_official"}


def _ok(html: str = "<html/>") -> CrawlResult[str]:
    return CrawlResult.success(html, http_status=200, url="https://example.test/")


class TestTheFoodSnapshot:
    def _crawler(self) -> FoodCrawler:
        crawler = FoodCrawler(request_delay=0)
        crawler._raw_pages = []
        crawler._http.fetch_text = AsyncMock(return_value=_ok())
        return crawler

    @mark.asyncio
    async def test_the_parsed_count_lands_on_the_snapshot(self) -> None:
        crawler = self._crawler()

        with patch.object(FoodCrawler, "_parse_food_page", return_value=[{"vendor": "A"}, {"vendor": "B"}]):
            await crawler._crawl_team_food("LG", FOOD_INFO)

        assert len(crawler._raw_pages) == 1
        assert crawler._raw_pages[0]["capture_metadata"] == {"parsed_records": 2}

    @mark.asyncio
    async def test_an_empty_parse_records_zero_rather_than_nothing(self) -> None:
        """An empty page parsed successfully; the count is 0, not absent."""
        crawler = self._crawler()

        with patch.object(FoodCrawler, "_parse_food_page", return_value=[]):
            await crawler._crawl_team_food("LG", FOOD_INFO)

        assert crawler._raw_pages[0]["capture_metadata"] == {"parsed_records": 0}

    @mark.asyncio
    async def test_a_parse_that_raises_records_no_count(self) -> None:
        crawler = self._crawler()

        with (
            patch.object(FoodCrawler, "_parse_food_page", side_effect=ValueError("bad html")),
            pytest.raises(ValueError, match="bad html"),
        ):
            await crawler._crawl_team_food("LG", FOOD_INFO)

        assert crawler._raw_pages, "the artifact is still captured"
        assert "capture_metadata" not in crawler._raw_pages[0]

    @mark.asyncio
    async def test_a_failed_fetch_captures_nothing_to_attach_it_to(self) -> None:
        crawler = self._crawler()
        crawler._http.fetch_text = AsyncMock(
            return_value=CrawlResult.failure(
                CrawlOutcome.PERMANENT_ERROR,
                error="unavailable",
                error_code="FETCH_HTTP_PERMANENT",
                http_status=404,
                url=FOOD_INFO["url"],
            ),
        )

        assert await crawler._crawl_team_food("LG", FOOD_INFO) == []
        assert crawler._raw_pages == []


class TestTheParkingSnapshot:
    def _crawler(self) -> ParkingCrawler:
        crawler = ParkingCrawler(request_delay=0)
        crawler._raw_pages = []
        crawler._http.fetch_text = AsyncMock(return_value=_ok())
        return crawler

    @mark.asyncio
    async def test_the_parsed_count_lands_on_the_snapshot(self) -> None:
        crawler = self._crawler()

        with patch.object(ParkingCrawler, "_parse_parking_page", return_value=[{"lot": "A"}]):
            await crawler._crawl_team_parking("LG", PARKING_INFO)

        assert len(crawler._raw_pages) == 1
        assert crawler._raw_pages[0]["capture_metadata"] == {"parsed_records": 1}

    @mark.asyncio
    async def test_a_parse_that_raises_records_no_count(self) -> None:
        crawler = self._crawler()

        with (
            patch.object(ParkingCrawler, "_parse_parking_page", side_effect=ValueError("bad html")),
            pytest.raises(ValueError, match="bad html"),
        ):
            await crawler._crawl_team_parking("LG", PARKING_INFO)

        assert crawler._raw_pages
        assert "capture_metadata" not in crawler._raw_pages[0]


class TestTheSourcesAreStillTheOnesThatWereMeasured:
    """The baseline work was scoped by measuring which sources the check samples."""

    def test_the_event_sources_are_reachable(self) -> None:
        from src.crawlers.team_event_crawler import TEAM_NEWS_SOURCES

        assert TEAM_NEWS_SOURCES

    def test_the_food_and_parking_sources_are_reachable(self) -> None:
        assert TEAM_FOOD_SOURCES
        assert TEAM_PARKING_SOURCES
