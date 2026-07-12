from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.crawlers.daily_roster_crawler import DailyRosterCrawler


class TestCleanCategory:
    def setup_method(self):
        self.crawler = DailyRosterCrawler()

    def test_removes_parenthetical_count(self):
        assert self.crawler._clean_category("투수 (14명)") == "투수"

    def test_no_parenthesis_returns_unchanged(self):
        assert self.crawler._clean_category("포수") == "포수"

    def test_empty_string(self):
        assert self.crawler._clean_category("") == ""


class TestDailyRosterCrawler:
    @pytest.mark.asyncio
    async def test_extract_table_normalizes_records(self, monkeypatch):
        page = AsyncMock()
        page.evaluate.return_value = [
            {
                "player_id": "123",
                "player_name": "홍길동",
                "back_number": "7",
                "category": "투수 (14명)",
            },
        ]
        crawler = DailyRosterCrawler()
        monkeypatch.setattr(
            "src.crawlers.daily_roster_crawler.resolve_team_code",
            lambda code, season: "SSG" if (code, season) == ("SK", 2025) else None,
        )

        records = await crawler._extract_table(page, "SK", date(2025, 5, 1))

        assert records == [
            {
                "roster_date": date(2025, 5, 1),
                "team_code": "SSG",
                "player_id": 123,
                "player_name": "홍길동",
                "position": "투수",
                "back_number": "7",
            },
        ]

    @pytest.mark.asyncio
    async def test_extract_table_returns_empty_when_page_has_no_tables(self):
        page = AsyncMock()
        page.evaluate.return_value = [{"status": "no_tables"}]

        records = await DailyRosterCrawler()._extract_table(page, "LG", date(2025, 5, 1))

        assert records == []

    @pytest.mark.asyncio
    async def test_crawl_date_range_saves_sync_results_and_releases_injected_pool(self):
        page = AsyncMock()
        pool = MagicMock()
        pool.start = AsyncMock()
        pool.acquire = AsyncMock(return_value=page)
        pool.release = AsyncMock()
        pool.close = AsyncMock()
        crawler = DailyRosterCrawler(pool=pool)
        crawler._crawl_date = AsyncMock(return_value=[{"player_id": 1}])
        save_callback = MagicMock()

        records = await crawler.crawl_date_range("2025-05-01", "2025-05-02", save_callback)

        assert records == [{"player_id": 1}, {"player_id": 1}]
        assert crawler._crawl_date.await_count == 2
        assert save_callback.call_count == 2
        pool.start.assert_awaited_once()
        pool.release.assert_awaited_once_with(page)
        pool.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_crawl_date_continues_after_a_team_error(self):
        class _ResponseWaiter:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *_args):
                return False

        page = MagicMock()
        page.expect_response.return_value = _ResponseWaiter()
        page.evaluate = AsyncMock(return_value="2025.05.01")
        page.wait_for_timeout = AsyncMock()
        crawler = DailyRosterCrawler()

        async def _extract(_page, team_code, _roster_date):
            if team_code == "LG":
                raise RuntimeError("LG failed")
            if team_code == "HH":
                return [{"player_id": 2}]
            return []

        crawler._extract_table = AsyncMock(side_effect=_extract)

        records = await crawler._crawl_date(page, date(2025, 5, 1))

        assert records == [{"player_id": 2}]
        assert crawler._extract_table.await_count == 10
