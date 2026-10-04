from __future__ import annotations

from argparse import Namespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.cli.crawl_schedule import _crawl_upcoming_months, crawl_schedule, main, parse_months


class TestCrawlScheduleCLI:
    def test_main_default_args(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            main(["--year", "2025"])

            MockCrawler.assert_called_once_with(request_delay=1.2)
            mock_instance.crawl_season.assert_called_once()
            # Persistence now happens inside each month's ledger row.
            assert mock_instance.crawl_season.await_args.kwargs == {"save": True}

    def test_main_custom_delay(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            main(["--year", "2025", "--delay", "0.5"])

            MockCrawler.assert_called_once_with(request_delay=0.5)

    def test_main_upcoming(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_schedule = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance

            main(["--upcoming"])

            mock_instance.crawl_schedule.assert_called()
            assert all(call_.kwargs == {"save": True} for call_ in mock_instance.crawl_schedule.await_args_list)

    async def test_crawl_schedule_parses_months_and_delegates_persistence(self):
        args = Namespace(year=2025, months="3-4,6", delay=0.25, upcoming=False)
        crawler = MagicMock()
        crawler.crawl_season = AsyncMock(return_value=[{"game_id": "G1"}])

        with patch("src.cli.crawl_schedule.ScheduleCrawler", return_value=crawler):
            await crawl_schedule(args)

        crawler.crawl_season.assert_awaited_once_with(2025, [3, 4, 6], save=True)

    async def test_upcoming_uses_explicit_year_and_months(self):
        args = Namespace(year=2025, months="3, 5", delay=0.25, upcoming=True)
        crawler = MagicMock()
        crawler.crawl_schedule = AsyncMock(side_effect=[[{"game_id": "G1"}], [{"game_id": "G2"}]])

        with patch("src.cli.crawl_schedule.ScheduleCrawler", return_value=crawler):
            await _crawl_upcoming_months(args)

        crawler.crawl_schedule.assert_has_awaits([call(2025, 3, save=True), call(2025, 5, save=True)])


class TestParseMonths:
    def test_defaults_to_regular_season_months(self):
        assert parse_months(None) == list(range(3, 11))

    def test_expands_ranges_and_deduplicates_months(self):
        assert parse_months("3-5, 4, 8") == [3, 4, 5, 8]

    @pytest.mark.parametrize(
        ("months", "expected"),
        [("bad,4", [4]), ("3-invalid,5", [5])],
    )
    def test_ignores_invalid_month_values(self, months, expected):
        assert parse_months(months) == expected
