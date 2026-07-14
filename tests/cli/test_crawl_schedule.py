from __future__ import annotations

from argparse import Namespace
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from src.cli.crawl_schedule import _crawl_upcoming_months, crawl_schedule, main, parse_months


class TestCrawlScheduleCLI:
    def test_main_default_args(self):
        with (
            patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler,
            patch("src.cli.crawl_schedule.save_schedule_games") as mock_save,
        ):
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--year", "2025"])

            MockCrawler.assert_called_once_with(request_delay=1.2)
            mock_instance.crawl_season.assert_called_once()
            mock_save.assert_called_once_with([])

    def test_main_custom_delay(self):
        with (
            patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler,
            patch("src.cli.crawl_schedule.save_schedule_games") as mock_save,
        ):
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--year", "2025", "--delay", "0.5"])

            MockCrawler.assert_called_once_with(request_delay=0.5)

    def test_main_upcoming(self):
        with (
            patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler,
            patch("src.cli.crawl_schedule.save_schedule_games") as mock_save,
        ):
            mock_instance = MagicMock()
            mock_instance.crawl_schedule = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--upcoming"])

            mock_instance.crawl_schedule.assert_called()
            mock_save.assert_called()

    async def test_crawl_schedule_parses_months_and_saves_crawled_games(self):
        args = Namespace(year=2025, months="3-4,6", delay=0.25, upcoming=False)
        crawler = MagicMock()
        crawler.crawl_season = AsyncMock(return_value=[{"game_id": "G1"}])
        result = MagicMock(saved=1, failed=0)

        with (
            patch("src.cli.crawl_schedule.ScheduleCrawler", return_value=crawler),
            patch("src.cli.crawl_schedule.save_schedule_games", return_value=result) as save_games,
        ):
            await crawl_schedule(args)

        crawler.crawl_season.assert_awaited_once_with(2025, [3, 4, 6])
        save_games.assert_called_once_with([{"game_id": "G1"}])

    async def test_upcoming_uses_explicit_year_and_months(self):
        args = Namespace(year=2025, months="3, 5", delay=0.25, upcoming=True)
        crawler = MagicMock()
        crawler.crawl_schedule = AsyncMock(side_effect=[[{"game_id": "G1"}], [{"game_id": "G2"}]])

        with (
            patch("src.cli.crawl_schedule.ScheduleCrawler", return_value=crawler),
            patch(
                "src.cli.crawl_schedule.save_schedule_games", side_effect=[MagicMock(saved=2), MagicMock(saved=3)]
            ) as save_games,
        ):
            await _crawl_upcoming_months(args)

        crawler.crawl_schedule.assert_has_awaits([call(2025, 3), call(2025, 5)])
        assert save_games.call_args_list == [call([{"game_id": "G1"}]), call([{"game_id": "G2"}])]


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
