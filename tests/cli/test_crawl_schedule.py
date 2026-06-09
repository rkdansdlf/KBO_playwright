from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_schedule import main


class TestCrawlScheduleCLI:
    def test_main_default_args(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler, \
             patch("src.cli.crawl_schedule.save_schedule_games") as mock_save:
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--year", "2025"])

            MockCrawler.assert_called_once_with(request_delay=1.2)
            mock_instance.crawl_season.assert_called_once()
            mock_save.assert_called_once_with([])

    def test_main_custom_delay(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler, \
             patch("src.cli.crawl_schedule.save_schedule_games") as mock_save:
            mock_instance = MagicMock()
            mock_instance.crawl_season = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--year", "2025", "--delay", "0.5"])

            MockCrawler.assert_called_once_with(request_delay=0.5)

    def test_main_upcoming(self):
        with patch("src.cli.crawl_schedule.ScheduleCrawler") as MockCrawler, \
             patch("src.cli.crawl_schedule.save_schedule_games") as mock_save:
            mock_instance = MagicMock()
            mock_instance.crawl_schedule = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            mock_save.return_value = MagicMock(saved=0, failed=0)

            main(["--upcoming"])

            mock_instance.crawl_schedule.assert_called()
            mock_save.assert_called()
