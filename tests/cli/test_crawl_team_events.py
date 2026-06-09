from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_team_events import main


class TestCrawlTeamEventsCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_team_events.TeamEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with(days_back=30)
            mock_instance.run.assert_called_once_with(save=True, team_filter=None)

    def test_main_save_with_team_and_days(self):
        with patch("src.cli.crawl_team_events.TeamEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--team", "LG", "--days", "7"])
            MockCrawler.assert_called_once_with(days_back=7)
            mock_instance.run.assert_called_once_with(save=True, team_filter="LG")

    def test_main_no_args(self):
        with patch("src.cli.crawl_team_events.TeamEventCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main([])
            MockCrawler.assert_called_once_with(days_back=30)
            mock_instance.run.assert_called_once_with(save=False, team_filter=None)
