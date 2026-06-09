from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_seat_sections import main


class TestCrawlSeatSectionsCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_seat_sections.SeatCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with()
            mock_instance.run.assert_called_once_with(save=True, team_filter=None)

    def test_main_save_with_team(self):
        with patch("src.cli.crawl_seat_sections.SeatCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main(["--save", "--team", "SSG"])
            mock_instance.run.assert_called_once_with(save=True, team_filter="SSG")

    def test_main_no_args(self):
        with patch("src.cli.crawl_seat_sections.SeatCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=[])
            MockCrawler.return_value = mock_instance
            main([])
            mock_instance.run.assert_called_once_with(save=False, team_filter=None)
