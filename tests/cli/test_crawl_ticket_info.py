from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_ticket_info import main


class TestCrawlTicketInfoCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_ticket_info.TicketCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with()
            mock_instance.run.assert_called_once_with(save=True, season=None)

    def test_main_save_with_season(self):
        with patch("src.cli.crawl_ticket_info.TicketCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--season", "2025"])
            mock_instance.run.assert_called_once_with(save=True, season=2025)

    def test_main_no_args(self):
        with patch("src.cli.crawl_ticket_info.TicketCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main([])
            mock_instance.run.assert_called_once_with(save=False, season=None)
