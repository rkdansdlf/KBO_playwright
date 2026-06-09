from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.crawl_roster_transactions import main


class TestCrawlRosterTransactionsCLI:
    def test_main_save(self):
        with patch("src.cli.crawl_roster_transactions.RosterTransactionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save"])
            MockCrawler.assert_called_once_with()
            mock_instance.run.assert_called_once_with(save=True, target_date=None)

    def test_main_save_with_date(self):
        with patch("src.cli.crawl_roster_transactions.RosterTransactionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main(["--save", "--date", "2025-06-01"])
            mock_instance.run.assert_called_once_with(save=True, target_date="2025-06-01")

    def test_main_no_args(self):
        with patch("src.cli.crawl_roster_transactions.RosterTransactionCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock()
            MockCrawler.return_value = mock_instance
            main([])
            mock_instance.run.assert_called_once_with(save=False, target_date=None)
