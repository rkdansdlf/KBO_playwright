from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.collect_rosters import main


class TestCollectRostersCLI:
    def test_main_year(self):
        with patch("src.cli.collect_rosters.DailyRosterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_date_range = AsyncMock()
            MockCrawler.return_value = mock_instance
            with patch("sys.argv", ["collect_rosters", "--year", "2025"]):
                main()
            MockCrawler.assert_called_once_with()
            mock_instance.crawl_date_range.assert_called_once()
            call_args = mock_instance.crawl_date_range.call_args.kwargs
            assert call_args["start_date"] == "2025-03-01"
            assert call_args["end_date"] == "2025-11-30"

    def test_main_year_month(self):
        with patch("src.cli.collect_rosters.DailyRosterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_date_range = AsyncMock()
            MockCrawler.return_value = mock_instance
            with patch("sys.argv", ["collect_rosters", "--year", "2025", "--month", "6"]):
                main()
            mock_instance.crawl_date_range.assert_called_once()
            call_args = mock_instance.crawl_date_range.call_args.kwargs
            assert call_args["start_date"] == "2025-06-01"
            assert call_args["end_date"] == "2025-06-30"

    def test_main_year_month_december(self):
        with patch("src.cli.collect_rosters.DailyRosterCrawler") as MockCrawler:
            mock_instance = MagicMock()
            mock_instance.crawl_date_range = AsyncMock()
            MockCrawler.return_value = mock_instance
            with patch("sys.argv", ["collect_rosters", "--year", "2025", "--month", "12"]):
                main()
            call_args = mock_instance.crawl_date_range.call_args.kwargs
            assert call_args["start_date"] == "2025-12-01"
            assert call_args["end_date"] == "2025-12-31"

    def test_main_missing_year(self):
        with patch("sys.argv", ["collect_rosters"]):
            try:
                main()
            except SystemExit:
                pass
