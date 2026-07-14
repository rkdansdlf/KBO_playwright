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

    def test_save_chunk_success(self):
        from src.cli.collect_rosters import save_chunk

        with (
            patch("src.cli.collect_rosters.SessionLocal") as mock_session_local,
            patch("src.cli.collect_rosters.TeamRepository") as mock_repo_cls,
        ):
            mock_session = MagicMock()
            mock_session_local.return_value = mock_session
            mock_repo = MagicMock()
            mock_repo.save_daily_rosters.return_value = 4
            mock_repo_cls.return_value = mock_repo
            save_chunk([{"team": "SSG"}])
            mock_repo.save_daily_rosters.assert_called_once()
            mock_session.close.assert_called_once()

    def test_save_chunk_handles_exception(self):
        from src.cli.collect_rosters import save_chunk

        with (
            patch("src.cli.collect_rosters.SessionLocal") as mock_session_local,
            patch("src.cli.collect_rosters.TeamRepository") as mock_repo_cls,
        ):
            mock_session = MagicMock()
            mock_session_local.return_value = mock_session
            mock_repo = MagicMock()
            mock_repo.save_daily_rosters.side_effect = ValueError("boom")
            mock_repo_cls.return_value = mock_repo
            save_chunk([{"team": "SSG"}])
            mock_session.close.assert_called_once()
