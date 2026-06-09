from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.run_advanced_daily import main


class TestRunAdvancedDailyCLI:
    def test_main_default_year(self):
        with patch("sys.argv", ["run_advanced_daily"]), \
             patch("src.cli.run_advanced_daily.crawl_all_fielding_stats") as mock_f, \
             patch("src.cli.run_advanced_daily.crawl_baserunning_stats") as mock_b, \
             patch("src.cli.run_advanced_daily.TeamBattingStatsCrawler") as MockTB, \
             patch("src.cli.run_advanced_daily.TeamPitchingStatsCrawler") as MockTP, \
             patch("src.cli.run_advanced_daily.datetime") as mock_dt:
            mock_dt.now.return_value.year = 2025
            mock_f.return_value = []
            mock_b.return_value = []
            mock_tb = MagicMock()
            mock_tb.crawl = MagicMock(return_value=[])
            MockTB.return_value = mock_tb
            mock_tp = MagicMock()
            mock_tp.crawl = MagicMock(return_value=[])
            MockTP.return_value = mock_tp
            main()
            mock_f.assert_called_once_with(2025)

    def test_main_with_year(self):
        with patch("sys.argv", ["run_advanced_daily", "--year", "2024"]), \
             patch("src.cli.run_advanced_daily.crawl_all_fielding_stats") as mock_f, \
             patch("src.cli.run_advanced_daily.crawl_baserunning_stats") as mock_b, \
             patch("src.cli.run_advanced_daily.TeamBattingStatsCrawler") as MockTB, \
             patch("src.cli.run_advanced_daily.TeamPitchingStatsCrawler") as MockTP, \
             patch("src.cli.run_advanced_daily.SessionLocal") as mock_sesh, \
             patch("src.cli.run_advanced_daily.PlayerSeasonFieldingRepository"), \
             patch("src.cli.run_advanced_daily.PlayerSeasonBaserunningRepository"):
            mock_f.return_value = []
            mock_b.return_value = []
            mock_tb = MagicMock()
            mock_tb.crawl = MagicMock(return_value=[])
            MockTB.return_value = mock_tb
            mock_tp = MagicMock()
            mock_tp.crawl = MagicMock(return_value=[])
            MockTP.return_value = mock_tp
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_sesh.return_value.__enter__.return_value = mock_session

            main()
            mock_f.assert_called_once_with(2024)
