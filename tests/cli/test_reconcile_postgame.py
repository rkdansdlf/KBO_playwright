from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.reconcile_postgame import main


class TestReconcilePostgameCLI:
    def test_main_single_date(self):
        with patch("src.cli.reconcile_postgame.SessionLocal"), \
             patch("src.cli.reconcile_postgame.PlayerIdResolver"), \
             patch("src.cli.reconcile_postgame.GameDetailCrawler"), \
             patch("src.cli.reconcile_postgame.reconcile_postgame_range", new_callable=AsyncMock) as mock_reconcile, \
             patch("src.cli.reconcile_postgame.format_reconciliation_report") as mock_format:
            mock_result = MagicMock()
            mock_result.start_date = "20251015"
            mock_result.end_date = "20251015"
            mock_result.candidates = 0
            mock_result.changes = []
            mock_reconcile.return_value = mock_result
            mock_format.return_value = ""

            result = main(["--date", "20251015"])
            assert result == 0
            mock_reconcile.assert_called_once()

    def test_main_with_lookback(self):
        with patch("src.cli.reconcile_postgame.SessionLocal"), \
             patch("src.cli.reconcile_postgame.PlayerIdResolver"), \
             patch("src.cli.reconcile_postgame.GameDetailCrawler"), \
             patch("src.cli.reconcile_postgame.reconcile_postgame_range", new_callable=AsyncMock) as mock_reconcile, \
             patch("src.cli.reconcile_postgame.format_reconciliation_report"):
            mock_result = MagicMock()
            mock_result.start_date = "20251013"
            mock_result.end_date = "20251015"
            mock_result.candidates = 5
            mock_result.changes = []
            mock_reconcile.return_value = mock_result

            result = main(["--date", "20251015", "--lookback-days", "2"])
            assert result == 0

    def test_main_with_date_range(self):
        with patch("src.cli.reconcile_postgame.SessionLocal"), \
             patch("src.cli.reconcile_postgame.PlayerIdResolver"), \
             patch("src.cli.reconcile_postgame.GameDetailCrawler"), \
             patch("src.cli.reconcile_postgame.reconcile_postgame_range", new_callable=AsyncMock) as mock_reconcile, \
             patch("src.cli.reconcile_postgame.format_reconciliation_report"):
            mock_result = MagicMock()
            mock_result.start_date = "20251001"
            mock_result.end_date = "20251015"
            mock_result.candidates = 10
            mock_result.changes = []
            mock_reconcile.return_value = mock_result

            result = main(["--start-date", "20251001", "--end-date", "20251015"])
            assert result == 0
