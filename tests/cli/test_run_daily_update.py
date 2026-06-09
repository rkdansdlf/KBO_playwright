from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.run_daily_update import main


class TestRunDailyUpdateCLI:
    def test_main_default_date(self):
        with patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update, \
             patch("src.cli.run_daily_update.datetime") as mock_dt:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "20251014"
            mock_dt.now.return_value = mock_now
            mock_dt.timedelta = MagicMock(return_value=MagicMock())
            mock_dt.strptime = __import__("datetime").datetime.strptime

            result = main([])
            assert result == mock_update.return_value
            mock_update.assert_called_once()

    def test_main_with_date(self):
        with patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update:
            result = main(["--date", "20251015"])
            assert result == mock_update.return_value
            mock_update.assert_called_once_with(
                "20251015", sync=False, headless=True, limit=None,
                summary_dir=None, seed_tomorrow_preview=False,
                run_auto_healer=True, run_postgame_reconciliation=True,
                postgame_reconcile_lookback_days=3, fix=False,
                skip_season_stats=False, skip_oci_supporting_sync=False,
                run_p0_non_game=True,
            )

    def test_main_with_sync(self):
        with patch("src.cli.run_daily_update.run_update", new_callable=AsyncMock) as mock_update:
            main(["--date", "20251015", "--sync"])
            mock_update.assert_called_once_with(
                "20251015", sync=True, headless=True, limit=None,
                summary_dir=None, seed_tomorrow_preview=False,
                run_auto_healer=True, run_postgame_reconciliation=True,
                postgame_reconcile_lookback_days=3, fix=False,
                skip_season_stats=False, skip_oci_supporting_sync=False,
                run_p0_non_game=True,
            )
