from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.run_periodic_extras import main


class TestRunPeriodicExtrasCLI:
    def test_main_default_year(self):
        with patch("sys.argv", ["run_periodic_extras"]), \
             patch("subprocess.run") as mock_run, \
             patch("src.cli.run_periodic_extras.datetime") as mock_dt:
            mock_dt.now.return_value.year = 2025
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = ""
            mock_run.return_value = mock_result

            main()

            assert mock_run.call_count == 2

    def test_main_with_year_and_sync(self):
        with patch("sys.argv", ["run_periodic_extras", "--year", "2024", "--sync"]), \
             patch("subprocess.run") as mock_run, \
             patch("src.cli.run_periodic_extras.SessionLocal"), \
             patch("src.cli.run_periodic_extras.OCISync") as MockSync:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = ""
            mock_run.return_value = mock_result
            mock_sync = MagicMock()
            MockSync.return_value.__enter__.return_value = mock_sync

            main()

            assert mock_run.call_count == 2
            MockSync.assert_called_once()
