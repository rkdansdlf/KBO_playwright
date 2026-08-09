from __future__ import annotations

from unittest.mock import AsyncMock, patch

from src.cli.run_periodic_extras import main


class TestRunPeriodicExtrasCLI:
    def test_main_default_year(self):
        with (
            patch("sys.argv", ["run_periodic_extras"]),
            patch("src.cli.run_periodic_extras._run_subprocess", new_callable=AsyncMock) as mock_run,
            patch("src.cli.run_periodic_extras.datetime") as mock_dt,
        ):
            mock_dt.now.return_value.year = 2025
            mock_run.return_value = (0, "", "")

            main()

            assert mock_run.call_count == 2

    def test_main_with_year(self):
        with (
            patch("sys.argv", ["run_periodic_extras", "--year", "2024"]),
            patch("src.cli.run_periodic_extras._run_subprocess", new_callable=AsyncMock) as mock_run,
        ):
            mock_run.return_value = (0, "", "")

            main()

            assert mock_run.call_count == 2
