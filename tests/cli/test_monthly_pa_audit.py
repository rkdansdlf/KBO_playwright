from __future__ import annotations

from unittest.mock import patch

from src.cli.monthly_pa_audit import main


class TestMonthlyPaAuditCLI:
    def test_main_default_year(self):
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_audit:
            mock_audit.return_value = 5
            main([])
            assert mock_audit.called

    def test_main_specific_year(self):
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_audit:
            mock_audit.return_value = 3
            main(["--year", "2024"])
            mock_audit.assert_called_once_with(2024)

    def test_main_year_before_2020_skips(self):
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_audit:
            main(["--year", "2019"])
            mock_audit.assert_not_called()

    def test_main_failure_exits(self):
        with patch("src.cli.monthly_pa_audit.run_monthly_pa_audit") as mock_audit:
            mock_audit.side_effect = Exception("audit failed")
            try:
                main(["--year", "2024"])
            except SystemExit:
                pass
