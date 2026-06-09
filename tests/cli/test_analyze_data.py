from __future__ import annotations

from unittest.mock import patch

from src.cli.analyze_data import main


class TestAnalyzeDataCLI:
    def test_main_calls_generate_report(self):
        with patch("src.cli.analyze_data.generate_report") as mock_report:
            mock_report.return_value = "test report"
            main([])
            mock_report.assert_called_once_with()

    def test_main_accepts_no_args(self):
        with patch("src.cli.analyze_data.generate_report") as mock_report:
            mock_report.return_value = "test report"
            main([])
            mock_report.assert_called_once_with()

    def test_main_unexpected_args_fails(self):
        with patch("src.cli.analyze_data.generate_report") as mock_report:
            try:
                main(["--unknown"])
            except SystemExit:
                pass
            mock_report.assert_not_called()
