from unittest.mock import patch

from src.cli.gap_report import main


class TestGapReport:
    def test_default_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main([])
            assert result is None

    def test_no_alert(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--no-alert"])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--dry-run"])
            assert result is None
