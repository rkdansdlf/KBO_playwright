from unittest.mock import patch

from src.cli.morning_pbp_report import main


class TestMorningPbpReport:
    def test_dry_run_no_summary(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary", return_value=None):
            result = main(["--dry-run"])
            assert result == 0

    def test_dry_run_with_summary(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary") as mock_find:
            mock_find.return_value = ("20250101", {"stability": {}})
            with patch("src.cli.morning_pbp_report._query_pbp_validation_summary", return_value={}):
                result = main(["--dry-run"])
                assert result == 0

    def test_specific_date(self):
        with patch("src.cli.morning_pbp_report._find_latest_summary") as mock_find:
            mock_find.return_value = ("20250101", {"stability": {}})
            with patch("src.cli.morning_pbp_report._query_pbp_validation_summary", return_value={}):
                result = main(["--date", "20250101", "--dry-run"])
                assert result == 0
