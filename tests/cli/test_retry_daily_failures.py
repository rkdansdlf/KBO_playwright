from unittest.mock import patch, MagicMock

from src.cli.retry_daily_failures import main


class TestRetryDailyFailures:
    def test_dry_run(self):
        with patch("src.cli.retry_daily_failures.load_daily_summary") as mock_load:
            mock_load.return_value = {
                "stability": {
                    "retry_candidates": {
                        "detail": ["20250401LGSS0"],
                        "relay": ["20250401LGSS0"],
                    }
                }
            }
            result = main(["--date", "20250401"])
            assert result == 0

    def test_apply(self):
        with patch("src.cli.retry_daily_failures.load_daily_summary") as mock_load:
            mock_load.return_value = {
                "stability": {
                    "retry_candidates": {
                        "detail": [],
                        "relay": [],
                    }
                }
            }
            result = main(["--date", "20250401", "--apply"])
            assert result == 0

    def test_invalid_date_format(self):
        try:
            main(["--date", "2025"])
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass
