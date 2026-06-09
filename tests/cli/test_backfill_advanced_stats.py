from unittest.mock import patch

from src.cli.backfill_advanced_stats import main


class TestBackfillAdvancedStats:
    def test_default_years(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            mock.return_value = None
            result = main([])
            assert result == 0
            mock.assert_called_once()
            args, _ = mock.call_args
            assert 2020 in args[0]

    def test_specific_year(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_with_series(self):
        with patch("src.cli.backfill_advanced_stats.backfill_stats") as mock:
            result = main(["--years", "2024", "--series", "postseason"])
            assert result == 0
            args, _ = mock.call_args
            assert args[1] == "postseason"
