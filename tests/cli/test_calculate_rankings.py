from unittest.mock import patch, MagicMock

from src.cli.calculate_rankings import main


class TestCalculateRankings:
    def test_required_year(self):
        with patch("src.cli.calculate_rankings.rebuild_rankings") as mock:
            mock.return_value = 0
            result = main(["--year", "2025"])
            assert result == 0
            mock.assert_called_once_with(2025)

    def test_no_year_errors(self):
        import argparse
        try:
            main([])
            assert False, "Should have raised SystemExit"
        except (argparse.ArgumentError, SystemExit):
            pass
