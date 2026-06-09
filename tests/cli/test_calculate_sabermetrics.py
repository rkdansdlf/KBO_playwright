from unittest.mock import patch

from src.cli.calculate_sabermetrics import main


class TestCalculateSabermetrics:
    def test_default_years(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main([])
            assert result == 0
            mock.assert_called_once()
            args, _ = mock.call_args
            assert 2020 in args[0]

    def test_specific_year(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_with_sync(self):
        with patch("src.cli.calculate_sabermetrics.batch_calculate_sabermetrics") as mock:
            result = main(["--sync"])
            assert result == 0
            _, kwargs = mock.call_args
            assert kwargs["sync_oci"] is True
