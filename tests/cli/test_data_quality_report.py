from unittest.mock import patch

from src.cli.data_quality_report import main


class TestDataQualityReport:
    def test_default_args(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main([])
            assert result == 0
            mock.assert_called_once()

    def test_specific_years(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--years", "2025"])
            assert result == 0
            args, _ = mock.call_args
            assert args[0] == [2025]

    def test_csv_format(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--format", "csv"])
            assert result == 0
            args, _ = mock.call_args
            assert args[1] == "csv"

    def test_with_db_url(self):
        with patch("src.cli.data_quality_report.generate_report") as mock:
            result = main(["--db-url", "postgresql://localhost/test"])
            assert result == 0
            args, _ = mock.call_args
            assert args[3] == "postgresql://localhost/test"
