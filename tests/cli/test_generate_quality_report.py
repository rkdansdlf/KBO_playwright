from unittest.mock import patch, MagicMock

from src.cli.generate_quality_report import main


class TestGenerateQualityReport:
    def test_default_run(self):
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_session.execute.return_value.all.return_value = []
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.first.return_value = (None,)
            result = main([])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_session.execute.return_value.all.return_value = []
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.first.return_value = (None,)
            result = main(["--date", "20250101"])
            assert result == 0

    def test_notify(self):
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []
            mock_session.execute.return_value.all.return_value = []
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.first.return_value = (None,)
            result = main(["--notify"])
            assert result == 0
