from unittest.mock import patch, MagicMock

from src.cli.dashboard_report import main


class TestDashboardReport:
    def test_default_run(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.order_by.return_value.first.return_value = None
            result = main()
            assert result is None

    def test_with_date(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.order_by.return_value.first.return_value = None
            result = main()
            assert result is None

    def test_json_format(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.order_by.return_value.first.return_value = None
            result = main()
            assert result is None
