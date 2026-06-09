from argparse import Namespace
from unittest.mock import patch, MagicMock

from src.cli.dashboard_report import main


class TestDashboardReport:
    def _setup_mocks(self, mock_sf, date=None, sections=None, fmt="terminal"):
        if sections is None:
            sections = ["standings"]
        mock_parse = patch("argparse.ArgumentParser.parse_args")
        ns = mock_parse.start()
        ns.return_value = Namespace(
            date=date, year=2025, sections=sections,
            format=fmt, report=False, notify=False,
        )
        mock_session = MagicMock()
        mock_sf.return_value.__enter__.return_value = mock_session
        mock_session.query.return_value.filter.return_value.order_by.return_value.first.return_value = None
        return mock_parse, mock_session

    def test_default_run(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_parse, mock_session = self._setup_mocks(mock_sf)
            try:
                result = main()
                assert result is None
            finally:
                mock_parse.stop()

    def test_with_date(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_parse, mock_session = self._setup_mocks(mock_sf, date="20250101")
            try:
                result = main()
                assert result is None
            finally:
                mock_parse.stop()

    def test_json_format(self):
        with patch("src.cli.dashboard_report.SessionLocal") as mock_sf:
            mock_parse, mock_session = self._setup_mocks(mock_sf, fmt="json")
            try:
                result = main()
                assert result is None
            finally:
                mock_parse.stop()
