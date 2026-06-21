from unittest.mock import MagicMock, patch

from src.cli.freshness_gate import main


class TestFreshnessGate:
    def test_default_run(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main([])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250101"])
            assert result == 0

    def test_with_json(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--json"])
            assert result == 0

    def test_with_max_hours(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--max-hours", "48"])
            assert result == 0
