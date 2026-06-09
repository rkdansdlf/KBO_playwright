from unittest.mock import patch, MagicMock

from src.cli.health_check import main


class TestHealthCheck:
    def test_default_run(self):
        with patch("src.cli.health_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.return_value = 0
            mock_session.execute.return_value.first.return_value = (None,)
            mock_session.query.return_value.all.return_value = []
            result = main([])
            assert result is None

    def test_with_args(self):
        with patch("src.cli.health_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.return_value = 10
            mock_session.execute.return_value.first.return_value = ("2025-01-01",)
            mock_session.query.return_value.all.return_value = []
            result = main([])
            assert result is None
