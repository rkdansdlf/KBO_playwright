from unittest.mock import MagicMock, patch

from src.cli.seed_relay_validation_metrics import main


class TestSeedRelayValidationMetrics:
    def test_default_run(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main([])
            assert result == 0

    def test_with_season(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--season", "2025"])
            assert result == 0

    def test_no_mark_legacy(self):
        with patch("src.cli.seed_relay_validation_metrics.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--no-mark-legacy-unavailable"])
            assert result == 0
