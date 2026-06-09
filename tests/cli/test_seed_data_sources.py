from unittest.mock import MagicMock, patch

from src.cli.seed_data_sources import main


class TestSeedDataSources:
    def test_default_run(self):
        with patch("src.cli.seed_data_sources.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.first.return_value = None
            result = main([])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.seed_data_sources.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            result = main(["--dry-run"])
            assert result is None
