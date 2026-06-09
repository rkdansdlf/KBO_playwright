from unittest.mock import patch, MagicMock

from src.cli.sync_oci import main


class TestSyncOCI:
    def test_no_target_url_errors(self):
        try:
            main(["--game-details"])
            assert False, "Should have raised SystemExit"
        except SystemExit:
            pass

    def test_game_details(self):
        with patch("src.cli.sync_oci.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.sync_oci.OCISync") as mock_sync:
                mock_sync.return_value.__enter__.return_value = MagicMock()
                result = main(["--game-details", "--target-url", "postgresql://localhost/test"])
                assert result is None

    def test_season_stats(self):
        with patch("src.cli.sync_oci.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.sync_oci.OCISync") as mock_sync:
                mock_sync.return_value.__enter__.return_value = MagicMock()
                result = main(["--season-stats", "--target-url", "postgresql://localhost/test"])
                assert result is None

    def test_teams(self):
        with patch("src.cli.sync_oci.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.sync_oci.OCISync") as mock_sync:
                mock_sync.return_value.__enter__.return_value = MagicMock()
                result = main(["--teams", "--target-url", "postgresql://localhost/test"])
                assert result is None
