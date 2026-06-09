from unittest.mock import MagicMock, patch

from src.cli.sync_oci import main


class TestSyncOCI:
    def test_no_target_url_errors(self):
        with patch("src.cli.sync_oci.get_oci_url", return_value=None):
            try:
                main(["--game-details"])
                raise AssertionError("Should have raised SystemExit")
            except SystemExit:
                pass

    def test_game_details(self):
        with patch("src.cli.sync_oci.get_oci_url", return_value=None), \
             patch("src.cli.sync_oci.SessionLocal") as mock_sf, \
             patch("src.cli.sync_oci.OCISync") as mock_sync:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_sync.return_value.close = MagicMock()
            result = main(["--game-details", "--target-url", "postgresql://localhost/test"])
            assert result is None

    def test_season_stats(self):
        with patch("src.cli.sync_oci.get_oci_url", return_value=None), \
             patch("src.cli.sync_oci.SessionLocal") as mock_sf, \
             patch("src.cli.sync_oci.OCISync") as mock_sync:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_sync.return_value.close = MagicMock()
            result = main(["--season-stats", "--target-url", "postgresql://localhost/test"])
            assert result is None

    def test_teams(self):
        with patch("src.cli.sync_oci.get_oci_url", return_value=None), \
             patch("src.cli.sync_oci.SessionLocal") as mock_sf, \
             patch("src.cli.sync_oci.OCISync") as mock_sync:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_sync.return_value.close = MagicMock()
            result = main(["--teams", "--target-url", "postgresql://localhost/test"])
            assert result is None
