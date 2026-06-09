from unittest.mock import patch, MagicMock

from src.cli.sync_pregame_previews import main


class TestSyncPregamePreviews:
    def test_dry_run(self):
        with patch("src.cli.sync_pregame_previews.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []
            result = main(["--dry-run", "--target-url", "postgresql://localhost/test"])
            assert result == 0

    def test_no_target_url(self):
        with patch("src.cli.sync_pregame_previews.get_oci_url", return_value=None):
            try:
                main(["--dry-run"])
                assert False, "Should have raised SystemExit"
            except SystemExit:
                pass

    def test_with_dates(self):
        with patch("src.cli.sync_pregame_previews.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.all.return_value = []
            result = main(["--start-date", "20250401", "--end-date", "20250402", "--target-url", "postgresql://localhost/test"])
            assert result == 0
