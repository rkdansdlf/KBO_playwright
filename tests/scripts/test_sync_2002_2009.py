from unittest.mock import MagicMock, patch


class TestSync20022009:
    def test_sync_success(self):
        with (
            patch("scripts.sync_2002_2009.load_dotenv"),
            patch("scripts.sync_2002_2009.os.getenv", return_value="postgresql://localhost/test"),
            patch("scripts.sync_2002_2009.SessionLocal") as mock_sf,
            patch("scripts.sync_2002_2009.OCISync") as mock_sync,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_syncer = MagicMock()
            mock_sync.return_value = mock_syncer
            from scripts.sync_2002_2009 import sync_2002_2009

            sync_2002_2009()
            assert mock_syncer.sync_simple_table.call_count == 2
            assert mock_syncer.sync_simple_table.call_args_list[0].args[1].conflict_keys == [
                "player_id",
                "season",
                "league",
                "level",
            ]
            assert mock_syncer.sync_simple_table.call_args_list[1].args[1].filters is not None

    def test_sync_no_url(self):
        with patch("scripts.sync_2002_2009.load_dotenv"), patch("scripts.sync_2002_2009.os.getenv", return_value=None):
            from scripts.sync_2002_2009 import sync_2002_2009

            sync_2002_2009()
