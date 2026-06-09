from unittest.mock import MagicMock, patch


class TestFastSyncStats:
    def test_fast_sync_stats_success(self):
        with patch("scripts.fast_sync_stats.load_dotenv"), \
             patch("scripts.fast_sync_stats.os.getenv", return_value="postgresql://localhost/test"), \
             patch("scripts.fast_sync_stats.SessionLocal") as mock_sf, \
             patch("scripts.fast_sync_stats.OCISync") as mock_sync:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_syncer = MagicMock()
            mock_sync.return_value = mock_syncer
            from scripts.fast_sync_stats import fast_sync_stats
            fast_sync_stats()
            assert mock_syncer.sync_simple_table.call_count == 2

    def test_fast_sync_stats_no_url(self):
        with patch("scripts.fast_sync_stats.load_dotenv"), \
             patch("scripts.fast_sync_stats.os.getenv", return_value=None):
            from scripts.fast_sync_stats import fast_sync_stats
            fast_sync_stats()
