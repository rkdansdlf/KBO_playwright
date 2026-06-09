from unittest.mock import MagicMock, patch


class TestCleanupCorruptedStats:
    def test_cleanup_no_corrupted(self):
        with patch("scripts.cleanup_corrupted_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []
            from scripts.cleanup_corrupted_stats import cleanup_corrupted_stats
            cleanup_corrupted_stats()
            mock_session.commit.assert_called_once()

    def test_cleanup_with_corrupted(self):
        with patch("scripts.cleanup_corrupted_stats.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_record = MagicMock()
            mock_session.query.return_value.filter.return_value.all.side_effect = [
                [mock_record],
                [],
            ]
            from scripts.cleanup_corrupted_stats import cleanup_corrupted_stats
            cleanup_corrupted_stats()
            assert mock_record.delete.call_count >= 1 or mock_session.delete.call_count >= 1
