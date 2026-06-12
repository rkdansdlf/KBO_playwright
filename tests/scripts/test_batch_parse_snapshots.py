from unittest.mock import MagicMock, patch


class TestBatchParseSnapshots:
    def test_main_default(self):
        with patch("scripts.batch_parse_snapshots.run_batch_parse") as mock_fn, patch("sys.argv", ["script"]):
            from scripts.batch_parse_snapshots import main

            main()
            mock_fn.assert_called_once_with(limit=50, dry_run=False, retry_failed=True, retry_after_hours=1)

    def test_main_dry_run(self):
        with (
            patch("scripts.batch_parse_snapshots.run_batch_parse") as mock_fn,
            patch("sys.argv", ["script", "--dry-run", "--limit", "10"]),
        ):
            from scripts.batch_parse_snapshots import main

            main()
            mock_fn.assert_called_once_with(limit=10, dry_run=True, retry_failed=True, retry_after_hours=1)

    def test_no_pending(self):
        with (
            patch("scripts.batch_parse_snapshots.SessionLocal") as mock_sf,
            patch("scripts.batch_parse_snapshots.RawSourceSnapshotRepository") as mock_repo,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance
            mock_repo_instance.get_unparsed.return_value = []
            mock_repo_instance.get_failed_for_retry.return_value = []
            from scripts.batch_parse_snapshots import run_batch_parse

            result = run_batch_parse(limit=10)
            assert result["processed"] == 0
