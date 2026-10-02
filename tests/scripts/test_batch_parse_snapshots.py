from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.services.snapshot_persist import SaveOutcome
from src.services.snapshot_replay import SnapshotParseResult, SnapshotReplayError


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


class TestProcessSnapshotDelegatesToService:
    def _session(self) -> tuple[MagicMock, MagicMock]:
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = SimpleNamespace(
            source_key="lg_twins_events",
            target_domain="event",
        )
        return session, MagicMock()

    def _parsed(self, *, success: bool = True) -> SnapshotParseResult:
        return SnapshotParseResult(
            snapshot_id=1,
            source_key="lg_twins_events",
            parser_version="team-event-v1",
            records=({"title": "a"},) if success else (),
            success=success,
            error=None if success else "bad html",
        )

    def test_successful_save_marks_done(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session, snap_repo = self._session()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        with (
            patch("scripts.batch_parse_snapshots.parse_snapshot", return_value=self._parsed()),
            patch(
                "scripts.batch_parse_snapshots.persist_parsed_records", return_value=SaveOutcome(saved=1, failed=0)
            ) as mock_save,
        ):
            result = _process_snapshot(session, snap_repo, snapshot, False, lambda: session)

        assert result == "done"
        mock_save.assert_called_once()
        snap_repo.update_parse_status.assert_called_once_with(1, "done", parser_version="team-event-v1")

    def test_partial_save_marks_partial(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session, snap_repo = self._session()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        with (
            patch("scripts.batch_parse_snapshots.parse_snapshot", return_value=self._parsed()),
            patch("scripts.batch_parse_snapshots.persist_parsed_records", return_value=SaveOutcome(saved=1, failed=1)),
        ):
            result = _process_snapshot(session, snap_repo, snapshot, False, lambda: session)

        assert result == "partial"
        assert snap_repo.update_parse_status.call_args.args[1] == "partial"

    def test_dry_run_skips_save(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session, snap_repo = self._session()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        with (
            patch("scripts.batch_parse_snapshots.parse_snapshot", return_value=self._parsed()),
            patch("scripts.batch_parse_snapshots.persist_parsed_records") as mock_save,
        ):
            result = _process_snapshot(session, snap_repo, snapshot, True, lambda: session)

        assert result == "done"
        mock_save.assert_not_called()

    def test_parse_failure_marks_failed(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session, snap_repo = self._session()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        with (
            patch("scripts.batch_parse_snapshots.parse_snapshot", return_value=self._parsed(success=False)),
            patch("scripts.batch_parse_snapshots.persist_parsed_records") as mock_save,
        ):
            result = _process_snapshot(session, snap_repo, snapshot, False, lambda: session)

        assert result == "failed"
        mock_save.assert_not_called()
        assert snap_repo.update_parse_status.call_args.args[1] == "failed"

    def test_replay_error_marks_failed(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session, snap_repo = self._session()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        with patch(
            "scripts.batch_parse_snapshots.parse_snapshot",
            side_effect=SnapshotReplayError("stored artifact not found"),
        ):
            result = _process_snapshot(session, snap_repo, snapshot, False, lambda: session)

        assert result == "failed"

    def test_missing_data_source_marks_failed(self) -> None:
        from scripts.batch_parse_snapshots import _process_snapshot

        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        snap_repo = MagicMock()
        snapshot = SimpleNamespace(id=1, data_source_id=2)
        result = _process_snapshot(session, snap_repo, snapshot, False, lambda: session)
        assert result == "failed"
