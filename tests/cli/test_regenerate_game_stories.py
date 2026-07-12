from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.cli.regenerate_game_stories import (
    StoryContext,
    StoryGameContext,
    StoryRegenReportRow,
    _collect_game_ids,
    _events_by_game,
    _load_existing_story_summaries,
    _load_game_ids_file,
    _process_story_batches,
    _process_story_game,
    _sync_story_summaries,
    _upsert_story_summary,
    _write_backup,
    _write_report,
    regenerate_game_stories,
    main,
)
from src.models.game import GameSummary


class TestRegenerateGameStories:
    def test_no_args_errors(self):
        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_dry_run_with_game_id(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0

    def test_with_apply(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0", "--apply"])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.regenerate_game_stories.regenerate_game_stories") as mock:
            mock.return_value = []
            result = main(["--date", "20250401"])
            assert result == 0


class TestGameStoryIoHelpers:
    def test_load_game_ids_file_skips_comments_and_blanks(self, tmp_path):
        path = tmp_path / "ids.txt"
        path.write_text("# comment\n\n g1 \ng2\n", encoding="utf-8")
        assert _load_game_ids_file(path) == ["g1", "g2"]

    def test_write_report(self, tmp_path):
        path = tmp_path / "report.csv"
        _write_report(
            [StoryRegenReportRow(game_id="g1", game_date="20250401", status="APPLIED", changed=True)],
            path,
        )
        content = path.read_text(encoding="utf-8")
        assert "game_id" in content
        assert "g1" in content

    def test_collect_game_ids_reads_file(self, tmp_path):
        path = tmp_path / "ids.txt"
        path.write_text("g2\n", encoding="utf-8")
        args = SimpleNamespace(game_id=["g1"], game_ids_file=str(path))
        assert _collect_game_ids(args) == ["g1", "g2"]

    def test_write_backup(self, tmp_path):
        row = SimpleNamespace(
            id=1, game_id="g1", summary_type="경기_스토리", player_id=None, player_name=None, detail_text="old"
        )
        session = MagicMock()
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [row]
        path = tmp_path / "backup.csv"

        _write_backup(session, ["g1"], path)

        content = path.read_text(encoding="utf-8")
        assert "detail_text" in content
        assert "old" in content


class TestGameStoryDbHelpers:
    def test_events_by_game_empty(self):
        assert _events_by_game(MagicMock(), []) == {}

    def test_events_by_game_groups_rows(self):
        session = MagicMock()
        first = SimpleNamespace(game_id="g1")
        second = SimpleNamespace(game_id="g1")
        third = SimpleNamespace(game_id="g2")
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [first, second, third]

        result = _events_by_game(session, ["g1", "g2", "g3"])

        assert result["g1"] == [first, second]
        assert result["g2"] == [third]
        assert result["g3"] == []

    def test_load_existing_story_summaries_empty(self):
        assert _load_existing_story_summaries(MagicMock(), []) == ({}, {})

    def test_load_existing_story_summaries_groups_first_detail(self):
        session = MagicMock()
        rows = [
            SimpleNamespace(game_id="g1", detail_text="old1"),
            SimpleNamespace(game_id="g1", detail_text="old2"),
        ]
        session.query.return_value.filter.return_value.order_by.return_value.all.return_value = rows
        summaries, details = _load_existing_story_summaries(session, [SimpleNamespace(game_id="g1")])

        assert summaries["g1"] == rows
        assert details["g1"] == "old1"


class TestUpsertStorySummary:
    def test_updates_existing_rows(self):
        summary = SimpleNamespace(detail_text="old")
        session = MagicMock()
        _upsert_story_summary(session, "g1", "new", {"g1": [summary]})
        assert summary.detail_text == "new"
        session.add.assert_not_called()

    def test_adds_new_summary_when_missing(self):
        session = MagicMock()
        _upsert_story_summary(session, "g1", "new", {})
        session.add.assert_called_once()
        added = session.add.call_args.args[0]
        assert isinstance(added, GameSummary)
        assert added.game_id == "g1"


class TestProcessStoryGame:
    def _completed_game(self):
        return SimpleNamespace(game_id="g1", game_date=date(2025, 4, 1), game_status="COMPLETED")

    def test_dry_run_changed(self):
        game = self._completed_game()
        builder = MagicMock()
        builder.build.return_value = {"timeline": [1], "source": {"warnings": []}}
        ctx = StoryGameContext(
            session=MagicMock(),
            game=game,
            events=[],
            builder=builder,
            inner_ctx=StoryContext(existing_summary_rows={}, existing_summaries={"g1": "old"}),
            apply=False,
        )
        with patch("src.cli.regenerate_game_stories.dump_story_json", return_value="new"):
            row, should_sync = _process_story_game(ctx)

        assert row.status == "DRY_RUN_READY"
        assert should_sync is False

    def test_apply_changed_upserts_and_syncs(self):
        game = self._completed_game()
        builder = MagicMock()
        builder.build.return_value = {"timeline": [1], "source": {"warnings": []}}
        ctx = StoryGameContext(
            session=MagicMock(),
            game=game,
            events=[],
            builder=builder,
            inner_ctx=StoryContext(existing_summary_rows={}, existing_summaries={"g1": "old"}),
            apply=True,
        )
        with (
            patch("src.cli.regenerate_game_stories.dump_story_json", return_value="new"),
            patch("src.cli.regenerate_game_stories._upsert_story_summary") as mock_upsert,
        ):
            row, should_sync = _process_story_game(ctx)

        assert row.status == "APPLIED"
        assert should_sync is True
        mock_upsert.assert_called_once()

    def test_apply_unchanged_does_not_upsert(self):
        game = self._completed_game()
        builder = MagicMock()
        builder.build.return_value = {"timeline": [], "source": {"warnings": []}}
        ctx = StoryGameContext(
            session=MagicMock(),
            game=game,
            events=[],
            builder=builder,
            inner_ctx=StoryContext(existing_summary_rows={}, existing_summaries={"g1": "same"}),
            apply=True,
        )
        with (
            patch("src.cli.regenerate_game_stories.dump_story_json", return_value="same"),
            patch("src.cli.regenerate_game_stories._upsert_story_summary") as mock_upsert,
        ):
            row, should_sync = _process_story_game(ctx)

        assert row.status == "UNCHANGED"
        assert should_sync is True
        mock_upsert.assert_not_called()


class TestProcessStoryBatches:
    def test_batches_collect_rows_and_sync_ids(self):
        games = [
            SimpleNamespace(game_id="g1", game_status="COMPLETED"),
            SimpleNamespace(game_id="g2", game_status="SCHEDULED"),
        ]
        with (
            patch("src.cli.regenerate_game_stories._events_by_game", return_value={"g1": []}) as mock_events,
            patch(
                "src.cli.regenerate_game_stories._process_story_game",
                side_effect=[
                    (StoryRegenReportRow(game_id="g1", game_date="20250401", status="APPLIED"), True),
                    (StoryRegenReportRow(game_id="g2", game_date="20250401", status="SKIPPED"), False),
                ],
            ),
        ):
            rows, sync_ids = _process_story_batches(
                MagicMock(),
                games,
                MagicMock(),
                ctx=StoryContext(existing_summary_rows={}, existing_summaries={}),
                apply=True,
            )

        assert [row.game_id for row in rows] == ["g1", "g2"]
        assert sync_ids == ["g1"]
        mock_events.assert_called_once()


class TestSyncStorySummaries:
    def test_empty_game_ids_returns_early(self):
        with patch("src.cli.regenerate_game_stories.SessionLocal") as mock_session_local:
            _sync_story_summaries([], [], oci_url="postgresql://db", log=MagicMock())
        mock_session_local.assert_not_called()

    def test_marks_synced_rows(self):
        rows = [StoryRegenReportRow(game_id="g1", game_date="20250401", status="APPLIED")]
        syncer = MagicMock()
        syncer.sync_review_summaries_for_games.return_value = {"summary": 1}
        with (
            patch("src.cli.regenerate_game_stories.SessionLocal") as mock_session_local,
            patch("src.cli.regenerate_game_stories.OCISync", return_value=syncer),
        ):
            mock_session_local.return_value.__enter__.return_value = MagicMock()
            _sync_story_summaries(["g1"], rows, oci_url="postgresql://db", log=MagicMock())

        assert rows[0].oci_status == "synced_summary:1"
        syncer.close.assert_called_once()


class TestRegenerateGameStoriesOrchestrator:
    def test_dry_run_writes_report_and_returns_rows(self, tmp_path):
        report_path = tmp_path / "report.csv"
        processed = [StoryRegenReportRow(game_id="g1", game_date="20250401", status="DRY_RUN_READY")]
        with (
            patch("src.cli.regenerate_game_stories.SessionLocal") as mock_session_local,
            patch("src.cli.regenerate_game_stories._query_target_games", return_value=[SimpleNamespace(game_id="g1")]),
            patch("src.cli.regenerate_game_stories._load_existing_story_summaries", return_value=({}, {})),
            patch("src.cli.regenerate_game_stories._process_story_batches", return_value=(processed, [])),
            patch("src.cli.regenerate_game_stories._write_report") as mock_write,
        ):
            mock_session_local.return_value.__enter__.return_value = MagicMock()
            rows = regenerate_game_stories(game_ids=["g1"], report_out=report_path, log=MagicMock())

        assert rows == processed
        mock_write.assert_called_once_with(processed, report_path)

    def test_apply_commit_failure_rolls_back(self, tmp_path):
        report_path = tmp_path / "report.csv"
        session = MagicMock()
        session.commit.side_effect = RuntimeError("commit failed")
        with (
            patch("src.cli.regenerate_game_stories.SessionLocal") as mock_session_local,
            patch("src.cli.regenerate_game_stories._query_target_games", return_value=[]),
            patch("src.cli.regenerate_game_stories._write_backup"),
            patch("src.cli.regenerate_game_stories._load_existing_story_summaries", return_value=({}, {})),
            patch("src.cli.regenerate_game_stories._process_story_batches", return_value=([], [])),
        ):
            mock_session_local.return_value.__enter__.return_value = session
            try:
                regenerate_game_stories(game_ids=["g1"], apply=True, report_out=report_path, log=MagicMock())
                raise AssertionError("expected RuntimeError")
            except RuntimeError:
                pass

        session.rollback.assert_called_once()
