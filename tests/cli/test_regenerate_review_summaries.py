from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.cli.regenerate_review_summaries import (
    ReviewRegenReportRow,
    _append_missing_review_rows,
    _build_review_report_row,
    _collect_game_ids,
    _count_crucial_moments,
    _count_noise_moments,
    _load_game_ids_file,
    _mark_review_oci_status,
    _process_review_games,
    _process_review_game,
    _short_hash,
    _skipped_review_row,
    _sync_review_summaries,
    _write_backup,
    _write_report,
    regenerate_review_summaries,
    main,
)


class TestRegenerateReviewSummaries:
    def test_no_args_errors(self):
        try:
            main([])
            raise AssertionError("Should have raised SystemExit")
        except SystemExit:
            pass

    def test_dry_run_with_game_id(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0"])
            assert result == 0

    def test_with_apply(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--game-id", "20250401LGSS0", "--apply"])
            assert result == 0

    def test_with_season(self):
        with patch("src.cli.regenerate_review_summaries.regenerate_review_summaries") as mock:
            mock.return_value = []
            result = main(["--season", "2025"])
            assert result == 0


def test_review_report_row_as_csv_row_serializes_bool():
    row = ReviewRegenReportRow(game_id="G1", game_date="20260402", status="APPLIED", changed=True)

    csv_row = row.as_csv_row()

    assert csv_row["changed"] == "true"
    assert csv_row["game_id"] == "G1"


def test_short_hash_and_load_game_ids_file(tmp_path):
    path = tmp_path / "ids.txt"
    path.write_text("# comment\nG1\n\n G2 \n", encoding="utf-8")

    assert _short_hash(None) == ""
    assert _short_hash("same") == _short_hash("same")
    assert _load_game_ids_file(path) == ["G1", "G2"]


def test_count_moments_handles_noise_and_non_lists():
    review_data = {
        "crucial_moments": [
            {"description": "경기 준비중"},
            {"description": "1회초 홈런"},
            "not dict",
        ],
    }

    assert _count_crucial_moments(review_data) == 3
    assert _count_noise_moments(review_data) == 1
    assert _count_crucial_moments({"crucial_moments": None}) == 0
    assert _count_noise_moments({"crucial_moments": None}) == 0


@dataclass
class _Game:
    game_id: str = "G1"
    game_date: date | None = date(2026, 4, 2)
    game_status: str = "COMPLETED"


def test_missing_and_skipped_review_rows():
    rows: list[ReviewRegenReportRow] = []

    _append_missing_review_rows(rows, ["G1", "G2", "G2"], {"G1": _Game()})

    assert [(row.game_id, row.status) for row in rows] == [("G2", "SKIPPED_GAME_NOT_FOUND")]
    skipped = _skipped_review_row(_Game(game_status="SCHEDULED"))
    assert skipped.status == "SKIPPED_NOT_COMPLETED"
    assert skipped.message == "status=SCHEDULED"


def test_build_review_report_row_hashes_and_counts():
    game = _Game(game_id="G9")
    review_data = {"crucial_moments": [{"description": "홈런"}, {"description": "안타"}]}

    row = _build_review_report_row(game, '{"old": true}', '{"new": true}', review_data, noise_moments=1)

    assert row.game_id == "G9"
    assert row.game_date == "20260402"
    assert row.changed is True
    assert row.crucial_moments == 2
    assert row.noise_moments == 1
    assert row.old_hash and row.new_hash and row.old_hash != row.new_hash


def test_mark_review_oci_status_dry_run_and_missing_url():
    rows = [
        ReviewRegenReportRow("G1", "20260402", "APPLIED"),
        ReviewRegenReportRow("G2", "20260402", "FAILED"),
    ]

    _mark_review_oci_status(rows, apply=False, oci_url="oci")
    assert [row.oci_status for row in rows] == ["skipped_dry_run", "skipped_dry_run"]

    rows = [
        ReviewRegenReportRow("G1", "20260402", "UNCHANGED"),
        ReviewRegenReportRow("G2", "20260402", "FAILED"),
    ]
    _mark_review_oci_status(rows, apply=True, oci_url=None)
    assert [row.oci_status for row in rows] == ["skipped_missing_oci_url", ""]


def test_process_review_game_branches():
    session = MagicMock()
    agg = MagicMock()

    skipped, should_sync = _process_review_game(session, agg, _Game(game_status="SCHEDULED"), apply=True)
    assert skipped.status == "SKIPPED_NOT_COMPLETED"
    assert should_sync is False

    with (
        patch("src.cli.regenerate_review_summaries._existing_review_json", return_value='{"same": true}'),
        patch("src.cli.regenerate_review_summaries._build_review_data", return_value={"same": True}),
    ):
        row, should_sync = _process_review_game(session, agg, _Game(), apply=False)
    assert row.status == "DRY_RUN_UNCHANGED"
    assert should_sync is False

    with (
        patch("src.cli.regenerate_review_summaries._existing_review_json", return_value=None),
        patch(
            "src.cli.regenerate_review_summaries._build_review_data",
            return_value={"crucial_moments": [{"description": "경기 준비중"}]},
        ),
    ):
        row, should_sync = _process_review_game(session, agg, _Game(), apply=True)
    assert row.status == "SKIPPED_REVIEW_MOMENT_NOISE"
    assert should_sync is False


def test_process_review_game_apply_changed_and_unchanged():
    session = MagicMock()
    agg = MagicMock()

    with (
        patch("src.cli.regenerate_review_summaries._existing_review_json", return_value=None),
        patch("src.cli.regenerate_review_summaries._build_review_data", return_value={"crucial_moments": []}),
        patch("src.cli.regenerate_review_summaries._upsert_review_summary") as mock_upsert,
    ):
        row, should_sync = _process_review_game(session, agg, _Game(), apply=True)
    assert row.status == "APPLIED"
    assert should_sync is True
    mock_upsert.assert_called_once()

    with (
        patch("src.cli.regenerate_review_summaries._existing_review_json", return_value='{"crucial_moments": []}'),
        patch("src.cli.regenerate_review_summaries._build_review_data", return_value={"crucial_moments": []}),
        patch("src.cli.regenerate_review_summaries._upsert_review_summary") as mock_upsert,
    ):
        row, should_sync = _process_review_game(session, agg, _Game(), apply=True)
    assert row.status == "UNCHANGED"
    assert should_sync is True
    mock_upsert.assert_not_called()


def test_process_review_games_collects_sync_ids():
    games = [_Game(game_id="G1"), _Game(game_id="G2")]
    with patch(
        "src.cli.regenerate_review_summaries._process_review_game",
        side_effect=[
            (ReviewRegenReportRow("G1", "20260402", "APPLIED"), True),
            (ReviewRegenReportRow("G2", "20260402", "SKIPPED"), False),
        ],
    ):
        rows, sync_ids = _process_review_games(MagicMock(), games, MagicMock(), apply=True)

    assert [row.game_id for row in rows] == ["G1", "G2"]
    assert sync_ids == ["G1"]


def test_write_report_and_backup(tmp_path):
    report_path = tmp_path / "report.csv"
    _write_report([ReviewRegenReportRow("G1", "20260402", "APPLIED", changed=True)], report_path)
    assert "G1" in report_path.read_text(encoding="utf-8")

    backup_path = tmp_path / "backup.csv"
    session = MagicMock()
    session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        SimpleNamespace(
            id=1, game_id="G1", summary_type="리뷰_WPA", player_id=None, player_name=None, detail_text="old"
        ),
    ]
    _write_backup(session, ["G1"], backup_path)
    assert "old" in backup_path.read_text(encoding="utf-8")


def test_collect_game_ids_reads_file(tmp_path):
    path = tmp_path / "ids.txt"
    path.write_text("G2\n", encoding="utf-8")
    args = SimpleNamespace(game_id=["G1"], game_ids_file=str(path))
    assert _collect_game_ids(args) == ["G1", "G2"]


def test_sync_review_summaries_empty_and_success():
    with patch("src.cli.regenerate_review_summaries.SessionLocal") as mock_session_local:
        _sync_review_summaries([], [], oci_url="postgresql://db", log=MagicMock())
    mock_session_local.assert_not_called()

    rows = [ReviewRegenReportRow("G1", "20260402", "APPLIED")]
    syncer = MagicMock()
    syncer.sync_review_summaries_for_games.return_value = {"summary": 2}
    with (
        patch("src.cli.regenerate_review_summaries.SessionLocal") as mock_session_local,
        patch("src.cli.regenerate_review_summaries.OCISync", return_value=syncer),
    ):
        mock_session_local.return_value.__enter__.return_value = MagicMock()
        _sync_review_summaries(["G1"], rows, oci_url="postgresql://db", log=MagicMock())

    assert rows[0].oci_status == "synced_summary:2"
    syncer.close.assert_called_once()


def test_regenerate_review_summaries_orchestrator_dry_run(tmp_path):
    report_path = tmp_path / "report.csv"
    processed = [ReviewRegenReportRow("G1", "20260402", "DRY_RUN_READY")]
    with (
        patch("src.cli.regenerate_review_summaries.SessionLocal") as mock_session_local,
        patch("src.cli.regenerate_review_summaries._query_target_games", return_value=[_Game(game_id="G1")]),
        patch("src.cli.regenerate_review_summaries.ContextAggregator"),
        patch("src.cli.regenerate_review_summaries._process_review_games", return_value=(processed, [])),
        patch("src.cli.regenerate_review_summaries._write_report") as mock_write,
    ):
        mock_session_local.return_value.__enter__.return_value = MagicMock()
        rows = regenerate_review_summaries(game_ids=["G1"], report_out=report_path, log=MagicMock())

    assert rows == processed
    mock_write.assert_called_once_with(processed, report_path)


def test_regenerate_review_summaries_apply_commit_failure_rolls_back(tmp_path):
    session = MagicMock()
    session.commit.side_effect = RuntimeError("commit failed")
    with (
        patch("src.cli.regenerate_review_summaries.SessionLocal") as mock_session_local,
        patch("src.cli.regenerate_review_summaries._query_target_games", return_value=[]),
        patch("src.cli.regenerate_review_summaries._write_backup"),
        patch("src.cli.regenerate_review_summaries.ContextAggregator"),
        patch("src.cli.regenerate_review_summaries._process_review_games", return_value=([], [])),
    ):
        mock_session_local.return_value.__enter__.return_value = session
        try:
            regenerate_review_summaries(
                game_ids=["G1"], apply=True, report_out=tmp_path / "report.csv", log=MagicMock()
            )
            raise AssertionError("expected RuntimeError")
        except RuntimeError:
            pass

    session.rollback.assert_called_once()
