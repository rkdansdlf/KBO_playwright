from dataclasses import dataclass
from datetime import date
from unittest.mock import MagicMock, patch

from src.cli.regenerate_review_summaries import (
    ReviewRegenReportRow,
    _append_missing_review_rows,
    _build_review_report_row,
    _count_crucial_moments,
    _count_noise_moments,
    _load_game_ids_file,
    _mark_review_oci_status,
    _process_review_game,
    _short_hash,
    _skipped_review_row,
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
