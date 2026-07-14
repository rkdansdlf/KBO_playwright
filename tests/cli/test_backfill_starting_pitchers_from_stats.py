from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli.backfill_starting_pitchers_from_stats import (
    _is_blank,
    _normalize_date,
    find_target_missing_ready_games,
    load_candidates,
    main,
    parse_args,
    repair_candidates,
    sync_to_oci,
    update_target_pitcher_fields,
)


class TestBackfillStartingPitchersFromStats:
    def test_dry_run(self):
        mock_args = MagicMock(
            start_date=None,
            end_date=None,
            dry_run=True,
            overwrite=False,
            sync=False,
            sync_target_missing=False,
            limit=None,
        )
        with patch("src.cli.backfill_starting_pitchers_from_stats.parse_args", return_value=mock_args):
            with patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as mock_session:
                mock_session.return_value.__enter__.return_value = MagicMock()
                result = main()
                assert result == 0

    def test_with_dates(self):
        mock_args = MagicMock(
            start_date="20250101",
            end_date="20250131",
            dry_run=True,
            overwrite=False,
            sync=False,
            sync_target_missing=False,
            limit=None,
        )
        with patch("src.cli.backfill_starting_pitchers_from_stats.parse_args", return_value=mock_args):
            with patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as mock_session:
                mock_session.return_value.__enter__.return_value = MagicMock()
                result = main()
                assert result == 0


class TestNormalizeDate:
    def test_none_returns_none(self):
        assert _normalize_date(None) is None

    def test_empty_returns_none(self):
        assert _normalize_date("") is None

    def test_compact_format(self):
        assert _normalize_date("20250615") == "2025-06-15"

    def test_already_formatted(self):
        assert _normalize_date("2025-06-15") == "2025-06-15"


class TestIsBlank:
    def test_none_is_blank(self):
        assert _is_blank(None) is True

    def test_empty_string_is_blank(self):
        assert _is_blank("") is True

    def test_whitespace_is_blank(self):
        assert _is_blank("   ") is True

    def test_value_not_blank(self):
        assert _is_blank("홍길동") is False


class TestRepairCandidates:
    def test_fills_blank_pitchers(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G1",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": "홍길동",
                "home_start": "김철수",
            },
        ]

        ids, away, home = repair_candidates(mock_session, candidates, overwrite=False, dry_run=True)
        assert ids == ["G1"]
        assert away == 1
        assert home == 1

    def test_does_not_overwrite_without_flag(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G2",
                "current_away_pitcher": "기존값",
                "current_home_pitcher": "기존값",
                "away_start": "홍길동",
                "home_start": "김철수",
            },
        ]

        ids, away, home = repair_candidates(mock_session, candidates, overwrite=False, dry_run=True)
        assert ids == []
        assert away == 0
        assert home == 0

    def test_overwrite_with_flag(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G3",
                "current_away_pitcher": "기존값",
                "current_home_pitcher": "기존값",
                "away_start": "홍길동",
                "home_start": "김철수",
            },
        ]

        ids, away, home = repair_candidates(mock_session, candidates, overwrite=True, dry_run=True)
        assert ids == ["G3"]
        assert away == 1
        assert home == 1

    def test_no_change_when_blank_start(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G4",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": None,
                "home_start": None,
            },
        ]

        ids, away, home = repair_candidates(mock_session, candidates, overwrite=False, dry_run=True)
        assert ids == []
        assert away == 0
        assert home == 0

    def test_dry_run_does_not_execute(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G5",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": "홍길동",
                "home_start": "김철수",
            },
        ]

        repair_candidates(mock_session, candidates, overwrite=False, dry_run=True)
        mock_session.execute.assert_not_called()

    def test_wet_run_executes_and_commits(self):
        mock_session = MagicMock()
        candidates = [
            {
                "game_id": "G6",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": "홍길동",
                "home_start": "김철수",
            },
        ]

        repair_candidates(mock_session, candidates, overwrite=False, dry_run=False)
        mock_session.execute.assert_called()
        mock_session.commit.assert_called_once()

    def test_empty_candidates(self):
        mock_session = MagicMock()
        ids, away, home = repair_candidates(mock_session, [], overwrite=False, dry_run=True)
        assert ids == []
        assert away == 0
        assert home == 0


class TestParseArgs:
    def test_default_dry_run(self):
        with patch("sys.argv", ["backfill_starting_pitchers"]):
            args = parse_args()
        assert args.dry_run is False

    def test_dry_run_flag(self):
        with patch("sys.argv", ["backfill_starting_pitchers", "--dry-run"]):
            args = parse_args()
        assert args.dry_run is True

    def test_overwrite_flag(self):
        with patch("sys.argv", ["backfill_starting_pitchers", "--overwrite"]):
            args = parse_args()
        assert args.overwrite is True

    def test_sync_flag(self):
        with patch("sys.argv", ["backfill_starting_pitchers", "--sync"]):
            args = parse_args()
        assert args.sync is True

    def test_limit(self):
        with patch("sys.argv", ["backfill_starting_pitchers", "--limit", "10"]):
            args = parse_args()
        assert args.limit == 10

    def test_dates(self):
        with patch("sys.argv", ["backfill_starting_pitchers", "--start-date", "20250101", "--end-date", "20250131"]):
            args = parse_args()
        assert args.start_date == "20250101"
        assert args.end_date == "20250131"


class TestCandidateAndTargetSync:
    def test_load_candidates_uses_dates_overwrite_and_limit(self):
        session = MagicMock()
        session.execute.return_value.mappings.return_value.all.return_value = [
            {
                "game_id": "G1",
                "game_date": "2025-01-02",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": "Away",
                "home_start": "Home",
            },
        ]
        args = Namespace(start_date="20250101", end_date="2025-01-31", overwrite=True, limit=1)

        candidates = load_candidates(session, args)

        assert candidates == [
            {
                "game_id": "G1",
                "game_date": "2025-01-02",
                "current_away_pitcher": None,
                "current_home_pitcher": None,
                "away_start": "Away",
                "home_start": "Home",
            },
        ]
        assert session.execute.call_args.args[1] == {
            "start_date": "2025-01-01",
            "end_date": "2025-01-31",
            "limit": 1,
        }

    def test_sync_to_oci_counts_successes_failures_and_exceptions(self):
        syncer = MagicMock()
        syncer.sync_specific_game.side_effect = [True, False, RuntimeError("unavailable")]
        with (
            patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value="oci-url"),
            patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
            patch("src.sync.oci_sync.OCISync", return_value=syncer),
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            success, failed = sync_to_oci(["G1", "G2", "G3"])

        assert (success, failed) == (1, 2)
        assert syncer.sync_specific_game.call_count == 3

    def test_sync_to_oci_requires_target_url(self):
        with patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value=None):
            with pytest.raises(RuntimeError, match="OCI_DB_URL"):
                sync_to_oci(["G1"])

    def test_find_target_missing_ready_games_filters_and_sorts_local_rows(self):
        session = MagicMock()
        session.execute.return_value.mappings.return_value.all.return_value = [
            {"game_id": "G2", "away_pitcher": "Away 2", "home_pitcher": "Home 2"},
            {"game_id": "G1", "away_pitcher": "Away 1", "home_pitcher": "Home 1"},
            {"game_id": "G3", "away_pitcher": "Away 3", "home_pitcher": "Home 3"},
        ]
        target_engine = MagicMock()
        target_engine.connect.return_value.__enter__.return_value.execute.return_value = [("G2",), ("G1",)]
        args = Namespace(start_date="20250101", end_date="20250131")

        with (
            patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value="oci-url"),
            patch("src.cli.backfill_starting_pitchers_from_stats.create_engine", return_value=target_engine),
        ):
            rows = find_target_missing_ready_games(session, args)

        assert [row["game_id"] for row in rows] == ["G1", "G2"]

    def test_find_target_missing_ready_games_returns_early_when_target_has_no_candidates(self):
        session = MagicMock()
        target_engine = MagicMock()
        target_engine.connect.return_value.__enter__.return_value.execute.return_value = []
        args = Namespace(start_date=None, end_date=None)

        with (
            patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value="oci-url"),
            patch("src.cli.backfill_starting_pitchers_from_stats.create_engine", return_value=target_engine),
        ):
            assert find_target_missing_ready_games(session, args) == []

        session.execute.assert_not_called()

    def test_find_target_missing_ready_games_requires_target_url(self):
        with patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value=None):
            with pytest.raises(RuntimeError, match="sync-target-missing"):
                find_target_missing_ready_games(MagicMock(), Namespace(start_date=None, end_date=None))

    def test_update_target_pitcher_fields_updates_rows_and_handles_empty_input(self):
        target_engine = MagicMock()
        target_engine.begin.return_value.__enter__.return_value.execute.return_value.rowcount = 2
        rows = [
            {"game_id": "G1", "away_pitcher": "Away", "home_pitcher": "Home"},
            {"game_id": "G2", "away_pitcher": "Away", "home_pitcher": "Home"},
        ]

        assert update_target_pitcher_fields([]) == 0
        with (
            patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value="oci-url"),
            patch("src.cli.backfill_starting_pitchers_from_stats.create_engine", return_value=target_engine),
        ):
            assert update_target_pitcher_fields(rows) == 2

        target_engine.begin.return_value.__enter__.return_value.execute.assert_called_once()

    def test_update_target_pitcher_fields_requires_target_url(self):
        with patch("src.cli.backfill_starting_pitchers_from_stats.get_oci_url", return_value=None):
            with pytest.raises(RuntimeError, match="OCI update"):
                update_target_pitcher_fields([{"game_id": "G1"}])


class TestMainSyncPaths:
    @staticmethod
    def _args(**overrides):
        values = {
            "start_date": None,
            "end_date": None,
            "dry_run": False,
            "overwrite": False,
            "sync": False,
            "sync_target_missing": False,
            "limit": None,
        }
        values.update(overrides)
        return Namespace(**values)

    def test_sync_target_missing_dry_run_skips_update(self):
        with (
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.parse_args",
                return_value=self._args(dry_run=True, sync_target_missing=True),
            ),
            patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
            patch("src.cli.backfill_starting_pitchers_from_stats.load_candidates", return_value=[]),
            patch("src.cli.backfill_starting_pitchers_from_stats.repair_candidates", return_value=([], 0, 0)),
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.find_target_missing_ready_games",
                return_value=[{"game_id": "G1"}],
            ),
            patch("src.cli.backfill_starting_pitchers_from_stats.update_target_pitcher_fields") as update_target,
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert main() == 0

        update_target.assert_not_called()

    def test_sync_target_missing_updates_ready_rows(self):
        with (
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.parse_args",
                return_value=self._args(sync_target_missing=True),
            ),
            patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
            patch("src.cli.backfill_starting_pitchers_from_stats.load_candidates", return_value=[]),
            patch("src.cli.backfill_starting_pitchers_from_stats.repair_candidates", return_value=([], 0, 0)),
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.find_target_missing_ready_games",
                return_value=[{"game_id": "G1"}],
            ),
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.update_target_pitcher_fields", return_value=1
            ) as update_target,
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert main() == 0

        update_target.assert_called_once_with([{"game_id": "G1"}])

    def test_dry_run_skips_oci_sync(self):
        with (
            patch(
                "src.cli.backfill_starting_pitchers_from_stats.parse_args",
                return_value=self._args(dry_run=True, sync=True),
            ),
            patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
            patch("src.cli.backfill_starting_pitchers_from_stats.load_candidates", return_value=[]),
            patch("src.cli.backfill_starting_pitchers_from_stats.repair_candidates", return_value=(["G1"], 1, 1)),
            patch("src.cli.backfill_starting_pitchers_from_stats.sync_to_oci") as sync,
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert main() == 0

        sync.assert_not_called()

    def test_sync_returns_failure_status_when_oci_has_failures(self):
        with (
            patch("src.cli.backfill_starting_pitchers_from_stats.parse_args", return_value=self._args(sync=True)),
            patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
            patch("src.cli.backfill_starting_pitchers_from_stats.load_candidates", return_value=[]),
            patch("src.cli.backfill_starting_pitchers_from_stats.repair_candidates", return_value=(["G1"], 1, 1)),
            patch("src.cli.backfill_starting_pitchers_from_stats.sync_to_oci", return_value=(0, 1)) as sync,
        ):
            session_local.return_value.__enter__.return_value = MagicMock()

            assert main() == 1

        sync.assert_called_once_with(["G1"])
