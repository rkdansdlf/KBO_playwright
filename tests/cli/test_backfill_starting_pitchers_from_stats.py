from unittest.mock import MagicMock, patch

import pytest

from src.cli.backfill_starting_pitchers_from_stats import (
    _is_blank,
    _normalize_date,
    main,
    parse_args,
    repair_candidates,
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
