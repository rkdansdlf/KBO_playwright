from argparse import Namespace
from unittest.mock import MagicMock, patch

from src.cli.backfill_starting_pitchers_from_stats import (
    _is_blank,
    _normalize_date,
    load_candidates,
    main,
    parse_args,
    repair_candidates,
)


def _args(**overrides: object) -> Namespace:
    values: dict[str, object] = {
        "start_date": None,
        "end_date": None,
        "dry_run": False,
        "overwrite": False,
        "limit": None,
    }
    values.update(overrides)
    return Namespace(**values)


def test_normalize_date_and_blank_helpers() -> None:
    assert _normalize_date(None) is None
    assert _normalize_date("") is None
    assert _normalize_date("20250615") == "2025-06-15"
    assert _normalize_date("2025-06-15") == "2025-06-15"
    assert _is_blank(None)
    assert _is_blank("   ")
    assert not _is_blank("pitcher")


def test_parse_args_supports_local_repair_options() -> None:
    with patch("sys.argv", ["backfill_starting_pitchers", "--dry-run", "--overwrite", "--limit", "10"]):
        args = parse_args()
    assert args.dry_run is True
    assert args.overwrite is True
    assert args.limit == 10


def test_load_candidates_normalizes_filters_and_limit() -> None:
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

    assert load_candidates(session, _args(start_date="20250101", end_date="20250131", overwrite=True, limit=1))
    assert session.execute.call_args.args[1] == {
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "limit": 1,
    }


def test_repair_candidates_dry_run_does_not_write() -> None:
    session = MagicMock()
    candidates = [
        {
            "game_id": "G1",
            "current_away_pitcher": None,
            "current_home_pitcher": None,
            "away_start": "Away",
            "home_start": "Home",
        },
    ]

    ids, away, home = repair_candidates(session, candidates, overwrite=False, dry_run=True)

    assert (ids, away, home) == (["G1"], 1, 1)
    session.execute.assert_not_called()
    session.commit.assert_not_called()


def test_repair_candidates_writes_and_commits() -> None:
    session = MagicMock()
    candidates = [
        {
            "game_id": "G1",
            "current_away_pitcher": None,
            "current_home_pitcher": None,
            "away_start": "Away",
            "home_start": "Home",
        },
    ]

    repair_candidates(session, candidates, overwrite=False, dry_run=False)

    session.execute.assert_called_once()
    session.commit.assert_called_once()


def test_repair_candidates_respects_overwrite_flag() -> None:
    session = MagicMock()
    candidates = [
        {
            "game_id": "G1",
            "current_away_pitcher": "Existing",
            "current_home_pitcher": "Existing",
            "away_start": "Away",
            "home_start": "Home",
        },
    ]

    unchanged = repair_candidates(session, candidates, overwrite=False, dry_run=True)
    overwritten = repair_candidates(session, candidates, overwrite=True, dry_run=True)

    assert unchanged == ([], 0, 0)
    assert overwritten == (["G1"], 1, 1)


def test_main_runs_local_repair_only() -> None:
    args = _args(dry_run=True)
    session = MagicMock()
    with (
        patch("src.cli.backfill_starting_pitchers_from_stats.parse_args", return_value=args),
        patch("src.cli.backfill_starting_pitchers_from_stats.SessionLocal") as session_local,
        patch("src.cli.backfill_starting_pitchers_from_stats.load_candidates", return_value=[]),
        patch("src.cli.backfill_starting_pitchers_from_stats.repair_candidates", return_value=([], 0, 0)),
    ):
        session_local.return_value.__enter__.return_value = session
        assert main() == 0
