"""Unit tests for Historical Detail Backfill Service and CLI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.cli.backfill_historical_details import build_parser, main
from src.services.historical_detail_backfill_service import HistoricalDetailBackfillService


def test_cli_argument_parsing() -> None:
    """Test CLI argument parsing for backfill_historical_details."""
    parser = build_parser()

    args_single = parser.parse_args(["--year", "2009", "--limit", "5", "--save"])
    assert args_single.year == 2009
    assert args_single.limit == 5
    assert args_single.save is True

    args_range = parser.parse_args(["--start-year", "2001", "--end-year", "2005", "--delay", "2.0"])
    assert args_range.start_year == 2001
    assert args_range.end_year == 2005
    assert args_range.delay == 2.0
    assert args_range.save is False


def test_get_missing_games() -> None:
    """Test get_missing_games queries database properly."""
    mock_session = MagicMock()
    service = HistoricalDetailBackfillService(session=mock_session)

    mock_row1 = MagicMock(game_id="20090404HHSK0", game_date="2009-04-04", home_team="SK", away_team="HH")
    mock_session.execute.return_value.all.return_value = [mock_row1]

    missing = service.get_missing_games(2009, limit=10)
    assert len(missing) == 1
    assert missing[0]["game_id"] == "20090404HHSK0"
    assert missing[0]["game_date"] == "20090404"


def test_process_game_dry_run_success() -> None:
    """Test process_game in dry-run mode."""
    mock_session = MagicMock()
    service = HistoricalDetailBackfillService(session=mock_session)

    mock_page = MagicMock()
    game_info = {"game_id": "20090404HHSK0", "game_date": "20090404"}

    valid_payload = {
        "game_id": "20090404HHSK0",
        "teams": {"home": "SK", "away": "HH"},
        "hitters": {"away": [{"name": "이영우"}], "home": [{"name": "정근우"}]},
        "pitchers": {"away": [{"name": "류현진"}], "home": [{"name": "김광현"}]},
    }

    with patch.object(service.crawler, "extract_game_details", return_value=valid_payload):
        ok, msg = service.process_game(mock_page, game_info, dry_run=True)
        assert ok is True
        assert msg == "validated_dry_run"


def test_process_game_validation_failure() -> None:
    """Test process_game when boxscore validation fails."""
    mock_session = MagicMock()
    service = HistoricalDetailBackfillService(session=mock_session)

    mock_page = MagicMock()
    game_info = {"game_id": "20090404HHSK0", "game_date": "20090404"}

    # Incomplete payload (no away pitchers)
    invalid_payload = {
        "game_id": "20090404HHSK0",
        "teams": {"home": "SK", "away": "HH"},
        "hitters": {"away": [{"name": "이영우"}], "home": [{"name": "정근우"}]},
        "pitchers": {"away": [], "home": [{"name": "김광현"}]},
    }

    with patch.object(service.crawler, "extract_game_details", return_value=invalid_payload):
        ok, msg = service.process_game(mock_page, game_info, dry_run=False)
        assert ok is False
        assert "validation_failed" in msg


def test_cli_main_dry_run() -> None:
    """Test executing CLI main function in dry-run mode."""
    with (
        patch("src.cli.backfill_historical_details.HistoricalDetailBackfillService") as mock_cls,
        patch("src.cli.backfill_historical_details.SessionLocal"),
    ):
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        mock_metric = MagicMock(
            year=2009,
            total_missing=10,
            attempted=2,
            saved=2,
            skipped_validation=0,
            failed=0,
            to_dict=MagicMock(return_value={"year": 2009, "saved": 2}),
        )
        mock_instance.run_backfill.return_value = [mock_metric]

        ret = main(["--year", "2009", "--limit", "2"])
        assert ret == 0
        mock_instance.run_backfill.assert_called_once_with(
            start_year=2009,
            end_year=2009,
            limit_per_season=2,
            dry_run=True,
            headless=True,
        )
