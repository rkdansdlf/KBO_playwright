from __future__ import annotations

import asyncio
import json
import logging
from argparse import ArgumentTypeError
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.backfill_pregame_previews import (
    PregameBackfillDate,
    _is_incomplete_after_backfill,
    _log_backfill_result,
    _log_backfill_targets,
    _preview_detail_has_starters,
    _resolve_backfill_range,
    _yyyymmdd,
    build_arg_parser,
    find_missing_pregame_dates,
    get_pregame_date_status,
    main,
    run_backfill,
)


def _run_and_close(coro):
    coro.close()
    return 0


class TestYyyymmdd:
    def test_valid_date(self):
        assert _yyyymmdd("20250615") == "20250615"

    def test_with_dashes(self):
        assert _yyyymmdd("2025-06-15") == "20250615"

    def test_invalid_length(self):
        with pytest.raises(ArgumentTypeError):
            _yyyymmdd("2025")

    def test_non_numeric(self):
        with pytest.raises(ArgumentTypeError):
            _yyyymmdd("2025ABCD")


class TestPreviewDetailHasStarters:
    def test_has_both_starters(self):
        detail = json.dumps({"away_starter": "홍길동", "home_starter": "김철수"})
        assert _preview_detail_has_starters(detail) is True

    def test_missing_away_starter(self):
        detail = json.dumps({"away_starter": "", "home_starter": "김철수"})
        assert _preview_detail_has_starters(detail) is False

    def test_missing_home_starter(self):
        detail = json.dumps({"away_starter": "홍길동", "home_starter": None})
        assert _preview_detail_has_starters(detail) is False

    def test_none_detail(self):
        assert _preview_detail_has_starters(None) is False

    def test_invalid_json(self):
        assert _preview_detail_has_starters("not json") is False

    def test_not_a_dict(self):
        assert _preview_detail_has_starters(json.dumps([1, 2, 3])) is False

    def test_no_starter_keys(self):
        assert _preview_detail_has_starters(json.dumps({"other": "data"})) is False

    def test_whitespace_only_starters(self):
        detail = json.dumps({"away_starter": "   ", "home_starter": "김철수"})
        assert _preview_detail_has_starters(detail) is False


class TestFindMissingPregameDates:
    def test_returns_empty_when_no_rows(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.all.return_value = []

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250102")
        assert result == []

    def test_filters_complete_dates(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = "홍길동"
        mock_row.home_pitcher = "김철수"
        mock_row.preview_detail_text = json.dumps({"away_starter": "홍길동", "home_starter": "김철수"})
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250101")
        assert len(result) == 0

    def test_includes_incomplete_date(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = None
        mock_row.home_pitcher = None
        mock_row.preview_detail_text = None
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250101")
        assert len(result) == 1
        assert result[0].scheduled_total == 1

    def test_include_complete_flag(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = "홍길동"
        mock_row.home_pitcher = "김철수"
        mock_row.preview_detail_text = json.dumps({"away_starter": "홍길동", "home_starter": "김철수"})
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250101", include_complete=True)
        assert len(result) == 1

    def test_limit_dates(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        rows = []
        for i in range(5):
            mock_row = MagicMock()
            mock_row.target_date = f"2025010{i + 1}"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            rows.append(mock_row)
        mock_session.execute.return_value.all.return_value = rows

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250105", limit_dates=2)
        assert len(result) == 2

    def test_multiple_games_same_date(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        rows = []
        for i in range(3):
            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            rows.append(mock_row)
        mock_session.execute.return_value.all.return_value = rows

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250101")
        assert len(result) == 1
        assert result[0].scheduled_total == 3

    def test_preview_missing_starters_counted(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = "홍길동"
        mock_row.home_pitcher = "김철수"
        mock_row.preview_detail_text = json.dumps({"other": "data"})
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = find_missing_pregame_dates(start_date="20250101", end_date="20250101")
        assert len(result) == 1
        assert result[0].preview_missing_starters == 1


class TestGetPregameDateStatus:
    def test_returns_status(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = None
        mock_row.home_pitcher = None
        mock_row.preview_detail_text = None
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = get_pregame_date_status("20250101")
        assert result is not None
        assert result.target_date == "20250101"

    def test_returns_none_when_empty(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.all.return_value = []

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = get_pregame_date_status("20250101")
        assert result is None


class TestIsIncompleteAfterBackfill:
    def test_complete_returns_none(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = "홍길동"
        mock_row.home_pitcher = "김철수"
        mock_row.preview_detail_text = json.dumps({"away_starter": "홍길동", "home_starter": "김철수"})
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = _is_incomplete_after_backfill("20250101")
        assert result is None

    def test_incomplete_returns_message(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)

        mock_row = MagicMock()
        mock_row.target_date = "20250101"
        mock_row.away_pitcher = None
        mock_row.home_pitcher = None
        mock_row.preview_detail_text = None
        mock_session.execute.return_value.all.return_value = [mock_row]

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = _is_incomplete_after_backfill("20250101")
        assert result is not None
        assert "starters" in result

    def test_no_scheduled_games_returns_none(self):
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.all.return_value = []

        with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
            result = _is_incomplete_after_backfill("20250101")
        assert result is None


class TestBuildArgParser:
    def test_default_args(self):
        parser = build_arg_parser()
        args = parser.parse_args([])
        assert args.days_ahead == 1
        assert args.dry_run is False
        assert args.include_complete is False
        assert args.fail_on_empty is False
        assert args.fail_on_incomplete is False

    def test_dry_run(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_limit_dates(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--limit-dates", "5"])
        assert args.limit_dates == 5

    def test_fail_on_empty(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--fail-on-empty"])
        assert args.fail_on_empty is True

    def test_fail_on_incomplete(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--fail-on-incomplete"])
        assert args.fail_on_incomplete is True

    def test_no_sync(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--no-sync"])
        assert args.no_sync is True


class TestResolveBackfillRange:
    def test_explicit_dates(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250110"])
        start, end = _resolve_backfill_range(args)
        assert start == "20250101"
        assert end == "20250110"

    def test_default_dates(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--days-ahead", "3"])
        start, end = _resolve_backfill_range(args)
        assert len(start) == 8
        assert len(end) == 8


class TestLogBackfillTargets:
    def test_logs_targets(self, caplog):
        targets = [
            PregameBackfillDate("20250101", 2, 1, 2, 1),
            PregameBackfillDate("20250102", 3, 3, 3, 0),
        ]
        with caplog.at_level(logging.INFO):
            _log_backfill_targets("20250101", "20250102", targets)
        assert "2 date(s)" in caplog.text


class TestLogBackfillResult:
    def test_logs_success(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_backfill_result(5, [], [])
        assert "saved_total=5" in caplog.text

    def test_logs_failed_dates(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_backfill_result(3, ["20250101"], [])
        assert "failed_empty=1" in caplog.text
        assert "20250101" in caplog.text

    def test_logs_incomplete(self, caplog):
        with caplog.at_level(logging.INFO):
            _log_backfill_result(3, [], ["20250101: starters=1/2"])
        assert "incomplete=1" in caplog.text


class TestRunBackfill:
    def test_no_targets_returns_zero(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)
            mock_session.execute.return_value.all.return_value = []

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                mock_session.__enter__.return_value = mock_session
                parser = build_arg_parser()
                args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250101"])
                result = await run_backfill(args)
                assert result == 0

        asyncio.run(_run())

    def test_dry_run_returns_zero(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            mock_session.execute.return_value.all.return_value = [mock_row]

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                parser = build_arg_parser()
                args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250101", "--dry-run"])
                result = await run_backfill(args)
                assert result == 0

        asyncio.run(_run())

    def test_backfill_with_targets(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            mock_session.execute.return_value.all.return_value = [mock_row]

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                with patch("src.cli.backfill_pregame_previews.run_preview_batch", new_callable=AsyncMock) as mock_batch:
                    mock_batch.return_value = ["game1", "game2"]
                    with patch("src.cli.backfill_pregame_previews._is_incomplete_after_backfill", return_value=None):
                        parser = build_arg_parser()
                        args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250101"])
                        result = await run_backfill(args)
                        assert result == 0
                        mock_batch.assert_called_once_with("20250101", sync_to_oci=True)

        asyncio.run(_run())

    def test_backfill_fail_on_empty(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            mock_session.execute.return_value.all.return_value = [mock_row]

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                with patch("src.cli.backfill_pregame_previews.run_preview_batch", new_callable=AsyncMock) as mock_batch:
                    mock_batch.return_value = []
                    parser = build_arg_parser()
                    args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250101", "--fail-on-empty"])
                    result = await run_backfill(args)
                    assert result == 1

        asyncio.run(_run())

    def test_backfill_fail_on_incomplete(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            mock_session.execute.return_value.all.return_value = [mock_row]

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                with patch("src.cli.backfill_pregame_previews.run_preview_batch", new_callable=AsyncMock) as mock_batch:
                    mock_batch.return_value = ["game1"]
                    with patch(
                        "src.cli.backfill_pregame_previews._is_incomplete_after_backfill", return_value="incomplete"
                    ):
                        parser = build_arg_parser()
                        args = parser.parse_args(
                            ["--start-date", "20250101", "--end-date", "20250101", "--fail-on-incomplete"]
                        )
                        result = await run_backfill(args)
                        assert result == 1

        asyncio.run(_run())

    def test_backfill_no_sync_flag(self):
        async def _run():
            mock_session = MagicMock()
            mock_session.__enter__ = MagicMock(return_value=mock_session)
            mock_session.__exit__ = MagicMock(return_value=False)

            mock_row = MagicMock()
            mock_row.target_date = "20250101"
            mock_row.away_pitcher = None
            mock_row.home_pitcher = None
            mock_row.preview_detail_text = None
            mock_session.execute.return_value.all.return_value = [mock_row]

            with patch("src.cli.backfill_pregame_previews.SessionLocal", return_value=mock_session):
                with patch("src.cli.backfill_pregame_previews.run_preview_batch", new_callable=AsyncMock) as mock_batch:
                    mock_batch.return_value = ["game1"]
                    with patch("src.cli.backfill_pregame_previews._is_incomplete_after_backfill", return_value=None):
                        parser = build_arg_parser()
                        args = parser.parse_args(["--start-date", "20250101", "--end-date", "20250101", "--no-sync"])
                        result = await run_backfill(args)
                        assert result == 0
                        mock_batch.assert_called_once_with("20250101", sync_to_oci=False)

        asyncio.run(_run())


class TestMain:
    def test_dry_run(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.side_effect = _run_and_close
            result = main(["--dry-run"])
            assert result == 0

    def test_specific_dates(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.side_effect = _run_and_close
            result = main(["--start-date", "20250401", "--end-date", "20250402", "--dry-run"])
            assert result == 0

    def test_with_include_complete(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.side_effect = _run_and_close
            result = main(["--include-complete", "--days-ahead", "3", "--dry-run"])
            assert result == 0

    def test_no_args(self):
        with patch("src.cli.backfill_pregame_previews.asyncio.run") as mock_run:
            mock_run.side_effect = _run_and_close
            result = main([])
            assert result == 0
