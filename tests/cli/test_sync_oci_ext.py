from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.sync_oci import (
    _parse_game_ids,
    build_arg_parser,
    get_available_years,
    run_parallel_sync,
)


class TestParseGameIds:
    def test_none_returns_empty(self):
        assert _parse_game_ids(None) == []

    def test_empty_string_returns_empty(self):
        assert _parse_game_ids("") == []

    def test_comma_separated(self):
        result = _parse_game_ids("2020,2021,2022")
        assert result == ["2020", "2021", "2022"]


class TestBuildArgParser:
    def test_parser_builds(self):
        parser = build_arg_parser()
        assert parser is not None

    def test_basic_args(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--target-url", "postgresql://localhost/test"])
        assert args.target_url == "postgresql://localhost/test"

    def test_game_details_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--game-details", "--target-url", "x"])
        assert args.game_details is True

    def test_season_stats_flag(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--season-stats", "--target-url", "x"])
        assert args.season_stats is True

    def test_workers_default(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--target-url", "x"])
        assert args.workers >= 1


class TestGetAvailableYears:
    def test_returns_years(self):
        mock_session = MagicMock()
        mock_row_1 = (2020,)
        mock_row_2 = (2021,)
        mock_session.query.return_value.select_from.return_value.all.return_value = [mock_row_1, mock_row_2]

        result = get_available_years(mock_session, MagicMock())
        assert 2020 in result
        assert 2021 in result

    def test_disallowed_column_raises(self):
        mock_session = MagicMock()
        with pytest.raises(ValueError, match="Disallowed"):
            get_available_years(mock_session, MagicMock(), column_name="malicious")


class TestRunParallelSync:
    def test_empty_years(self):
        mock_fn = MagicMock()
        run_parallel_sync(mock_fn, "postgresql://localhost/test", [], 1)
        mock_fn.assert_not_called()

    def test_calls_sync_fn(self):
        mock_fn = MagicMock()
        mock_syncer = MagicMock()
        mock_session = MagicMock()
        with patch("src.cli.sync_oci.SessionLocal") as mock_sf:
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.sync_oci.OCISync", return_value=mock_syncer):
                run_parallel_sync(mock_fn, "postgresql://localhost/test", [2020], 1)
                mock_fn.assert_called_once()
