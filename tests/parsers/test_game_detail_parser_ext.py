"""Tests for game_detail_parser.py — coverage of previously-uncovered branches."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.parsers.game_detail_parser import (
    _build_hitter_payload,
    _build_pitcher_payload,
    _build_team_info,
    _parse_duration_minutes,
    _resolve_missing_player_id,
    _safe_player_id,
    _season_year_from_game,
)


class TestSeasonYearFromGameUnicodeDigits:
    def test_superscript_digits_trigger_value_error(self):
        assert _season_year_from_game("²²²²") is None

    def test_mixed_valid_and_superscript(self):
        assert _season_year_from_game("20²²") is None


class TestSafePlayerIdEdgeCases2:
    def test_value_with_trailing_whitespace(self):
        assert _safe_player_id("  42  ") == 42

    def test_digit_with_plus_prefix(self):
        assert _safe_player_id("+123") is None


class TestParseDurationMinutesEdgeCases2:
    def test_zero_hours(self):
        assert _parse_duration_minutes("0:00") == 0

    def test_leading_zeros(self):
        assert _parse_duration_minutes("01:05") == 65


class TestBuildTeamInfoHomeRowNone:
    def test_scoreboard_with_single_row(self):
        df = pd.DataFrame({"팀": ["LG"], "R": [5], "H": [10], "E": [0]})
        teams = _build_team_info(df, "20250325LGSS0", 2025)
        assert teams["home"]["name"] is None
        assert teams["home"]["score"] is None


class TestBuildHitterPayloadWithDbSession:
    def test_resolve_missing_player_id_called(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        df = pd.DataFrame(
            {
                "선수": ["홍길동"],
                "선수ID": [None],
                "타수": [3],
                "안타": [1],
            },
        )
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_hitter_payload([df], teams, db_session=mock_session)
        assert result["away"][0]["player_id"] is None
        mock_session.execute.assert_called_once()

    def test_resolve_missing_player_id_with_playerId_col(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        df = pd.DataFrame(
            {
                "선수명": ["김철수"],
                "playerId": ["N/A"],
                "타수": [4],
                "안타": [2],
            },
        )
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_hitter_payload([df], teams, db_session=mock_session)
        assert result["away"][0]["player_id"] is None


class TestBuildPitcherPayloadWithDbSession:
    def test_resolve_missing_player_id_called(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        df = pd.DataFrame(
            {
                "선수": ["투수1"],
                "선수ID": [None],
                "이닝": ["5.0"],
                "삼진": [3],
            },
        )
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams, db_session=mock_session)
        assert result["away"][0]["player_id"] is None
        mock_session.execute.assert_called_once()

    def test_resolve_missing_player_id_with_playerId_col(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        df = pd.DataFrame(
            {
                "선수명": ["투수2"],
                "playerId": ["N/A"],
                "IP": ["3.0"],
                "삼진": [2],
            },
        )
        teams = {"away": {"code": "LG"}, "home": {"code": "SS"}}
        result = _build_pitcher_payload([df], teams, db_session=mock_session)
        assert result["away"][0]["player_id"] is None


class TestResolveMissingPlayerId:
    def test_no_session_returns_none(self):
        assert _resolve_missing_player_id(None, "홍길동", "LG") is None

    def test_empty_name_returns_none(self):
        mock_session = MagicMock()
        assert _resolve_missing_player_id(mock_session, "", "LG") is None

    def test_no_rows_returns_none(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_session.execute.return_value = mock_result

        assert _resolve_missing_player_id(mock_session, "홍길동", "LG") is None

    def test_single_row_returns_player_id(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(12345, "LG 트윈스")]
        mock_session.execute.return_value = mock_result

        assert _resolve_missing_player_id(mock_session, "홍길동", "LG") == 12345

    def test_multiple_rows_team_match(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (11111, "LG 트윈스"),
            (22222, "한화 이글스"),
        ]
        mock_session.execute.return_value = mock_result

        with patch("src.utils.team_codes.STANDARD_TEAM_CODES", {"HH": {"name": "한화"}}):
            result = _resolve_missing_player_id(mock_session, "김철수", "HH")
            assert result == 22222

    def test_multiple_rows_no_team_match_returns_first(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (33333, "LG 트윈스"),
            (44444, "삼성 라이온즈"),
        ]
        mock_session.execute.return_value = mock_result

        with patch("src.utils.team_codes.STANDARD_TEAM_CODES", {"HH": {"name": "한화"}}):
            result = _resolve_missing_player_id(mock_session, "박민수", "HH")
            assert result == 33333

    def test_multiple_rows_with_null_team_skipped(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            (55555, None),
            (66666, "한화 이글스"),
        ]
        mock_session.execute.return_value = mock_result

        with patch("src.utils.team_codes.STANDARD_TEAM_CODES", {"HH": {"name": "한화"}}):
            result = _resolve_missing_player_id(mock_session, "이영희", "HH")
            assert result == 66666

    def test_sqlalchemy_error_returns_none(self):
        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError("connection lost")

        assert _resolve_missing_player_id(mock_session, "홍길동", "LG") is None

    def test_name_with_space_handling(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(77777, "KT 위즈")]
        mock_session.execute.return_value = mock_result

        result = _resolve_missing_player_id(mock_session, "김 태연", "KT")
        assert result == 77777
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["n1"] == "김태연"
        assert call_args[0][1]["n2"] == "김 태연"

    def test_short_name_no_space_insert(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(88888, "NC 다이노스")]
        mock_session.execute.return_value = mock_result

        result = _resolve_missing_player_id(mock_session, "김", "NC")
        assert result == 88888
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["n1"] == "김"
        assert call_args[0][1]["n2"] == "김"

    def test_two_char_name_no_space_insert(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(99999, "두산 베어스")]
        mock_session.execute.return_value = mock_result

        result = _resolve_missing_player_id(mock_session, "김태", "DB")
        assert result == 99999
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["n1"] == "김태"
        assert call_args[0][1]["n2"] == "김태"

    def test_three_char_name_space_insert(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(10101, "KT 위즈")]
        mock_session.execute.return_value = mock_result

        result = _resolve_missing_player_id(mock_session, "김태연", "KT")
        assert result == 10101
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["n1"] == "김태연"
        assert call_args[0][1]["n2"] == "김 태연"
