from __future__ import annotations

from datetime import date as date_cls
from unittest.mock import MagicMock, patch

from src.cli.live_boxscore import (
    _build_game_payload,
    _fetch_inning_scores,
    _fetch_live_games,
    _format_text,
    _resolve_statuses,
    _resolve_target_date,
    main,
)


class TestResolveTargetDate:
    def test_valid_date_string(self) -> None:
        result = _resolve_target_date("20260627")
        assert result == date_cls(2026, 6, 27)

    def test_invalid_format_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid date format"):
            _resolve_target_date("2026-06-27")

    def test_invalid_length_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid date format"):
            _resolve_target_date("2026062")


class TestResolveStatuses:
    def test_default(self) -> None:
        result = _resolve_statuses(None)
        assert result == ("live", "in_progress", "delayed", "suspended")

    def test_custom(self) -> None:
        result = _resolve_statuses("COMPLETED")
        assert result == ("COMPLETED",)

    def test_multiple(self) -> None:
        result = _resolve_statuses("COMPLETED,DRAW")
        assert result == ("COMPLETED", "DRAW")


class TestBuildGamePayload:
    def test_basic_payload(self) -> None:
        game = MagicMock()
        game.game_id = "20260627HTOB0"
        game.game_date = date_cls(2026, 6, 27)
        game.game_status = "COMPLETED"
        game.away_team = "KIA"
        game.home_team = "DB"
        game.away_score = 2
        game.home_score = 3
        game.stadium = "잠실"

        innings = [
            MagicMock(team_side="away", team_code="KIA", inning=1, runs=0),
            MagicMock(team_side="away", team_code="KIA", inning=2, runs=1),
            MagicMock(team_side="home", team_code="DB", inning=1, runs=0),
            MagicMock(team_side="home", team_code="DB", inning=2, runs=2),
        ]

        payload = _build_game_payload(game, innings)
        assert payload["game_id"] == "20260627HTOB0"
        assert payload["away"]["runs"] == 1
        assert payload["home"]["runs"] == 2
        assert payload["away"]["line_score"] == [0, 1]
        assert payload["home"]["line_score"] == [0, 2]

    def test_empty_innings(self) -> None:
        game = MagicMock()
        game.game_id = "20260627HTOB0"
        game.game_date = date_cls(2026, 6, 27)
        game.game_status = "SCHEDULED"
        game.away_team = "KIA"
        game.home_team = "DB"
        game.away_score = None
        game.home_score = None
        game.stadium = None

        payload = _build_game_payload(game, [])
        assert payload["away"]["runs"] == 0
        assert payload["away"]["line_score"] == []
        assert payload["away"]["code"] == "KIA"


class TestFormatText:
    def test_basic_format(self) -> None:
        payload = {
            "game_id": "20260627HTOB0",
            "game_status": "COMPLETED",
            "away": {"code": "KIA", "runs": 2, "line_score": [0, 0, 1, 0, 0, 0, 0, 1, 0]},
            "home": {"code": "DB", "runs": 3, "line_score": [1, 0, 0, 1, 0, 1, 0, 0, 0]},
        }
        result = _format_text(payload)
        assert "20260627HTOB0" in result
        assert "KIA 2 vs DB 3" in result

    def test_none_inning_displayed_as_dash(self) -> None:
        payload = {
            "game_id": "20260627HTOB0",
            "game_status": "LIVE",
            "away": {"code": "KIA", "runs": 0, "line_score": [0, 0, None]},
            "home": {"code": "DB", "runs": 0, "line_score": [0, None, None]},
        }
        result = _format_text(payload)
        assert "-" in result


class TestMain:
    def test_json_output(self) -> None:
        with patch("src.cli.live_boxscore.SessionLocal") as mock_session_factory:
            mock_session = MagicMock()
            mock_session_factory.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)
            mock_session.execute.return_value.scalars.return_value.all.return_value = []

            result = main(["--date", "20260627", "--status", "COMPLETED", "--json"])
            assert result == 0

    def test_invalid_date_returns_1(self) -> None:
        result = main(["--date", "invalid"])
        assert result == 1
