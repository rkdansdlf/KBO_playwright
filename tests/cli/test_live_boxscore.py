from __future__ import annotations

from datetime import date as date_cls
from unittest.mock import MagicMock, patch

from src.cli.live_boxscore import (
    _build_game_payload,
    _configure_cli_logging,
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

    def test_empty_line_score_omits_inning_box(self) -> None:
        payload = {
            "game_id": "20260627HTOB0",
            "game_status": "SCHEDULED",
            "away": {"code": "KIA", "runs": 0, "line_score": []},
            "home": {"code": "DB", "runs": 0, "line_score": []},
        }
        result = _format_text(payload)
        assert "20260627HTOB0" in result
        assert "KIA 0 vs DB 0" in result
        assert len(result.splitlines()) == 1


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

    def test_default_uses_today(self) -> None:
        result = _resolve_target_date(None)
        assert isinstance(result, date_cls)

    def test_configure_cli_logging_adds_handler_when_missing(self) -> None:
        import logging as log_mod

        saved = list(log_mod.getLogger().handlers)
        log_mod.getLogger().handlers.clear()
        try:
            _configure_cli_logging()
        finally:
            log_mod.getLogger().handlers[:] = saved
        assert log_mod.getLogger().handlers

    def test_main_no_games_text(self) -> None:
        with patch("src.cli.live_boxscore.SessionLocal") as mock_session_factory:
            session = MagicMock()
            mock_session_factory.return_value.__enter__.return_value = session
            mock_session_factory.return_value.__exit__.return_value = False
            session.execute.return_value.scalars.return_value.all.return_value = []

            result = main(["--date", "20260627"])
        assert result == 0

    def test_main_with_games_json(self) -> None:
        game = MagicMock()
        game.game_id = "20260627HTOB0"
        game.game_date = date_cls(2026, 6, 27)
        game.game_status = "LIVE"
        game.away_team = "KIA"
        game.home_team = "DB"
        game.away_score = 1
        game.home_score = 2
        game.stadium = "잠실"
        with patch("src.cli.live_boxscore.SessionLocal") as mock_session_factory:
            session = MagicMock()
            mock_session_factory.return_value.__enter__.return_value = session
            mock_session_factory.return_value.__exit__.return_value = False
            session.execute.return_value.scalars.return_value.all.side_effect = [[game], []]

            result = main(["--date", "20260627", "--json"])
        assert result == 0

    def test_main_with_games_text(self) -> None:
        game = MagicMock()
        game.game_id = "20260627HTOB0"
        game.game_date = date_cls(2026, 6, 27)
        game.game_status = "LIVE"
        game.away_team = "KIA"
        game.home_team = "DB"
        game.away_score = 1
        game.home_score = 2
        game.stadium = "잠실"
        with patch("src.cli.live_boxscore.SessionLocal") as mock_session_factory:
            session = MagicMock()
            mock_session_factory.return_value.__enter__.return_value = session
            mock_session_factory.return_value.__exit__.return_value = False
            session.execute.return_value.scalars.return_value.all.side_effect = [[game], []]

            result = main(["--date", "20260627"])
        assert result == 0


class TestFetchLiveGames:
    def test_game_id_filter_is_applied(self) -> None:
        session = MagicMock()
        game = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = [game]

        result = _fetch_live_games(session, "20260627", "20260627HTOB0", 20, ("live",))

        assert result == [game]


class TestFetchInningScores:
    def test_empty_game_ids_returns_empty(self) -> None:
        assert _fetch_inning_scores(MagicMock(), []) == {}

    def test_groups_rows_by_game(self) -> None:
        session = MagicMock()
        row1 = MagicMock(game_id="g1", team_side="away")
        row2 = MagicMock(game_id="g1", team_side="home")
        session.execute.return_value.scalars.return_value.all.return_value = [row1, row2]

        result = _fetch_inning_scores(session, ["g1"])

        assert set(result.keys()) == {"g1"}
