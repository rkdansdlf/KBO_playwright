from __future__ import annotations

import logging
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.cli.calculate_standings import (
    GameResultData,
    StandingsCalculator,
    TeamStandingsDaily,
    TeamState,
    _apply_game_to_standings,
    _build_daily_snapshots,
    _build_snapshot,
    _group_games_by_date,
    _team_state_for,
    _weekly_win_pcts,
    calculate_games_behind,
    iso_week_number,
    main,
)


class TestCalculateGamesBehind:
    def test_equal(self):
        assert calculate_games_behind(10, 5, 10, 5) == 0.0

    def test_one_game_behind(self):
        assert calculate_games_behind(9, 5, 10, 5) == 0.5

    def test_two_games_behind(self):
        assert calculate_games_behind(8, 5, 10, 5) == 1.0

    def test_leader_has_more_losses(self):
        assert calculate_games_behind(10, 4, 10, 5) == -0.5

    def test_float_result(self):
        assert calculate_games_behind(7, 5, 10, 5) == 1.5


class TestIsoWeekNumber:
    def test_format(self):
        d = date(2026, 6, 24)
        result = iso_week_number(d)
        assert "-" in result
        assert "W" in result

    def test_year_boundary(self):
        d = date(2025, 1, 1)
        result = iso_week_number(d)
        assert "-" in result


class TestTeamState:
    def test_init(self):
        state = TeamState("LG")
        assert state.team_code == "LG"
        assert state.wins == 0
        assert state.losses == 0
        assert state.draws == 0

    def test_games_played(self):
        state = TeamState("LG")
        state.wins = 10
        state.losses = 5
        state.draws = 2
        assert state.games_played == 17

    def test_win_pct(self):
        state = TeamState("LG")
        state.wins = 10
        state.losses = 5
        assert abs(state.win_pct - 10 / 15) < 0.001

    def test_win_pct_zero(self):
        state = TeamState("LG")
        assert state.win_pct == 0.0

    def test_recent_10_all_wins(self):
        state = TeamState("LG")
        for _ in range(10):
            state.add_game(
                GameResultData(
                    is_win=True,
                    is_loss=False,
                    is_draw=False,
                    runs_for=1,
                    runs_against=0,
                    is_home=True,
                    game_date=date(2026, 6, 24),
                ),
            )
        assert state.recent_10_wins == 10
        assert state.recent_10_losses == 0
        assert state.recent_10_draws == 0

    def test_recent_10_mixed(self):
        state = TeamState("LG")
        for _ in range(5):
            state.add_game(
                GameResultData(
                    is_win=True,
                    is_loss=False,
                    is_draw=False,
                    runs_for=1,
                    runs_against=0,
                    is_home=True,
                    game_date=date(2026, 6, 24),
                ),
            )
        for _ in range(5):
            state.add_game(
                GameResultData(
                    is_win=False,
                    is_loss=True,
                    is_draw=False,
                    runs_for=0,
                    runs_against=1,
                    is_home=True,
                    game_date=date(2026, 6, 24),
                ),
            )
        assert state.recent_10_wins == 5
        assert state.recent_10_losses == 5

    def test_add_win_home(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=3,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.wins == 1
        assert state.home_wins == 1
        assert state.current_streak == 1

    def test_add_win_away(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=3,
                is_home=False,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.wins == 1
        assert state.away_wins == 1

    def test_add_loss(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=2,
                runs_against=5,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.losses == 1
        assert state.current_streak == -1

    def test_streak_accumulation(self):
        state = TeamState("LG")
        for _ in range(5):
            state.add_game(
                GameResultData(
                    is_win=True,
                    is_loss=False,
                    is_draw=False,
                    runs_for=1,
                    runs_against=0,
                    is_home=True,
                    game_date=date(2026, 6, 24),
                ),
            )
        assert state.current_streak == 5

    def test_draw(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=False,
                is_loss=False,
                is_draw=True,
                runs_for=3,
                runs_against=3,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.draws == 1
        assert state.recent_games[-1] == "D"

    def test_streak_recovery_from_negative(self):
        state = TeamState("LG")
        state.current_streak = -3
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=2,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.current_streak == 1

    def test_streak_from_positive_to_negative(self):
        state = TeamState("LG")
        state.current_streak = 2
        state.add_game(
            GameResultData(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=2,
                runs_against=5,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.current_streak == -1

    def test_runs_scored_and_allowed(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=3,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        assert state.runs_scored == 5
        assert state.runs_allowed == 3

    def test_recent_games_max_10(self):
        state = TeamState("LG")
        for i in range(15):
            state.add_game(
                GameResultData(
                    is_win=True,
                    is_loss=False,
                    is_draw=False,
                    runs_for=1,
                    runs_against=0,
                    is_home=True,
                    game_date=date(2026, 6, 24),
                ),
            )
        assert len(state.recent_games) == 10
        assert state.recent_10_wins == 10


class TestGroupGamesByDate:
    def test_groups_correctly(self):
        game1 = MagicMock()
        game1.game_date = date(2026, 6, 15)
        game2 = MagicMock()
        game2.game_date = date(2026, 6, 15)
        game3 = MagicMock()
        game3.game_date = date(2026, 6, 16)

        result = _group_games_by_date([game1, game2, game3])
        assert len(result[date(2026, 6, 15)]) == 2
        assert len(result[date(2026, 6, 16)]) == 1


class TestTeamStateFor:
    def test_creates_new_team(self):
        teams: dict[str, TeamState] = {}
        state = _team_state_for(teams, "LG")
        assert state.team_code == "LG"
        assert "LG" in teams

    def test_returns_existing_team(self):
        teams: dict[str, TeamState] = {}
        state1 = _team_state_for(teams, "LG")
        state2 = _team_state_for(teams, "LG")
        assert state1 is state2


class TestApplyGameToStandings:
    def test_home_win(self):
        game = MagicMock()
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = 5
        game.away_score = 3
        teams: dict[str, TeamState] = {}

        _apply_game_to_standings(game, teams, date(2026, 6, 15))

        assert teams["LG"].wins == 1
        assert teams["SSG"].losses == 1
        assert teams["LG"].runs_scored == 5
        assert teams["SSG"].runs_scored == 3

    def test_away_win(self):
        game = MagicMock()
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = 2
        game.away_score = 7
        teams: dict[str, TeamState] = {}

        _apply_game_to_standings(game, teams, date(2026, 6, 15))

        assert teams["SSG"].wins == 1
        assert teams["LG"].losses == 1

    def test_draw(self):
        game = MagicMock()
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = 4
        game.away_score = 4
        teams: dict[str, TeamState] = {}

        _apply_game_to_standings(game, teams, date(2026, 6, 15))

        assert teams["LG"].draws == 1
        assert teams["SSG"].draws == 1

    def test_none_scores_treated_as_zero(self):
        game = MagicMock()
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = None
        game.away_score = None
        teams: dict[str, TeamState] = {}

        _apply_game_to_standings(game, teams, date(2026, 6, 15))

        assert teams["LG"].draws == 1
        assert teams["SSG"].draws == 1


class TestWeeklyWinPcts:
    def test_returns_none_when_no_games(self):
        state = TeamState("LG")
        result = _weekly_win_pcts(state)
        assert result is None

    def test_returns_pct_per_week(self):
        state = TeamState("LG")
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=3,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=5,
                runs_against=3,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        state.add_game(
            GameResultData(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=3,
                runs_against=5,
                is_home=True,
                game_date=date(2026, 6, 24),
            ),
        )
        result = _weekly_win_pcts(state)
        assert result is not None
        week_key = list(result.keys())[0]
        assert result[week_key] == pytest.approx(0.667, abs=0.01)


class TestBuildSnapshot:
    def test_rank_1_gets_top_5(self):
        state = TeamState("LG")
        state.wins = 10
        state.losses = 5
        state.runs_scored = 50
        state.runs_allowed = 30

        with patch("src.cli.calculate_standings.TeamStandingsDaily") as mock_model:
            _build_snapshot(date(2026, 6, 15), state, 1, 10, 5)
            mock_model.assert_called_once()
            call_kwargs = mock_model.call_args[1]
            assert call_kwargs["rank"] == 1
            assert call_kwargs["top_5"] == 1
            assert call_kwargs["games_behind"] == 0.0

    def test_rank_6_not_top_5(self):
        state = TeamState("KT")
        state.wins = 8
        state.losses = 7
        state.runs_scored = 40
        state.runs_allowed = 35

        with patch("src.cli.calculate_standings.TeamStandingsDaily") as mock_model:
            _build_snapshot(date(2026, 6, 15), state, 6, 10, 5)
            call_kwargs = mock_model.call_args[1]
            assert call_kwargs["top_5"] == 0

    def test_negative_games_behind_clamped(self):
        state = TeamState("LG")
        state.wins = 12
        state.losses = 3
        state.runs_scored = 60
        state.runs_allowed = 20

        with patch("src.cli.calculate_standings.TeamStandingsDaily") as mock_model:
            _build_snapshot(date(2026, 6, 15), state, 1, 10, 5)
            call_kwargs = mock_model.call_args[1]
            assert call_kwargs["games_behind"] == 0.0


class TestBuildDailySnapshots:
    def test_single_game_produces_two_snapshots(self):
        game = MagicMock()
        game.game_date = date(2026, 6, 15)
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = 5
        game.away_score = 3

        with patch("src.cli.calculate_standings.TeamStandingsDaily") as mock_model:
            _build_daily_snapshots([game])
            assert mock_model.call_count == 2

    def test_multiple_dates_produce_ranked_snapshots(self):
        game1 = MagicMock()
        game1.game_date = date(2026, 6, 15)
        game1.home_team = "LG"
        game1.away_team = "SSG"
        game1.home_score = 5
        game1.away_score = 3

        game2 = MagicMock()
        game2.game_date = date(2026, 6, 16)
        game2.home_team = "LG"
        game2.away_team = "KT"
        game2.home_score = 2
        game2.away_score = 4

        with patch("src.cli.calculate_standings.TeamStandingsDaily") as mock_model:
            _build_daily_snapshots([game1, game2])
            assert mock_model.call_count == 5


class TestStandingsCalculator:
    def test_calculate_year_no_games(self):
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = []

        calc = StandingsCalculator(mock_session)
        calc.calculate_year(2026)

    def test_calculate_year_with_games(self):
        game = MagicMock()
        game.game_date = date(2026, 6, 15)
        game.home_team = "LG"
        game.away_team = "SSG"
        game.home_score = 5
        game.away_score = 3

        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [game]

        calc = StandingsCalculator(mock_session)
        calc.calculate_year(2026)
        mock_session.bulk_save_objects.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_save_snapshots_deletes_old(self):
        mock_session = MagicMock()
        calc = StandingsCalculator(mock_session)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query

        snapshots = [MagicMock()]
        calc._save_snapshots(2026, snapshots)

        mock_session.query.assert_called_with(TeamStandingsDaily)
        mock_session.bulk_save_objects.assert_called_once_with(snapshots)
        mock_session.commit.assert_called_once()


class TestMain:
    def test_default_year(self):
        with patch("sys.argv", ["calculate_standings"]), patch("src.cli.calculate_standings.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.join.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.all.return_value = []
            result = main()
            assert result is None

    def test_specific_year(self):
        with (
            patch("sys.argv", ["calculate_standings", "--year", "2025"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.join.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.all.return_value = []
            result = main()
            assert result is None

    def test_report_mode(self):
        with (
            patch("sys.argv", ["calculate_standings", "--report"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value.return_value.first.return_value = None
            result = main()
            assert result is None

    def test_report_mode_with_date(self):
        with (
            patch("sys.argv", ["calculate_standings", "--report", "--date", "2025-06-15"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value.return_value.first.return_value = None
            result = main()
            assert result is None

    def test_trend_mode_all(self):
        with (
            patch("sys.argv", ["calculate_standings", "--trend"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value.return_value.all.return_value = []
            result = main()
            assert result is None

    def test_trend_mode_specific_team(self):
        with (
            patch("sys.argv", ["calculate_standings", "--trend", "LG"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value.return_value.all.return_value = []
            result = main()
            assert result is None

    def test_all_years(self):
        with (
            patch("sys.argv", ["calculate_standings", "--all"]),
            patch("src.cli.calculate_standings.SessionLocal") as mock_sf,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.distinct.return_value.all.return_value = [(2025,), (2026,)]

            mock_query = MagicMock()
            mock_session.query.return_value = mock_query
            mock_query.join.return_value = mock_query
            mock_query.filter.return_value = mock_query
            mock_query.order_by.return_value = mock_query
            mock_query.all.return_value = []
            result = main()
            assert result is None

    def test_exception_handling(self):
        with patch("sys.argv", ["calculate_standings"]), patch("src.cli.calculate_standings.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value = mock_session
            mock_session.query.side_effect = RuntimeError("DB error")
            result = main()
            assert result is None
            mock_session.rollback.assert_called_once()
            mock_session.close.assert_called_once()
