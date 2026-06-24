from __future__ import annotations

from datetime import date
from src.cli.calculate_standings import (
    calculate_games_behind,
    iso_week_number,
    TeamState,
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
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=1,
                runs_against=0,
                is_home=True,
                game_date=date(2026, 6, 24),
            )
        assert state.recent_10_wins == 10
        assert state.recent_10_losses == 0
        assert state.recent_10_draws == 0

    def test_recent_10_mixed(self):
        state = TeamState("LG")
        for _ in range(5):
            state.add_game(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=1,
                runs_against=0,
                is_home=True,
                game_date=date(2026, 6, 24),
            )
        for _ in range(5):
            state.add_game(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=0,
                runs_against=1,
                is_home=True,
                game_date=date(2026, 6, 24),
            )
        assert state.recent_10_wins == 5
        assert state.recent_10_losses == 5

    def test_add_win_home(self):
        state = TeamState("LG")
        state.add_game(
            is_win=True,
            is_loss=False,
            is_draw=False,
            runs_for=5,
            runs_against=3,
            is_home=True,
            game_date=date(2026, 6, 24),
        )
        assert state.wins == 1
        assert state.home_wins == 1
        assert state.current_streak == 1

    def test_add_win_away(self):
        state = TeamState("LG")
        state.add_game(
            is_win=True,
            is_loss=False,
            is_draw=False,
            runs_for=5,
            runs_against=3,
            is_home=False,
            game_date=date(2026, 6, 24),
        )
        assert state.wins == 1
        assert state.away_wins == 1

    def test_add_loss(self):
        state = TeamState("LG")
        state.add_game(
            is_win=False,
            is_loss=True,
            is_draw=False,
            runs_for=2,
            runs_against=5,
            is_home=True,
            game_date=date(2026, 6, 24),
        )
        assert state.losses == 1
        assert state.current_streak == -1

    def test_streak_accumulation(self):
        state = TeamState("LG")
        for _ in range(5):
            state.add_game(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=1,
                runs_against=0,
                is_home=True,
                game_date=date(2026, 6, 24),
            )
        assert state.current_streak == 5

    def test_draw(self):
        state = TeamState("LG")
        state.add_game(
            is_win=False,
            is_loss=False,
            is_draw=True,
            runs_for=3,
            runs_against=3,
            is_home=True,
            game_date=date(2026, 6, 24),
        )
        assert state.draws == 1
        assert state.recent_games[-1] == "D"
