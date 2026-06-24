from __future__ import annotations

from src.models.matchup import (
    BatterHomeAwaySplit,
    BatterTeamSplit,
    PitcherHomeAwaySplit,
    PitcherTeamSplit,
)


class TestBatterTeamSplit:
    def test_tablename(self):
        assert BatterTeamSplit.__tablename__ == "matchup_batter_team_split"

    def test_has_season_year(self):
        assert hasattr(BatterTeamSplit, "season_year")

    def test_has_player_id(self):
        assert hasattr(BatterTeamSplit, "player_id")

    def test_has_team_code(self):
        assert hasattr(BatterTeamSplit, "team_code")

    def test_has_opponent_team_code(self):
        assert hasattr(BatterTeamSplit, "opponent_team_code")

    def test_default_games(self):
        col = BatterTeamSplit.__table__.c
        assert col.games.default.arg == 0

    def test_default_hits(self):
        col = BatterTeamSplit.__table__.c
        assert col.hits.default.arg == 0

    def test_unique_constraint(self):
        constraints = BatterTeamSplit.__table_args__
        assert len(constraints) == 1
        constraint = constraints[0]
        cols = str(constraint.columns)
        assert "season_year" in cols
        assert "player_id" in cols
        assert "team_code" in cols
        assert "opponent_team_code" in cols


class TestBatterHomeAwaySplit:
    def test_tablename(self):
        assert BatterHomeAwaySplit.__tablename__ == "matchup_batter_home_away"

    def test_has_location(self):
        assert hasattr(BatterHomeAwaySplit, "location")

    def test_has_ops(self):
        assert hasattr(BatterHomeAwaySplit, "ops")

    def test_unique_constraint(self):
        constraints = BatterHomeAwaySplit.__table_args__
        assert len(constraints) == 1
        constraint = constraints[0]
        cols = str(constraint.columns)
        assert "player_id" in cols
        assert "season_year" in cols
        assert "location" in cols


class TestPitcherTeamSplit:
    def test_tablename(self):
        assert PitcherTeamSplit.__tablename__ == "matchup_pitcher_team_split"

    def test_has_era(self):
        assert hasattr(PitcherTeamSplit, "era")

    def test_has_whip(self):
        assert hasattr(PitcherTeamSplit, "whip")

    def test_default_games(self):
        col = PitcherTeamSplit.__table__.c
        assert col.games.default.arg == 0


class TestPitcherHomeAwaySplit:
    def test_tablename(self):
        assert PitcherHomeAwaySplit.__tablename__ == "matchup_pitcher_home_away"

    def test_has_location(self):
        assert hasattr(PitcherHomeAwaySplit, "location")

    def test_has_era(self):
        assert hasattr(PitcherHomeAwaySplit, "era")

    def test_unique_constraint(self):
        constraints = PitcherHomeAwaySplit.__table_args__
        assert len(constraints) == 1
        constraint = constraints[0]
        cols = str(constraint.columns)
        assert "player_id" in cols
        assert "season_year" in cols
        assert "location" in cols
