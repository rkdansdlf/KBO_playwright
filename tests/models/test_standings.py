from __future__ import annotations

from src.models.standings import TeamStandingsDaily


class TestTeamStandingsDaily:
    def test_tablename(self):
        assert TeamStandingsDaily.__tablename__ == "team_standings_daily"

    def test_has_standings_date(self):
        assert hasattr(TeamStandingsDaily, "standings_date")

    def test_has_team_code(self):
        assert hasattr(TeamStandingsDaily, "team_code")

    def test_has_games_played(self):
        assert hasattr(TeamStandingsDaily, "games_played")

    def test_has_wins(self):
        assert hasattr(TeamStandingsDaily, "wins")

    def test_has_losses(self):
        assert hasattr(TeamStandingsDaily, "losses")

    def test_has_draws(self):
        assert hasattr(TeamStandingsDaily, "draws")

    def test_has_rank(self):
        assert hasattr(TeamStandingsDaily, "rank")

    def test_has_timestamp_mixin(self):
        assert hasattr(TeamStandingsDaily, "created_at")
        assert hasattr(TeamStandingsDaily, "updated_at")

    def test_default_values_in_table(self):
        col = TeamStandingsDaily.__table__.c
        assert col.games_played.default.arg == 0
        assert col.wins.default.arg == 0
        assert col.losses.default.arg == 0
        assert col.draws.default.arg == 0
        assert col.win_pct.default.arg == 0.0
        assert col.games_behind.default.arg == 0.0
        assert col.rank.default.arg == 0

    def test_unique_constraint(self):
        constraints = TeamStandingsDaily.__table_args__
        assert len(constraints) == 1
        constraint = constraints[0]
        assert "standings_date" in str(constraint.columns)
        assert "team_code" in str(constraint.columns)
