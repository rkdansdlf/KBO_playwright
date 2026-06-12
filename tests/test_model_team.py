"""Tests for src/models/team.py, team_stats.py, and standings.py."""

from datetime import date

from src.models.standings import TeamStandingsDaily
from src.models.team import Team, TeamDailyRoster, TeamSeasonBaserunning, TeamSeasonFielding
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from tests.factories import (
    build_session,
    make_standings_daily,
    make_team,
    make_team_daily_roster,
    make_team_season_baserunning,
    make_team_season_batting,
    make_team_season_fielding,
    make_team_season_pitching,
)


def _create_tables(session):
    tables = [
        Team.__table__,
        TeamDailyRoster.__table__,
        TeamSeasonBatting.__table__,
        TeamSeasonPitching.__table__,
        TeamSeasonFielding.__table__,
        TeamSeasonBaserunning.__table__,
        TeamStandingsDaily.__table__,
    ]
    for table in tables:
        table.create(bind=session.bind, checkfirst=True)


class TestTeam:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        t = make_team()
        session.add(t)
        session.commit()

        saved = session.query(Team).filter_by(team_id="LG").one()
        assert saved.team_name == "LG Twins"
        assert saved.city == "서울"
        assert saved.is_active is True

    def test_optional_fields(self):
        t = make_team(founded_year=1990, franchise_id=1)
        assert t.founded_year == 1990
        assert t.franchise_id == 1

    def test_repr(self):
        t = make_team()
        assert "Team" in repr(t)
        assert "LG" in repr(t)


class TestTeamDailyRoster:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()

        roster = make_team_daily_roster()
        session.add(roster)
        session.commit()

        saved = session.query(TeamDailyRoster).filter_by(team_code="LG", player_id=12345).one()
        assert saved.roster_date == date(2025, 4, 1)
        assert saved.position == "투수"
        assert saved.person_type == "player"

    def test_default_person_type(self):
        roster = make_team_daily_roster()
        assert roster.person_type == "player"


class TestTeamSeasonBatting:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()

        tsb = make_team_season_batting()
        session.add(tsb)
        session.commit()

        saved = session.query(TeamSeasonBatting).filter_by(team_id="LG", season=2025).one()
        assert saved.avg == 0.280
        assert saved.games == 144

    def test_optional_stats(self):
        tsb = make_team_season_batting(home_runs=200, ops=0.800)
        assert tsb.home_runs == 200

    def test_unique_constraint(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()
        t1 = make_team_season_batting()
        t2 = make_team_season_batting()
        session.add(t1)
        session.commit()
        session.add(t2)
        import pytest
        from sqlalchemy.exc import IntegrityError

        with pytest.raises(IntegrityError):
            session.commit()


class TestTeamSeasonPitching:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()

        tsp = make_team_season_pitching()
        session.add(tsp)
        session.commit()

        saved = session.query(TeamSeasonPitching).filter_by(team_id="LG", season=2025).one()
        assert saved.era == 3.75
        assert saved.wins == 85

    def test_optional_stats(self):
        tsp = make_team_season_pitching(strikeouts=1200, saves=40)
        assert tsp.strikeouts == 1200


class TestTeamSeasonFielding:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()

        tsf = make_team_season_fielding()
        session.add(tsf)
        session.commit()

        saved = session.query(TeamSeasonFielding).filter_by(team_code="LG", season=2025).one()
        assert saved.errors == 0

    def test_optional_metrics(self):
        tsf = make_team_season_fielding(errors=50, fielding_pct=0.985)
        assert tsf.errors == 50
        assert tsf.fielding_pct == 0.985


class TestTeamSeasonBaserunning:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        session.add(make_team())
        session.commit()

        tsbr = make_team_season_baserunning()
        session.add(tsbr)
        session.commit()

        saved = session.query(TeamSeasonBaserunning).filter_by(team_code="LG", season=2025).one()
        assert saved.stolen_bases == 0

    def test_optional_metrics(self):
        tsbr = make_team_season_baserunning(stolen_bases=120, caught_stealing=30)
        assert tsbr.stolen_bases == 120


class TestTeamStandingsDaily:
    def test_create(self):
        _, session = build_session()
        _create_tables(session)
        sd = make_standings_daily()
        session.add(sd)
        session.commit()

        saved = session.query(TeamStandingsDaily).filter_by(team_code="LG").one()
        assert saved.standings_date == date(2025, 4, 1)
        assert saved.wins == 1
        assert saved.rank == 1
        assert saved.top_5 is True

    def test_games_behind_default(self):
        sd = make_standings_daily(games_behind=0.5)
        assert sd.games_behind == 0.5

    def test_repr(self):
        sd = make_standings_daily()
        assert "TeamStandingsDaily" in repr(sd)


class TestTeamAdvanced:
    def test_fk_violation_invalid_team_season_pitching(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        tsp = make_team_season_pitching(team_id="ZZ")
        session.add(tsp)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_team_season_batting_composite_pk(self):
        import pytest
        from sqlalchemy.exc import IntegrityError

        _, session = build_session()
        _create_tables(session)
        t = make_team()
        session.add(t)
        session.commit()
        t1 = make_team_season_batting()
        t2 = make_team_season_batting()
        session.add(t1)
        session.commit()
        session.add(t2)
        with pytest.raises(IntegrityError):
            session.commit()
