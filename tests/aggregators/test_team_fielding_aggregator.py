"""Tests for TeamFieldingAggregator — fielding and baserunning team rollup."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
from src.models.player import PlayerSeasonBaserunning, PlayerSeasonFielding
from src.models.team import Team, TeamSeasonBaserunning, TeamSeasonFielding


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    PlayerSeasonFielding.__table__.create(bind=engine)
    PlayerSeasonBaserunning.__table__.create(bind=engine)
    Team.__table__.create(bind=engine)
    TeamSeasonFielding.__table__.create(bind=engine)
    TeamSeasonBaserunning.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_fielding(session, year=2025, team_id="LG", errors=5, double_plays=50,
                  putouts=300, assists=200, innings=1000, position_id="PO",
                  player_id=10001):
    session.add(PlayerSeasonFielding(
        player_id=player_id, year=year, team_id=team_id, position_id=position_id,
        errors=errors, double_plays=double_plays,
        putouts=putouts, assists=assists, innings=innings,
    ))
    session.commit()


def _add_baserunning(session, year=2025, team_id="LG",
                     stolen_bases=50, caught_stealing=10, out_on_base=5):
    session.add(PlayerSeasonBaserunning(
        player_id=10001, year=year, team_id=team_id,
        stolen_bases=stolen_bases, caught_stealing=caught_stealing,
        out_on_base=out_on_base,
    ))
    session.commit()


class TestAggregateFielding:
    def test_single_player(self, session):
        _add_fielding(session)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        assert result["putouts"] == 300
        assert result["assists"] == 200
        assert result["errors"] == 5
        assert result["total_chances"] == 505
        # fielding_pct = (300 + 200) / 505 = 500/505 = 0.9901
        assert result["fielding_pct"] == pytest.approx(0.9901, abs=0.001)

    def test_zero_errors(self, session):
        _add_fielding(session, errors=0)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        assert result["fielding_pct"] == 1.0

    def test_fielding_pct_none_when_zero_chances(self, session):
        session.add(PlayerSeasonFielding(
            player_id=10001, year=2025, team_id="LG", position_id="PO",
            errors=0, double_plays=0, putouts=0, assists=0, innings=0,
        ))
        session.commit()
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        assert result["fielding_pct"] is None
        assert result["range_factor_per_game"] is None

    def test_range_factor(self, session):
        _add_fielding(session)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        # RF = (300 + 200) / (1000 / 9) = 500 / 111.111 = 4.5
        assert result["range_factor_per_game"] == pytest.approx(4.5, abs=0.001)

    def test_multiple_players_summed(self, session):
        _add_fielding(session, putouts=300, assists=200)
        _add_fielding(session, putouts=100, assists=50, player_id=10002)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        assert result["putouts"] == 400
        assert result["assists"] == 250

    def test_no_rows_returns_zeros(self, session):
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_fielding(2025, "LG")
        assert result["putouts"] == 0
        assert result["fielding_pct"] is None


class TestAggregateBaserunning:
    def test_single_player(self, session):
        _add_baserunning(session)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_baserunning(2025, "LG")
        assert result["stolen_bases"] == 50
        assert result["caught_stealing"] == 10
        assert result["sb_success_rate"] == pytest.approx(0.833, abs=0.001)

    def test_sb_rate_none_when_no_attempts(self, session):
        _add_baserunning(session, stolen_bases=0, caught_stealing=0)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_baserunning(2025, "LG")
        assert result["sb_success_rate"] is None

    def test_perfect_sb_rate(self, session):
        _add_baserunning(session, stolen_bases=30, caught_stealing=0)
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_baserunning(2025, "LG")
        assert result["sb_success_rate"] == 1.0

    def test_no_rows_returns_zeros(self, session):
        agg = TeamFieldingAggregator(session)
        result = agg.aggregate_baserunning(2025, "LG")
        assert result["stolen_bases"] == 0
        assert result["sb_success_rate"] is None


class TestRunAll:
    def test_filters_to_kbo_teams_with_data(self, session):
        session.add(Team(team_id="LG", franchise_id=1, team_name="LG", team_short_name="LG", city="서울"))
        session.add(Team(team_id="ALL", franchise_id=None, team_name="올스타", team_short_name="ALL", city="서울"))
        session.commit()
        _add_fielding(session, team_id="LG")
        _add_baserunning(session, team_id="LG")

        agg = TeamFieldingAggregator(session)
        agg.run_all(2025, ["LG", "ALL"])

        # LG should have records saved
        saved_f = session.query(TeamSeasonFielding).filter_by(team_code="LG").first()
        assert saved_f is not None
        assert saved_f.putouts == 300

        # ALL (no franchise_id) should be skipped
        saved_all = session.query(TeamSeasonFielding).filter_by(team_code="ALL").first()
        assert saved_all is None

    def test_upserts_existing_record(self, session):
        session.add(Team(team_id="LG", franchise_id=1, team_name="LG", team_short_name="LG", city="서울"))
        session.commit()
        session.add(TeamSeasonFielding(season=2025, team_code="LG", putouts=0, assists=0, errors=0,
                                       total_chances=0, double_plays=0, triple_plays=0, def_innings=0))
        session.commit()
        _add_fielding(session, team_id="LG")

        agg = TeamFieldingAggregator(session)
        agg.run_all(2025, ["LG"])

        saved = session.query(TeamSeasonFielding).filter_by(team_code="LG").first()
        assert saved.putouts == 300  # updated, not duplicated
