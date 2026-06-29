"""Tests for HomeAwaySplitAggregator — home/away batting splits."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.home_away_split_aggregator import HomeAwaySplitAggregator
from src.models.game import Game, GameBattingStat
from src.models.matchup import BatterHomeAwaySplit
from src.models.season import KboSeason


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameBattingStat.__table__.create(bind=engine)
    BatterHomeAwaySplit.__table__.create(bind=engine)
    KboSeason.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_season(session, season_id=1, year=2025):
    session.add(KboSeason(season_id=season_id, season_year=year, league_type_code=1, league_type_name="정규시즌"))
    session.commit()


def _add_game(session, game_id="20250101", home="LG", away="SS", status="COMPLETED", season_id=1):
    session.add(
        Game(
            game_id=game_id,
            stadium="잠실",
            home_team=home,
            away_team=away,
            game_status=status,
            season_id=season_id,
            game_date=date(2025, 1, 1),
        ),
    )
    session.commit()


def _add_batting_stat(
    session,
    game_id="20250101",
    player_id=10001,
    team_code="LG",
    pa=5,
    ab=4,
    hits=2,
    doubles=1,
    triples=0,
    home_runs=1,
    rbi=3,
    walks=1,
    strikeouts=1,
    stolen_bases=0,
    caught_stealing=0,
    hbp=0,
    sacrifice_flies=0,
    team_side="home",
    appearance_seq=1,
    player_name="홍길동",
):
    session.add(
        GameBattingStat(
            game_id=game_id,
            player_id=player_id,
            team_code=team_code,
            team_side=team_side,
            appearance_seq=appearance_seq,
            player_name=player_name,
            plate_appearances=pa,
            at_bats=ab,
            hits=hits,
            doubles=doubles,
            triples=triples,
            home_runs=home_runs,
            rbi=rbi,
            walks=walks,
            strikeouts=strikeouts,
            stolen_bases=stolen_bases,
            caught_stealing=caught_stealing,
            hbp=hbp,
            sacrifice_flies=sacrifice_flies,
        ),
    )
    session.commit()


class TestHomeAwaySplitAggregator:
    def test_returns_empty_when_no_games(self, session):
        _add_season(session)
        agg = HomeAwaySplitAggregator(session)
        assert agg.aggregate_batting(2025) == []

    def test_home_game_classified_correctly(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(session, team_code="LG")  # same as home -> HOME
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        assert len(results) == 2  # HOME + AWAY entries per player
        home = next(r for r in results if r["location"] == "HOME")
        assert home["at_bats"] == 4

    def test_away_game_classified_correctly(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(session, team_code="SS", player_id=10002)  # same as away -> AWAY
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        assert len(results) == 2  # HOME + AWAY entries per player
        away = next(r for r in results if r["location"] == "AWAY")
        assert away["at_bats"] == 4

    def test_both_home_and_away_splits(self, session):
        _add_season(session)
        _add_game(session, game_id="20250101", home="LG", away="SS")
        _add_game(session, game_id="20250102", home="SS", away="LG")
        _add_batting_stat(session, game_id="20250101", team_code="LG")  # HOME
        _add_batting_stat(session, game_id="20250102", team_code="LG", player_id=10001)  # AWAY
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        assert len(results) == 2
        locations = {r["location"] for r in results}
        assert locations == {"HOME", "AWAY"}

    def test_derived_stats_calculated(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(
            session,
            team_code="LG",
            ab=4,
            hits=2,
            doubles=1,
            home_runs=1,
            walks=1,
            hbp=0,
            sacrifice_flies=0,
        )
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        r = results[0]
        # TB = 2 + 1 + 2*0 + 3*1 = 6
        # AVG = 2/4 = 0.5
        # OBP = (2+1+0) / (4+1+0+0) = 3/5 = 0.6
        # SLG = 6/4 = 1.5
        # OPS = 0.6 + 1.5 = 2.1
        assert r["avg"] == 0.5
        assert r["obp"] == 0.6
        assert r["slg"] == 1.5
        assert r["ops"] == 2.1

    def test_skips_missing_player_id(self, session):
        _add_season(session)
        _add_game(session)
        _add_batting_stat(session, player_id=None, team_code="LG")
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        assert results == []

    def test_zero_at_bats(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(session, team_code="LG", ab=0, hits=0, pa=1, walks=1)
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        r = results[0]
        assert r["avg"] == 0.0
        assert r["slg"] == 0.0
        # OBP = (0+1+0) / (0+1+0+0) = 1.0
        assert r["obp"] == 1.0

    def test_filters_non_regular_season(self, session):
        session.add(KboSeason(season_id=1, season_year=2025, league_type_code=2, league_type_name="POSTSEASON"))
        session.commit()
        _add_game(session)
        _add_batting_stat(session)
        agg = HomeAwaySplitAggregator(session)
        assert agg.aggregate_batting(2025) == []

    def test_persist_batting_saves_to_db(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(session, team_code="LG")
        agg = HomeAwaySplitAggregator(session)
        agg.persist_batting(2025)
        saved = session.query(BatterHomeAwaySplit).all()
        assert len(saved) == 2  # HOME + AWAY
        home = next(s for s in saved if s.location == "HOME")
        assert home is not None

    def test_persist_batting_replaces_old_data(self, session):
        _add_season(session)
        _add_game(session, home="LG", away="SS")
        _add_batting_stat(session, team_code="LG")
        agg = HomeAwaySplitAggregator(session)
        agg.persist_batting(2025)  # first time
        agg.persist_batting(2025)  # second time (delete + insert)
        saved = session.query(BatterHomeAwaySplit).all()
        assert len(saved) == 2  # replaced, not duplicated (HOME + AWAY)

    def test_skips_missing_team_code(self, session):
        _add_season(session)
        _add_game(session)
        _add_batting_stat(session, player_id=10001, team_code=None)
        agg = HomeAwaySplitAggregator(session)
        results = agg.aggregate_batting(2025)
        assert results == []


class TestPrintReport:
    def test_returns_early_when_empty(self, session, caplog):
        _add_season(session)
        agg = HomeAwaySplitAggregator(session)
        with caplog.at_level("INFO"):
            agg.print_report(2025)
        assert "홈/원정" not in caplog.text

    def test_outputs_report_when_results(self, session, caplog):
        _add_season(session)
        _add_game(session, game_id="20250101", home="LG", away="SS")
        _add_game(session, game_id="20250102", home="SS", away="LG")
        _add_batting_stat(session, game_id="20250101", team_code="LG", player_id=10001, pa=60, ab=50)
        _add_batting_stat(session, game_id="20250102", team_code="LG", player_id=10001, pa=55, ab=50)
        agg = HomeAwaySplitAggregator(session)
        with caplog.at_level("INFO"):
            agg.print_report(2025, top_n=3)
        assert "홈/원정" in caplog.text
        assert "PlayerID" in caplog.text
