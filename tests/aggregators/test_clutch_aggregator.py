"""Tests for ClutchAggregator — WPA-based clutch metric aggregation."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.aggregators.clutch_aggregator import ClutchAggregator
from src.models.game import Game, GameEvent
from src.models.season import KboSeason


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    KboSeason.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_season(session, season_id=1, year=2025, league_type_code=1, league_type_name="정규시즌"):
    session.add(KboSeason(season_id=season_id, season_year=year, league_type_code=league_type_code, league_type_name=league_type_name))
    session.commit()


def _add_game(session, game_id="20250101", status="COMPLETED", season_id=1):
    session.add(Game(
        game_id=game_id,
        stadium="잠실",
        game_status=status,
        season_id=season_id,
        game_date=date(2025, 1, 1),
        home_team="LG",
        away_team="SS",
    ))
    session.commit()


def _add_event(session, game_id="20250101", batter_id=10001,
               wpa=0.05, win_expectancy_before=0.5, event_seq=1):
    session.add(GameEvent(
        game_id=game_id,
        batter_id=batter_id,
        event_seq=event_seq,
        wpa=wpa,
        win_expectancy_before=win_expectancy_before,
    ))
    session.commit()


class TestClutchAggregator:
    def test_returns_empty_when_no_events(self, session):
        _add_season(session)
        agg = ClutchAggregator(session)
        assert agg.aggregate(2025) == []

    def test_aggregates_single_event(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session)
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        assert len(results) == 1
        r = results[0]
        assert r["batter_id"] == 10001
        assert r["wpa_sum"] == 0.05
        assert r["event_count"] == 1
        assert r["avg_wpa"] == 0.05

    def test_aggregates_multiple_events_same_batter(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.05)
        _add_event(session, batter_id=10001, wpa=-0.03, game_id="20250102")
        _add_game(session, game_id="20250102")
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        assert len(results) == 1
        r = results[0]
        assert r["wpa_sum"] == 0.02
        assert r["wpa_abs_sum"] == 0.08
        assert r["avg_wpa"] == pytest.approx(0.01, abs=0.001)

    def test_high_leverage_classification(self, session):
        _add_season(session)
        _add_game(session)
        # High leverage: we_before close to 0.5 (±0.15)
        _add_event(session, batter_id=10001, wpa=0.10, win_expectancy_before=0.55)
        # Low leverage: we_before far from 0.5
        _add_event(session, batter_id=10001, wpa=0.05, win_expectancy_before=0.80, game_id="20250102")
        _add_game(session, game_id="20250102")
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        r = results[0]
        # leverage = |0.55 - 0.5| = 0.05 (<= 0.15) -> high leverage
        # leverage = |0.80 - 0.5| = 0.30 (> 0.15) -> NOT high leverage
        assert r["high_leverage_wpa"] == 0.10
        assert r["high_leverage_count"] == 1

    def test_skips_missing_batter_id(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=None)
        agg = ClutchAggregator(session)
        assert agg.aggregate(2025) == []

    def test_handles_null_wpa(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, wpa=None)
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        assert results == []  # null wpa events filtered out

    def test_handles_null_win_expectancy(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, win_expectancy_before=None)
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        # Default we_before = 0.5, leverage = 0.0 <= 0.15 -> high leverage
        assert results[0]["high_leverage_wpa"] == 0.05
        assert results[0]["high_leverage_count"] == 1

    def test_sorts_by_wpa_sum_descending(self, session):
        _add_season(session)
        _add_game(session)
        _add_game(session, game_id="20250102")
        _add_event(session, batter_id=10001, wpa=0.02)
        _add_event(session, batter_id=10002, wpa=0.10, game_id="20250102")
        agg = ClutchAggregator(session)
        results = agg.aggregate(2025)
        assert results[0]["batter_id"] == 10002
        assert results[1]["batter_id"] == 10001

    def test_filters_non_regular_season(self, session):
        _add_season(session, league_type_name="POSTSEASON")
        _add_game(session)
        _add_event(session)
        agg = ClutchAggregator(session)
        assert agg.aggregate(2025) == []

    def test_filters_non_completed_games(self, session):
        _add_season(session)
        _add_game(session, status="INPROG")
        _add_event(session)
        agg = ClutchAggregator(session)
        assert agg.aggregate(2025) == []
