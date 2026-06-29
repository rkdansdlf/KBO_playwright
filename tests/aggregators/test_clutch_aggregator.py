"""Tests for ClutchAggregator — WPA-based clutch metric aggregation."""

from datetime import date

import pytest
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker

from src.aggregators.clutch_aggregator import ClutchAggregator
from src.models.game import Game, GameEvent
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.team import Team


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Game.__table__.create(bind=engine)
    GameEvent.__table__.create(bind=engine)
    KboSeason.__table__.create(bind=engine)
    PlayerBasic.__table__.create(bind=engine)
    Team.__table__.create(bind=engine)
    PlayerSeasonBatting.__table__.create(bind=engine)
    sess = sessionmaker(bind=engine)()
    yield sess
    sess.close()


def _add_season(session, season_id=1, year=2025, league_type_code=1, league_type_name="정규시즌"):
    session.add(
        KboSeason(
            season_id=season_id,
            season_year=year,
            league_type_code=league_type_code,
            league_type_name=league_type_name,
        ),
    )
    session.commit()


def _add_game(session, game_id="20250101", status="COMPLETED", season_id=1):
    session.add(
        Game(
            game_id=game_id,
            stadium="잠실",
            game_status=status,
            season_id=season_id,
            game_date=date(2025, 1, 1),
            home_team="LG",
            away_team="SS",
        ),
    )
    session.commit()


def _add_event(session, game_id="20250101", batter_id=10001, wpa=0.05, win_expectancy_before=0.5, event_seq=1):
    session.add(
        GameEvent(
            game_id=game_id,
            batter_id=batter_id,
            event_seq=event_seq,
            wpa=wpa,
            win_expectancy_before=win_expectancy_before,
        ),
    )
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


def _add_player(session, player_id, name="테스트"):
    session.add(PlayerBasic(player_id=player_id, name=name))
    session.commit()


def _add_team(session, team_id="LG"):
    session.add(Team(team_id=team_id, team_name="LG", team_short_name="LG", city="서울", is_active=True))
    session.commit()


def _add_player_season_batting(session, player_id, season=2025, league="REGULAR"):
    session.add(
        PlayerSeasonBatting(
            player_id=player_id,
            season=season,
            league=league,
            level="KBO1",
            source="ROLLUP",
            team_code="LG",
        ),
    )
    session.commit()


class TestPersistToExtraStats:
    def test_nothing_to_persist_when_empty(self, session, caplog):
        _add_season(session)
        agg = ClutchAggregator(session)
        agg.persist_to_extra_stats(2025)

    def test_persists_wpa_to_extra_stats(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.15)
        _add_player(session, player_id=10001)
        _add_team(session)
        _add_player_season_batting(session, player_id=10001)
        agg = ClutchAggregator(session)
        agg.persist_to_extra_stats(2025)
        psb = session.query(PlayerSeasonBatting).filter_by(player_id=10001, season=2025).first()
        assert psb is not None
        assert psb.extra_stats is not None
        assert psb.extra_stats["wpa_sum"] == 0.15
        assert psb.extra_stats["clutch"] is not None

    def test_skips_null_batter_id(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=None, wpa=0.10)
        _add_player(session, player_id=10001)
        _add_team(session)
        _add_player_season_batting(session, player_id=10001)
        agg = ClutchAggregator(session)
        agg.persist_to_extra_stats(2025)
        psb = session.query(PlayerSeasonBatting).filter_by(player_id=10001, season=2025).first()
        assert psb.extra_stats is None

    def test_skips_missing_player_season(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.10)
        _add_player(session, player_id=10001)
        _add_team(session)
        agg = ClutchAggregator(session)
        agg.persist_to_extra_stats(2025)
        session.commit()

    def test_fk_error_fallback_to_raw_sql(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.20)
        _add_player(session, player_id=10001)
        _add_team(session)
        _add_player_season_batting(session, player_id=10001)
        agg = ClutchAggregator(session)
        original_commit = session.commit
        call_count = 0

        def raise_fk_commit():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise exc.IntegrityError("INSERT ...", {}, Exception("foreign key constraint failed"))
            return original_commit()

        session.commit = raise_fk_commit
        agg.persist_to_extra_stats(2025)
        session.commit = original_commit
        psb = session.query(PlayerSeasonBatting).filter_by(player_id=10001, season=2025).first()
        assert psb is not None
        assert psb.extra_stats is not None
        assert psb.extra_stats["wpa_sum"] == 0.20

    def test_non_fk_error_reraises(self, session):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.10)
        _add_player(session, player_id=10001)
        _add_team(session)
        _add_player_season_batting(session, player_id=10001)
        agg = ClutchAggregator(session)

        def raise_other_commit():
            raise exc.IntegrityError("INSERT ...", {}, Exception("some other error"))

        session.commit = raise_other_commit
        with pytest.raises(exc.IntegrityError):
            agg.persist_to_extra_stats(2025)
        session.rollback()


class TestPrintReport:
    def test_returns_early_when_empty(self, session, caplog):
        _add_season(session)
        agg = ClutchAggregator(session)
        with caplog.at_level("INFO"):
            agg.print_report(2025)
        assert "WPA합계" not in caplog.text

    def test_outputs_report_when_results(self, session, caplog):
        _add_season(session)
        _add_game(session)
        _add_event(session, batter_id=10001, wpa=0.15)
        agg = ClutchAggregator(session)
        with caplog.at_level("INFO"):
            agg.print_report(2025)
        assert "WPA합계" in caplog.text or "순위" in caplog.text
