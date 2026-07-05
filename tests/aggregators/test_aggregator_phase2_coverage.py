from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker

from src.aggregators.clutch_aggregator import ClutchAggregator
from src.aggregators.ranking_aggregator import MetricConfig, RankingAggregator
from src.aggregators.team_fielding_aggregator import TeamFieldingAggregator
from src.models.game import Game, GameEvent
from src.models.player import PlayerBasic, PlayerSeasonBaserunning, PlayerSeasonBatting, PlayerSeasonFielding
from src.models.season import KboSeason
from src.models.team import Team, TeamSeasonBaserunning, TeamSeasonFielding


class TestRankingAggregatorEdgeBranches:
    def test_build_batting_configs_none_stats(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        assert agg._build_batting_configs(None, None) == []

    def test_build_batting_configs_empty_list(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        assert agg._build_batting_configs([], 100) == []

    def test_build_pitching_configs_none_stats(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        assert agg._build_pitching_configs(None, None) == []

    def test_build_pitching_configs_empty_list(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        assert agg._build_pitching_configs([], 100) == []

    def test_build_rankings_empty_rows(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        assert agg._build_rankings(2025, [], []) == []

    def test_passes_ranking_filters_min_games_field_missing(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(
            name="test",
            source="FIELDING",
            value_key="fielding_pct",
            min_games_field="games",
            min_games=10,
        )
        row = {"games": 5, "fielding_pct": 0.990}
        assert agg._passes_ranking_filters(row, config) is False

    def test_passes_ranking_filters_min_games_field_none(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(
            name="test",
            source="FIELDING",
            value_key="fielding_pct",
            min_games_field="games",
            min_games=10,
        )
        row = {"games": None, "fielding_pct": 0.990}
        assert agg._passes_ranking_filters(row, config) is False

    def test_passes_ranking_filters_min_games_field_passes(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(
            name="test",
            source="FIELDING",
            value_key="fielding_pct",
            min_games_field="games",
            min_games=10,
        )
        row = {"games": 15, "fielding_pct": 0.990}
        assert agg._passes_ranking_filters(row, config) is True

    def test_resolve_value_saber_config_no_extra(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="woba", source="BATTING", value_key="woba", min_pa=0)
        row = {"player_id": 1, "plate_appearances": 100}
        result = agg._resolve_value(row, config)
        assert result is None

    def test_resolve_value_non_saber_extra_stats_fallback(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0)
        row = {"player_id": 1, "plate_appearances": 100, "extra_stats": {"avg": 0.312}}
        result = agg._resolve_value(row, config)
        assert result == 0.312

    def test_resolve_value_non_saber_extra_stats_upper(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0)
        row = {"player_id": 1, "plate_appearances": 100, "extra_stats": {"AVG": 0.298}}
        result = agg._resolve_value(row, config)
        assert result == 0.298

    def test_resolve_value_non_saber_extra_stats_no_match(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0)
        row = {"player_id": 1, "plate_appearances": 100, "extra_stats": {"obp": 0.350}}
        result = agg._resolve_value(row, config)
        assert result is None

    def test_ranking_entry_no_entity_id(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="test", source="FIELDING", value_key="fielding_pct")
        row = {"fielding_pct": 0.990}
        assert agg._ranking_entry(row, config) is None

    def test_ranking_entry_fails_filters(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="test", source="BATTING", value_key="avg", min_pa=100)
        row = {"player_id": 1, "player_name": "A", "plate_appearances": 50, "avg": 0.300}
        assert agg._ranking_entry(row, config) is None

    def test_ranking_entry_success(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="test", source="BATTING", value_key="avg", min_pa=0)
        row = {"player_id": 1, "player_name": "A", "team_id": "LG", "plate_appearances": 100, "avg": 0.300}
        entry = agg._ranking_entry(row, config)
        assert entry is not None
        assert entry["entity_id"] == "1"

    def test_ranking_entry_uses_player_name_as_fallback(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="test", source="FIELDING", value_key="fielding_pct")
        row = {"player_name": "TestPlayer", "fielding_pct": 0.980}
        entry = agg._ranking_entry(row, config)
        assert entry is not None
        assert entry["entity_id"] == "TestPlayer"

    def test_ranking_extra_batting_qualified(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=100)
        entry = {"entity_id": "1", "raw": {"plate_appearances": 150}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["pa"] == 150
        assert extra["min_pa"] == 100
        assert extra["qualified"] is True

    def test_ranking_extra_batting_not_qualified(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=100)
        entry = {"entity_id": "1", "raw": {"plate_appearances": 50}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["qualified"] is False

    def test_ranking_extra_batting_uses_kbo_min_pa(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg")
        entry = {"entity_id": "1", "raw": {"plate_appearances": 80}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=200, kbo_min_ip_outs=None)
        assert extra["min_pa"] == 200
        assert extra["qualified"] is False

    def test_ranking_extra_pitching_qualified(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="era", source="PITCHING", value_key="era", min_ip_outs=100)
        entry = {"entity_id": "1", "raw": {"innings_outs": 200}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["innings_outs"] == 200
        assert extra["min_ip_outs"] == 100
        assert extra["qualified"] is True

    def test_ranking_extra_pitching_not_qualified(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="era", source="PITCHING", value_key="era", min_ip_outs=100)
        entry = {"entity_id": "1", "raw": {"innings_outs": 30}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["qualified"] is False

    def test_ranking_extra_pitching_uses_kbo_min_ip_outs(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="era", source="PITCHING", value_key="era")
        entry = {"entity_id": "1", "raw": {"innings_outs": 50}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=150)
        assert extra["min_ip_outs"] == 150
        assert extra["qualified"] is False

    def test_ranking_extra_rank_mode_all(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg_all", source="BATTING", value_key="avg")
        entry = {"entity_id": "1", "raw": {"plate_appearances": 50}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["rank_mode"] == "all"

    def test_ranking_extra_rank_mode_qualified(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg")
        entry = {"entity_id": "1", "raw": {"plate_appearances": 50}}
        extra = agg._ranking_extra(entry, config, kbo_min_pa=None, kbo_min_ip_outs=None)
        assert extra["rank_mode"] == "qualified"

    def test_ranking_entry_none_value_skipped(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="test", source="FIELDING", value_key="fielding_pct")
        row = {"player_id": 1, "player_name": "A", "fielding_pct": None}
        assert agg._ranking_entry(row, config) is None

    def test_rank_single_metric_ties_first_entry_not_tie(self):
        agg = RankingAggregator.__new__(RankingAggregator)
        config = MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0)
        rows = [
            {"player_id": 1, "player_name": "A", "team_id": "LG", "plate_appearances": 100, "avg": 0.300},
            {"player_id": 2, "player_name": "B", "team_id": "SS", "plate_appearances": 100, "avg": 0.300},
        ]
        ranked = agg._rank_single_metric(2025, rows, config)
        assert ranked[0]["rank"] == 1
        assert ranked[0]["is_tie"] is False
        assert ranked[1]["rank"] == 1
        assert ranked[1]["is_tie"] is True

    def test_generate_rankings_persist_true(self):
        fake_repo = _FakeRepo()
        agg = RankingAggregator(fake_repo)
        rows = [
            {
                "player_id": 1,
                "player_name": "A",
                "team_id": "LG",
                "fielding_pct": 0.990,
                "putouts": 50,
                "assists": 10,
                "errors": 2,
            }
        ]
        results = agg.generate_rankings(2025, fielding_stats=rows, persist=True)
        assert len(results) >= 1
        assert fake_repo.called


class _FakeRepo:
    def __init__(self):
        self.called = False

    def save_rankings(self, rankings):
        self.called = True
        return len(rankings)


class TestTeamFieldingAggregatorEdgeBranches:
    @pytest.fixture
    def session(self):
        engine = create_engine("sqlite:///:memory:")
        PlayerSeasonFielding.__table__.create(bind=engine)
        PlayerSeasonBaserunning.__table__.create(bind=engine)
        Team.__table__.create(bind=engine)
        TeamSeasonFielding.__table__.create(bind=engine)
        TeamSeasonBaserunning.__table__.create(bind=engine)
        sess = sessionmaker(bind=engine)()
        yield sess
        sess.close()

    def test_baserunning_upsert_existing(self, session):
        session.add(Team(team_id="LG", franchise_id=1, team_name="LG", team_short_name="LG", city="서울"))
        session.commit()
        session.add(
            TeamSeasonBaserunning(
                season=2025,
                team_code="LG",
                stolen_bases=0,
                caught_stealing=0,
                sb_success_rate=None,
                out_on_base=0,
            ),
        )
        session.commit()
        session.add(
            PlayerSeasonBaserunning(
                player_id=10001,
                year=2025,
                team_id="LG",
                stolen_bases=40,
                caught_stealing=8,
                out_on_base=3,
            ),
        )
        session.commit()
        agg = TeamFieldingAggregator(session)
        agg.run_all(2025, ["LG"])
        saved = session.query(TeamSeasonBaserunning).filter_by(team_code="LG").first()
        assert saved.stolen_bases == 40


class TestClutchAggregatorEdgeBranches:
    @pytest.fixture
    def session(self):
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

    def _add_season(self, session, season_id=1, year=2025):
        session.add(
            KboSeason(season_id=season_id, season_year=year, league_type_code=1, league_type_name="정규시즌"),
        )
        session.commit()

    def _add_game(self, session, game_id="20250101", status="COMPLETED", season_id=1):
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

    def _add_event(
        self, session, game_id="20250101", batter_id=10001, wpa=0.05, win_expectancy_before=0.5, event_seq=1
    ):
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

    def _add_player(self, session, player_id=10001, name="테스트"):
        session.add(PlayerBasic(player_id=player_id, name=name))
        session.commit()

    def _add_team(self, session, team_id="LG"):
        session.add(
            Team(team_id=team_id, team_name="LG", team_short_name="LG", city="서울", is_active=True),
        )
        session.commit()

    def _add_player_season_batting(self, session, player_id=10001, season=2025):
        session.add(
            PlayerSeasonBatting(
                player_id=player_id,
                season=season,
                league="REGULAR",
                level="KBO1",
                source="ROLLUP",
                team_code="LG",
            ),
        )
        session.commit()

    def test_persist_skips_null_pid_in_fk_fallback(self, session):
        self._add_season(session)
        self._add_game(session)
        self._add_game(session, game_id="20250102")
        self._add_event(session, batter_id=10001, wpa=0.20)
        self._add_event(session, batter_id=10002, wpa=0.10, game_id="20250102")
        self._add_player(session, player_id=10001)
        self._add_player(session, player_id=10002)
        self._add_team(session)
        self._add_player_season_batting(session, player_id=10001)
        self._add_player_season_batting(session, player_id=10002)
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
        psb1 = session.query(PlayerSeasonBatting).filter_by(player_id=10001, season=2025).first()
        assert psb1.extra_stats is not None
        assert psb1.extra_stats["wpa_sum"] == 0.20

    def test_persist_propagates_non_fk_error(self, session):
        self._add_season(session)
        self._add_game(session)
        self._add_event(session, batter_id=10001, wpa=0.10)
        self._add_player(session, player_id=10001)
        self._add_team(session)
        self._add_player_season_batting(session, player_id=10001)
        agg = ClutchAggregator(session)

        def raise_other():
            raise exc.SQLAlchemyError("db connection lost")

        session.commit = raise_other
        with pytest.raises(exc.SQLAlchemyError):
            agg.persist_to_extra_stats(2025)
        session.rollback()
