import datetime
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.aggregators.season_stat_aggregator import SeasonStatAggregator
from src.models.game import Game, GameBattingStat, GameEvent, GameLineup, GamePitchingStat
from src.models.player import PlayerBasic
from src.models.season import KboSeason


def test_aggregate_pitching_season_decisions(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'test_aggregator.db'}")
    for table in (
        KboSeason.__table__,
        Game.__table__,
        GamePitchingStat.__table__,
        PlayerBasic.__table__,
    ):
        table.create(bind=engine)

    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        season = KboSeason(season_id=1, season_year=2025, league_type_code=1, league_type_name="KBO 정규시즌")
        session.add(season)

        g1 = Game(game_id="20250401OBWO0", season_id=1, game_date=datetime.date(2025, 4, 1))
        g2 = Game(game_id="20250402OBWO0", season_id=1, game_date=datetime.date(2025, 4, 2))
        g3 = Game(game_id="20250403OBWO0", season_id=1, game_date=datetime.date(2025, 4, 3))
        session.add_all([g1, g2, g3])

        p1 = GamePitchingStat(
            game_id="20250401OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision="W",
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=15,
        )
        p2 = GamePitchingStat(
            game_id="20250402OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision=None,
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=6,
        )
        p3 = GamePitchingStat(
            game_id="20250403OBWO0",
            player_id=9999,
            player_name="테스트투수",
            appearance_seq=1,
            team_side="away",
            decision="W",
            wins=2,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=18,
        )
        session.add_all([p1, p2, p3])
        session.commit()

        stats = SeasonStatAggregator.aggregate_pitching_season(session, 9999, 2025, "regular")
        assert stats is not None
        assert stats["games"] == 3
        assert stats["wins"] == 2
        assert stats["losses"] == 0
        assert stats["saves"] == 0
        assert stats["holds"] == 0
        assert stats["innings_outs"] == 39
        assert stats["innings_pitched"] == 13.0

        bulk_stats = SeasonStatAggregator.aggregate_pitching_season_bulk(session, 2025, "regular")
        assert len(bulk_stats) == 1
        p_stats = bulk_stats[0]
        assert p_stats["player_id"] == 9999
        assert p_stats["games"] == 3
        assert p_stats["wins"] == 2
        assert p_stats["losses"] == 0
        assert p_stats["saves"] == 0
        assert p_stats["holds"] == 0


def _create_session(tmp_path, db_name="test_aggregator.db"):
    engine = create_engine(f"sqlite:///{tmp_path / db_name}")
    for table in (
        KboSeason.__table__,
        Game.__table__,
        PlayerBasic.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameLineup.__table__,
        GameEvent.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine)


def _add_season(session, season_id=1, year=2025, league_type_name="정규시즌"):
    session.add(KboSeason(season_id=season_id, season_year=year, league_type_code=1, league_type_name=league_type_name))
    session.commit()


def _add_game(session, game_id="20250315LGSS0", season_id=1, status="COMPLETED"):
    session.add(
        Game(
            game_id=game_id,
            game_status=status,
            season_id=season_id,
            game_date=date(2025, 3, 15),
            home_team="LG",
            away_team="SS",
            stadium="잠실",
        )
    )
    session.commit()


def _add_player(session, player_id, name, team="LG"):
    session.add(PlayerBasic(player_id=player_id, name=name, team=team))
    session.commit()


def _add_batting_stat(session, game_id, player_id, player_name="Test", team_code="LG", **stats):
    defaults = {
        "team_side": "HOME",
        "plate_appearances": 4,
        "at_bats": 3,
        "runs": 0,
        "hits": 1,
        "doubles": 0,
        "triples": 0,
        "home_runs": 0,
        "rbi": 0,
        "walks": 1,
        "intentional_walks": 0,
        "hbp": 0,
        "strikeouts": 0,
        "stolen_bases": 0,
        "caught_stealing": 0,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,
        "gdp": 0,
    }
    defaults.update(stats)
    session.add(
        GameBattingStat(
            game_id=game_id,
            player_id=player_id,
            player_name=player_name,
            team_code=team_code,
            appearance_seq=1,
            **defaults,
        )
    )
    session.commit()


def _add_pitching_stat(session, game_id, player_id, player_name="Test", team_code="LG", **stats):
    defaults = {
        "team_side": "HOME",
        "innings_outs": 18,
        "hits_allowed": 5,
        "runs_allowed": 2,
        "earned_runs": 2,
        "home_runs_allowed": 1,
        "walks_allowed": 2,
        "hit_batters": 0,
        "strikeouts": 4,
        "wild_pitches": 0,
        "balks": 0,
        "batters_faced": 20,
        "pitches": 70,
        "decision": "",
        "is_starting": False,
    }
    defaults.update(stats)
    session.add(
        GamePitchingStat(
            game_id=game_id,
            player_id=player_id,
            player_name=player_name,
            team_code=team_code,
            appearance_seq=1,
            **defaults,
        )
    )
    session.commit()


def _add_lineup(session, game_id, player_id, standard_position, player_name="Test", appearance_seq=1):
    session.add(
        GameLineup(
            game_id=game_id,
            team_side="HOME",
            player_id=player_id,
            player_name=player_name,
            standard_position=standard_position,
            appearance_seq=appearance_seq,
        )
    )
    session.commit()


def _add_event(session, game_id, description, batter_id=None, event_seq=1):
    session.add(GameEvent(game_id=game_id, event_seq=event_seq, batter_id=batter_id, description=description))
    session.commit()


class TestGetLeagueNamePattern:
    def test_regular(self):
        assert SeasonStatAggregator._get_league_name_pattern("regular") == "정규시즌"

    def test_korean_series(self):
        assert SeasonStatAggregator._get_league_name_pattern("korean_series") == "한국시리즈"

    def test_case_insensitive(self):
        assert SeasonStatAggregator._get_league_name_pattern("Regular") == "정규시즌"

    def test_unknown_passthrough(self):
        assert SeasonStatAggregator._get_league_name_pattern("championship") == "championship"

    def test_empty_string(self):
        assert SeasonStatAggregator._get_league_name_pattern("") == ""


class TestAggregateBattingSeason:
    def test_returns_none_when_no_stats(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is None

    def test_aggregates_single_game(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", hits=2, doubles=1
            )
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["games"] == 1
            assert result["hits"] == 2
            assert result["doubles"] == 1
            assert result["at_bats"] == 3
            assert result["player_id"] == 10001
            assert result["season"] == 2025
            assert result["league"] == "REGULAR"

    def test_aggregates_multiple_games(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", hits=2, runs=1)
            _add_batting_stat(session, game_id="20250316LGSS0", player_id=10001, player_name="김테스트", hits=1, runs=2)
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["games"] == 2
            assert result["hits"] == 3
            assert result["runs"] == 3

    def test_null_coalescing(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                hits=1,
                doubles=None,
                triples=None,
                home_runs=None,
            )
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["doubles"] == 0
            assert result["triples"] == 0
            assert result["home_runs"] == 0

    def test_filters_by_year(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, year=2024)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트")
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is None

    def test_filters_by_series(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, league_type_name="POSTSEASON")
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트")
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is None

    def test_ratios_calculated(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                plate_appearances=4,
                at_bats=3,
                hits=2,
                doubles=1,
                home_runs=0,
                walks=1,
                hbp=0,
                sacrifice_flies=0,
                strikeouts=0,
            )
            result = SeasonStatAggregator.aggregate_batting_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["avg"] == pytest.approx(0.667, abs=0.001)
            assert result["obp"] == pytest.approx(0.75, abs=0.001)
            assert result["slg"] == pytest.approx(1.0, abs=0.001)
            assert result["ops"] == pytest.approx(1.75, abs=0.001)


class TestAggregateBattingSeasonBulk:
    def test_empty_when_no_stats(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            results = SeasonStatAggregator.aggregate_batting_season_bulk(session, year=2025, series="regular")
            assert results == []

    def test_single_player(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", hits=2)
            _add_batting_stat(session, game_id="20250316LGSS0", player_id=10001, player_name="김테스트", hits=1)
            results = SeasonStatAggregator.aggregate_batting_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["games"] == 2
            assert results[0]["hits"] == 3
            assert results[0]["player_id"] == 10001

    def test_multiple_players(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_player(session, player_id=10002, name="박테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", hits=2)
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10002, player_name="박테스트", hits=1)
            results = SeasonStatAggregator.aggregate_batting_season_bulk(session, year=2025, series="regular")
            assert len(results) == 2
            by_pid = {r["player_id"]: r for r in results}
            assert by_pid[10001]["hits"] == 2
            assert by_pid[10002]["hits"] == 1

    def test_null_values_coalesced(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                hits=1,
                doubles=None,
                triples=None,
                home_runs=None,
            )
            results = SeasonStatAggregator.aggregate_batting_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["doubles"] == 0
            assert results[0]["triples"] == 0
            assert results[0]["home_runs"] == 0

    def test_filters_by_series(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, league_type_name="정규시즌")
            _add_season(session, season_id=2, league_type_name="POSTSEASON")
            _add_game(session, game_id="20250315LGSS0", season_id=1)
            _add_game(session, game_id="20250316LGSS0", season_id=2)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", hits=2)
            _add_batting_stat(session, game_id="20250316LGSS0", player_id=10001, player_name="김테스트", hits=5)
            results = SeasonStatAggregator.aggregate_batting_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["hits"] == 2


class TestAggregatePitchingSeasonNullAndFilters:
    def test_aggregates_single_game(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(
                session,
                game_id="20250315LGSS0",
                player_id=20001,
                player_name="투수A",
                innings_outs=18,
                earned_runs=2,
                strikeouts=4,
                hits_allowed=5,
                walks_allowed=2,
                home_runs_allowed=1,
            )
            result = SeasonStatAggregator.aggregate_pitching_season(
                session, player_id=20001, year=2025, series="regular"
            )
            assert result is not None
            assert result["games"] == 1
            assert result["earned_runs"] == 2
            assert result["strikeouts"] == 4
            assert result["innings_outs"] == 18
            assert result["innings_pitched"] == pytest.approx(6.0, abs=0.1)

    def test_null_coalescing(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(
                session,
                game_id="20250315LGSS0",
                player_id=20001,
                player_name="투수A",
                home_runs_allowed=None,
                hit_batters=None,
                wild_pitches=None,
                balks=None,
            )
            result = SeasonStatAggregator.aggregate_pitching_season(
                session, player_id=20001, year=2025, series="regular"
            )
            assert result is not None
            assert result["home_runs_allowed"] == 0
            assert result["hit_batters"] == 0
            assert result["wild_pitches"] == 0
            assert result["balks"] == 0

    def test_filters_by_year(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, year=2024)
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(session, game_id="20250315LGSS0", player_id=20001, player_name="투수A")
            result = SeasonStatAggregator.aggregate_pitching_season(
                session, player_id=20001, year=2025, series="regular"
            )
            assert result is None

    def test_filters_by_series(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, league_type_name="POSTSEASON")
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(session, game_id="20250315LGSS0", player_id=20001, player_name="투수A")
            result = SeasonStatAggregator.aggregate_pitching_season(
                session, player_id=20001, year=2025, series="regular"
            )
            assert result is None


class TestAggregatePitchingSeasonBulkExtended:
    def test_single_player(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(
                session, game_id="20250315LGSS0", player_id=20001, player_name="투수A", earned_runs=2, innings_outs=18
            )
            _add_pitching_stat(
                session, game_id="20250316LGSS0", player_id=20001, player_name="투수A", earned_runs=4, innings_outs=18
            )
            results = SeasonStatAggregator.aggregate_pitching_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["games"] == 2
            assert results[0]["earned_runs"] == 6
            assert results[0]["innings_outs"] == 36

    def test_multiple_players(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_player(session, player_id=20002, name="투수B")
            _add_pitching_stat(session, game_id="20250315LGSS0", player_id=20001, player_name="투수A", strikeouts=5)
            _add_pitching_stat(session, game_id="20250315LGSS0", player_id=20002, player_name="투수B", strikeouts=3)
            results = SeasonStatAggregator.aggregate_pitching_season_bulk(session, year=2025, series="regular")
            assert len(results) == 2
            by_pid = {r["player_id"]: r for r in results}
            assert by_pid[20001]["strikeouts"] == 5
            assert by_pid[20002]["strikeouts"] == 3

    def test_null_values_coalesced(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=20001, name="투수A")
            _add_pitching_stat(
                session,
                game_id="20250315LGSS0",
                player_id=20001,
                player_name="투수A",
                home_runs_allowed=None,
                hit_batters=None,
            )
            results = SeasonStatAggregator.aggregate_pitching_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["home_runs_allowed"] == 0
            assert results[0]["hit_batters"] == 0


class TestAggregateBaserunningSeason:
    def test_returns_none_when_no_stats(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            result = SeasonStatAggregator.aggregate_baserunning_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is None

    def test_aggregates_sb_cs(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=3,
                caught_stealing=1,
            )
            _add_batting_stat(
                session,
                game_id="20250316LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=1,
                caught_stealing=0,
            )
            result = SeasonStatAggregator.aggregate_baserunning_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["stolen_bases"] == 4
            assert result["caught_stealing"] == 1
            assert result["stolen_base_attempts"] == 5
            assert result["stolen_base_percentage"] == pytest.approx(80.0, abs=0.1)

    def test_zero_attempts(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=0,
                caught_stealing=0,
            )
            result = SeasonStatAggregator.aggregate_baserunning_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["stolen_base_attempts"] == 0
            assert result["stolen_base_percentage"] == 0.0

    def test_null_values(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=None,
                caught_stealing=None,
            )
            result = SeasonStatAggregator.aggregate_baserunning_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is not None
            assert result["stolen_bases"] == 0
            assert result["caught_stealing"] == 0
            assert result["stolen_base_attempts"] == 0
            assert result["stolen_base_percentage"] == 0.0

    def test_filters_by_year(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session, year=2024)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(session, game_id="20250315LGSS0", player_id=10001, player_name="김테스트", stolen_bases=5)
            result = SeasonStatAggregator.aggregate_baserunning_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert result is None


class TestAggregateBaserunningSeasonBulk:
    def test_empty(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            results = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year=2025, series="regular")
            assert results == []

    def test_single_player(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=2,
                caught_stealing=1,
            )
            _add_batting_stat(
                session,
                game_id="20250316LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=3,
                caught_stealing=0,
            )
            results = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["stolen_bases"] == 5
            assert results[0]["caught_stealing"] == 1
            assert results[0]["stolen_base_attempts"] == 6
            assert results[0]["stolen_base_percentage"] == pytest.approx(83.3, abs=0.1)

    def test_multiple_players(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_player(session, player_id=10002, name="박테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=4,
                caught_stealing=1,
            )
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10002,
                player_name="박테스트",
                stolen_bases=1,
                caught_stealing=2,
            )
            results = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year=2025, series="regular")
            assert len(results) == 2
            by_pid = {r["player_id"]: r for r in results}
            assert by_pid[10001]["stolen_base_percentage"] == pytest.approx(80.0, abs=0.1)
            assert by_pid[10002]["stolen_base_percentage"] == pytest.approx(33.3, abs=0.1)

    def test_zero_attempts_in_bulk(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_batting_stat(
                session,
                game_id="20250315LGSS0",
                player_id=10001,
                player_name="김테스트",
                stolen_bases=0,
                caught_stealing=0,
            )
            results = SeasonStatAggregator.aggregate_baserunning_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["stolen_base_attempts"] == 0
            assert results[0]["stolen_base_percentage"] == 0.0


class TestAggregateFieldingSeason:
    def test_returns_empty_when_player_not_found(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=99999, year=2025, series="regular"
            )
            assert result == []

    def test_returns_position_games(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS")
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 1
            assert result[0]["position_id"] == "SS"
            assert result[0]["games"] == 1
            assert result[0]["errors"] == 0

    def test_multiple_positions(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS", appearance_seq=1)
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="2B", appearance_seq=2)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 2
            by_pos = {r["position_id"]: r for r in result}
            assert "SS" in by_pos
            assert "2B" in by_pos

    def test_counts_errors_by_name(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS")
            _add_event(session, game_id="20250315LGSS0", description="김테스트 실책", batter_id=10001)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 1
            assert result[0]["errors"] == 1

    def test_counts_errors_by_position(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS")
            _add_event(session, game_id="20250315LGSS0", description="SS 실책", batter_id=10001)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 1
            assert result[0]["errors"] == 1

    def test_ignores_unrelated_errors(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS")
            _add_event(session, game_id="20250315LGSS0", description="박다른 실책", batter_id=10002, event_seq=2)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 1
            assert result[0]["errors"] == 0

    def test_single_player_null_position_skipped(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position=None, appearance_seq=2)
            result = SeasonStatAggregator.aggregate_fielding_season(
                session, player_id=10001, year=2025, series="regular"
            )
            assert len(result) == 1
            assert result[0]["position_id"] == "SS"


class TestAggregateFieldingSeasonBulk:
    def test_empty_when_no_lineups(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            results = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year=2025, series="regular")
            assert results == []

    def test_aggregates_all_players(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_player(session, player_id=10002, name="박테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS", appearance_seq=1)
            _add_lineup(session, game_id="20250315LGSS0", player_id=10002, standard_position="2B", appearance_seq=2)
            results = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year=2025, series="regular")
            assert len(results) == 2
            by_pid = {r["player_id"]: r for r in results}
            assert by_pid[10001]["position_id"] == "SS"
            assert by_pid[10002]["position_id"] == "2B"

    def test_error_assignment(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_player(session, player_id=10002, name="박테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS", appearance_seq=1)
            _add_lineup(session, game_id="20250315LGSS0", player_id=10002, standard_position="2B", appearance_seq=2)
            _add_event(session, game_id="20250315LGSS0", description="김테스트 실책", batter_id=10001)
            results = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year=2025, series="regular")
            by_pid = {r["player_id"]: r for r in results}
            assert by_pid[10001]["errors"] == 1
            assert by_pid[10002]["errors"] == 0

    def test_skips_null_pid_in_lineup(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session)
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250315LGSS0", player_id=10001, standard_position="SS", appearance_seq=1)
            _add_lineup(session, game_id="20250315LGSS0", player_id=None, standard_position="2B", appearance_seq=2)
            _add_event(session, game_id="20250315LGSS0", description="아무개 실책", batter_id=99999, event_seq=2)
            results = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year=2025, series="regular")
            by_pid = {r["player_id"]: r for r in results}
            assert 10001 in by_pid
            assert by_pid[10001]["errors"] == 0

    def test_no_lineups_for_game_skips_gracefully(self, tmp_path):
        SessionLocal = _create_session(tmp_path)
        with SessionLocal() as session:
            _add_season(session)
            _add_game(session, game_id="20250315LGSS0")
            _add_game(session, game_id="20250316LGSS0")
            _add_player(session, player_id=10001, name="김테스트")
            _add_lineup(session, game_id="20250316LGSS0", player_id=10001, standard_position="SS")
            _add_event(session, game_id="20250315LGSS0", description="김테스트 실책", batter_id=10001, event_seq=1)
            results = SeasonStatAggregator.aggregate_fielding_season_bulk(session, year=2025, series="regular")
            assert len(results) == 1
            assert results[0]["errors"] == 0
