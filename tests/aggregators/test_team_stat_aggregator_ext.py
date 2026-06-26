from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.aggregators.team_stat_aggregator import (
    DEFAULT_TEAM_NAMES,
    TeamAggregationQuery,
    TeamStatAggregator,
)
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import Team


def _make_batting(**kwargs):
    defaults = {
        "id": 1,
        "season": 2025,
        "team_code": "OB",
        "league": "REGULAR",
        "games": 10,
        "plate_appearances": 40,
        "at_bats": 35,
        "runs": 5,
        "hits": 10,
        "doubles": 2,
        "triples": 0,
        "home_runs": 1,
        "rbi": 5,
        "walks": 4,
        "intentional_walks": 0,
        "hbp": 1,
        "strikeouts": 5,
        "stolen_bases": 1,
        "caught_stealing": 0,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,
        "gdp": 1,
    }
    defaults.update(kwargs)
    return PlayerSeasonBatting(**defaults)


def _make_pitching(**kwargs):
    defaults = {
        "id": 1,
        "season": 2025,
        "team_code": "OB",
        "league": "REGULAR",
        "games": 5,
        "wins": 2,
        "losses": 1,
        "saves": 1,
        "holds": 0,
        "innings_outs": 15,
        "hits_allowed": 10,
        "runs_allowed": 4,
        "earned_runs": 3,
        "home_runs_allowed": 1,
        "walks_allowed": 5,
        "intentional_walks": 0,
        "hit_batters": 1,
        "strikeouts": 8,
    }
    defaults.update(kwargs)
    return PlayerSeasonPitching(**defaults)


def _mock_row(**attrs):
    row = MagicMock()
    for k, v in attrs.items():
        setattr(row, k, v)
    return row


def _mock_db_session_for_agg(query_rows, team_rows=None):
    mock_session = MagicMock()
    agg_query = MagicMock()
    agg_query.all.return_value = query_rows
    agg_query.filter.return_value = agg_query
    agg_query.group_by.return_value = agg_query
    team_query_result = MagicMock()
    team_query_result.all.return_value = team_rows or []
    mock_session.query.side_effect = [agg_query, team_query_result]
    return mock_session


class TestBuildAggregationQuery:
    def setup_method(self):
        self.agg = TeamStatAggregator()

    def test_passthrough_team_aggregation_query(self):
        q = TeamAggregationQuery(season=2025, team_id="OB")
        result = self.agg._build_aggregation_query(q)
        assert result is q

    def test_int_season(self):
        result = self.agg._build_aggregation_query(
            2025,
            team_id="OB",
            rows=None,
            team_names={"OB": "두산"},
            team_games_map={(2025, "OB"): 144},
            dry_run=True,
        )
        assert isinstance(result, TeamAggregationQuery)
        assert result.season == 2025
        assert result.team_id == "OB"
        assert result.team_names == {"OB": "두산"}
        assert result.team_games_map == {(2025, "OB"): 144}
        assert result.dry_run is True

    def test_iterable_rows(self):
        rows = [_make_batting()]
        result = self.agg._build_aggregation_query(
            rows,
            team_id="OB",
            team_names={"OB": "두산"},
        )
        assert isinstance(result, TeamAggregationQuery)
        assert result.season is None
        assert result.rows is rows


class TestAggregateBattingDispatch:
    def setup_method(self):
        self.agg = TeamStatAggregator()

    def test_raises_when_no_season_or_rows(self):
        with pytest.raises(ValueError, match="Either an integer season or rows iterable"):
            self.agg.aggregate_batting(TeamAggregationQuery())

    def test_dispatches_to_db_for_season(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        with patch.object(agg, "_aggregate_batting_db", return_value=[]) as m:
            result = agg.aggregate_batting(TeamAggregationQuery(season=2025))
            m.assert_called_once_with(2025, None, dry_run=False)
            assert result == []

    def test_dispatches_to_mem_for_rows(self):
        rows = [_make_batting()]
        with patch.object(self.agg, "_aggregate_batting_mem", return_value=[]) as m:
            result = self.agg.aggregate_batting(rows)
            m.assert_called_once()
            assert result == []

    def test_int_query_dispatches_to_db(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        with patch.object(agg, "_aggregate_batting_db", return_value=[]) as m:
            agg.aggregate_batting(2025, team_id="OB")
            m.assert_called_once_with(2025, "OB", dry_run=False)


class TestAggregatePitchingDispatch:
    def setup_method(self):
        self.agg = TeamStatAggregator()

    def test_raises_when_no_season_or_rows(self):
        with pytest.raises(ValueError, match="Either an integer season or rows iterable"):
            self.agg.aggregate_pitching(TeamAggregationQuery())

    def test_dispatches_to_db_for_season(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        with patch.object(agg, "_aggregate_pitching_db", return_value=[]) as m:
            result = agg.aggregate_pitching(TeamAggregationQuery(season=2025))
            m.assert_called_once_with(2025, None, dry_run=False)
            assert result == []

    def test_dispatches_to_mem_for_rows(self):
        rows = [_make_pitching()]
        with patch.object(self.agg, "_aggregate_pitching_mem", return_value=[]) as m:
            result = self.agg.aggregate_pitching(rows)
            m.assert_called_once()
            assert result == []


class TestAggregateBattingDB:
    def test_raises_without_session(self):
        agg = TeamStatAggregator(None)
        with pytest.raises(ValueError, match="Database session is required"):
            agg._aggregate_batting_db(2025)

    def test_returns_empty_when_no_rows(self):
        session = _mock_db_session_for_agg([])
        agg = TeamStatAggregator(session)
        result = agg._aggregate_batting_db(2025, dry_run=True)
        assert result == []

    def test_dry_run_skips_save(self):
        row = _mock_row(
            team_id="OB",
            plate_appearances=100,
            at_bats=80,
            runs=15,
            hits=25,
            doubles=5,
            triples=1,
            home_runs=3,
            rbi=10,
            walks=12,
            intentional_walks=2,
            hbp=3,
            strikeouts=10,
            stolen_bases=5,
            caught_stealing=1,
            sacrifice_hits=2,
            sacrifice_flies=1,
            gdp=3,
            max_player_games=50,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        with patch.object(agg, "_get_team_games", return_value=0):
            with patch.object(agg, "_save_batting_records") as mock_save:
                result = agg._aggregate_batting_db(2025, dry_run=True)
                mock_save.assert_not_called()
        assert len(result) == 1
        assert result[0]["team_id"] == "OB"
        assert result[0]["games"] == 50

    def test_with_team_id_filter(self):
        session = _mock_db_session_for_agg([])
        agg = TeamStatAggregator(session)
        agg._aggregate_batting_db(2025, team_id="OB", dry_run=True)

    def test_saves_when_not_dry_run(self):
        row = _mock_row(
            team_id="OB",
            plate_appearances=100,
            at_bats=80,
            runs=15,
            hits=25,
            doubles=5,
            triples=1,
            home_runs=3,
            rbi=10,
            walks=12,
            intentional_walks=2,
            hbp=3,
            strikeouts=10,
            stolen_bases=5,
            caught_stealing=1,
            sacrifice_hits=2,
            sacrifice_flies=1,
            gdp=3,
            max_player_games=50,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        with patch.object(agg, "_get_team_games", return_value=0):
            with patch.object(agg, "_save_batting_records") as mock_save:
                agg._aggregate_batting_db(2025, dry_run=False)
                mock_save.assert_called_once()

    def test_fallback_to_max_player_games(self):
        row = _mock_row(
            team_id="OB",
            plate_appearances=100,
            at_bats=80,
            runs=15,
            hits=25,
            doubles=5,
            triples=1,
            home_runs=3,
            rbi=10,
            walks=12,
            intentional_walks=2,
            hbp=3,
            strikeouts=10,
            stolen_bases=5,
            caught_stealing=1,
            sacrifice_hits=2,
            sacrifice_flies=1,
            gdp=3,
            max_player_games=77,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        with patch.object(agg, "_get_team_games", return_value=0):
            result = agg._aggregate_batting_db(2025, dry_run=True)
        assert result[0]["games"] == 77

    def test_uses_team_games_from_standings(self):
        row = _mock_row(
            team_id="OB",
            plate_appearances=100,
            at_bats=80,
            runs=15,
            hits=25,
            doubles=5,
            triples=1,
            home_runs=3,
            rbi=10,
            walks=12,
            intentional_walks=2,
            hbp=3,
            strikeouts=10,
            stolen_bases=5,
            caught_stealing=1,
            sacrifice_hits=2,
            sacrifice_flies=1,
            gdp=3,
            max_player_games=50,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        with patch.object(agg, "_get_team_games", return_value=144):
            result = agg._aggregate_batting_db(2025, dry_run=True)
        assert result[0]["games"] == 144


class TestAggregatePitchingDB:
    def test_raises_without_session(self):
        agg = TeamStatAggregator(None)
        with pytest.raises(ValueError, match="Database session is required"):
            agg._aggregate_pitching_db(2025)

    def test_returns_empty_when_no_rows(self):
        session = _mock_db_session_for_agg([])
        agg = TeamStatAggregator(session)
        result = agg._aggregate_pitching_db(2025, dry_run=True)
        assert result == []

    def test_pitching_db_with_standings_record(self):
        row = _mock_row(
            team_id="OB",
            wins=70,
            losses=60,
            saves=30,
            holds=50,
            innings_outs=3600,
            hits_allowed=1200,
            runs_allowed=500,
            earned_runs=450,
            home_runs_allowed=100,
            walks_allowed=300,
            intentional_walks=30,
            hit_batters=40,
            strikeouts=900,
            tbf=5400,
            complete_games=5,
            shutouts=3,
            wild_pitches=15,
            balks=3,
            sacrifices_allowed=50,
            sacrifice_flies_allowed=40,
            max_player_games=144,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        rec = {"games": 144, "wins": 70, "losses": 60, "ties": 14}
        with patch.object(agg, "_get_team_record_from_standings", return_value=rec):
            with patch.object(agg, "_save_pitching_records"):
                result = agg._aggregate_pitching_db(2025, dry_run=False)

        assert len(result) == 1
        r = result[0]
        assert r["team_id"] == "OB"
        assert r["games"] == 144
        assert r["ties"] == 14
        assert r["avg_against"] == pytest.approx(1200 / (5400 - 300 - 40 - 50 - 40), rel=1e-4)

    def test_pitching_db_no_standings_fallback(self):
        row = _mock_row(
            team_id="OB",
            wins=70,
            losses=60,
            saves=30,
            holds=50,
            innings_outs=3600,
            hits_allowed=1200,
            runs_allowed=500,
            earned_runs=450,
            home_runs_allowed=100,
            walks_allowed=300,
            intentional_walks=30,
            hit_batters=40,
            strikeouts=900,
            tbf=5400,
            complete_games=5,
            shutouts=3,
            wild_pitches=15,
            balks=3,
            sacrifices_allowed=50,
            sacrifice_flies_allowed=40,
            max_player_games=100,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        rec = {"games": 0, "wins": 0, "losses": 0, "ties": 0}
        with patch.object(agg, "_get_team_record_from_standings", return_value=rec):
            with patch.object(agg, "_save_pitching_records"):
                result = agg._aggregate_pitching_db(2025, dry_run=True)
        r = result[0]
        assert r["games"] == 100
        assert r["ties"] == 0

    def test_pitching_db_zero_opp_ab_avg_against(self):
        row = _mock_row(
            team_id="OB",
            wins=0,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=0,
            hits_allowed=0,
            runs_allowed=0,
            earned_runs=0,
            home_runs_allowed=0,
            walks_allowed=0,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=0,
            tbf=0,
            complete_games=0,
            shutouts=0,
            wild_pitches=0,
            balks=0,
            sacrifices_allowed=0,
            sacrifice_flies_allowed=0,
            max_player_games=0,
        )
        session = _mock_db_session_for_agg(
            [row],
            [MagicMock(team_id="OB", team_name="두산")],
        )
        agg = TeamStatAggregator(session)
        rec = {"games": 0, "wins": 0, "losses": 0, "ties": 0}
        with patch.object(agg, "_get_team_record_from_standings", return_value=rec):
            result = agg._aggregate_pitching_db(2025, dry_run=True)
        assert result[0]["avg_against"] == 0.0


class TestSaveRecords:
    def test_save_batting_records_empty(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        agg._save_batting_records([])
        mock_session.execute.assert_not_called()

    def test_save_pitching_records_empty(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        agg._save_pitching_records([])
        mock_session.execute.assert_not_called()

    def test_save_batting_records_commits(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonBattingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="postgresql"):
                agg._save_batting_records([{"team_id": "OB", "season": 2025}])
        mock_session.commit.assert_called_once()

    def test_save_pitching_records_commits(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonPitchingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="postgresql"):
                agg._save_pitching_records([{"team_id": "OB", "season": 2025}])
        mock_session.commit.assert_called_once()

    def test_save_batting_records_rollback_on_error(self):
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("DB error")
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonBattingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="postgresql"):
                with pytest.raises(Exception, match="DB error"):
                    agg._save_batting_records([{"team_id": "OB", "season": 2025}])
        mock_session.rollback.assert_called_once()

    def test_save_pitching_records_rollback_on_error(self):
        from sqlalchemy.exc import SQLAlchemyError

        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError("DB error")
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonPitchingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="postgresql"):
                with pytest.raises(SQLAlchemyError):
                    agg._save_pitching_records([{"team_id": "OB", "season": 2025}])
        mock_session.rollback.assert_called_once()

    def test_save_pitching_records_sqlite_pragma(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonPitchingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="sqlite"):
                agg._save_pitching_records([{"team_id": "OB", "season": 2025}])
        pragma_calls = [
            c for c in mock_session.execute.call_args_list if hasattr(c[0][0], "text") and "PRAGMA" in c[0][0].text
        ]
        assert len(pragma_calls) == 2
        pragma_texts = [c[0][0].text for c in pragma_calls]
        assert any("OFF" in t for t in pragma_texts)
        assert any("ON" in t for t in pragma_texts)

    def test_save_batting_records_sqlite_pragma(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        mock_repo = MagicMock()
        mock_repo._filter_model_fields.side_effect = lambda x: x
        mock_repo._filter_none.side_effect = lambda x: x
        mock_repo._build_insert_stmt.return_value = MagicMock()
        with patch("src.repositories.team_stats_repository.TeamSeasonBattingRepository", return_value=mock_repo):
            with patch("src.aggregators.team_stat_aggregator.get_database_type", return_value="sqlite"):
                agg._save_batting_records([{"team_id": "OB", "season": 2025}])
        pragma_calls = [
            c for c in mock_session.execute.call_args_list if hasattr(c[0][0], "text") and "PRAGMA" in c[0][0].text
        ]
        assert len(pragma_calls) == 1
        assert "OFF" in pragma_calls[0][0][0].text


class TestGetTeamRecordFromStandings:
    def test_returns_standings_record(self):
        mock_session = MagicMock()
        mock_standings = MagicMock()
        mock_standings.games_played = 144
        mock_standings.wins = 80
        mock_standings.losses = 56
        mock_standings.draws = 8
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = mock_standings
        mock_session.query.return_value = mock_query
        result = TeamStatAggregator._get_team_record_from_standings(mock_session, "OB", 2025)
        assert result == {"games": 144, "wins": 80, "losses": 56, "ties": 8}

    def test_returns_zeros_when_no_standings(self):
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.first.return_value = None
        mock_session.query.return_value = mock_query
        result = TeamStatAggregator._get_team_record_from_standings(mock_session, "OB", 2025)
        assert result == {"games": 0, "wins": 0, "losses": 0, "ties": 0}


class TestAggregateBattingMem:
    def test_skips_missing_season(self):
        p = _make_batting(season=None, id=99)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result == []

    def test_uses_team_games_map(self):
        p = _make_batting()
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p], team_games_map={(2025, "OB"): 144})
        assert result[0]["games"] == 144

    def test_default_team_names(self):
        p = _make_batting(team_code="LG")
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result[0]["team_name"] == DEFAULT_TEAM_NAMES["LG"]

    def test_fallback_team_name_to_code(self):
        p = _make_batting(team_code="XX")
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result[0]["team_name"] == "XX"

    def test_multiple_teams_grouped(self):
        p1 = _make_batting(id=1, team_code="OB", hits=10)
        p2 = _make_batting(id=2, team_code="SS", hits=5)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p1, p2])
        assert len(result) == 2
        ob = next(r for r in result if r["team_id"] == "OB")
        ss = next(r for r in result if r["team_id"] == "SS")
        assert ob["hits"] == 10
        assert ss["hits"] == 5

    def test_skips_empty_team_code(self):
        p = _make_batting(team_code="", id=99)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result == []

    def test_skips_all_team_code(self):
        p = _make_batting(team_code="ALL", id=98)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result == []

    def test_skips_dash_team_code(self):
        p = _make_batting(team_code="-", id=97)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result == []

    def test_none_stats_default_to_zero(self):
        p = _make_batting(hits=None, walks=None, gdp=None)
        agg = TeamStatAggregator()
        result = agg._aggregate_batting_mem([p])
        assert result[0]["hits"] == 0
        assert result[0]["walks"] == 0
        assert result[0]["gdp"] == 0


class TestAggregatePitchingMem:
    def test_skips_missing_season(self):
        p = _make_pitching(season=None, id=99)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result == []

    def test_uses_team_games_map(self):
        p = _make_pitching()
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p], team_games_map={(2025, "OB"): 144})
        assert result[0]["games"] == 144

    def test_default_team_names(self):
        p = _make_pitching(team_code="LG")
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result[0]["team_name"] == DEFAULT_TEAM_NAMES["LG"]

    def test_innings_pitched_calculated(self):
        p = _make_pitching(innings_outs=27)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result[0]["innings_pitched"] == 9.0

    def test_ties_default_zero(self):
        p = _make_pitching()
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result[0]["ties"] == 0

    def test_multiple_pitchers_aggregated(self):
        p1 = _make_pitching(id=1, wins=5, losses=3)
        p2 = _make_pitching(id=2, wins=10, losses=7)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p1, p2])
        assert len(result) == 1
        assert result[0]["wins"] == 15
        assert result[0]["losses"] == 10

    def test_max_player_games_as_fallback(self):
        p1 = _make_pitching(id=1, games=30)
        p2 = _make_pitching(id=2, games=50)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p1, p2])
        assert result[0]["games"] == 50

    def test_skips_empty_team_code(self):
        p = _make_pitching(team_code="", id=99)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result == []

    def test_skips_all_team_code(self):
        p = _make_pitching(team_code="ALL", id=98)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result == []

    def test_none_stats_default_to_zero(self):
        p = _make_pitching(wins=None, losses=None, strikeouts=None)
        agg = TeamStatAggregator()
        result = agg._aggregate_pitching_mem([p])
        assert result[0]["wins"] == 0
        assert result[0]["losses"] == 0
        assert result[0]["strikeouts"] == 0


class TestAggregateAll:
    def test_aggregate_all_returns_both(self):
        mock_session = MagicMock()
        agg = TeamStatAggregator(mock_session)
        with patch.object(agg, "_aggregate_batting_db", return_value=[{"team_id": "OB"}]):
            with patch.object(agg, "_aggregate_pitching_db", return_value=[{"team_id": "OB"}]):
                result = agg.aggregate_all(2025, dry_run=True)
        assert "batting" in result
        assert "pitching" in result
        assert len(result["batting"]) == 1
        assert len(result["pitching"]) == 1


class TestTeamAggregationQueryDataclass:
    def test_defaults(self):
        q = TeamAggregationQuery()
        assert q.season is None
        assert q.team_id is None
        assert q.rows is None
        assert q.team_names is None
        assert q.team_games_map is None
        assert q.dry_run is False

    def test_frozen(self):
        q = TeamAggregationQuery(season=2025)
        with pytest.raises(AttributeError):
            q.season = 2026


class TestDefaultTeamNames:
    def test_contains_all_teams(self):
        assert len(DEFAULT_TEAM_NAMES) == 10
        assert "OB" in DEFAULT_TEAM_NAMES
        assert "LT" in DEFAULT_TEAM_NAMES
        assert "SS" in DEFAULT_TEAM_NAMES
        assert "WO" in DEFAULT_TEAM_NAMES
        assert "HE" in DEFAULT_TEAM_NAMES
        assert "SK" in DEFAULT_TEAM_NAMES
        assert "HT" in DEFAULT_TEAM_NAMES
        assert "LG" in DEFAULT_TEAM_NAMES
        assert "KT" in DEFAULT_TEAM_NAMES
        assert "NC" in DEFAULT_TEAM_NAMES
