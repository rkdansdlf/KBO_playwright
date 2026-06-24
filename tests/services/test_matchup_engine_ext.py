from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.models.stat_dataclasses import BattingStats
from src.services.matchup_engine import MatchupEngine


class TestCalcRateStats:
    def test_full_stats(self):
        engine = MatchupEngine()
        stats = BattingStats(hits=10, at_bats=30, walks=4, hbp=1, sf=0, strikeouts=0, doubles=2, triples=1, home_runs=1)
        avg, obp, slg, ops = engine._calc_rate_stats(stats, pa=35)
        assert avg == 0.333  # 10/30
        assert obp > 0
        assert slg > 0
        assert ops > 0

    def test_zero_ab(self):
        engine = MatchupEngine()
        stats = BattingStats(hits=0, at_bats=0, walks=0, hbp=0, sf=0, strikeouts=0, doubles=0, triples=0, home_runs=0)
        avg, obp, slg, ops = engine._calc_rate_stats(stats, pa=0)
        assert avg == 0.0
        assert ops == 0.0

    def test_no_hits(self):
        engine = MatchupEngine()
        stats = BattingStats(hits=0, at_bats=10, walks=0, hbp=0, sf=0, strikeouts=0, doubles=0, triples=0, home_runs=0)
        avg, obp, slg, ops = engine._calc_rate_stats(stats, pa=10)
        assert avg == 0.0

    def test_is_full_false_skips_slg(self):
        engine = MatchupEngine()
        stats = BattingStats(hits=5, at_bats=20, walks=2, hbp=0, sf=0, strikeouts=0, doubles=0, triples=0, home_runs=0)
        avg, obp, slg, ops = engine._calc_rate_stats(stats, pa=22, is_full=False)
        assert avg == 0.25
        assert slg == 0.0


class TestExecuteAll:
    def test_execute_all_commits_on_success(self):
        session = MagicMock()
        engine = MatchupEngine(session=session)
        with patch.multiple(
            engine,
            _calc_batter_team_splits=MagicMock(),
            _calc_pitcher_team_splits=MagicMock(),
            _calc_batter_stadium_splits=MagicMock(),
            _calc_batter_vs_starter=MagicMock(),
            _calc_precise_bvp=MagicMock(),
            _calc_situational_splits=MagicMock(),
        ):
            engine.execute_all(2024)
            session.commit.assert_called_once()

    def test_execute_all_rollback_on_error(self):
        session = MagicMock()
        engine = MatchupEngine(session=session)
        with patch.object(engine, "_calc_batter_team_splits", side_effect=ValueError("fail")):
            with pytest.raises(ValueError):
                engine.execute_all(2024)
            session.rollback.assert_called_once()

    def test_execute_all_closes_session_when_owned(self):
        session = MagicMock()
        engine = MatchupEngine(session=None)
        with patch("src.services.matchup_engine.SessionLocal", return_value=session):
            with patch.multiple(
                engine,
                _calc_batter_team_splits=MagicMock(),
                _calc_pitcher_team_splits=MagicMock(),
                _calc_batter_stadium_splits=MagicMock(),
                _calc_batter_vs_starter=MagicMock(),
                _calc_precise_bvp=MagicMock(),
                _calc_situational_splits=MagicMock(),
            ):
                engine.execute_all(2024)
            session.close.assert_called_once()


class TestCalcBatterTeamSplits:
    def test_deletes_and_adds_splits(self):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        engine = MatchupEngine(session=session)
        engine._calc_batter_team_splits(session, 2024)
        session.query.return_value.filter.return_value.delete.assert_called_once()

    def test_processes_rows(self):
        session = MagicMock()
        mock_row = MagicMock()
        mock_row.player_id = 1
        mock_row.player_name = "Kim"
        mock_row.team_code = "LG"
        mock_row.opponent_team_code = "SS"
        mock_row.games = 5
        mock_row.plate_appearances = 20
        mock_row.at_bats = 18
        mock_row.runs = 3
        mock_row.hits = 6
        mock_row.doubles = 1
        mock_row.triples = 0
        mock_row.home_runs = 1
        mock_row.rbi = 4
        mock_row.walks = 2
        mock_row.intentional_walks = 0
        mock_row.hbp = 0
        mock_row.strikeouts = 3
        mock_row.stolen_bases = 0
        mock_row.caught_stealing = 0
        mock_row.gdp = 0
        session.execute.return_value.fetchall.return_value = [mock_row]
        engine = MatchupEngine(session=session)
        engine._calc_batter_team_splits(session, 2024)
        session.add_all.assert_called_once()


class TestCalcPitcherTeamSplits:
    def test_deletes_and_adds_splits(self):
        session = MagicMock()
        session.execute.return_value.fetchall.return_value = []
        engine = MatchupEngine(session=session)
        engine._calc_pitcher_team_splits(session, 2024)
        session.query.return_value.filter.return_value.delete.assert_called_once()


class TestCalcPreciseBvp:
    def test_processes_events(self):
        session = MagicMock()
        mock_event = MagicMock()
        mock_event.batter_id = 1
        mock_event.pitcher_id = 2
        mock_event.batter_name = "Kim"
        mock_event.pitcher_name = "Park"
        mock_event.description = "안타"
        mock_event.rbi = 1
        q = session.query.return_value
        q.join.return_value.filter.return_value.filter.return_value.filter.return_value.all.return_value = [mock_event]
        session.query.return_value.filter_by.return_value.first.return_value = None
        engine = MatchupEngine(session=session)
        engine._calc_precise_bvp(session, 2024)
        session.add.assert_called_once()
