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
        assert avg == pytest.approx(0.333, abs=0.001)
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


class TestBvpPureHelpers:
    def test_empty_bvp_stats_uses_event_names(self):
        event = MagicMock(batter_name="Kim", pitcher_name="Park")

        stats = MatchupEngine._empty_bvp_stats(event)

        assert stats["batter_name"] == "Kim"
        assert stats["pitcher_name"] == "Park"
        assert stats["pa"] == 0

    @pytest.mark.parametrize(
        ("description", "expected"),
        [
            ("좌전 안타", {"pa": 1, "ab": 1, "h": 1}),
            ("우중간 2루타", {"d2": 1, "h": 1, "ab": 1}),
            ("좌중간 3루타", {"d3": 1, "h": 1, "ab": 1}),
            ("좌월 홈런", {"hr": 1, "h": 1, "ab": 1}),
            ("볼넷", {"bb": 1, "ab": 0}),
            ("몸에 맞는 사구", {"hbp": 1, "ab": 0}),
            ("희생플라이", {"sf": 1, "ab": 0}),
            ("희생번트", {"ab": 0}),
            ("삼진", {"so": 1, "ab": 1}),
        ],
    )
    def test_apply_bvp_event_classifies_korean_descriptions(self, description, expected):
        stats = {
            "batter_name": "Kim",
            "pitcher_name": "Park",
            "pa": 0,
            "ab": 0,
            "h": 0,
            "d2": 0,
            "d3": 0,
            "hr": 0,
            "bb": 0,
            "hbp": 0,
            "so": 0,
            "sf": 0,
            "rbi": 0,
        }
        event = MagicMock(description=description, rbi=2)

        MatchupEngine._apply_bvp_event(stats, event)

        assert stats["pa"] == 1
        assert stats["rbi"] == 2
        for key, value in expected.items():
            assert stats[key] == value

    def test_build_bvp_map_groups_by_batter_pitcher_pair(self):
        engine = MatchupEngine()
        events = [
            MagicMock(
                batter_id=1,
                pitcher_id=2,
                batter_name="Kim",
                pitcher_name="Park",
                description="안타",
                rbi=1,
            ),
            MagicMock(
                batter_id=1,
                pitcher_id=2,
                batter_name="Kim",
                pitcher_name="Park",
                description="볼넷",
                rbi=0,
            ),
        ]

        result = engine._build_bvp_map(events)

        assert result[(1, 2)]["pa"] == 2
        assert result[(1, 2)]["h"] == 1
        assert result[(1, 2)]["bb"] == 1

    def test_update_existing_bvp_accumulates_and_recalculates_rates(self):
        engine = MatchupEngine()
        existing = MagicMock(
            plate_appearances=3,
            at_bats=3,
            hits=1,
            doubles=0,
            triples=0,
            home_runs=0,
            rbi=0,
            walks=0,
            hbp=0,
            strikeouts=1,
            sacrifice_flies=0,
        )
        stats = {"pa": 2, "ab": 1, "h": 1, "d2": 1, "d3": 0, "hr": 0, "rbi": 2, "bb": 1, "hbp": 0, "so": 0, "sf": 0}

        engine._update_existing_bvp(existing, stats)

        assert existing.plate_appearances == 5
        assert existing.at_bats == 4
        assert existing.hits == 2
        assert existing.walks == 1
        assert existing.avg == 0.5
        assert existing.obp == 0.6
        assert existing.ops > existing.obp


class TestSituationalPureHelpers:
    @pytest.mark.parametrize(
        ("description", "expected"),
        [
            ("안타", {"is_hit": True, "is_hr": False, "is_bb": False, "is_ab": True}),
            ("홈런", {"is_hit": True, "is_hr": True, "is_bb": False, "is_ab": True}),
            ("볼넷", {"is_hit": False, "is_hr": False, "is_bb": True, "is_ab": False}),
            ("희생플라이", {"is_hit": False, "is_hr": False, "is_bb": False, "is_ab": False, "is_sf": True}),
            ("희생번트", {"is_hit": False, "is_hr": False, "is_bb": False, "is_ab": False}),
        ],
    )
    def test_classify_event_flags(self, description, expected):
        result = MatchupEngine._classify_event(description)

        for key, value in expected.items():
            assert result[key] is value
        assert result["is_pa"] is True

    def test_update_batter_split_accumulates_all_flags(self):
        engine = MatchupEngine()
        splits = {}
        event_flags = MatchupEngine._classify_event("홈런")

        engine._update_batter_split(splits, 10, "RISP", event_flags)
        engine._update_batter_split(splits, 10, "RISP", MatchupEngine._classify_event("볼넷"))

        assert splits[10]["RISP"] == {"pa": 2, "ab": 1, "h": 1, "bb": 1, "hbp": 0, "hr": 1, "sf": 0}

    def test_update_pitcher_split_counts_outs_and_strikeouts(self):
        engine = MatchupEngine()
        splits = {}

        engine._update_pitcher_split(splits, 20, "vsL", MatchupEngine._classify_event("삼진"))
        engine._update_pitcher_split(splits, 20, "vsL", MatchupEngine._classify_event("실책"))
        engine._update_pitcher_split(splits, 20, "vsL", MatchupEngine._classify_event("안타"))

        assert splits[20]["vsL"] == {"bf": 3, "h": 1, "hr": 0, "bb": 0, "so": 1, "outs": 1}

    def test_update_situational_maps_builds_risp_and_handedness_splits(self):
        engine = MatchupEngine()
        event = MagicMock(batter_id=10, pitcher_id=20, description="홈런", bases_before="23")
        players = {
            10: MagicMock(bats="L"),
            20: MagicMock(throws="R"),
        }
        bat_splits = {}
        pit_splits = {}

        engine._update_situational_maps(event, players, bat_splits, pit_splits)

        assert bat_splits[10]["RISP"]["hr"] == 1
        assert bat_splits[10]["vsR"]["hr"] == 1
        assert pit_splits[20]["RISP"]["hr"] == 1
        assert pit_splits[20]["vsL"]["hr"] == 1

    def test_update_situational_maps_defaults_unknown_handedness(self):
        engine = MatchupEngine()
        event = MagicMock(batter_id=10, pitcher_id=20, description="볼넷", bases_before="")
        players = {10: MagicMock(bats=None), 20: MagicMock(throws=None)}
        bat_splits = {}
        pit_splits = {}

        engine._update_situational_maps(event, players, bat_splits, pit_splits)

        assert bat_splits[10]["vsR"]["bb"] == 1
        assert pit_splits[20]["vsR"]["bb"] == 1

    def test_insert_situational_splits_adds_batter_and_pitcher_models(self):
        engine = MatchupEngine()
        session = MagicMock()
        bat_splits = {10: {"RISP": {"pa": 2, "ab": 1, "h": 1, "bb": 1, "hbp": 0, "hr": 1, "sf": 0}}}
        pit_splits = {20: {"vsL": {"bf": 4, "h": 1, "hr": 1, "bb": 1, "so": 2, "outs": 3}}}

        engine._insert_batter_situational_splits(session, 2025, bat_splits)
        engine._insert_pitcher_situational_splits(session, 2025, pit_splits)

        batter_split = session.add.call_args_list[0].args[0]
        pitcher_split = session.add.call_args_list[1].args[0]
        assert batter_split.player_id == 10
        assert batter_split.avg == 1.0
        assert pitcher_split.player_id == 20
        assert pitcher_split.avg_against == pytest.approx(0.333)
        assert pitcher_split.whip == 2.0
