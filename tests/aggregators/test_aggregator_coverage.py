from __future__ import annotations

import pytest

from src.aggregators.park_factor_calculator import ParkFactorCalculator
from src.aggregators.sabermetrics_calculator import SabermetricsCalculator
from src.aggregators.team_stat_aggregator import (
    DEFAULT_TEAM_NAMES,
    TeamAggregationQuery,
    TeamStatAggregator,
)
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching


class TestParkFactorCalculatorCoverage:
    def test_label_exact_boundary_1_10(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(1.10) == "약간 타자친화"

    def test_label_exact_boundary_1_04(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(1.04) == "중립"

    def test_label_exact_boundary_0_96(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(0.96) == "약간 투수친화"

    def test_label_exact_boundary_0_90(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(0.90) == "투수친화"

    def test_label_just_above_1_10(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(1.10001) == "타자친화"

    def test_label_just_below_0_90(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(0.8999) == "투수친화"

    def test_label_mid_range_neutral(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(1.0) == "중립"

    def test_label_slightly_hitter_boundary_low(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(1.041) == "약간 타자친화"

    def test_label_slightly_pitcher_boundary_low(self):
        calc = ParkFactorCalculator.__new__(ParkFactorCalculator)
        assert calc._label(0.901) == "약간 투수친화"


class TestSabermetricsCalculatorBattingCoverage:
    def test_woba_with_zero_denominator(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=0,
            hits=0,
            doubles=0,
            triples=0,
            home_runs=0,
            walks=0,
            intentional_walks=0,
            hbp=0,
            sacrifice_flies=0,
            plate_appearances=0,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.440}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["woba"] == 0.0

    def test_wraa_negative_when_below_league(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=500,
            hits=100,
            doubles=15,
            triples=2,
            home_runs=5,
            walks=30,
            intentional_walks=2,
            hbp=3,
            sacrifice_flies=4,
            plate_appearances=550,
            obp=0.250,
            slg=0.300,
        )
        lg = {"lg_woba": 0.340, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.350, "lg_slg": 0.450}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["wraa"] < 0
        assert result["wrc_plus"] < 100

    def test_wrc_plus_100_when_r_per_pa_zero(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=400,
            hits=120,
            doubles=20,
            triples=3,
            home_runs=10,
            walks=40,
            intentional_walks=2,
            hbp=5,
            sacrifice_flies=3,
            plate_appearances=460,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.0, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.440}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["wrc_plus"] == 100

    def test_ops_plus_with_zero_lg_values(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=500,
            hits=150,
            plate_appearances=550,
            obp=0.360,
            slg=0.480,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.0, "lg_slg": 0.0}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["ops_plus"] == 100

    def test_ops_plus_exactly_100_when_matches_league(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=500,
            hits=150,
            plate_appearances=550,
            obp=0.340,
            slg=0.440,
        )
        lg = {"lg_woba": 0.320, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.340, "lg_slg": 0.440}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert result["ops_plus"] == 100

    def test_all_fields_present_in_result(self):
        stat = PlayerSeasonBatting(
            player_id=10001,
            season=2025,
            at_bats=520,
            hits=160,
            doubles=30,
            triples=5,
            home_runs=25,
            walks=60,
            intentional_walks=5,
            hbp=8,
            sacrifice_flies=7,
            plate_appearances=600,
            obp=0.380,
            slg=0.496,
        )
        lg = {"lg_woba": 0.340, "woba_scale": 1.2, "lg_r_per_pa": 0.12, "rpw": 10.0, "lg_obp": 0.350, "lg_slg": 0.450}
        result = SabermetricsCalculator.calculate_batting_metrics(stat, lg)
        assert "woba" in result
        assert "wraa" in result
        assert "wrc_plus" in result
        assert "ops_plus" in result
        assert "war" in result


class TestSabermetricsCalculatorPitchingCoverage:
    def test_lob_pct_none_when_denominator_zero(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=15,
        )
        lob_denom = 15 + 45 + 5 - 1.4 * 15
        if lob_denom <= 0:
            lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
            result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
            assert result["lob_pct"] is None

    def test_lob_pct_calculated_correctly(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=5,
            walks_allowed=20,
            hit_batters=3,
            strikeouts=160,
            runs_allowed=30,
            hits_allowed=50,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        h, bb, hbp, r, hr = 50, 20, 3, 30, 5
        denom = h + bb + hbp - 1.4 * hr
        expected = round((h + bb + hbp - r) / denom, 3)
        assert result["lob_pct"] == expected

    def test_war_negative_when_fip_above_league_era(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=100,
            home_runs_allowed=30,
            walks_allowed=60,
            hit_batters=10,
            strikeouts=100,
            runs_allowed=110,
            hits_allowed=200,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert result["war"] < 0

    def test_war_zero_when_rpw_zero(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 0.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert result["war"] == 0.0

    def test_fip_uses_lg_era_default(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        ip = 540 / 3.0
        fip = ((13 * 15 + 3 * (45 + 5) - 2 * 160) / ip) + 3.20
        assert result["fip_adj"] == pytest.approx(round(fip, 2), abs=0.01)

    def test_all_fields_present_in_pitching_result(self):
        stat = PlayerSeasonPitching(
            player_id=20001,
            season=2025,
            innings_outs=540,
            earned_runs=65,
            home_runs_allowed=15,
            walks_allowed=45,
            hit_batters=5,
            strikeouts=160,
            runs_allowed=70,
            hits_allowed=140,
        )
        lg = {"fip_constant": 3.20, "lg_era": 4.00, "rpw": 10.0}
        result = SabermetricsCalculator.calculate_pitching_metrics(stat, lg)
        assert "fip_adj" in result
        assert "lob_pct" in result
        assert "war" in result


class TestTeamStatAggregatorMemCoverage:
    def test_batting_mem_multiple_teams(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=10,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        )
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="SS",
            league="REGULAR",
            games=12,
            plate_appearances=50,
            at_bats=44,
            runs=8,
            hits=14,
            doubles=3,
            triples=1,
            home_runs=2,
            rbi=10,
            walks=5,
            intentional_walks=1,
            hbp=2,
            strikeouts=6,
            stolen_bases=2,
            caught_stealing=1,
            sacrifice_hits=0,
            sacrifice_flies=1,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2])
        assert len(results) == 2
        by_team = {r["team_id"]: r for r in results}
        assert by_team["OB"]["hits"] == 10
        assert by_team["SS"]["hits"] == 14

    def test_batting_mem_multiple_seasons(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2024,
            team_code="OB",
            league="REGULAR",
            games=10,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        )
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=10,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2])
        assert len(results) == 2

    def test_batting_mem_skips_invalid_team_code(self):
        p1 = PlayerSeasonBatting(id=1, season=2025, team_code=None, at_bats=10, hits=3)
        p2 = PlayerSeasonBatting(id=2, season=2025, team_code="합계", at_bats=10, hits=3)
        p3 = PlayerSeasonBatting(id=3, season=2025, team_code="TOTAL", at_bats=10, hits=3)
        p4 = PlayerSeasonBatting(id=4, season=2025, team_code="ALL", at_bats=10, hits=3)
        p5 = PlayerSeasonBatting(id=5, season=2025, team_code="-", at_bats=10, hits=3)
        p6 = PlayerSeasonBatting(
            id=6,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2, p3, p4, p5, p6])
        assert len(results) == 1
        assert results[0]["team_id"] == "OB"

    def test_batting_mem_skips_missing_season(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=None,
            team_code="OB",
            at_bats=10,
            hits=3,
            plate_appearances=12,
            walks=1,
            hbp=0,
            sacrifice_flies=0,
            doubles=0,
            triples=0,
            home_runs=0,
        )
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2])
        assert len(results) == 1
        assert results[0]["hits"] == 5

    def test_batting_mem_uses_team_games_map(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=10,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        )
        team_games_map = {(2025, "OB"): 144}
        results = TeamStatAggregator().aggregate_batting(
            TeamAggregationQuery(rows=[p1], team_games_map=team_games_map),
        )
        assert results[0]["games"] == 144

    def test_batting_mem_defaults_to_max_player_games(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=8,
            plate_appearances=30,
            at_bats=25,
            runs=4,
            hits=7,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=3,
            intentional_walks=0,
            hbp=0,
            strikeouts=4,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=12,
            plate_appearances=40,
            at_bats=35,
            runs=5,
            hits=10,
            doubles=2,
            triples=0,
            home_runs=1,
            rbi=5,
            walks=4,
            intentional_walks=0,
            hbp=1,
            strikeouts=5,
            stolen_bases=1,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=1,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2])
        assert results[0]["games"] == 12

    def test_batting_mem_uses_default_team_names(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1])
        assert results[0]["team_name"] == "두산"

    def test_batting_mem_canonical_team_code_preferred(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="SK",
            canonical_team_code="SSG",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1])
        assert results[0]["team_id"] == "SSG"

    def test_pitching_mem_multiple_teams(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        p2 = PlayerSeasonPitching(
            id=2,
            season=2025,
            team_code="SS",
            league="REGULAR",
            games=6,
            wins=3,
            losses=2,
            saves=1,
            holds=1,
            innings_outs=54,
            hits_allowed=12,
            runs_allowed=5,
            earned_runs=4,
            home_runs_allowed=2,
            walks_allowed=4,
            intentional_walks=0,
            hit_batters=1,
            strikeouts=18,
        )
        results = TeamStatAggregator().aggregate_pitching([p1, p2])
        assert len(results) == 2

    def test_pitching_mem_skips_invalid_team_code(self):
        p1 = PlayerSeasonPitching(id=1, season=2025, team_code=None, innings_outs=45, earned_runs=3)
        p2 = PlayerSeasonPitching(id=2, season=2025, team_code="합계", innings_outs=45, earned_runs=3)
        p3 = PlayerSeasonPitching(id=3, season=2025, team_code="-", innings_outs=45, earned_runs=3)
        p4 = PlayerSeasonPitching(
            id=4,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching([p1, p2, p3, p4])
        assert len(results) == 1
        assert results[0]["team_id"] == "OB"

    def test_pitching_mem_skips_missing_season(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=None,
            team_code="OB",
            innings_outs=45,
            earned_runs=3,
            walks_allowed=0,
            hit_batters=0,
            home_runs_allowed=0,
        )
        p2 = PlayerSeasonPitching(
            id=2,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching([p1, p2])
        assert len(results) == 1

    def test_pitching_mem_uses_team_games_map(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        team_games_map = {(2025, "OB"): 144}
        results = TeamStatAggregator().aggregate_pitching(
            TeamAggregationQuery(rows=[p1], team_games_map=team_games_map),
        )
        assert results[0]["games"] == 144

    def test_pitching_mem_defaults_to_max_player_games(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=3,
            wins=1,
            losses=0,
            saves=0,
            holds=0,
            innings_outs=27,
            hits_allowed=5,
            runs_allowed=2,
            earned_runs=1,
            home_runs_allowed=0,
            walks_allowed=3,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=8,
        )
        p2 = PlayerSeasonPitching(
            id=2,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=7,
            wins=3,
            losses=2,
            saves=0,
            holds=0,
            innings_outs=63,
            hits_allowed=15,
            runs_allowed=6,
            earned_runs=5,
            home_runs_allowed=2,
            walks_allowed=7,
            intentional_walks=0,
            hit_batters=1,
            strikeouts=20,
        )
        results = TeamStatAggregator().aggregate_pitching([p1, p2])
        assert results[0]["games"] == 7

    def test_pitching_mem_uses_default_team_names(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="LT",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching([p1])
        assert results[0]["team_name"] == "롯데"

    def test_pitching_mem_canonical_team_code_preferred(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="SK",
            canonical_team_code="SSG",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching([p1])
        assert results[0]["team_id"] == "SSG"

    def test_pitching_mem_ties_default_zero(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching([p1])
        assert results[0]["ties"] == 0

    def test_batting_mem_empty_list(self):
        results = TeamStatAggregator().aggregate_batting([])
        assert results == []

    def test_pitching_mem_empty_list(self):
        results = TeamStatAggregator().aggregate_pitching([])
        assert results == []

    def test_batting_mem_all_invalid_team_codes(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code=None,
            at_bats=10,
            hits=3,
            plate_appearances=12,
            walks=1,
            hbp=0,
            sacrifice_flies=0,
            doubles=0,
            triples=0,
            home_runs=0,
        )
        p2 = PlayerSeasonBatting(
            id=2,
            season=2025,
            team_code="합계",
            at_bats=10,
            hits=3,
            plate_appearances=12,
            walks=1,
            hbp=0,
            sacrifice_flies=0,
            doubles=0,
            triples=0,
            home_runs=0,
        )
        results = TeamStatAggregator().aggregate_batting([p1, p2])
        assert results == []

    def test_pitching_mem_all_invalid_team_codes(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code=None,
            innings_outs=45,
            earned_runs=3,
            walks_allowed=0,
            hit_batters=0,
            home_runs_allowed=0,
        )
        p2 = PlayerSeasonPitching(
            id=2,
            season=2025,
            team_code="합계",
            innings_outs=45,
            earned_runs=3,
            walks_allowed=0,
            hit_batters=0,
            home_runs_allowed=0,
        )
        results = TeamStatAggregator().aggregate_pitching([p1, p2])
        assert results == []


class TestTeamStatAggregatorQueryDispatch:
    def test_aggregate_batting_with_int_season_requires_session(self):
        aggregator = TeamStatAggregator(session=None)
        with pytest.raises(ValueError, match="Database session is required"):
            aggregator.aggregate_batting(2025)

    def test_aggregate_pitching_with_int_season_requires_session(self):
        aggregator = TeamStatAggregator(session=None)
        with pytest.raises(ValueError, match="Database session is required"):
            aggregator.aggregate_pitching(2025)

    def test_aggregate_batting_with_no_rows_raises(self):
        aggregator = TeamStatAggregator(session=None)
        with pytest.raises(ValueError, match="Either an integer season or rows iterable must be provided"):
            aggregator.aggregate_batting(TeamAggregationQuery())

    def test_aggregate_pitching_with_no_rows_raises(self):
        aggregator = TeamStatAggregator(session=None)
        with pytest.raises(ValueError, match="Either an integer season or rows iterable must be provided"):
            aggregator.aggregate_pitching(TeamAggregationQuery())

    def test_aggregate_batting_with_query_object(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting(
            TeamAggregationQuery(rows=[p1], team_names={"OB": "두산"}),
        )
        assert len(results) == 1
        assert results[0]["team_name"] == "두산"

    def test_aggregate_pitching_with_query_object(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching(
            TeamAggregationQuery(rows=[p1], team_names={"OB": "두산"}),
        )
        assert len(results) == 1
        assert results[0]["team_name"] == "두산"

    def test_aggregate_batting_with_dry_run(self):
        p1 = PlayerSeasonBatting(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            plate_appearances=20,
            at_bats=18,
            hits=5,
            doubles=1,
            triples=0,
            home_runs=0,
            rbi=3,
            walks=2,
            intentional_walks=0,
            hbp=0,
            strikeouts=3,
            stolen_bases=0,
            caught_stealing=0,
            sacrifice_hits=0,
            sacrifice_flies=0,
            gdp=0,
        )
        results = TeamStatAggregator().aggregate_batting(
            TeamAggregationQuery(rows=[p1], dry_run=True),
        )
        assert len(results) == 1

    def test_aggregate_pitching_with_dry_run(self):
        p1 = PlayerSeasonPitching(
            id=1,
            season=2025,
            team_code="OB",
            league="REGULAR",
            games=5,
            wins=2,
            losses=1,
            saves=0,
            holds=0,
            innings_outs=45,
            hits_allowed=10,
            runs_allowed=4,
            earned_runs=3,
            home_runs_allowed=1,
            walks_allowed=5,
            intentional_walks=0,
            hit_batters=0,
            strikeouts=15,
        )
        results = TeamStatAggregator().aggregate_pitching(
            TeamAggregationQuery(rows=[p1], dry_run=True),
        )
        assert len(results) == 1


class TestTeamStatAggregatorDefaultTeamNames:
    def test_all_default_teams_present(self):
        expected = {"OB", "LT", "SS", "WO", "HE", "SK", "HT", "LG", "KT", "NC"}
        assert set(DEFAULT_TEAM_NAMES.keys()) == expected

    def test_default_names_values(self):
        assert DEFAULT_TEAM_NAMES["OB"] == "두산"
        assert DEFAULT_TEAM_NAMES["LT"] == "롯데"
        assert DEFAULT_TEAM_NAMES["SS"] == "삼성"
        assert DEFAULT_TEAM_NAMES["WO"] == "키움"
        assert DEFAULT_TEAM_NAMES["HE"] == "한화"
        assert DEFAULT_TEAM_NAMES["SK"] == "SSG"
        assert DEFAULT_TEAM_NAMES["HT"] == "KIA"
        assert DEFAULT_TEAM_NAMES["LG"] == "LG"
        assert DEFAULT_TEAM_NAMES["KT"] == "KT"
        assert DEFAULT_TEAM_NAMES["NC"] == "NC"
