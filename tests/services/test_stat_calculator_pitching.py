from __future__ import annotations

from src.services.stat_calculator import PitchingStatCalculator


class TestPitchingStatCalculatorEdgeCases:
    def test_zero_innings_all_zero(self):
        data = {
            "innings_outs": 0,
            "earned_runs": 5,
            "hits_allowed": 10,
            "walks_allowed": 5,
            "strikeouts": 0,
            "home_runs_allowed": 3,
            "hit_batters": 2,
            "batters_faced": 0,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0
        assert result["whip"] == 0.0
        assert result["k_per_nine"] == 0.0
        assert result["bb_per_nine"] == 0.0
        assert result["fip"] == 0.0

    def test_zero_walks_kbb_uses_so(self):
        data = {
            "innings_outs": 27,
            "earned_runs": 3,
            "hits_allowed": 5,
            "walks_allowed": 0,
            "strikeouts": 5,
            "home_runs_allowed": 1,
            "hit_batters": 0,
            "batters_faced": 30,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["kbb"] == 5.0

    def test_zero_walks_zero_so_kbb_zero(self):
        data = {
            "innings_outs": 27,
            "earned_runs": 3,
            "hits_allowed": 5,
            "walks_allowed": 0,
            "strikeouts": 0,
            "home_runs_allowed": 1,
            "hit_batters": 0,
            "batters_faced": 30,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["kbb"] == 0.0

    def test_perfect_era(self):
        data = {
            "innings_outs": 162,
            "earned_runs": 0,
            "hits_allowed": 0,
            "walks_allowed": 0,
            "strikeouts": 54,
            "home_runs_allowed": 0,
            "hit_batters": 0,
            "batters_faced": 72,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0
        assert result["whip"] == 0.0
        assert result["k_per_nine"] == 9.0

    def test_high_era(self):
        data = {
            "innings_outs": 9,
            "earned_runs": 15,
            "hits_allowed": 20,
            "walks_allowed": 10,
            "strikeouts": 5,
            "home_runs_allowed": 5,
            "hit_batters": 2,
            "batters_faced": 40,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] > 10.0

    def test_custom_fip_constant(self):
        data = {
            "innings_outs": 54,
            "earned_runs": 6,
            "hits_allowed": 10,
            "walks_allowed": 5,
            "strikeouts": 20,
            "home_runs_allowed": 2,
            "hit_batters": 1,
            "batters_faced": 70,
        }
        default_fip = PitchingStatCalculator.calculate_ratios(data)["fip"]
        custom_fip = PitchingStatCalculator.calculate_ratios(data, fip_constant=2.50)["fip"]
        assert custom_fip < default_fip

    def test_fip_with_hbp(self):
        data = {
            "innings_outs": 54,
            "earned_runs": 6,
            "hits_allowed": 10,
            "walks_allowed": 3,
            "strikeouts": 20,
            "home_runs_allowed": 2,
            "hit_batters": 5,
            "batters_faced": 70,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["fip"] > 0

    def test_default_fip_constant(self):
        assert PitchingStatCalculator.FIP_CONSTANT == 3.10

    def test_missing_keys_default_to_zero(self):
        data = {}
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0
        assert result["whip"] == 0.0
        assert result["k_per_nine"] == 0.0
        assert result["bb_per_nine"] == 0.0
        assert result["kbb"] == 0.0
        assert result["fip"] == 0.0

    def test_none_values_treated_as_zero(self):
        data = {
            "innings_outs": None,
            "earned_runs": None,
            "hits_allowed": None,
            "walks_allowed": None,
            "strikeouts": None,
            "home_runs_allowed": None,
            "hit_batters": None,
            "batters_faced": None,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0

    def test_full_inning_outs(self):
        data = {
            "innings_outs": 9,
            "earned_runs": 1,
            "hits_allowed": 2,
            "walks_allowed": 1,
            "strikeouts": 2,
            "home_runs_allowed": 0,
            "hit_batters": 0,
            "batters_faced": 10,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 3.0
        assert result["whip"] == 1.0
        assert result["k_per_nine"] == 6.0
        assert result["bb_per_nine"] == 3.0

    def test_partial_inning(self):
        data = {
            "innings_outs": 1,
            "earned_runs": 1,
            "hits_allowed": 1,
            "walks_allowed": 0,
            "strikeouts": 0,
            "home_runs_allowed": 0,
            "hit_batters": 0,
            "batters_faced": 4,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 27.0
