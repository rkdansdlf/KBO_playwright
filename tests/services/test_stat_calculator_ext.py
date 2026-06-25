from __future__ import annotations

from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator


class TestBattingStatCalculator:
    def test_all_zeros(self):
        data = dict.fromkeys(
            (
                "at_bats",
                "hits",
                "walks",
                "hbp",
                "sacrifice_flies",
                "sacrifice_hits",
                "doubles",
                "triples",
                "home_runs",
                "strikeouts",
                "plate_appearances",
            ),
            0,
        )
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 0.0
        assert result["obp"] == 0.0
        assert result["slg"] == 0.0
        assert result["ops"] == 0.0

    def test_perfect_batting(self):
        data = {
            "at_bats": 10,
            "hits": 10,
            "walks": 0,
            "hbp": 0,
            "sacrifice_flies": 0,
            "sacrifice_hits": 0,
            "doubles": 0,
            "triples": 0,
            "home_runs": 10,
            "strikeouts": 0,
            "plate_appearances": 10,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 1.0
        assert result["slg"] == 4.0
        assert result["ops"] > 0

    def test_typical_line(self):
        data = {
            "at_bats": 100,
            "hits": 30,
            "walks": 10,
            "hbp": 2,
            "sacrifice_flies": 1,
            "sacrifice_hits": 3,
            "doubles": 5,
            "triples": 1,
            "home_runs": 3,
            "strikeouts": 15,
            "plate_appearances": 113,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 0.3
        assert result["obp"] > 0
        assert result["slg"] > 0

    def test_babip_calculation(self):
        data = {
            "at_bats": 100,
            "hits": 30,
            "walks": 5,
            "hbp": 1,
            "sacrifice_flies": 2,
            "sacrifice_hits": 0,
            "doubles": 5,
            "triples": 1,
            "home_runs": 4,
            "strikeouts": 20,
            "plate_appearances": 108,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["babip"] > 0
        assert result["iso"] > 0

    def test_xr_calculation(self):
        data = {
            "at_bats": 100,
            "hits": 30,
            "walks": 10,
            "hbp": 2,
            "sacrifice_flies": 1,
            "sacrifice_hits": 3,
            "doubles": 5,
            "triples": 1,
            "home_runs": 3,
            "strikeouts": 15,
            "plate_appearances": 113,
            "intentional_walks": 0,
            "stolen_bases": 2,
            "caught_stealing": 1,
            "gdp": 2,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert "xr" in result
        assert result["xr"] != 0.0

    def test_no_at_bats(self):
        data = {
            "at_bats": 0,
            "hits": 0,
            "walks": 0,
            "hbp": 0,
            "sacrifice_flies": 0,
            "sacrifice_hits": 0,
            "doubles": 0,
            "triples": 0,
            "home_runs": 0,
            "strikeouts": 0,
            "plate_appearances": 0,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 0.0
        assert result["slg"] == 0.0


class TestPitchingStatCalculator:
    def test_all_zeros(self):
        data = dict.fromkeys(
            (
                "innings_outs",
                "earned_runs",
                "hits_allowed",
                "walks_allowed",
                "strikeouts",
                "home_runs_allowed",
                "hit_batters",
                "batters_faced",
            ),
            0,
        )
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0
        assert result["whip"] == 0.0

    def test_perfect_game(self):
        data = {
            "innings_outs": 27,
            "earned_runs": 0,
            "hits_allowed": 0,
            "walks_allowed": 0,
            "strikeouts": 10,
            "home_runs_allowed": 0,
            "hit_batters": 0,
            "batters_faced": 27,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0
        assert result["whip"] == 0.0
        assert result["k_per_nine"] > 0

    def test_typical_line(self):
        data = {
            "innings_outs": 54,
            "earned_runs": 6,
            "hits_allowed": 15,
            "walks_allowed": 5,
            "strikeouts": 20,
            "home_runs_allowed": 2,
            "hit_batters": 1,
            "batters_faced": 75,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 3.0  # 6 ER / 18 IP * 9 = 3.0
        assert result["whip"] > 0
        assert result["fip"] > 0

    def test_kbb_ratio(self):
        data = {
            "innings_outs": 54,
            "earned_runs": 6,
            "hits_allowed": 10,
            "walks_allowed": 5,
            "strikeouts": 25,
            "home_runs_allowed": 1,
            "hit_batters": 0,
            "batters_faced": 70,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["kbb"] == 5.0

    def test_bb_is_zero_kbb_uses_so(self):
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
        result = PitchingStatCalculator.calculate_ratios(data, fip_constant=3.20)
        assert result["fip"] != PitchingStatCalculator.calculate_ratios(data)["fip"]
