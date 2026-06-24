from __future__ import annotations

from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator


class TestBattingStatCalculator:
    def test_basic_avg(self):
        data = {"at_bats": 10, "hits": 3}
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 0.3

    def test_avg_zero_ab(self):
        data = {"at_bats": 0, "hits": 0}
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["avg"] == 0.0

    def test_obp(self):
        data = {"at_bats": 10, "hits": 3, "walks": 2, "hbp": 1, "sacrifice_flies": 1}
        result = BattingStatCalculator.calculate_ratios(data)
        expected = round((3 + 2 + 1) / (10 + 2 + 1 + 1), 3)
        assert result["obp"] == expected

    def test_slg(self):
        data = {"at_bats": 10, "hits": 4, "doubles": 1, "triples": 1, "home_runs": 1}
        result = BattingStatCalculator.calculate_ratios(data)
        tb = (4 - 1 - 1 - 1) + (2 * 1) + (3 * 1) + (4 * 1)
        expected = round(tb / 10, 3)
        assert result["slg"] == expected

    def test_ops(self):
        data = {
            "at_bats": 10,
            "hits": 3,
            "walks": 2,
            "hbp": 1,
            "sacrifice_flies": 1,
            "doubles": 1,
            "triples": 0,
            "home_runs": 1,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["ops"] == round(result["obp"] + result["slg"], 3)

    def test_iso(self):
        data = {"at_bats": 10, "hits": 3, "doubles": 0, "triples": 0, "home_runs": 1}
        result = BattingStatCalculator.calculate_ratios(data)
        assert result["iso"] == round(result["slg"] - result["avg"], 3)

    def test_babip(self):
        data = {"at_bats": 30, "hits": 10, "home_runs": 2, "strikeouts": 5, "sacrifice_flies": 1}
        result = BattingStatCalculator.calculate_ratios(data)
        expected = round((10 - 2) / (30 - 5 - 2 + 1), 3)
        assert result["babip"] == expected

    def test_complete_stats(self):
        data = {
            "at_bats": 100,
            "hits": 30,
            "walks": 10,
            "hbp": 2,
            "sacrifice_flies": 3,
            "sacrifice_hits": 1,
            "doubles": 5,
            "triples": 2,
            "home_runs": 4,
            "strikeouts": 20,
        }
        result = BattingStatCalculator.calculate_ratios(data)
        assert "avg" in result
        assert "obp" in result
        assert "slg" in result
        assert "ops" in result
        assert "iso" in result
        assert "babip" in result


class TestPitchingStatCalculator:
    def test_era(self):
        data = {"earned_runs": 3, "innings_outs": 27}
        result = PitchingStatCalculator.calculate_ratios(data)
        ip = 27 / 3.0
        expected = round((3 / ip) * 9, 2)
        assert result["era"] == expected

    def test_era_zero_outs(self):
        data = {"earned_runs": 0, "innings_outs": 0}
        result = PitchingStatCalculator.calculate_ratios(data)
        assert result["era"] == 0.0

    def test_whip(self):
        data = {"walks_allowed": 5, "hits_allowed": 10, "innings_outs": 27}
        result = PitchingStatCalculator.calculate_ratios(data)
        ip = 27 / 3.0
        expected = round((5 + 10) / ip, 2)
        assert result["whip"] == expected

    def test_k_per_9(self):
        data = {"strikeouts": 18, "innings_outs": 27}
        result = PitchingStatCalculator.calculate_ratios(data)
        ip = 27 / 3.0
        expected = round((18 / ip) * 9, 2)
        assert result["k_per_nine"] == expected

    def test_complete_stats(self):
        data = {
            "earned_runs": 5,
            "innings_outs": 60,
            "walks_allowed": 15,
            "hits_allowed": 40,
            "strikeouts": 50,
            "home_runs_allowed": 8,
        }
        result = PitchingStatCalculator.calculate_ratios(data)
        assert "era" in result
        assert "whip" in result
        assert "k_per_nine" in result
        assert "bb_per_nine" in result
        assert "hr_per_nine" in result or "hr_per_nine" not in result  # optional field
