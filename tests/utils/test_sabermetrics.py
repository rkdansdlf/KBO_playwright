from __future__ import annotations

from src.utils.sabermetrics import PitchingStats, calculate_era, calculate_fip


class TestCalculateFip:
    def test_basic(self):
        stats = PitchingStats(home_runs=1, walks=2, hit_batters=1, strikeouts=5)
        result = calculate_fip(stats, 6.0, 3.10)
        expected = round((13 * 1 + 3 * (2 + 1) - 2 * 5) / 6.0 + 3.10, 2)
        assert result == expected

    def test_zero_ip_returns_zero(self):
        stats = PitchingStats(home_runs=5, walks=5, hit_batters=5, strikeouts=10)
        assert calculate_fip(stats, 0.0, 3.10) == 0.0

    def test_negative_ip_returns_zero(self):
        stats = PitchingStats()
        assert calculate_fip(stats, -1.0, 3.10) == 0.0

    def test_all_zero_stats(self):
        stats = PitchingStats()
        result = calculate_fip(stats, 9.0, 3.10)
        assert result == round(3.10, 2)

    def test_high_strikeouts(self):
        stats = PitchingStats(home_runs=0, walks=0, hit_batters=0, strikeouts=20)
        result = calculate_fip(stats, 7.0, 3.10)
        expected = round((0 + 0 - 40) / 7.0 + 3.10, 2)
        assert result == expected

    def test_high_home_runs(self):
        stats = PitchingStats(home_runs=10, walks=0, hit_batters=0, strikeouts=0)
        result = calculate_fip(stats, 5.0, 3.10)
        expected = round((130 + 0 - 0) / 5.0 + 3.10, 2)
        assert result == expected

    def test_fractional_ip(self):
        stats = PitchingStats(home_runs=1, walks=1, hit_batters=0, strikeouts=3)
        result = calculate_fip(stats, 5.2, 3.10)
        expected = round((13 + 3 - 6) / 5.2 + 3.10, 2)
        assert result == expected

    def test_rounding(self):
        stats = PitchingStats(home_runs=1, walks=1, hit_batters=0, strikeouts=3)
        result = calculate_fip(stats, 7.0, 3.10)
        expected = round((13 + 3 - 6) / 7.0 + 3.10, 2)
        assert result == expected
        assert isinstance(result, float)


class TestCalculateEra:
    def test_basic(self):
        assert calculate_era(3, 6.0) == round((3 / 6.0) * 9, 2)

    def test_zero_ip_returns_zero(self):
        assert calculate_era(5, 0.0) == 0.0

    def test_negative_ip_returns_zero(self):
        assert calculate_era(5, -1.0) == 0.0

    def test_zero_earned_runs(self):
        assert calculate_era(0, 9.0) == 0.0

    def test_perfect_game(self):
        assert calculate_era(0, 9.0) == 0.0

    def test_high_era(self):
        result = calculate_era(10, 5.0)
        assert result == round((10 / 5.0) * 9, 2)

    def test_nine_innings(self):
        assert calculate_era(4, 9.0) == round((4 / 9.0) * 9, 2)

    def test_fractional_ip(self):
        result = calculate_era(2, 5.1)
        assert result == round((2 / 5.1) * 9, 2)

    def test_rounding(self):
        result = calculate_era(1, 7.0)
        expected = round((1 / 7.0) * 9, 2)
        assert result == expected
        assert isinstance(result, float)


class TestPitchingStats:
    def test_default_values(self):
        stats = PitchingStats()
        assert stats.home_runs == 0
        assert stats.walks == 0
        assert stats.hit_batters == 0
        assert stats.strikeouts == 0

    def test_custom_values(self):
        stats = PitchingStats(home_runs=5, walks=3, hit_batters=2, strikeouts=10)
        assert stats.home_runs == 5
        assert stats.walks == 3
        assert stats.hit_batters == 2
        assert stats.strikeouts == 10
