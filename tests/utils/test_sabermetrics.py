from __future__ import annotations

from src.utils.sabermetrics import PitchingStats, calculate_era, calculate_fip


class TestCalculateFip:
    def test_basic_fip(self) -> None:
        stats = PitchingStats(home_runs=2, walks=3, hit_batters=1, strikeouts=10)
        result = calculate_fip(stats, ip=7.0, fip_constant=3.10)
        expected = round(((13 * 2) + (3 * (3 + 1)) - (2 * 10)) / 7.0 + 3.10, 2)
        assert result == expected

    def test_zero_ip_returns_zero(self) -> None:
        stats = PitchingStats(home_runs=5, walks=5, hit_batters=2, strikeouts=20)
        assert calculate_fip(stats, ip=0.0, fip_constant=3.10) == 0.0

    def test_negative_ip_returns_zero(self) -> None:
        stats = PitchingStats()
        assert calculate_fip(stats, ip=-1.0, fip_constant=3.10) == 0.0

    def test_zero_stats(self) -> None:
        stats = PitchingStats()
        result = calculate_fip(stats, ip=6.0, fip_constant=3.10)
        assert result == round((0 + 0 - 0) / 6.0 + 3.10, 2)


class TestCalculateEra:
    def test_basic_era(self) -> None:
        assert calculate_era(earned_runs=3, ip=6.0) == round((3 / 6.0) * 9, 2)

    def test_zero_ip_returns_zero(self) -> None:
        assert calculate_era(earned_runs=5, ip=0.0) == 0.0

    def test_zero_earned_runs(self) -> None:
        assert calculate_era(earned_runs=0, ip=7.0) == 0.0

    def test_high_era(self) -> None:
        assert calculate_era(earned_runs=10, ip=3.0) == round((10 / 3.0) * 9, 2)


class TestPitchingStats:
    def test_default_values(self) -> None:
        stats = PitchingStats()
        assert stats.home_runs == 0
        assert stats.walks == 0
        assert stats.hit_batters == 0
        assert stats.strikeouts == 0

    def test_custom_values(self) -> None:
        stats = PitchingStats(home_runs=10, walks=50, hit_batters=5, strikeouts=200)
        assert stats.home_runs == 10
        assert stats.walks == 50
        assert stats.hit_batters == 5
        assert stats.strikeouts == 200
