from __future__ import annotations

from src.models.stat_dataclasses import BattingStats, PitchingStats


class TestBattingStats:
    def test_defaults(self):
        stats = BattingStats()
        assert stats.hits == 0
        assert stats.at_bats == 0
        assert stats.walks == 0
        assert stats.hbp == 0
        assert stats.sf == 0
        assert stats.strikeouts == 0
        assert stats.doubles == 0
        assert stats.triples == 0
        assert stats.home_runs == 0

    def test_custom_values(self):
        stats = BattingStats(hits=3, at_bats=10, home_runs=1)
        assert stats.hits == 3
        assert stats.at_bats == 10
        assert stats.home_runs == 1
        assert stats.walks == 0

    def test_equality(self):
        a = BattingStats(hits=2, at_bats=5)
        b = BattingStats(hits=2, at_bats=5)
        assert a == b


class TestPitchingStats:
    def test_defaults(self):
        stats = PitchingStats()
        assert stats.total_outs == 0
        assert stats.hits == 0
        assert stats.bb == 0
        assert stats.er == 0
        assert stats.k == 0
        assert stats.hr == 0

    def test_custom_values(self):
        stats = PitchingStats(total_outs=27, k=10, er=3)
        assert stats.total_outs == 27
        assert stats.k == 10
        assert stats.er == 3
