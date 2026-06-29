"""Unit tests for recalc_player_stats pure calculation functions."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.cli.recalc_player_stats import (
    _compute_batting_rates,
    _compute_pitching_rates,
)


def _batting_row(
    at_bats: int = 400,
    hits: int = 120,
    doubles: int = 20,
    triples: int = 5,
    home_runs: int = 15,
    walks: int = 50,
    hbp: int = 5,
    sacrifice_flies: int = 3,
    strikeouts: int = 100,
) -> object:
    return SimpleNamespace(
        at_bats=at_bats,
        hits=hits,
        doubles=doubles,
        triples=triples,
        home_runs=home_runs,
        walks=walks,
        hbp=hbp,
        sacrifice_flies=sacrifice_flies,
        strikeouts=strikeouts,
    )


class TestComputeBattingRates:
    def test_normal_row(self) -> None:
        row = _batting_row()
        result = _compute_batting_rates(row)
        assert result["avg"] == pytest.approx(0.300, abs=0.001)
        assert result["obp"] == pytest.approx(0.382, abs=0.001)
        assert result["slg"] == pytest.approx(0.487, abs=0.001)
        assert result["ops"] == pytest.approx(0.869, abs=0.001)

    def test_zero_ab_avoids_division(self) -> None:
        row = _batting_row(at_bats=0)
        result = _compute_batting_rates(row)
        assert result["avg"] == 0.0
        assert result["slg"] == 0.0

    def test_zero_sf_adds_one(self) -> None:
        row = _batting_row(sacrifice_flies=0)
        result = _compute_batting_rates(row)
        assert result["slg"] > 0

    def test_high_values(self) -> None:
        row = _batting_row(at_bats=600, hits=200, walks=100, home_runs=40)
        result = _compute_batting_rates(row)
        assert result["avg"] == pytest.approx(0.333, abs=0.001)
        assert result["ops"] > 0.9

    def test_all_zero_row(self) -> None:
        row = _batting_row(at_bats=0, hits=0, walks=0, home_runs=0, doubles=0, triples=0, strikeouts=0)
        result = _compute_batting_rates(row)
        assert result["avg"] == 0
        assert result["slg"] == 0

    def test_missing_none_ab(self) -> None:
        row = SimpleNamespace(
            at_bats=None,
            hits=None,
            doubles=None,
            triples=None,
            home_runs=None,
            walks=None,
            hbp=None,
            sacrifice_flies=None,
            strikeouts=None,
        )
        result = _compute_batting_rates(row)
        assert result["avg"] == 0
        assert result["slg"] == 0

    def test_iso_diff(self) -> None:
        row = _batting_row()
        result = _compute_batting_rates(row)
        assert result["iso"] == pytest.approx(result["slg"] - result["avg"], abs=0.001)

    def test_babip(self) -> None:
        row = _batting_row(at_bats=400, hits=120, home_runs=15, strikeouts=100, sacrifice_flies=3)
        result = _compute_batting_rates(row)
        expected = (120 - 15) / (400 - 100 - 15 + 3)
        assert result["babip"] == pytest.approx(expected, abs=0.001)


class TestComputePitchingRates:
    def test_normal(self) -> None:
        result = _compute_pitching_rates(540, 150, 40, 45, 160)
        assert result["innings_pitched"] == 180.0
        assert result["era"] == pytest.approx(2.25, abs=0.01)
        assert result["whip"] == pytest.approx(1.06, abs=0.01)
        assert result["k_per_nine"] == pytest.approx(8.0, abs=0.01)

    def test_zero_outs(self) -> None:
        result = _compute_pitching_rates(0, 0, 0, 0, 0)
        assert result["era"] == 0
        assert result["whip"] == 0

    def test_high_so_low_bb(self) -> None:
        result = _compute_pitching_rates(540, 100, 5, 30, 200)
        assert result["kbb"] == pytest.approx(40.0, abs=0.01)
        assert result["k_per_nine"] == pytest.approx(10.0, abs=0.01)

    def test_no_walks_avoids_division(self) -> None:
        result = _compute_pitching_rates(540, 150, 0, 45, 160)
        assert result["kbb"] == 0
        assert result["bb_per_nine"] == 0

    def test_low_era_high_k(self) -> None:
        result = _compute_pitching_rates(720, 100, 20, 25, 250)
        assert result["era"] < 3.0
        assert result["k_per_nine"] > 9.0
