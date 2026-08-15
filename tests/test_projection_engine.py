"""Unit tests for MarcelProjectionEngine."""

from __future__ import annotations

from src.aggregators.projection_engine import MarcelProjectionEngine, MarcelWeights


def test_aging_curve_multiplier() -> None:
    """Age 29 is peak (1.0), younger is > 1.0 (growth), older is < 1.0 (decline)."""
    engine = MarcelProjectionEngine()
    assert engine.get_aging_factor(29) == 1.0
    assert engine.get_aging_factor(25) > 1.0  # Young player growth
    assert engine.get_aging_factor(35) < 1.0  # Aging decline


def test_hitter_projection_reproducibility() -> None:
    """Given identical inputs, projection output must be 100% deterministic and reproducible."""
    engine = MarcelProjectionEngine(MarcelWeights(5.0, 4.0, 3.0))

    history = [
        {
            "pa": 500,
            "ab": 440,
            "hits": 132,
            "doubles": 25,
            "home_runs": 20,
            "walks": 50,
            "strikeouts": 80,
        },  # Y-1 (0.300)
        {
            "pa": 450,
            "ab": 400,
            "hits": 112,
            "doubles": 20,
            "home_runs": 15,
            "walks": 40,
            "strikeouts": 75,
        },  # Y-2 (0.280)
        {
            "pa": 400,
            "ab": 360,
            "hits": 90,
            "doubles": 18,
            "home_runs": 10,
            "walks": 35,
            "strikeouts": 70,
        },  # Y-3 (0.250)
    ]
    league_rates = {
        "h_per_pa": 0.235,
        "hr_per_pa": 0.024,
        "bb_per_pa": 0.082,
        "so_per_pa": 0.185,
        "tb_per_pa": 0.355,
        "ab_per_pa": 0.875,
    }

    res1 = engine.project_hitter(history_seasons=history, league_rates=league_rates, age=27, park_factor=1.0)
    res2 = engine.project_hitter(history_seasons=history, league_rates=league_rates, age=27, park_factor=1.0)

    assert res1 == res2
    assert res1["projected_avg"] > 0.250
    assert res1["projected_home_runs"] > 10.0
    assert res1["projected_pa"] > 0


def test_pitcher_projection_reproducibility() -> None:
    """Given identical inputs, pitcher projection must be deterministic."""
    engine = MarcelProjectionEngine()

    history = [
        {
            "innings_outs": 450,
            "earned_runs": 60,
            "strikeouts": 130,
            "walks": 45,
            "hits": 140,
            "home_runs": 12,
        },  # 150 IP, 3.60 ERA
        {
            "innings_outs": 400,
            "earned_runs": 65,
            "strikeouts": 110,
            "walks": 40,
            "hits": 135,
            "home_runs": 15,
        },  # 133.1 IP, 4.39 ERA
    ]
    league_rates = {
        "er_per_out": 0.145,
        "so_per_out": 0.210,
        "bb_per_out": 0.085,
        "h_per_out": 0.275,
        "hr_per_out": 0.026,
    }

    res = engine.project_pitcher(history_seasons=history, league_rates=league_rates, age=28, park_factor=1.0)
    assert res["projected_era"] > 3.00
    assert res["projected_fip"] > 3.00
    assert res["projected_ip"] > 100.0
