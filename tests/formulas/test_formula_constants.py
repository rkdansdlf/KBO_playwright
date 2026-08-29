"""Tests for League Environment Constants and Linear Weights Calibration."""

from __future__ import annotations

from src.formulas.constants import LeagueConstantsEngine


def test_baseline_constants_eras() -> None:
    """Test LeagueConstantsEngine baseline constants across historical KBO eras."""
    c_1985 = LeagueConstantsEngine.get_baseline_constants(1985)
    assert c_1985["w_hr"] == 2.050
    assert c_1985["c_fip"] == 3.450

    c_1995 = LeagueConstantsEngine.get_baseline_constants(1995)
    assert c_1995["w_hr"] == 2.080
    assert c_1995["c_fip"] == 4.050

    c_2016 = LeagueConstantsEngine.get_baseline_constants(2016)
    assert c_2016["w_hr"] == 2.150
    assert c_2016["c_fip"] == 4.450

    c_2024 = LeagueConstantsEngine.get_baseline_constants(2024)
    assert c_2024["w_hr"] == 2.100
    assert c_2024["c_fip"] == 3.850
    assert c_2024["w_1b"] == 0.890
    assert c_2024["w_2b"] == 1.270
    assert c_2024["w_3b"] == 1.620
