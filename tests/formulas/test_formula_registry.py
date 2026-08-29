"""Tests for Central Formula Registry Catalog and Metrics."""

from __future__ import annotations

import pytest

from src.formulas.models import MetricCategory
from src.formulas.registry import FormulaRegistry


def test_registry_count_and_categories() -> None:
    """Verify registry has at least 30 metrics spanning all categories."""
    assert FormulaRegistry.count() >= 30

    bat_metrics = FormulaRegistry.list_all(MetricCategory.BATTING)
    pit_metrics = FormulaRegistry.list_all(MetricCategory.PITCHING)
    field_metrics = FormulaRegistry.list_all(MetricCategory.FIELDING)
    base_metrics = FormulaRegistry.list_all(MetricCategory.BASERUNNING)

    assert len(bat_metrics) >= 15
    assert len(pit_metrics) >= 12
    assert len(field_metrics) >= 2
    assert len(base_metrics) >= 1


def test_get_metric_case_insensitive() -> None:
    """Verify get() retrieves definitions case-insensitively."""
    m_woba = FormulaRegistry.get("woba")
    assert m_woba.metric_id == "wOBA"
    assert m_woba.category == MetricCategory.BATTING

    m_ops = FormulaRegistry.get("ops_plus")
    assert m_ops.metric_id == "OPS_INDEX_NO_PARK"
    assert m_ops.is_deprecated_alias is True

    with pytest.raises(KeyError):
        FormulaRegistry.get("NON_EXISTENT_METRIC")


def test_batting_math_evaluations() -> None:
    """Verify exact calculation for standard batting formulas."""
    constants = {
        "w_bb": 0.690,
        "w_hbp": 0.720,
        "w_1b": 0.890,
        "w_2b": 1.270,
        "w_3b": 1.620,
        "w_hr": 2.100,
        "lg_woba": 0.335,
        "woba_scale": 1.150,
        "lg_r_per_pa": 0.125,
        "lg_obp": 0.340,
        "lg_slg": 0.410,
    }

    # Example: 100 H (70 1B, 20 2B, 2 3B, 8 HR), 300 AB, 40 BB, 5 HBP, 3 SF, 350 PA
    inputs = {
        "hits": 100,
        "doubles": 20,
        "triples": 2,
        "home_runs": 8,
        "at_bats": 300,
        "walks": 40,
        "intentional_walks": 2,
        "hbp": 5,
        "sacrifice_flies": 3,
        "plate_appearances": 350,
        "strikeouts": 50,
    }

    avg = FormulaRegistry.get("AVG").evaluate(inputs, constants)
    assert avg == 0.333

    obp = FormulaRegistry.get("OBP").evaluate(inputs, constants)
    # (100 + 40 + 5) / (300 + 40 + 5 + 3) = 145 / 348 = 0.41666... -> 0.417
    assert obp == 0.417

    slg = FormulaRegistry.get("SLG").evaluate(inputs, constants)
    # 70 + 40 + 6 + 32 = 148 TB / 300 AB = 0.49333... -> 0.493
    assert slg == 0.493

    ops = FormulaRegistry.get("OPS").evaluate(inputs, constants)
    assert ops == round(0.417 + 0.493, 3)

    iso = FormulaRegistry.get("ISO").evaluate(inputs, constants)
    assert iso == round(0.493 - 0.333, 3)


def test_pitching_math_evaluations() -> None:
    """Verify exact calculation for standard pitching formulas."""
    constants = {"c_fip": 3.850, "lg_era": 4.500}

    # Example: 50 ER, 450 Outs (150.0 IP), 130 H, 40 BB, 2 IBB, 5 HBP, 120 K, 12 HR
    inputs = {
        "earned_runs": 50,
        "innings_outs": 450,
        "hits_allowed": 130,
        "walks_allowed": 40,
        "intentional_walks_allowed": 2,
        "hit_batters": 5,
        "strikeouts": 120,
        "home_runs_allowed": 12,
        "batters_faced": 620,
    }

    era = FormulaRegistry.get("ERA").evaluate(inputs, constants)
    # (50 * 27) / 450 = 1350 / 450 = 3.00
    assert era == 3.00

    whip = FormulaRegistry.get("WHIP").evaluate(inputs, constants)
    # 3 * (130 + 40) / 450 = 510 / 450 = 1.133... -> 1.13
    assert whip == 1.13

    fip = FormulaRegistry.get("FIP").evaluate(inputs, constants)
    # fip_comp = (3 * (13*12 + 3*(40 + 5) - 2*120)) / 450 = (3 * (156 + 135 - 240)) / 450 = 3 * 51 / 450 = 0.34
    # 0.34 + 3.85 = 4.19
    assert fip == 4.19
