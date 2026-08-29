"""Tests for Formula Registry handling edge-cases, division-by-zero, and corrupted inputs."""

from __future__ import annotations

import pytest

from src.formulas.registry import FormulaRegistry


def test_division_by_zero_handling() -> None:
    """Formulas should safely return 0.0 or baseline without throwing ZeroDivisionError."""
    empty_inputs = {
        "hits": 0,
        "at_bats": 0,
        "walks": 0,
        "hbp": 0,
        "sacrifice_flies": 0,
        "earned_runs": 0,
        "innings_outs": 0,
        "plate_appearances": 0,
    }
    constants = {"w_bb": 0.69, "lg_woba": 0.335, "woba_scale": 1.15, "lg_r_per_pa": 0.125}

    for m_id in FormulaRegistry.list_metric_ids():
        m_def = FormulaRegistry.get(m_id)
        val = m_def.evaluate(empty_inputs, constants)
        assert isinstance(val, (int, float))
        assert val is not None


def test_negative_or_corrupted_inputs() -> None:
    """Formulas handle unexpected nulls or strings gracefully."""
    corrupted_inputs = {
        "hits": None,
        "at_bats": "invalid",
        "walks": 0,
    }
    val = FormulaRegistry.get("AVG").evaluate(corrupted_inputs)
    assert val == 0.000
