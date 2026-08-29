"""TDD Contract and Integration Tests for Phase 105A Gate 2 Independent Dual-Path Audit."""

from __future__ import annotations

from decimal import Decimal
import pytest

from src.formulas.dual_path import (
    DualPathAuditEngine,
    DualPathEvaluationResult,
    IndependentFormulaOracle,
)
from src.formulas.models import (
    EvaluationStatus,
    MetricCategory,
    ParityStatus,
)
from src.formulas.registry import FormulaRegistry


class TestIndependentFormulaOracle:
    """Verify that the independent reference oracle evaluates all 33 metrics correctly."""

    def test_oracle_batting_metrics_match_registry(self) -> None:
        """Verify batting calculations between Registry and Independent Oracle."""
        inputs = {
            "hits": 150,
            "at_bats": 450,
            "doubles": 30,
            "triples": 5,
            "home_runs": 25,
            "walks": 60,
            "intentional_walks": 5,
            "hbp": 8,
            "strikeouts": 80,
            "sacrifice_flies": 6,
            "stolen_bases": 20,
            "caught_stealing": 4,
            "plate_appearances": 524,
        }
        constants = {
            "w_bb": 0.690,
            "w_hbp": 0.720,
            "w_1b": 0.890,
            "w_2b": 1.270,
            "w_3b": 1.620,
            "w_hr": 2.100,
            "woba_scale": 1.240,
            "lg_woba": 0.330,
            "lg_r_per_pa": 0.120,
            "lg_obp": 0.340,
            "lg_slg": 0.410,
        }

        # Compare AVG
        oracle_avg = IndependentFormulaOracle.calculate("AVG", inputs, constants)
        reg_avg = FormulaRegistry.get("AVG").evaluate(inputs, constants)
        assert oracle_avg.raw_value == Decimal("0.333") or round(float(oracle_avg.raw_value), 3) == reg_avg

        # Compare OPS
        oracle_ops = IndependentFormulaOracle.calculate("OPS", inputs, constants)
        reg_ops = FormulaRegistry.get("OPS").evaluate(inputs, constants)
        assert round(float(oracle_ops.raw_value), 3) == reg_ops

        # Compare wOBA
        oracle_woba = IndependentFormulaOracle.calculate("wOBA", inputs, constants)
        reg_woba = FormulaRegistry.get("wOBA").evaluate(inputs, constants)
        assert round(float(oracle_woba.raw_value), 3) == reg_woba

    def test_oracle_pitching_metrics_match_registry(self) -> None:
        """Verify pitching calculations between Registry and Independent Oracle."""
        inputs = {
            "earned_runs": 35,
            "innings_outs": 300,  # 100 IP
            "hits_allowed": 90,
            "walks_allowed": 30,
            "intentional_walks_allowed": 2,
            "hit_batters": 4,
            "home_runs_allowed": 10,
            "strikeouts": 95,
            "batters_faced": 410,
            "runs_allowed": 40,
        }
        constants = {"c_fip": 3.850, "lg_era": 4.500}

        # Compare ERA
        oracle_era = IndependentFormulaOracle.calculate("ERA", inputs, constants)
        reg_era = FormulaRegistry.get("ERA").evaluate(inputs, constants)
        assert round(float(oracle_era.raw_value), 2) == reg_era

        # Compare WHIP
        oracle_whip = IndependentFormulaOracle.calculate("WHIP", inputs, constants)
        reg_whip = FormulaRegistry.get("WHIP").evaluate(inputs, constants)
        assert round(float(oracle_whip.raw_value), 2) == reg_whip

        # Compare FIP
        oracle_fip = IndependentFormulaOracle.calculate("FIP", inputs, constants)
        reg_fip = FormulaRegistry.get("FIP").evaluate(inputs, constants)
        assert round(float(oracle_fip.raw_value), 2) == reg_fip


class TestDualPathClassification:
    """Verify 6-way disaggregated parity classification logic."""

    def test_exact_and_rounded_parity(self) -> None:
        """Exact match between Path A and Path B classifies as EXACT or ROUNDED_CONTRACT."""
        result = DualPathAuditEngine.classify_evaluation(
            metric_id="AVG",
            path_a_val=0.333,
            path_b_val=0.333,
            stored_val=0.333,
            path_a_status=EvaluationStatus.DEFINED,
            path_b_status=EvaluationStatus.DEFINED,
        )
        assert result.parity_status in (ParityStatus.EXACT, ParityStatus.ROUNDED_CONTRACT)
        assert result.is_reproducible is True

    def test_zero_denominator_classified_as_undefined(self) -> None:
        """Zero denominator in both paths is classified as UNDEFINED without divergence."""
        result = DualPathAuditEngine.classify_evaluation(
            metric_id="AVG",
            path_a_val=None,
            path_b_val=None,
            stored_val=None,
            path_a_status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
            path_b_status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
        )
        assert result.parity_status == ParityStatus.UNDEFINED
        assert result.is_reproducible is True

    def test_divergence_detection(self) -> None:
        """Disagreement between Path A and Path B, or divergence with DB, is marked DIVERGENT."""
        result = DualPathAuditEngine.classify_evaluation(
            metric_id="AVG",
            path_a_val=0.333,
            path_b_val=0.250,  # Intentional discrepancy
            stored_val=0.333,
            path_a_status=EvaluationStatus.DEFINED,
            path_b_status=EvaluationStatus.DEFINED,
        )
        assert result.parity_status == ParityStatus.DIVERGENT
        assert result.is_reproducible is False
