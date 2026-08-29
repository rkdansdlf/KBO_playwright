"""TDD Contract Tests for Phase 105A Gate 1 Formula Contract Remediation."""

from __future__ import annotations

from decimal import Decimal
import pytest

from src.formulas.constants import LeagueConstantsEngine
from src.formulas.models import (
    EvaluationStatus,
    MetricCategory,
    ParityStatus,
    RuleSeverity,
)
from src.formulas.registry import FormulaRegistry


class TestSeasonResolverContract:
    """Verify era resolution for 1982~2026 with no gaps, no duplicates, and fail-closed out-of-bounds."""

    def test_all_45_seasons_resolve_exactly_once(self) -> None:
        """1982~2026 must all resolve to a valid profile with deterministic era identifier."""
        resolved_eras: set[str] = set()
        for season in range(1982, 2027):
            profile = LeagueConstantsEngine.resolve_era_profile(season)
            assert profile is not None
            assert profile.season_start <= season <= profile.season_end
            resolved_eras.add(profile.era_id)

        expected_eras = {
            "KBO_ERA_1982_1990_APPROX_V1",
            "KBO_ERA_1991_2000_APPROX_V1",
            "KBO_ERA_2001_2013_UNCALIBRATED",
            "KBO_ERA_2014_2018_APPROX_V1",
            "KBO_ERA_2019_2026_APPROX_V1",
        }
        assert resolved_eras == expected_eras

    def test_2001_2013_uncalibrated_status(self) -> None:
        """2001~2013 must explicitly report UNCALIBRATED without silent fallback to modern constants."""
        profile = LeagueConstantsEngine.resolve_era_profile(2008)
        assert profile.is_calibrated is False
        assert profile.era_id == "KBO_ERA_2001_2013_UNCALIBRATED"
        # When attempting to retrieve static baseline weights without dynamic session aggregates:
        with pytest.raises(ValueError, match="UNCALIBRATED"):
            LeagueConstantsEngine.get_baseline_constants(2008)

    def test_out_of_bounds_seasons_fail_closed(self) -> None:
        """Seasons before 1982 or after 2026 must fail closed."""
        with pytest.raises(ValueError, match="out of supported range"):
            LeagueConstantsEngine.resolve_era_profile(1981)

        with pytest.raises(ValueError, match="out of supported range"):
            LeagueConstantsEngine.resolve_era_profile(2027)


class TestMetricNamingAndAliases:
    """Verify No-Park canonical identifiers and deprecated alias contract."""

    def test_canonical_no_park_metrics_exist(self) -> None:
        """WRC_INDEX_NO_PARK, OPS_INDEX_NO_PARK, ERA_INDEX_NO_PARK must be canonical."""
        wrc = FormulaRegistry.get("WRC_INDEX_NO_PARK")
        assert wrc.metric_id == "WRC_INDEX_NO_PARK"
        assert wrc.is_park_adjusted is False

        ops = FormulaRegistry.get("OPS_INDEX_NO_PARK")
        assert ops.metric_id == "OPS_INDEX_NO_PARK"
        assert ops.is_park_adjusted is False

        era = FormulaRegistry.get("ERA_INDEX_NO_PARK")
        assert era.metric_id == "ERA_INDEX_NO_PARK"
        assert era.is_park_adjusted is False

    def test_deprecated_aliases_forward_with_warning(self) -> None:
        """wRC_PLUS, OPS_PLUS, ERA_PLUS must resolve with deprecation warning flag."""
        wrc_alias = FormulaRegistry.get("wRC_PLUS")
        assert wrc_alias.metric_id == "WRC_INDEX_NO_PARK"
        assert wrc_alias.is_deprecated_alias is True
        assert "Park factor is not applied" in wrc_alias.deprecation_warning

        ops_alias = FormulaRegistry.get("OPS_PLUS")
        assert ops_alias.metric_id == "OPS_INDEX_NO_PARK"
        assert ops_alias.is_deprecated_alias is True

        era_alias = FormulaRegistry.get("ERA_PLUS")
        assert era_alias.metric_id == "ERA_INDEX_NO_PARK"
        assert era_alias.is_deprecated_alias is True


class TestZeroDenominatorAndUndefinedContract:
    """Verify zero denominator yields Undefined status with reason code and audit inclusion."""

    def test_zero_ab_returns_undefined_status(self) -> None:
        """AVG with AB=0 returns UNDEFINED_ZERO_DENOMINATOR with raw_value=None."""
        metric = FormulaRegistry.get("AVG")
        inputs = {"hits": 0, "at_bats": 0}
        eval_result = metric.evaluate_detailed(inputs, {})

        assert eval_result.status == EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR
        assert eval_result.raw_value is None
        assert eval_result.rounded_value is None
        assert eval_result.eligible_for_numeric_comparison is False
        assert eval_result.included_in_audit_population is True
        assert eval_result.reason_code == "ZERO_AT_BATS"

    def test_zero_outs_returns_undefined_status(self) -> None:
        """ERA with Outs=0 returns UNDEFINED_ZERO_DENOMINATOR with raw_value=None."""
        metric = FormulaRegistry.get("ERA")
        inputs = {"earned_runs": 2, "innings_outs": 0}
        eval_result = metric.evaluate_detailed(inputs, {})

        assert eval_result.status == EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR
        assert eval_result.raw_value is None
        assert eval_result.reason_code == "ZERO_INNINGS_OUTS"


class TestValidationSeverityContract:
    """Verify 4-tier validation rule severity classification."""

    def test_algebraic_failure_marks_hard_fail(self) -> None:
        """AVG > 1.0 is an ALGEBRAIC violation and marks evaluation as invalid."""
        metric = FormulaRegistry.get("AVG")
        inputs = {"hits": 10, "at_bats": 5}  # AVG = 2.0
        eval_result = metric.evaluate_detailed(inputs, {})
        assert eval_result.status == EvaluationStatus.INVALID_SOURCE_INPUT
        assert any(r.severity == RuleSeverity.ALGEBRAIC for r in eval_result.validation_failures)

    def test_plausibility_warning_does_not_fail_evaluation(self) -> None:
        """Negative FIP is physically possible in small samples (PLAUSIBILITY warning)."""
        metric = FormulaRegistry.get("FIP")
        constants = {"c_fip": 3.850}
        # 1 inning (3 outs), 3 K, 0 HR, 0 BB -> fip_comp = -6 -> FIP = -2.15
        inputs = {
            "home_runs_allowed": 0,
            "walks_allowed": 0,
            "intentional_walks_allowed": 0,
            "hit_batters": 0,
            "strikeouts": 3,
            "innings_outs": 3,
        }
        eval_result = metric.evaluate_detailed(inputs, constants)
        assert eval_result.status == EvaluationStatus.DEFINED
        assert eval_result.rounded_value == Decimal("-2.15")
        # Should have plausibility warning, not hard failure
        assert any(w.severity == RuleSeverity.PLAUSIBILITY for w in eval_result.validation_warnings)
