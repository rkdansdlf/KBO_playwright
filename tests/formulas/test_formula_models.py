"""Tests for Formula Registry Domain Models and Contracts."""

from __future__ import annotations

from src.formulas.models import (
    FormulaAuditReport,
    FormulaVersion,
    MetricCategory,
    MetricConstant,
    MetricDefinition,
    MetricEvaluationResult,
    ValidationRule,
    ZeroDivisionStrategy,
)


def test_formula_version_applicability() -> None:
    """Test FormulaVersion effective season matching."""
    v1 = FormulaVersion("1.0.0", effective_season_start=2015, effective_season_end=2020)
    assert v1.is_applicable_to_season(2014) is False
    assert v1.is_applicable_to_season(2015) is True
    assert v1.is_applicable_to_season(2020) is True
    assert v1.is_applicable_to_season(2021) is False

    v_open = FormulaVersion("2.0.0", effective_season_start=2021)
    assert v_open.is_applicable_to_season(2020) is False
    assert v_open.is_applicable_to_season(2026) is True


def test_metric_constant_serialization() -> None:
    """Test MetricConstant serialization."""
    c = MetricConstant(key="w_hr", value=2.10, season=2024, description="Linear weight for HR")
    d = c.to_dict()
    assert d["key"] == "w_hr"
    assert d["value"] == 2.10
    assert d["season"] == 2024


def test_validation_rule_evaluation() -> None:
    """Test ValidationRule invariant execution."""
    rule = ValidationRule(
        name="non_negative",
        rule_fn=lambda v, _: v >= 0.0,
        latex_repr=r"v \ge 0",
        error_message="Value must be non-negative",
    )
    assert rule.validate(1.5, {}) is True
    assert rule.validate(-0.1, {}) is False


def test_metric_definition_evaluation() -> None:
    """Test MetricDefinition evaluate method."""
    m = MetricDefinition(
        metric_id="SLG_TEST",
        name="Slugging",
        korean_name="장타율",
        category=MetricCategory.BATTING,
        version=FormulaVersion("1.0.0", 1982),
        latex_formula=r"\frac{TB}{AB}",
        eval_fn=lambda inp, _c: inp.get("tb", 0) / inp.get("ab", 1),
        input_fields=["tb", "ab"],
        zero_division_strategy=ZeroDivisionStrategy.RETURN_ZERO,
        precision=3,
    )
    val = m.evaluate({"tb": 150, "ab": 300})
    assert val == 0.500
    d = m.to_dict()
    assert d["metric_id"] == "SLG_TEST"
    assert d["category"] == "BATTING"


def test_audit_report_serialization() -> None:
    """Test FormulaAuditReport serialization."""
    rep = FormulaAuditReport(
        audit_mode="ALL_SEASONS",
        season=None,
        category="BATTING",
        total_metrics_evaluated=16,
        total_entities_checked=500,
        reproducible_count=500,
        divergent_count=0,
        reproducibility_ratio=1.0,
        metric_breakdowns={"AVG": {"evaluations": 500, "reproducible": 500, "divergent": 0}},
        duration_ms=45.2,
        is_compliant=True,
        git_sha="abcdef",
        generated_at_utc="2026-08-30T00:00:00Z",
        sha256_checksum="123456",
    )
    d = rep.to_dict()
    assert d["is_compliant"] is True
    assert d["reproducibility_ratio"] == 1.0
    assert d["git_sha"] == "abcdef"
