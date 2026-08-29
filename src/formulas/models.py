"""Domain Models, Invariants, and Value Objects for Sabermetrics Formula Registry."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class MetricCategory(StrEnum):
    """Categorical taxonomy for KBO sabermetric formulas."""

    BATTING = "BATTING"
    PITCHING = "PITCHING"
    FIELDING = "FIELDING"
    BASERUNNING = "BASERUNNING"
    COMPOSITE = "COMPOSITE"


class AggregationScope(StrEnum):
    """Scope of metric aggregation."""

    INDIVIDUAL_GAME = "INDIVIDUAL_GAME"
    PLAYER_SEASON = "PLAYER_SEASON"
    PLAYER_CAREER = "PLAYER_CAREER"
    TEAM_GAME = "TEAM_GAME"
    TEAM_SEASON = "TEAM_SEASON"
    LEAGUE_SEASON = "LEAGUE_SEASON"


class ZeroDivisionStrategy(StrEnum):
    """Strategy for handling zero denominators."""

    RETURN_ZERO = "RETURN_ZERO"
    RETURN_NULL = "RETURN_NULL"
    RETURN_ONE = "RETURN_ONE"
    UNDEFINED = "UNDEFINED"


@dataclass(frozen=True)
class MetricConstant:
    """Named constant used within a metric calculation formula."""

    key: str = ""
    name: str = ""
    value: float = 0.0
    season: int | None = None
    description: str = ""
    season_specific: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert constant to dictionary."""
        return asdict(self)


class EvaluationStatus(StrEnum):
    """Execution outcome status for metric evaluation."""

    DEFINED = "DEFINED"
    UNDEFINED_ZERO_DENOMINATOR = "UNDEFINED_ZERO_DENOMINATOR"
    UNDEFINED_CALIBRATION_UNAVAILABLE = "UNDEFINED_CALIBRATION_UNAVAILABLE"
    INVALID_SOURCE_INPUT = "INVALID_SOURCE_INPUT"


class ParityStatus(StrEnum):
    """Disaggregated classification of reproducibility audit comparisons."""

    EXACT = "EXACT"
    ROUNDED_CONTRACT = "ROUNDED_CONTRACT"
    FLOATING_TOLERANCE = "FLOATING_TOLERANCE"
    UNDEFINED = "UNDEFINED"
    EXCLUDED = "EXCLUDED"
    DIVERGENT = "DIVERGENT"


class RuleSeverity(StrEnum):
    """Severity classification for metric validation rules."""

    DOMAIN = "DOMAIN"
    ALGEBRAIC = "ALGEBRAIC"
    SOURCE_SCHEMA_INTEGRITY = "SOURCE_SCHEMA_INTEGRITY"
    PLAUSIBILITY = "PLAUSIBILITY"


@dataclass(frozen=True)
class FormulaVersion:
    """Immutably identifies a specific mathematical specification version."""

    version: str
    effective_season_start: int
    effective_season_end: int | None = None
    changelog: str = ""

    def is_applicable_to_season(self, season: int) -> bool:
        """Check if this formula version applies to a target season."""
        if season < self.effective_season_start:
            return False
        return self.effective_season_end is None or season <= self.effective_season_end


@dataclass(frozen=True)
class EraProfile:
    """Historical KBO era calibration specification."""

    era_id: str
    name: str
    season_start: int
    season_end: int
    is_calibrated: bool
    constants: dict[str, float] | None = None
    provenance: str = ""


@dataclass(frozen=True)
class ValidationRule:
    """Declarative invariant or plausibility constraint for metric validation."""

    name: str
    rule_fn: Callable[[float | Decimal, dict[str, Any]], bool] | None = None
    latex_repr: str = ""
    error_message: str = ""
    severity: RuleSeverity = RuleSeverity.ALGEBRAIC
    validator_fn: Callable[[float | Decimal, dict[str, Any]], bool] | None = None
    latex_rule: str = ""

    def __post_init__(self) -> None:
        """Normalize validator function and latex representation."""
        fn = self.rule_fn or self.validator_fn
        if fn is None:
            msg = "Either rule_fn or validator_fn must be provided to ValidationRule."
            raise ValueError(msg)
        object.__setattr__(self, "validator_fn", fn)
        object.__setattr__(self, "rule_fn", fn)

        latex = self.latex_repr or self.latex_rule
        object.__setattr__(self, "latex_repr", latex)
        object.__setattr__(self, "latex_rule", latex)

    def validate(self, val: float | Decimal | None, inputs: dict[str, Any]) -> bool:
        """Execute invariant predicate against calculated value and input context."""
        if val is None:
            return True
        try:
            fn = self.rule_fn or self.validator_fn
            return fn(val, inputs) if fn else True
        except (ValueError, TypeError, KeyError, AttributeError, ZeroDivisionError, ArithmeticError):
            return False


@dataclass(frozen=True)
class FormulaEvaluation:
    """Strict typed result of a mathematical formula evaluation."""

    status: EvaluationStatus
    raw_value: Decimal | None
    rounded_value: Decimal | None
    reason_code: str | None = None
    eligible_for_numeric_comparison: bool = True
    included_in_audit_population: bool = True
    validation_failures: list[ValidationRule] = field(default_factory=list)
    validation_warnings: list[ValidationRule] = field(default_factory=list)


@dataclass(frozen=True)
class MetricDefinition:
    """Full specification of a standardized KBO sabermetric formula."""

    metric_id: str
    name: str
    korean_name: str
    category: MetricCategory
    version: FormulaVersion
    latex_formula: str
    eval_fn: Callable[[dict[str, Any], dict[str, float]], float]
    input_fields: list[str]
    constants_required: list[str] = field(default_factory=list)
    validation_rules: list[ValidationRule] = field(default_factory=list)
    zero_division_strategy: ZeroDivisionStrategy = ZeroDivisionStrategy.RETURN_ZERO
    precision: int = 3
    description: str = ""
    is_park_adjusted: bool = True
    is_deprecated_alias: bool = False
    deprecation_warning: str | None = None

    def evaluate(self, inputs: dict[str, Any], constants: dict[str, float] | None = None) -> float:
        """Execute the pure evaluation function with inputs and constants."""
        c = constants or {}
        return self.eval_fn(inputs, c)

    def _check_domain_zero_denominators(self, inputs: dict[str, Any]) -> str | None:
        """Check if inputs violate domain conditions with a zero denominator."""
        if self.metric_id in ("AVG", "SLG", "ISO", "SecA") and float(inputs.get("at_bats") or 0) <= 0:
            return "ZERO_AT_BATS"

        if self.metric_id in ("ERA", "WHIP", "FIP", "K_9", "BB_9", "HR_9", "DICE", "ERA_INDEX_NO_PARK"):
            outs = float(inputs.get("innings_outs") or 0)
            ip = float(inputs.get("innings_pitched") or 0)
            if outs <= 0 and ip <= 0:
                return "ZERO_INNINGS_OUTS"

        if self.metric_id == "BB_TO_K_BAT" and float(inputs.get("strikeouts") or 0) <= 0:
            return "ZERO_STRIKEOUTS"

        if self.metric_id in ("BB_PCT_BAT", "K_PCT_BAT", "wRAA", "wRC", "WRC_INDEX_NO_PARK"):
            pa = float(inputs.get("plate_appearances") or inputs.get("pa") or 0)
            ab = float(inputs.get("at_bats") or 0)
            bb = float(inputs.get("walks") or 0)
            if pa <= 0 and (ab + bb) <= 0:
                return "ZERO_PLATE_APPEARANCES"

        return None

    def evaluate_detailed(
        self,
        inputs: dict[str, Any],
        constants: dict[str, float] | None = None,
    ) -> FormulaEvaluation:
        """Execute evaluation with strict zero-denominator detection and Decimal precision."""
        zero_reason = self._check_domain_zero_denominators(inputs)
        if zero_reason is not None:
            return FormulaEvaluation(
                status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                raw_value=None,
                rounded_value=None,
                reason_code=zero_reason,
                eligible_for_numeric_comparison=False,
                included_in_audit_population=True,
            )

        c = constants or {}
        try:
            val_float = self.eval_fn(inputs, c)
            dec_raw = Decimal(str(val_float))
            format_str = f"0.{'0' * self.precision}" if self.precision > 0 else "0"
            dec_rounded = dec_raw.quantize(Decimal(format_str))
        except (ValueError, TypeError, KeyError, AttributeError, ZeroDivisionError, ArithmeticError):
            return FormulaEvaluation(
                status=EvaluationStatus.INVALID_SOURCE_INPUT,
                raw_value=None,
                rounded_value=None,
                reason_code="EVALUATION_EXCEPTION",
                eligible_for_numeric_comparison=False,
                included_in_audit_population=True,
            )

        failures = [
            r
            for r in self.validation_rules
            if not r.validate(dec_raw, inputs)
            and r.severity in (RuleSeverity.ALGEBRAIC, RuleSeverity.SOURCE_SCHEMA_INTEGRITY)
        ]
        warnings = [
            r
            for r in self.validation_rules
            if not r.validate(dec_raw, inputs) and r.severity == RuleSeverity.PLAUSIBILITY
        ]

        status = EvaluationStatus.INVALID_SOURCE_INPUT if failures else EvaluationStatus.DEFINED

        return FormulaEvaluation(
            status=status,
            raw_value=dec_raw,
            rounded_value=dec_rounded,
            reason_code=None if status == EvaluationStatus.DEFINED else "INVARIANT_VIOLATION",
            eligible_for_numeric_comparison=status == EvaluationStatus.DEFINED,
            included_in_audit_population=True,
            validation_failures=failures,
            validation_warnings=warnings,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert metric definition to JSON-serializable dictionary."""
        return {
            "metric_id": self.metric_id,
            "name": self.name,
            "korean_name": self.korean_name,
            "category": self.category.value,
            "version": asdict(self.version),
            "latex_formula": self.latex_formula,
            "input_fields": self.input_fields,
            "constants_required": self.constants_required,
            "precision": self.precision,
            "description": self.description,
            "is_park_adjusted": self.is_park_adjusted,
            "is_deprecated_alias": self.is_deprecated_alias,
            "deprecation_warning": self.deprecation_warning,
        }


@dataclass(frozen=True)
class MetricEvaluationResult:
    """Result of an evaluation comparison between calculated formula and DB stored value."""

    metric_id: str
    entity_id: int | str
    season: int
    calculated_value: float | Decimal | None
    stored_value: float | None
    inputs_used: dict[str, Any]
    constants_used: dict[str, float]
    delta: float
    is_reproducible: bool
    invariants_passed: bool
    validation_errors: list[str] = field(default_factory=list)
    execution_time_us: float = 0.0
    parity_status: ParityStatus = ParityStatus.EXACT


@dataclass(frozen=True)
class FormulaAuditReport:
    """Comprehensive mathematical reproducibility audit report across entities and seasons."""

    audit_mode: str
    season: int | None
    category: str | None
    total_metrics_evaluated: int
    total_entities_checked: int
    reproducible_count: int
    divergent_count: int
    reproducibility_ratio: float
    metric_breakdowns: dict[str, Any]
    duration_ms: float
    is_compliant: bool
    git_sha: str
    generated_at_utc: str
    sha256_checksum: str

    def to_dict(self) -> dict[str, Any]:
        """Convert audit report to dictionary."""
        return asdict(self)


__all__ = [
    "AggregationScope",
    "EraProfile",
    "EvaluationStatus",
    "FormulaAuditReport",
    "FormulaEvaluation",
    "FormulaVersion",
    "MetricCategory",
    "MetricConstant",
    "MetricDefinition",
    "MetricEvaluationResult",
    "ParityStatus",
    "RuleSeverity",
    "ValidationRule",
    "ZeroDivisionStrategy",
]
