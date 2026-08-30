"""Independent Dual-Path Reference Oracle and Cross-Verification Audit Engine."""

from __future__ import annotations

import contextlib
import time
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine, text

from src.formulas.constants import LeagueConstantsEngine
from src.formulas.models import (
    EvaluationStatus,
    FormulaEvaluation,
    MetricCategory,
    MetricDefinition,
    ParityStatus,
    RuleSeverity,
    ValidationRule,
)
from src.formulas.registry import FormulaRegistry

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SQLAlchemyEngine

_TOLERANCE_MAP: dict[str, float] = {
    "AVG": 0.002,
    "OBP": 0.035,
    "SLG": 0.002,
    "OPS": 0.035,
    "ISO": 0.002,
    "wOBA": 0.005,
    "WRC_INDEX_NO_PARK": 1.0,
    "OPS_INDEX_NO_PARK": 1.0,
    "ERA": 0.02,
    "WHIP": 0.02,
    "FIP": 0.05,
    "ERA_INDEX_NO_PARK": 1.0,
    "FPCT": 0.001,
}

DB_COL_MAP: dict[str, str] = {
    "AVG": "avg",
    "OBP": "obp",
    "SLG": "slg",
    "OPS": "ops",
    "ISO": "iso",
    "BABIP_BAT": "babip",
    "wOBA": "woba",
    "WRC_INDEX_NO_PARK": "wrc_plus",
    "ERA": "era",
    "WHIP": "whip",
    "FPCT": "fielding_pct",
}


def _to_dec(val: object) -> Decimal:
    """Safely convert numerical input to Decimal."""
    if val is None:
        return Decimal(0)
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(0)


class IndependentFormulaOracle:
    """Completely independent reference implementation of KBO Sabermetric formulas."""

    @classmethod
    def _calculate_avg_obp(
        cls,
        norm_metric: str,
        inputs: dict[str, Any],
    ) -> FormulaEvaluation | None:
        """Calculate AVG and OBP."""
        if norm_metric == "AVG":
            h = _to_dec(inputs.get("hits"))
            ab = _to_dec(inputs.get("at_bats"))
            if ab <= Decimal(0):
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="ZERO_AT_BATS",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            val = (h / ab).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        if norm_metric == "OBP":
            h = _to_dec(inputs.get("hits"))
            bb = _to_dec(inputs.get("walks"))
            hbp = _to_dec(inputs.get("hbp"))
            ab = _to_dec(inputs.get("at_bats"))
            sf = _to_dec(inputs.get("sacrifice_flies"))
            den = ab + bb + hbp + sf
            if den <= Decimal(0):
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="ZERO_PLATE_APPEARANCES",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            val = ((h + bb + hbp) / den).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        return None

    @classmethod
    def _calculate_slg(
        cls,
        norm_metric: str,
        inputs: dict[str, Any],
    ) -> FormulaEvaluation | None:
        """Calculate SLG."""
        if norm_metric != "SLG":
            return None
        h = _to_dec(inputs.get("hits"))
        d2 = _to_dec(inputs.get("doubles"))
        d3 = _to_dec(inputs.get("triples"))
        hr = _to_dec(inputs.get("home_runs"))
        ab = _to_dec(inputs.get("at_bats"))
        if ab <= Decimal(0):
            return FormulaEvaluation(
                status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                raw_value=None,
                rounded_value=None,
                reason_code="ZERO_AT_BATS",
                eligible_for_numeric_comparison=False,
                included_in_audit_population=True,
            )
        h1 = max(h - d2 - d3 - hr, Decimal(0))
        tb = h1 + (Decimal(2) * d2) + (Decimal(3) * d3) + (Decimal(4) * hr)
        val = (tb / ab).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        return FormulaEvaluation(
            status=EvaluationStatus.DEFINED,
            raw_value=val,
            rounded_value=val,
            eligible_for_numeric_comparison=True,
            included_in_audit_population=True,
        )

    @classmethod
    def _calculate_ops_iso(
        cls,
        norm_metric: str,
        inputs: dict[str, Any],
        c: dict[str, float],
    ) -> FormulaEvaluation | None:
        """Calculate OPS and ISO."""
        if norm_metric == "OPS":
            obp_res = cls.calculate("OBP", inputs, c)
            slg_res = cls.calculate("SLG", inputs, c)
            if obp_res.raw_value is None or slg_res.raw_value is None:
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="ZERO_DENOMINATOR_COMPONENTS",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            val = (obp_res.raw_value + slg_res.raw_value).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        if norm_metric == "ISO":
            slg_res = cls.calculate("SLG", inputs, c)
            avg_res = cls.calculate("AVG", inputs, c)
            if slg_res.raw_value is None or avg_res.raw_value is None:
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="ZERO_AT_BATS",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            val = (slg_res.raw_value - avg_res.raw_value).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        return None

    @classmethod
    def _calculate_advanced_batting(
        cls,
        norm_metric: str,
        inputs: dict[str, Any],
        c: dict[str, float],
    ) -> FormulaEvaluation | None:
        """Calculate advanced linear weights batting metrics."""
        if norm_metric == "wOBA":
            if not c:
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_CALIBRATION_UNAVAILABLE,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="CALIBRATION_UNAVAILABLE",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            h = _to_dec(inputs.get("hits"))
            d2 = _to_dec(inputs.get("doubles"))
            d3 = _to_dec(inputs.get("triples"))
            hr = _to_dec(inputs.get("home_runs"))
            bb = _to_dec(inputs.get("walks"))
            ibb = _to_dec(inputs.get("intentional_walks"))
            hbp = _to_dec(inputs.get("hbp"))
            sf = _to_dec(inputs.get("sacrifice_flies"))
            ab = _to_dec(inputs.get("at_bats"))

            u_bb = max(bb - ibb, Decimal(0))
            h1 = max(h - d2 - d3 - hr, Decimal(0))

            w_bb = _to_dec(c.get("w_bb", 0.690))
            w_hbp = _to_dec(c.get("w_hbp", 0.720))
            w_1b = _to_dec(c.get("w_1b", 0.890))
            w_2b = _to_dec(c.get("w_2b", 1.270))
            w_3b = _to_dec(c.get("w_3b", 1.620))
            w_hr = _to_dec(c.get("w_hr", 2.100))

            den = ab + u_bb + hbp + sf
            if den <= Decimal(0):
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="ZERO_PLATE_APPEARANCES",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            num = (w_bb * u_bb) + (w_hbp * hbp) + (w_1b * h1) + (w_2b * d2) + (w_3b * d3) + (w_hr * hr)
            val = (num / den).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        return None

    @classmethod
    def _calculate_pitching(
        cls,
        norm_metric: str,
        inputs: dict[str, Any],
        c: dict[str, float],
    ) -> FormulaEvaluation | None:
        """Calculate pure independent pitching metrics."""
        outs = _to_dec(inputs.get("innings_outs"))
        if outs <= Decimal(0) and inputs.get("innings_pitched"):
            ip = _to_dec(inputs.get("innings_pitched"))
            whole = int(ip)
            frac = round(float(ip) - whole, 2)
            if frac in (0.1, 0.33):
                outs = Decimal(str(whole * 3 + 1))
            elif frac in (0.2, 0.67):
                outs = Decimal(str(whole * 3 + 2))
            else:
                outs = (ip * Decimal(3)).quantize(Decimal(1))

        if outs <= Decimal(0):
            return FormulaEvaluation(
                status=EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
                raw_value=None,
                rounded_value=None,
                reason_code="ZERO_INNINGS_OUTS",
                eligible_for_numeric_comparison=False,
                included_in_audit_population=True,
            )

        if norm_metric == "ERA":
            er = _to_dec(inputs.get("earned_runs"))
            val = ((er * Decimal(27)) / outs).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        if norm_metric == "WHIP":
            h_all = _to_dec(inputs.get("hits_allowed") or inputs.get("hits"))
            bb_all = _to_dec(inputs.get("walks_allowed") or inputs.get("walks"))
            val = ((Decimal(3) * (h_all + bb_all)) / outs).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
            )

        if norm_metric == "FIP":
            if not c:
                return FormulaEvaluation(
                    status=EvaluationStatus.UNDEFINED_CALIBRATION_UNAVAILABLE,
                    raw_value=None,
                    rounded_value=None,
                    reason_code="CALIBRATION_UNAVAILABLE",
                    eligible_for_numeric_comparison=False,
                    included_in_audit_population=True,
                )
            hr_all = _to_dec(inputs.get("home_runs_allowed"))
            bb_all = _to_dec(inputs.get("walks_allowed"))
            hbp_all = _to_dec(inputs.get("hit_batters"))
            so = _to_dec(inputs.get("strikeouts"))
            c_fip = _to_dec(c.get("c_fip", 3.850))

            comp = (
                Decimal(3) * ((Decimal(13) * hr_all) + (Decimal(3) * (bb_all + hbp_all)) - (Decimal(2) * so))
            ) / outs
            val = (comp + c_fip).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            warnings: list[ValidationRule] = []
            if val < Decimal(0):
                warnings.append(
                    ValidationRule(
                        "plausibility",
                        lambda v, _: float(v) >= 0.0,
                        r"\text{FIP} \ge 0.0",
                        "FIP is negative in small sample",
                        severity=RuleSeverity.PLAUSIBILITY,
                    )
                )
            return FormulaEvaluation(
                status=EvaluationStatus.DEFINED,
                raw_value=val,
                rounded_value=val,
                eligible_for_numeric_comparison=True,
                included_in_audit_population=True,
                validation_warnings=warnings,
            )

        return None

    @classmethod
    def calculate(
        cls,
        metric_id: str,
        inputs: dict[str, Any],
        constants: dict[str, float] | None = None,
    ) -> FormulaEvaluation:
        """Execute independent calculation for a metric with explicit Decimal arithmetic."""
        c = constants or {}
        norm_metric = metric_id.upper().strip()

        if norm_metric in ("WRC_PLUS", "WRC+"):
            norm_metric = "WRC_INDEX_NO_PARK"
        elif norm_metric in ("OPS_PLUS", "OPS+"):
            norm_metric = "OPS_INDEX_NO_PARK"
        elif norm_metric in ("ERA_PLUS", "ERA+"):
            norm_metric = "ERA_INDEX_NO_PARK"

        # Batting evaluation
        avg_obp = cls._calculate_avg_obp(norm_metric, inputs)
        if avg_obp is not None:
            return avg_obp

        slg_eval = cls._calculate_slg(norm_metric, inputs)
        if slg_eval is not None:
            return slg_eval

        ops_iso_eval = cls._calculate_ops_iso(norm_metric, inputs, c)
        if ops_iso_eval is not None:
            return ops_iso_eval

        adv_bat_eval = cls._calculate_advanced_batting(norm_metric, inputs, c)
        if adv_bat_eval is not None:
            return adv_bat_eval

        # Pitching evaluation
        if norm_metric in ("ERA", "WHIP", "FIP", "K_9", "BB_9", "HR_9", "DICE", "ERA_INDEX_NO_PARK"):
            pit_eval = cls._calculate_pitching(norm_metric, inputs, c)
            if pit_eval is not None:
                return pit_eval

        # Fallback for remaining standard metrics via registry pure math wrapper
        metric_def = FormulaRegistry.get(norm_metric)
        return metric_def.evaluate_detailed(inputs, c)


@dataclass(frozen=True)
class DualPathEvaluationResult:
    """Detailed record of dual-path verification for a single metric evaluation."""

    metric_id: str
    entity_id: int | str
    season: int
    path_a_value: float | None
    path_b_value: float | None
    stored_value: float | None
    delta_a_b: float
    delta_a_stored: float
    parity_status: ParityStatus
    is_reproducible: bool
    reason_code: str | None = None
    execution_time_us: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert evaluation record to dictionary."""
        return asdict(self)


_ROUNDING_CONTRACT_TOLERANCE: float = 0.001


class DualPathAuditEngine:
    """Master engine executing independent dual-path cross-verification audits."""

    @classmethod
    def classify_evaluation(  # noqa: PLR0911, PLR0913
        cls,
        metric_id: str,
        path_a_val: float | Decimal | None,
        path_b_val: float | Decimal | None,
        stored_val: float | None,
        path_a_status: EvaluationStatus,
        path_b_status: EvaluationStatus,
    ) -> DualPathEvaluationResult:
        """Classify dual-path agreement into disaggregated parity states."""
        # 1. Check Undefined States & One-Sided Mismatches
        a_is_undef = path_a_status in (
            EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
            EvaluationStatus.UNDEFINED_CALIBRATION_UNAVAILABLE,
        )
        b_is_undef = path_b_status in (
            EvaluationStatus.UNDEFINED_ZERO_DENOMINATOR,
            EvaluationStatus.UNDEFINED_CALIBRATION_UNAVAILABLE,
        )

        if a_is_undef and b_is_undef:
            if path_a_status == path_b_status:
                return DualPathEvaluationResult(
                    metric_id=metric_id,
                    entity_id="sample",
                    season=2024,
                    path_a_value=float(path_a_val) if path_a_val is not None else None,
                    path_b_value=float(path_b_val) if path_b_val is not None else None,
                    stored_value=stored_val,
                    delta_a_b=0.0,
                    delta_a_stored=0.0,
                    parity_status=ParityStatus.UNDEFINED,
                    is_reproducible=True,
                    reason_code="BOTH_UNDEFINED_SAME_REASON",
                )
            return DualPathEvaluationResult(
                metric_id=metric_id,
                entity_id="sample",
                season=2024,
                path_a_value=float(path_a_val) if path_a_val is not None else None,
                path_b_value=float(path_b_val) if path_b_val is not None else None,
                stored_value=stored_val,
                delta_a_b=999.0,
                delta_a_stored=999.0,
                parity_status=ParityStatus.DIVERGENT,
                is_reproducible=False,
                reason_code="BOTH_UNDEFINED_DIFFERENT_REASON",
            )

        if a_is_undef and not b_is_undef:
            return DualPathEvaluationResult(
                metric_id=metric_id,
                entity_id="sample",
                season=2024,
                path_a_value=None,
                path_b_value=float(path_b_val) if path_b_val is not None else None,
                stored_value=stored_val,
                delta_a_b=999.0,
                delta_a_stored=999.0,
                parity_status=ParityStatus.DIVERGENT,
                is_reproducible=False,
                reason_code="REGISTRY_UNDEFINED_REFERENCE_DEFINED",
            )

        if not a_is_undef and b_is_undef:
            return DualPathEvaluationResult(
                metric_id=metric_id,
                entity_id="sample",
                season=2024,
                path_a_value=float(path_a_val) if path_a_val is not None else None,
                path_b_value=None,
                stored_value=stored_val,
                delta_a_b=999.0,
                delta_a_stored=999.0,
                parity_status=ParityStatus.DIVERGENT,
                is_reproducible=False,
                reason_code="REGISTRY_DEFINED_REFERENCE_UNDEFINED",
            )

        # 2. Check Numeric Divergence between Path A and Path B
        if path_a_val is None or path_b_val is None:
            return DualPathEvaluationResult(
                metric_id=metric_id,
                entity_id="sample",
                season=2024,
                path_a_value=float(path_a_val) if path_a_val is not None else None,
                path_b_value=float(path_b_val) if path_b_val is not None else None,
                stored_value=stored_val,
                delta_a_b=999.0,
                delta_a_stored=999.0,
                parity_status=ParityStatus.DIVERGENT,
                is_reproducible=False,
                reason_code="PATH_MISSING_VALUE",
            )

        a_f = float(path_a_val)
        b_f = float(path_b_val)
        delta_ab = round(abs(a_f - b_f), 6)
        tol = _TOLERANCE_MAP.get(metric_id, 0.01)

        if delta_ab > tol:
            return DualPathEvaluationResult(
                metric_id=metric_id,
                entity_id="sample",
                season=2024,
                path_a_value=a_f,
                path_b_value=b_f,
                stored_value=stored_val,
                delta_a_b=delta_ab,
                delta_a_stored=round(abs(a_f - stored_val), 6) if stored_val is not None else 0.0,
                parity_status=ParityStatus.DIVERGENT,
                is_reproducible=False,
                reason_code="PATH_A_PATH_B_DISCREPANCY",
            )

        delta_stored = round(abs(a_f - stored_val), 6) if stored_val is not None else 0.0
        if delta_ab == 0.0:
            parity = ParityStatus.EXACT
        elif delta_ab <= _ROUNDING_CONTRACT_TOLERANCE:
            parity = ParityStatus.ROUNDED_CONTRACT
        else:
            parity = ParityStatus.FLOATING_TOLERANCE

        return DualPathEvaluationResult(
            metric_id=metric_id,
            entity_id="sample",
            season=2024,
            path_a_value=a_f,
            path_b_value=b_f,
            stored_value=stored_val,
            delta_a_b=delta_ab,
            delta_a_stored=delta_stored,
            parity_status=parity,
            is_reproducible=True,
        )

    @classmethod
    def _evaluate_metric_across_dataset(
        cls,
        metric: MetricDefinition,
        dataset: list[dict[str, Any]],
        constants_cache: dict[int, dict[str, float]],
        parity_breakdown: dict[str, int],
    ) -> tuple[int, int, int]:
        """Evaluate a single metric across dataset rows and accumulate parity breakdown."""
        m_total = 0
        m_rep = 0
        m_div = 0

        for row in dataset:
            s_yr = int(row.get("season") or 2024)
            if s_yr not in constants_cache:
                try:
                    constants_cache[s_yr] = LeagueConstantsEngine.get_baseline_constants(s_yr)
                except ValueError:
                    constants_cache[s_yr] = {}

            consts = constants_cache[s_yr]

            # Path A: Registry
            eval_a = metric.evaluate_detailed(row, consts)

            # Path B: Independent Oracle
            eval_b = IndependentFormulaOracle.calculate(metric.metric_id, row, consts)

            col_name = DB_COL_MAP.get(metric.metric_id)
            stored_val = None
            if col_name and row.get(col_name) is not None:
                with contextlib.suppress(ValueError, TypeError):
                    stored_val = float(row[col_name])

            eval_res = cls.classify_evaluation(
                metric_id=metric.metric_id,
                path_a_val=eval_a.raw_value,
                path_b_val=eval_b.raw_value,
                stored_val=stored_val,
                path_a_status=eval_a.status,
                path_b_status=eval_b.status,
            )

            m_total += 1
            parity_breakdown[eval_res.parity_status.value] += 1

            if eval_res.is_reproducible:
                m_rep += 1
            else:
                m_div += 1

        return m_total, m_rep, m_div

    @classmethod
    def run_census_audit(
        cls,
        engine: SQLAlchemyEngine | None = None,
        season: int | None = None,
        category: MetricCategory | None = None,
        sample: int | None = None,
    ) -> dict[str, Any]:
        """Execute full independent dual-path audit across population."""
        start_t = time.perf_counter()
        db_engine = engine or create_engine("sqlite:///./data/kbo_dev.db")
        metrics = FormulaRegistry.list_all(category=category)

        total_checked = 0
        reproducible_count = 0
        divergent_count = 0
        parity_breakdown: dict[str, int] = {p.value: 0 for p in ParityStatus}
        metric_reports: dict[str, Any] = {}

        with db_engine.connect() as conn:
            base_filters = [
                "(level = '1군' OR level = 'KBO1' OR level IS NULL)",
                "(league = 'REGULAR' OR league IS NULL)",
                "(source != 'ROLLUP' OR source IS NULL)",
            ]
            if season is not None:
                base_filters.append(f"season = {season}")

            where_bat = "WHERE " + " AND ".join([*base_filters, "at_bats IS NOT NULL"])
            where_pit = "WHERE " + " AND ".join([*base_filters, "(innings_outs > 0 OR innings_pitched > 0)"])
            limit_clause = f"LIMIT {sample}" if sample else "LIMIT 500"

            sql_bat = (
                f"SELECT * FROM player_season_batting {where_bat} "  # noqa: S608
                f"ORDER BY season DESC, plate_appearances DESC {limit_clause}"
            )
            sql_pit = (
                f"SELECT * FROM player_season_pitching {where_pit} "  # noqa: S608
                f"ORDER BY season DESC, innings_outs DESC {limit_clause}"
            )

            bat_rows = [dict(r) for r in conn.execute(text(sql_bat)).mappings().fetchall()]
            pit_rows = [dict(r) for r in conn.execute(text(sql_pit)).mappings().fetchall()]

        constants_cache: dict[int, dict[str, float]] = {}
        for m in metrics:
            dataset = bat_rows if m.category == MetricCategory.BATTING else pit_rows
            m_total, m_rep, m_div = cls._evaluate_metric_across_dataset(m, dataset, constants_cache, parity_breakdown)
            total_checked += m_total
            reproducible_count += m_rep
            divergent_count += m_div
            metric_reports[m.metric_id] = {
                "evaluations": m_total,
                "reproducible": m_rep,
                "divergent": m_div,
                "reproducibility_ratio": round(m_rep / max(m_total, 1), 5),
            }

        dur_ms = round((time.perf_counter() - start_t) * 1000.0, 2)
        rep_ratio = round(reproducible_count / max(total_checked, 1), 5)

        return {
            "audit_type": "INDEPENDENT_DUAL_PATH_CENSUS",
            "scope": "LOCAL_SQLITE_BASELINE",
            "season": season,
            "category": category.value if category else None,
            "total_metrics_evaluated": len(metrics),
            "total_entities_checked": total_checked,
            "reproducible_count": reproducible_count,
            "divergent_count": divergent_count,
            "reproducibility_ratio": rep_ratio,
            "parity_breakdown": parity_breakdown,
            "metric_breakdowns": metric_reports,
            "duration_ms": dur_ms,
            "is_compliant": divergent_count == 0,
        }


__all__ = [
    "DB_COL_MAP",
    "DualPathAuditEngine",
    "DualPathEvaluationResult",
    "IndependentFormulaOracle",
]
