"""Formatting, ASCII Tree, and JSON Rendering Utilities for Formula Registry."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.formulas.models import (
        FormulaAuditReport,
        MetricDefinition,
        MetricEvaluationResult,
    )

MAX_LATEX_LEN: int = 20


class FormulaReporter:
    """Renders sabermetric formula catalog, LaTeX specifications, and audit reports."""

    @classmethod
    def render_catalog(cls, metrics: list[MetricDefinition]) -> str:
        """Render registered formula catalog as a formatted ASCII table."""
        lines = [
            "╔═══════════════════════════════════════════════════════════════════════════════════════════════════╗",
            "║ 📐 KBO SABERMETRICS FORMULA REGISTRY CATALOG                                                      ║",
            "╠══════════════╦══════════════════════════════════════╦══════════════╦═════════╦══════════════════════╣",
            "║ Metric ID    ║ Name / Korean                        ║ Category     ║ Version ║ LaTeX Formula        ║",
            "╠══════════════╬══════════════════════════════════════╬══════════════╬═════════╬══════════════════════╣",
        ]

        for m in metrics:
            k_name = f"{m.name} ({m.korean_name})"
            lat = m.latex_formula
            if len(lat) > MAX_LATEX_LEN:
                lat = lat[:17] + "..."
            lines.append(
                f"║ {m.metric_id:<12} ║ {k_name:<36} ║ {m.category.value:<12} ║ {m.version.version:<7} ║ {lat:<20} ║"
            )

        lines.append(
            "╚══════════════╩══════════════════════════════════════╩══════════════╩═════════╩══════════════════════╝"
        )
        return "\n".join(lines)

    @classmethod
    def render_explanation(cls, metric: MetricDefinition, constants: dict[str, float] | None = None) -> str:
        """Render comprehensive mathematical card and constant calibration for a specific metric."""
        c_dict = constants or {}
        lines = [
            f"📍 Metric Specification: {metric.metric_id} — {metric.name} ({metric.korean_name})",
            f"   Category: {metric.category.value} | Specification Version: {metric.version.version}",
            "   ├── 📐 Mathematical Formulation",
            f"   │   ├── LaTeX: {metric.latex_formula}",
            f"   │   └── Description: {metric.description}",
            "   ├── 📥 Input Variables",
            f"   │   └── Required Fields: {', '.join(metric.input_fields)}",
        ]

        if metric.constants_required:
            lines.append("   ├── ⚙️ Environmental Constants & Linear Weights")
            for c_key in metric.constants_required:
                val = c_dict.get(c_key, "Dynamic/Uncalibrated")
                lines.append(f"   │   ├── {c_key}: {val}")
        else:
            lines.append("   ├── ⚙️ Environmental Constants: None (Pure Classical Metric)")

        if metric.validation_rules:
            lines.append("   ├── 🛡️ Mathematical Invariant Gates")
            lines.extend(f"   │   ├── {r.name}: {r.latex_repr} ({r.error_message})" for r in metric.validation_rules)
        else:
            lines.append("   ├── 🛡️ Mathematical Invariant Gates: Standard Non-Negative")

        return "\n".join(lines)

    @classmethod
    def render_player_eval(cls, res: MetricEvaluationResult, metric_def: MetricDefinition) -> str:
        """Render step-by-step mathematical evaluation and DB parity check for a player."""
        icon = "✅ REPRODUCIBLE" if res.is_reproducible else "❌ DIVERGENT"
        inv_icon = "✅ PASS" if res.invariants_passed else "⚠️ INVARIANT VIOLATION"
        st_val = res.stored_value if res.stored_value is not None else "N/A (Derived Only)"

        lines = [
            f"📍 Metric Evaluation: {res.metric_id} on Player ID {res.entity_id} (Season {res.season})",
            f"   Metric: {metric_def.name} ({metric_def.korean_name})",
            "   ├── 📐 Formula Derivation",
            f"   │   ├── LaTeX: {metric_def.latex_formula}",
            f"   │   ├── Calculated Value: {res.calculated_value}",
            f"   │   ├── Stored DB Value:  {st_val}",
            f"   │   ├── Delta:            {res.delta}",
            f"   │   └── Parity Status:    {icon}",
            "   ├── 📥 Inputs Ingested",
        ]

        for k in metric_def.input_fields:
            v = res.inputs_used.get(k, "0")
            lines.append(f"   │   ├── {k}: {v}")

        if metric_def.constants_required:
            lines.append("   ├── ⚙️ Calibrated Constants Used")
            for c_key in metric_def.constants_required:
                c_val = res.constants_used.get(c_key, "N/A")
                lines.append(f"   │   ├── {c_key}: {c_val}")

        lines.extend(
            [
                "   └── 🛡️ Invariant Check",
                f"       └── Status: {inv_icon} (Latency: {res.execution_time_us:.1f} µs)",
            ]
        )

        return "\n".join(lines)

    @classmethod
    def render_audit_report(cls, report: FormulaAuditReport) -> str:
        """Render full metric reproducibility certification audit banner."""
        ratio_pct = report.reproducibility_ratio * 100.0
        status_str = "✅ REPRODUCIBLE (100.0%)" if report.is_compliant else "⚠️ DIVERGENCES DETECTED"
        s_str = str(report.season) if report.season else "ALL SEASONS"

        lines = [
            "╔════════════════════════════════════════════════════════════════════════════════╗",
            f"║ 🧭 KBO SABERMETRICS FORMULA REPRODUCIBILITY AUDIT (SEASON: {s_str:<17}) ║",
            "╠════════════════════════════════════════════════════════════════════════════════╣",
            f"║ Total Metrics Evaluated:     {report.total_metrics_evaluated:<48} ║",
            f"║ Total Entities Checked:      {report.total_entities_checked:<48} ║",
            f"║ Exact / Tolerant Matches:    {report.reproducible_count:<48} ║",
            f"║ Divergent Calculations:      {report.divergent_count:<48} ║",
            f"║ Reproducibility Ratio:       {ratio_pct:>7.3f}%{' ' * 40} ║",
            f"║ Duration:                    {report.duration_ms:>7.2f} ms{' ' * 38} ║",
            f"║ Compliance Status:           {status_str:<48} ║",
            f"║ Git SHA / Evidence Checksum: {report.git_sha} | {report.sha256_checksum[:16]:<25} ║",
            "╠════════════════════════════════════════════════════════════════════════════════╣",
            "║ Metric Reproducibility Breakdown:                                             ║",
        ]

        for m_id, stats in report.metric_breakdowns.items():
            ev = stats["evaluations"]
            rep = stats["reproducible"]
            r_pct = stats["reproducibility_ratio"] * 100.0
            info = f"{ev:>4} evals | {rep:>4} match ({r_pct:5.1f}%)"
            lines.append(f"║ ├── {m_id:<20}: {info:<51} ║")

        lines.append("╚════════════════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    @classmethod
    def render_json(cls, data: object) -> str:
        """Serialize object to formatted JSON."""
        if hasattr(data, "to_dict"):
            return json.dumps(data.to_dict(), indent=2, ensure_ascii=False)
        return json.dumps(data, indent=2, ensure_ascii=False)


__all__ = [
    "FormulaReporter",
]
