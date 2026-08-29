"""Visualization and artifact export for KBO Production Certification reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.certification.models import CertificationReport


class CertificationReporter:
    """Renders terminal ASCII certification cards and writes JSON evidence artifacts."""

    @staticmethod
    def render_ascii_card(report: CertificationReport) -> str:
        """Render a terminal ASCII certification dashboard card."""
        if report.status == "CERTIFIED":
            status_icon = "🌟"
        elif report.status == "CERTIFIED_WITH_WARNINGS":
            status_icon = "⚠️"
        else:
            status_icon = "❌"

        lines = [
            "╔════════════════════════════════════════════════════════════════════╗",
            "║ 🛡️ KBO PLATFORM PRODUCTION CERTIFICATION REPORT                    ║",
            f"║ Run ID: {report.run_id:<20} | Target: {report.target.upper():<10} | Git: {report.git_revision:<8}║",
            "╠════════════════════════════════════════════════════════════════════╣",
        ]

        for idx, g in enumerate(report.gates, start=1):
            if g.status.value == "PASS":
                icon = "✅ PASS"
            elif g.status.value == "WARN":
                icon = "⚠️ WARN"
            elif g.status.value == "SKIP":
                icon = "⏭️ SKIP"
            else:
                icon = "❌ FAIL"

            detail = g.message or ""
            if not detail and g.metrics:
                first_items = list(g.metrics.items())[:2]
                detail = ", ".join(f"{k}={v}" for k, v in first_items)

            line_str = f"║ [{idx}] {g.name:<32} {icon:<8} ({detail[:20]})"
            lines.append(line_str.ljust(68) + "║")

        elapsed_sec = report.total_duration_ms / 1000.0
        summary_stats = (
            f"Failures: {report.blocking_failures:<3} | Warnings: {report.warnings:<3} | Elapsed: {elapsed_sec:.2f}s"
        )
        lines.extend(
            [
                "╠════════════════════════════════════════════════════════════════════╣",
                f"║ {summary_stats}".ljust(68) + "║",
                f"║ {status_icon} STATUS: {report.status}".ljust(68) + "║",
                "╚════════════════════════════════════════════════════════════════════╝",
            ]
        )
        return "\n".join(lines)

    @staticmethod
    def save_json_report(report: CertificationReport, output_path: Path | str) -> Path:
        """Save machine-readable certification report with verifiable evidence to disk."""
        target_path = Path(output_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
        return target_path


__all__ = [
    "CertificationReporter",
]
