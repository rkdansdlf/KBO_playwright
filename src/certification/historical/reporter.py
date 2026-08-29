"""Historical Certification Reporter rendering 45-season ASCII matrix and JSON artifacts."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from src.certification.historical.models import (
        HistoricalAuditReport,
        SeasonAuditResult,
    )

_MATRIX_CONDENSE_THRESHOLD = 16


def _fmt_status(st: str) -> str:
    """Format single invariant layer status for ASCII table."""
    if st == "PASS":
        return "PASS "
    if st in {"PASS_WITH_EXCEPTION", "N_A", "AS_OF_CUTOFF", "NOT_COMPARABLE"}:
        return "N/A* " if st in {"N_A", "NOT_COMPARABLE"} else "WARN*"
    if st == "FAIL":
        return "FAIL "
    return "SKIP "


def _fmt_verdict(v: str) -> str:
    """Format season overall verdict for ASCII table."""
    if v == "PASS":
        return "PASS   "
    if v == "PASS_WITH_DECLARED_EXCEPTIONS":
        return "PASS*  "
    if v == "FAIL":
        return "FAIL   "
    return "SKIP   "


def _render_season_row(s_res: SeasonAuditResult) -> str:
    """Render a single season row for the matrix table."""
    h01 = _fmt_status(s_res.layer_status.get("H01", "PASS"))
    h02 = _fmt_status(s_res.layer_status.get("H02", "PASS"))
    h03 = _fmt_status(s_res.layer_status.get("H03", "PASS"))
    h04 = _fmt_status(s_res.layer_status.get("H04", "PASS"))
    h05 = _fmt_status(s_res.layer_status.get("H05", "PASS"))
    h06 = _fmt_status(s_res.layer_status.get("H06", "PASS"))
    h07 = _fmt_status(s_res.layer_status.get("H07", "PASS"))
    verd = _fmt_verdict(s_res.verdict.value)
    st_text = f"{s_res.status.value[:7]:<7}"
    return f"║ {s_res.season} │ {st_text} │ {h01} │ {h02} │ {h03} │ {h04} │ {h05} │ {h06} │ {h07} │ {verd} ║"


class HistoricalReporter:
    """Formatter for terminal matrix scorecards and JSON certification artifacts."""

    @classmethod
    def render_ascii_matrix(cls, report: HistoricalAuditReport, *, verbose: bool = False) -> str:
        """Render terminal ASCII matrix table of all audited seasons."""
        dur_s = report.total_duration_ms / 1000.0
        header_meta = (
            f"Run ID: {report.run_id[:16]:<16} | Target: {report.target.upper():<7} | "
            f"Git: {report.git_revision[:8]:<8} | Elapsed: {dur_s:4.2f}s"
        )
        lines = [
            "╔════════════════════════════════════════════════════════════════════════════════════════════╗",
            "║ 🏛️  KBO HISTORICAL DATA CERTIFICATION MATRIX (45 SEASONS: 1982~2026)                       ║",
            f"║ {header_meta:<90} ║",
            "╠════════════════════════════════════════════════════════════════════════════════════════════╣",
            "║ Season │ Status  │ H01   │ H02   │ H03   │ H04   │ H05   │ H06   │ H07   │ Verdict ║",
            "╟────────┼─────────┼───────┼───────┼───────┼───────┼───────┼───────┼───────┼─────────╢",
        ]

        seasons = report.seasons
        if not verbose and len(seasons) > _MATRIX_CONDENSE_THRESHOLD:
            lines.extend([_render_season_row(s) for s in seasons[:5]])
            lines.append("║  ...   │   ...   │  ...  │  ...  │  ...  │  ...  │  ...  │  ...  │  ...  │   ...   ║")
            lines.extend([_render_season_row(s) for s in seasons[-5:]])
        else:
            lines.extend([_render_season_row(s) for s in seasons])

        summary_meta = (
            f"Total: {report.total_seasons:<3} │ Passed: {report.passed_seasons:<3} │ "
            f"Passed(Exc): {report.passed_with_exceptions:<3} │ Failed: {report.failed_seasons:<3} │ "
            f"Violations: {report.total_violations:<4}"
        )
        lines.extend(
            [
                "╠════════════════════════════════════════════════════════════════════════════════════════════╣",
                f"║ {summary_meta:<90} ║",
                f"║ 🏁 OVERALL HISTORICAL VERDICT: {report.overall_verdict:<58} ║",
                "╚════════════════════════════════════════════════════════════════════════════════════════════╝",
            ]
        )

        return "\n".join(lines)

    @classmethod
    def save_json_report(cls, report: HistoricalAuditReport, target_path: Path) -> Path:
        """Serialize historical report to file as formatted JSON."""
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False)
        return target_path


__all__ = [
    "HistoricalReporter",
]
