"""Unified Reporting Engine for Data Quality, Gaps, Freshness, and Executive Dashboards."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.engine import Engine, get_db_session
from src.reporting.dto import (
    ReportCategory,
    ReportFormat,
    ReportSection,
    UnifiedExecutiveReport,
)

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SqlEngine
    from sqlalchemy.orm import Session

    from src.reporting.scouting_dto import ScoutingReport

logger = logging.getLogger(__name__)


class ReportingEngine:
    """Orchestrates comprehensive reporting across data quality, gaps, freshness, and dashboards."""

    def __init__(self, engine: SqlEngine | None = None) -> None:
        """Initialize reporting engine."""
        self.engine = engine or Engine

    def generate_quality_report(
        self,
        year: int | None = None,
        session: Session | None = None,
    ) -> UnifiedExecutiveReport:
        """Generate a data quality report checking PA invariants and player completeness."""
        report_id = f"quality_report_{year or 'all'}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        title = f"KBO Data Quality Intelligence Report ({year or 'All Seasons'})"
        sections: list[ReportSection] = []
        overall_status = "PASS"

        def _audit(s: Session) -> dict[str, int]:
            metrics = {"total_batting_rows": 0, "pa_violations": 0, "null_player_ids": 0}
            try:
                r1 = s.execute(text("SELECT COUNT(*) FROM game_batting_stats")).fetchone()
                metrics["total_batting_rows"] = int(r1[0]) if r1 else 0

                sql_pa = """
                SELECT COUNT(*) FROM game_batting_stats
                WHERE plate_appearances != (
                    at_bats + walks + COALESCE(hbp, 0) + COALESCE(sacrifice_hits, 0) + COALESCE(sacrifice_flies, 0)
                )
                """
                r2 = s.execute(text(sql_pa)).fetchone()
                metrics["pa_violations"] = int(r2[0]) if r2 else 0

                sql_null = "SELECT COUNT(*) FROM game_batting_stats WHERE player_id IS NULL"
                r3 = s.execute(text(sql_null)).fetchone()
                metrics["null_player_ids"] = int(r3[0]) if r3 else 0
            except SQLAlchemyError:
                pass
            return metrics

        if session is not None:
            data = _audit(session)
        else:
            with get_db_session() as s:
                data = _audit(s)

        pa_status = "PASS" if data["pa_violations"] == 0 else "FAIL"
        if pa_status == "FAIL":
            overall_status = "FAIL"
        sections.append(
            ReportSection(
                title="PA Formula Integrity",
                content_markdown=(
                    f"- Total Batting Records: {data['total_batting_rows']:,}\n"
                    f"- PA Invariant Violations: {data['pa_violations']:,}\n"
                    f"- Compliance Rate: "
                    f"{(1.0 - (data['pa_violations'] / max(1, data['total_batting_rows']))) * 100:.2f}%"
                ),
                metrics={"pa_violations": data["pa_violations"]},
                status=pa_status,
            )
        )

        null_status = "PASS" if data["null_player_ids"] == 0 else "WARN"
        if null_status == "WARN" and overall_status == "PASS":
            overall_status = "WARN"
        sections.append(
            ReportSection(
                title="Player Registry Resolution",
                content_markdown=f"- Unresolved NULL Player IDs: {data['null_player_ids']:,}",
                metrics={"null_player_ids": data["null_player_ids"]},
                status=null_status,
            )
        )

        return UnifiedExecutiveReport(
            report_id=report_id,
            category=ReportCategory.QUALITY_GATE,
            title=title,
            generated_at=datetime.now(UTC).isoformat(),
            overall_status=overall_status,
            summary_metrics=data,
            sections=sections,
        )

    def generate_gap_report(self, session: Session | None = None) -> UnifiedExecutiveReport:
        """Generate a gap analysis report analyzing missing games, lineups, and events."""
        report_id = f"gap_report_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        title = "KBO Data Lake Completeness & Gap Analysis Report"
        sections: list[ReportSection] = []

        def _audit_gaps(s: Session) -> dict[str, int]:
            metrics = {"total_games": 0, "games_missing_lineups": 0, "games_missing_pbp": 0}
            try:
                r1 = s.execute(text("SELECT COUNT(*) FROM game")).fetchone()
                metrics["total_games"] = int(r1[0]) if r1 else 0

                sql_missing_lineups = """
                SELECT COUNT(*) FROM game g
                WHERE g.game_status = 'COMPLETED'
                  AND NOT EXISTS (SELECT 1 FROM game_lineups l WHERE l.game_id = g.game_id)
                """
                r2 = s.execute(text(sql_missing_lineups)).fetchone()
                metrics["games_missing_lineups"] = int(r2[0]) if r2 else 0

                sql_missing_pbp = """
                SELECT COUNT(*) FROM game g
                WHERE g.game_status = 'COMPLETED'
                  AND NOT EXISTS (SELECT 1 FROM game_play_by_play p WHERE p.game_id = g.game_id)
                """
                r3 = s.execute(text(sql_missing_pbp)).fetchone()
                metrics["games_missing_pbp"] = int(r3[0]) if r3 else 0
            except SQLAlchemyError:
                pass
            return metrics

        if session is not None:
            data = _audit_gaps(session)
        else:
            with get_db_session() as s:
                data = _audit_gaps(s)

        lineup_status = "PASS" if data["games_missing_lineups"] == 0 else "WARN"
        sections.append(
            ReportSection(
                title="Lineup Coverage",
                content_markdown=(
                    f"- Total Completed Games: {data['total_games']:,}\n"
                    f"- Games Missing Lineups: {data['games_missing_lineups']:,}"
                ),
                metrics={"missing_lineups": data["games_missing_lineups"]},
                status=lineup_status,
            )
        )

        pbp_status = "PASS" if data["games_missing_pbp"] == 0 else "INFO"
        sections.append(
            ReportSection(
                title="Play-by-Play Relay Coverage",
                content_markdown=f"- Games Missing Play-by-Play: {data['games_missing_pbp']:,}",
                metrics={"missing_pbp": data["games_missing_pbp"]},
                status=pbp_status,
            )
        )

        overall = "WARN" if lineup_status == "WARN" else "PASS"

        return UnifiedExecutiveReport(
            report_id=report_id,
            category=ReportCategory.GAP_ANALYSIS,
            title=title,
            generated_at=datetime.now(UTC).isoformat(),
            overall_status=overall,
            summary_metrics=data,
            sections=sections,
        )

    def generate_freshness_report(self, session: Session | None = None) -> UnifiedExecutiveReport:
        """Generate a data freshness report on core database tables."""
        report_id = f"freshness_report_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        title = "KBO Data Lake Freshness & Ingestion Latency Report"
        sections: list[ReportSection] = []

        def _audit_freshness(s: Session) -> dict[str, int]:
            metrics: dict[str, int] = {}
            for tbl in ("game", "player_basic", "rag_chunks", "team_standings_daily"):
                try:
                    r = s.execute(text(f"SELECT COUNT(*) FROM {tbl}")).fetchone()  # noqa: S608
                    metrics[tbl] = int(r[0]) if r else 0
                except SQLAlchemyError:
                    metrics[tbl] = 0
            return metrics

        if session is not None:
            data = _audit_freshness(session)
        else:
            with get_db_session() as s:
                data = _audit_freshness(s)

        md_lines = [f"- `{tbl}`: {cnt:,} records" for tbl, cnt in data.items()]
        sections.append(
            ReportSection(
                title="Core Table Volumes",
                content_markdown="\n".join(md_lines),
                metrics=data,
                status="PASS",
            )
        )

        return UnifiedExecutiveReport(
            report_id=report_id,
            category=ReportCategory.FRESHNESS,
            title=title,
            generated_at=datetime.now(UTC).isoformat(),
            overall_status="PASS",
            summary_metrics=data,
            sections=sections,
        )

    def generate_executive_dashboard(self, session: Session | None = None) -> UnifiedExecutiveReport:
        """Generate an aggregated executive dashboard report."""
        report_id = f"dashboard_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        title = "KBO Platform Executive Quality & Operations Dashboard"
        sections: list[ReportSection] = []

        q_rep = self.generate_quality_report(session=session)
        g_rep = self.generate_gap_report(session=session)
        f_rep = self.generate_freshness_report(session=session)

        sections.extend(q_rep.sections)
        sections.extend(g_rep.sections)
        sections.extend(f_rep.sections)

        combined_metrics = {
            **q_rep.summary_metrics,
            **g_rep.summary_metrics,
            **f_rep.summary_metrics,
        }

        overall = "PASS"
        if any(s.status == "FAIL" for s in sections):
            overall = "FAIL"
        elif any(s.status == "WARN" for s in sections):
            overall = "WARN"

        return UnifiedExecutiveReport(
            report_id=report_id,
            category=ReportCategory.EXECUTIVE_DASHBOARD,
            title=title,
            generated_at=datetime.now(UTC).isoformat(),
            overall_status=overall,
            summary_metrics=combined_metrics,
            sections=sections,
        )

    def generate_scouting_report(
        self,
        player_name_or_id: str | int = "김도영",
        year: int | None = None,
        session: Session | None = None,
    ) -> UnifiedExecutiveReport:
        """Generate a 5-axis sabermetric scouting report for a player."""
        target_year = year or 2024
        report_id = f"scouting_{player_name_or_id}_{target_year}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"

        def _run_scout(s: Session) -> ScoutingReport:
            from src.reporting.scouting_engine import ScoutingReportEngine

            scout_engine = ScoutingReportEngine(s)
            return scout_engine.generate_scouting_report(player_name_or_id, year=target_year)

        if session is not None:
            scout_rep = _run_scout(session)
        else:
            with get_db_session() as s:
                scout_rep = _run_scout(s)

        sections = [
            ReportSection(
                title=f"5-Axis Scouting Radar: {scout_rep.player_name} ({scout_rep.overall_grade})",
                content_markdown=f"```\n{scout_rep.to_ascii_card()}\n```",
                metrics={
                    "overall_grade": scout_rep.overall_grade,
                    "tier": scout_rep.scouting_tier,
                    "dimensions": [d.to_dict() for d in scout_rep.dimensions],
                },
                status="PASS",
            ),
            ReportSection(
                title="Detailed Scouting Breakdown",
                content_markdown=scout_rep.to_markdown(),
                metrics={"classic": scout_rep.classic_stats, "advanced": scout_rep.advanced_stats},
                status="PASS",
            ),
        ]

        return UnifiedExecutiveReport(
            report_id=report_id,
            category=ReportCategory.SCOUTING,
            title=f"KBO Scouting Report: {scout_rep.player_name} ({scout_rep.team_code}, {target_year})",
            generated_at=datetime.now(UTC).isoformat(),
            overall_status="PASS",
            summary_metrics={"overall_grade": scout_rep.overall_grade, "tier": scout_rep.scouting_tier},
            sections=sections,
        )

    def render_report(
        self,
        report: UnifiedExecutiveReport,
        format_type: ReportFormat | str = ReportFormat.MARKDOWN,
    ) -> str:
        """Render the executive report in Markdown, JSON, or HTML format."""
        f_enum = ReportFormat(format_type) if isinstance(format_type, str) else format_type

        if f_enum == ReportFormat.JSON:
            return json.dumps(report.to_dict(), indent=2, ensure_ascii=False)

        if f_enum == ReportFormat.HTML:
            html_parts = [
                "<!DOCTYPE html>",
                "<html><head><meta charset='utf-8'>",
                f"<title>{report.title}</title>",
                "<style>",
                "body { font-family: sans-serif; margin: 40px; line-height: 1.6; }",
                ".status { font-weight: bold; }",
                ".PASS { color: green; } .WARN { color: orange; } .FAIL { color: red; }",
                "</style>",
                f"</head><body><h1>{report.title}</h1>",
                (
                    f"<p><strong>Status:</strong> "
                    f"<span class='status {report.overall_status}'>{report.overall_status}</span> | "
                    f"Generated: {report.generated_at}</p>"
                ),
            ]
            for sec in report.sections:
                html_parts.append(f"<h2>{sec.title} [{sec.status}]</h2>")
                html_parts.append(f"<pre>{sec.content_markdown}</pre>")
            html_parts.append("</body></html>")
            return "\n".join(html_parts)

        # Markdown / Text
        md_lines = [
            f"# {report.title}",
            f"**Status**: `{report.overall_status}` | **Generated**: `{report.generated_at}`",
            "",
            "---",
        ]
        for sec in report.sections:
            md_lines.append(f"## {sec.title} [{sec.status}]")
            md_lines.append(sec.content_markdown)
            md_lines.append("")
        return "\n".join(md_lines)
