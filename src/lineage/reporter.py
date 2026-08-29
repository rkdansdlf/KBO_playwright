"""Formatting and Visualization Utilities for Data Lineage Reports."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.lineage.models import (
        GameLineageReport,
        LineageAuditReport,
        LineageGraph,
        PlayerMetricLineageReport,
    )


class LineageReporter:
    """Renders data lineage graphs as ASCII hierarchical trees, Mermaid DAGs, and JSON artifacts."""

    @classmethod
    def render_game_tree(cls, report: GameLineageReport) -> str:
        """Render a formatted ASCII hierarchical tree for a game lineage report."""
        h_sc = report.home_score or 0
        a_sc = report.away_score or 0
        lines = [
            f"📍 Game Lineage: {report.game_id} ({report.game_date})",
            f"   Matchup: {report.home_team} {h_sc} vs {report.away_team} {a_sc} [{report.game_status}]",
            "   ├── 🌐 Source Snapshot",
        ]

        for src in report.sources:
            lines.append(f"   │   ├── Provider: {src.get('source_name')}")
            lines.append(f"   │   └── URL: {src.get('base_url')}")

        bat_rows = report.stored_tables.get("game_batting_stats", 0)
        pitch_rows = report.stored_tables.get("game_pitching_stats", 0)
        pbp_rows = report.stored_tables.get("game_play_by_play", 0)
        lines.extend(
            [
                "   ├── 🕷️ Ingestion Pipeline",
                "   │   ├── Crawler: src.crawlers.game_detail_crawler",
                "   │   └── Parser: GameDetailParser (v2.1)",
                "   ├── 💾 Stored Database Entities",
                f"   │   ├── Table 'game': 1 parent row (PK: {report.game_id})",
                f"   │   ├── Table 'game_batting_stats': {bat_rows} batter rows",
                f"   │   ├── Table 'game_pitching_stats': {pitch_rows} pitcher rows",
                f"   │   └── Table 'game_play_by_play': {pbp_rows} pitch events",
            ]
        )

        if report.corrections:
            lines.append("   ├── 🛠️ Remediation & Patch History")
            for c in report.corrections:
                lines.append(f"   │   ├── Remediation ID: {c.remediation_id}")
                lines.append(f"   │   │   ├── Field: {c.field_name} ({c.original_value} -> {c.corrected_value})")
                lines.append(f"   │   │   └── Reason: {c.reason}")
        else:
            lines.append("   ├── 🛠️ Remediation: None (Clean source extraction)")

        lines.append("   └── 🛡️ Certification Verification")
        for gate, st in report.certification_status.items():
            icon = "✅" if "PASS" in st else "⚠️"
            lines.append(f"       ├── {gate}: {icon} {st}")

        return "\n".join(lines)

    @classmethod
    def render_player_tree(cls, report: PlayerMetricLineageReport) -> str:
        """Render a formatted ASCII hierarchical tree for a player season metric lineage report."""
        cov_pct = report.lineage_coverage * 100.0
        part_pct = report.participation_rate * 100.0
        part_str = f"{part_pct:.1f}% ({report.player_appeared_games}/{report.team_scheduled_games} games)"
        cov_str = (
            f"{cov_pct:.1f}% "
            f"({report.observed_contributing_rows}/{report.expected_contributing_rows} observed vs expected)"
        )
        lines = [
            f"📍 Player Metric Lineage: {report.player_name} (ID: {report.player_id})",
            f"   Season: {report.season} | Metric: {report.metric_name.upper()} = {report.metric_value}",
            "   ├── 📐 Mathematical Derivation",
            f"   │   ├── Formula: {report.formula}",
            f"   │   ├── Player Appeared Games: {report.player_appeared_games} games",
            f"   │   ├── Team Scheduled Games: {report.team_scheduled_games} games",
            f"   │   ├── Participation Rate: {part_str}",
            f"   │   └── Lineage Coverage: {cov_str}",
            "   ├── 🔄 Transformation Chain",
        ]

        lines.extend([f"   │   ├── {step}" for step in report.transformation_chain])

        if report.contributing_rows_sample:
            lines.append("   ├── 📊 Contributing Game Rows (Sample Top-5)")
            for r in report.contributing_rows_sample[:5]:
                gid = r.get("game_id", "N/A")
                h = r.get("hits", 0)
                hr = r.get("home_runs", 0)
                ab = r.get("at_bats", 0)
                lines.append(f"   │   ├── Game {gid}: {h}H / {ab}AB ({hr} HR)")

        lines.append("   └── 🛡️ Invariant Certification")
        for gate, st in report.certification_status.items():
            icon = "✅" if "PASS" in st else "⚠️"
            lines.append(f"       ├── {gate}: {icon} {st}")

        return "\n".join(lines)

    @classmethod
    def render_audit_tree(cls, report: LineageAuditReport) -> str:
        """Render a formatted ASCII tree for a lineage audit report."""
        mode_str = f"MODE: {report.audit_mode}"
        season_str = str(report.season) if report.season else "ALL SEASONS"
        lines = [
            "╔════════════════════════════════════════════════════════════════════════════════╗",
            f"║ 🧭 KBO DATA LINEAGE & PROVENANCE AUDIT ({mode_str} | {season_str})",
            "╠════════════════════════════════════════════════════════════════════════════════╣",
            f"║ Total Population:            {report.total_population:<48} ║",
            f"║ Eligible Entities Checked:   {report.eligible_entities:<48} ║",
            f"║ Fully Traceable Records:     {report.fully_traceable_count:<48} ║",
            f"║ Broken Lineage Chains:       {report.broken_lineage_count:<48} ║",
            f"║ Cycles / Orphan Nodes:       {report.cycles_detected} / {len(report.orphaned_nodes):<44} ║",
            f"║ Traceability Ratio:          {report.traceability_ratio * 100:>7.3f}%{' ' * 40} ║",
            f"║ Duration:                    {report.duration_ms:>7.2f} ms{' ' * 38} ║",
            f"║ Compliance Status:           {report.compliance_status:<48} ║",
            "╠════════════════════════════════════════════════════════════════════════════════╣",
            "║ Population Census Breakdown:                                                  ║",
        ]

        for tbl, c in report.table_breakdowns.items():
            ratio_pct = c.traceability_ratio * 100.0
            info = f"{c.eligible_rows:>6} eligible | {c.traceable_rows:>6} traceable ({ratio_pct:5.1f}%)"
            lines.append(f"║ ├── {tbl:<25}: {info:<44} ║")

        lines.append("╚════════════════════════════════════════════════════════════════════════════════╝")
        return "\n".join(lines)

    @classmethod
    def render_mermaid(cls, graph: LineageGraph) -> str:
        """Render the lineage DAG into a Mermaid flowchart code block."""
        lines = ["```mermaid", "graph TD"]

        # Add nodes with sanitization
        for nid, node in graph.nodes.items():
            safe_id = nid.replace(":", "_").replace("-", "_").replace(".", "_")
            safe_label = node.label.replace('"', "'")
            lines.append(f'    {safe_id}["{safe_label}"]')

        # Add edges
        for edge in graph.edges:
            s_id = edge.source_node_id.replace(":", "_").replace("-", "_").replace(".", "_")
            t_id = edge.target_node_id.replace(":", "_").replace("-", "_").replace(".", "_")
            desc = f"|{edge.description}|" if edge.description else ""
            lines.append(f"    {s_id} -->{desc} {t_id}")

        lines.append("```")
        return "\n".join(lines)

    @classmethod
    def render_json(cls, report: GameLineageReport | PlayerMetricLineageReport | LineageAuditReport) -> str:
        """Serialize lineage report to formatted JSON."""
        return json.dumps(report.to_dict(), indent=2, ensure_ascii=False)


__all__ = [
    "LineageReporter",
]
