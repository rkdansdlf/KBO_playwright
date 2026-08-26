"""Unit tests for src.reporting.dto."""

from __future__ import annotations

from src.reporting.dto import (
    ReportCategory,
    ReportFormat,
    ReportSection,
    UnifiedExecutiveReport,
)


def test_report_format_values() -> None:
    assert ReportFormat.MARKDOWN == "markdown"
    assert ReportFormat.JSON == "json"
    assert ReportFormat.HTML == "html"
    assert ReportFormat.TEXT == "text"


def test_report_category_values() -> None:
    assert ReportCategory.QUALITY_GATE == "quality_gate"
    assert ReportCategory.GAP_ANALYSIS == "gap_analysis"
    assert ReportCategory.FRESHNESS == "freshness"
    assert ReportCategory.EXECUTIVE_DASHBOARD == "executive_dashboard"


def test_report_section_to_dict() -> None:
    sec = ReportSection(
        title="PA Invariants",
        content_markdown="- PA Violations: 0",
        metrics={"violations": 0},
        status="PASS",
    )
    d = sec.to_dict()
    assert d["title"] == "PA Invariants"
    assert d["status"] == "PASS"
    assert d["metrics"]["violations"] == 0


def test_unified_executive_report_to_dict() -> None:
    report = UnifiedExecutiveReport(
        report_id="rep_001",
        category=ReportCategory.QUALITY_GATE,
        title="Quality Report",
        generated_at="2026-08-26T22:00:00Z",
        overall_status="PASS",
        summary_metrics={"score": 100},
        sections=[],
    )
    d = report.to_dict()
    assert d["report_id"] == "rep_001"
    assert d["category"] == "quality_gate"
    assert d["overall_status"] == "PASS"
