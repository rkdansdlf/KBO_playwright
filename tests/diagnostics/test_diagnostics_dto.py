"""Unit tests for src.diagnostics.dto."""

from __future__ import annotations

from src.diagnostics.dto import (
    DiagnosticSeverity,
    SubsystemCheckItem,
    SubsystemType,
    UnifiedDiagnosticsReport,
)


def test_subsystem_type_values() -> None:
    assert SubsystemType.DATABASE == "database"
    assert SubsystemType.SCHEDULER == "scheduler"
    assert SubsystemType.CRAWLER == "crawler"
    assert SubsystemType.PIPELINE == "pipeline"
    assert SubsystemType.API_GATEWAY == "api_gateway"
    assert SubsystemType.RAG_VECTOR == "rag_vector"


def test_diagnostic_severity_values() -> None:
    assert DiagnosticSeverity.HEALTHY == "HEALTHY"
    assert DiagnosticSeverity.INFO == "INFO"
    assert DiagnosticSeverity.WARNING == "WARNING"
    assert DiagnosticSeverity.CRITICAL == "CRITICAL"


def test_subsystem_check_item_to_dict() -> None:
    item = SubsystemCheckItem(
        name="db_check",
        subsystem=SubsystemType.DATABASE,
        severity=DiagnosticSeverity.HEALTHY,
        status="OK",
        message="Database is responsive.",
        metrics={"latency_ms": 1.25},
        remediation_hint=None,
    )
    d = item.to_dict()
    assert d["name"] == "db_check"
    assert d["subsystem"] == "database"
    assert d["severity"] == "HEALTHY"
    assert d["status"] == "OK"
    assert d["metrics"]["latency_ms"] == 1.25


def test_unified_diagnostics_report_to_dict() -> None:
    report = UnifiedDiagnosticsReport(
        overall_status="HEALTHY",
        total_checks=5,
        healthy_count=5,
        warning_count=0,
        critical_count=0,
        checks=[],
        generated_at="2026-08-26T22:00:00Z",
    )
    d = report.to_dict()
    assert d["overall_status"] == "HEALTHY"
    assert d["total_checks"] == 5
    assert d["healthy_count"] == 5
    assert d["warning_count"] == 0
