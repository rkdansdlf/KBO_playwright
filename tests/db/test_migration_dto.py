"""Unit tests for src.db.dto."""

from __future__ import annotations

from src.db.dto import (
    MigrationDialect,
    MigrationExecutionResult,
    MigrationFileMeta,
    MigrationStatusReport,
)


def test_migration_dialect_values() -> None:
    assert MigrationDialect.ORACLE == "oracle"
    assert MigrationDialect.SQLITE == "sqlite"
    assert MigrationDialect.POSTGRESQL == "postgresql"
    assert MigrationDialect.PGVECTOR == "pgvector"


def test_migration_file_meta_to_dict() -> None:
    meta = MigrationFileMeta(
        version=42,
        filename="042_crawl_runs_unique.sql",
        path="/path/to/042_crawl_runs_unique.sql",
        dialect=MigrationDialect.ORACLE,
        is_safety_gated=False,
        checksum="abc12345",
    )
    d = meta.to_dict()
    assert d["version"] == 42
    assert d["filename"] == "042_crawl_runs_unique.sql"
    assert d["dialect"] == "oracle"
    assert d["checksum"] == "abc12345"


def test_migration_execution_result_to_dict() -> None:
    result = MigrationExecutionResult(
        filename="001_init.sql",
        version=1,
        status="APPLIED",
        duration_seconds=0.123,
    )
    d = result.to_dict()
    assert d["filename"] == "001_init.sql"
    assert d["status"] == "APPLIED"
    assert d["duration_seconds"] == 0.123


def test_migration_status_report_to_dict() -> None:
    report = MigrationStatusReport(
        dialect="oracle",
        total_available=10,
        applied_count=8,
        pending_count=2,
        applied_versions=[1, 2, 3, 4, 5, 6, 7, 8],
        pending_versions=[9, 10],
    )
    d = report.to_dict()
    assert d["dialect"] == "oracle"
    assert d["total_available"] == 10
    assert d["applied_count"] == 8
    assert d["pending_versions"] == [9, 10]
