"""Tests for RAG R2 identity census service."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.services.rag_identity_census import (
    IdentityCensusReport,
    SourceIdentityRecord,
    ExistingIdentityRow,
    IdentityCensusEntry,
    SourceIdentitySummary,
    _is_legacy_numeric_id,
    _target_disposition,
    build_identity_census,
    validate_source_tables,
    iter_source_identity_records,
)


class TestSourceIdentityRecord:
    def test_identity_record_creation(self) -> None:
        record = SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인")
        assert record.source_table == "awards"
        assert record.source_row_id == "1"
        assert record.natural_source_row_id == "2025_골든글러브_투수_원태인"


class TestExistingIdentityRow:
    def test_existing_row_creation(self) -> None:
        row = ExistingIdentityRow(100, "awards", "1", "abc123", "ACTIVE")
        assert row.chunk_id == 100
        assert row.source_table == "awards"
        assert row.source_row_id == "1"
        assert row.content_hash == "abc123"
        assert row.index_status == "ACTIVE"


class TestIsLegacyNumericId:
    def test_numeric_id_is_legacy(self) -> None:
        assert _is_legacy_numeric_id("123")
        assert _is_legacy_numeric_id("1")
        assert _is_legacy_numeric_id("469")

    def test_non_numeric_id_is_not_legacy(self) -> None:
        assert not _is_legacy_numeric_id("2025_골든글러브_투수_원태인")
        assert not _is_legacy_numeric_id("abc")
        assert not _is_legacy_numeric_id("2025_2001_KIA_REGULAR")
        assert not _is_legacy_numeric_id("20260401OBHT0")


class TestTargetDisposition:
    def test_safe_rekey_when_no_targets(self) -> None:
        legacy = ExistingIdentityRow(1, "awards", "1", "hash1", "ACTIVE")
        assert _target_disposition(legacy, []) == "SAFE_REKEY"

    def test_target_exists_same_content(self) -> None:
        legacy = ExistingIdentityRow(1, "awards", "1", "hash1", "ACTIVE")
        targets = [
            ExistingIdentityRow(2, "awards", "2025_골든글러브_투수_원태인", "hash1", "ACTIVE"),
        ]
        assert _target_disposition(legacy, targets) == "TARGET_EXISTS_SAME_CONTENT"

    def test_target_exists_content_mismatch(self) -> None:
        legacy = ExistingIdentityRow(1, "awards", "1", "hash1", "ACTIVE")
        targets = [
            ExistingIdentityRow(2, "awards", "2025_골든글러브_투수_원태인", "hash2", "ACTIVE"),
        ]
        assert _target_disposition(legacy, targets) == "TARGET_EXISTS_CONTENT_MISMATCH"

    def test_target_exists_unknown_content(self) -> None:
        legacy = ExistingIdentityRow(1, "awards", "1", None, "ACTIVE")
        targets = [
            ExistingIdentityRow(2, "awards", "2025_골든글러브_투수_원태인", "hash2", "ACTIVE"),
        ]
        assert _target_disposition(legacy, targets) == "TARGET_EXISTS_UNKNOWN_CONTENT"

    def test_target_exists_unknown_content_when_target_has_none(self) -> None:
        legacy = ExistingIdentityRow(1, "awards", "1", "hash1", "ACTIVE")
        targets = [
            ExistingIdentityRow(2, "awards", "2025_골든글러브_투수_원태인", None, "ACTIVE"),
        ]
        assert _target_disposition(legacy, targets) == "TARGET_EXISTS_UNKNOWN_CONTENT"


class TestValidateSourceTables:
    def test_valid_single_table(self) -> None:
        assert validate_source_tables(["awards"]) == ("awards",)

    def test_valid_multiple_tables(self) -> None:
        assert validate_source_tables(["awards", "team_history", "milestone"]) == (
            "awards",
            "team_history",
            "milestone",
        )

    def test_removes_duplicates(self) -> None:
        assert validate_source_tables(["awards", "awards", "team_history"]) == (
            "awards",
            "team_history",
        )

    def test_invalid_table_raises(self) -> None:
        with pytest.raises(ValueError, match="unsupported R2 source table"):
            validate_source_tables(["invalid_table"])

    def test_multiple_invalid_tables_sorted_in_error(self) -> None:
        with pytest.raises(ValueError, match="unsupported R2 source table"):
            validate_source_tables(["invalid_b", "invalid_a"])


class TestBuildIdentityCensus:
    def test_empty_inputs(self) -> None:
        report = build_identity_census([], [], source_tables=["awards"])
        assert report.source_tables == ("awards",)
        assert len(report.entries) == 0
        assert report.totals()["source_rows"] == 0

    def test_orphan_legacy_row(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "999", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "ORPHAN_SOURCE_ROW"
        assert entry.legacy_source_row_id == "999"
        assert entry.natural_source_row_id is None

    def test_safe_rekey_when_no_target(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "SAFE_REKEY"
        assert entry.legacy_source_row_id == "1"
        assert entry.natural_source_row_id == "2025_골든글러브_투수_원태인"
        assert entry.target_chunk_ids == ()

    def test_target_exists_same_content(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "awards", "2025_골든글러브_투수_원태인", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "TARGET_EXISTS_SAME_CONTENT"
        assert entry.target_chunk_ids == (200,)
        assert entry.target_content_hashes == ("hash1",)

    def test_target_exists_content_mismatch(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "awards", "2025_골든글러브_투수_원태인", "hash2", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "TARGET_EXISTS_CONTENT_MISMATCH"
        assert entry.target_chunk_ids == (200,)
        assert entry.target_content_hashes == ("hash2",)

    def test_source_collision_when_multiple_candidates(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
            SourceIdentityRecord("awards", "1", "2025_골든글러브_타자_김도영"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "SOURCE_COLLISION"
        assert len(entry.source_record_ids) == 2

    def test_source_collision_when_multiple_natural_records(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
            SourceIdentityRecord("awards", "2", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        entry = report.entries[0]
        assert entry.disposition == "SOURCE_COLLISION"
        assert len(entry.source_record_ids) == 2

    def test_multiple_source_tables(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
            SourceIdentityRecord("team_history", "10", "2020_LG"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "team_history", "10", "hash2", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards", "team_history"])
        assert report.source_tables == ("awards", "team_history")
        assert len(report.entries) == 2

    def test_non_legacy_rows_excluded_from_entries(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "awards", "2025_골든글러브_투수_원태인", "hash2", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        assert len(report.entries) == 1
        assert report.entries[0].legacy_source_row_id == "1"

    def test_unsafe_entry_count(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
            SourceIdentityRecord("awards", "2", "2025_골든글러브_타자_김도영"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "awards", "999", "hash2", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        # One SAFE_REKEY (legacy "1" maps to source), one ORPHAN_SOURCE_ROW (legacy "999" has no source)
        assert report.unsafe_entry_count == 1


class TestIterSourceIdentityRecords:
    def test_unsupported_table_raises(self) -> None:
        mock_session = Mock()
        with pytest.raises(ValueError, match="unsupported R2 source table"):
            list(iter_source_identity_records(mock_session, "invalid_table"))


class TestReportSerialization:
    def test_to_summary_dict(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        summary = report.to_summary_dict(sample_limit=10)
        assert summary["manifest_version"] == "r2-identity-census-v1"
        assert summary["target_index_version"] == "rag-v2"
        assert summary["read_only"] is True
        assert summary["source_tables"] == ["awards"]
        assert summary["unsafe_entry_count"] == 0
        assert "sources" in summary
        assert "totals" in summary

    def test_to_manifest_dict(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        manifest = report.to_manifest_dict()
        assert manifest["manifest_version"] == "r2-identity-census-v1"
        assert manifest["target_index_version"] == "rag-v2"
        assert manifest["read_only"] is True
        assert "entries" in manifest
        assert len(manifest["entries"]) == 1

    def test_totals_aggregation(self) -> None:
        source_records = [
            SourceIdentityRecord("awards", "1", "2025_골든글러브_투수_원태인"),
            SourceIdentityRecord("awards", "2", "2025_골든글러브_타자_김도영"),
        ]
        existing_rows = [
            ExistingIdentityRow(100, "awards", "1", "hash1", "ACTIVE"),
            ExistingIdentityRow(200, "awards", "999", "hash2", "ACTIVE"),
        ]
        report = build_identity_census(existing_rows, source_records, source_tables=["awards"])
        totals = report.totals()
        assert totals["source_rows"] == 2
        assert totals["legacy_numeric_rows"] == 2
        assert totals["safe_source_matches"] == 1
        assert totals["orphan_rows"] == 1
        assert totals["safe_rekey_candidates"] == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
