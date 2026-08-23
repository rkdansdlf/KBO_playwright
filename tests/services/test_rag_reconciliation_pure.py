"""Pure tests for RAG identity manifest reconciliation logic."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.constants import KST
from src.services.rag_reconciliation import (
    ManifestEntry,
    entry_from_manifest_row,
    parse_updated_at,
    read_manifest,
    reconcile_manifests,
    write_manifest,
)


def _entry(
    table: str,
    row_id: str,
    *,
    content_hash: str | None = "h1",
    version: str | None = "v1",
    status: str | None = "ACTIVE",
    embedding_present: bool | None = True,
    updated_at: str | None = None,
) -> ManifestEntry:
    """Build a manifest entry with sensible defaults."""
    return ManifestEntry(
        source_table=table,
        source_row_id=row_id,
        content_hash=content_hash,
        index_version=version,
        index_status=status,
        embedding_present=embedding_present,
        updated_at=parse_updated_at(updated_at) if updated_at else None,
    )


class TestManifestRoundtrip:
    def test_write_then_read_preserves_entries(self, tmp_path: Path) -> None:
        entries = [
            _entry("awards", "1"),
            _entry("game", "abc", content_hash=None, embedding_present=False),
            _entry("game_pbp", "x-1", updated_at="2026-08-22T17:40:39+09:00"),
        ]
        path = tmp_path / "manifest.ndjson"

        written = write_manifest(entries, path)
        loaded = list(read_manifest(path))

        assert written == 3
        assert loaded == entries

    def test_naive_timestamp_normalized_to_kst(self) -> None:
        entry = entry_from_manifest_row(
            {"source_table": "t", "source_row_id": "1", "updated_at": "2026-08-22T10:00:00"}
        )

        assert entry.updated_at is not None
        assert entry.updated_at.tzinfo == KST

    def test_key_format_matches_source_identity(self) -> None:
        assert _entry("awards", "7").key == "awards:7"


class TestReconcileWithoutAsOf:
    def test_identical_manifests_are_clean(self) -> None:
        left = [_entry("awards", "1"), _entry("game", "2")]
        report = reconcile_manifests(left, list(left), left_label="a", right_label="b")

        assert report.is_clean
        assert report.common_count == 2
        assert report.total_union_count == 2

    def test_hash_mismatch_is_unexplained(self) -> None:
        left = [_entry("awards", "1", content_hash="h1")]
        right = [_entry("awards", "1", content_hash="h2")]
        report = reconcile_manifests(left, right)

        assert not report.is_clean
        assert report.unexplained_issues["CONTENT_HASH_MISMATCH"] == ("awards:1",)

    def test_left_only_and_right_only_missing_issues(self) -> None:
        left = [_entry("awards", "1"), _entry("game", "2")]
        right = [_entry("awards", "1"), _entry("game", "3")]
        report = reconcile_manifests(left, right)

        assert report.unexplained_issues["MISSING_IN_RIGHT"] == ("game:2",)
        assert report.unexplained_issues["MISSING_IN_LEFT"] == ("game:3",)
        assert report.common_count == 1

    def test_embedding_missing_flagged(self) -> None:
        left = [_entry("awards", "1")]
        right = [_entry("awards", "1", embedding_present=False)]
        report = reconcile_manifests(left, right)

        assert report.unexplained_issues["EMBEDDING_MISSING"] == ("awards:1",)

    def test_summary_breakdown_by_source_table(self) -> None:
        left = [_entry("game", "1", content_hash="h1"), _entry("game", "2", content_hash="h1")]
        right = [_entry("game", "1", content_hash="h2"), _entry("game", "2", content_hash="h2")]
        report = reconcile_manifests(left, right, left_label="adb", right_label="staging")
        summary = report.to_summary_dict({e.key: e for e in left}, {e.key: e for e in right})

        assert summary["unexplained_hash_mismatch_by_source"] == {"game": 2}
        assert summary["left"] == "adb"
        assert summary["is_clean"] is False


class TestReconcileWithAsOf:
    CUTOFF = datetime(2026, 8, 21, tzinfo=KST)

    def test_common_key_changed_after_cutoff_is_time_explainable(self) -> None:
        left = [_entry("awards", "1", content_hash="old", updated_at="2026-08-20T00:00:00+09:00")]
        right = [_entry("awards", "1", content_hash="new", updated_at="2026-08-22T00:00:00+09:00")]
        report = reconcile_manifests(left, right, as_of=self.CUTOFF)

        assert report.is_clean
        assert report.time_explainable_keys == ("awards:1",)

    def test_single_side_addition_after_cutoff_explained(self) -> None:
        left = [
            _entry("game", "keep", updated_at="2026-08-20T00:00:00+09:00"),
            _entry("game", "fresh", updated_at="2026-08-23T00:00:00+09:00"),
        ]
        right = [_entry("game", "keep", updated_at="2026-08-19T00:00:00+09:00")]
        report = reconcile_manifests(left, right, as_of=self.CUTOFF)

        assert report.is_clean
        assert report.left_only_after_cutoff == ("game:fresh",)
        assert report.right_only_after_cutoff == ()

    def test_missing_timestamp_stays_conservative(self) -> None:
        left = [_entry("awards", "1", content_hash="h1")]
        right = [_entry("awards", "1", content_hash="h2", updated_at="2026-08-25T00:00:00+09:00")]
        report = reconcile_manifests(left, right, as_of=self.CUTOFF)

        assert report.time_explainable_keys == ("awards:1",)

    def test_both_before_cutoff_mismatch_remains_unexplained(self) -> None:
        left = [_entry("awards", "1", content_hash="h1", updated_at="2026-08-20T00:00:00+09:00")]
        right = [_entry("awards", "1", content_hash="h2", updated_at="2026-08-20T12:00:00+09:00")]
        report = reconcile_manifests(left, right, as_of=self.CUTOFF)

        assert not report.is_clean
        assert report.unexplained_count == 1
