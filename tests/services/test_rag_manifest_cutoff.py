"""The comparison cutoff travels with the manifest, and says where it came from.

`TIME_EXPLAINABLE` was structurally always zero: the procedure told operators to
pass `--as-of <min(left.exported_at, right.exported_at)>`, and no manifest carried
an `exported_at`. The cost of that gap was measured on 2026-10-11 -- an in-flight
re-index produced 28,000 findings that were pure time difference, and the gate had
no way to say so.

The sidecar fixes the absence. These tests pin the part that is easy to get wrong
afterwards: a cutoff must never be applied silently, and it must never be derived
from one side alone. A cutoff nobody can see is indistinguishable from a finding
that was explained away.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from src.constants import KST
from src.services.rag_reconciliation import (
    ManifestEntry,
    meta_path,
    read_manifest,
    read_manifest_meta,
    write_manifest,
)
from src.cli.rag.reconcile_rag_stores import _resolve_as_of, main


def _entry(row_id: str, *, updated_at: datetime | None = None) -> ManifestEntry:
    return ManifestEntry(
        source_table="game",
        source_row_id=row_id,
        content_hash="a" * 64,
        index_version="rag-v1",
        index_status="ACTIVE",
        embedding_present=True,
        updated_at=updated_at,
    )


class TestTheSidecarRecordsWhenAnExportWasTaken:
    def test_writing_with_a_time_creates_the_sidecar(self, tmp_path: Path) -> None:
        path = tmp_path / "m.ndjson"
        stamp = datetime(2026, 10, 11, 3, 0, tzinfo=KST)

        write_manifest([_entry("1")], path, exported_at=stamp, side="primary")

        meta = read_manifest_meta(path)
        assert meta is not None
        assert meta.exported_at == stamp
        assert meta.side == "primary"
        assert meta.rows == 1

    def test_writing_without_a_time_leaves_no_sidecar(self, tmp_path: Path) -> None:
        """A manifest that did not measure its time must not pretend it did."""
        path = tmp_path / "m.ndjson"

        write_manifest([_entry("1")], path)

        assert not meta_path(path).exists()
        assert read_manifest_meta(path) is None

    def test_the_sidecar_is_not_a_manifest_row(self, tmp_path: Path) -> None:
        """One line, one chunk -- the reason the metadata is a separate file.

        A header line inside the NDJSON would have to be recognised and skipped,
        and a reader that gets that wrong either reads a header as a chunk or
        drops a chunk it mistook for a header. A separate file cannot be either.
        """
        path = tmp_path / "m.ndjson"
        write_manifest([_entry("1"), _entry("2")], path, exported_at=datetime(2026, 10, 11, tzinfo=KST))

        rows = list(read_manifest(path))

        assert [row.source_row_id for row in rows] == ["1", "2"]
        assert len(path.read_text(encoding="utf-8").splitlines()) == 2

    def test_the_row_count_is_recorded_so_a_renamed_file_is_detectable(self, tmp_path: Path) -> None:
        path = tmp_path / "m.ndjson"
        write_manifest([_entry("1"), _entry("2")], path, exported_at=datetime(2026, 10, 11, tzinfo=KST))

        assert read_manifest_meta(path).rows == 2

    def test_a_corrupt_sidecar_reads_as_absent(self, tmp_path: Path) -> None:
        """Absent and unreadable both mean "no measured cutoff"."""
        path = tmp_path / "m.ndjson"
        write_manifest([_entry("1")], path)
        meta_path(path).write_text("{not json", encoding="utf-8")

        assert read_manifest_meta(path) is None

    def test_a_sidecar_without_a_time_reads_as_absent(self, tmp_path: Path) -> None:
        path = tmp_path / "m.ndjson"
        write_manifest([_entry("1")], path)
        meta_path(path).write_text(json.dumps({"side": "primary", "rows": 1}), encoding="utf-8")

        assert read_manifest_meta(path) is None


class TestTheCutoffIsDerivedOnlyFromBothSides:
    def test_both_sidecars_give_the_earlier_time(self, tmp_path: Path) -> None:
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left, exported_at=datetime(2026, 10, 11, 5, 0, tzinfo=KST))
        write_manifest([_entry("1")], right, exported_at=datetime(2026, 10, 11, 3, 0, tzinfo=KST))

        cutoff, source = _resolve_as_of(left, right, None)

        assert cutoff == datetime(2026, 10, 11, 3, 0, tzinfo=KST)
        assert source == "manifest"

    def test_one_sidecar_is_not_enough(self, tmp_path: Path) -> None:
        """A cutoff from one side bounds the other by a moment it never saw.

        That is how a file exported weeks ago quietly becomes "no drift": its own
        timestamp is old, everything on the other side looks newer, and every
        difference is filed as background churn. Requiring both is what stops it.
        """
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left, exported_at=datetime(2026, 10, 11, 5, 0, tzinfo=KST))
        write_manifest([_entry("1")], right)

        cutoff, source = _resolve_as_of(left, right, None)

        assert cutoff is None
        assert source == "none"

    def test_no_sidecar_means_no_cutoff(self, tmp_path: Path) -> None:
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left)
        write_manifest([_entry("1")], right)

        cutoff, source = _resolve_as_of(left, right, None)

        assert cutoff is None
        assert source == "none"

    def test_an_explicit_cutoff_wins(self, tmp_path: Path) -> None:
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left, exported_at=datetime(2026, 10, 11, 5, 0, tzinfo=KST))
        write_manifest([_entry("1")], right, exported_at=datetime(2026, 10, 11, 5, 0, tzinfo=KST))

        cutoff, source = _resolve_as_of(left, right, "2026-01-01T00:00:00+09:00")

        assert cutoff == datetime(2026, 1, 1, tzinfo=KST)
        assert source == "explicit"


class TestTheReportSaysWhichRuleWasApplied:
    def test_the_summary_records_the_source(self, tmp_path: Path) -> None:
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left, exported_at=datetime(2026, 10, 11, 3, 0, tzinfo=KST))
        write_manifest([_entry("1")], right, exported_at=datetime(2026, 10, 11, 3, 0, tzinfo=KST))
        out = tmp_path / "rep"

        assert main(["compare", "--left", str(left), "--right", str(right), "--output-dir", str(out)]) == 0

        summary = json.loads((out / "comparison_summary.json").read_text(encoding="utf-8"))
        assert summary["as_of_source"] == "manifest"
        assert summary["as_of"] is not None

    def test_a_missing_sidecar_is_reported_as_no_cutoff(self, tmp_path: Path) -> None:
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        write_manifest([_entry("1")], left)
        write_manifest([_entry("1")], right)
        out = tmp_path / "rep"

        assert main(["compare", "--left", str(left), "--right", str(right), "--output-dir", str(out)]) == 0

        summary = json.loads((out / "comparison_summary.json").read_text(encoding="utf-8"))
        assert summary["as_of_source"] == "none"
        assert summary["as_of"] is None


class TestTheCutoffActuallyClassifies:
    def test_a_difference_after_the_cutoff_is_time_explainable(self, tmp_path: Path) -> None:
        """The whole point: this is the case the sidecar exists to catch.

        Both sides were exported at 03:00; one row was written at 04:00. Without a
        cutoff that row is drift, which is what happened to 28,000 PBP chunks
        during the October re-index.
        """
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        stamp = datetime(2026, 10, 11, 3, 0, tzinfo=KST)
        write_manifest([_entry("1", updated_at=datetime(2026, 10, 11, 4, 0, tzinfo=KST))], left, exported_at=stamp)
        write_manifest([], right, exported_at=stamp)
        out = tmp_path / "rep"

        assert main(["compare", "--left", str(left), "--right", str(right), "--output-dir", str(out)]) == 0

        summary = json.loads((out / "comparison_summary.json").read_text(encoding="utf-8"))
        assert summary["unexplained_count"] == 0
        # The row sits in `left` and not in `right`, so it is a left-only key --
        # the same side as `MISSING_IN_RIGHT`.
        assert summary["left_only_after_cutoff_count"] == 1

    def test_the_same_difference_without_a_cutoff_is_drift(self, tmp_path: Path) -> None:
        """The conservative direction is the one that fails loudly."""
        left, right = tmp_path / "l.ndjson", tmp_path / "r.ndjson"
        yesterday = datetime(2026, 10, 1, tzinfo=KST)
        write_manifest([_entry("1", updated_at=yesterday)], left)
        write_manifest([], right)
        out = tmp_path / "rep"

        assert main(["compare", "--left", str(left), "--right", str(right), "--output-dir", str(out)]) == 0

        summary = json.loads((out / "comparison_summary.json").read_text(encoding="utf-8"))
        assert summary["unexplained_count"] == 1
        assert summary["as_of_source"] == "none"
