"""Tests for applying a rekey manifest to the vector store.

The store this writes is the pair of the one the rekey tool writes, and the
failure that matters is the two disagreeing: a chunk whose keys differ between
stores is one document to a person and two to the hybrid retriever, and a
tombstone applied on one side only leaves deleted content retrievable on the
other.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from pathlib import Path

from src.services.rag_vector_rekey import (
    DISPOSITION_REKEY,
    DISPOSITION_TOMBSTONE,
    RekeyEntry,
    apply_entries,
    load_entries,
    summarize_manifest,
)


def _entry(
    *,
    disposition: str = DISPOSITION_REKEY,
    legacy: str = "467",
    natural: str | None = "2026_올스타전MVP_NONE_허인서",
    content_hash: str = "h1",
    table: str = "awards",
) -> RekeyEntry:
    """Build one manifest entry."""
    return RekeyEntry(
        chunk_id=1,
        disposition=disposition,
        source_table=table,
        legacy_source_row_id=legacy,
        natural_source_row_id=natural,
        content_hash=content_hash,
    )


class _Fixture:
    """A SQLite store shaped like the vector table."""

    def __init__(self) -> None:
        """Create the table the statements expect."""
        self.engine = create_engine("sqlite://")
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE rag_chunks ("
                    "source_table TEXT, source_row_id TEXT, content_hash TEXT, "
                    "index_status TEXT, updated_at TIMESTAMP)"
                )
            )

    def session(self) -> Session:
        """Return a session bound to the store."""
        return Session(bind=self.engine)

    def insert(self, key: str, *, content_hash: str = "h1", status: str = "ACTIVE", table: str = "awards") -> None:
        """Add one row."""
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO rag_chunks (source_table, source_row_id, content_hash, index_status) "
                    "VALUES (:t, :k, :h, :s)"
                ),
                {"t": table, "k": key, "h": content_hash, "s": status},
            )

    def rows(self) -> list[tuple[str, str, str]]:
        """Return (row_id, status) pairs for assertions."""
        with self.engine.connect() as connection:
            return [
                (row[0], row[1])
                for row in connection.execute(
                    text("SELECT source_row_id, index_status FROM rag_chunks ORDER BY source_row_id")
                ).fetchall()
            ]


class TestLoadEntries:
    """Pin the manifest reading the census produces."""

    def test_reads_entries_and_skips_rows_without_identity(self, tmp_path: Path) -> None:
        """Keep only entries that name a source table and a legacy id."""
        manifest = tmp_path / "m.json"
        manifest.write_text(
            json.dumps(
                {
                    "entries": [
                        {
                            "chunk_id": 1,
                            "disposition": "SAFE_REKEY",
                            "source_table": "awards",
                            "legacy_source_row_id": "467",
                            "natural_source_row_id": "natural",
                            "legacy_content_hash": "h1",
                        },
                        {"chunk_id": 2, "disposition": "SAFE_REKEY", "source_table": "awards"},
                    ]
                }
            ),
            encoding="utf-8",
        )

        entries = load_entries(manifest)

        assert len(entries) == 1
        assert entries[0].legacy_source_row_id == "467"
        assert entries[0].natural_source_row_id == "natural"

    def test_rejects_a_manifest_without_entries(self, tmp_path: Path) -> None:
        """Refuse to guess at a file that is not a census manifest."""
        manifest = tmp_path / "m.json"
        manifest.write_text(json.dumps({"totals": {}}), encoding="utf-8")
        try:
            load_entries(manifest)
        except TypeError as error:
            assert "entries" in str(error)
        else:  # pragma: no cover - the assertion above is the point
            raise AssertionError("expected ValueError")


class TestSummary:
    """Pin the preview count."""

    def test_counts_by_disposition(self) -> None:
        """Group the manifest the way the operator reads it."""
        entries = [_entry(), _entry(disposition=DISPOSITION_TOMBSTONE), _entry(disposition="ORPHAN_SOURCE_ROW")]
        assert summarize_manifest(entries) == {
            DISPOSITION_REKEY: 1,
            DISPOSITION_TOMBSTONE: 1,
            "ORPHAN_SOURCE_ROW": 1,
        }


class TestApply:
    """Pin what the statements do to a real store."""

    def test_rekey_moves_the_key(self) -> None:
        """Rename the identity without touching anything else."""
        fixture = _Fixture()
        fixture.insert("467")
        session = fixture.session()
        report = apply_entries(session, [_entry()], dry_run=False, now=datetime(2026, 10, 10))
        assert report.rekeyed == 1
        assert fixture.rows() == [("2026_올스타전MVP_NONE_허인서", "ACTIVE")]

    def test_tombstone_marks_the_duplicate_deleted(self) -> None:
        """Hide the redundant legacy row rather than losing it."""
        fixture = _Fixture()
        fixture.insert("467")
        fixture.insert("natural")
        session = fixture.session()
        report = apply_entries(session, [_entry(disposition=DISPOSITION_TOMBSTONE)], dry_run=False)
        assert report.tombstoned == 1
        assert ("467", "DELETED") in fixture.rows()

    def test_a_rekeyed_row_is_not_missed_on_a_second_run(self) -> None:
        """Count an already-applied entry as such, not as drift."""
        fixture = _Fixture()
        fixture.insert("467")
        apply_entries(fixture.session(), [_entry()], dry_run=False)
        report = apply_entries(fixture.session(), [_entry()], dry_run=False)
        assert report.already_applied == 1
        assert report.missing == 0

    def test_a_content_hash_mismatch_is_counted_as_missing(self) -> None:
        """Refuse to touch a row whose content changed under the manifest."""
        fixture = _Fixture()
        fixture.insert("467", content_hash="different")
        report = apply_entries(fixture.session(), [_entry()], dry_run=False)
        assert report.missing == 1
        assert report.rekeyed == 0

    def test_unsupported_dispositions_are_skipped(self) -> None:
        """Leave the decisions the sparse side has not made."""
        fixture = _Fixture()
        fixture.insert("467")
        report = apply_entries(fixture.session(), [_entry(disposition="ORPHAN_SOURCE_ROW")], dry_run=False)
        assert report.skipped_unsupported == 1
        assert fixture.rows() == [("467", "ACTIVE")]

    def test_dry_run_writes_nothing(self) -> None:
        """Report the plan without moving a key."""
        fixture = _Fixture()
        fixture.insert("467")
        report = apply_entries(fixture.session(), [_entry()], dry_run=True)
        assert report.planned_rekeyed == 1
        assert report.rekeyed == 0
        assert fixture.rows() == [("467", "ACTIVE")]

    def test_both_keys_present_is_a_conflict(self) -> None:
        """Leave the merge to a human rather than guess at it."""
        fixture = _Fixture()
        fixture.insert("467")
        fixture.insert("2026_올스타전MVP_NONE_허인서")
        report = apply_entries(fixture.session(), [_entry()], dry_run=False)
        assert report.conflicted == 1
        assert report.rekeyed == 0
        assert ("467", "ACTIVE") in fixture.rows()

    def test_a_vanished_legacy_row_with_its_target_is_already_applied(self) -> None:
        """Read the aftermath of an interrupted run as done, not as drift."""
        fixture = _Fixture()
        fixture.insert("2026_올스타전MVP_NONE_허인서")
        report = apply_entries(fixture.session(), [_entry()], dry_run=False)
        assert report.already_applied == 1
        assert report.conflicted == 0
        assert report.missing == 0
