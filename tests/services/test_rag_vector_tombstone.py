"""Tests for vector-store tombstones.

The tool exists because the primary-side tombstone no longer covers every copy of
a chunk: since the vectors moved to pgvector, a tombstone applied to the sparse
store alone leaves the dense leg serving deleted content, and the reconciliation
gate reporting an index_status mismatch for every retired key.

The failure that matters is therefore the opposite direction too -- a run that
reports success without having retired anything hides both problems.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.services.rag_vector_tombstone import (
    inspect_vector_tombstones,
    split_source_key,
    tombstone_vector_rows,
)


class _Fixture:
    """A SQLite store shaped like the chunk table."""

    def __init__(self) -> None:
        """Create the table the statements expect."""
        self.engine = create_engine("sqlite://")
        with self.engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE rag_chunks (source_table TEXT, source_row_id TEXT, index_status TEXT)")
            )

    def session(self) -> Session:
        """Return a session bound to the store."""
        return Session(bind=self.engine)

    def insert(self, table: str, row_id: str, status: str = "ACTIVE") -> None:
        """Add one chunk."""
        with self.engine.begin() as connection:
            connection.execute(
                text("INSERT INTO rag_chunks (source_table, source_row_id, index_status) VALUES (:t, :r, :s)"),
                {"t": table, "r": row_id, "s": status},
            )

    def status_of(self, table: str, row_id: str) -> str | None:
        """Return one row's status."""
        with self.engine.connect() as connection:
            row = connection.execute(
                text("SELECT index_status FROM rag_chunks WHERE source_table = :t AND source_row_id = :r"),
                {"t": table, "r": row_id},
            ).first()
        return None if row is None else str(row[0])


class TestSplitSourceKey:
    """Pin the accepted key shape."""

    def test_splits_table_and_row_id(self) -> None:
        """Accept the documented form."""
        assert split_source_key("awards:2025_MVP_투수_폰세") == ("awards", "2025_MVP_투수_폰세")

    def test_rejects_a_key_without_a_separator(self) -> None:
        """Refuse a row id that could silently mean the whole table."""
        try:
            split_source_key("awards")
        except ValueError as error:
            assert "table:row_id" in str(error)
        else:  # pragma: no cover - the assertion above is the point
            raise AssertionError("expected ValueError")


class TestInspect:
    """Pin what inspection reports without writing."""

    def test_reports_found_and_status(self) -> None:
        """Report a row that exists, unchanged."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        report = inspect_vector_tombstones(fixture.session(), ("awards:1",))[0]
        assert report.found
        assert report.previous_status == "ACTIVE"
        assert report.final_status == "ACTIVE"

    def test_reports_a_missing_row_as_not_found(self) -> None:
        """Distinguish absent from retired."""
        report = inspect_vector_tombstones(_Fixture().session(), ("awards:999",))[0]
        assert not report.found

    def test_leaves_the_store_untouched(self) -> None:
        """Inspection must not write."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        inspect_vector_tombstones(fixture.session(), ("awards:1",))
        assert fixture.status_of("awards", "1") == "ACTIVE"


class TestApply:
    """Pin what an apply does."""

    def test_retires_a_row(self) -> None:
        """Mark the row DELETED and keep its previous status in the record."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        (report,) = tombstone_vector_rows(fixture.session(), ("awards:1",))
        assert report.final_status == "DELETED"
        assert report.previous_status == "ACTIVE"
        assert fixture.status_of("awards", "1") == "DELETED"

    def test_a_second_run_is_not_counted_as_new_work(self) -> None:
        """An already-retired row reports itself as not new."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        tombstone_vector_rows(fixture.session(), ("awards:1",))
        (second,) = tombstone_vector_rows(fixture.session(), ("awards:1",))
        assert second.final_status == "DELETED"
        assert second.previous_status == "DELETED"

    def test_a_missing_row_reports_itself_missing(self) -> None:
        """Do not fabricate a success for a row that was never there."""
        fixture = _Fixture()
        (report,) = tombstone_vector_rows(fixture.session(), ("awards:999",))
        assert not report.found
        assert report.error is None

    def test_a_malformed_key_is_reported_as_an_error(self) -> None:
        """Carry the rejection forward rather than dropping the row."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        (report,) = tombstone_vector_rows(fixture.session(), ("awards",))
        assert report.error is not None
        assert fixture.status_of("awards", "1") == "ACTIVE"

    def test_a_failed_update_leaves_the_row_active(self) -> None:
        """Record the failure and keep the status truthful.

        The update is made to fail by dropping the column it sets, so the
        rollback path runs against a real error rather than a stub that only
        looks like one.
        """
        fixture = _Fixture()
        fixture.insert("awards", "1")
        with fixture.engine.begin() as connection:
            connection.execute(
                text("CREATE TRIGGER block_update BEFORE UPDATE ON rag_chunks BEGIN SELECT RAISE(ABORT, 'no'); END")
            )
        try:
            (report,) = tombstone_vector_rows(fixture.session(), ("awards:1",))
        finally:
            with fixture.engine.begin() as connection:
                connection.execute(text("DROP TRIGGER block_update"))
        assert report.error is not None
        assert report.previous_status == "ACTIVE"
        assert fixture.status_of("awards", "1") == "ACTIVE"
