"""Tests for golden-set coverage checking.

The check exists because a rekey renames the very chunks a golden set points
at, after which a perfect retrieval scores as a miss. That mistake is easy to
attribute to the retriever and expensive to notice, because every metric still
looks like a metric -- only lower.

These tests pin the distinction the check is responsible for: an id the store
does not hold is reported, and an id it does hold is not.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from src.services.rag_golden_coverage import (
    MISSING_SAMPLE_CAP,
    check_golden_set_coverage,
)


class _Fixture:
    """A SQLite store shaped like the chunk table."""

    def __init__(self) -> None:
        """Create the table the check queries."""
        self.engine = create_engine("sqlite://")
        with self.engine.begin() as connection:
            connection.execute(
                text("CREATE TABLE rag_chunks (source_table TEXT, source_row_id TEXT, content_hash TEXT)")
            )

    def session(self) -> Session:
        """Return a session bound to the store."""
        return Session(bind=self.engine)

    def insert(self, table: str, row_id: str) -> None:
        """Add one chunk."""
        with self.engine.begin() as connection:
            connection.execute(
                text("INSERT INTO rag_chunks (source_table, source_row_id, content_hash) VALUES (:t, :r, 'h')"),
                {"t": table, "r": row_id},
            )


class TestCoverage:
    """Pin what the check reports."""

    def test_a_present_id_is_not_reported(self) -> None:
        """Confirm the everyday case produces nothing to act on."""
        fixture = _Fixture()
        fixture.insert("awards", "2025_MVP_NONE_폰세")
        report = check_golden_set_coverage(fixture.session(), ["awards:2025_MVP_NONE_폰세"])
        assert report.is_clean
        assert report.missing_count == 0
        assert report.expected_count == 1

    def test_a_renamed_id_is_reported(self) -> None:
        """Catch the exact failure this check was written for."""
        fixture = _Fixture()
        fixture.insert("awards", "2025_MVP_NONE_폰세")
        report = check_golden_set_coverage(fixture.session(), ["awards:470"])
        assert not report.is_clean
        assert report.missing_ids == ("awards:470",)

    def test_a_table_the_store_lacks_is_reported_separately(self) -> None:
        """Distinguish a missing table from a missing chunk."""
        fixture = _Fixture()
        fixture.insert("awards", "1")
        report = check_golden_set_coverage(fixture.session(), ["awards:1", "team_events:7"])
        assert report.unknown_sources == ("team_events",)
        assert "team_events:7" in report.missing_ids

    def test_duplicate_ids_are_counted_once(self) -> None:
        """Treat the id list as a set of names, not as a tally."""
        fixture = _Fixture()
        fixture.insert("game", "20260721HHHT0")
        report = check_golden_set_coverage(fixture.session(), ["game:20260721HHHT0", "game:20260721HHHT0"])
        assert report.expected_count == 1
        assert report.is_clean

    def test_many_missing_ids_are_capped_in_the_sample(self) -> None:
        """Keep the sample readable while the count stays exact."""
        fixture = _Fixture()
        fixture.insert("game", "present")
        missing = [f"game:absent{i}" for i in range(MISSING_SAMPLE_CAP + 5)]
        report = check_golden_set_coverage(fixture.session(), ["game:present", *missing])
        assert report.missing_count == MISSING_SAMPLE_CAP
        assert len(report.missing_ids) == MISSING_SAMPLE_CAP

    def test_a_store_holding_everything_reports_clean_summary(self) -> None:
        """Render the summary an operator reads."""
        fixture = _Fixture()
        fixture.insert("game", "a")
        fixture.insert("awards", "b")
        report = check_golden_set_coverage(fixture.session(), ["game:a", "awards:b"])
        assert report.summary == "expected=2 missing=0 clean=True"
