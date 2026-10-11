"""Check that a golden set's expected chunk ids still exist in a store.

A golden set names the chunks that *should* answer each query. A rekey renames
those chunks -- ``awards:470`` becomes ``awards:2025_MVP_NONE_폰세`` -- and the
set then scores a perfect retrieval as a miss. Nothing is obviously wrong: the
numbers move down, no query errors, and the cause is a stale fixture rather than
a broken index.

That failure is invisible in the metrics themselves, because recall cannot
distinguish "the retriever missed" from "the answer was renamed away". So the
check is separate and explicit: it resolves every expected id against the store
and reports which ones are absent, which is a fixture problem by construction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: Cap on reported missing ids; the count carries the rest.
MISSING_SAMPLE_CAP = 10


@dataclass(frozen=True)
class GoldenSetCoverage:
    """Whether a golden set's expected ids are still present in a store."""

    expected_count: int
    missing_ids: tuple[str, ...] = ()
    unknown_sources: tuple[str, ...] = ()

    @property
    def missing_count(self) -> int:
        """Return how many expected ids the store does not hold."""
        return len(self.missing_ids)

    @property
    def is_clean(self) -> bool:
        """Return whether every expected id resolves."""
        return not self.missing_ids

    @property
    def summary(self) -> str:
        """Return a one-line rendering for logs and CLI output."""
        return f"expected={self.expected_count} missing={self.missing_count} clean={self.is_clean}"


@dataclass
class _Accumulator:
    """Collect the missing ids across chunked lookups."""

    expected: set[str] = field(default_factory=set)
    missing: list[str] = field(default_factory=list)


def check_golden_set_coverage(
    session: Session,
    expected_ids: Sequence[str],
    *,
    chunk_size: int = 500,
) -> GoldenSetCoverage:
    """Return which of ``expected_ids`` the store does not hold.

    Lookups are chunked rather than issued per id: the same lesson the rekey
    tools learned, at a smaller scale -- a hundred round trips for a check that
    one of them answers.

    Ids whose ``source_table`` has no rows at all are reported separately from
    absent ids. A table the store never had is a different problem from a chunk
    that was renamed or deleted, and conflating them hides which one happened.
    """
    wanted = list(dict.fromkeys(expected_ids))
    accumulator = _Accumulator(expected=set(wanted))
    for start in range(0, len(wanted), chunk_size):
        chunk = wanted[start : start + chunk_size]
        found = _existing_keys(session, chunk)
        accumulator.missing.extend(key for key in chunk if key not in found)
    present_sources = _sources_present(session, (key.split(":", 1)[0] for key in accumulator.expected))
    unknown = tuple(sorted({key.split(":", 1)[0] for key in accumulator.expected} - present_sources))
    missing = tuple(accumulator.missing[:MISSING_SAMPLE_CAP])
    if accumulator.missing:
        logger.warning(
            "Golden set references %d id(s) absent from the store (showing up to %d)",
            len(accumulator.missing),
            MISSING_SAMPLE_CAP,
        )
    return GoldenSetCoverage(
        expected_count=len(wanted),
        missing_ids=missing,
        unknown_sources=unknown,
    )


def _existing_keys(session: Session, keys: Sequence[str]) -> set[str]:
    """Return which of ``keys`` name a chunk in the store.

    Built as a bound IN-list rather than ``= ANY(:rows)`` because ``ANY`` is a
    PostgreSQL array operator. The same check runs against SQLite in tests, and a
    dialect-specific helper would only ensure the untested path is the one that
    breaks first.
    """
    tables = sorted({key.split(":", 1)[0] for key in keys})
    found: set[str] = set()
    for table in tables:
        table_rows = [key.split(":", 1)[1] if ":" in key else key for key in keys if key.split(":", 1)[0] == table]
        if not table_rows:
            continue
        present = _lookup_rows(session, table, table_rows)
        found.update(f"{table}:{value}" for value in present)
    return found


def _lookup_rows(session: Session, table: str, row_ids: Sequence[str]) -> Sequence[str]:
    """Return which ``row_ids`` the store holds for one source table."""
    placeholders = ", ".join(f":row_{i}" for i in range(len(row_ids)))
    # Only the generated ":row_N" bind names are interpolated; every value stays bound.
    statement = text(
        f"SELECT source_row_id FROM rag_chunks WHERE source_table = :table AND source_row_id IN ({placeholders})"  # noqa: S608
    )
    params = {"table": table, **{f"row_{i}": value for i, value in enumerate(row_ids)}}
    return session.execute(statement, params).scalars().all()


def _sources_present(session: Session, tables: Sequence[str]) -> set[str]:
    """Return which source tables have rows at all."""
    unique = sorted({table for table in tables if table})
    if not unique:
        return set()
    placeholders = ", ".join(f":table_{i}" for i in range(len(unique)))
    # Only the generated ":table_N" bind names are interpolated; values stay bound.
    statement = text(f"SELECT DISTINCT source_table FROM rag_chunks WHERE source_table IN ({placeholders})")  # noqa: S608
    params = {f"table_{i}": value for i, value in enumerate(unique)}
    return set(session.execute(statement, params).scalars().all())
