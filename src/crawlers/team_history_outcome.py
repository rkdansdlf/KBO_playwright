"""What reading the team-history page means, before any ledger exists.

One page carries every season: a table of years, each row holding a rank cell per
team slot. The failure this page fails in is not an exception and not an empty
table -- it is a table that is still there and no longer means anything. A year
row whose ``th`` is gone, or whose header no longer parses as a year, is skipped
one row at a time, so a redesign that breaks every row produces the same
``[]`` as a page that genuinely had nothing.

That ``[]`` is what used to be reported, and it is the worst shape this crawler
can produce: the run says it read the page, the ledger says success, nothing is
queued, and the table in the database keeps whatever it last held. A selector
that no longer matches is exactly the case where the data silently stops moving,
so the count of rows found has to be compared against the count actually parsed
before anyone is allowed to call it a quiet page.

This vocabulary is separate from :class:`~src.crawlers.result.CrawlOutcome` for
the same reason ``kbo_event_outcome`` is: that one models an HTTP fetch, and
this is a statement about what a browser-driven page said.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from src.crawlers.failure_taxonomy import FailureCode

SOURCE_HISTORY = "kbo_team_history"

#: Reason keys understood by :func:`classify_history_failure`.
NO_ROWS_REASON = "history_rows_missing"
NO_YEARS_REASON = "history_years_unreadable"


class TeamHistoryStatus(StrEnum):
    """How the team-history page ended."""

    SUCCESS = "success"
    """Rows were found and at least one season was parsed out of them."""

    EMPTY = "empty"
    """Rows were found, but none carried a readable year.

    Kept distinct from the two failures below because a page can legitimately
    arrive in this state -- an empty season table is not an outage, and alerting
    on it would fire on a quiet day.
    """

    SCHEMA_CHANGED = "schema_changed"
    """The table the sweep reads is no longer there, or is no longer readable.

    Never retryable. The same redesigned document comes back every time, so
    spending the retry budget on it only delays the failures that would clear.
    """

    FETCH_FAILED = "fetch_failed"
    """The page could not be retrieved at all.

    Retryable, and distinct from drift: a connection error is a fact about this
    moment, a missing table is a fact about the site.
    """


#: Page-level reasons mapped to the taxonomy. Values are
#: (code, human explanation, terminal).
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool]] = {
    NO_ROWS_REASON: (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the history table itself was not found on the page",
        True,
    ),
    NO_YEARS_REASON: (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the history table was found but no season row could be read",
        True,
    ),
}


@dataclass(frozen=True)
class TeamHistoryRead:
    """One read of the history page and what it came to."""

    status: TeamHistoryStatus
    reason: str | None = None
    rows_found: int = 0
    years_parsed: int = 0
    entries: list[dict] = field(default_factory=list)

    @property
    def rows_skipped(self) -> int:
        """Return how many rows the parser found and then could not use.

        This is the number that distinguishes a redesign from a quiet page: a
        healthy page skips the rows whose slots happen to be blank, but a broken
        one skips everything.
        """
        return max(0, self.rows_found - self.years_parsed)

    @property
    def is_terminal(self) -> bool:
        """Return whether this outcome can still change on its own."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return bool(entry and entry[2])


def read_team_history(*, rows_found: int, years_parsed: int, entries: list[dict] | None = None) -> TeamHistoryRead:
    """Say what a page read meant, given what it found and what it parsed.

    The two counts are the whole argument. A page that found no rows at all was
    not read; a page whose rows all failed to parse was read as nothing and is
    not the same claim; and a page that parsed seasons is a success even when
    most individual rows were blank, because blank slots are how this table is
    supposed to look -- 1983 has six ranks and no team names on the live page,
    and that is a fact about the season rather than a broken selector.
    """
    if rows_found == 0:
        return TeamHistoryRead(
            status=TeamHistoryStatus.SCHEMA_CHANGED,
            reason=NO_ROWS_REASON,
            rows_found=0,
            years_parsed=0,
        )
    if years_parsed == 0:
        return TeamHistoryRead(
            status=TeamHistoryStatus.EMPTY,
            reason=NO_YEARS_REASON,
            rows_found=rows_found,
            years_parsed=0,
        )
    return TeamHistoryRead(
        status=TeamHistoryStatus.SUCCESS,
        rows_found=rows_found,
        years_parsed=years_parsed,
        entries=list(entries or []),
    )


def classify_history_failure(reason: str, *, source: str = SOURCE_HISTORY) -> tuple[str, bool]:
    """Return the taxonomy code and terminality for a page-level reason.

    Args:
        reason: A key from the module's reason constants.
        source: The crawler name recorded alongside the code.

    Returns:
        The failure code value and whether the failure is terminal.

    """
    entry = _REASON_FAILURES.get(reason)
    if entry is None:
        return FailureCode.UNKNOWN.value, False
    code, _explanation, terminal = entry
    del source  # The taxonomy code is the same wherever the page was reached from.
    return code.value, terminal


__all__ = [
    "NO_ROWS_REASON",
    "NO_YEARS_REASON",
    "SOURCE_HISTORY",
    "TeamHistoryRead",
    "TeamHistoryStatus",
    "classify_history_failure",
    "read_team_history",
]
