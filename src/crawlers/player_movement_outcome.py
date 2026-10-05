"""What reading one year of player movements means, before any ledger exists.

The movements page is a single document that a year selector and a search button
drive, and the table it fills is `.tbl-type02`. Three things can produce no rows,
and from outside the crawler they look identical:

    * the year genuinely recorded no transfers, which is most years;
    * the table is gone, so the page is no longer the document the crawl asked
      for; or
    * the page never loaded.

Only the last is retryable in the ordinary sense, and only the second is a fact
about the site rather than about this moment. The distinction matters because the
three are handled differently: a quiet year contributes nothing and raises
nothing, a drift must not spend the retry budget, and a fetch failure should.

The evidence for drift is the page's own controls, not an empty result. Selecting
a year is impossible without `#selYear` and `#btnSearch`, so their absence means
the crawl is driving a document it does not understand -- the same argument
`kbo_event_outcome` makes about the site frame, applied to the controls this
page is operated by.

This vocabulary is separate from :class:`~src.crawlers.result.CrawlOutcome` on
purpose. That one models an HTTP fetch; this crawler drives a browser and makes
none.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from src.crawlers.failure_taxonomy import FailureCode

#: Recorded when the page no longer carries the controls the crawl operates.
CONTROLS_MISSING_REASON = "controls_missing"

#: Recorded when the page could not be read at all.
PAGE_READ_FAILED_REASON = "page_read_failed"

SOURCE_PAGE = "player_movement_page"

#: Selectors that prove the page is still the document the crawl operates.
#: A year cannot be selected without them, so their absence is drift rather than
#: an empty result.
MOVEMENT_FRAME_SELECTORS = ("#selYear", "#btnSearch", ".tbl-type02")


class PlayerMovementStatus(StrEnum):
    """How one year of the movements page ended."""

    SUCCESS = "success"
    """The page was readable and carried at least one movement row."""

    EMPTY = "empty"
    """The page was readable and carried none.

    The expected state for most years. Alerting on it would fire every year of
    every off-season.
    """

    SCHEMA_CHANGED = "schema_changed"
    """The document is no longer the page the crawl expected.

    Never retryable: the same controls would be missing on the next attempt, and
    spending the retry budget delays the failures that would clear.
    """

    FETCH_FAILED = "fetch_failed"
    """The page could not be read at all.

    Retryable, and a fact about this moment rather than about the site.
    """


#: Page-level reasons mapped to the taxonomy. A reason is what the crawl
#: observed; a code is what the ledger, the queue and the metric all key on.
#: Values are (code, human explanation, terminal).
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool]] = {
    CONTROLS_MISSING_REASON: (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the page lost the year selector, the search button or the results table",
        True,
    ),
    PAGE_READ_FAILED_REASON: (
        FailureCode.FETCH_HTTP_ERROR,
        "the page could not be read",
        False,
    ),
}


@dataclass(frozen=True)
class PlayerMovementPageRead:
    """One year-page read and whatever rows the crawl kept from it."""

    status: PlayerMovementStatus
    reason: str | None = None
    rows: list[dict[str, object]] = field(default_factory=list)

    @property
    def is_terminal(self) -> bool:
        """Return whether retrying could still change this outcome.

        Terminal means the source answered and will keep answering the same way.
        A fetch failure is the opposite.
        """
        entry = _REASON_FAILURES.get(self.reason or "")
        return bool(entry and entry[2])


def classify_page_failure(reason: str, *, source: str = SOURCE_PAGE) -> tuple[str, bool]:
    """Return the taxonomy code and terminal flag for a page-level reason.

    Args:
        reason: The reason the crawl recorded.
        source: Where the reason came from, for the caller that enqueues.

    Returns:
        ``(code, terminal)``. An unknown reason is reported as a retryable
        fetch failure rather than being trusted as terminal: over-queueing a
        page is recoverable, silently dropping a drifted one is not.

    """
    entry = _REASON_FAILURES.get(reason)
    if entry is None:
        return FailureCode.FETCH_HTTP_ERROR.value, False
    del source  # The caller supplies it; kept in the signature for that caller.
    return entry[0].value, entry[2]


__all__ = [
    "CONTROLS_MISSING_REASON",
    "MOVEMENT_FRAME_SELECTORS",
    "PAGE_READ_FAILED_REASON",
    "SOURCE_PAGE",
    "PlayerMovementPageRead",
    "PlayerMovementStatus",
    "classify_page_failure",
]
