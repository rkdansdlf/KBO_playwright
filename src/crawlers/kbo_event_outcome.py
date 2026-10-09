"""What reading one official-events page means, before any ledger exists.

The sweep visits seven standing KBO pages -- an MVP hall, a draft notice, a
media-day notice, a safety guide, a purchase guide. Most of them are not event
pages and are not supposed to link to an event, so "no candidates here" is the
correct answer for most of a healthy sweep. That is why an empty list cannot be
read as a failure, and why it cannot be read as success either: the two things
an empty page means are "the site has nothing running" and "the page is no
longer a page we can read", and they look identical from outside.

The distinguishing evidence is the site's own frame. Every page in this section
carries the same header, navigation and footer; if those are gone, the response
is no longer the document the sweep asked for, whatever text it carries. That is
the same argument `roster_transaction_crawler` makes before its desktop
fallback, and it is deliberately checked here rather than inferred from an empty
result.

This vocabulary is separate from :class:`~src.crawlers.result.CrawlOutcome` on
purpose. That one models an HTTP fetch and is reused as-is; this one states what
a *page* said, which the HTTP layer cannot know.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from src.crawlers.failure_taxonomy import FailureCode

SOURCE_PAGE = "kbo_event_page"

#: Reason keys. These are the vocabulary's own identifiers, so they live beside the
#: table that gives them meaning rather than in the crawler that happens to pass
#: them in -- the outcome module cannot name a fetch failure without importing its
#: own caller otherwise.
FRAME_MISSING_REASON = "site_frame_missing"
FETCH_FAILED_REASON = "page_fetch_failed"


class KboEventStatus(StrEnum):
    """How one page of the official-events sweep ended."""

    SUCCESS = "success"
    """The page was readable and carried at least one event candidate."""

    EMPTY = "empty"
    """The page was readable and carried none.

    The normal state for a standing guide page, and not a failure: an alert for
    it would fire on every off-season day.
    """

    SCHEMA_CHANGED = "schema_changed"
    """The document is no longer the page the sweep expected.

    Never retryable. Retrying returns the same unparseable document, and
    spending the retry budget on it delays the failures that would clear.
    """

    FETCH_FAILED = "fetch_failed"
    """The page could not be retrieved at all.

    Retryable, and distinct from drift: a connection error is a fact about this
    moment, while a missing frame is a fact about the site.
    """


#: Page-level reasons mapped to the taxonomy. A reason is what the crawl
#: observed; a code is what the ledger, the queue and the metric all key on.
#: Values are (code, human explanation, terminal).
#:
#: Three columns, matching `team_history_outcome` and `player_movement_outcome`.
#: A fourth "absence" column once sat here, `False` in every entry and unpacked
#: into `_absence` by its only caller. It named a distinction the module's
#: docstring argues at length -- an empty guide page and an unreadable one look
#: identical from outside -- and then discarded the evidence at the point of use.
#: A constant is not evidence, so the column only made the module read as though
#: absence tracking were implemented (BUG-005). The distinction itself is real
#: and still made, just not through a column that says so without meaning it.
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool]] = {
    FRAME_MISSING_REASON: (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the page lost the site header, navigation or footer",
        True,
    ),
    FETCH_FAILED_REASON: (
        FailureCode.FETCH_HTTP_ERROR,
        "the page could not be retrieved",
        False,
    ),
}


@dataclass(frozen=True)
class KboEventPageRead:
    """One page's outcome and whatever the sweep kept from it.

    `url` names the page this read describes. It is recorded on every path, not
    just failures: a reads list covering seven pages cannot say which page any
    entry refers to otherwise, and the sweep's own bookkeeping is a separate
    structure that a reader of the vocabulary does not have.
    """

    status: KboEventStatus
    reason: str | None = None
    events: list[dict[str, object]] = field(default_factory=list)
    url: str | None = None

    @property
    def is_terminal(self) -> bool:
        """Return whether this outcome can still change on its own.

        Terminal means "retrying will not help": the source answered and its
        answer will keep answering the same way. A fetch failure is the opposite
        and stays retryable.
        """
        entry = _REASON_FAILURES.get(self.reason or "")
        return bool(entry and entry[2])


def fetch_failed_read(url: str) -> KboEventPageRead:
    """Return the read for a page that could not be retrieved at all.

    Constructed by the crawler rather than by `read_kbo_event_page`, because the
    parser only ever receives a document: a page that never arrived produces no
    HTML to parse, so the state has no other way to exist.

    `FETCH_FAILED` was declared here from the start but nothing produced it --
    the crawler caught the exception, recorded a dead letter and returned, which
    is the correct handling and still left the state unreachable. That made the
    enum claim a distinction the code could not make, and left
    `_REASON_FAILURES["page_fetch_failed"]` looking like a reachable row when it
    was handled by a different branch entirely (BUG-006).

    Args:
        url: The page that could not be fetched, kept so the read can say which
            of the sweep's pages it describes.

    Returns:
        A non-terminal read carrying the fetch-failure reason.

    """
    return KboEventPageRead(status=KboEventStatus.FETCH_FAILED, reason=FETCH_FAILED_REASON, url=url)


def classify_page_failure(reason: str, *, source: str = SOURCE_PAGE) -> tuple[str, bool]:
    """Return the taxonomy code and terminality for a page-level reason.

    Args:
        reason: A key from :data:`_REASON_FAILURES`.
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
    "SOURCE_PAGE",
    "KboEventPageRead",
    "KboEventStatus",
    "classify_page_failure",
]
