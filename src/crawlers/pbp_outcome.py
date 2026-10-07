"""Typed outcomes for one game's KBO play-by-play read.

``pbp_crawler`` drives a browser, so it has no HTTP request to describe with
``CrawlResult``. What it does have is a page read, and that read has more
outcomes than "returned events" and "returned nothing": the crawl can be
blocked by robots.txt, redirected to the login page, find a live-text document
that is no longer the one it reads, or answer with a genuinely empty game.
Collapsing them into ``None`` -- which is what the crawler used to return --
made a blocked crawl indistinguishable from a rained-out game, and the two call
for opposite responses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from src.crawlers.failure_taxonomy import FailureCode


class PbpStatus(StrEnum):
    """How one game's play-by-play read ended."""

    SUCCESS = "success"
    """The page was readable and carried the game's plays."""

    EMPTY = "empty"
    """The page was readable and the game has no plays to show.

    The normal state before first pitch and for a cancelled game, so it is not a
    failure: the caller knows the game's state and the crawler does not, which is
    why this stays distinct from an unreadable page rather than being folded
    into one of the failure statuses.
    """

    SCHEMA_CHANGED = "schema_changed"
    """The live-text document no longer matches what the crawl reads.

    Terminal. Retrying returns the same document, and spending the retry budget
    on it delays the failures that would clear.
    """

    AUTH_REQUIRED = "auth_required"
    """The crawl was redirected to the login or error page.

    Terminal for the same reason: without credentials a retry lands on the same
    redirect. Distinct from a fetch failure because the answer is an operator
    decision, not patience.
    """

    FETCH_FAILED = "fetch_failed"
    """The page could not be retrieved at all.

    Retryable, and distinct from drift: a pool or connection error is a fact
    about this moment, while a missing document is a fact about the site.
    """


#: Reason keys mapped to their taxonomy code and terminality. A reason is what
#: the crawl observed; a code is what the ledger, the queue and the metric key
#: on, so the mapping lives here rather than at each observation site.
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool]] = {
    "compliance_blocked": (
        FailureCode.FETCH_BLOCKED,
        "the live-text page is disallowed by robots.txt",
        True,
    ),
    "auth_required": (
        FailureCode.FETCH_BLOCKED,
        "the crawl was redirected to the login or error page",
        True,
    ),
    "document_changed": (
        FailureCode.PARSE_SELECTOR_MISSING,
        "the live-text document no longer matches the one the crawl reads",
        True,
    ),
    "crawl_error": (
        FailureCode.FETCH_HTTP_ERROR,
        "the play-by-play page could not be read",
        False,
    ),
    "pool_error": (
        FailureCode.FETCH_HTTP_ERROR,
        "the browser pool could not serve the page",
        False,
    ),
}


@dataclass(frozen=True)
class PbpGameRead:
    """One game's outcome together with whatever the crawl kept from it."""

    status: PbpStatus
    reason: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def is_terminal(self) -> bool:
        """Return whether this outcome can still change on its own.

        Terminal means "retrying will not help": the site answered and its answer
        will keep answering the same way. A fetch failure is the opposite and
        stays retryable.
        """
        entry = _REASON_FAILURES.get(self.reason or "")
        return bool(entry and entry[2])

    @property
    def error_code(self) -> str | None:
        """Return the taxonomy code for a failure, or ``None`` when readable."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return entry[0].value if entry else None

    @property
    def explanation(self) -> str | None:
        """Return the human-readable detail for the reason, for the ledger."""
        entry = _REASON_FAILURES.get(self.reason or "")
        return entry[1] if entry else None


def classify_game_failure(reason: str) -> tuple[str, bool]:
    """Return the taxonomy code and terminality for a game-level reason.

    Args:
        reason: A key from :data:`_REASON_FAILURES`.

    Returns:
        The failure code value and whether the failure is terminal. An unknown
        reason is reported as ``UNKNOWN`` and retryable, so a new observation
        site that forgets to register here degrades to "try again" rather than
        to "give up".

    """
    entry = _REASON_FAILURES.get(reason)
    if entry is None:
        return FailureCode.UNKNOWN.value, False
    return entry[0].value, entry[2]


__all__ = [
    "PbpGameRead",
    "PbpStatus",
    "classify_game_failure",
]
