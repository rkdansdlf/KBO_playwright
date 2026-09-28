"""What one game-detail attempt actually produced.

`GameDetailCrawler` returns a payload or nothing, and the caller recovers which
game failed by diffing the target list against the payload list and consulting
`_last_failure_reason`. That works, but it cannot answer the question a run
ledger needs: what happened to *this* game.

The deeper problem is that "nothing came back" is not one thing. A full detail
crawl can return a payload with no boxscore, which is worth storing and worth
re-fetching. A lightweight crawl can return the same payload as the thing it was
asked for. A third game can fail navigation outright. The current signature
collapses all three into `None`, so they are indistinguishable at the boundary.

This module names them. No framework, no database, just a vocabulary the crawler
can emit and the ledger can record:

    lightweight=True + score/metadata   -> SUCCESS   (degraded, but intended)
    full detail + hitters and pitchers  -> SUCCESS
    full detail + boxscore missing but
      a recovery anchor is present       -> PARTIAL   (storable, re-fetchable)
    no anchor, navigation or validation
      failure                            -> FAILED

The existing `GameDetailCrawler` recovery contract is preserved exactly: a
quality failure is still retryable, because the same page fetched again often
renders completely.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.result import CrawlOutcome, CrawlResult

#: Reasons the crawler records in ``_last_failure_reason``, mapped to the shared
#: taxonomy. Anything unmapped is treated as unknown rather than guessed at, so
#: an unexpected reason cannot masquerade as a specific, actionable cause.
_REASON_CODES: dict[str, FailureCode] = {
    "timeout": FailureCode.FETCH_TIMEOUT,
    "navigation_error": FailureCode.FETCH_HTTP_ERROR,
    "kbo_robots_blocked": FailureCode.FETCH_BLOCKED,
    "blocked": FailureCode.FETCH_BLOCKED,
    # The sections genuinely do not exist for a cancelled game, so re-fetching
    # cannot create them. This is why it maps here rather than to a quality code.
    "cancelled": FailureCode.PARSE_SELECTOR_MISSING,
    "incomplete_detail": FailureCode.VALIDATION_QUALITY,
    "hitter_totals_mismatch": FailureCode.VALIDATION_QUALITY,
    "inning_score_mismatch": FailureCode.VALIDATION_QUALITY,
    "exception": FailureCode.UNKNOWN,
}


class GameDetailStatus(StrEnum):
    """How a single game-detail attempt ended."""

    SUCCESS = "success"
    """The requested detail was obtained, including a lightweight result."""

    PARTIAL = "partial"
    """Storable but incomplete detail, worth keeping and worth re-fetching."""

    FAILED = "failed"
    """Nothing usable came back."""


@dataclass(frozen=True)
class GameDetailAttempt:
    """The durable outcome of one game-detail attempt.

    Attributes:
        game_id: KBO game ID, normalized.
        payload: The extracted detail, or None when the attempt failed.
        status: Which of the three outcomes this was.
        error_code: Failure taxonomy code. None for a success.
        error_message: Human-readable detail for logs and evidence.
        reason: The crawler's own failure reason, kept for compatibility with the
            existing ``game_collection_service`` vocabulary.
        source: Which source produced the payload, when known.

    """

    game_id: str
    status: GameDetailStatus
    payload: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    reason: str | None = None
    source: str | None = None

    @property
    def ok(self) -> bool:
        """Return whether the attempt produced a payload worth keeping."""
        return self.status is not GameDetailStatus.FAILED and self.payload is not None

    @property
    def needs_refetch(self) -> bool:
        """Return whether this game should be queued again."""
        return self.status in {GameDetailStatus.PARTIAL, GameDetailStatus.FAILED}


def error_code_for_reason(reason: str | None) -> str:
    """Map a crawler failure reason onto the shared taxonomy.

    Args:
        reason: The crawler's own reason string, e.g. ``"timeout"``.

    Returns:
        A canonical failure code, defaulting to ``UNKNOWN`` for anything the
        crawler recorded that this mapping does not recognise.

    """
    if not reason:
        return FailureCode.UNKNOWN.value
    return _REASON_CODES.get(reason.strip().lower(), FailureCode.UNKNOWN).value


def has_full_detail_rows(payload: dict[str, Any] | None) -> bool:
    """Return whether the payload carries both boxscores completely.

    Args:
        payload: An extracted detail payload.

    Returns:
        True when hitters and pitchers are present for both teams.

    """
    if not payload:
        return False
    hitters = payload.get("hitters") or {}
    pitchers = payload.get("pitchers") or {}
    return (
        bool(hitters.get("away"))
        and bool(hitters.get("home"))
        and bool(pitchers.get("away"))
        and bool(pitchers.get("home"))
    )


def has_partial_detail_anchor(payload: dict[str, Any] | None) -> bool:
    """Return whether the payload has enough context to store a degraded result.

    Mirrors ``game_collection_service._has_partial_detail_anchor``: both team
    codes, plus any score, line score, stadium, or attendance. Without one of
    these there is nothing to save, so the attempt is a failure rather than a
    partial.

    Args:
        payload: An extracted detail payload.

    Returns:
        True when the payload could be stored as a degraded detail.

    """
    if not payload:
        return False
    teams = payload.get("teams") or {}
    away = teams.get("away") or {}
    home = teams.get("home") or {}
    metadata = payload.get("metadata") or {}

    has_teams = bool(away.get("code")) and bool(home.get("code"))
    has_scores = (
        bool(away.get("line_score"))
        or bool(home.get("line_score"))
        or away.get("score") is not None
        or home.get("score") is not None
    )
    has_metadata = bool(metadata.get("stadium")) or bool(metadata.get("attendance"))
    return has_teams and (has_scores or has_metadata)


def classify_payload(
    payload: dict[str, Any] | None,
    *,
    lightweight: bool,
) -> GameDetailStatus:
    """Return what a payload means for the request that produced it.

    A lightweight crawl asked for score and metadata, so a payload without a
    boxscore is the answer rather than a shortfall. In full mode the same
    payload is a degraded result: worth storing, worth re-fetching.

    Args:
        payload: The extracted detail, or None.
        lightweight: Whether the request was a lightweight one.

    Returns:
        The status, or ``FAILED`` when there is no payload at all.

    """
    if not payload:
        return GameDetailStatus.FAILED
    if lightweight or has_full_detail_rows(payload):
        return GameDetailStatus.SUCCESS
    if has_partial_detail_anchor(payload):
        return GameDetailStatus.PARTIAL
    return GameDetailStatus.FAILED


def attempt_from_result(
    game_id: str,
    payload: dict[str, Any] | None,
    *,
    lightweight: bool,
    reason: str | None = None,
    source: str | None = None,
) -> GameDetailAttempt:
    """Build the durable outcome of one attempt from what the crawler produced.

    Args:
        game_id: The game this attempt was for.
        payload: The extracted detail, or None.
        lightweight: Whether the request was a lightweight one.
        reason: The crawler's own failure reason, if it recorded one.
        source: Which source produced the payload.

    Returns:
        The attempt, carrying a taxonomy code whenever it did not fully succeed.

    """
    status = classify_payload(payload, lightweight=lightweight)
    if status is GameDetailStatus.SUCCESS:
        return GameDetailAttempt(game_id=game_id, status=status, payload=payload, source=source)
    if status is GameDetailStatus.PARTIAL:
        # A partial carries no failure reason: the crawler only records one when
        # it gives up. `UNKNOWN` means "we could not classify this", which would
        # be a worse answer than the one we actually have -- the payload is
        # well-shaped and merely incomplete, and that is a quality failure.
        return GameDetailAttempt(
            game_id=game_id,
            status=status,
            payload=payload,
            error_code=FailureCode.VALIDATION_QUALITY.value,
            reason=reason,
            source=source,
        )
    return GameDetailAttempt(
        game_id=game_id,
        status=status,
        payload=payload,
        error_code=error_code_for_reason(reason),
        error_message=reason,
        reason=reason,
        source=source,
    )


#: Codes that describe "we could not classify this" rather than a cause, so they
#: never displace a real classification from another source.
_UNINFORMATIVE_CODES: frozenset[str] = frozenset(
    {
        FailureCode.UNKNOWN.value,
        "",
    }
)


def canonical_failure_code(
    primary: CrawlResult[Any] | None,
    fallback_reason: str | None,
) -> str:
    """Return the cause to record when both sources failed.

    The primary attempt only claims the cause when it actually has one:

    * a meaningful primary failure code wins, because a replay restarts there
    * a primary ``EMPTY`` is not a failure and has no claim at all -- the KBO
      page was needed precisely because Naver had no record
    * an uninformative primary code yields to the fallback's specific one
    * with neither, ``UNKNOWN`` is the honest answer

    Args:
        primary: The primary source's result, or None if it was not consulted.
        fallback_reason: The fallback's own failure reason, if it failed.

    Returns:
        A canonical failure code.

    """
    fallback_code = error_code_for_reason(fallback_reason)
    if primary is None or primary.ok or primary.outcome is CrawlOutcome.EMPTY:
        return fallback_code
    primary_code = primary.error_code or FailureCode.UNKNOWN.value
    if primary_code in _UNINFORMATIVE_CODES:
        return fallback_code
    return primary_code


@dataclass(frozen=True)
class GameDetailSources:
    """What each source reported for one game.

    Bundling them keeps the two outcomes travelling together, so a caller cannot
    record a primary failure while silently dropping the fallback's reason.

    Attributes:
        primary: The Naver record API's classified result, or None if it was not
            consulted.
        fallback_reason: The KBO GameCenter page's own failure reason, if it
            failed.
        source: Which source produced the payload, when one did.

    """

    primary: CrawlResult[Any] | None = None
    fallback_reason: str | None = None
    source: str | None = None

    @property
    def fallback_failed(self) -> bool:
        """Return whether the fallback was consulted and failed."""
        return self.fallback_reason is not None


def resolve_attempt(
    game_id: str,
    payload: dict[str, Any] | None,
    sources: GameDetailSources,
    *,
    lightweight: bool,
) -> GameDetailAttempt:
    """Build the final attempt for a game, given both sources' outcomes.

    A payload from either source is a success for the request that was made, so a
    primary failure is not carried into the record once the fallback produced
    something. Only a game that came back empty-handed from both is attributed,
    and then only to the source that actually knows why.

    Args:
        game_id: The game this attempt was for.
        payload: The extracted detail, or None.
        sources: What each source reported.
        lightweight: Whether the request was a lightweight one.

    Returns:
        The attempt for this game.

    """
    status = classify_payload(payload, lightweight=lightweight)
    if status is GameDetailStatus.SUCCESS:
        # Whichever source answered, the request was satisfied. Leaving the
        # primary's failure on the record would report a recovered game as broken.
        return GameDetailAttempt(game_id=game_id, status=status, payload=payload, source=sources.source)
    if status is GameDetailStatus.PARTIAL:
        return GameDetailAttempt(
            game_id=game_id,
            status=status,
            payload=payload,
            error_code=FailureCode.VALIDATION_QUALITY.value,
            source=sources.source,
        )
    return GameDetailAttempt(
        game_id=game_id,
        status=status,
        payload=payload,
        error_code=canonical_failure_code(sources.primary, sources.fallback_reason),
        error_message=sources.fallback_reason,
        reason=sources.fallback_reason,
        source=sources.source,
    )


__all__ = [
    "GameDetailAttempt",
    "GameDetailSources",
    "GameDetailStatus",
    "attempt_from_result",
    "canonical_failure_code",
    "classify_payload",
    "error_code_for_reason",
    "has_full_detail_rows",
    "has_partial_detail_anchor",
    "resolve_attempt",
]
