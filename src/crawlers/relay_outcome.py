"""What one relay attempt actually produced.

`RelayCrawler` returns a payload or `None`, and the caller recovers what went
wrong by reading `_last_failure_reason`. That cannot answer the questions a run
ledger or a dead-letter queue needs, because "nothing came back" is four
different things that want three different responses:

    a Naver game that has no relay at all       -> terminal, never retry
    a relay that has not changed since last run -> terminal, nothing to write
    an inning loop that ended because the game ended -> terminal, and a success
    a fetch that failed, or a payload we could not match -> retry

The important asymmetry is the first one. A game the public source simply does
not carry will never acquire relay data, so queuing it spends the whole retry
budget on a game that cannot change and leaves a dead letter behind. Reporting
that as a failure is what the old `None` + reason string could not avoid.

The vocabulary here is deliberately source-aware: a 404 from the relay endpoint
means the relay is absent, while a 404 from the schedule endpoint means we could
not look the game up. Same status, opposite consequences, and the old code could
not tell them apart.

`crawl_game_relay` keeps its original signature and return shape. This module
describes the same outcomes more precisely; it does not change what the crawler
writes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from src.crawlers.failure_taxonomy import FailureCode

#: Downstream recovery buckets, kept identical to the names
#: `relay_recovery_service` already reports so the two stay comparable.
BUCKET_MATCH_FAILED = "relay_match_failed"
BUCKET_API_FAILED = "relay_api_failed"
BUCKET_EMPTY = "relay_empty"

#: Which endpoint a reason came from. A status code means different things to
#: each of them, so the source cannot be inferred from the reason alone.
SOURCE_RELAY = "relay"
SOURCE_SCHEDULE = "schedule"
SOURCE_UNKNOWN = "unknown"


class InningStop(StrEnum):
    """What ended the inning loop, which the payload alone cannot say."""

    COMPLETED = "completed"
    """Every inning requested returned data."""

    EMPTY_INNING = "empty_inning"
    """An inning came back with no relays at all.

    Expected at the end of a game, and indistinguishable from a real absence at
    the first inning -- which is why the inning that saw it has to be recorded.
    """

    TERMINAL_MARKER = "terminal_marker"
    """An inning came back with entries but no text options.

    The source's own end-of-game marker: entries exist, but nothing to report.
    """

    FETCH_FAILED = "fetch_failed"
    """A request did not come back."""

    INNINGS_EXHAUSTED = "innings_exhausted"
    """The loop ran out of innings to ask for, with data in hand."""


class RelayStatus(StrEnum):
    """How one relay attempt ended."""

    SUCCESS = "success"
    """Relay data was obtained and carries content."""

    NOT_MODIFIED = "not_modified"
    """The payload matches what is already stored.

    A successful no-op. It must not be queued, and it must not overwrite the
    rows it matched.
    """

    EMPTY = "empty"
    """The source genuinely has no relay for this game.

    Terminal by definition: the public source does not carry it, so another
    attempt cannot produce it. Recorded so the absence is explainable, never
    retried.
    """

    FAILED = "failed"
    """Nothing usable came back, and another attempt might do better."""


@dataclass(frozen=True)
class RelayFailure:
    """A classified relay failure.

    Attributes:
        code: The failure taxonomy code.
        message: Human-readable detail for logs and evidence.
        terminal: True when re-fetching cannot change the outcome. A terminal
            failure must never reach the dead-letter queue.
        bucket: The downstream recovery bucket this maps onto.

    """

    code: str
    message: str
    terminal: bool
    bucket: str


#: Reason to (code, message, terminal, bucket). A table rather than a chain of
#: branches so the vocabulary reads in one place, and so an unrecognised reason
#: is visibly absent from the table instead of hiding in a last branch.
_REASON_FAILURES: dict[str, tuple[FailureCode, str, bool, str]] = {
    # Refused on purpose. Another attempt now would be another refusal.
    "blocked": (
        FailureCode.FETCH_BLOCKED,
        "relay request blocked by compliance policy",
        True,
        BUCKET_API_FAILED,
    ),
    # The request failed before a payload was parsed.
    "relay_api_error": (
        FailureCode.FETCH_HTTP_ERROR,
        "relay request failed before a payload was parsed",
        False,
        BUCKET_API_FAILED,
    ),
    "relay_request_failed": (
        FailureCode.FETCH_HTTP_ERROR,
        "relay request failed before a payload was parsed",
        False,
        BUCKET_API_FAILED,
    ),
    # The schedule listed games and none of them was this one. Worth another
    # attempt: the match is scored on time and stadium, and a late-updating
    # schedule can change the answer.
    "invalid_relay_match": (
        FailureCode.VALIDATION_QUALITY,
        "no schedule entry matched this game",
        False,
        BUCKET_MATCH_FAILED,
    ),
    "relay_match_failed": (
        FailureCode.VALIDATION_QUALITY,
        "no schedule entry matched this game",
        False,
        BUCKET_MATCH_FAILED,
    ),
    # The schedule query succeeded and carried no games for this date, so the
    # source does not have this game at all.
    "relay_not_found": (
        FailureCode.PARSE_SELECTOR_MISSING,
        "schedule carried no games for this game",
        True,
        BUCKET_EMPTY,
    ),
    # Events were fetched but produced neither events nor rows: well formed and
    # thin, which is often a mid-render capture rather than a permanent defect.
    "relay_empty": (
        FailureCode.VALIDATION_QUALITY,
        "relay payload produced no events or rows",
        False,
        BUCKET_EMPTY,
    ),
    # A response arrived but did not parse. The shape changed, so the same
    # request returns the same unusable body.
    "relay_schema_drift": (
        FailureCode.PARSE_INVALID_FORMAT,
        "relay response was not in the expected shape",
        True,
        BUCKET_EMPTY,
    ),
    "relay_invalid_payload": (
        FailureCode.PARSE_INVALID_FORMAT,
        "relay response was not in the expected shape",
        True,
        BUCKET_EMPTY,
    ),
}


def _failure(code: FailureCode, message: str, *, terminal: bool, bucket: str) -> RelayFailure:
    return RelayFailure(code=code.value, message=message, terminal=terminal, bucket=bucket)


def _status_failure(status: str, *, source: str) -> RelayFailure:
    """Classify a bare HTTP status, which means different things per endpoint."""
    if status == "404" and source == SOURCE_RELAY:
        # The endpoint is fine and the relay is not there. This is the case that
        # must never be retried: the public source does not carry it.
        return _failure(
            FailureCode.PARSE_SELECTOR_MISSING,
            "relay endpoint reports no relay (status 404)",
            terminal=True,
            bucket=BUCKET_EMPTY,
        )
    return _failure(
        FailureCode.FETCH_HTTP_ERROR,
        f"relay request returned status {status}",
        terminal=False,
        bucket=BUCKET_API_FAILED,
    )


def classify_relay_failure(reason: str | None, *, source: str = SOURCE_UNKNOWN) -> RelayFailure:
    """Classify one of the crawler's own reason strings.

    Args:
        reason: The crawler's reason, e.g. ``"relay_api_error"``.
        source: Which endpoint produced it. A 404 from the relay endpoint means
            the relay is absent; the same status from the schedule endpoint means
            the lookup failed, and those want opposite handling.

    Returns:
        The classified failure. An unrecognised reason is treated as unknown
        rather than guessed at, so a new reason cannot masquerade as a specific
        actionable cause.

    """
    value = str(reason or "").strip().lower()
    spec = _REASON_FAILURES.get(value)
    if spec is not None:
        code, message, terminal, bucket = spec
        return _failure(code, message, terminal=terminal, bucket=bucket)
    if value.startswith("http_"):
        return _status_failure(value.removeprefix("http_"), source=source)
    return _failure(
        FailureCode.UNKNOWN,
        f"unclassified relay failure: {value or '<none>'}",
        terminal=False,
        bucket=BUCKET_API_FAILED,
    )


def is_terminal(reason: str | None, *, source: str = SOURCE_UNKNOWN) -> bool:
    """Return whether a reason describes an outcome retrying cannot change."""
    return classify_relay_failure(reason, source=source).terminal


@dataclass(frozen=True)
class AttemptSeed:
    """Everything one relay attempt produced, before it is classified.

    Bundled rather than passed as nine arguments so the fields stay named at the
    call site and adding one does not change every caller.
    """

    status: RelayStatus
    result: dict[str, Any] | None = None
    reason: str | None = None
    source: str = SOURCE_UNKNOWN
    naver_game_id: str | None = None
    innings_fetched: int = 0
    stop: InningStop = InningStop.COMPLETED
    resolution_attempted: bool = False


@dataclass(frozen=True)
class RelayAttempt:
    """The durable outcome of one relay attempt.

    Attributes:
        game_id: KBO game ID, normalized.
        status: Which of the four outcomes this was.
        result: The crawler result dict, for a success or a no-op.
        error_code: Taxonomy code. None for anything that is not a failure.
        error_message: Human-readable detail.
        reason: The crawler's own reason string, kept for compatibility with the
            existing `relay_recovery_service` vocabulary.
        naver_game_id: The Naver identifier actually fetched, when one was.
        innings_fetched: How many innings contributed data.
        stop: What ended the inning loop.
        resolution_attempted: Whether the schedule lookup ran, which separates
            "the source has nothing" from "we could not find it".

    """

    game_id: str
    status: RelayStatus
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    reason: str | None = None
    naver_game_id: str | None = None
    innings_fetched: int = 0
    stop: InningStop = InningStop.COMPLETED
    resolution_attempted: bool = False
    alternatives: tuple[str, ...] = field(default_factory=tuple)


def build_attempt(game_id: str, seed: AttemptSeed) -> RelayAttempt:
    """Build the typed attempt for a game.

    A terminal failure becomes `EMPTY` rather than staying `FAILED`, because "the
    source has nothing" and "we could not get it" must not share a status: the
    first is never retried and the second always is. It carries no error code,
    since there is no failure to retry.
    """
    if seed.status is not RelayStatus.FAILED or seed.reason is None:
        return RelayAttempt(
            game_id=game_id,
            status=seed.status,
            result=seed.result,
            reason=seed.reason,
            naver_game_id=seed.naver_game_id,
            innings_fetched=seed.innings_fetched,
            stop=seed.stop,
            resolution_attempted=seed.resolution_attempted,
        )

    failure = classify_relay_failure(seed.reason, source=seed.source)
    if failure.terminal:
        return RelayAttempt(
            game_id=game_id,
            status=RelayStatus.EMPTY,
            result=seed.result,
            error_message=failure.message,
            reason=seed.reason,
            naver_game_id=seed.naver_game_id,
            innings_fetched=seed.innings_fetched,
            stop=seed.stop,
            resolution_attempted=seed.resolution_attempted,
        )
    return RelayAttempt(
        game_id=game_id,
        status=seed.status,
        result=seed.result,
        error_code=failure.code,
        error_message=failure.message,
        reason=seed.reason,
        naver_game_id=seed.naver_game_id,
        innings_fetched=seed.innings_fetched,
        stop=seed.stop,
        resolution_attempted=seed.resolution_attempted,
    )


__all__ = [
    "BUCKET_API_FAILED",
    "BUCKET_EMPTY",
    "BUCKET_MATCH_FAILED",
    "SOURCE_RELAY",
    "SOURCE_SCHEDULE",
    "SOURCE_UNKNOWN",
    "AttemptSeed",
    "InningStop",
    "RelayAttempt",
    "RelayFailure",
    "RelayStatus",
    "build_attempt",
    "classify_relay_failure",
    "is_terminal",
]
