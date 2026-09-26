"""Classified outcomes for crawler HTTP requests.

A crawler request does not have two outcomes. KBO sites return HTTP 200 with
an empty table, throttle with 429, or silently replace their HTML with a
different structure. Collapsing all of those into `None` makes an outage
indistinguishable from an empty season, so every request reports a
`CrawlOutcome` instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class CrawlOutcome(StrEnum):
    """Why a crawler request ended the way it did."""

    SUCCESS = "SUCCESS"
    """The target answered and the payload parsed."""

    EMPTY = "EMPTY"
    """The target answered correctly and there is genuinely no data.

    Distinct from failure: an empty off-season day must not raise an alert.
    """

    RETRYABLE_ERROR = "RETRYABLE_ERROR"
    """A transient fault worth retrying: 429, 5xx, timeout, connection reset."""

    SCHEMA_CHANGED = "SCHEMA_CHANGED"
    """The target answered 200 but no longer matches the expected structure.

    This is the selector-drift signal. Retrying will not help; a human needs
    to know the page markup moved.
    """

    PERMANENT_ERROR = "PERMANENT_ERROR"
    """A fault that will not resolve on retry: 4xx other than 429, or a blocked URL."""


#: Outcomes that justify another attempt against the same target.
RETRYABLE_OUTCOMES = frozenset({CrawlOutcome.RETRYABLE_ERROR})


@dataclass(frozen=True)
class CrawlResult[T]:
    """The classified result of a single crawler request.

    `error_code` carries the failure taxonomy code for a failed request. It is
    attached at the point where the failure is best understood -- the transport
    that saw the timeout or the status -- rather than recovered later by parsing
    `error`. Re-parsing an error string is how the same failure ends up with
    three different names in the ledger, the dead letter queue, and the metrics.
    """

    outcome: CrawlOutcome
    data: Any = None
    http_status: int | None = None
    attempts: int = 1
    elapsed_seconds: float = 0.0
    retry_after: float | None = None
    error: str | None = None
    error_code: str | None = None
    url: str = ""

    def __post_init__(self) -> None:
        """Reject a classification that contradicts the outcome.

        A failure is allowed to have no `error_code`: results built before the
        taxonomy was attached still need to normalize, so `None` means "unknown
        to the producer" rather than "not a failure". A success carrying a code
        is a bug, because it would be counted as a failure downstream.
        """
        if self.outcome in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY} and self.error_code is not None:
            message = f"{self.outcome} cannot carry a failure error_code (got {self.error_code!r})"
            raise ValueError(message)

    @property
    def ok(self) -> bool:
        """Return whether the request produced usable data.

        `EMPTY` is not `ok`: the caller still has to decide what an empty
        season means for its own write path.
        """
        return self.outcome is CrawlOutcome.SUCCESS

    @property
    def should_retry(self) -> bool:
        """Return whether another attempt is worthwhile."""
        return self.outcome in RETRYABLE_OUTCOMES

    @classmethod
    def success(
        cls,
        data: Any,  # noqa: ANN401 - JSON payloads are untyped by nature
        *,
        http_status: int | None = None,
        retry_after: float | None = None,
        url: str = "",
    ) -> CrawlResult[Any]:
        """Build a successful result."""
        return cls(
            outcome=CrawlOutcome.SUCCESS,
            data=data,
            http_status=http_status,
            retry_after=retry_after,
            url=url,
        )

    @classmethod
    def empty(
        cls,
        *,
        http_status: int | None = None,
        url: str = "",
    ) -> CrawlResult[Any]:
        """Build a result for a target that answered with no records."""
        return cls(outcome=CrawlOutcome.EMPTY, http_status=http_status, url=url)

    @classmethod
    def failure(  # noqa: PLR0913 - public builder; the keyword-only call shape is the API
        cls,
        outcome: CrawlOutcome,
        *,
        error: str,
        error_code: str | None = None,
        http_status: int | None = None,
        retry_after: float | None = None,
        url: str = "",
    ) -> CrawlResult[Any]:
        """Build a failed result, rejecting outcomes that are not failures.

        Args:
            outcome: A failure outcome.
            error: Human-readable detail, for logs and evidence.
            error_code: Failure taxonomy code. Optional because a producer that
                predates the taxonomy still has to be normalizable; prefer
                passing it whenever the cause is known.
            http_status: Response status, when there was one.
            retry_after: Server-requested retry delay.
            url: Target URL.

        Returns:
            A failed result.

        """
        if outcome in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY}:
            message = f"{outcome} is not a failure outcome"
            raise ValueError(message)
        return cls(
            outcome=outcome,
            error=error,
            error_code=error_code,
            http_status=http_status,
            retry_after=retry_after,
            url=url,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result for logs and evidence artifacts."""
        return {
            "outcome": str(self.outcome),
            "http_status": self.http_status,
            "attempts": self.attempts,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "retry_after": self.retry_after,
            "error": self.error,
            "error_code": self.error_code,
            "url": self.url,
        }


__all__ = ["RETRYABLE_OUTCOMES", "CrawlOutcome", "CrawlResult"]
