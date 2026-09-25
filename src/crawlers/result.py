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
    """The classified result of a single crawler request."""

    outcome: CrawlOutcome
    data: Any = None
    http_status: int | None = None
    attempts: int = 1
    elapsed_seconds: float = 0.0
    retry_after: float | None = None
    error: str | None = None
    url: str = ""

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
    def failure(
        cls,
        outcome: CrawlOutcome,
        *,
        error: str,
        http_status: int | None = None,
        retry_after: float | None = None,
        url: str = "",
    ) -> CrawlResult[Any]:
        """Build a failed result, rejecting outcomes that are not failures."""
        if outcome in {CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY}:
            message = f"{outcome} is not a failure outcome"
            raise ValueError(message)
        return cls(
            outcome=outcome,
            error=error,
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
            "url": self.url,
        }


__all__ = ["RETRYABLE_OUTCOMES", "CrawlOutcome", "CrawlResult"]
