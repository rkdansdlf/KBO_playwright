"""Retry policy for the crawler dead letter queue.

Backoff contract (``retry_count`` is incremented *before* a replay runs):

    attempt #1 fails -> RETRY_SCHEDULE[0] = 60s
    attempt #2 fails -> RETRY_SCHEDULE[1] = 300s
    attempt #3 fails -> RETRY_SCHEDULE[2] = 900s
    attempt #4 fails -> RETRY_SCHEDULE[3] = 3600s
    attempt #5 fails -> exhausted

``DEFAULT_MAX_RETRIES`` is the default budget; a letter may carry its own
``max_retries``. The backoff index is clamped so a larger budget keeps repeating
the final delay instead of raising ``IndexError``.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MAX_RETRIES = 5
RETRY_SCHEDULE: tuple[int, ...] = (60, 300, 900, 3600)

RETRYABLE_CODES: frozenset[str] = frozenset(
    {
        "FETCH_TIMEOUT",
        "FETCH_HTTP_ERROR",
        "FETCH_RATE_LIMITED",
        "PERSIST_CONNECTION",
        "PERSIST_TIMEOUT",
        "SOURCE_PARTIAL",
        "UNKNOWN",
    },
)

NON_RETRYABLE_CODES: frozenset[str] = frozenset(
    {
        "FETCH_BLOCKED",
        "PARSE_SELECTOR_MISSING",
        "PARSE_INVALID_FORMAT",
        "PARSE_EMPTY",
        "VALIDATION_SCHEMA",
        "VALIDATION_QUALITY",
        "PERSIST_CONSTRAINT",
    },
)


@dataclass(frozen=True)
class RetryDecision:
    """Outcome of evaluating the retry policy for one failure."""

    retryable: bool
    delay_seconds: int | None
    reason: str


def _backoff_delay(retry_count: int) -> int:
    """Return the bounded backoff delay for an attempt count."""
    schedule_index = min(retry_count - 1, len(RETRY_SCHEDULE) - 1)
    return RETRY_SCHEDULE[schedule_index]


def decide(
    error_code: str,
    retry_count: int,
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> RetryDecision:
    """Return the retry decision for an error code at a given attempt count."""
    if str(error_code) not in RETRYABLE_CODES:
        return RetryDecision(retryable=False, delay_seconds=None, reason=f"non-retryable:{error_code}")
    if retry_count >= max_retries:
        return RetryDecision(retryable=False, delay_seconds=None, reason="max retries exhausted")
    delay = _backoff_delay(retry_count) if retry_count >= 1 else None
    return RetryDecision(retryable=True, delay_seconds=delay, reason="retryable")
