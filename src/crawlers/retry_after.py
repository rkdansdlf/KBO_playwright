"""Parsing helpers for HTTP throttling response headers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime

MAX_RETRY_AFTER_SECONDS = 300.0
"""Upper bound applied to any server-provided retry delay."""

SECONDS_PER_DAY = 86_400


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse a `Retry-After` header into a bounded number of seconds.

    Supports both forms from RFC 9110: delay-seconds and an HTTP-date.
    Anything unparseable, negative, or absurdly long returns None so the
    caller falls back to its own backoff instead of trusting a bad value.

    Args:
        value: Raw header value.
        now: Reference time for the HTTP-date form. Defaults to the current
            UTC time.

    Returns:
        A delay in seconds within (0, MAX_RETRY_AFTER_SECONDS], or None.

    """
    if not value:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    try:
        seconds = float(candidate)
    except ValueError:
        return _parse_http_date(candidate, now=now)

    if seconds <= 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


def _parse_http_date(candidate: str, *, now: datetime | None) -> float | None:
    """Parse the HTTP-date form of `Retry-After` into a relative delay."""
    try:
        target = parsedate_to_datetime(candidate)
    except (TypeError, ValueError):
        return None
    if target is None:
        return None

    reference = now or datetime.now(UTC)
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)

    delta = target - reference
    if delta <= timedelta(0):
        return None
    return min(delta.total_seconds(), MAX_RETRY_AFTER_SECONDS)


__all__ = ["MAX_RETRY_AFTER_SECONDS", "parse_retry_after"]
