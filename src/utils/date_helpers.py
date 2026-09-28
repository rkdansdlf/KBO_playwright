"""Shared date parsing utilities."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)


def parse_date_str(value: str, fmt: str = "%Y%m%d") -> date:
    """Parse date str.

    Args:
        value: Value.
        fmt: Fmt.
        value: Value.
        fmt: Fmt.
        value: Value.
        fmt: Fmt.

    Returns:
        date instance.

    """
    return datetime.strptime(value, fmt).replace(tzinfo=KST).date()


def parse_datetime_str(value: str, fmt: str = "%Y%m%d") -> datetime:
    """Parse datetime str.

    Args:
        value: Value.
        fmt: Fmt.
        value: Value.
        fmt: Fmt.
        value: Value.
        fmt: Fmt.

    Returns:
        datetime instance.

    """
    return datetime.strptime(value, fmt).replace(tzinfo=KST)


def parse_date_str_lenient(
    value: str,
    fmt: str = "%Y%m%d",
    *,
    fallback: date | None = None,
) -> date:
    """Parse a date string, substituting a fallback instead of raising.

    Scheduled jobs resolve their target dates from strings, and a job that
    raises on an unparseable date simply stops running -- silently, because the
    scheduler logs the failure and moves on. That is how a malformed value turns
    into a missing check with no incident and no alert. A lenient parse keeps
    the job alive and records why the value was rejected.

    Args:
        value: Value.
        fmt: Fmt.
        fallback: Date to use when `value` cannot be parsed. Defaults to the
            current KST date. Callers whose contract is anchored to a relative
            day (for example "yesterday") must pass that day here: a plain
            KST-today fallback would silently shift their window onto data
            that is not finished yet.

    Returns:
        date instance.

    """
    try:
        return datetime.strptime(value, fmt).replace(tzinfo=KST).date()
    except (TypeError, ValueError) as exc:
        resolved = fallback or datetime.now(KST).date()
        logger.warning(
            "Failed to parse date %r with format %r; falling back to %s: %s",
            value,
            fmt,
            resolved,
            exc,
        )
        return resolved


def normalize_to_date(value: str) -> date:
    """Normalize to date.

    Args:
        value: Value.
        value: Value.
        value: Value.

    Returns:
        date instance.

    """
    cleaned = value.replace("-", "").replace("/", "").replace(".", "")

    return datetime.strptime(cleaned, "%Y%m%d").replace(tzinfo=KST).date()
