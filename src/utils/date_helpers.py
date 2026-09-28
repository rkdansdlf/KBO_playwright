"""Shared date parsing utilities."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, cast

from src.constants import DATE_STR_LEN, KST

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)

#: Sentinel for `parse_date_str_lenient(fallback=...)` to reject malformed input
#: instead of substituting a date. Distinguishing "no fallback given" from
#: "substitute the current KST date" matters: a gate that filters on a typo must
#: fail loudly, while a scheduler job that must keep running substitutes.
RAISE_ON_UNPARSABLE: date = cast("date", object())


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
    """Parse a compact (``YYYYMMDD``) or ISO (``YYYY-MM-DD``) date string.

    Gate helpers in this repository receive both forms depending on the caller
    (``date.isoformat()`` vs ``date.strftime("%Y%m%d")``), so they must not
    assume a single wire format. A value matching `fmt` exactly is parsed
    directly; anything else falls back to the separator-stripping normalizer.

    Unparseable input does not raise when `fallback` is supplied: scheduled jobs
    resolve their target dates from strings, and a job that raises simply stops
    running -- silently, because the scheduler logs the failure and moves on.
    That is how a malformed value becomes a missing check with no incident and
    no alert. Without `fallback` the previous raising behaviour is kept, so
    callers that must reject bad input still can.

    Args:
        value: Date string in compact or ISO form.
        fmt: Format for a directly parseable value.
        fallback: Date to substitute when `value` cannot be parsed. Defaults to
            the current KST date. Callers whose contract is anchored to a
            relative day (for example "yesterday") must pass that day here: a
            plain KST-today fallback would silently shift their window onto data
            that is not finished yet. Pass `RAISE_ON_UNPARSABLE` to reject
            malformed input instead of substituting.

    Returns:
        date instance.

    """
    try:
        cleaned = value.strip()
        if fmt == "%Y%m%d" and len(cleaned) == DATE_STR_LEN and cleaned.isdigit():
            return parse_date_str(cleaned)
        if fmt == "%Y%m%d":
            return normalize_to_date(cleaned)
        return datetime.strptime(cleaned, fmt).replace(tzinfo=KST).date()
    except (AttributeError, TypeError, ValueError) as exc:
        if fallback is RAISE_ON_UNPARSABLE:
            msg = f"cannot parse {value!r} as a date"
            raise ValueError(msg) from exc
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
