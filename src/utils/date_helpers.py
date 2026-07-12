"""Shared date parsing utilities."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from src.constants import KST

if TYPE_CHECKING:
    from datetime import date


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
