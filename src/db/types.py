"""SQLAlchemy types shared across database backends."""

from __future__ import annotations

from datetime import time
from typing import TYPE_CHECKING

from sqlalchemy import Time
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect


class PortableTime(TypeDecorator):
    """Native ``TIME`` column that also accepts ISO-8601 strings on bind."""

    impl = Time
    cache_ok = True

    def process_bind_param(self, value: time | str | None, _dialect: Dialect) -> time | None:
        """Convert ISO time strings to :class:`datetime.time` before binding."""
        if value is None:
            return None
        if isinstance(value, str):
            return time.fromisoformat(value)
        return value
