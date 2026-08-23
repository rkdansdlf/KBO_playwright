"""SQLAlchemy types shared across database backends."""

from __future__ import annotations

from datetime import time
from typing import TYPE_CHECKING

from sqlalchemy import String, Time
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine import Dialect


class OracleCompatibleTime(TypeDecorator):
    """Store standalone time values as strings on Oracle."""

    impl = Time
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> Time | String:
        """Select a native time type or Oracle's string representation."""
        if dialect.name == "oracle":
            return dialect.type_descriptor(String(32))
        return dialect.type_descriptor(Time())

    def process_bind_param(self, value: time | str | None, dialect: Dialect) -> time | str | None:
        """Convert Python time values to Oracle-compatible bind values."""
        if value is None:
            return None
        if dialect.name == "oracle":
            return value.isoformat() if isinstance(value, time) else value
        if isinstance(value, str):
            return time.fromisoformat(value)
        return value

    def process_result_value(self, value: object, dialect: Dialect) -> time | object | None:
        """Convert Oracle time strings back to Python time values."""
        if value is None or dialect.name != "oracle" or isinstance(value, time):
            return value
        if isinstance(value, str):
            try:
                return time.fromisoformat(value)
            except ValueError:
                return value
        return value
