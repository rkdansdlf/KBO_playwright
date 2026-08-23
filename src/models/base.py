"""Base models and common types for SQLAlchemy ORM."""

from __future__ import annotations

from typing import Any

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model instance column values to a dictionary."""
        excluded = exclude or set()
        columns = getattr(self.__table__, "columns", None)
        if columns is None:
            return {}
        return {
            col.name: getattr(self, col.name) for col in columns if col.name not in excluded and hasattr(self, col.name)
        }

    def __repr__(self) -> str:
        """Provide a clean string representation with class name and primary key values."""
        cls_name = self.__class__.__name__
        columns = getattr(self.__table__, "columns", None)
        if columns is None:
            return f"<{cls_name}>"
        pk_cols = [c.name for c in columns if c.primary_key]
        if not pk_cols:
            pk_cols = [c.name for c in list(columns)[:2]]
        attrs = [f"{k}={getattr(self, k, None)!r}" for k in pk_cols if hasattr(self, k)]
        return f"<{cls_name}({', '.join(attrs)})>"


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""

    created_at = mapped_column(DateTime, default=func.now(), server_default=func.now(), nullable=False)
    updated_at = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        server_default=func.now(),
        nullable=False,
    )
