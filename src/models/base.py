"""Base models and common types for SQLAlchemy ORM
"""

from __future__ import annotations

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models"""


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps"""

    created_at = mapped_column(DateTime, default=func.now(), server_default=func.now(), nullable=False)
    updated_at = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        server_default=func.now(),
        nullable=False,
    )
