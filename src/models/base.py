"""
Base models and common types for SQLAlchemy ORM
"""
from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import String, Integer, DateTime, func
from datetime import datetime


class Base(DeclarativeBase):
    """Base class for all ORM models"""
    pass


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps"""
    created_at = mapped_column(DateTime, default=func.now(), nullable=False)
    updated_at = mapped_column(
        DateTime,
        default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
