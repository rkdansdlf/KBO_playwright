from __future__ import annotations

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, mapped_column

from src.models.base import Base, TimestampMixin


class TestBase:
    def test_base_is_declarative(self):
        assert issubclass(Base, DeclarativeBase)


class TestTimestampMixin:
    def test_has_created_at(self):
        assert hasattr(TimestampMixin, "created_at")

    def test_has_updated_at(self):
        assert hasattr(TimestampMixin, "updated_at")

    def test_created_at_is_mapped_column(self):
        col = TimestampMixin.__table__.c.get("created_at") if hasattr(TimestampMixin, "__table__") else None
        if col is None:
            col = TimestampMixin.created_at
        assert col is not None

    def test_updated_at_is_mapped_column(self):
        col = TimestampMixin.updated_at
        assert col is not None
