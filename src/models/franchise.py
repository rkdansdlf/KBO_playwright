"""Franchise model definition.
"""

from __future__ import annotations

from sqlalchemy import JSON, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Franchise(Base, TimestampMixin):
    """Represents a KBO Franchise (the entity behind the persistent technical ID).
    e.g., The 'SK' technical ID represents the franchise that is now 'SSG'.
    """

    __tablename__ = "team_franchises"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), nullable=False)
    original_code: Mapped[str] = mapped_column(
        String(10),
        unique=True,
        nullable=False,
        comment="KBO game_id technical segment",
    )
    current_code: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="Active code, or final code for dissolved franchises",
    )

    # New Fields for Phase 7
    metadata_json: Mapped[dict] = mapped_column(JSON, nullable=True, comment="Owner, CEO, Found Date, etc.")
    web_url: Mapped[str] = mapped_column(String(255), nullable=True, comment="KBO Team Info URL")

    def __repr__(self) -> str:
        return f"<Franchise(code='{self.original_code}', current='{self.current_code}')>"
