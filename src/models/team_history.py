"""
Team History model definition.

Tracks changes in team name, logo, city, etc. over time.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TeamHistory(Base, TimestampMixin):
    """
    Represents historical snapshots of a team's identity for a specific season.

    e.g. 1990 LG Twins, 1982 MBC Blue Dragons.
    """

    __tablename__ = "team_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    franchise_id: Mapped[int] = mapped_column(ForeignKey("team_franchises.id"), nullable=False)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    team_name: Mapped[str] = mapped_column(String(50), nullable=False)
    team_code: Mapped[str] = mapped_column(
        ForeignKey("teams.team_id"),
        nullable=False,
        comment="The code used in that season (e.g. OB)",
    )

    logo_url: Mapped[str | None] = mapped_column(String(255), nullable=True)
    ranking: Mapped[int | None] = mapped_column(Integer, nullable=True)

    stadium: Mapped[str | None] = mapped_column(String(50), nullable=True)
    city: Mapped[str | None] = mapped_column(String(30), nullable=True)
    color: Mapped[str | None] = mapped_column(String(50), nullable=True)

    def __repr__(self) -> str:
        """Returns a string representation of this object."""
        return f"<TeamHistory(season={self.season}, name='{self.team_name}')>"
