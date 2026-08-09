"""ORM models for Futures League schedule and team standings."""

from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class FuturesGameSchedule(Base, TimestampMixin):
    """Futures League Game Schedule."""

    __tablename__ = "futures_game_schedules"

    __table_args__ = (UniqueConstraint("game_id", name="uq_futures_game_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    game_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    game_id: Mapped[str] = mapped_column(String(50), nullable=False)

    away_team: Mapped[str] = mapped_column(String(20), nullable=False)
    home_team: Mapped[str] = mapped_column(String(20), nullable=False)
    stadium: Mapped[str | None] = mapped_column(String(50), nullable=True)

    away_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_score: Mapped[int | None] = mapped_column(Integer, nullable=True)

    game_status: Mapped[str] = mapped_column(String(20), nullable=False, default="SCHEDULED")
    cancel_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)

    def __repr__(self) -> str:
        """Return representation string."""
        return f"<FuturesGameSchedule(id={self.game_id}, date={self.game_date}, {self.away_team}@{self.home_team})>"


class FuturesTeamStandings(Base, TimestampMixin):
    """Futures League Division Team Standings."""

    __tablename__ = "futures_team_standings"

    __table_args__ = (UniqueConstraint("season", "division", "team_code", name="uq_futures_team_standings"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    division: Mapped[str] = mapped_column(String(20), nullable=False, comment="북부/남부")
    team_code: Mapped[str] = mapped_column(String(20), nullable=False)

    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    draws: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    win_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    games_behind: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        """Return representation string."""
        return f"<FuturesTeamStandings(season={self.season}, div={self.division}, team={self.team_code})>"
