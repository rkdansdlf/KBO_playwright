"""
ORM models for daily team standings and magic numbers.
"""
from __future__ import annotations
from datetime import date
from typing import Optional

from sqlalchemy import Integer, String, Date, Float, UniqueConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin

class TeamStandingsDaily(Base, TimestampMixin):
    """
    Daily snapshot of Team Standings.
    Calculated from Game results.
    """
    __tablename__ = "team_standings_daily"
    __table_args__ = (
        UniqueConstraint("standings_date", "team_code", name="uq_team_standings_daily"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    standings_date: Mapped[date] = mapped_column(Date, nullable=False, index=True, comment="상태 기준일")
    team_code: Mapped[str] = mapped_column(String(10), nullable=False, comment="팀 코드 (ex: KIA)")
    
    games_played: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    draws: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    
    win_pct: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    games_behind: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    
    current_streak: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="+N 연승, -N 연패")
    
    runs_scored: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    runs_allowed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    run_differential: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    def __repr__(self) -> str:
        return f"<TeamStandingsDaily(date={self.standings_date}, team={self.team_code}, {self.wins}W-{self.losses}L, GB={self.games_behind})>"
