"""ORM models for daily team standings and magic numbers."""

from __future__ import annotations

from datetime import date
from typing import Any

from sqlalchemy import JSON, Date, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TeamStandingsDaily(Base, TimestampMixin):
    """
    Daily snapshot of Team Standings.

    Calculated from Game results.

    """

    __tablename__ = "team_standings_daily"

    __table_args__ = (UniqueConstraint("standings_date", "team_code", name="uq_team_standings_daily"),)

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

    rank: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="순위 (1-10)")
    top_5: Mapped[bool] = mapped_column(Integer, nullable=False, default=False, comment="5강권 여부 (1-5위)")

    recent_10_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="최근 10경기 승")
    recent_10_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="최근 10경기 패")
    recent_10_draws: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="최근 10경기 무")

    weekly_win_pcts: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True, comment="주차별 승률 추이 JSON")

    home_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="홈 승")
    home_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="홈 패")
    away_wins: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="원정 승")
    away_losses: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="원정 패")

    def __repr__(self) -> str:
        """Return a string representation of this object."""
        return f"<TeamStandingsDaily(date={self.standings_date}, team={self.team_code}, rank={self.rank}, {self.wins}W-{self.losses}L, GB={self.games_behind})>"
