"""
Team-related ORM models
"""
from __future__ import annotations

from typing import Optional
from sqlalchemy import Integer, String, Boolean, JSON, UniqueConstraint, Date
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class Team(Base, TimestampMixin):
    """
    Represents a KBO team.
    Data is seeded from Docs/schema/teams (구단 정보).csv
    """
    __tablename__ = "teams"

    team_id: Mapped[str] = mapped_column(String(10), primary_key=True, comment="구단 고유 ID")
    team_name: Mapped[str] = mapped_column(String(50), nullable=False, comment="구단 정식 명칭")
    team_short_name: Mapped[str] = mapped_column(String(20), nullable=False, comment="구단 약칭")
    city: Mapped[str] = mapped_column(String(30), nullable=False, comment="연고 도시")
    founded_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="창단 연도")
    stadium_name: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, comment="홈 구장 명칭")
    franchise_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="Franchise ID")
    
    # New Fields for Phase 7
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="Currently active team code")
    
    # Use ARRAY for Postgres compatibility, JSON for others (like SQLite)
    from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY
    aliases: Mapped[list] = mapped_column(JSON().with_variant(PG_ARRAY(String), "postgresql"), nullable=True, comment="List of team name aliases")

    # franchise: Mapped["Franchise"] = relationship(back_populates="teams")

    def __repr__(self) -> str:
        return f"<Team(team_id='{self.team_id}', name='{self.team_name}')>"


class TeamDailyRoster(Base, TimestampMixin):
    """
    Daily snapshot of 1st team registration.
    Source: https://www.koreabaseball.com/Player/Register.aspx
    """
    __tablename__ = "team_daily_roster"
    __table_args__ = (
        UniqueConstraint("roster_date", "team_code", "player_id", name="uq_team_daily_roster"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    roster_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    team_code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False)
    player_name: Mapped[str] = mapped_column(String(50), nullable=False)
    position: Mapped[str] = mapped_column(String(20)) # e.g. 투수, 포수, 감독
    back_number: Mapped[Optional[str]] = mapped_column(String(10))
    
    # Optional: is_coach boolean if we want to distinguish easily
    # But position string is descriptive.

    def __repr__(self) -> str:
        return f"<Roster({self.roster_date}, {self.team_code}, {self.player_name})>"


class TeamCodeMap(Base, TimestampMixin):
    """
    Canonical mapping of team codes by season.
    Bridging legacy codes, external codes, and franchise IDs.
    """
    __tablename__ = "team_code_map"
    __table_args__ = (
        UniqueConstraint("season", "curr_code", name="uq_team_code_map"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    franchise_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    curr_code: Mapped[str] = mapped_column(String(10), nullable=False, comment="The code used in that season (e.g. SK)")
    canonical_code: Mapped[str] = mapped_column(String(10), nullable=False, comment="Modern canonical code (e.g. SSG)")
    is_canonical: Mapped[bool] = mapped_column(Boolean, default=False, comment="Is this the modern canonical code?")
    
    def __repr__(self) -> str:
        return f"<TeamCodeMap(season={self.season}, code='{self.curr_code}', franchise={self.franchise_id})>"