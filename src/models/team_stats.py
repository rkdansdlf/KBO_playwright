"""
ORM models for team-level season statistics.
"""
from __future__ import annotations

from typing import Optional, Dict, Any
from sqlalchemy import (
    Integer,
    String,
    Float,
    ForeignKey,
    UniqueConstraint,
    Index,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class TeamSeasonBatting(Base, TimestampMixin):
    """
    Season-level batting aggregates for each team.
    """

    __tablename__ = "team_season_batting"
    __table_args__ = (
        UniqueConstraint("team_id", "season", "league", name="uq_team_season_batting"),
        Index("idx_team_batting_season", "season", "league"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(
        String(10), ForeignKey("teams.team_id"), nullable=False, comment="팀 코드"
    )
    team_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="KBO 표기 팀명")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 연도")
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")

    games: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    plate_appearances: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    at_bats: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hits: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    doubles: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    triples: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rbi: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    stolen_bases: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    caught_stealing: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    walks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    obp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    slg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ops: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<TeamSeasonBatting(team_id='{self.team_id}', season={self.season}, "
            f"league='{self.league}')>"
        )


class TeamSeasonPitching(Base, TimestampMixin):
    """
    Season-level pitching aggregates for each team.
    """

    __tablename__ = "team_season_pitching"
    __table_args__ = (
        UniqueConstraint("team_id", "season", "league", name="uq_team_season_pitching"),
        Index("idx_team_pitching_season", "season", "league"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id: Mapped[str] = mapped_column(
        String(10), ForeignKey("teams.team_id"), nullable=False, comment="팀 코드"
    )
    team_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="KBO 표기 팀명")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 연도")
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")

    games: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wins: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    losses: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ties: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    saves: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    holds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    innings_pitched: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    earned_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hits_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    walks_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    era: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    whip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    avg_against: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<TeamSeasonPitching(team_id='{self.team_id}', season={self.season}, "
            f"league='{self.league}')>"
        )
