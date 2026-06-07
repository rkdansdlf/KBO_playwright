"""
ORM models for team-level season statistics.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import (
    JSON,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
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
    team_id: Mapped[str] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=False, comment="팀 코드")
    team_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="KBO 표기 팀명")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 연도")
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")

    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plate_appearances: Mapped[int | None] = mapped_column(Integer, nullable=True)
    at_bats: Mapped[int | None] = mapped_column(Integer, nullable=True)
    runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hits: Mapped[int | None] = mapped_column(Integer, nullable=True)
    doubles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    triples: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rbi: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_bases: Mapped[int | None] = mapped_column(Integer, nullable=True)
    caught_stealing: Mapped[int | None] = mapped_column(Integer, nullable=True)
    walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    avg: Mapped[float | None] = mapped_column(Float, nullable=True)
    obp: Mapped[float | None] = mapped_column(Float, nullable=True)
    slg: Mapped[float | None] = mapped_column(Float, nullable=True)
    ops: Mapped[float | None] = mapped_column(Float, nullable=True)
    intentional_walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hbp: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_hits: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_flies: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gdp: Mapped[int | None] = mapped_column(Integer, nullable=True)
    iso: Mapped[float | None] = mapped_column(Float, nullable=True)
    babip: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return f"<TeamSeasonBatting(team_id='{self.team_id}', season={self.season}, league='{self.league}')>"


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
    team_id: Mapped[str] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=False, comment="팀 코드")
    team_name: Mapped[str] = mapped_column(String(64), nullable=False, comment="KBO 표기 팀명")
    season: Mapped[int] = mapped_column(Integer, nullable=False, comment="시즌 연도")
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")

    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wins: Mapped[int | None] = mapped_column(Integer, nullable=True)
    losses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ties: Mapped[int | None] = mapped_column(Integer, nullable=True)
    saves: Mapped[int | None] = mapped_column(Integer, nullable=True)
    holds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    innings_pitched: Mapped[float | None] = mapped_column(Float, nullable=True)
    runs_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    earned_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hits_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_runs_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    walks_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    era: Mapped[float | None] = mapped_column(Float, nullable=True)
    whip: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_against: Mapped[float | None] = mapped_column(Float, nullable=True)
    innings_outs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    intentional_walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hit_batters: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tbf: Mapped[int | None] = mapped_column(Integer, nullable=True)
    complete_games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    shutouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wild_pitches: Mapped[int | None] = mapped_column(Integer, nullable=True)
    balks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifices_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_flies_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    k_per_nine: Mapped[float | None] = mapped_column(Float, nullable=True)
    bb_per_nine: Mapped[float | None] = mapped_column(Float, nullable=True)
    kbb: Mapped[float | None] = mapped_column(Float, nullable=True)
    fip: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    def __repr__(self) -> str:
        return f"<TeamSeasonPitching(team_id='{self.team_id}', season={self.season}, league='{self.league}')>"
