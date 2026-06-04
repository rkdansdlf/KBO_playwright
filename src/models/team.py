"""
Team-related ORM models
"""

from __future__ import annotations

from sqlalchemy import JSON, Boolean, Date, Float, ForeignKey, Integer, String, UniqueConstraint
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
    founded_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="창단 연도")
    stadium_name: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="홈 구장 명칭")
    franchise_id: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Franchise ID")

    # New Fields for Phase 7
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default="1",
        comment="Currently active team code",
    )

    # Use ARRAY for Postgres compatibility, JSON for others (like SQLite)
    from sqlalchemy.dialects.postgresql import ARRAY as PG_ARRAY

    aliases: Mapped[list] = mapped_column(
        JSON().with_variant(PG_ARRAY(String), "postgresql"), nullable=True, comment="List of team name aliases"
    )

    # franchise: Mapped["Franchise"] = relationship(back_populates="teams")

    def __repr__(self) -> str:
        return f"<Team(team_id='{self.team_id}', name='{self.team_name}')>"


class TeamDailyRoster(Base, TimestampMixin):
    """
    Daily snapshot of 1st team registration.
    Source: https://www.koreabaseball.com/Player/Register.aspx
    """

    __tablename__ = "team_daily_roster"
    __table_args__ = (UniqueConstraint("roster_date", "team_code", "player_id", name="uq_team_daily_roster"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    roster_date: Mapped[Date] = mapped_column(Date, nullable=False, index=True)
    team_code: Mapped[str] = mapped_column(
        String(10), ForeignKey("teams.team_id", ondelete="RESTRICT"), nullable=False, index=True
    )
    player_id: Mapped[int] = mapped_column(Integer, nullable=False)
    player_basic_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
        comment="Canonical player link for roster rows that represent players; staff rows keep this NULL",
    )
    person_type: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="player",
        server_default="player",
        comment="player|staff|unknown",
    )
    player_name: Mapped[str] = mapped_column(String(50), nullable=False)
    position: Mapped[str] = mapped_column(String(20))  # e.g. 투수, 포수, 감독
    back_number: Mapped[str | None] = mapped_column(String(10))

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
    __table_args__ = (UniqueConstraint("season", "curr_code", name="uq_team_code_map"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    franchise_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    curr_code: Mapped[str] = mapped_column(String(10), nullable=False, comment="The code used in that season (e.g. SK)")
    canonical_code: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="Active/final franchise code (e.g. SSG)"
    )
    is_canonical: Mapped[bool] = mapped_column(Boolean, default=False, comment="Is this the modern canonical code?")

    def __repr__(self) -> str:
        return f"<TeamCodeMap(season={self.season}, code='{self.curr_code}', franchise={self.franchise_id})>"


class TeamSeasonFielding(Base, TimestampMixin):
    """
    Team-level fielding stats aggregated from player data per season.
    """

    __tablename__ = "team_season_fielding"
    __table_args__ = (UniqueConstraint("season", "team_code", name="uq_team_season_fielding"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    team_code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)

    # Core defensive stats
    errors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    double_plays: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    triple_plays: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Fielding percentage
    total_chances: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    putouts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    assists: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Advanced
    def_innings: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Total defensive innings")
    fielding_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    range_factor_per_game: Mapped[float | None] = mapped_column(Float, nullable=True)


class TeamSeasonBaserunning(Base, TimestampMixin):
    """
    Team-level baserunning stats aggregated from player data per season.
    """

    __tablename__ = "team_season_baserunning"
    __table_args__ = (UniqueConstraint("season", "team_code", name="uq_team_season_baserunning"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    season: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    team_code: Mapped[str] = mapped_column(String(10), nullable=False, index=True)

    # Stolen base attempts
    stolen_bases: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    caught_stealing: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sb_success_rate: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Extra base running
    extra_bases_taken: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="1st→3rd on single, 1st→home on double, etc."
    )
    out_on_base: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, comment="Picked off / thrown out advancing"
    )

    # Bunting
    sacrifice_hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sacrifice_flies: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    bunt_hits: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
