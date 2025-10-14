"""
Player-related ORM models
Aligns with Docs/schema/playerProfileSchemaGuide.md design
"""
from __future__ import annotations

from typing import Optional, Dict, Any
from sqlalchemy import Integer, String, Text, Boolean, ForeignKey, Date, UniqueConstraint, Index, JSON, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base, TimestampMixin


class Player(Base, TimestampMixin):
    """
    Player master record representing an individual person.
    """

    __tablename__ = "players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kbo_person_id: Mapped[Optional[str]] = mapped_column(String(32), unique=True)
    birth_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    birth_place: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    height_cm: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    weight_kg: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    bats: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)  # R/L/S
    throws: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)  # R/L
    is_foreign_player: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    debut_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    retire_year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    identities: Mapped[list["PlayerIdentity"]] = relationship(
        back_populates="player",
        cascade="all, delete-orphan",
    )
    codes: Mapped[list["PlayerCode"]] = relationship(
        back_populates="player",
        cascade="all, delete-orphan",
    )
    stints: Mapped[list["PlayerStint"]] = relationship(
        back_populates="player",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Player(id={self.id}, status={self.status})>"


class PlayerIdentity(Base, TimestampMixin):
    """
    Track player naming history (renames, alternative spellings).
    """

    __tablename__ = "player_identities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False
    )
    name_kor: Mapped[str] = mapped_column(String(64), nullable=False)
    name_eng: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    start_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    player: Mapped["Player"] = relationship(back_populates="identities")

    __table_args__ = (
        Index("idx_player_identities_player_period", "player_id", "start_date", "end_date"),
    )

    def __repr__(self) -> str:
        return f"<PlayerIdentity(player_id={self.player_id}, name='{self.name_kor}')>"


class PlayerCode(Base, TimestampMixin):
    """
    Mapping external codes to player IDs (KBO, STATIZ, etc.)
    """

    __tablename__ = "player_codes"

    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("players.id", ondelete="CASCADE"), primary_key=True
    )
    source: Mapped[str] = mapped_column(String(16), primary_key=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    player: Mapped["Player"] = relationship(back_populates="codes")


class PlayerStint(Base, TimestampMixin):
    """
    Player affiliation history (team, uniform numbers, positions).
    """

    __tablename__ = "player_stints"
    __table_args__ = (
        UniqueConstraint(
            "player_id", "franchise_id", "start_date", "end_date", name="uq_player_stints_period"
        ),
        Index("idx_player_stints_player", "player_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False
    )
    franchise_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("franchises.id", ondelete="RESTRICT"), nullable=False
    )
    identity_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("team_identities.id", ondelete="SET NULL"), nullable=True
    )
    uniform_number: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    primary_pos: Mapped[Optional[str]] = mapped_column(String(4), nullable=True)
    start_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    end_date: Mapped[Optional[Date]] = mapped_column(Date, nullable=True)
    is_current: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    player: Mapped["Player"] = relationship(back_populates="stints")


class PlayerSeasonBatting(Base, TimestampMixin):
    """
    Season-level batting aggregates by league/split.
    """

    __tablename__ = "player_season_batting"
    __table_args__ = (
        UniqueConstraint(
            "player_id",
            "season",
            "league",
            "level",
            name="uq_player_season_batting",
        ),
        Index("idx_psb_player", "player_id", "season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False
    )
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="KBO1")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="ROLLUP")
    team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    games: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    plate_appearances: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    at_bats: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hits: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    doubles: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    triples: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    rbi: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    walks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    intentional_walks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hbp: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    stolen_bases: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    caught_stealing: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sacrifice_hits: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sacrifice_flies: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gdp: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    obp: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    slg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ops: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    iso: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    babip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)


class PlayerSeasonPitching(Base, TimestampMixin):
    """
    Season-level pitching aggregates by league/split.
    """

    __tablename__ = "player_season_pitching"
    __table_args__ = (
        UniqueConstraint(
            "player_id",
            "season",
            "league",
            "level",
            name="uq_player_season_pitching",
        ),
        Index("idx_psp_player", "player_id", "season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False
    )
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="KBO1")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="ROLLUP")
    team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    games: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    games_started: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wins: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    losses: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    saves: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    holds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    innings_outs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hits_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    earned_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    walks_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    intentional_walks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hit_batters: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wild_pitches: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    balks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    era: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    whip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    k_per_nine: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bb_per_nine: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kbb: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
