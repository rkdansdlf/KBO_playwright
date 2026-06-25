"""Player-related ORM models
Aligns with Docs/schema/playerProfileSchemaGuide.md design.

PlayerBasic: Simple table from player search crawler (Docs/PLAYERID_CRAWLING.md)
Player: Complex relational model for detailed player data
"""

from __future__ import annotations

from datetime import date as date_type
from typing import Any

from sqlalchemy import JSON, Boolean, Date, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, TimestampMixin


class PlayerBasic(Base, TimestampMixin):
    """Simple player table populated from player search crawler.
    Source: https://www.koreabaseball.com/Player/Search.aspx.

    This table contains basic player information from the search page:
    - player_id: KBO's unique player identifier
    - name: Player name in Korean
    - uniform_no: Current uniform number
    - team: Current team
    - position: Primary position
    - birth_date: Birth date (original string format)
    - birth_date_date: Parsed birth date
    - height_cm/weight_kg: Physical stats
    - career: School/origin (출신교)

    Design rationale: Keep original string values for verification,
    add parsed columns for querying.
    """

    __tablename__ = "player_basic"

    player_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False, comment="KBO player ID")
    name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Player name (Korean)")
    uniform_no: Mapped[str | None] = mapped_column(String(10), nullable=True, comment="Current uniform number")
    team: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Current team")
    position: Mapped[str | None] = mapped_column(String(50), nullable=True, comment="Primary position")

    # Birth date: keep original string + parsed date
    birth_date: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Birth date (original string)")
    birth_date_date: Mapped[date_type | None] = mapped_column(Date, nullable=True, comment="Parsed birth date")

    # Physical stats
    height_cm: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Height in cm")
    weight_kg: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Weight in kg")

    # Career/origin
    career: Mapped[str | None] = mapped_column(String(200), nullable=True, comment="School/origin (출신교)")
    status: Mapped[str | None] = mapped_column(String(16), nullable=True, comment="active|retired|staff")
    staff_role: Mapped[str | None] = mapped_column(String(32), nullable=True, comment="manager|coach|trainer")
    status_source: Mapped[str | None] = mapped_column(String(16), nullable=True, comment="heuristic|profile|register")

    # Extended profile fields (from detail page)
    photo_url: Mapped[str | None] = mapped_column(String(500), nullable=True, comment="KBO CDN profile image URL")
    bats: Mapped[str | None] = mapped_column(String(4), nullable=True, comment="Batting hand: R/L/S")
    throws: Mapped[str | None] = mapped_column(String(4), nullable=True, comment="Throwing hand: R/L")
    debut_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Year of entry/debut")
    salary_original: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Salary as shown on KBO site (e.g. '1억 5천만원')",
    )
    signing_bonus_original: Mapped[str | None] = mapped_column(
        String(50),
        nullable=True,
        comment="Signing bonus original string",
    )
    draft_info: Mapped[str | None] = mapped_column(
        String(100),
        nullable=True,
        comment="Draft info string (e.g. '23 KT 2라운드 10순위')",
    )

    # Parsed structured profile details
    salary_amount: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Parsed salary amount")
    salary_currency: Mapped[str | None] = mapped_column(
        String(8),
        nullable=True,
        comment="Salary currency (KRW or USD)",
    )
    signing_bonus_amount: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Parsed signing bonus")
    signing_bonus_currency: Mapped[str | None] = mapped_column(
        String(8),
        nullable=True,
        comment="Signing bonus currency",
    )
    draft_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Draft year")
    draft_round: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Draft round number")
    draft_pick_overall: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Draft pick overall position",
    )
    draft_type: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Draft type (e.g. 1차, 2차, 자유선발)",
    )
    education_path: Mapped[list | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Structured education/career history path",
    )

    __table_args__ = (
        Index("idx_player_basic_name", "name"),
        Index("idx_player_basic_team", "team"),
        Index("idx_player_basic_position", "position"),
        Index("idx_player_basic_team_pos", "team", "position"),
    )

    def __repr__(self) -> str:
        return f"<PlayerBasic(player_id={self.player_id}, name='{self.name}', team='{self.team}')>"


class Player(Base, TimestampMixin):
    """Player master record representing an individual person."""

    __tablename__ = "players"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    kbo_person_id: Mapped[str | None] = mapped_column(String(32), unique=True)
    player_basic_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        unique=True,
        nullable=True,
        index=True,
        comment="Canonical player_basic.player_id mirror when this row represents a KBO player",
    )
    birth_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    birth_place: Mapped[str | None] = mapped_column(String(64), nullable=True)
    height_cm: Mapped[int | None] = mapped_column(Integer, nullable=True)
    weight_kg: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bats: Mapped[str | None] = mapped_column(String(1), nullable=True)  # R/L/S
    throws: Mapped[str | None] = mapped_column(String(1), nullable=True)  # R/L
    is_foreign_player: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    debut_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    retire_year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Extended profile fields (relational mirror of PlayerBasic)
    photo_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    salary_original: Mapped[str | None] = mapped_column(String(50), nullable=True)
    signing_bonus_original: Mapped[str | None] = mapped_column(String(50), nullable=True)
    draft_info: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Parsed structured profile details
    salary_amount: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Parsed salary amount")
    salary_currency: Mapped[str | None] = mapped_column(
        String(8),
        nullable=True,
        comment="Salary currency (KRW or USD)",
    )
    signing_bonus_amount: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Parsed signing bonus")
    signing_bonus_currency: Mapped[str | None] = mapped_column(
        String(8),
        nullable=True,
        comment="Signing bonus currency",
    )
    draft_year: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Draft year")
    draft_round: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Draft round number")
    draft_pick_overall: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        comment="Draft pick overall position",
    )
    draft_type: Mapped[str | None] = mapped_column(
        String(32),
        nullable=True,
        comment="Draft type (e.g. 1차, 2차, 자유선발)",
    )
    education_path: Mapped[list | None] = mapped_column(
        JSON,
        nullable=True,
        comment="Structured education/career history path",
    )

    def __repr__(self) -> str:
        return f"<Player(id={self.id}, status={self.status})>"


class PlayerIdentity(Base, TimestampMixin):
    """Player naming/identity history.
    Tracks name changes or variations (e.g. Korean name, English name).
    """

    __tablename__ = "player_identities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("players.id"), nullable=False)
    name_kor: Mapped[str | None] = mapped_column(String(100), nullable=True)
    name_eng: Mapped[str | None] = mapped_column(String(100), nullable=True)
    start_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[Date | None] = mapped_column(Date, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_player_identity_player", "player_id"),
        Index("idx_player_identity_name", "name_kor"),
    )


class PlayerSeasonBatting(Base, TimestampMixin):
    """Season-level batting aggregates by league/split."""

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
        Integer,
        ForeignKey("player_basic.player_id"),
        nullable=False,
        comment="KBO player ID",
    )
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="KBO1")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="ROLLUP")
    team_code: Mapped[str | None] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=True)
    franchise_id: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Canonical franchise ID")
    canonical_team_code: Mapped[str | None] = mapped_column(
        String(10),
        nullable=True,
        comment="Modern canonical team code",
    )
    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    plate_appearances: Mapped[int | None] = mapped_column(Integer, nullable=True)
    at_bats: Mapped[int | None] = mapped_column(Integer, nullable=True)
    runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hits: Mapped[int | None] = mapped_column(Integer, nullable=True)
    doubles: Mapped[int | None] = mapped_column(Integer, nullable=True)
    triples: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rbi: Mapped[int | None] = mapped_column(Integer, nullable=True)
    walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    intentional_walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hbp: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_bases: Mapped[int | None] = mapped_column(Integer, nullable=True)
    caught_stealing: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_hits: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_flies: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gdp: Mapped[int | None] = mapped_column(Integer, nullable=True)
    avg: Mapped[float | None] = mapped_column(Float, nullable=True)
    obp: Mapped[float | None] = mapped_column(Float, nullable=True)
    slg: Mapped[float | None] = mapped_column(Float, nullable=True)
    ops: Mapped[float | None] = mapped_column(Float, nullable=True)
    iso: Mapped[float | None] = mapped_column(Float, nullable=True)
    babip: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra_stats: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PlayerSeasonPitching(Base, TimestampMixin):
    """Season-level pitching aggregates by league/split.
    Compatible with pitcher crawler data structure.
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
        Index("idx_psp_player_season", "player_id", "season"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id"),
        nullable=False,
        comment="KBO player ID",
    )
    season: Mapped[int] = mapped_column(Integer, nullable=False)
    league: Mapped[str] = mapped_column(String(16), nullable=False, default="REGULAR")
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="KBO1")
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="CRAWLER")
    team_code: Mapped[str | None] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=True)
    franchise_id: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Canonical franchise ID")
    canonical_team_code: Mapped[str | None] = mapped_column(
        String(10),
        nullable=True,
        comment="Modern canonical team code",
    )

    # Basic pitching stats
    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    games_started: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wins: Mapped[int | None] = mapped_column(Integer, nullable=True)
    losses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    saves: Mapped[int | None] = mapped_column(Integer, nullable=True)
    holds: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Innings and outs
    innings_pitched: Mapped[float | None] = mapped_column(Float, nullable=True)
    innings_outs: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Pitching results
    hits_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    runs_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    earned_runs: Mapped[int | None] = mapped_column(Integer, nullable=True)
    home_runs_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    walks_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    intentional_walks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hit_batters: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    wild_pitches: Mapped[int | None] = mapped_column(Integer, nullable=True)
    balks: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Advanced stats
    era: Mapped[float | None] = mapped_column(Float, nullable=True)
    whip: Mapped[float | None] = mapped_column(Float, nullable=True)
    fip: Mapped[float | None] = mapped_column(Float, nullable=True)
    k_per_nine: Mapped[float | None] = mapped_column(Float, nullable=True)
    bb_per_nine: Mapped[float | None] = mapped_column(Float, nullable=True)
    kbb: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Extended stats from Basic2 (promoted from extra_stats)
    complete_games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    shutouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quality_starts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    blown_saves: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tbf: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Total batters faced")
    np: Mapped[int | None] = mapped_column(Integer, nullable=True, comment="Number of pitches")
    avg_against: Mapped[float | None] = mapped_column(Float, nullable=True)
    doubles_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    triples_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifices_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sacrifice_flies_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Additional metadata
    extra_stats: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class PlayerMovement(Base, TimestampMixin):
    """Records player status changes (Trade, FA, Waiver, etc.).
    Source: https://www.koreabaseball.com/Player/Trade.aspx.
    """

    __tablename__ = "player_movements"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    movement_date: Mapped[Date] = mapped_column(Date, nullable=False, comment="Event date")
    section: Mapped[str] = mapped_column(String(50), nullable=False, comment="Movement type (e.g. Trade)")
    team_code: Mapped[str] = mapped_column(String(20), nullable=False, comment="Related team")
    canonical_team_id: Mapped[str | None] = mapped_column(
        String(10),
        ForeignKey("teams.team_id", ondelete="RESTRICT"),
        nullable=True,
        comment="Resolved teams.team_id; raw team_code is retained as source snapshot",
    )
    player_basic_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("player_basic.player_id", ondelete="RESTRICT"),
        nullable=True,
        comment="Resolved player_basic.player_id when the movement can be unambiguously linked",
    )
    resolution_status: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="unresolved",
        server_default="unresolved",
        comment="resolved|unresolved|unresolved_player|unresolved_team",
    )
    player_name: Mapped[str] = mapped_column(String(100), nullable=False, comment="Player name (with position info)")
    remarks: Mapped[str | None] = mapped_column(Text, nullable=True, comment="Detailed remarks")

    __table_args__ = (
        UniqueConstraint("movement_date", "team_code", "player_name", "section", name="uq_player_movement"),
        Index("idx_player_movement_date", "movement_date"),
        Index("idx_player_movement_player", "player_name"),
        Index("idx_player_movement_player_basic", "player_basic_id"),
        Index("idx_player_movement_canonical_team", "canonical_team_id"),
    )

    def __repr__(self) -> str:
        return f"<PlayerMovement(date={self.movement_date}, section='{self.section}', player='{self.player_name}')>"


class PlayerSeasonFielding(Base, TimestampMixin):
    """Season-level fielding stats.
    Source: https://www.koreabaseball.com/Record/Player/Defense/Basic.aspx.
    """

    __tablename__ = "player_season_fielding"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "year", "position_id", name="uq_player_season_fielding"),
        Index("idx_psf_player_year", "player_id", "year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("player_basic.player_id"), nullable=False)
    team_id: Mapped[str] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    position_id: Mapped[str] = mapped_column(String(10), nullable=False, comment="POS (e.g. C, 1B, SS)")

    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    games_started: Mapped[int | None] = mapped_column(Integer, nullable=True)
    innings: Mapped[float | None] = mapped_column(Float, nullable=True)
    putouts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    assists: Mapped[int | None] = mapped_column(Integer, nullable=True)
    errors: Mapped[int | None] = mapped_column(Integer, nullable=True)
    double_plays: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fielding_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    pickoffs: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Specialized catcher metrics
    caught_stealing: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_bases_allowed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    passed_balls: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cs_pct: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Caught Stealing Percentage")

    source: Mapped[str | None] = mapped_column(String(20), nullable=True, default="CRAWLER")

    def __repr__(self) -> str:
        return f"<PlayerSeasonFielding(player_id={self.player_id}, year={self.year}, pos='{self.position_id}')>"


class PlayerSeasonBaserunning(Base, TimestampMixin):
    """Season-level baserunning stats.
    Source: https://www.koreabaseball.com/Record/Player/Runner/Basic.aspx.
    """

    __tablename__ = "player_season_baserunning"
    __table_args__ = (
        UniqueConstraint("player_id", "team_id", "year", name="uq_player_season_baserunning"),
        Index("idx_psb_run_player_year", "player_id", "year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    player_id: Mapped[int] = mapped_column(Integer, ForeignKey("player_basic.player_id"), nullable=False)
    team_id: Mapped[str] = mapped_column(String(10), ForeignKey("teams.team_id"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    player_name: Mapped[str | None] = mapped_column(String(100), nullable=True)

    games: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_base_attempts: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_bases: Mapped[int | None] = mapped_column(Integer, nullable=True)
    caught_stealing: Mapped[int | None] = mapped_column(Integer, nullable=True)
    stolen_base_percentage: Mapped[float | None] = mapped_column(Float, nullable=True)
    out_on_base: Mapped[int | None] = mapped_column(Integer, nullable=True)
    picked_off: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source: Mapped[str | None] = mapped_column(String(20), nullable=True, default="CRAWLER")

    def __repr__(self) -> str:
        return f"<PlayerSeasonBaserunning(player_id={self.player_id}, year={self.year})>"
