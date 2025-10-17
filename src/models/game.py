"""
Game-related models (schedules, games, box scores)
"""
from sqlalchemy import (
    Column, Integer, String, Date, Time, DateTime, Text,
    Enum, ForeignKey, UniqueConstraint, Index, Float, JSON
)
from sqlalchemy.orm import relationship, mapped_column, Mapped
from typing import Optional, Dict, Any
from datetime import date, time, datetime

from .base import Base, TimestampMixin


class GameSchedule(Base, TimestampMixin):
    """Game schedule table"""
    __tablename__ = "game_schedules"

    schedule_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)

    season_year: Mapped[int] = mapped_column(Integer, nullable=False)
    season_type: Mapped[str] = mapped_column(
        Enum("preseason", "regular", "postseason", name="season_type"),
        nullable=False
    )

    game_date: Mapped[date] = mapped_column(Date, nullable=False)
    game_time: Mapped[Optional[time]] = mapped_column(Time, nullable=True)

    # Team identifiers (will add FK later when franchise table is implemented)
    home_team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    away_team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)

    # Stadium
    stadium: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Game status
    game_status: Mapped[str] = mapped_column(
        Enum("scheduled", "postponed", "in_progress", "completed", "cancelled", name="game_status"),
        nullable=False,
        default="scheduled"
    )
    postpone_reason: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    # Doubleheader
    doubleheader_no: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Postseason series info
    series_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    series_type: Mapped[Optional[str]] = mapped_column(
        Enum("wildcard", "semi_playoff", "playoff", "korean_series", name="series_type"),
        nullable=True
    )
    series_game_number: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Crawl status
    crawl_status: Mapped[str] = mapped_column(
        Enum("pending", "ready", "crawled", "parsed", "saved", "failed", "skipped", "completed", name="crawl_status"),
        nullable=False,
        default="pending"
    )
    last_crawl_attempt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    crawl_error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Indexes
    __table_args__ = (
        UniqueConstraint("game_id", name="uq_sched_gid"),
        Index("idx_sched_type", "season_year", "season_type"),
        Index("idx_sched_date", "game_date"),
        Index("idx_sched_status", "game_status", "crawl_status"),
    )

    def __repr__(self):
        return f"<GameSchedule(game_id='{self.game_id}', date='{self.game_date}', status='{self.crawl_status}')>"


class Game(Base, TimestampMixin):
    """Game detail table (metadata, results)"""
    __tablename__ = "games"

    game_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    game_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    home_team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    away_team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)

    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration_min: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    attendance: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    weather: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    stadium: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    home_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    away_score: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Winning/losing/save pitchers (player IDs)
    winning_pitcher_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    losing_pitcher_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    save_pitcher_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    def __repr__(self):
        return f"<Game(game_id='{self.game_id}', score={self.away_score}-{self.home_score})>"


class GameLineup(Base):
    """Game lineup table"""
    __tablename__ = "game_lineups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), ForeignKey("games.game_id"), nullable=False)
    team_side: Mapped[str] = mapped_column(Enum("home", "away", name="team_side"), nullable=False)
    order_no: Mapped[int] = mapped_column(Integer, nullable=False)  # Batting order
    player_id: Mapped[int] = mapped_column(Integer, nullable=False)
    player_name: Mapped[str] = mapped_column(String(64), nullable=False)
    pos_at_start: Mapped[Optional[str]] = mapped_column(String(3), nullable=True)  # P, C, 1B, etc.
    is_starting: Mapped[bool] = mapped_column(Integer, nullable=False, default=1)

    __table_args__ = (
        UniqueConstraint("game_id", "team_side", "order_no", name="uq_lineup"),
        Index("idx_lineup_game", "game_id"),
    )

    def __repr__(self):
        return f"<GameLineup(game_id='{self.game_id}', {self.team_side}, order={self.order_no}, player='{self.player_name}')>"


class PlayerGameStats(Base):
    """Player game statistics (hitters and pitchers)"""
    __tablename__ = "player_game_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), ForeignKey("games.game_id"), nullable=False)
    player_id: Mapped[int] = mapped_column(Integer, nullable=False)
    player_name: Mapped[str] = mapped_column(String(64), nullable=False)
    team_side: Mapped[str] = mapped_column(Enum("home", "away", name="team_side"), nullable=False)
    player_type: Mapped[str] = mapped_column(Enum("hitter", "pitcher", name="player_type"), nullable=False)

    # Store raw data as JSON for flexibility
    raw_stats: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON string

    # Common hitter stats
    AB: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # At bats
    R: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # Runs
    H: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # Hits
    RBI: Mapped[Optional[int]] = mapped_column(Integer, nullable=True) # RBIs
    BB: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Walks
    SO: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Strikeouts

    # Common pitcher stats
    IP: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)  # Innings pitched (e.g., "6.1")
    ER: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Earned runs
    W: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # Wins
    L: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)   # Losses
    SV: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Saves

    __table_args__ = (
        UniqueConstraint("game_id", "player_id", "player_type", name="uq_player_game"),
        Index("idx_pgstats_game", "game_id"),
        Index("idx_pgstats_player", "player_id"),
    )

    def __repr__(self):
        return f"<PlayerGameStats(game_id='{self.game_id}', player='{self.player_name}', type='{self.player_type}')>"


class PlayerGameBatting(Base, TimestampMixin):
    """Structured batting box score per game"""
    __tablename__ = "player_game_batting"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_batting_player"),
        Index("idx_game_batting_game", "game_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), ForeignKey("games.game_id"), nullable=False)
    player_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    player_name: Mapped[str] = mapped_column(String(64), nullable=False)
    team_side: Mapped[str] = mapped_column(Enum("home", "away", name="team_side"), nullable=False)
    team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    batting_order: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    appearance_seq: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    position: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    is_starter: Mapped[bool] = mapped_column(Integer, nullable=False, default=1)
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="GAMECENTER")

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
    extras: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)


class PlayerGamePitching(Base, TimestampMixin):
    """Structured pitching box score per game"""
    __tablename__ = "player_game_pitching"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", name="uq_game_pitching_player"),
        Index("idx_game_pitching_game", "game_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), ForeignKey("games.game_id"), nullable=False)
    player_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    player_name: Mapped[str] = mapped_column(String(64), nullable=False)
    team_side: Mapped[str] = mapped_column(Enum("home", "away", name="team_side"), nullable=False)
    team_code: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    is_starting: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)
    appearance_seq: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="GAMECENTER")

    innings_outs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hits_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    earned_runs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    home_runs_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    walks_allowed: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    strikeouts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    hit_batters: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wild_pitches: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    balks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    wins: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    losses: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    saves: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    holds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    decision: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    batters_faced: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    era: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    whip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fip: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    k_per_nine: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    bb_per_nine: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    kbb: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    extras: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)


class GamePlayByPlay(Base, TimestampMixin):
    """Play-by-play events from RELAY section"""
    __tablename__ = "game_play_by_play"
    __table_args__ = (
        Index("idx_pbp_game", "game_id"),
        Index("idx_pbp_inning", "game_id", "inning", "half"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    game_id: Mapped[str] = mapped_column(String(20), ForeignKey("games.game_id"), nullable=False)

    # Inning info
    inning: Mapped[int] = mapped_column(Integer, nullable=False)
    half: Mapped[str] = mapped_column(Enum("top", "bottom", name="inning_half"), nullable=False)
    play_seq: Mapped[int] = mapped_column(Integer, nullable=False)  # Sequence within inning

    # Play details
    event_type: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="unknown"
    )  # batting, pitching_change, steal, walk, strikeout, hit, home_run, etc.

    description: Mapped[str] = mapped_column(Text, nullable=False)  # Full play text

    # Player involvement
    batter_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pitcher_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Result
    result: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)  # K, BB, H, HR, etc.

    # Game state after play
    outs_after: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Store full event data as JSON for flexibility
    raw_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    def __repr__(self):
        return f"<GamePlayByPlay(game_id='{self.game_id}', {self.inning}{self.half[0]}, seq={self.play_seq})>"
