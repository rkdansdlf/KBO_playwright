from sqlalchemy import (
    Column,
    String,
    Integer,
    Date,
    Time,
    ForeignKey,
    JSON,
    Boolean,
    Float,
    Text,
    UniqueConstraint,
    Numeric,
)
from sqlalchemy.orm import relationship

from src.models.base import Base, TimestampMixin

class Game(Base, TimestampMixin):
    """KBO Game basic information"""
    __tablename__ = "game"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=False, unique=True, index=True)
    game_date = Column(Date, nullable=False)
    stadium = Column(String(50))
    home_team = Column(String(20)) # Team Code
    away_team = Column(String(20))
    home_score = Column(Integer)
    away_score = Column(Integer)
    away_pitcher = Column(String(30))
    home_pitcher = Column(String(30))
    winning_team = Column(String(20))
    winning_score = Column(Integer)
    season_id = Column(Integer)
    game_status = Column(String(32), nullable=True)
    
    # Canonical/Franchise IDs for stable analysis
    home_franchise_id = Column(Integer, nullable=True)
    away_franchise_id = Column(Integer, nullable=True)
    winning_franchise_id = Column(Integer, nullable=True)
    
    # Relationships
    summary = relationship("GameSummary", back_populates="game")
    plays = relationship("GamePlayByPlay", back_populates="game")
    metadata_entry = relationship("GameMetadata", back_populates="game", uselist=False)
    innings = relationship("GameInningScore", back_populates="game")
    lineups = relationship("GameLineup", back_populates="game")
    batting_stats = relationship("GameBattingStat", back_populates="game")
    pitching_stats = relationship("GamePitchingStat", back_populates="game")
    events = relationship("GameEvent", back_populates="game")

class GameSummary(Base, TimestampMixin):
    """Summary of game results (pitcher decisions, home runs, etc.)"""
    __tablename__ = "game_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    summary_type = Column(String(50))
    player_id = Column(Integer, ForeignKey("player_basic.player_id"), nullable=True)
    player_name = Column(String(50))
    detail_text = Column(Text)

    game = relationship("Game", back_populates="summary")


class GamePlayByPlay(Base, TimestampMixin):
    """Detailed event logs (play-by-play)"""
    __tablename__ = "game_play_by_play"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    inning = Column(Integer)
    inning_half = Column(String(10)) # "초" or "말"
    pitcher_name = Column(String(50))
    batter_name = Column(String(50))
    play_description = Column(String(1000))
    event_type = Column(String(50))
    result = Column(String(100))

    game = relationship("Game", back_populates="plays")


class GameMetadata(Base, TimestampMixin):
    """Captured metadata for a game (attendance, times, etc.)."""
    __tablename__ = "game_metadata"

    game_id = Column(String(20), ForeignKey("game.game_id"), primary_key=True)
    stadium_code = Column(String(30))
    stadium_name = Column(String(64))
    attendance = Column(Integer)
    start_time = Column(Time)
    end_time = Column(Time)
    game_time_minutes = Column(Integer)
    weather = Column(String(32))
    source_payload = Column(JSON)

    game = relationship("Game", back_populates="metadata_entry")


class GameInningScore(Base, TimestampMixin):
    """Line score broken down by inning for each side."""
    __tablename__ = "game_inning_scores"
    __table_args__ = (
        UniqueConstraint("game_id", "team_side", "inning", name="uq_game_inning_team"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    team_side = Column(String(5), nullable=False)  # away/home
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    inning = Column(Integer, nullable=False)
    runs = Column(Integer, default=0)
    is_extra = Column(Boolean, default=False)

    game = relationship("Game", back_populates="innings")


class GameLineup(Base, TimestampMixin):
    """Lineup order and defensive position snapshot."""
    __tablename__ = "game_lineups"
    __table_args__ = (
        UniqueConstraint("game_id", "team_side", "appearance_seq", name="uq_game_lineup_entry"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer)
    player_name = Column(String(64), nullable=False)
    uniform_no = Column(String(10))
    batting_order = Column(Integer)
    position = Column(String(8))
    standard_position = Column(String(10))
    is_starter = Column(Boolean, default=False)
    appearance_seq = Column(Integer, nullable=False)
    notes = Column(String(64))

    game = relationship("Game", back_populates="lineups")


class GameBattingStat(Base, TimestampMixin):
    """Per-player batting metrics for a single game."""
    __tablename__ = "game_batting_stats"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", "appearance_seq", name="uq_game_batting_player"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer)
    player_name = Column(String(64), nullable=False)
    uniform_no = Column(String(10))
    batting_order = Column(Integer)
    is_starter = Column(Boolean, default=False)
    appearance_seq = Column(Integer, nullable=False)
    position = Column(String(8))
    standard_position = Column(String(10))
    plate_appearances = Column(Integer, default=0)
    at_bats = Column(Integer, default=0)
    runs = Column(Integer, default=0)
    hits = Column(Integer, default=0)
    doubles = Column(Integer, default=0)
    triples = Column(Integer, default=0)
    home_runs = Column(Integer, default=0)
    rbi = Column(Integer, default=0)
    walks = Column(Integer, default=0)
    intentional_walks = Column(Integer, default=0)
    hbp = Column(Integer, default=0)
    strikeouts = Column(Integer, default=0)
    stolen_bases = Column(Integer, default=0)
    caught_stealing = Column(Integer, default=0)
    sacrifice_hits = Column(Integer, default=0)
    sacrifice_flies = Column(Integer, default=0)
    gdp = Column(Integer, default=0)
    avg = Column(Float)
    obp = Column(Float)
    slg = Column(Float)
    ops = Column(Float)
    iso = Column(Float)
    babip = Column(Float)
    extra_stats = Column(JSON)

    game = relationship("Game", back_populates="batting_stats")


class GamePitchingStat(Base, TimestampMixin):
    """Per-player pitching stats per game."""
    __tablename__ = "game_pitching_stats"
    __table_args__ = (
        UniqueConstraint("game_id", "player_id", "appearance_seq", name="uq_game_pitching_player"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer)
    player_name = Column(String(64), nullable=False)
    uniform_no = Column(String(10))
    is_starting = Column(Boolean, default=False)
    appearance_seq = Column(Integer, nullable=False)
    standard_position = Column(String(10))
    innings_outs = Column(Integer, default=0)
    innings_pitched = Column(Numeric(5, 3))
    batters_faced = Column(Integer, default=0)
    pitches = Column(Integer, default=0)
    hits_allowed = Column(Integer, default=0)
    runs_allowed = Column(Integer, default=0)
    earned_runs = Column(Integer, default=0)
    home_runs_allowed = Column(Integer, default=0)
    walks_allowed = Column(Integer, default=0)
    strikeouts = Column(Integer, default=0)
    hit_batters = Column(Integer, default=0)
    wild_pitches = Column(Integer, default=0)
    balks = Column(Integer, default=0)
    wins = Column(Integer, default=0)
    losses = Column(Integer, default=0)
    saves = Column(Integer, default=0)
    holds = Column(Integer, default=0)
    decision = Column(String(2))
    era = Column(Float)
    whip = Column(Float)
    k_per_nine = Column(Float)
    bb_per_nine = Column(Float)
    kbb = Column(Float)
    extra_stats = Column(JSON)

    game = relationship("Game", back_populates="pitching_stats")


class GameEvent(Base, TimestampMixin):
    """Normalized Play-by-Play events."""
    __tablename__ = "game_events"
    __table_args__ = (
        UniqueConstraint("game_id", "event_seq", name="uq_game_event_seq"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id"), nullable=False)
    event_seq = Column(Integer, nullable=False)
    inning = Column(Integer)
    inning_half = Column(String(6))
    outs = Column(Integer)
    batter_id = Column(Integer)
    batter_name = Column(String(64))
    pitcher_id = Column(Integer)
    pitcher_name = Column(String(64))
    description = Column(Text)
    event_type = Column(String(32))
    result_code = Column(String(16))
    rbi = Column(Integer)
    bases_before = Column(String(3))
    bases_after = Column(String(3))
    
    # WPA & State Columns
    wpa = Column(Float)
    win_expectancy_before = Column(Float)
    win_expectancy_after = Column(Float)
    score_diff = Column(Integer) # Home - Away
    base_state = Column(Integer) # Bitmask
    home_score = Column(Integer)
    away_score = Column(Integer)

    extra_json = Column(JSON)

    game = relationship("Game", back_populates="events")
