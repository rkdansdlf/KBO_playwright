from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from src.models.base import Base, TimestampMixin


class Game(Base, TimestampMixin):
    """KBO Game basic information"""

    __tablename__ = "game"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=False, unique=True, index=True)
    game_date = Column(Date, nullable=False, index=True)
    stadium = Column(String(50))
    home_team = Column(String(20))  # Team Code
    away_team = Column(String(20))
    home_score = Column(Integer)
    away_score = Column(Integer)
    away_pitcher = Column(String(30))
    home_pitcher = Column(String(30))
    winning_team = Column(String(20))
    winning_score = Column(Integer)
    season_id = Column(Integer, index=True)
    game_status = Column(String(32), nullable=True)
    game_lifecycle_state = Column(String(32), nullable=True)
    is_primary = Column(Boolean, nullable=False, default=True, server_default="1")

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
    aliases = relationship("GameIdAlias", back_populates="canonical_game")
    validation_metrics = relationship("GameValidationMetrics", back_populates="game", uselist=False)
    highlights = relationship("GameHighlight", back_populates="game", cascade="all, delete-orphan")


class GameIdAlias(Base, TimestampMixin):
    """Alternate source IDs that resolve to the canonical KBO legacy game_id."""

    __tablename__ = "game_id_aliases"

    alias_game_id = Column(String(20), primary_key=True)
    canonical_game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, index=True)
    source = Column(String(50))
    reason = Column(String(120))

    canonical_game = relationship("Game", back_populates="aliases")


class GameSummary(Base, TimestampMixin):
    """Summary of game results (pitcher decisions, home runs, etc.)"""

    __tablename__ = "game_summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, index=True)
    summary_type = Column(String(50))
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True, index=True)
    player_name = Column(String(50))
    detail_text = Column(Text)

    game = relationship("Game", back_populates="summary")


class GamePlayByPlay(Base, TimestampMixin):
    """Detailed event logs (play-by-play)"""

    __tablename__ = "game_play_by_play"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
    inning = Column(Integer)
    inning_half = Column(String(10))  # "초" or "말"
    pitcher_name = Column(String(50))
    batter_name = Column(String(50))
    play_description = Column(String(1000))
    event_type = Column(String(50))
    result = Column(String(100))
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True, index=True)
    resolver_confidence = Column(String(16), nullable=True)
    resolver_reason = Column(String(64), nullable=True)
    unresolved_player_name = Column(String(64), nullable=True)
    provider_log_id = Column(String(64), nullable=True)
    source_row_index = Column(Integer, nullable=True)
    source_name = Column(String(32), nullable=True)

    game = relationship("Game", back_populates="plays")


class GameMetadata(Base, TimestampMixin):
    """Captured metadata for a game (attendance, times, etc.)."""

    __tablename__ = "game_metadata"

    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), primary_key=True)
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
    __table_args__ = (UniqueConstraint("game_id", "team_side", "inning", name="uq_game_inning_team"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
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
    __table_args__ = (UniqueConstraint("game_id", "team_side", "appearance_seq", name="uq_game_lineup_entry"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True)
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
    __table_args__ = (UniqueConstraint("game_id", "player_id", "appearance_seq", name="uq_game_batting_player"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True)
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
    __table_args__ = (UniqueConstraint("game_id", "player_id", "appearance_seq", name="uq_game_pitching_player"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    franchise_id = Column(Integer, nullable=True)
    canonical_team_code = Column(String(10), nullable=True)
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True)
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
    __table_args__ = (UniqueConstraint("game_id", "event_seq", name="uq_game_event_seq"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False)
    event_seq = Column(Integer, nullable=False)
    inning = Column(Integer)
    inning_half = Column(String(6))
    outs = Column(Integer)
    batter_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True, index=True)
    batter_name = Column(String(64))
    pitcher_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=True, index=True)
    pitcher_name = Column(String(64))
    description = Column(Text)
    event_type = Column(String(32))
    result_code = Column(String(16))
    rbi = Column(Integer)
    bases_before = Column(String(3))
    bases_after = Column(String(3))

    # Dedup & Traceability
    provider_log_id = Column(String(64), nullable=True)
    source_row_index = Column(Integer, nullable=True)

    # At-Bat Grouping (Phase 2 preparation)
    at_bat_seq = Column(Integer, nullable=True)
    at_bat_event_role = Column(String(16), nullable=True)
    at_bat_confidence = Column(String(16), nullable=True)

    # Pitch Count
    balls = Column(Integer, nullable=True, default=0)
    strikes = Column(Integer, nullable=True, default=0)

    # WPA & State Columns
    wpa = Column(Float)
    win_expectancy_before = Column(Float)
    win_expectancy_after = Column(Float)
    score_diff = Column(Integer)  # Home - Away
    base_state = Column(Integer)  # Bitmask
    home_score = Column(Integer)
    away_score = Column(Integer)

    extra_json = Column(JSON)

    game = relationship("Game", back_populates="events")


class GameValidationMetrics(Base, TimestampMixin):
    """Per-game validation and relay-source tracking metrics."""

    __tablename__ = "game_validation_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, unique=True)
    validation_status = Column(String(32), nullable=False, default="pending_live")
    previous_status = Column(String(32), nullable=True)
    source_used = Column(String(16), nullable=True)
    fallback_trigger_count = Column(Integer, default=0)
    fallback_trigger_reason = Column(String(64), nullable=True)
    last_fallback_at = Column(DateTime, nullable=True)
    duplicate_event_count = Column(Integer, default=0)
    unclassified_event_count = Column(Integer, default=0)
    finish_mismatch_count = Column(Integer, default=0)
    last_successful_event_at = Column(DateTime, nullable=True)
    parser_version = Column(String(32), nullable=True)
    source_schema_version = Column(String(32), nullable=True)
    payload_hash = Column(String(16), nullable=True)
    evidence_json = Column(JSON, nullable=True)

    game = relationship("Game", back_populates="validation_metrics")


class GameHighlight(Base, TimestampMixin):
    """Highlight plays / moments for a game computed from play-by-play events."""

    __tablename__ = "game_highlights"
    __table_args__ = (UniqueConstraint("game_id", "highlight_type", "event_seq", name="uq_game_highlight_event"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, index=True)
    event_seq = Column(Integer, nullable=True)  # Can be null for summary-level highlights
    inning = Column(Integer, nullable=True)
    inning_half = Column(String(10), nullable=True)
    highlight_type = Column(String(32), nullable=False)  # "BIG_PLAY", "LEAD_CHANGE", "WALK_OFF", "CLUTCH", etc.
    description = Column(Text, nullable=False)
    wpa = Column(Float, nullable=True)
    importance_score = Column(Float, nullable=False, default=0.0)  # Used to rank highlights within a game
    tags = Column(JSON, nullable=True)  # e.g., ["홈런", "역전", "끝내기", "만루", "병살"]

    game = relationship("Game", back_populates="highlights")


class PlayerGameBatting(Base, TimestampMixin):
    """Per-player batting aggregation across a single game."""

    __tablename__ = "player_game_batting"
    __table_args__ = (UniqueConstraint("game_id", "player_id", name="uq_player_game_batting"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, index=True)
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=False, index=True)
    player_name = Column(String(64), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    batting_order = Column(Integer)
    appearance_seq = Column(Integer)
    position = Column(String(8))
    is_starter = Column(Boolean, default=False)
    source = Column(String(16))
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


class PlayerGamePitching(Base, TimestampMixin):
    """Per-player pitching aggregation across a single game."""

    __tablename__ = "player_game_pitching"
    __table_args__ = (UniqueConstraint("game_id", "player_id", name="uq_player_game_pitching"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("game.game_id", ondelete="CASCADE"), nullable=False, index=True)
    player_id = Column(Integer, ForeignKey("player_basic.player_id", ondelete="RESTRICT"), nullable=False, index=True)
    player_name = Column(String(64), nullable=False)
    team_side = Column(String(5), nullable=False)
    team_code = Column(String(10))
    is_starting = Column(Boolean, default=False)
    appearance_seq = Column(Integer)
    source = Column(String(16))
    innings_outs = Column(Integer, default=0)
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
    batters_faced = Column(Integer, default=0)
    era = Column(Float)
    whip = Column(Float)
    fip = Column(Float)
    k_per_nine = Column(Float)
    bb_per_nine = Column(Float)
    kbb = Column(Float)
    extra_stats = Column(JSON)
