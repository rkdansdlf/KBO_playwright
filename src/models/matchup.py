from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Numeric,
    UniqueConstraint,
)
from src.models.base import Base, TimestampMixin

class BatterTeamSplit(Base, TimestampMixin):
    """Batting statistics aggregated by opposing team."""
    __tablename__ = "matchup_batter_team_split"
    __table_args__ = (
        UniqueConstraint("season_year", "league_type_code", "player_id", "team_code", "opponent_team_code", name="uq_batter_team_split"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    season_year = Column(Integer, nullable=False)
    league_type_code = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    player_name = Column(String(64), nullable=False)
    team_code = Column(String(20), nullable=False)
    opponent_team_code = Column(String(20), nullable=False)
    
    # Aggregated Stats
    games = Column(Integer, default=0)
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
    gdp = Column(Integer, default=0)

    # Derived Stats
    avg = Column(Float)
    obp = Column(Float)
    slg = Column(Float)
    ops = Column(Float)


class PitcherTeamSplit(Base, TimestampMixin):
    """Pitching statistics aggregated by opposing team."""
    __tablename__ = "matchup_pitcher_team_split"
    __table_args__ = (
        UniqueConstraint("season_year", "league_type_code", "player_id", "team_code", "opponent_team_code", name="uq_pitcher_team_split"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    season_year = Column(Integer, nullable=False)
    league_type_code = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    player_name = Column(String(64), nullable=False)
    team_code = Column(String(20), nullable=False)
    opponent_team_code = Column(String(20), nullable=False)

    # Aggregated Stats
    games = Column(Integer, default=0)
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

    # Derived Stats
    era = Column(Float)
    whip = Column(Float)


class BatterStadiumSplit(Base, TimestampMixin):
    """Batting statistics aggregated by stadium."""
    __tablename__ = "matchup_batter_stadium_split"
    __table_args__ = (
        UniqueConstraint("season_year", "league_type_code", "player_id", "team_code", "stadium_name", name="uq_batter_stadium_split"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    season_year = Column(Integer, nullable=False)
    league_type_code = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    player_name = Column(String(64), nullable=False)
    team_code = Column(String(20), nullable=False)
    stadium_name = Column(String(100), nullable=False)
    
    games = Column(Integer, default=0)
    plate_appearances = Column(Integer, default=0)
    at_bats = Column(Integer, default=0)
    runs = Column(Integer, default=0)
    hits = Column(Integer, default=0)
    doubles = Column(Integer, default=0)
    triples = Column(Integer, default=0)
    home_runs = Column(Integer, default=0)
    rbi = Column(Integer, default=0)
    walks = Column(Integer, default=0)
    strikeouts = Column(Integer, default=0)
    
    avg = Column(Float)
    obp = Column(Float)
    slg = Column(Float)
    ops = Column(Float)


class BatterVsStarter(Base, TimestampMixin):
    """Batting stat heuristic evaluating player performance in games where a specific pitcher started."""
    __tablename__ = "matchup_batter_vs_starter"
    __table_args__ = (
        UniqueConstraint("season_year", "league_type_code", "player_id", "pitcher_name", name="uq_batter_vs_starter"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    season_year = Column(Integer, nullable=False)
    league_type_code = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    player_name = Column(String(64), nullable=False)
    pitcher_name = Column(String(64), nullable=False) # Only string available commonly for opposing starter
    
    games = Column(Integer, default=0)
    plate_appearances = Column(Integer, default=0)
    at_bats = Column(Integer, default=0)
    hits = Column(Integer, default=0)
    home_runs = Column(Integer, default=0)
    walks = Column(Integer, default=0)
    strikeouts = Column(Integer, default=0)
    
    avg = Column(Float)
    obp = Column(Float)
    slg = Column(Float)
    ops = Column(Float)
