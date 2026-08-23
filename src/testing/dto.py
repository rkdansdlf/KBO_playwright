"""Standard Data Transfer Objects (DTOs) for Synthetic Data Generation and Testing."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class SyntheticPlayerScenario:
    """Represents a generated player profile for simulation."""

    player_id: int
    name: str
    team_code: str
    position: str
    is_pitcher: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert player scenario to dictionary."""
        return asdict(self)


@dataclass
class SyntheticGameScenario:
    """Represents a generated game with complete boxscore, lineup, and event graph."""

    game_id: str
    game_date: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    innings_count: int = 9
    lineups_count: int = 18
    pbp_events_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert game scenario to dictionary."""
        return asdict(self)


@dataclass
class SyntheticSeasonConfig:
    """Configuration options for synthetic season generation."""

    season_year: int = 2026
    team_codes: list[str] = field(
        default_factory=lambda: ["LG", "OB", "SSG", "KT", "KIA", "NC", "SS", "LT", "HH", "WO"]
    )
    games_per_team: int = 5
    players_per_team: int = 15
    include_pbp: bool = True

    def to_dict(self) -> dict[str, Any]:
        """Convert season configuration to dictionary."""
        return asdict(self)


@dataclass
class SyntheticGenerationResult:
    """Summary of generated synthetic entities and execution metrics."""

    total_games: int
    total_players: int
    total_lineups: int
    total_pbp_events: int
    elapsed_seconds: float

    def to_dict(self) -> dict[str, Any]:
        """Convert generation result to dictionary."""
        return {
            "total_games": self.total_games,
            "total_players": self.total_players,
            "total_lineups": self.total_lineups,
            "total_pbp_events": self.total_pbp_events,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
        }
