"""Standard Data Transfer Objects (DTOs) for the parser layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar

T = TypeVar("T")


@dataclass
class ParseResult[T]:
    """Generic wrapper for parsing results, capturing data, status, and diagnostic messages."""

    data: T
    success: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def has_errors(self) -> bool:
        """Check if any errors occurred during parsing."""
        return len(self.errors) > 0


@dataclass
class ScheduleGameParsed:
    """Parsed representation of a scheduled game."""

    game_id: str
    game_date: str
    season: int
    league_type: str = "regular"
    away_team: str | None = None
    home_team: str | None = None
    stadium: str | None = None
    status: str = "SCHEDULED"
    doubleheader_no: int = 0
    away_score: int | None = None
    home_score: int | None = None
    raw_info: dict[str, Any] = field(default_factory=dict)


@dataclass
class TeamEventParsed:
    """Parsed representation of a team event/promotion."""

    title: str
    event_date: str | None = None
    event_type: str = "이벤트"
    team_code: str = ""
    source_url: str | None = None
    thumbnail_url: str | None = None
    content: str | None = None
    extra_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TicketPriceParsed:
    """Parsed representation of a stadium ticket price grade."""

    seat_grade: str
    weekday_price: int | None = None
    weekend_price: int | None = None
    team_code: str = ""
    stadium_id: str = ""
    season: int = 0
    raw_info: dict[str, Any] = field(default_factory=dict)


@dataclass
class GameDetailParsed:
    """Parsed representation of a game box score and detail review."""

    game_id: str
    game_date: str
    season: int
    away_team: str
    home_team: str
    scoreboard: dict[str, Any] = field(default_factory=dict)
    hitter_stats: list[dict[str, Any]] = field(default_factory=list)
    pitcher_stats: list[dict[str, Any]] = field(default_factory=list)
    game_meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class RetiredPlayerStatsParsed:
    """Parsed representation of a retired or futures player career stats."""

    player_id: int | None = None
    player_name: str = ""
    batting_stats: list[dict[str, Any]] = field(default_factory=list)
    pitching_stats: list[dict[str, Any]] = field(default_factory=list)
    defense_stats: list[dict[str, Any]] = field(default_factory=list)
