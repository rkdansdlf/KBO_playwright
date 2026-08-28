"""Data Transfer Objects for KBO live game event simulation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SimulationEvent:
    """Represents a single play/at-bat event during a game simulation."""

    event_seq: int
    inning: int
    is_bottom: bool
    batter_name: str
    pitcher_name: str
    result_type: str
    description: str
    outs_before: int
    runners_before: int
    outs_after: int
    runners_after: int
    runs_scored: int = 0
    home_score: int = 0
    away_score: int = 0
    win_prob_before: float = 0.50
    win_prob_after: float = 0.50
    wpa: float = 0.0
    leverage_index: float = 1.0
    is_hot_moment: bool = False
    batter_id: int | None = None
    pitcher_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert event to serializable dictionary."""
        return {
            "event_seq": self.event_seq,
            "inning": self.inning,
            "half": "BOTTOM" if self.is_bottom else "TOP",
            "batter_name": self.batter_name,
            "pitcher_name": self.pitcher_name,
            "result_type": self.result_type,
            "description": self.description,
            "outs_before": self.outs_before,
            "runners_before": self.runners_before,
            "outs_after": self.outs_after,
            "runners_after": self.runners_after,
            "runs_scored": self.runs_scored,
            "home_score": self.home_score,
            "away_score": self.away_score,
            "win_prob_before": round(self.win_prob_before, 4),
            "win_prob_after": round(self.win_prob_after, 4),
            "wpa": round(self.wpa, 4),
            "leverage_index": round(self.leverage_index, 2),
            "is_hot_moment": self.is_hot_moment,
            "batter_id": self.batter_id,
            "pitcher_id": self.pitcher_id,
        }


@dataclass
class SimulationGameState:
    """In-memory live state tracking for an ongoing simulated game."""

    game_id: str
    home_team: str = "KIA"
    away_team: str = "LG"
    current_inning: int = 1
    is_bottom: bool = False
    outs: int = 0
    runners: int = 0
    home_score: int = 0
    away_score: int = 0
    is_finished: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert game state to dictionary."""
        return {
            "game_id": self.game_id,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "current_inning": self.current_inning,
            "half": "BOTTOM" if self.is_bottom else "TOP",
            "outs": self.outs,
            "runners": self.runners,
            "home_score": self.home_score,
            "away_score": self.away_score,
            "is_finished": self.is_finished,
        }


@dataclass
class SimulationSummary:
    """Summary of a completed game simulation."""

    game_id: str
    home_team: str
    away_team: str
    final_score: str
    winner: str
    total_innings: int
    total_events: int
    hot_moments_count: int
    hero_player: str
    hero_wpa: float
    goat_player: str
    goat_wpa: float
    duration_seconds: float
    events: list[SimulationEvent] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert summary to serializable dictionary."""
        return {
            "game_id": self.game_id,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "final_score": self.final_score,
            "winner": self.winner,
            "total_innings": self.total_innings,
            "total_events": self.total_events,
            "hot_moments_count": self.hot_moments_count,
            "hero_player": self.hero_player,
            "hero_wpa": round(self.hero_wpa, 4),
            "goat_player": self.goat_player,
            "goat_wpa": round(self.goat_wpa, 4),
            "duration_seconds": round(self.duration_seconds, 2),
            "total_plays": len(self.events),
        }
