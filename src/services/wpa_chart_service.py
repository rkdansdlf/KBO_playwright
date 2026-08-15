"""WPA (Win Probability Added) and Win Expectancy Chart Service."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.game import Game, GameEvent, GameHighlight, GamePlayByPlay
from src.services.wpa_calculator import WPACalculator

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

GAME_CHANGER_WPA_THRESHOLD = 0.25
CLUTCH_WPA_THRESHOLD = 0.15
BIG_PLAY_WPA_THRESHOLD = 0.08


@dataclass
class WpaTimelineItem:
    """Single timeline item for Win Expectancy chart."""

    event_seq: int
    inning: int
    inning_half: str
    batter_name: str | None
    pitcher_name: str | None
    description: str
    home_win_prob: float
    wpa: float
    home_score: int
    away_score: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to serializable dictionary."""
        return {
            "event_seq": self.event_seq,
            "inning": self.inning,
            "inning_half": self.inning_half,
            "batter_name": self.batter_name,
            "pitcher_name": self.pitcher_name,
            "description": self.description,
            "home_win_prob": round(self.home_win_prob, 4),
            "wpa": round(self.wpa, 4),
            "home_score": self.home_score,
            "away_score": self.away_score,
        }


@dataclass
class WpaTurningPoint:
    """Key momentum shifting play in a game."""

    event_seq: int
    inning: int
    inning_half: str
    description: str
    batter_name: str | None
    wpa: float
    importance_score: float
    impact_type: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to serializable dictionary."""
        return {
            "event_seq": self.event_seq,
            "inning": self.inning,
            "inning_half": self.inning_half,
            "description": self.description,
            "batter_name": self.batter_name,
            "wpa": round(self.wpa, 4),
            "importance_score": round(self.importance_score, 4),
            "impact_type": self.impact_type,
        }


class WpaChartService:
    """Service to compute and assemble Win Probability time-series for games."""

    def __init__(self, session: Session) -> None:
        """Initialize WPA Chart Service."""
        self.session = session
        self.wpa_calc = WPACalculator()

    def get_game_wpa_chart(self, game_id: str, top_turning_points: int = 5) -> dict[str, Any] | None:
        """Generate full Win Expectancy timeline and key turning points for a game.

        Args:
            game_id: Canonical game ID.
            top_turning_points: Number of top momentum plays to extract.

        Returns:
            Dictionary payload for WPA chart or None if game not found.

        """
        game = self.session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if not game:
            return None

        # 1. Try to load pre-calculated GameEvents
        events_stmt = select(GameEvent).where(GameEvent.game_id == game_id).order_by(GameEvent.event_seq.asc())
        events = list(self.session.execute(events_stmt).scalars().all())

        timeline: list[WpaTimelineItem] = []
        if events and any(e.win_expectancy_after is not None for e in events):
            timeline = self._build_timeline_from_events(events)
        else:
            # Fallback to play-by-play dynamic simulation
            timeline = self._build_timeline_from_pbp(game_id)

        # 2. Extract turning points (Top N absolute WPA changes)
        turning_points = self._extract_turning_points(timeline, top_n=top_turning_points)

        # 3. Calculate team total WPAs
        home_wpa = sum(item.wpa for item in timeline if item.inning_half.lower() in ("bottom", "말", "b"))
        away_wpa = sum(item.wpa for item in timeline if item.inning_half.lower() in ("top", "초", "t"))

        return {
            "game_id": game.game_id,
            "game_date": str(game.game_date),
            "stadium": game.stadium or "",
            "home_team": game.home_team or "",
            "away_team": game.away_team or "",
            "home_score": game.home_score or 0,
            "away_score": game.away_score or 0,
            "game_status": game.game_status or "COMPLETED",
            "timeline": [t.to_dict() for t in timeline],
            "turning_points": [tp.to_dict() for tp in turning_points],
            "home_total_wpa": round(home_wpa, 4),
            "away_total_wpa": round(away_wpa, 4),
        }

    def _build_timeline_from_events(self, events: list[GameEvent]) -> list[WpaTimelineItem]:
        """Assemble timeline from stored GameEvent rows."""
        items: list[WpaTimelineItem] = []
        for e in events:
            win_prob = e.win_expectancy_after if e.win_expectancy_after is not None else 0.5
            wpa_val = e.wpa if e.wpa is not None else 0.0

            items.append(
                WpaTimelineItem(
                    event_seq=e.event_seq or len(items) + 1,
                    inning=e.inning or 1,
                    inning_half=e.inning_half or "top",
                    batter_name=e.batter_name,
                    pitcher_name=e.pitcher_name,
                    description=e.description or "",
                    home_win_prob=win_prob,
                    wpa=wpa_val,
                    home_score=e.home_score or 0,
                    away_score=e.away_score or 0,
                )
            )
        return items

    def _build_timeline_from_pbp(self, game_id: str) -> list[WpaTimelineItem]:
        """Dynamically compute win probabilities from play-by-play data."""
        pbp_stmt = select(GamePlayByPlay).where(GamePlayByPlay.game_id == game_id).order_by(GamePlayByPlay.id.asc())
        plays = list(self.session.execute(pbp_stmt).scalars().all())

        items: list[WpaTimelineItem] = []
        current_home_score = 0
        current_away_score = 0
        prev_home_prob = 0.50

        for idx, play in enumerate(plays, start=1):
            inn = play.inning or 1
            half = (play.inning_half or "top").lower()
            is_bottom = half in ("bottom", "말", "b")

            score_diff = current_home_score - current_away_score

            home_prob = self.wpa_calc.get_win_probability(
                inning=inn,
                is_bottom=is_bottom,
                outs=1,
                runners=0,
                score_diff=score_diff,
            )

            wpa_change = (home_prob - prev_home_prob) if is_bottom else (prev_home_prob - home_prob)

            items.append(
                WpaTimelineItem(
                    event_seq=idx,
                    inning=inn,
                    inning_half=play.inning_half or "top",
                    batter_name=play.batter_name,
                    pitcher_name=play.pitcher_name,
                    description=play.play_description or "",
                    home_win_prob=home_prob,
                    wpa=wpa_change,
                    home_score=current_home_score,
                    away_score=current_away_score,
                )
            )
            prev_home_prob = home_prob

        if not items:
            items.append(
                WpaTimelineItem(
                    event_seq=1,
                    inning=1,
                    inning_half="top",
                    batter_name=None,
                    pitcher_name=None,
                    description="경기 시작",
                    home_win_prob=0.50,
                    wpa=0.0,
                    home_score=0,
                    away_score=0,
                )
            )

        return items

    def _extract_turning_points(self, timeline: list[WpaTimelineItem], top_n: int = 5) -> list[WpaTurningPoint]:
        """Extract top N plays with the largest absolute WPA change."""
        if not timeline:
            return []

        sorted_items = sorted(timeline, key=lambda x: abs(x.wpa), reverse=True)
        top_items = sorted_items[:top_n]

        turning_points: list[WpaTurningPoint] = []
        for item in top_items:
            abs_wpa = abs(item.wpa)
            if abs_wpa >= GAME_CHANGER_WPA_THRESHOLD:
                impact = "GAME_CHANGER"
            elif abs_wpa >= CLUTCH_WPA_THRESHOLD:
                impact = "CLUTCH"
            elif abs_wpa >= BIG_PLAY_WPA_THRESHOLD:
                impact = "BIG_PLAY"
            else:
                impact = "MOMENTUM"

            turning_points.append(
                WpaTurningPoint(
                    event_seq=item.event_seq,
                    inning=item.inning,
                    inning_half=item.inning_half,
                    description=item.description,
                    batter_name=item.batter_name,
                    wpa=item.wpa,
                    importance_score=abs_wpa,
                    impact_type=impact,
                )
            )

        turning_points.sort(key=lambda x: x.event_seq)
        return turning_points

    def get_game_highlights(self, game_id: str) -> list[dict[str, Any]]:
        """Retrieve stored highlights for a game."""
        stmt = (
            select(GameHighlight)
            .where(GameHighlight.game_id == game_id)
            .order_by(GameHighlight.importance_score.desc())
        )
        highlights = list(self.session.execute(stmt).scalars().all())

        return [
            {
                "id": h.id,
                "game_id": h.game_id,
                "event_seq": h.event_seq,
                "inning": h.inning,
                "inning_half": h.inning_half,
                "highlight_type": h.highlight_type,
                "description": h.description,
                "wpa": h.wpa,
                "importance_score": h.importance_score,
                "tags": h.tags or [],
            }
            for h in highlights
        ]
