from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.game import GameEvent, GameHighlight


class HighlightAggregator:
    """Computes and tags game highlights from Play-by-Play event records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def _query_play_events(self, game_id: str) -> list[GameEvent]:
        return (
            self.session.query(GameEvent)
            .filter(
                GameEvent.game_id == game_id,
                GameEvent.wpa.isnot(None),
                GameEvent.event_type.isnot(None),
                ~func.lower(GameEvent.event_type).in_(("unknown", "other", "substitution")),
            )
            .order_by(GameEvent.event_seq.asc())
            .all()
        )

    @staticmethod
    def _is_walkoff(event: GameEvent, score_diff_before: int, score_diff_after: int) -> bool:
        is_bottom_late = (event.inning or 1) >= 9 and event.inning_half == "bottom"
        return (
            is_bottom_late
            and score_diff_before <= 0
            and score_diff_after > 0
            or event.description
            and "끝내기" in event.description
        )

    @staticmethod
    def _is_home_run(event: GameEvent) -> bool:
        return bool(
            event.description
            and "홈런" in event.description
            or event.event_type
            and event.event_type.lower() in ("hr", "homerun"),
        )

    def _detect_tags_and_type(
        self, event: GameEvent, score_diff_before: int, score_diff_after: int
    ) -> tuple[list[str], str]:
        tags = []
        highlight_type = "OTHER"

        if self._is_walkoff(event, score_diff_before, score_diff_after):
            tags.append("끝내기")
            highlight_type = "WALK_OFF"

        if score_diff_before * score_diff_after < 0:
            tags.append("역전")
            if highlight_type == "OTHER":
                highlight_type = "LEAD_CHANGE"
        elif score_diff_before != 0 and score_diff_after == 0:
            tags.append("동점")
            if highlight_type == "OTHER":
                highlight_type = "GAME_TYING"
        elif score_diff_before == 0 and score_diff_after != 0:
            tags.append("동점 균열")
            if highlight_type == "OTHER":
                highlight_type = "GO_AHEAD"

        self._add_event_tags(event, tags)
        return tags, highlight_type

    @staticmethod
    def _add_event_tags(event: GameEvent, tags: list[str]) -> None:
        if event.bases_before == "123":
            tags.append("만루")
        if event.description and "병살" in event.description:
            tags.append("병살")
        if HighlightAggregator._is_home_run(event):
            tags.append("홈런")
            if "만루" in tags:
                tags.append("만루홈런")

    @staticmethod
    def _importance_score(event: GameEvent, tags: list[str], abs_wpa: float) -> float:
        importance = abs_wpa
        if "끝내기" in tags:
            importance += 0.5
        if "역전" in tags:
            importance += 0.25
        if "동점" in tags:
            importance += 0.15
        if "동점 균열" in tags:
            importance += 0.10
        if "홈런" in tags:
            importance += 0.05
        return importance + 0.01 * (event.inning or 1)

    @staticmethod
    def _is_significant(highlight_type: str, tags: list[str], abs_wpa: float, min_wpa_threshold: float) -> bool:
        return (
            highlight_type in ("WALK_OFF", "LEAD_CHANGE", "GAME_TYING", "GO_AHEAD", "BIG_PLAY")
            or any(tag in tags for tag in ("홈런", "만루홈런"))
            or abs_wpa >= min_wpa_threshold
        )

    def _highlight_from_event(
        self,
        game_id: str,
        event: GameEvent,
        prev_home_score: int,
        prev_away_score: int,
        min_wpa_threshold: float,
    ) -> GameHighlight | None:
        home_score = event.home_score if event.home_score is not None else 0
        away_score = event.away_score if event.away_score is not None else 0
        score_diff_before = prev_home_score - prev_away_score
        score_diff_after = home_score - away_score
        wpa_val = event.wpa or 0.0
        abs_wpa = abs(wpa_val)
        tags, highlight_type = self._detect_tags_and_type(event, score_diff_before, score_diff_after)
        importance = self._importance_score(event, tags, abs_wpa)

        if abs_wpa >= min_wpa_threshold and highlight_type == "OTHER":
            highlight_type = "BIG_PLAY"
        if not self._is_significant(highlight_type, tags, abs_wpa, min_wpa_threshold):
            return None
        if not tags:
            tags.append("빅플레이")
        return GameHighlight(
            game_id=game_id,
            event_seq=event.event_seq,
            inning=event.inning,
            inning_half=event.inning_half,
            highlight_type=highlight_type,
            description=event.description,
            wpa=wpa_val,
            importance_score=round(importance, 4),
            tags=tags,
        )

    def aggregate_game_highlights(self, game_id: str, min_wpa_threshold: float = 0.05) -> list[GameHighlight]:
        """
        Scans all play events for a given game, detects significant plays,
        tags them, and calculates an importance score for ranking.
        """
        events = self._query_play_events(game_id)
        if not events:
            return []

        highlights = []
        prev_home_score = 0
        prev_away_score = 0

        for event in events:
            highlight = self._highlight_from_event(game_id, event, prev_home_score, prev_away_score, min_wpa_threshold)
            if highlight:
                highlights.append(highlight)
            home_score = event.home_score if event.home_score is not None else 0
            away_score = event.away_score if event.away_score is not None else 0
            prev_home_score = home_score
            prev_away_score = away_score

        # Sort highlights by importance_score descending
        highlights.sort(key=lambda h: h.importance_score, reverse=True)
        return highlights

    def save_highlights(self, game_id: str, highlights: list[GameHighlight]) -> int:
        """Deletes existing highlights for a game and saves the new ones."""
        existing = self.session.query(GameHighlight).filter(GameHighlight.game_id == game_id).all()
        for highlight in existing:
            self.session.delete(highlight)
        if existing:
            self.session.flush()
        if highlights:
            self.session.add_all(highlights)
        self.session.commit()
        return len(highlights)
