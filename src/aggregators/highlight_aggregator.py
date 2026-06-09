from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.game import GameEvent, GameHighlight


class HighlightAggregator:
    """Computes and tags game highlights from Play-by-Play event records."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def aggregate_game_highlights(self, game_id: str, min_wpa_threshold: float = 0.05) -> list[GameHighlight]:
        """
        Scans all play events for a given game, detects significant plays,
        tags them, and calculates an importance score for ranking.
        """
        # Query play events only, ignoring noisy pitch-by-pitch rows, substitutions, etc.
        events = (
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

        if not events:
            return []

        highlights = []
        prev_home_score = 0
        prev_away_score = 0

        for index, e in enumerate(events):
            home_score = e.home_score if e.home_score is not None else 0
            away_score = e.away_score if e.away_score is not None else 0

            # For the very first event, initialize prev scores to the state before the event
            if index == 0:
                # If there are already runs before the first recorded event,
                # initialize prev scores by deducting this event's RBI / run changes
                # but default to 0-0 since baseball games start 0-0.
                pass

            score_diff_before = prev_home_score - prev_away_score
            score_diff_after = home_score - away_score

            wpa_val = e.wpa or 0.0
            abs_wpa = abs(wpa_val)

            tags = []
            highlight_type = "OTHER"

            # 1. Walk-off (끝내기) Detection
            is_walkoff = False
            # Bottom of 9th or later, score diff transitions to home team lead, ending the game
            is_bottom_late = (e.inning or 1) >= 9 and e.inning_half == "bottom"
            if (
                is_bottom_late
                and score_diff_before <= 0
                and score_diff_after > 0
                or e.description
                and "끝내기" in e.description
            ):
                is_walkoff = True

            if is_walkoff:
                tags.append("끝내기")
                highlight_type = "WALK_OFF"

            # 2. Lead Change (역전) Detection
            if score_diff_before * score_diff_after < 0:
                tags.append("역전")
                if highlight_type == "OTHER":
                    highlight_type = "LEAD_CHANGE"

            # 3. Game Tying (동점) or Go-Ahead (동점 균열) Detection
            elif score_diff_before != 0 and score_diff_after == 0:
                tags.append("동점")
                if highlight_type == "OTHER":
                    highlight_type = "GAME_TYING"
            elif score_diff_before == 0 and score_diff_after != 0:
                tags.append("동점 균열")
                if highlight_type == "OTHER":
                    highlight_type = "GO_AHEAD"

            # 4. Bases Loaded (만루) Detection
            if e.bases_before == "123":
                tags.append("만루")

            # 5. Double Play (병살) Detection
            if e.description and "병살" in e.description:
                tags.append("병살")

            # 6. Home Run (홈런) Detection
            is_hr = False
            if e.description and "홈런" in e.description or e.event_type and e.event_type.lower() in ("hr", "homerun"):
                is_hr = True

            if is_hr:
                tags.append("홈런")
                if "만루" in tags:
                    tags.append("만루홈런")

            # 7. Importance Score calculation (heuristics)
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

            # Late inning clutch multiplier/bonus
            inning_val = e.inning or 1
            importance += 0.01 * inning_val

            # Tag as BIG_PLAY if WPA is high enough
            if abs_wpa >= min_wpa_threshold and highlight_type == "OTHER":
                highlight_type = "BIG_PLAY"

            # Retain the highlight if it's significant or has interesting tags
            is_significant = (
                highlight_type in ("WALK_OFF", "LEAD_CHANGE", "GAME_TYING", "GO_AHEAD", "BIG_PLAY")
                or any(t in tags for t in ("홈런", "만루홈런"))
                or abs_wpa >= min_wpa_threshold
            )

            if is_significant:
                if not tags:
                    tags.append("빅플레이")

                h = GameHighlight(
                    game_id=game_id,
                    event_seq=e.event_seq,
                    inning=e.inning,
                    inning_half=e.inning_half,
                    highlight_type=highlight_type,
                    description=e.description,
                    wpa=wpa_val,
                    importance_score=round(importance, 4),
                    tags=tags,
                )
                highlights.append(h)

            prev_home_score = home_score
            prev_away_score = away_score

        # Sort highlights by importance_score descending
        highlights.sort(key=lambda h: h.importance_score, reverse=True)
        return highlights

    def save_highlights(self, game_id: str, highlights: list[GameHighlight]) -> int:
        """Deletes existing highlights for a game and saves the new ones."""
        self.session.query(GameHighlight).filter(GameHighlight.game_id == game_id).delete()
        if highlights:
            self.session.add_all(highlights)
        self.session.commit()
        return len(highlights)
