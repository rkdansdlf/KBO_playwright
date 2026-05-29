"""Build LLM-ready game story timelines from normalized game events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Sequence

from src.models.game import Game, GameEvent
from src.utils.relay_text import compact_relay_text, is_relay_noise_text

STORY_SCHEMA_VERSION = "game_story.v1"
STORY_TIMELINE_LIMIT = 8
STORY_MIN_TIMELINE_EVENTS = 5
STORY_SUMMARY_TYPE = "경기_스토리"

_TAG_ORDER = (
    "walk_off",
    "decisive_score",
    "lead_change",
    "go_ahead",
    "game_tying",
    "home_run",
    "critical_error",
    "late_high_wpa",
    "high_wpa",
    "scoring_play",
    "rbi",
    "wpa_supplement",
)

_PRIMARY_TAGS = {
    "walk_off",
    "decisive_score",
    "lead_change",
    "go_ahead",
    "game_tying",
    "home_run",
    "critical_error",
    "late_high_wpa",
}


@dataclass
class _StoryContext:
    event: GameEvent
    inning_half: str | None
    batting_team: str | None
    score_before: dict[str, int | None]
    score_after: dict[str, int | None]
    score_diff_before: int | None
    score_diff_after: int | None
    runs_scored: int
    tags: set[str]
    importance_score: float


class GameStoryBuilder:
    """Derive a compact, structured game story from stored play-by-play events."""

    def build(self, game: Game, events: Iterable[GameEvent]) -> dict[str, Any]:
        ordered_events = sorted(
            list(events),
            key=lambda event: (
                event.event_seq if event.event_seq is not None else 10**9,
                event.id if event.id is not None else 10**9,
            ),
        )
        warnings: list[str] = []
        if not ordered_events:
            warnings.append("missing_game_events")

        contexts = self._build_contexts(game, ordered_events)
        if ordered_events and not contexts:
            warnings.append("no_valid_story_event_candidates")

        self._mark_decisive_score(game, contexts)
        for context in contexts:
            context.importance_score = self._importance_score(context)

        selected = self._select_timeline_contexts(contexts)
        if contexts and not selected:
            warnings.append("no_story_events_selected")
        if any(
            context.score_before["away"] is None
            or context.score_before["home"] is None
            or context.score_after["away"] is None
            or context.score_after["home"] is None
            for context in contexts
        ):
            warnings.append("event_score_state_incomplete")

        return {
            "schema_version": STORY_SCHEMA_VERSION,
            "game_id": game.game_id,
            "game_date": self._date_string(game.game_date),
            "teams": {
                "away": game.away_team,
                "home": game.home_team,
            },
            "final_score": {
                "away": game.away_score,
                "home": game.home_score,
                "text": self._final_score_text(game),
            },
            "timeline": [self._timeline_item(context) for context in selected],
            "story_flags": {
                "home_runs": sum(1 for context in selected if "home_run" in context.tags),
                "lead_changes": sum(1 for context in selected if "lead_change" in context.tags),
                "critical_errors": sum(1 for context in selected if "critical_error" in context.tags),
                "walk_off": any("walk_off" in context.tags for context in selected),
            },
            "source": {
                "game_events_rows": len(ordered_events),
                "story_event_candidates": len(contexts),
                "generated_at": self._source_timestamp(game, ordered_events),
                "warnings": warnings,
            },
        }

    def _build_contexts(self, game: Game, events: Sequence[GameEvent]) -> list[_StoryContext]:
        contexts: list[_StoryContext] = []
        previous_score = {"away": 0, "home": 0}

        for event in events:
            score_before = dict(previous_score)
            score_after = self._score_after(event, score_before)
            if score_after["away"] is not None and score_after["home"] is not None:
                previous_score = {
                    "away": int(score_after["away"]),
                    "home": int(score_after["home"]),
                }

            if not self._is_valid_event_row(event):
                continue

            inning_half = self._normalize_half(event.inning_half)
            batting_team = self._batting_team(game, inning_half)
            diff_before = self._score_diff(score_before)
            diff_after = self._score_diff(score_after)
            runs_scored = self._runs_scored(event, score_before, score_after)
            tags = self._base_tags(event, inning_half, diff_before, diff_after, runs_scored)
            context = _StoryContext(
                event=event,
                inning_half=inning_half,
                batting_team=batting_team,
                score_before=score_before,
                score_after=score_after,
                score_diff_before=diff_before,
                score_diff_after=diff_after,
                runs_scored=runs_scored,
                tags=tags,
                importance_score=0.0,
            )
            contexts.append(context)

        return contexts

    def _is_valid_event_row(self, event: GameEvent) -> bool:
        description = compact_relay_text(event.description)
        if not description or is_relay_noise_text(description):
            return False
        event_type = str(event.event_type or "").strip().lower()
        return event_type != "substitution"

    def _score_after(self, event: GameEvent, score_before: dict[str, int | None]) -> dict[str, int | None]:
        away_score = event.away_score if event.away_score is not None else score_before["away"]
        home_score = event.home_score if event.home_score is not None else score_before["home"]
        return {
            "away": int(away_score) if away_score is not None else None,
            "home": int(home_score) if home_score is not None else None,
        }

    def _runs_scored(
        self,
        event: GameEvent,
        score_before: dict[str, int | None],
        score_after: dict[str, int | None],
    ) -> int:
        if None not in (
            score_before["away"],
            score_before["home"],
            score_after["away"],
            score_after["home"],
        ):
            before_total = int(score_before["away"] or 0) + int(score_before["home"] or 0)
            after_total = int(score_after["away"] or 0) + int(score_after["home"] or 0)
            return max(0, after_total - before_total)
        return max(0, int(event.rbi or 0))

    def _base_tags(
        self,
        event: GameEvent,
        inning_half: str | None,
        score_diff_before: int | None,
        score_diff_after: int | None,
        runs_scored: int,
    ) -> set[str]:
        tags: set[str] = set()
        abs_wpa = abs(float(event.wpa or 0.0))
        description = compact_relay_text(event.description)
        result_code = str(event.result_code or "").strip().upper()

        if runs_scored > 0 or int(event.rbi or 0) > 0 or "홈인" in description:
            tags.add("scoring_play")
        if int(event.rbi or 0) > 0:
            tags.add("rbi")
        if result_code == "HR" or "홈런" in description:
            tags.add("home_run")
        if "실책" in description or result_code in {"E", "ROE"}:
            if runs_scored > 0 or abs_wpa >= 0.2 or (event.inning or 0) >= 7:
                tags.add("critical_error")
        if score_diff_before is not None and score_diff_after is not None:
            if score_diff_before != 0 and score_diff_after == 0:
                tags.add("game_tying")
            elif score_diff_before == 0 and score_diff_after != 0:
                tags.add("go_ahead")
            elif score_diff_before * score_diff_after < 0:
                tags.add("lead_change")
        if (event.inning or 0) >= 7 and abs_wpa >= 0.15:
            tags.add("late_high_wpa")
        if abs_wpa >= 0.25:
            tags.add("high_wpa")
        if (
            inning_half == "bottom"
            and (event.inning or 0) >= 9
            and score_diff_before is not None
            and score_diff_after is not None
            and score_diff_before <= 0
            and score_diff_after > 0
            and runs_scored > 0
        ):
            tags.add("walk_off")

        return tags

    def _mark_decisive_score(self, game: Game, contexts: Sequence[_StoryContext]) -> None:
        final_diff = self._final_score_diff(game)
        final_sign = self._sign(final_diff)
        if final_sign == 0:
            return

        candidate: _StoryContext | None = None
        for context in contexts:
            before_sign = self._sign(context.score_diff_before)
            after_sign = self._sign(context.score_diff_after)
            if context.runs_scored > 0 and after_sign == final_sign and before_sign != final_sign:
                candidate = context
        if candidate is not None:
            candidate.tags.add("decisive_score")

    def _select_timeline_contexts(self, contexts: Sequence[_StoryContext]) -> list[_StoryContext]:
        selected: dict[int, _StoryContext] = {}
        for context in contexts:
            if context.tags & _PRIMARY_TAGS:
                selected[self._context_key(context)] = context

        if len(selected) < STORY_MIN_TIMELINE_EVENTS:
            for context in sorted(
                contexts,
                key=lambda item: (
                    -abs(float(item.event.wpa or 0.0)),
                    item.event.event_seq if item.event.event_seq is not None else 10**9,
                ),
            ):
                key = self._context_key(context)
                if key in selected:
                    continue
                context.tags.add("wpa_supplement")
                context.importance_score = self._importance_score(context)
                selected[key] = context
                if len(selected) >= min(STORY_MIN_TIMELINE_EVENTS, len(contexts)):
                    break

        chosen = sorted(
            selected.values(),
            key=lambda item: (
                -item.importance_score,
                -abs(float(item.event.wpa or 0.0)),
                item.event.event_seq if item.event.event_seq is not None else 10**9,
            ),
        )[:STORY_TIMELINE_LIMIT]
        return sorted(
            chosen,
            key=lambda item: item.event.event_seq if item.event.event_seq is not None else 10**9,
        )

    def _importance_score(self, context: _StoryContext) -> float:
        score = abs(float(context.event.wpa or 0.0)) * 100
        weights = {
            "walk_off": 70,
            "decisive_score": 50,
            "lead_change": 45,
            "go_ahead": 35,
            "game_tying": 35,
            "home_run": 35,
            "critical_error": 25,
            "late_high_wpa": 20,
            "high_wpa": 15,
            "scoring_play": 10,
            "rbi": 5,
            "wpa_supplement": 2,
        }
        score += sum(weight for tag, weight in weights.items() if tag in context.tags)
        return round(score, 4)

    def _timeline_item(self, context: _StoryContext) -> dict[str, Any]:
        event = context.event
        return {
            "event_seq": event.event_seq,
            "inning_label": self._inning_label(event.inning, context.inning_half),
            "batting_team": context.batting_team,
            "tags": self._ordered_tags(context.tags),
            "importance_score": context.importance_score,
            "wpa": event.wpa,
            "score_before": context.score_before,
            "score_after": context.score_after,
            "runs_scored": context.runs_scored,
            "rbi": event.rbi,
            "batter": event.batter_name,
            "pitcher": event.pitcher_name,
            "description": compact_relay_text(event.description),
        }

    def _ordered_tags(self, tags: set[str]) -> list[str]:
        return [tag for tag in _TAG_ORDER if tag in tags]

    def _context_key(self, context: _StoryContext) -> int:
        if context.event.event_seq is not None:
            return int(context.event.event_seq)
        return int(context.event.id or 0)

    def _batting_team(self, game: Game, inning_half: str | None) -> str | None:
        if inning_half == "top":
            return game.away_team
        if inning_half == "bottom":
            return game.home_team
        return None

    def _normalize_half(self, value: Any) -> str | None:
        normalized = str(value or "").strip().lower()
        if normalized in {"top", "away", "초"}:
            return "top"
        if normalized in {"bottom", "home", "말"}:
            return "bottom"
        return None

    def _inning_label(self, inning: int | None, inning_half: str | None) -> str | None:
        if inning is None:
            return None
        suffix = {"top": "초", "bottom": "말"}.get(inning_half, "")
        return f"{inning}회{suffix}"

    def _score_diff(self, score: dict[str, int | None]) -> int | None:
        if score["away"] is None or score["home"] is None:
            return None
        return int(score["home"] or 0) - int(score["away"] or 0)

    def _final_score_diff(self, game: Game) -> int | None:
        if game.home_score is None or game.away_score is None:
            return None
        return int(game.home_score) - int(game.away_score)

    def _sign(self, value: int | None) -> int:
        if value is None or value == 0:
            return 0
        return 1 if value > 0 else -1

    def _date_string(self, value: date | datetime | None) -> str | None:
        if value is None:
            return None
        return value.strftime("%Y%m%d")

    def _final_score_text(self, game: Game) -> str:
        return f"{game.away_team} {game.away_score} : {game.home_score} {game.home_team}"

    def _source_timestamp(self, game: Game, events: Sequence[GameEvent]) -> str:
        timestamps = [
            value
            for value in (
                getattr(game, "updated_at", None),
                getattr(game, "created_at", None),
                *(getattr(event, "updated_at", None) for event in events),
                *(getattr(event, "created_at", None) for event in events),
            )
            if isinstance(value, datetime)
        ]
        if timestamps:
            return self._format_timestamp(max(timestamps))
        if game.game_date:
            return f"{game.game_date.strftime('%Y-%m-%d')}T00:00:00Z"
        return "1970-01-01T00:00:00Z"

    def _format_timestamp(self, value: datetime) -> str:
        value = value.replace(microsecond=0)
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return f"{value.isoformat()}Z"
