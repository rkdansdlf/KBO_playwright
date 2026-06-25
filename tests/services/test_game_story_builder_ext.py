from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from src.models.game import Game, GameEvent
from src.services.game_story_builder import (
    STORY_TIMELINE_LIMIT,
    GameStoryBuilder,
)


def _make_game(**kwargs):
    defaults = {
        "game_id": "20240501LGSS0",
        "game_date": date(2024, 5, 1),
        "away_team": "LG",
        "home_team": "SS",
        "away_score": 3,
        "home_score": 5,
        "game_status": "completed",
    }
    defaults.update(kwargs)
    return MagicMock(spec=Game, **defaults)


def _make_event(**kwargs):
    defaults = {
        "id": 1,
        "event_seq": 1,
        "inning": 1,
        "inning_half": "top",
        "away_score": 0,
        "home_score": 0,
        "description": "안타",
        "event_type": "hit",
        "result_code": "H",
        "rbi": 0,
        "wpa": 0.05,
        "batter_name": "Kim",
        "pitcher_name": "Park",
        "batter_id": 1,
        "pitcher_id": 2,
        "outs_before": 0,
        "outs_after": 1,
        "bases_before": "000",
        "bases_after": "100",
    }
    defaults.update(kwargs)
    return MagicMock(spec=GameEvent, **defaults)


class TestBuild:
    def test_no_events_returns_warning(self):
        builder = GameStoryBuilder()
        game = _make_game()
        result = builder.build(game, [])
        assert "missing_game_events" in result["source"]["warnings"]
        assert result["timeline"] == []

    def test_single_event_creates_timeline(self):
        builder = GameStoryBuilder()
        game = _make_game()
        event = _make_event(description="홈런", result_code="HR", rbi=2, wpa=0.8, inning=5, inning_half="bottom")
        result = builder.build(game, [event])
        assert len(result["timeline"]) >= 1
        assert result["story_flags"]["home_runs"] >= 0

    def test_final_score_text_format(self):
        builder = GameStoryBuilder()
        game = _make_game(away_team="LG", home_team="SS", away_score=3, home_score=5)
        assert builder._final_score_text(game) == "LG 3 : 5 SS"


class TestIsValidEventRow:
    def test_valid_event(self):
        builder = GameStoryBuilder()
        event = _make_event(description="안타", event_type="hit")
        assert builder._is_valid_event_row(event) is True

    def test_substitution_is_invalid(self):
        builder = GameStoryBuilder()
        event = _make_event(description="교체", event_type="substitution")
        assert builder._is_valid_event_row(event) is False

    def test_noisy_description_is_invalid(self):
        builder = GameStoryBuilder()
        event = _make_event(description="파울")
        with pytest.MonkeyPatch().context() as mp:
            mp.setattr("src.services.game_story_builder.is_relay_noise_text", lambda x: True)
            assert builder._is_valid_event_row(event) is False


class TestBaseTags:
    def test_home_run_tag(self):
        builder = GameStoryBuilder()
        event = _make_event(result_code="HR", description="홈런")
        tags = builder._base_tags(event, "top", 0, 1, 1)
        assert "home_run" in tags

    def test_scoring_play_tag(self):
        builder = GameStoryBuilder()
        event = _make_event(rbi=1)
        tags = builder._base_tags(event, "top", 0, 1, 1)
        assert "scoring_play" in tags

    def test_game_tying_tag(self):
        builder = GameStoryBuilder()
        tags = builder._base_tags(
            MagicMock(wpa=0.1, rbi=0, result_code="H", description="안타", inning=5, inning_half="top"), "top", -1, 0, 1
        )
        assert "game_tying" in tags

    def test_go_ahead_tag(self):
        builder = GameStoryBuilder()
        tags = builder._base_tags(
            MagicMock(wpa=0.1, rbi=0, result_code="H", description="안타", inning=5, inning_half="top"), "top", 0, 1, 1
        )
        assert "go_ahead" in tags

    def test_walk_off_tag(self):
        builder = GameStoryBuilder()
        tags = builder._base_tags(
            MagicMock(wpa=0.5, rbi=1, result_code="H", description="끝내기 안타", inning=9, inning_half="bottom"),
            "bottom",
            -1,
            1,
            1,
        )
        assert "walk_off" in tags

    def test_high_wpa_tag(self):
        builder = GameStoryBuilder()
        tags = builder._base_tags(
            MagicMock(
                spec=["wpa", "rbi", "result_code", "description", "inning", "event_type"],
                wpa=0.3,
                rbi=0,
                result_code="H",
                description="안타",
                inning=5,
                event_type="hit",
            ),
            "top",
            0,
            1,
            1,
        )
        assert "high_wpa" in tags


class TestNormalizeHalf:
    def test_top_variants(self):
        builder = GameStoryBuilder()
        assert builder._normalize_half("top") == "top"
        assert builder._normalize_half("away") == "top"
        assert builder._normalize_half("초") == "top"

    def test_bottom_variants(self):
        builder = GameStoryBuilder()
        assert builder._normalize_half("bottom") == "bottom"
        assert builder._normalize_half("home") == "bottom"
        assert builder._normalize_half("말") == "bottom"

    def test_invalid(self):
        builder = GameStoryBuilder()
        assert builder._normalize_half("") is None
        assert builder._normalize_half(None) is None


class TestBattingTeam:
    def test_top_is_away(self):
        builder = GameStoryBuilder()
        game = _make_game(away_team="LG", home_team="SS")
        assert builder._batting_team(game, "top") == "LG"

    def test_bottom_is_home(self):
        builder = GameStoryBuilder()
        game = _make_game(away_team="LG", home_team="SS")
        assert builder._batting_team(game, "bottom") == "SS"

    def test_none(self):
        builder = GameStoryBuilder()
        game = _make_game()
        assert builder._batting_team(game, None) is None


class TestImportanceScore:
    def test_based_on_wpa_and_tags(self):
        builder = GameStoryBuilder()
        ctx = MagicMock()
        ctx.event.wpa = 0.5
        ctx.tags = {"home_run", "go_ahead"}
        score = builder._importance_score(ctx)
        assert score > 50

    def test_minimal_score(self):
        builder = GameStoryBuilder()
        ctx = MagicMock()
        ctx.event.wpa = 0.0
        ctx.tags = {"rbi"}
        score = builder._importance_score(ctx)
        assert score == 5.0

    def test_no_tags_score(self):
        builder = GameStoryBuilder()
        ctx = MagicMock()
        ctx.event.wpa = 0.0
        ctx.tags = set()
        score = builder._importance_score(ctx)
        assert score == 0.0


class TestSelectTimelineContexts:
    def test_returns_limited_results(self):
        builder = GameStoryBuilder()
        contexts = []
        for i in range(20):
            ctx = MagicMock()
            ctx.event.wpa = 0.1
            ctx.tags = {"scoring_play"}
            ctx.event.event_seq = i
            contexts.append(ctx)
        result = builder._select_timeline_contexts(contexts)
        assert len(result) <= STORY_TIMELINE_LIMIT

    def test_primary_tags_prioritized(self):
        builder = GameStoryBuilder()
        high = MagicMock(spec=["event", "tags", "importance_score"])
        high.event = MagicMock(spec=["wpa", "event_seq", "id"])
        high.event.wpa = 0.01
        high.event.event_seq = 0
        high.event.id = 1
        high.tags = {"home_run"}
        high.importance_score = 0.0
        low = MagicMock(spec=["event", "tags", "importance_score"])
        low.event = MagicMock(spec=["wpa", "event_seq", "id"])
        low.event.wpa = 0.5
        low.event.event_seq = 1
        low.event.id = 2
        low.tags = {"rbi"}
        low.importance_score = 0.0
        result = builder._select_timeline_contexts([low, high])
        assert len(result) >= 1
        assert any("home_run" in ctx.tags for ctx in result)
