"""Tests for game_story_builder pure helper methods."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from src.services.game_story_builder import GameStoryBuilder


@dataclass
class _MockGameEvent:
    rbi: int | None = None
    wpa: float | None = None
    inning: int | None = None
    inning_half: str | None = None
    description: str | None = None
    result_code: str | None = None
    hat: str | None = None


class TestScoringTags:
    def _builder(self) -> GameStoryBuilder:
        return GameStoryBuilder.__new__(GameStoryBuilder)

    def test_scoring_play_on_runs(self) -> None:
        b = self._builder()
        event = _MockGameEvent(rbi=1)
        tags = b._scoring_tags(event, "", "1B", 1)
        assert "scoring_play" in tags
        assert "rbi" in tags

    def test_home_run_tag(self) -> None:
        b = self._builder()
        event = _MockGameEvent(rbi=4)
        tags = b._scoring_tags(event, "홈런", "HR", 4)
        assert "home_run" in tags
        assert "rbi" in tags

    def test_no_scoring_on_no_runs(self) -> None:
        b = self._builder()
        event = _MockGameEvent(rbi=0)
        tags = b._scoring_tags(event, "", "GO", 0)
        assert "scoring_play" not in tags

    def test_critical_error_roE_with_runs(self) -> None:
        b = self._builder()
        event = _MockGameEvent(wpa=0.3, inning=8)
        tags = b._scoring_tags(event, "", "ROE", 1)
        assert "critical_error" in tags


class TestScoreDiffTags:
    def _builder(self) -> GameStoryBuilder:
        return GameStoryBuilder.__new__(GameStoryBuilder)

    def test_game_tying(self) -> None:
        b = self._builder()
        assert "game_tying" in b._score_diff_tags(2, 0)

    def test_go_ahead(self) -> None:
        b = self._builder()
        assert "go_ahead" in b._score_diff_tags(0, 1)

    def test_lead_change(self) -> None:
        b = self._builder()
        assert "lead_change" in b._score_diff_tags(1, -1)

    def test_none_returns_empty(self) -> None:
        b = self._builder()
        assert b._score_diff_tags(None, 0) == set()

    def test_no_change(self) -> None:
        b = self._builder()
        assert b._score_diff_tags(1, 2) == set()


class TestWpaTags:
    def _builder(self) -> GameStoryBuilder:
        return GameStoryBuilder.__new__(GameStoryBuilder)

    def test_late_high_wpa_combined(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=8)
        tags = b._wpa_tags(event, 0.3)
        assert "late_high_wpa" in tags
        assert "high_wpa" in tags

    def test_late_only_wpa(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=7)
        tags = b._wpa_tags(event, 0.18)
        assert "late_high_wpa" in tags
        assert "high_wpa" not in tags

    def test_high_wpa_only(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=5)
        tags = b._wpa_tags(event, 0.3)
        assert "high_wpa" in tags
        assert "late_high_wpa" not in tags

    def test_low_wpa(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=3)
        assert b._wpa_tags(event, 0.1) == set()


class TestIsWalkOff:
    def _builder(self) -> GameStoryBuilder:
        return GameStoryBuilder.__new__(GameStoryBuilder)

    def test_walkoff(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=10, inning_half="bottom")
        result = b._is_walk_off(event, "bottom", -1, 1, 2)
        assert result is True

    def test_not_walkoff_top_inning(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=9, inning_half="top")
        result = b._is_walk_off(event, 0, -1, 1, 2)
        assert result is False

    def test_not_walkoff_too_early(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=8, inning_half="bottom")
        result = b._is_walk_off(event, 0, -1, 1, 2)
        assert result is False

    def test_not_walkoff_already_winning(self) -> None:
        b = self._builder()
        event = _MockGameEvent(inning=10, inning_half="bottom")
        result = b._is_walk_off(event, 1, 0, 1, 1)
        assert result is False
