"""Unit tests for freshness_gate pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.freshness_gate import (
    _check_scores,
    _check_starting_pitchers,
    _empty_issue_map,
    _has_review_moments,
    _review_moments_have_noise,
    _send_freshness_alert,
)


class TestEmptyIssueMap:
    def test_returns_all_keys(self) -> None:
        result = _empty_issue_map()
        assert len(result) == 14
        for key in (
            "missing_start_time",
            "missing_scores",
            "missing_lineups",
            "missing_inning_scores",
            "missing_events",
            "missing_wpa",
            "missing_starting_pitchers",
            "missing_pitching_stats",
            "missing_pitching_starters",
            "missing_review_wpa",
            "missing_review_moments",
            "review_moment_noise",
            "inning_score_mismatch",
            "past_scheduled_games",
        ):
            assert key in result

    def test_all_values_are_empty_lists(self) -> None:
        result = _empty_issue_map()
        for value in result.values():
            assert value == []


class TestHasReviewMoments:
    def test_valid_json_with_moments(self) -> None:
        text = '{"crucial_moments": [{"description": "home run"}]}'
        assert _has_review_moments(text) is True

    def test_empty_moments_list(self) -> None:
        text = '{"crucial_moments": []}'
        assert _has_review_moments(text) is False

    def test_none_input(self) -> None:
        assert _has_review_moments(None) is False

    def test_empty_string(self) -> None:
        assert _has_review_moments("") is False

    def test_invalid_json(self) -> None:
        assert _has_review_moments("not json") is False

    def test_missing_key(self) -> None:
        text = '{"other_key": "value"}'
        assert _has_review_moments(text) is False

    def test_moments_not_list(self) -> None:
        text = '{"crucial_moments": "not a list"}'
        assert _has_review_moments(text) is False


class TestReviewMomentsHaveNoise:
    def test_none_input(self) -> None:
        assert _review_moments_have_noise(None) is False

    def test_empty_string(self) -> None:
        assert _review_moments_have_noise("") is False

    def test_invalid_json(self) -> None:
        assert _review_moments_have_noise("not json") is False

    def test_no_noise(self) -> None:
        text = '{"crucial_moments": [{"description": "home run"}]}'
        with patch("src.cli.freshness_gate.is_relay_noise_text", return_value=False):
            assert _review_moments_have_noise(text) is False

    def test_has_noise(self) -> None:
        text = '{"crucial_moments": [{"description": "noise text"}]}'
        with patch("src.cli.freshness_gate.is_relay_noise_text", return_value=True):
            assert _review_moments_have_noise(text) is True

    def test_mixed_moments(self) -> None:
        text = '{"crucial_moments": [{"description": "a"}, {"description": "b"}]}'
        with patch("src.cli.freshness_gate.is_relay_noise_text", side_effect=[False, True]):
            assert _review_moments_have_noise(text) is True

    def test_moments_not_list(self) -> None:
        text = '{"crucial_moments": "not list"}'
        assert _review_moments_have_noise(text) is False


class TestCheckScores:
    def test_both_scores_present(self) -> None:
        game = MagicMock(away_score=5, home_score=3)
        issues: dict[str, list[str]] = {"missing_scores": []}
        _check_scores(game, issues)
        assert issues["missing_scores"] == []

    def test_away_score_none(self) -> None:
        game = MagicMock(away_score=None, home_score=3)
        issues: dict[str, list[str]] = {"missing_scores": []}
        _check_scores(game, issues)
        assert len(issues["missing_scores"]) == 1

    def test_home_score_none(self) -> None:
        game = MagicMock(away_score=5, home_score=None)
        issues: dict[str, list[str]] = {"missing_scores": []}
        _check_scores(game, issues)
        assert len(issues["missing_scores"]) == 1

    def test_both_scores_none(self) -> None:
        game = MagicMock(away_score=None, home_score=None)
        issues: dict[str, list[str]] = {"missing_scores": []}
        _check_scores(game, issues)
        assert len(issues["missing_scores"]) == 1


class TestCheckStartingPitchers:
    def test_both_present(self) -> None:
        game = MagicMock(away_pitcher="P1", home_pitcher="P2")
        issues: dict[str, list[str]] = {"missing_starting_pitchers": []}
        _check_starting_pitchers(game, issues)
        assert issues["missing_starting_pitchers"] == []

    def test_away_empty(self) -> None:
        game = MagicMock(away_pitcher="", home_pitcher="P2")
        issues: dict[str, list[str]] = {"missing_starting_pitchers": []}
        _check_starting_pitchers(game, issues)
        assert len(issues["missing_starting_pitchers"]) == 1

    def test_home_none(self) -> None:
        game = MagicMock(away_pitcher="P1", home_pitcher=None)
        issues: dict[str, list[str]] = {"missing_starting_pitchers": []}
        _check_starting_pitchers(game, issues)
        assert len(issues["missing_starting_pitchers"]) == 1

    def test_both_missing(self) -> None:
        game = MagicMock(away_pitcher=None, home_pitcher=None)
        issues: dict[str, list[str]] = {"missing_starting_pitchers": []}
        _check_starting_pitchers(game, issues)
        assert len(issues["missing_starting_pitchers"]) == 1


class TestSendFreshnessAlert:
    def test_single_failure(self) -> None:
        with patch("src.cli.freshness_gate.SlackWebhookClient.send_alert") as mock_send:
            _send_freshness_alert(["missing_scores: 1 game(s) -> G1"])
            mock_send.assert_called_once()
            call_args = mock_send.call_args[0][0]
            assert "KBO Freshness Gate Failed" in call_args
            assert "missing_scores" in call_args

    def test_truncates_at_20(self) -> None:
        failures = [f"issue: 1 game(s) -> G{i}" for i in range(25)]
        with patch("src.cli.freshness_gate.SlackWebhookClient.send_alert") as mock_send:
            _send_freshness_alert(failures)
            call_args = mock_send.call_args[0][0]
            assert "... and 5 more failures" in call_args
