"""Unit tests for freshness_gate pure functions."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.cli.freshness_gate import (
    _apply_freshness_date_filter,
    _check_events_wpa,
    _check_freshness_game,
    _check_inning_scores,
    _check_lineups,
    _check_metadata_start_time,
    _check_pitching_stats,
    _check_review_summary,
    _check_scores,
    _check_starting_pitchers,
    _empty_issue_map,
    _has_review_moments,
    _review_moments_have_noise,
    _send_freshness_alert,
)
from src.constants import KST


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


class TestCheckMetadataStartTime:
    def test_metadata_exists_with_start_time(self) -> None:
        session = MagicMock()
        session.query().filter().one_or_none.return_value = MagicMock(start_time="2026-07-05 14:00:00")
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_start_time": []}
        _check_metadata_start_time(session, game, issues)
        assert issues["missing_start_time"] == []

    def test_metadata_is_none(self) -> None:
        session = MagicMock()
        session.query().filter().one_or_none.return_value = None
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_start_time": []}
        _check_metadata_start_time(session, game, issues)
        assert issues["missing_start_time"] == ["G1"]

    def test_start_time_is_none(self) -> None:
        session = MagicMock()
        session.query().filter().one_or_none.return_value = MagicMock(start_time=None)
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_start_time": []}
        _check_metadata_start_time(session, game, issues)
        assert issues["missing_start_time"] == ["G1"]


class TestCheckLineups:
    def test_lineups_exist(self) -> None:
        session = MagicMock()
        session.query().filter().count.return_value = 5
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_lineups": []}
        _check_lineups(session, game, issues)
        assert issues["missing_lineups"] == []

    def test_no_lineups(self) -> None:
        session = MagicMock()
        session.query().filter().count.return_value = 0
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_lineups": []}
        _check_lineups(session, game, issues)
        assert issues["missing_lineups"] == ["G1"]


class TestCheckInningScores:
    def test_no_inning_rows(self) -> None:
        session = MagicMock()
        session.query().filter().all.return_value = []
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_inning_scores": [], "inning_score_mismatch": []}
        _check_inning_scores(session, game, issues)
        assert issues["missing_inning_scores"] == ["G1"]

    def test_scores_match(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", runs=2),
            MagicMock(team_side="away", runs=1),
            MagicMock(team_side="home", runs=0),
            MagicMock(team_side="home", runs=3),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1", away_score=3, home_score=3)
        issues: dict[str, list[str]] = {"missing_inning_scores": [], "inning_score_mismatch": []}
        _check_inning_scores(session, game, issues)
        assert issues["inning_score_mismatch"] == []

    def test_away_mismatch(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", runs=2),
            MagicMock(team_side="home", runs=3),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1", away_score=5, home_score=3)
        issues: dict[str, list[str]] = {"missing_inning_scores": [], "inning_score_mismatch": []}
        _check_inning_scores(session, game, issues)
        assert issues["inning_score_mismatch"] == ["G1"]

    def test_home_mismatch(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", runs=2),
            MagicMock(team_side="home", runs=1),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1", away_score=2, home_score=5)
        issues: dict[str, list[str]] = {"missing_inning_scores": [], "inning_score_mismatch": []}
        _check_inning_scores(session, game, issues)
        assert issues["inning_score_mismatch"] == ["G1"]


class TestCheckEventsWpa:
    def test_no_events(self) -> None:
        session = MagicMock()
        session.query().filter().count.return_value = 0
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_events": [], "missing_wpa": []}
        _check_events_wpa(session, game, issues)
        assert issues["missing_events"] == ["G1"]

    def test_events_no_wpa(self) -> None:
        session = MagicMock()
        session.query().filter().count.return_value = 5
        session.query().filter().first.return_value = None
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_events": [], "missing_wpa": []}
        _check_events_wpa(session, game, issues)
        assert issues["missing_wpa"] == ["G1"]

    def test_events_with_wpa(self) -> None:
        session = MagicMock()
        session.query().filter().count.return_value = 5
        session.query().filter().first.return_value = MagicMock(id=1)
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_events": [], "missing_wpa": []}
        _check_events_wpa(session, game, issues)
        assert issues["missing_events"] == []
        assert issues["missing_wpa"] == []


class TestCheckPitchingStats:
    def test_no_pitching_rows(self) -> None:
        session = MagicMock()
        session.query().filter().all.return_value = []
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_pitching_stats": [], "missing_pitching_starters": []}
        _check_pitching_stats(session, game, issues)
        assert issues["missing_pitching_stats"] == ["G1"]

    def test_both_starters_present(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", is_starting=True),
            MagicMock(team_side="away", is_starting=False),
            MagicMock(team_side="home", is_starting=True),
            MagicMock(team_side="home", is_starting=False),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_pitching_stats": [], "missing_pitching_starters": []}
        _check_pitching_stats(session, game, issues)
        assert issues["missing_pitching_starters"] == []

    def test_missing_away_starter(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", is_starting=False),
            MagicMock(team_side="home", is_starting=True),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_pitching_stats": [], "missing_pitching_starters": []}
        _check_pitching_stats(session, game, issues)
        assert issues["missing_pitching_starters"] == ["G1"]

    def test_missing_home_starter(self) -> None:
        session = MagicMock()
        rows = [
            MagicMock(team_side="away", is_starting=True),
            MagicMock(team_side="home", is_starting=False),
        ]
        session.query().filter().all.return_value = rows
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {"missing_pitching_stats": [], "missing_pitching_starters": []}
        _check_pitching_stats(session, game, issues)
        assert issues["missing_pitching_starters"] == ["G1"]


class TestCheckReviewSummary:
    def test_no_review(self) -> None:
        session = MagicMock()
        session.query().filter().first.return_value = None
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {
            "missing_review_wpa": [],
            "missing_review_moments": [],
            "review_moment_noise": [],
        }
        _check_review_summary(session, game, issues)
        assert issues["missing_review_wpa"] == ["G1"]

    def test_review_no_moments(self) -> None:
        session = MagicMock()
        session.query().filter().first.return_value = MagicMock(detail_text='{"crucial_moments": []}')
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {
            "missing_review_wpa": [],
            "missing_review_moments": [],
            "review_moment_noise": [],
        }
        _check_review_summary(session, game, issues)
        assert issues["missing_review_moments"] == ["G1"]

    def test_review_moments_have_noise(self) -> None:
        session = MagicMock()
        session.query().filter().first.return_value = MagicMock(
            detail_text='{"crucial_moments": [{"description": "noise"}]}'
        )
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {
            "missing_review_wpa": [],
            "missing_review_moments": [],
            "review_moment_noise": [],
        }
        with patch("src.cli.freshness_gate.is_relay_noise_text", return_value=True):
            _check_review_summary(session, game, issues)
        assert issues["review_moment_noise"] == ["G1"]

    def test_review_clean(self) -> None:
        session = MagicMock()
        session.query().filter().first.return_value = MagicMock(
            detail_text='{"crucial_moments": [{"description": "clean"}]}'
        )
        game = MagicMock(game_id="G1")
        issues: dict[str, list[str]] = {
            "missing_review_wpa": [],
            "missing_review_moments": [],
            "review_moment_noise": [],
        }
        with patch("src.cli.freshness_gate.is_relay_noise_text", return_value=False):
            _check_review_summary(session, game, issues)
        assert issues["missing_review_wpa"] == []
        assert issues["missing_review_moments"] == []
        assert issues["review_moment_noise"] == []


class TestCheckFreshnessGame:
    def test_calls_all_sub_checkers(self) -> None:
        session = MagicMock()
        game = MagicMock(game_id="G1", away_score=3, home_score=3, away_pitcher="P1", home_pitcher="P2")
        issues: dict[str, list[str]] = {
            "missing_start_time": [],
            "missing_scores": [],
            "missing_lineups": [],
            "missing_inning_scores": [],
            "missing_events": [],
            "missing_wpa": [],
            "missing_starting_pitchers": [],
            "missing_pitching_stats": [],
            "missing_pitching_starters": [],
            "missing_review_wpa": [],
            "missing_review_moments": [],
            "review_moment_noise": [],
            "inning_score_mismatch": [],
        }
        session.query().filter().count.return_value = 1
        session.query().filter().one_or_none.return_value = MagicMock(start_time="14:00")
        session.query().filter().all.return_value = [
            MagicMock(team_side="away", runs=2),
            MagicMock(team_side="away", runs=1),
            MagicMock(team_side="home", runs=1),
            MagicMock(team_side="home", runs=2),
        ]
        session.query().filter().first.return_value = MagicMock(
            detail_text='{"crucial_moments": [{"description": "play"}]}'
        )
        with patch("src.cli.freshness_gate.is_relay_noise_text", return_value=False):
            _check_freshness_game(session, game, issues)
        assert issues["missing_start_time"] == []
        assert issues["missing_scores"] == []
        assert issues["missing_lineups"] == []
        assert issues["missing_inning_scores"] == []
        assert issues["missing_events"] == []
        assert issues["missing_starting_pitchers"] == []
        assert issues["missing_pitching_stats"] == []
        assert issues["missing_review_wpa"] == []


class TestApplyFreshnessDateFilter:
    def test_days_filter(self) -> None:
        query = MagicMock()
        result = _apply_freshness_date_filter(query, target_date=None, days=7, max_hours=None)
        assert result == query.filter.return_value
        query.filter.assert_called_once()

    def test_no_filter(self) -> None:
        query = MagicMock()
        result = _apply_freshness_date_filter(query, target_date=None, days=None, max_hours=None)
        assert result == query
