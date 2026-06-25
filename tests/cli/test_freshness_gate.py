import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli.freshness_gate import (
    _check_past_scheduled_games,
    _check_scores,
    _evaluate_issues,
    _format_freshness_output,
    main,
)


class TestFreshnessGate:
    def test_default_run(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main([])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--date", "20250101"])
            assert result == 0

    def test_with_json(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--json"])
            assert result == 0

    def test_with_max_hours(self):
        with patch("src.cli.freshness_gate.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.order_by.return_value.all.return_value = []
            result = main(["--max-hours", "48"])
            assert result == 0

    def test_failure_exit_code(self):
        with (
            patch("src.cli.freshness_gate.SessionLocal") as mock_sf,
            patch("src.cli.freshness_gate.collect_freshness_issues") as mock_collect,
            patch("src.cli.freshness_gate.evaluate_freshness_gate") as mock_evaluate,
        ):
            mock_sf.return_value.__enter__.return_value = MagicMock()
            mock_collect.return_value = {"missing_events": ["20250101LGKT"]}
            mock_evaluate.return_value = ["missing_events: 1 game(s) -> 20250101LGKT"]

            result = main([])

            assert result == 1

    def test_json_output_contains_issue_payload(self, caplog):
        with (
            patch("src.cli.freshness_gate.SessionLocal") as mock_sf,
            patch("src.cli.freshness_gate.collect_freshness_issues") as mock_collect,
            patch("src.cli.freshness_gate.evaluate_freshness_gate") as mock_evaluate,
            caplog.at_level(logging.INFO, logger="src.cli.freshness_gate"),
        ):
            mock_sf.return_value.__enter__.return_value = MagicMock()
            mock_collect.return_value = {"missing_events": ["20250101LGKT"]}
            mock_evaluate.return_value = ["missing_events: 1 game(s) -> 20250101LGKT"]

            result = main(["--json"])

            assert result == 1
            payload = json.loads(caplog.records[-1].message)
            assert payload == {"ok": False, "issues": {"missing_events": ["20250101LGKT"]}}

    def test_past_scheduled_games_are_reported(self):
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.filter.return_value = mock_query
        mock_query.order_by.return_value = mock_query
        mock_query.all.return_value = [SimpleNamespace(game_id="20250101LGKT0")]
        mock_session.query.return_value = mock_query
        issues = {"past_scheduled_games": []}

        _check_past_scheduled_games(
            mock_session,
            target_date="20250101",
            days=None,
            max_hours=None,
            issues=issues,
        )

        assert issues["past_scheduled_games"] == ["20250101LGKT0"]

    def test_completed_games_with_missing_scores_are_reported(self):
        issues = {"missing_scores": []}

        _check_scores(SimpleNamespace(game_id="20250101LGKT0", away_score=None, home_score=3), issues)

        assert issues["missing_scores"] == ["20250101LGKT0"]

    def test_both_scores_missing(self):
        issues = {"missing_scores": []}
        _check_scores(SimpleNamespace(game_id="G1", away_score=None, home_score=None), issues)
        assert issues["missing_scores"] == ["G1"]

    def test_no_scores_missing(self):
        issues = {"missing_scores": []}
        _check_scores(SimpleNamespace(game_id="G1", away_score=3, home_score=2), issues)
        assert issues["missing_scores"] == []

    def test_away_score_missing_only(self):
        issues = {"missing_scores": []}
        _check_scores(SimpleNamespace(game_id="G1", away_score=None, home_score=2), issues)
        assert issues["missing_scores"] == ["G1"]


class TestEvaluateIssues:
    def test_no_issues_returns_empty(self):
        result = _evaluate_issues({"past_scheduled_games": [], "missing_scores": []})
        assert result == []

    def test_with_issues_returns_list(self):
        result = _evaluate_issues({"past_scheduled_games": ["G1"], "missing_scores": []})
        assert len(result) > 0
        assert "G1" in result[0]


class TestFormatFreshnessOutput:
    def test_format_with_issues(self):
        result = _format_freshness_output(False, ["issue1", "issue2"], {})
        assert "issue1" in result

    def test_format_ok(self):
        result = _format_freshness_output(True, [], {})
        assert result == "PASS" or "ok" in result.lower()
