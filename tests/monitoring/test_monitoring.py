"""Tests for src.monitoring modules."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.monitoring.crawl_gate import CrawlGate
from src.monitoring.sla_tracker import SlaTracker


class TestSlaTracker:
    def test_compute_daily_sla_no_games(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla("20260624")

        assert result["total"] == 0
        assert result["date"] == "20260624"

    def test_compute_daily_sla_with_completed_games(self) -> None:
        session = MagicMock()

        game1 = MagicMock()
        game1.game_status = "COMPLETED"
        game1.game_id = "20260624LGSS0"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game1]

        pbp_query = MagicMock()
        pbp_query.filter.return_value.distinct.return_value.count.return_value = 1

        detail_query = MagicMock()
        detail_query.filter.return_value.count.return_value = 1

        call_count = [0]

        def query_side_effect(model):
            call_count[0] += 1
            if call_count[0] == 1:
                return games_query
            if call_count[0] == 2:
                return pbp_query
            return detail_query

        session.query.side_effect = query_side_effect

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla("20260624")

        assert result["total"] == 1
        assert result["completed"] == 1

    def test_compute_daily_sla_all_scheduled(self) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "SCHEDULED"
        game.game_id = "20260624LGSS0"

        session.query.return_value.filter.return_value.all.return_value = [game]

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla("20260624")

        assert result["total"] == 1
        assert result["completed"] == 0
        assert result["completion_rate"] == 0

    def test_compute_daily_sla_date_object(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla(date(2026, 6, 24))

        assert result["total"] == 0


class TestCrawlGate:
    def test_check_freshness_no_issues(self) -> None:
        session = MagicMock()
        gate = CrawlGate(session, enforce=False)

        with patch("src.cli.freshness_gate.collect_freshness_issues", return_value={}):
            result = gate.check_freshness("20260624")

        assert result is True
        assert len(gate.issues) == 0

    def test_check_freshness_with_issues(self) -> None:
        session = MagicMock()
        gate = CrawlGate(session, enforce=False)

        issues = {
            "20260624LGSS0": ["missing_pbp", "missing_detail"],
        }

        with patch("src.cli.freshness_gate.collect_freshness_issues", return_value=issues):
            result = gate.check_freshness("20260624")

        assert result is False
        assert len(gate.issues) == 2

    def test_check_game_completion_rate_no_games(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        gate = CrawlGate(session, enforce=False)
        result = gate.check_game_completion_rate("20260624")

        assert result is True

    def test_check_game_completion_rate_high_rate(self) -> None:
        session = MagicMock()

        games = []
        for _i in range(10):
            game = MagicMock()
            game.game_status = "COMPLETED"
            games.append(game)

        session.query.return_value.filter.return_value.all.return_value = games

        gate = CrawlGate(session, enforce=False)
        result = gate.check_game_completion_rate("20260624")

        assert result is True

    def test_check_game_completion_rate_low_rate(self) -> None:
        session = MagicMock()

        games = []
        for _i in range(10):
            game = MagicMock()
            game.game_status = "SCHEDULED"
            games.append(game)

        session.query.return_value.filter.return_value.all.return_value = games

        gate = CrawlGate(session, enforce=False)
        result = gate.check_game_completion_rate("20260624")

        assert result is False
