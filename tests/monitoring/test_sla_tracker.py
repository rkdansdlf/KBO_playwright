from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from src.monitoring.sla_tracker import SlaTracker
from src.utils.alerting import SlackWebhookClient


class TestSlaTrackerInit:
    def test_init_stores_session(self) -> None:
        session = MagicMock()
        tracker = SlaTracker(session)
        assert tracker.session is session


class TestComputeDailySla:
    def test_no_games_returns_total_zero(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla("20260624")

        assert result == {"date": "20260624", "total": 0}

    def test_date_object_input(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla(date(2026, 6, 24))

        assert result["total"] == 0

    def test_no_completed_games(self) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "SCHEDULED"
        game.game_id = "GAME1"

        session.query.return_value.filter.return_value.all.return_value = [game, game]

        tracker = SlaTracker(session)
        result = tracker.compute_daily_sla("20260624")

        assert result["total"] == 2
        assert result["completed"] == 0
        assert result["pbp_coverage"] == 0
        assert result["detail_coverage"] == 0
        assert result["completion_rate"] == 0

    def test_fully_completed_all_details(self) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "COMPLETED"
        game.game_id = "GAME1"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game]

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
        assert result["pbp_coverage"] == 1.0
        assert result["detail_coverage"] == 1.0
        assert result["completion_rate"] == 1.0

    def test_partial_pbp_coverage(self) -> None:
        session = MagicMock()

        game1 = MagicMock()
        game1.game_status = "COMPLETED"
        game1.game_id = "GAME1"

        game2 = MagicMock()
        game2.game_status = "COMPLETED"
        game2.game_id = "GAME2"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game1, game2]

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

        assert result["completed"] == 2
        assert result["pbp_coverage"] == 0.5

    def test_no_full_detail_coverage(self) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "COMPLETED"
        game.game_id = "GAME1"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game]

        pbp_query = MagicMock()
        pbp_query.filter.return_value.distinct.return_value.count.return_value = 1

        detail_query = MagicMock()
        detail_query.filter.return_value.count.return_value = 0

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

        assert result["detail_coverage"] == 0.0

    def test_mixed_status_completion_rate(self) -> None:
        session = MagicMock()

        game1 = MagicMock()
        game1.game_status = "COMPLETED"
        game1.game_id = "GAME1"

        game2 = MagicMock()
        game2.game_status = "SCHEDULED"
        game2.game_id = "GAME2"

        game3 = MagicMock()
        game3.game_status = "COMPLETED"
        game3.game_id = "GAME3"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game1, game2, game3]

        pbp_query = MagicMock()
        pbp_query.filter.return_value.distinct.return_value.count.return_value = 2

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

        assert result["total"] == 3
        assert result["completed"] == 2
        assert result["completion_rate"] == round(2 / 3, 3)


class TestComputeWeeklySla:
    def test_returns_7_days_by_default(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        results = tracker.compute_weekly_sla("20260624")

        assert len(results) == 7

    def test_custom_days_parameter(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        results = tracker.compute_weekly_sla("20260624", days=3)

        assert len(results) == 3

    def test_dates_count_down_from_end(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)
        results = tracker.compute_weekly_sla("20260624", days=3)

        assert results[0]["date"] == "20260624"
        assert results[1]["date"] == "20260623"
        assert results[2]["date"] == "20260622"


class TestPrintWeeklyReport:
    def test_logs_no_data_when_empty(self, caplog: pytest.LogCaptureFixture) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)

        with caplog.at_level(logging.INFO):
            tracker.print_weekly_report("20260624")

        assert "No data for week ending" in caplog.text

    def test_logs_report_with_games(self, caplog: pytest.LogCaptureFixture) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "COMPLETED"
        game.game_id = "GAME1"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game]

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

        with caplog.at_level(logging.INFO):
            tracker.print_weekly_report("20260624")

        assert "SLA Report" in caplog.text
        assert "TOTAL" in caplog.text


class TestSendWeeklySlaReport:
    def test_skips_when_no_active_days(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.all.return_value = []

        tracker = SlaTracker(session)

        with patch.object(SlackWebhookClient, "send_alert") as mock_send:
            tracker.send_weekly_sla_report("20260624")

        mock_send.assert_not_called()

    def test_send_report_no_low_days(self) -> None:
        session = MagicMock()

        game = MagicMock()
        game.game_status = "COMPLETED"
        game.game_id = "GAME1"

        games_query = MagicMock()
        games_query.filter.return_value.all.return_value = [game]

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

        with patch.object(SlackWebhookClient, "send_alert") as mock_send:
            tracker.send_weekly_sla_report("20260624")

            assert mock_send.called
            msg = mock_send.call_args[0][0]
            assert "주간 SLA 리포트" in msg
            assert "낮은 완료율" not in msg

    def test_send_report_with_low_days(self) -> None:
        session = MagicMock()

        completed_game = MagicMock()
        completed_game.game_status = "COMPLETED"
        completed_game.game_id = "GAME1"

        scheduled_game = MagicMock()
        scheduled_game.game_status = "SCHEDULED"
        scheduled_game.game_id = "GAME2"

        day_results = [
            {
                "total": 1,
                "completed": 1,
                "completion_rate": 1.0,
                "pbp_coverage": 1.0,
                "detail_coverage": 1.0,
                "date": "20260624",
            },
            {
                "total": 1,
                "completed": 0,
                "completion_rate": 0.0,
                "pbp_coverage": 0.0,
                "detail_coverage": 0.0,
                "date": "20260623",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260622",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260621",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260620",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260619",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260618",
            },
        ]

        tracker = SlaTracker(session)

        with patch.object(SlaTracker, "compute_weekly_sla", return_value=day_results):
            with patch.object(SlackWebhookClient, "send_alert") as mock_send:
                tracker.send_weekly_sla_report("20260624")

        assert mock_send.called
        msg = mock_send.call_args[0][0]
        assert "낮은 완료율" in msg

    def test_default_end_date_uses_utc_now(self) -> None:
        session = MagicMock()
        tracker = SlaTracker(session)

        day_results = [
            {
                "total": 1,
                "completed": 1,
                "completion_rate": 1.0,
                "pbp_coverage": 1.0,
                "detail_coverage": 1.0,
                "date": "20260624",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260623",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260622",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260621",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260620",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260619",
            },
            {
                "total": 0,
                "completed": 0,
                "completion_rate": 0,
                "pbp_coverage": 0,
                "detail_coverage": 0,
                "date": "20260618",
            },
        ]

        with patch.object(SlaTracker, "compute_weekly_sla", return_value=day_results):
            with patch.object(SlackWebhookClient, "send_alert"):
                with patch("src.monitoring.sla_tracker.datetime") as mock_dt:
                    mock_now = datetime(2026, 6, 24, 15, 0, 0, tzinfo=UTC)
                    mock_dt.now.return_value = mock_now

                    tracker.send_weekly_sla_report()

                    mock_dt.now.assert_called_once()
