from unittest.mock import MagicMock, patch

from src.monitoring.sla_tracker import SlaTracker


def test_send_weekly_sla_report_sends_alert():
    mock_session = MagicMock()
    tracker = SlaTracker(mock_session)

    # Mock compute_weekly_sla to return active SLA data
    mock_weekly_data = [
        {
            "date": "20260525",
            "total": 5,
            "completed": 5,
            "completion_rate": 1.0,
            "pbp_coverage": 1.0,
            "detail_coverage": 1.0,
        },
        {
            "date": "20260526",
            "total": 5,
            "completed": 3,
            "completion_rate": 0.6,
            "pbp_coverage": 0.8,
            "detail_coverage": 0.8,
        },
    ]

    with (
        patch.object(tracker, "compute_weekly_sla", return_value=mock_weekly_data),
        patch("src.monitoring.sla_tracker.send_notification") as mock_send,
    ):
        tracker.send_weekly_sla_report(end_date="20260526")

        assert mock_send.call_count == 1
        title, body = mock_send.call_args.args[:2]
        assert "주간 SLA 리포트" in title
        assert "총 경기: 10 | 완료: 8" in body
        assert "평균 완료율: 80.0%" in body
        assert "낮은 완료율 날짜" in body
        assert "20260526: 60%" in body


def test_send_weekly_sla_report_skips_if_no_games():
    mock_session = MagicMock()
    tracker = SlaTracker(mock_session)

    with (
        patch.object(tracker, "compute_weekly_sla", return_value=[]),
        patch("src.monitoring.sla_tracker.send_notification") as mock_send,
    ):
        tracker.send_weekly_sla_report(end_date="20260526")
        assert mock_send.call_count == 0
