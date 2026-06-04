from unittest.mock import patch

from scripts.verification.audit_fallback_stats import StatAudit


def test_send_remediation_success_alert():
    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        fixed_players = [
            {"name": "홍길동", "diffs": ["hits: 5→2", "at_bats: 10→12"]},
            {"name": "이순신", "diffs": ["wins: 2→1"]},
        ]
        StatAudit.send_remediation_success_alert(
            year=2025,
            series="regular",
            category="BATTING",
            mismatches_count=2,
            fixed_players=fixed_players,
        )
        assert mock_send_alert.call_count == 1
        args, kwargs = mock_send_alert.call_args
        msg = args[0]
        assert "Auto-Remediation 완료" in msg
        assert "홍길동" in msg
        assert "이순신" in msg


def test_send_audit_warning_alert():
    with (
        patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert,
        patch("src.utils.fallback_monitor.FallbackMonitor.save_audit_event") as mock_save_event,
    ):
        mismatches = [
            {"player_id": 1001, "name": "홍길동", "diffs": ["hits: 5→2"]},
        ]
        StatAudit.send_audit_warning_alert(
            year=2025,
            series="regular",
            category="BATTING",
            mismatches=mismatches,
        )
        assert mock_send_alert.call_count == 1
        assert "Stats Mismatch 발견" in mock_send_alert.call_args[0][0]

        assert mock_save_event.call_count == 1
        category, event_type, data = mock_save_event.call_args[0]
        assert category == "BATTING"
        assert event_type == "warning"
        assert data["year"] == 2025
        assert data["mismatches"][0]["name"] == "홍길동"


def test_send_remediation_abort_alert():
    with (
        patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert,
        patch("src.utils.fallback_monitor.FallbackMonitor.save_audit_event") as mock_save_event,
    ):
        StatAudit.send_remediation_abort_alert(
            year=2025,
            series="regular",
            category="BATTING",
            reason="Total mismatches exceeds threshold",
        )
        assert mock_send_alert.call_count == 1
        assert "Auto-Remediation Aborted" in mock_send_alert.call_args[0][0]

        assert mock_save_event.call_count == 1
        category, event_type, data = mock_save_event.call_args[0]
        assert category == "BATTING"
        assert event_type == "abort"
        assert "threshold" in data["reason"]
