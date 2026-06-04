import json
from datetime import datetime, timedelta
from unittest.mock import patch

from src.monitoring.trend_tracker import TrendTracker


def test_send_degradation_alert_triggered(tmp_path):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    report1 = {
        "metrics": {
            "date": yesterday,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 100,
        }
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 25,  # +150% increase (threshold is +50%)
            },
            "completed_count": 100,
        }
    }

    with open(tmp_path / f"{yesterday}.json", "w") as f:
        json.dump(report1, f)
    with open(tmp_path / f"{today}.json", "w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 1
        msg = mock_send_alert.call_args[0][0]
        assert "열화 감지" in msg
        assert "recent_missing_count" in msg


def test_send_degradation_alert_triggered_negative_threshold(tmp_path):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    report1 = {
        "metrics": {
            "date": yesterday,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 100,
        }
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 70,  # -30% drop (threshold is -20%)
        }
    }

    with open(tmp_path / f"{yesterday}.json", "w") as f:
        json.dump(report1, f)
    with open(tmp_path / f"{today}.json", "w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 1
        msg = mock_send_alert.call_args[0][0]
        assert "completed_count" in msg


def test_send_degradation_alert_quiet_when_healthy(tmp_path):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    report1 = {
        "metrics": {
            "date": yesterday,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 100,
        }
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 11,  # +10% increase (threshold is +50%)
            },
            "completed_count": 95,  # -5% drop (threshold is -20%)
        }
    }

    with open(tmp_path / f"{yesterday}.json", "w") as f:
        json.dump(report1, f)
    with open(tmp_path / f"{today}.json", "w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 0
