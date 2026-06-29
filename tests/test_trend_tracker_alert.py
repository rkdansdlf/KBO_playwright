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
        },
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 25,  # +150% increase (threshold is +50%)
            },
            "completed_count": 100,
        },
    }

    with (tmp_path / f"{yesterday}.json").open("w") as f:
        json.dump(report1, f)
    with (tmp_path / f"{today}.json").open("w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 1
        msg = mock_send_alert.call_args[0][0]
        assert "열화 감지" in msg
        assert "recent_missing_count" in msg


def test_detect_degradations_supports_completed_count_when_explicit(tmp_path):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    report1 = {
        "metrics": {
            "date": yesterday,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 100,
        },
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 70,  # -30% drop (threshold is -20%)
        },
    }

    with (tmp_path / f"{yesterday}.json").open("w") as f:
        json.dump(report1, f)
    with (tmp_path / f"{today}.json").open("w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    degradations = tracker.detect_degradations({"metrics.completed_count": -20.0}, days=2)
    assert len(degradations) == 1
    assert degradations[0]["metric"] == "metrics.completed_count"


def test_send_degradation_alert_ignores_completed_count_by_default(tmp_path):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    today = datetime.now().strftime("%Y%m%d")

    report1 = {
        "metrics": {
            "date": yesterday,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 100,
        },
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 10,
            },
            "completed_count": 0,
        },
    }

    with (tmp_path / f"{yesterday}.json").open("w") as f:
        json.dump(report1, f)
    with (tmp_path / f"{today}.json").open("w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 0


def test_load_reports_dedupes_by_metrics_date_with_latest_generated_at(tmp_path):
    today = datetime.now().strftime("%Y%m%d")
    stale_report = {
        "metrics": {
            "date": today,
            "completed_count": 0,
        },
        "generated_at": "2026-06-10T03:11:00+09:00",
    }
    latest_report = {
        "metrics": {
            "date": today,
            "completed_count": 5,
        },
        "generated_at": "2026-06-10T04:00:00+09:00",
    }

    with (tmp_path / "20250101.json").open("w") as f:
        json.dump(stale_report, f)
    with (tmp_path / f"{today}.json").open("w") as f:
        json.dump(latest_report, f)

    tracker = TrendTracker(report_dir=tmp_path)

    reports = tracker.load_reports(days=2)

    assert len(reports) == 1
    assert reports[0]["metrics"]["completed_count"] == 5


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
        },
    }
    report2 = {
        "metrics": {
            "date": today,
            "relay_integrity": {
                "recent_missing_count": 11,  # +10% increase (threshold is +50%)
            },
            "completed_count": 95,  # -5% drop (threshold is -20%)
        },
    }

    with (tmp_path / f"{yesterday}.json").open("w") as f:
        json.dump(report1, f)
    with (tmp_path / f"{today}.json").open("w") as f:
        json.dump(report2, f)

    tracker = TrendTracker(report_dir=tmp_path)

    with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send_alert:
        tracker.send_degradation_alert(days=2)
        assert mock_send_alert.call_count == 0
