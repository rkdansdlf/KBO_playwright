from unittest.mock import MagicMock, patch

import src.cli.generate_quality_report as module
from src.cli.generate_quality_report import get_daily_metrics, main

_BASE_METRICS = {
    "date": "20260609",
    "status_counts": {"SCHEDULED": 9},
    "detail_integrity": [],
    "new_players": [],
    "relay_integrity": {"ok": True},
    "standings_integrity": {"ok": True, "mismatches": [], "missing_score_games": []},
    "top_performer": None,
    "parity": {"ok": True, "local_count": 0, "oci_count": 0, "diff": 0},
    "total_games": 9,
    "completed_count": 0,
    "auto_remediation": {"status": "no_issues"},
    "pa_formula_integrity": {"ok": True},
    "pa_formula_trend": {"months": [], "direction": "stable"},
    "team_stats_trend": {"months": [], "direction": "stable"},
}

_BASE_GATE = {
    "ok": True,
    "batting": {"ok": True, "mismatches": []},
    "pitching": {"ok": True, "mismatches": []},
    "team_batting": {"ok": True, "checked_players": 0, "mismatches": []},
    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
}


class TestGenerateQualityReport:
    def test_default_run(self):
        with (
            patch("src.cli.generate_quality_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics,
            patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main([])
            assert result == 0

    def test_with_date(self):
        with (
            patch("src.cli.generate_quality_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics,
            patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main(["--date", "20250101"])
            assert result == 0

    def test_notify(self):
        with (
            patch("src.cli.generate_quality_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics,
            patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main(["--notify"])
            assert result == 0


def test_get_daily_metrics_passes_team_stats_snapshot_args(monkeypatch):
    query = MagicMock()
    query.filter.return_value = query
    query.filter_by.return_value = query
    query.group_by.return_value = query
    query.join.return_value = query
    query.all.return_value = []
    query.count.return_value = 0
    query.scalar.return_value = 0
    session = MagicMock()
    session.query.return_value = query

    monkeypatch.setattr(module, "get_relay_integrity_metrics", lambda *_args: {"ok": True})
    monkeypatch.setattr(
        module,
        "validate_standings_integrity",
        lambda *_args: {"ok": True, "mismatches": [], "missing_score_games": []},
    )
    monkeypatch.setattr(module, "get_auto_remediation_summary", lambda *_args: {"status": "no_issues"})
    monkeypatch.setattr(module, "get_pa_formula_integrity", lambda *_args: {"ok": True})
    monkeypatch.setattr(module, "get_pa_formula_trend", lambda *_args, **_kwargs: {"months": []})
    monkeypatch.setattr(module, "get_oci_url", lambda: None)

    def fake_team_stats_trend(_session, gate_result=None):
        assert gate_result == _BASE_GATE
        return {"months": [], "direction": "stable"}

    monkeypatch.setattr(module, "get_team_stats_trend", fake_team_stats_trend)

    metrics = get_daily_metrics(session, "20260609", gate_result=_BASE_GATE)

    assert metrics["team_stats_trend"] == {"months": [], "direction": "stable"}
