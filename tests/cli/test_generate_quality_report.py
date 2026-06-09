from unittest.mock import patch, MagicMock

from src.cli.generate_quality_report import main

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
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf, \
             patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics, \
             patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main([])
            assert result == 0

    def test_with_date(self):
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf, \
             patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics, \
             patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main(["--date", "20250101"])
            assert result == 0

    def test_notify(self):
        with patch("src.cli.generate_quality_report.SessionLocal") as mock_sf, \
             patch("src.cli.generate_quality_report.get_daily_metrics") as mock_metrics, \
             patch("src.cli.generate_quality_report.run_quality_gate") as mock_gate:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_metrics.return_value = dict(_BASE_METRICS)
            mock_gate.return_value = dict(_BASE_GATE)
            result = main(["--notify"])
            assert result == 0
