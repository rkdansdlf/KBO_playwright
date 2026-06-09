import sys
from unittest.mock import MagicMock, patch

# Mock missing modules before importing
sys.modules["scripts.legacy.maintenance.quality_gate"] = MagicMock()
sys.modules["scripts.legacy.maintenance.quality_gate"].run_quality_gate = MagicMock(return_value={"ok": True, "failures": []})

from scripts.verification.run_full_integrity_suite import format_report_md, get_latest_game_date


class TestGetLatestGameDate:
    @patch("scripts.verification.run_full_integrity_suite.SessionLocal")
    def test_no_games(self, mock_session_local):
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_session.query.return_value.order_by.return_value.first.return_value = None
        assert get_latest_game_date() is None

    @patch("scripts.verification.run_full_integrity_suite.SessionLocal")
    def test_with_game(self, mock_session_local):
        from datetime import date

        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_game = MagicMock()
        mock_game.game_date = date(2025, 4, 1)
        mock_session.query.return_value.order_by.return_value.first.return_value = mock_game
        assert get_latest_game_date() == date(2025, 4, 1)


class TestFormatReportMd:
    def test_all_pass(self):
        report = format_report_md(
            timestamp=MagicMock(strftime=lambda x: "2025-04-01 00:00:00"),
            orphan_results={"ok": True, "checks": []},
            logic_violations=[],
            qgate_results={"ok": True, "failures": []},
            standings_results=[],
            strict_mode=False,
        )
        assert "PASS" in report

    def test_with_failures(self):
        report = format_report_md(
            timestamp=MagicMock(strftime=lambda x: "2025-04-01 00:00:00"),
            orphan_results={"ok": False, "checks": [{"status": "FAIL", "name": "test", "row_count": 5, "distinct_count": 2, "severity": "HIGH", "samples": []}], "database": "local"},
            logic_violations=[{"game_id": "G1", "game_date": "2025-04-01", "reason": "test"}],
            qgate_results={"ok": False, "failures": ["local past_missing_runs=5 exceeds baseline 0"]},
            standings_results=[{"checked_date": "2025-04-01", "ok": True}],
            strict_mode=True,
        )
        assert "FAIL" in report
