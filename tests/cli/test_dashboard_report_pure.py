"""Tests for dashboard_report pure functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.cli.dashboard_report import (
    ViolationContext,
    _append_first_mismatch_line,
    _append_quality_notify_lines,
    _append_quality_violation_lines,
    _date_or_today,
    _emit_dashboard,
    _format_freshness_terminal,
    _format_json,
    _format_park_factor_terminal,
    _format_pa_trend_terminal,
    _format_quality_terminal,
    _format_rankings_terminal,
    _format_standings_terminal,
    _format_sync_terminal,
    _format_team_defense_terminal,
    _format_team_gate_terminal,
    _format_terminal,
    _format_unified_audit_terminal,
    _normalize_sections,
    _parse_args,
    _row_value,
    _r2dict,
    _send_dashboard_notification,
)


class TestR2Dict:
    def test_converts_orm_like_object(self) -> None:
        class MockColumn:
            def __init__(self, name: str) -> None:
                self.name = name

        class MockTable:
            columns = [MockColumn("id"), MockColumn("name"), MockColumn("value")]

        class MockModel:
            __table__ = MockTable()

        obj = MagicMock()
        obj.id = 1
        obj.name = "test"
        obj.value = 42

        result = _r2dict(obj, MockModel)
        assert result == {"id": 1, "name": "test", "value": 42}


class TestDateOrToday:
    def test_returns_today_when_none(self) -> None:
        result = _date_or_today(None)
        assert len(result) == 8
        assert result.isdigit()

    def test_returns_today_when_empty(self) -> None:
        result = _date_or_today("")
        assert len(result) == 8
        assert result.isdigit()

    def test_returns_input_when_valid(self) -> None:
        result = _date_or_today("20250101")
        assert result == "20250101"


class TestRowValue:
    def test_attribute_access(self) -> None:
        class Row:
            name = "test"
            value = 42

        row = Row()
        assert _row_value(row, "name") == "test"
        assert _row_value(row, "value") == 42

    def test_dict_access(self) -> None:
        row = {"name": "test", "value": 42}
        assert _row_value(row, "name") == "test"
        assert _row_value(row, "value") == 42

    def test_default_for_missing_attr(self) -> None:
        class Row:
            name = "test"

        row = Row()
        assert _row_value(row, "missing", "default") == "default"

    def test_default_for_missing_dict_key(self) -> None:
        row = {"name": "test"}
        assert _row_value(row, "missing", "default") == "default"

    def test_none_default(self) -> None:
        class Row:
            pass

        row = Row()
        assert _row_value(row, "missing") is None


class TestDashboardFormatting:
    def test_normalize_sections_expands_all(self) -> None:
        assert _normalize_sections(["all"]) == [
            "standings",
            "park_factor",
            "rankings",
            "team_defense",
            "quality",
            "freshness",
            "sync",
        ]
        assert _normalize_sections(["quality", "sync"]) == ["quality", "sync"]

    def test_format_json_preserves_korean_and_default_str(self) -> None:
        rendered = _format_json({"team": "두산", "value": object()})

        assert "두산" in rendered
        assert "<object object" in rendered

    def test_emit_dashboard_json_logs_json(self) -> None:
        with patch("src.cli.dashboard_report.logger") as mock_logger:
            _emit_dashboard({"quality": {"ok": True}}, ["quality"], "json")

        mock_logger.info.assert_called_once()
        assert "quality" in mock_logger.info.call_args.args[0]

    def test_emit_dashboard_terminal_delegates_formatter(self) -> None:
        with patch("src.cli.dashboard_report._format_terminal") as mock_format:
            _emit_dashboard({"sync": {"status": "ok"}}, ["sync"], "terminal")

        mock_format.assert_called_once_with({"sync": {"status": "ok"}}, ["sync"])


class TestDashboardTerminalFormatters:
    def test_parse_args(self, monkeypatch) -> None:
        monkeypatch.setattr(
            "sys.argv",
            [
                "dashboard_report",
                "--date",
                "20260402",
                "--year",
                "2026",
                "--sections",
                "quality",
                "sync",
                "--format",
                "json",
                "--notify",
            ],
        )

        args = _parse_args()

        assert args.date == "20260402"
        assert args.year == 2026
        assert args.sections == ["quality", "sync"]
        assert args.format == "json"
        assert args.notify is True

    def test_standings_terminal_logs_rows(self) -> None:
        with patch("src.cli.dashboard_report.logger") as mock_logger:
            _format_standings_terminal(
                {
                    "date": "20260402",
                    "rows": [
                        {
                            "rank": 1,
                            "team_code": "LG",
                            "wins": 10,
                            "losses": 2,
                            "draws": 1,
                            "win_pct": 0.833,
                            "top_5": True,
                            "current_streak": -2,
                        },
                    ],
                },
                2026,
            )
        rendered = "\n".join(str(call.args) for call in mock_logger.info.call_args_list)
        assert "순위표" in rendered
        assert "LG" in rendered

    def test_park_factor_rankings_and_team_defense_terminal(self) -> None:
        with patch("src.cli.dashboard_report.logger") as mock_logger:
            _format_park_factor_terminal(
                {
                    "year": 2026,
                    "results": [
                        {
                            "stadium": "잠실",
                            "games": 10,
                            "runs_per_game": 8.5,
                            "park_factor": 1.1,
                            "park_factor_label": "타자",
                        }
                    ],
                },
            )
            _format_rankings_terminal({"year": 2026, "top5": {"WAR": [{"player_name": "Kim", "value": 5.5}]}})
            _format_team_defense_terminal(
                {
                    "year": 2026,
                    "fielding": [{"team_code": "LG", "fielding_pct": 0.99, "errors": 1}],
                    "baserunning": [
                        {"team_code": "LG", "stolen_bases": 10, "caught_stealing": 2, "sb_success_rate": 0.833}
                    ],
                },
            )
        rendered = "\n".join(str(call.args) for call in mock_logger.info.call_args_list)
        assert "잠실" in rendered
        assert "Kim" in rendered
        assert "LG" in rendered

    def test_team_gate_and_quality_terminal(self) -> None:
        quality = {
            "date": "20260402",
            "completed_count": 2,
            "total_games": 3,
            "relay_integrity": {"recent_missing_count": 1},
            "standings_integrity": {"ok": False},
            "pa_formula_integrity": {"ok": False, "violation_count": 2},
            "quality_gate": {
                "team_batting": {
                    "ok": False,
                    "checked_players": 10,
                    "mismatches": [{"team_id": "LG", "issue": "hits", "diffs": ["H", "R"]}],
                },
                "team_pitching": {"ok": True, "checked_players": 10, "mismatches": []},
            },
            "pa_formula_trend": {
                "direction": "improving",
                "months": [{"month": "2026-04", "violation_count": 0, "total_checked": 10}],
            },
        }
        with patch("src.cli.dashboard_report.logger") as mock_logger:
            _format_team_gate_terminal("팀 타격", quality["quality_gate"]["team_batting"])
            _format_quality_terminal(quality)
        info_text = "\n".join(str(call.args) for call in mock_logger.info.call_args_list)
        error_text = "\n".join(str(call.args) for call in mock_logger.error.call_args_list)
        assert "팀 타격" in info_text
        assert "Quality Report" in info_text
        assert "PA 공식" in error_text

    def test_pa_trend_unified_freshness_sync_and_dispatch(self) -> None:
        with patch("src.cli.dashboard_report.logger") as mock_logger:
            _format_pa_trend_terminal(
                {
                    "pa_formula_trend": {
                        "direction": "worsening",
                        "months": [{"month": "2026-04", "violation_count": 2, "total_checked": 10}],
                    }
                },
            )
            _format_unified_audit_terminal(
                {
                    "pa_formula_integrity": {"ok": True},
                    "quality_gate": {"team_batting": {"ok": True}, "team_pitching": {"ok": True}},
                },
            )
            _format_freshness_terminal({"date": "20260402", "total_issues": 1, "issues": {"G1": ["missing"]}})
            _format_sync_terminal({"status": "failed", "reason": "diff"})
            _format_terminal({"sync": {"status": "ok", "ok_count": 1, "table_count": 1}}, ["sync"])
        rendered = "\n".join(
            str(call.args) for call in mock_logger.info.call_args_list + mock_logger.warning.call_args_list
        )
        assert "PA 추세" in rendered
        assert "전체 통과" in rendered
        assert "Freshness" in rendered
        assert "diff" in rendered


class TestDashboardNotificationHelpers:
    def test_append_quality_notify_lines_all_ok(self) -> None:
        lines: list[str] = []

        _append_quality_notify_lines(
            lines,
            {
                "completed_count": 5,
                "total_games": 5,
                "pa_formula_integrity": {"ok": True},
                "quality_gate": {"team_batting": {"ok": True}, "team_pitching": {"ok": True}},
            },
        )

        assert lines == ["완료: 5/5", "통합 감사: ✅ 전체 통과"]

    def test_append_quality_violation_lines_with_first_mismatch(self) -> None:
        lines: list[str] = []
        gate = {
            "team_batting": {"mismatches": [{"team_id": "LG", "issue": "runs mismatch"}]},
            "team_pitching": {"mismatches": [{"team_id": "DB", "issue": "ip mismatch"}]},
        }

        _append_quality_violation_lines(
            lines,
            {"pa_formula_integrity": {"violation_count": 3}},
            gate,
            ctx=ViolationContext(pa_ok=False, team_bat_ok=False, team_pit_ok=True),
        )

        assert lines == ["통합 감사: ❌ (PA 3건, 팀타격 1건)", "  - 팀타격 [LG]: runs mismatch"]

    def test_append_first_mismatch_line_skips_when_ok_or_missing(self) -> None:
        lines: list[str] = []

        _append_first_mismatch_line(lines, {}, "team_batting", "팀타격", is_ok=True)
        _append_first_mismatch_line(lines, {}, "team_batting", "팀타격", is_ok=False)

        assert lines == []

    def test_send_dashboard_notification_builds_message(self) -> None:
        with patch("src.utils.alerting.SlackWebhookClient.send_alert") as mock_send:
            _send_dashboard_notification(
                {
                    "standings": {"rows": [{"team_code": "LG"}, {"team_code": "DB"}]},
                    "quality": {"completed_count": 2, "total_games": 3},
                },
                "20260402",
            )

        message = mock_send.call_args.args[0]
        assert "KBO Dashboard Report (20260402)" in message
        assert "순위: 2팀" in message
        assert "완료: 2/3" in message
