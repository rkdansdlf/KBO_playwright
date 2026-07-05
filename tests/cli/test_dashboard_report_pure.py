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
    _format_json,
    _normalize_sections,
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
