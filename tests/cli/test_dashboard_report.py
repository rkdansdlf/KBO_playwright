from __future__ import annotations

import logging
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from src.cli.dashboard_report import (
    AVAILABLE_SECTIONS,
    ViolationContext,
    _append_first_mismatch_line,
    _append_quality_notify_lines,
    _append_quality_violation_lines,
    _date_or_today,
    _format_json,
    _normalize_sections,
    _row_value,
    _r2dict,
)


class TestDateOrToday:
    def test_with_date(self):
        assert _date_or_today("20260624") == "20260624"

    def test_none_returns_today(self):
        result = _date_or_today(None)
        assert len(result) == 8
        assert result.isdigit()


class TestAvailableSections:
    def test_sections_defined(self):
        assert "standings" in AVAILABLE_SECTIONS
        assert "park_factor" in AVAILABLE_SECTIONS
        assert "quality" in AVAILABLE_SECTIONS
        assert "all" in AVAILABLE_SECTIONS


class TestR2Dict:
    def test_basic(self):
        mock_col1 = MagicMock()
        mock_col1.name = "id"
        mock_col2 = MagicMock()
        mock_col2.name = "name"

        class FakeModel:
            __table__ = MagicMock(columns=[mock_col1, mock_col2])

        class FakeObj:
            id = 42
            name = "test"

        result = _r2dict(FakeObj(), FakeModel)
        assert result == {"id": 42, "name": "test"}


class TestRowValue:
    def test_attribute_access(self):
        class Obj:
            name = "test"

        assert _row_value(Obj(), "name") == "test"

    def test_dict_fallback(self):
        assert _row_value({"key": "val"}, "key") == "val"

    def test_default(self):
        assert _row_value({}, "missing", "default") == "default"


class TestNormalizeSections:
    def test_all_expands(self):
        result = _normalize_sections(["all"])
        assert result == AVAILABLE_SECTIONS[:-1]
        assert "all" not in result

    def test_specific_sections(self):
        result = _normalize_sections(["standings", "quality"])
        assert result == ["standings", "quality"]


class TestFormatJson:
    def test_basic(self):
        data = {"key": "value"}
        result = _format_json(data)
        assert '"key"' in result
        assert '"value"' in result


class TestViolationContext:
    def test_dataclass(self):
        ctx = ViolationContext(pa_ok=True, team_bat_ok=False, team_pit_ok=True)
        assert ctx.pa_ok is True
        assert ctx.team_bat_ok is False
        assert ctx.team_pit_ok is True


class TestAppendQualityViolationLines:
    def test_all_ok(self, caplog):
        msg_lines = []
        quality = {"pa_formula_integrity": {"ok": True}}
        gate = {"team_batting": {"ok": True}, "team_pitching": {"ok": True}}
        ctx = ViolationContext(pa_ok=True, team_bat_ok=True, team_pit_ok=True)
        _append_quality_violation_lines(msg_lines, quality, gate, ctx=ctx)
        assert len(msg_lines) == 1
        assert "통합 감사" in msg_lines[0]

    def test_pa_violation(self, caplog):
        msg_lines = []
        quality = {"pa_formula_integrity": {"ok": False, "violation_count": 3}}
        gate = {"team_batting": {"ok": True}, "team_pitching": {"ok": True}}
        ctx = ViolationContext(pa_ok=False, team_bat_ok=True, team_pit_ok=True)
        _append_quality_violation_lines(msg_lines, quality, gate, ctx=ctx)
        assert "PA 3건" in msg_lines[0]

    def test_team_batting_violation(self, caplog):
        msg_lines = []
        quality = {"pa_formula_integrity": {"ok": True}}
        gate = {
            "team_batting": {"ok": False, "mismatches": [{"team_id": "LG"}, {"team_id": "SS"}]},
            "team_pitching": {"ok": True},
        }
        ctx = ViolationContext(pa_ok=True, team_bat_ok=False, team_pit_ok=True)
        _append_quality_violation_lines(msg_lines, quality, gate, ctx=ctx)
        assert "팀타격 2건" in msg_lines[0]

    def test_team_pitching_violation(self, caplog):
        msg_lines = []
        quality = {"pa_formula_integrity": {"ok": True}}
        gate = {
            "team_batting": {"ok": True},
            "team_pitching": {"ok": False, "mismatches": [{"team_id": "KT"}]},
        }
        ctx = ViolationContext(pa_ok=True, team_bat_ok=True, team_pit_ok=False)
        _append_quality_violation_lines(msg_lines, quality, gate, ctx=ctx)
        assert "팀투수 1건" in msg_lines[0]


class TestAppendFirstMismatchLine:
    def test_ok_returns_early(self):
        msg_lines = []
        _append_first_mismatch_line(msg_lines, {}, "team_batting", "팀타격", is_ok=True)
        assert len(msg_lines) == 0

    def test_mismatch_appended(self):
        msg_lines = []
        gate = {"team_batting": {"mismatches": [{"team_id": "LG", "issue": "value_mismatch"}]}}
        _append_first_mismatch_line(msg_lines, gate, "team_batting", "팀타격", is_ok=False)
        assert len(msg_lines) == 1
        assert "팀타격" in msg_lines[0]
        assert "LG" in msg_lines[0]


class TestAppendQualityNotifyLines:
    def test_all_passed(self, caplog):
        caplog.set_level(logging.INFO)
        msg_lines = []
        quality = {
            "completed_count": 10,
            "total_games": 10,
            "quality_gate": {"team_batting": {"ok": True}, "team_pitching": {"ok": True}},
            "pa_formula_integrity": {"ok": True},
        }
        _append_quality_notify_lines(msg_lines, quality)
        assert "전체 통과" in msg_lines[1]

    def test_with_failures(self, caplog):
        caplog.set_level(logging.INFO)
        msg_lines = []
        quality = {
            "completed_count": 8,
            "total_games": 10,
            "quality_gate": {"team_batting": {"ok": False}, "team_pitching": {"ok": True}},
            "pa_formula_integrity": {"ok": False, "violation_count": 2},
        }
        _append_quality_notify_lines(msg_lines, quality)
        assert "❌" in msg_lines[1]
