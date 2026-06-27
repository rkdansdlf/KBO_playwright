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
    _format_freshness_terminal,
    _format_json,
    _format_pa_trend_terminal,
    _format_park_factor_terminal,
    _format_quality_gate_terminal,
    _format_quality_terminal,
    _format_rankings_terminal,
    _format_standings_terminal,
    _format_sync_terminal,
    _format_team_defense_terminal,
    _format_team_gate_terminal,
    _format_unified_audit_terminal,
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


class TestFormatStandingsTerminal:
    def test_with_rows(self, caplog):
        standings = {
            "date": "2026-06-24",
            "rows": [
                {
                    "rank": 1,
                    "team_code": "LG",
                    "wins": 50,
                    "losses": 30,
                    "draws": 0,
                    "win_pct": 0.625,
                    "games_behind": "-",
                    "recent_10_wins": 7,
                    "recent_10_losses": 3,
                    "current_streak": 3,
                    "home_wins": 30,
                    "home_losses": 10,
                    "away_wins": 20,
                    "away_losses": 20,
                    "top_5": True,
                },
            ],
        }
        with caplog.at_level(logging.INFO):
            _format_standings_terminal(standings, 2026)
        assert "KBO 2026년 순위표" in caplog.text
        assert "LG" in caplog.text

    def test_empty_rows(self, caplog):
        standings = {"rows": []}
        with caplog.at_level(logging.INFO):
            _format_standings_terminal(standings, 2026)
        assert "KBO 2026년 순위표" in caplog.text


class TestFormatParkFactorTerminal:
    def test_with_results(self, caplog):
        park_factor = {
            "year": 2026,
            "results": [
                {
                    "stadium": "잠실",
                    "games": 20,
                    "runs_per_game": 4.5,
                    "park_factor": 1.05,
                    "park_factor_label": "높음",
                },
            ],
        }
        with caplog.at_level(logging.INFO):
            _format_park_factor_terminal(park_factor)
        assert "구장별 파크팩터" in caplog.text

    def test_empty_results(self, caplog):
        park_factor = {"year": 2026, "results": []}
        with caplog.at_level(logging.INFO):
            _format_park_factor_terminal(park_factor)


class TestFormatRankingsTerminal:
    def test_with_data(self, caplog):
        rankings = {
            "year": 2026,
            "top5": {
                "batting": [{"player_name": "Kim", "value": 0.350}],
                "ops": [{"player_name": "Lee", "value": 1.100}],
            },
        }
        with caplog.at_level(logging.INFO):
            _format_rankings_terminal(rankings)
        assert "세이버메트릭" in caplog.text


class TestFormatTeamDefenseTerminal:
    def test_with_data(self, caplog):
        defense = {
            "year": 2026,
            "teams": [{"team_code": "LG", "drS": 100, "drP": 50, "drC": 30}],
        }
        with caplog.at_level(logging.INFO):
            _format_team_defense_terminal(defense)
        assert "수비" in caplog.text or "defense" in caplog.text


class TestFormatTeamGateTerminal:
    def test_with_ok_result(self, caplog):
        result = {"ok": True, "mismatches": []}
        with caplog.at_level(logging.INFO):
            _format_team_gate_terminal("팀타격", result)

    def test_with_mismatches(self, caplog):
        result = {"ok": False, "mismatches": [{"team_id": "LG", "issue": "HR mismatch"}]}
        with caplog.at_level(logging.INFO):
            _format_team_gate_terminal("팀타격", result)


class TestFormatQualityGateTerminal:
    def test_all_ok(self, caplog):
        quality = {"team_batting": {"ok": True}, "team_pitching": {"ok": True}}
        with caplog.at_level(logging.INFO):
            _format_quality_gate_terminal(quality)


class TestFormatPaTrendTerminal:
    def test_with_data(self, caplog):
        quality = {"pa_formula_integrity": {"ok": True, "checked_players": 50}}
        with caplog.at_level(logging.INFO):
            _format_pa_trend_terminal(quality)


class TestFormatUnifiedAuditTerminal:
    def test_with_data(self, caplog):
        quality = {"team_batting": {"ok": True, "mismatches": []}, "team_pitching": {"ok": True, "mismatches": []}}
        with caplog.at_level(logging.INFO):
            _format_unified_audit_terminal(quality)


class TestFormatQualityTerminal:
    def test_with_data(self, caplog):
        quality = {"quality_gate": {"team_batting": {"ok": True}, "team_pitching": {"ok": True}}}
        with caplog.at_level(logging.INFO):
            _format_quality_terminal(quality)


class TestFormatFreshnessTerminal:
    def test_with_data(self, caplog):
        freshness = {"games_total": 10, "games_with_stats": 8, "freshness_pct": 80.0}
        with caplog.at_level(logging.INFO):
            _format_freshness_terminal(freshness)


class TestFormatSyncTerminal:
    def test_with_data(self, caplog):
        sync = {"synced": 100, "failed": 0, "tables": ["games", "batting"]}
        with caplog.at_level(logging.INFO):
            _format_sync_terminal(sync)
