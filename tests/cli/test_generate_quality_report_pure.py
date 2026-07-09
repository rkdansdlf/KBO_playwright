"""Unit tests for generate_quality_report pure functions."""

from __future__ import annotations

import pytest

from src.cli.generate_quality_report import (
    _add_unique_category,
    _append_auto_remediation_section,
    _append_detail_integrity_section,
    _append_new_players_section,
    _append_pa_formula_section,
    _append_pa_formula_trend_section,
    _append_player_stats_section,
    _append_relay_integrity_section,
    _append_standings_integrity_section,
    _append_team_stats_section,
    _append_team_stats_trend_section,
    _append_top_performer_section,
    _audit_category_from_filename,
    _auto_remediation_status,
    _category_counts,
    _empty_auto_remediation_summary,
    _record_auto_remediation_abort,
    _record_auto_remediation_fixed,
    _record_auto_remediation_warning,
    format_telegram_report,
    get_team_stats_integrity,
    _has_report_issues,
)


class TestAuditCategoryFromFilename:
    def test_batting(self) -> None:
        assert _audit_category_from_filename("20260531_10001_batting.json") == "BATTING"

    def test_pitching(self) -> None:
        assert _audit_category_from_filename("warning_pitching.json") == "PITCHING"

    def test_single_word(self) -> None:
        assert _audit_category_from_filename("file.json") == "FILE"

    def test_uppercase(self) -> None:
        assert _audit_category_from_filename("test.json") == "TEST"


class TestAutoRemediationStatus:
    def test_aborted(self) -> None:
        assert _auto_remediation_status(has_abort=True, has_warning=True, has_fixed=True) == "aborted"

    def test_warning(self) -> None:
        assert _auto_remediation_status(has_abort=False, has_warning=True, has_fixed=True) == "warning"

    def test_fixed(self) -> None:
        assert _auto_remediation_status(has_abort=False, has_warning=False, has_fixed=True) == "fixed"

    def test_no_issues(self) -> None:
        assert _auto_remediation_status(has_abort=False, has_warning=False, has_fixed=False) == "no_issues"


class TestAutoRemediationSummaryHelpers:
    def test_empty_summary_shape(self) -> None:
        summary = _empty_auto_remediation_summary()
        assert summary["status"] == "no_issues"
        assert summary["total_fixed"] == 0
        assert summary["players_fixed"] == []

    def test_add_unique_category_appends_once(self) -> None:
        summary = {"categories_fixed": ["BATTING"]}
        _add_unique_category(summary, "categories_fixed", "BATTING")
        _add_unique_category(summary, "categories_fixed", "PITCHING")
        assert summary["categories_fixed"] == ["BATTING", "PITCHING"]

    def test_record_abort(self) -> None:
        summary = _empty_auto_remediation_summary()
        _record_auto_remediation_abort(summary, "20260402_abort_batting.json", {"reason": "unsafe"})
        assert summary["categories_aborted"] == ["BATTING"]
        assert summary["abort_reasons"] == ["BATTING: unsafe"]

    def test_record_warning(self) -> None:
        summary = _empty_auto_remediation_summary()
        _record_auto_remediation_warning(
            summary,
            "20260402_warning_pitching.json",
            {"mismatches": [{"name": "A", "player_id": 1, "diffs": ["era"]}]},
        )
        assert summary["categories_warning"] == ["PITCHING"]
        assert summary["total_warning"] == 1
        assert summary["players_warning"][0]["name"] == "A"

    def test_record_fixed_from_list_and_single_snapshot(self) -> None:
        summary = _empty_auto_remediation_summary()
        _record_auto_remediation_fixed(
            summary,
            "20260402_100_batting.json",
            [
                {
                    "player_id": 1,
                    "player_name": "A",
                    "original": {"games": 10},
                    "calculated": {"games": 11},
                },
            ],
        )
        _record_auto_remediation_fixed(
            summary,
            "bad.json",
            {"player_id": 2, "calculated": {"player_name": "B"}, "original": {}},
        )
        assert summary["categories_fixed"] == ["BATTING", "UNKNOWN"]
        assert summary["total_fixed"] == 2
        assert summary["players_fixed"][0]["diffs"] == ["games: 10→11"]
        assert summary["players_fixed"][1]["name"] == "B"


class TestCategoryCounts:
    def test_empty(self) -> None:
        assert _category_counts([]) == {}

    def test_single_category(self) -> None:
        items = [{"category": "a"}, {"category": "a"}, {"category": "b"}]
        result = _category_counts(items)
        assert result == {"a": 2, "b": 1}

    def test_multiple_categories(self) -> None:
        items = [{"category": "x"}, {"category": "y"}, {"category": "z"}, {"category": "x"}]
        result = _category_counts(items)
        assert result == {"x": 2, "y": 1, "z": 1}


class TestHasReportIssues:
    def test_all_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is False

    def test_gate_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": False,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_detail_incomplete(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": False}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_relay_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": False},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_standings_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": False},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_auto_remediation_warning(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "warning"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_auto_remediation_aborted(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "aborted"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_pa_formula_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": False},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_trend_worsening(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "worsening"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_team_batting_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": False},
            "team_pitching": {"ok": True},
        }
        assert _has_report_issues(metrics, gate_result) is True

    def test_team_pitching_not_ok(self) -> None:
        metrics = {
            "detail_integrity": [{"is_complete": True}],
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable"},
        }
        gate_result = {
            "ok": True,
            "team_batting": {"ok": True},
            "team_pitching": {"ok": False},
        }
        assert _has_report_issues(metrics, gate_result) is True


class TestFixedSnapshotDiffs:
    def test_same_values(self) -> None:
        from src.cli.generate_quality_report import _fixed_snapshot_diffs

        snapshot = {"original": {"games": 100}, "calculated": {"games": 100}}
        assert _fixed_snapshot_diffs(snapshot) == []

    def test_different_values(self) -> None:
        from src.cli.generate_quality_report import _fixed_snapshot_diffs

        snapshot = {"original": {"games": 100}, "calculated": {"games": 120}}
        result = _fixed_snapshot_diffs(snapshot)
        assert len(result) == 1
        assert "games" in result[0]

    def test_multiple_diffs(self) -> None:
        from src.cli.generate_quality_report import _fixed_snapshot_diffs

        snapshot = {"original": {"games": 100, "hits": 50}, "calculated": {"games": 120, "hits": 55}}
        result = _fixed_snapshot_diffs(snapshot)
        assert len(result) == 2


class TestAppendSections:
    def test_collection_section(self) -> None:
        from src.cli.generate_quality_report import _append_collection_section

        lines: list[str] = []
        metrics = {"total_games": 500, "completed_count": 480, "status_counts": {"completed": 480, "failed": 10}}
        _append_collection_section(lines, metrics)
        assert len(lines) > 0
        assert any("500" in line for line in lines)

    def test_parity_section_ok_no_output(self) -> None:
        from src.cli.generate_quality_report import _append_parity_section

        lines: list[str] = []
        parity = {"ok": True, "details": []}
        _append_parity_section(lines, parity)
        assert len(lines) == 0

    def test_parity_section_not_ok(self) -> None:
        from src.cli.generate_quality_report import _append_parity_section

        lines: list[str] = []
        parity = {"ok": False, "local_count": 100, "oci_count": 95, "diff": 5}
        _append_parity_section(lines, parity)
        assert len(lines) == 1
        assert "Parity" in lines[0]

    def test_detail_integrity_section_ok_and_missing(self) -> None:
        lines: list[str] = []
        _append_detail_integrity_section(lines, {"detail_integrity": [{"game_id": "G1", "is_complete": True}]})
        assert "100%" in lines[0]

        lines = []
        _append_detail_integrity_section(
            lines,
            {"detail_integrity": [{"game_id": "G1", "is_complete": False}, {"game_id": "G2", "is_complete": False}]},
        )
        assert "2 games" in lines[0]
        assert "G1" in lines[1]

    def test_player_stats_section_ok_and_mismatch(self) -> None:
        lines: list[str] = []
        _append_player_stats_section(lines, {"batting": {"ok": True}, "pitching": {"ok": True}})
        assert "Consistent" in lines[0]

        lines = []
        _append_player_stats_section(
            lines,
            {"batting": {"ok": False, "mismatches": [1, 2]}, "pitching": {"ok": False, "mismatches": [3]}},
        )
        assert "3 mismatches" in lines[0]
        assert "Batting" in lines[1]
        assert "Pitching" in lines[2]

    def test_top_performer_and_new_players_sections(self) -> None:
        lines: list[str] = []
        _append_top_performer_section(lines, {"top_performer": {"name": "Kim", "war": 6.5}})
        _append_new_players_section(lines, {"new_players": [{"name": "A"}, {"name": "B"}]})
        assert "Kim" in lines[0]
        assert "A, B" in lines[1]

    def test_relay_and_standings_integrity_sections(self) -> None:
        lines: list[str] = []
        _append_relay_integrity_section(
            lines,
            {
                "relay_integrity": {
                    "ok": False,
                    "recent_missing_count": 1,
                    "current_season_missing_count": 2,
                    "missing_game_ids": ["G1"],
                }
            },
        )
        _append_standings_integrity_section(
            lines,
            {
                "standings_integrity": {
                    "ok": False,
                    "mismatches": [{"team_code": "LG", "issue": "wins"}],
                    "missing_score_games": ["G2"],
                }
            },
        )
        assert "1 recent / 2" in lines[0]
        assert "LG" in "\n".join(lines)

    @pytest.mark.parametrize("status", ["fixed", "warning", "aborted", "no_issues"])
    def test_auto_remediation_section_statuses(self, status: str) -> None:
        lines: list[str] = []
        payload = {
            "status": status,
            "total_fixed": 4,
            "players_fixed": [{"name": "A", "category": "BATTING", "diffs": ["games"]}] * 4,
            "total_warning": 1,
            "players_warning": [{"name": "B", "category": "PITCHING", "diffs": ["era"]}],
            "categories_aborted": ["BATTING"],
            "abort_reasons": ["BATTING: unsafe"],
        }
        _append_auto_remediation_section(lines, {"auto_remediation": payload})
        assert lines

    def test_pa_formula_sections(self) -> None:
        lines: list[str] = []
        _append_pa_formula_section(
            lines,
            {
                "pa_formula_integrity": {
                    "ok": False,
                    "violation_count": 1,
                    "violations": [{"game_date": "20260402", "player_name": "A", "pa": 4}],
                }
            },
        )
        _append_pa_formula_trend_section(
            lines,
            {
                "pa_formula_trend": {
                    "direction": "worsening",
                    "months": [{"month": "2026-04", "violation_count": 1, "total_checked": 10, "violation_pct": 10.0}],
                }
            },
        )
        assert "1 violations" in lines[0]
        assert "worsening" in lines[2]

    def test_team_stats_sections(self) -> None:
        lines: list[str] = []
        gate_result = {
            "team_batting": {
                "ok": False,
                "checked_players": 10,
                "mismatches": [{"team_id": "LG", "issue": "hits", "diffs": ["H"]}],
            },
            "team_pitching": {"ok": True, "checked_players": 10, "mismatches": []},
        }
        _append_team_stats_section(lines, gate_result)
        _append_team_stats_trend_section(
            lines,
            {
                "team_stats_trend": {
                    "direction": "stable",
                    "months": [{"month": "2026-04", "total_violations": 0, "teams_checked": 10}],
                }
            },
        )
        assert "1 mismatches" in lines[0]
        assert "Team Stats Trend" in "\n".join(lines)


class TestTeamStatsIntegrityAndTelegramReport:
    def test_get_team_stats_integrity(self) -> None:
        result = get_team_stats_integrity(
            {
                "team_batting": {"ok": False, "checked_players": 8, "mismatches": [{"team_id": "LG"}]},
                "team_pitching": {"ok": True, "checked_players": 9, "mismatches": []},
            },
        )
        assert result["ok"] is False
        assert result["total_mismatches"] == 1
        assert result["batting_checked"] == 8

    def test_format_telegram_report_includes_key_sections(self) -> None:
        metrics = {
            "date": "20260402",
            "total_games": 2,
            "completed_count": 2,
            "status_counts": {"completed": 2},
            "parity": {"ok": True},
            "detail_integrity": [{"game_id": "G1", "is_complete": True}],
            "top_performer": {"name": "Kim", "war": 6.5},
            "relay_integrity": {"ok": True},
            "standings_integrity": {"ok": True},
            "auto_remediation": {"status": "no_issues"},
            "pa_formula_integrity": {"ok": True},
            "pa_formula_trend": {"direction": "stable", "months": []},
            "team_stats_trend": {"direction": "stable", "months": []},
            "new_players": [{"name": "Rookie"}],
        }
        report = format_telegram_report(
            metrics,
            {
                "batting": {"ok": True},
                "pitching": {"ok": True},
                "team_batting": {"ok": True},
                "team_pitching": {"ok": True},
            },
        )
        assert "KBO Quality Report" in report
        assert "Top Performer" in report
        assert "Rookie" in report
