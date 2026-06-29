"""Unit tests for generate_quality_report pure functions."""

from __future__ import annotations

import pytest

from src.cli.generate_quality_report import (
    _audit_category_from_filename,
    _auto_remediation_status,
    _category_counts,
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
