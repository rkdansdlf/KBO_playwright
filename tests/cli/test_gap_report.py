from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.gap_report import (
    _check_freshness,
    _check_id_resolution,
    _check_pa_formula,
    _check_profile,
    _check_relay,
    _check_season_team_code,
    _check_staleness,
    _check_standings,
    _check_team_stats,
    _freshness_summary_parts,
    _gap_severity,
    _gap_summary_parts,
    _pa_formula_detail_items,
    _team_stats_summary_parts,
    build_gap_report,
    check_id_resolution_gaps,
    check_pa_formula_gaps,
    check_profile_gaps,
    check_relay_gaps,
    check_season_stat_team_code_gaps,
    check_team_stats_gaps,
    format_report_summary,
    main,
    run_gap_report,
    send_gap_alerts,
)


class TestGapReport:
    def test_default_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main([])
            assert result is None

    def test_no_alert(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--no-alert"])
            assert result is None

    def test_dry_run(self):
        with patch("src.cli.gap_report.run_gap_report") as mock:
            mock.return_value = {"gaps": {}}
            result = main(["--dry-run"])
            assert result is None

    def test_send_gap_alerts_formats_relay(self):
        report = {
            "gaps": {
                "RELAY": {
                    "ok": False,
                    "missing_count": 6,
                    "missing_game_ids": ["G1", "G2", "G3", "G4", "G5", "G6"],
                },
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with("RELAY", "6 games missing PBP", ["G1", "G2", "G3", "G4", "G5"])

    def test_send_gap_alerts_formats_freshness(self):
        report = {
            "gaps": {
                "FRESHNESS": {
                    "ok": False,
                    "total_issues": 3,
                    "details": {"detail": ["G1", "G2"], "relay": ["G3"]},
                },
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with(
            "FRESHNESS",
            "3 total issues, detail: 2 games, relay: 1 games",
            ["detail: G1", "detail: G2", "relay: G3"],
        )

    def test_send_gap_alerts_formats_team_stats_and_skips_ok(self):
        report = {
            "gaps": {
                "TEAM_STATS": {
                    "ok": False,
                    "total": 2,
                    "batting_mismatches": 1,
                    "pitching_mismatches": 1,
                    "details": {
                        "batting": [{"team_id": "LG", "issue": "hits mismatch", "diffs": ["H +1", "AB -1"]}],
                        "pitching": [{"team_id": "SS", "issue": "era mismatch", "diffs": ["ER +2"]}],
                    },
                },
                "PROFILE": {"ok": True, "missing_count": 0},
            },
        }

        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)

        mock.assert_called_once_with(
            "TEAM_STATS",
            "2 team stat mismatches, batting=1, pitching=1",
            ["타격 [LG]: hits mismatch", "  H +1", "  AB -1", "투수 [SS]: era mismatch", "  ER +2"],
        )


class TestCheckRelayGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = []
            mock_session.execute.return_value = mock_result

            result = check_relay_gaps()
            assert result["ok"] is True

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_result = MagicMock()
            mock_result.scalars.return_value.all.return_value = ["G1"]
            mock_session.execute.return_value = mock_result

            result = check_relay_gaps()
            assert result["ok"] is False
            assert result["missing_count"] == 1


class TestCheckProfileGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = []

            result = check_profile_gaps()
            assert result["ok"] is True

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            row = MagicMock()
            row.player_id = 1
            row.team_code = "LG"
            row.name = "Kim"
            row.season = 2025
            row.external_id = None
            mock_session.query.return_value.filter.return_value.all.return_value = [row]

            result = check_profile_gaps()
            assert result["ok"] is False


class TestCheckIdResolutionGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.return_value = 0

            result = check_id_resolution_gaps()
            assert result["ok"] is True
            assert result["total"] == 0

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_session.execute.return_value.scalar.side_effect = [5, 3, 2]

            result = check_id_resolution_gaps()
            assert result["ok"] is False
            assert result["total"] == 10


class TestCheckTeamStatsGaps:
    def test_no_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.validators.quality_gate.run_quality_gate") as mock_gate:
                mock_gate.return_value = {"team_batting": {"ok": True}, "team_pitching": {"ok": True}}
                result = check_team_stats_gaps()
                assert result["ok"] is True

    def test_finds_gaps(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.validators.quality_gate.run_quality_gate") as mock_gate:
                mock_gate.return_value = {
                    "team_batting": {"ok": False, "mismatches": [{"team_id": "LG"}]},
                    "team_pitching": {"ok": True},
                }
                result = check_team_stats_gaps()
                assert result["ok"] is False
                assert result["batting_mismatches"] == 1


class TestRunGapReport:
    def test_returns_structured_result(self):
        with (
            patch("src.cli.gap_report.check_relay_gaps") as mock_relay,
            patch("src.cli.gap_report.check_profile_gaps") as mock_profile,
            patch("src.cli.gap_report.check_id_resolution_gaps") as mock_id,
            patch("src.cli.gap_report.check_pa_formula_gaps") as mock_pa,
            patch("src.cli.gap_report.check_team_stats_gaps") as mock_team,
            patch("src.cli.gap_report.collect_freshness_issues") as mock_fresh,
        ):
            mock_relay.return_value = {"ok": True, "missing_count": 0}
            mock_profile.return_value = {"ok": True, "missing_count": 0}
            mock_id.return_value = {"ok": True}
            mock_pa.return_value = {"ok": True}
            mock_team.return_value = {"ok": True, "total": 0}
            mock_fresh.return_value = {}

            result = build_gap_report()
            assert isinstance(result, dict)
            assert "gaps" in result


class TestSendGapAlertsEdgeCases:
    def test_skips_ok_categories(self):
        report = {"gaps": {"RELAY": {"ok": True, "missing_count": 0}}}
        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)
            mock.assert_not_called()

    def test_empty_gaps(self):
        report = {"gaps": {}}
        with patch("src.cli.gap_report.SlackWebhookClient.send_gap_alert") as mock:
            send_gap_alerts(report)
            mock.assert_not_called()


class TestCheckPaFormulaGaps:
    def test_ok(self):
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_pa_formula_integrity") as mock_pa,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
            result = check_pa_formula_gaps()
            assert result["ok"] is True
            assert result["violation_count"] == 0

    def test_with_violations(self):
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_pa_formula_integrity") as mock_pa,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_pa.return_value = {
                "ok": False,
                "violation_count": 3,
                "violations": [
                    {
                        "game_date": "2026-07-05",
                        "player_name": "Kim",
                        "pa": 4,
                        "ab": 4,
                        "bb": 0,
                        "hbp": 0,
                        "sh": 0,
                        "sf": 0,
                    }
                ],
            }
            result = check_pa_formula_gaps()
            assert result["ok"] is False
            assert result["violation_count"] == 3
            assert len(result["violations"]) == 1

    def test_ok_defaults(self):
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_pa_formula_integrity") as mock_pa,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_pa.return_value = {}
            result = check_pa_formula_gaps()
            assert result["ok"] is True

    def test_violations_truncated_to_20(self):
        violations = [
            {
                "game_date": f"2026-07-{d:02d}",
                "player_name": f"P{d}",
                "pa": 4,
                "ab": 4,
                "bb": 0,
                "hbp": 0,
                "sh": 0,
                "sf": 0,
            }
            for d in range(1, 30)
        ]
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.generate_quality_report.get_pa_formula_integrity") as mock_pa,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_pa.return_value = {"ok": False, "violation_count": 25, "violations": violations}
            result = check_pa_formula_gaps()
            assert len(result["violations"]) == 20


class TestCheckSeasonStatTeamCodeGaps:
    def test_ok(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [0, 100, 0, 100]
            mock_session.execute.return_value = mock_execute
            result = check_season_stat_team_code_gaps()
            assert result["ok"] is True
            assert result["total_null"] == 0

    def test_finds_nulls(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [5, 100, 3, 100]
            mock_session.execute.return_value = mock_execute
            result = check_season_stat_team_code_gaps()
            assert result["ok"] is False
            assert result["batting_null"] == 5
            assert result["pitching_null"] == 3
            assert result["total_null"] == 8
            assert result["batting_null_rate"] == 5.0
            assert result["pitching_null_rate"] == 3.0

    def test_below_threshold_is_reported_without_alert(self, monkeypatch):
        monkeypatch.setenv("SEASON_TEAM_CODE_GAP_ALERT_RATE", "10")
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [1, 100, 0, 100]
            mock_session.execute.return_value = mock_execute

            result = check_season_stat_team_code_gaps()

        assert result["ok"] is False
        assert result["alert"] is False
        assert _gap_severity(result) == "ok"
        assert "alert_threshold=10.0%" in _gap_summary_parts("SEASON_TEAM_CODE", result)[0]

    def test_above_threshold_triggers_alert(self, monkeypatch):
        monkeypatch.setenv("SEASON_TEAM_CODE_GAP_ALERT_RATE", "1")
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [2, 100, 0, 100]
            mock_session.execute.return_value = mock_execute

            result = check_season_stat_team_code_gaps()

        assert result["alert"] is True
        assert _gap_severity(result) == "warning"

    def test_zero_total_handling(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [0, 0, 0, 0]
            mock_session.execute.return_value = mock_execute
            result = check_season_stat_team_code_gaps()
            assert result["ok"] is True
            assert result["batting_null_rate"] == 0
            assert result["pitching_null_rate"] == 0

    def test_pitching_only_nulls(self):
        with patch("src.cli.gap_report.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_execute = MagicMock()
            mock_execute.scalar.side_effect = [0, 100, 5, 200]
            mock_session.execute.return_value = mock_execute
            result = check_season_stat_team_code_gaps()
            assert result["ok"] is False
            assert result["batting_null"] == 0
            assert result["pitching_null"] == 5
            assert result["total_null"] == 5


class TestFormatReportSummary:
    def test_all_ok(self):
        report = {"gaps": {"RELAY": {"ok": True, "missing_count": 0}, "PROFILE": {"ok": True, "missing_count": 0}}}
        summary = format_report_summary(report)
        assert "✅" in summary
        assert "RELAY" in summary
        assert "PROFILE" in summary

    def test_mixed_gaps(self):
        report = {
            "gaps": {
                "RELAY": {"ok": True, "missing_count": 0},
                "FRESHNESS": {"ok": False, "total_issues": 3},
                "TEAM_STATS": {"ok": False, "error": "db error"},
            },
        }
        summary = format_report_summary(report)
        assert "✅" in summary
        assert "⚠️" in summary
        assert "❌" in summary

    def test_empty_gaps(self):
        summary = format_report_summary({"gaps": {}})
        assert summary == ""


class TestRunGapReport2:
    def test_dry_run(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts") as mock_alert,
        ):
            mock_build.return_value = {"gaps": {}}
            result = run_gap_report(dry_run=True)
            mock_alert.assert_not_called()
            assert result == {"gaps": {}}

    def test_alert_false(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts") as mock_alert,
        ):
            mock_build.return_value = {"gaps": {}}
            result = run_gap_report(alert=False, dry_run=False)
            mock_alert.assert_not_called()
            assert result == {"gaps": {}}

    def test_alert_true(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts") as mock_alert,
        ):
            mock_build.return_value = {"gaps": {"RELAY": {"ok": True}}}
            result = run_gap_report(alert=True, dry_run=False)
            mock_alert.assert_called_once()
            assert result == {"gaps": {"RELAY": {"ok": True}}}

    def _assert_log_contains(self, mock_logger, gap_type: str, count_str: str) -> None:
        info_calls = mock_logger.info.call_args_list
        for call_args in info_calls:
            formatted = str(call_args)
            if gap_type in formatted and count_str in formatted:
                return
        pytest.fail(f"logger.info call with {gap_type} and {count_str} not found in {info_calls}")

    def test_formats_gap_count_from_missing_count(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts"),
            patch("src.cli.gap_report.logger") as mock_logger,
        ):
            mock_build.return_value = {"gaps": {"RELAY": {"ok": False, "missing_count": 5}}}
            run_gap_report()
            self._assert_log_contains(mock_logger, "RELAY", "(5)")

    def test_formats_gap_count_from_total_issues(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts"),
            patch("src.cli.gap_report.logger") as mock_logger,
        ):
            mock_build.return_value = {"gaps": {"FRESHNESS": {"ok": False, "total_issues": 3}}}
            run_gap_report()
            self._assert_log_contains(mock_logger, "FRESHNESS", "(3)")

    def test_formats_gap_count_from_stale_count(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts"),
            patch("src.cli.gap_report.logger") as mock_logger,
        ):
            mock_build.return_value = {"gaps": {"STALENESS": {"ok": False, "stale_count": 2}}}
            run_gap_report()
            self._assert_log_contains(mock_logger, "STALENESS", "(2)")

    def test_formats_gap_count_from_total(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts"),
            patch("src.cli.gap_report.logger") as mock_logger,
        ):
            mock_build.return_value = {"gaps": {"ID_RESOLUTION": {"ok": False, "total": 10}}}
            run_gap_report()
            self._assert_log_contains(mock_logger, "ID_RESOLUTION", "(10)")

    def test_formats_gap_count_from_violation_count(self):
        with (
            patch("src.cli.gap_report.build_gap_report") as mock_build,
            patch("src.cli.gap_report.send_gap_alerts"),
            patch("src.cli.gap_report.logger") as mock_logger,
        ):
            mock_build.return_value = {"gaps": {"PA_FORMULA": {"ok": False, "violation_count": 7}}}
            run_gap_report()
            self._assert_log_contains(mock_logger, "PA_FORMULA", "(7)")


class TestCheckWrappers:
    def test_check_freshness_success(self):
        report: dict = {"gaps": {}}
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.gap_report.collect_freshness_issues") as mock_collect,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_collect.return_value = {"missing_scores": ["G1"]}
            _check_freshness(report)
        assert report["gaps"]["FRESHNESS"]["ok"] is False
        assert report["gaps"]["FRESHNESS"]["total_issues"] == 1

    def test_check_freshness_exception(self):
        report: dict = {"gaps": {}}
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.gap_report.collect_freshness_issues") as mock_collect,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_collect.side_effect = SQLAlchemyError("mock error")
            _check_freshness(report)
        assert report["gaps"]["FRESHNESS"]["ok"] is False
        assert "error" in report["gaps"]["FRESHNESS"]

    def test_check_relay_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_relay_gaps") as mock_check:
            mock_check.side_effect = ValueError("mock error")
            _check_relay(report)
        assert report["gaps"]["RELAY"]["ok"] is False
        assert "error" in report["gaps"]["RELAY"]

    def test_check_staleness_success(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_freshness") as mock_check:
            mock_check.return_value = ["stale1", "stale2"]
            _check_staleness(report)
            mock_check.assert_called_once_with(dry_run=True)
        assert report["gaps"]["STALENESS"]["ok"] is False
        assert report["gaps"]["STALENESS"]["stale_count"] == 2

    def test_check_staleness_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_freshness") as mock_check:
            mock_check.side_effect = RuntimeError("mock error")
            _check_staleness(report)
        assert report["gaps"]["STALENESS"]["ok"] is False
        assert "error" in report["gaps"]["STALENESS"]

    def test_check_standings_success(self):
        report: dict = {"gaps": {}}
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.gap_report.validate_standings_integrity") as mock_val,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_val.return_value = {"ok": True, "mismatches": [], "missing_score_games": []}
            _check_standings(report)
        assert report["gaps"]["STANDINGS"]["ok"] is True
        assert report["gaps"]["STANDINGS"]["mismatches"] == 0

    def test_check_standings_exception(self):
        report: dict = {"gaps": {}}
        with (
            patch("src.cli.gap_report.SessionLocal") as mock_sf,
            patch("src.cli.gap_report.validate_standings_integrity") as mock_val,
        ):
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            mock_val.side_effect = OSError("mock error")
            _check_standings(report)
        assert report["gaps"]["STANDINGS"]["ok"] is False
        assert "error" in report["gaps"]["STANDINGS"]

    def test_check_profile_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_profile_gaps") as mock_check:
            mock_check.side_effect = ValueError("mock error")
            _check_profile(report)
        assert report["gaps"]["PROFILE"]["ok"] is False
        assert "error" in report["gaps"]["PROFILE"]

    def test_check_id_resolution_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_id_resolution_gaps") as mock_check:
            mock_check.side_effect = SQLAlchemyError("mock error")
            _check_id_resolution(report)
        assert report["gaps"]["ID_RESOLUTION"]["ok"] is False
        assert "error" in report["gaps"]["ID_RESOLUTION"]

    def test_check_pa_formula_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_pa_formula_gaps") as mock_check:
            mock_check.side_effect = RuntimeError("mock error")
            _check_pa_formula(report)
        assert report["gaps"]["PA_FORMULA"]["ok"] is False
        assert "error" in report["gaps"]["PA_FORMULA"]

    def test_check_team_stats_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_team_stats_gaps") as mock_check:
            mock_check.side_effect = ValueError("mock error")
            _check_team_stats(report)
        assert report["gaps"]["TEAM_STATS"]["ok"] is False
        assert "error" in report["gaps"]["TEAM_STATS"]

    def test_check_season_team_code_success(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_season_stat_team_code_gaps") as mock_check:
            mock_check.return_value = {"ok": True, "total_null": 0}
            _check_season_team_code(report)
        assert report["gaps"]["SEASON_TEAM_CODE"]["ok"] is True
        assert report["gaps"]["SEASON_TEAM_CODE"]["total_null"] == 0

    def test_check_season_team_code_exception(self):
        report: dict = {"gaps": {}}
        with patch("src.cli.gap_report.check_season_stat_team_code_gaps") as mock_check:
            mock_check.side_effect = SQLAlchemyError("mock error")
            _check_season_team_code(report)
        assert report["gaps"]["SEASON_TEAM_CODE"]["ok"] is False
        assert "error" in report["gaps"]["SEASON_TEAM_CODE"]


class TestGapSummaryPartsEdgeCases:
    def test_freshness_empty_details(self):
        gap_data = {"total_issues": 0, "details": {}}
        result = _freshness_summary_parts(gap_data)
        assert "0 total issues" in result

    def test_team_stats_no_batting(self):
        gap_data = {"total": 1, "batting_mismatches": 0, "pitching_mismatches": 1}
        result = _team_stats_summary_parts(gap_data)
        assert "batting=0" not in result
        assert "pitching=1" in result

    def test_team_stats_no_pitching(self):
        gap_data = {"total": 1, "batting_mismatches": 1, "pitching_mismatches": 0}
        result = _team_stats_summary_parts(gap_data)
        assert "batting=1" in result
        assert "pitching=0" not in result

    def test_gap_summary_parts_error_path(self):
        gap_data = {"ok": False, "error": "connection failed"}
        result = _gap_summary_parts("RELAY", gap_data)
        assert "Error: connection failed" in result

    def test_pa_formula_detail_items(self):
        violations = [
            {"game_date": "2026-07-05", "player_name": "Kim", "pa": 4, "ab": 4, "bb": 0, "hbp": 0, "sh": 0, "sf": 0},
            {"game_date": "2026-07-06", "player_name": "Lee", "pa": 3, "ab": 3, "bb": 0, "hbp": 0, "sh": 0, "sf": 0},
        ]
        gap_data = {"violations": violations}
        result = _pa_formula_detail_items(gap_data)
        assert len(result) == 2
        assert "2026-07-05" in result[0]
        assert "Kim" in result[0]
        assert "PA=4" in result[0]

    def test_pa_formula_detail_items_empty(self):
        result = _pa_formula_detail_items({"violations": []})
        assert result == []

    def test_pa_formula_detail_items_truncated_to_5(self):
        violations = [
            {
                "game_date": f"2026-07-{d:02d}",
                "player_name": f"P{d}",
                "pa": 4,
                "ab": 4,
                "bb": 0,
                "hbp": 0,
                "sh": 0,
                "sf": 0,
            }
            for d in range(1, 10)
        ]
        gap_data = {"violations": violations}
        result = _pa_formula_detail_items(gap_data)
        assert len(result) == 5
