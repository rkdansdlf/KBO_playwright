import json
from argparse import Namespace
from unittest.mock import patch

import pytest

import src.cli.monthly_unified_audit as monthly_unified_audit
from src.cli.monthly_unified_audit import main


class TestMonthlyUnifiedAudit:
    def _run_main(self, **ns_kw):
        with patch("argparse.ArgumentParser.parse_args") as mock_parse:
            ns = Namespace(year=None, json=False, dry_run=False, pa_only=False, team_only=False)
            for k, v in ns_kw.items():
                setattr(ns, k, v)
            mock_parse.return_value = ns
            return main()

    def test_default_year(self):
        with (
            patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team,
            patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix,
            patch("src.cli.monthly_unified_audit.run_pa_audit") as mock_pa,
        ):
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            mock_fix.return_value = {"ok": True, "fixed_rows": 0}
            mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
            result = self._run_main()
            assert result == 0

    def test_dry_run(self):
        with (
            patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix,
            patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team,
        ):
            mock_fix.return_value = {"ok": True, "fixed_rows": 0}
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = self._run_main(dry_run=True)
            assert result == 0

    def test_pa_only(self):
        with (
            patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix,
            patch("src.cli.monthly_unified_audit.run_pa_audit") as mock_pa,
        ):
            mock_fix.return_value = {"ok": True, "fixed_rows": 0}
            mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
            result = self._run_main(pa_only=True)
            assert result == 0

    def test_team_only(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team:
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = self._run_main(team_only=True)
            assert result == 0

    def test_skips_year_before_supported_audit_range(self):
        assert self._run_main(year=2019) == 0


def _team_result(*, batting_ok=True, pitching_ok=True, mismatches=None):
    mismatches = mismatches or []
    return {
        "year": 2025,
        "batting": {"ok": batting_ok, "checked_teams": 10, "mismatches": mismatches},
        "pitching": {"ok": pitching_ok, "checked_teams": 10, "mismatches": mismatches},
    }


class TestMonthlyUnifiedAuditJob:
    def test_scheduled_job_writes_successful_audit_reports(self, tmp_path):
        pa_result = {"year": 2025, "ok": True, "violation_count": 0, "violations": []}
        team_result = _team_result()
        with (
            patch("src.cli.monthly_unified_audit.datetime") as mock_datetime,
            patch("src.cli.monthly_unified_audit.Path", side_effect=lambda _path: tmp_path),
            patch("src.cli.monthly_unified_audit.run_pa_fix", return_value={"ok": True, "fixed_rows": 2}) as fix,
            patch("src.cli.monthly_unified_audit.run_pa_audit", return_value=pa_result) as pa_audit,
            patch("src.cli.monthly_unified_audit.run_monthly_team_audit", return_value=team_result) as team_audit,
        ):
            mock_datetime.now.return_value.year = 2026

            monthly_unified_audit.crawl_monthly_unified_audit_job()

        fix.assert_called_once_with(2025, dry_run=False)
        pa_audit.assert_called_once_with(2025)
        team_audit.assert_called_once_with(2025)
        assert json.loads((tmp_path / "pa_audit_2025.json").read_text(encoding="utf-8")) == pa_result
        assert json.loads((tmp_path / "team_audit_2025.json").read_text(encoding="utf-8")) == team_result

    def test_scheduled_job_skips_year_before_supported_range(self):
        with (
            patch("src.cli.monthly_unified_audit.datetime") as mock_datetime,
            patch("src.cli.monthly_unified_audit.KBO_QUALITY_AUDIT_START_YEAR", 2025),
            patch("src.cli.monthly_unified_audit.run_pa_fix") as fix,
        ):
            mock_datetime.now.return_value.year = 2025

            monthly_unified_audit.crawl_monthly_unified_audit_job()

        fix.assert_not_called()

    def test_scheduled_job_raises_when_pa_fix_fails(self):
        with (
            patch("src.cli.monthly_unified_audit.datetime") as mock_datetime,
            patch(
                "src.cli.monthly_unified_audit.run_pa_fix", return_value={"ok": False, "error": "database unavailable"}
            ),
        ):
            mock_datetime.now.return_value.year = 2026

            with pytest.raises(RuntimeError, match="PA formula fix failed"):
                monthly_unified_audit.crawl_monthly_unified_audit_job()

    def test_scheduled_job_raises_when_post_fix_audits_fail(self, tmp_path):
        with (
            patch("src.cli.monthly_unified_audit.datetime") as mock_datetime,
            patch("src.cli.monthly_unified_audit.Path", side_effect=lambda _path: tmp_path),
            patch("src.cli.monthly_unified_audit.run_pa_fix", return_value={"ok": True, "fixed_rows": 0}),
            patch(
                "src.cli.monthly_unified_audit.run_pa_audit",
                return_value={"year": 2025, "ok": False, "violation_count": 2, "violations": []},
            ),
            patch("src.cli.monthly_unified_audit.run_monthly_team_audit", return_value=_team_result()),
        ):
            mock_datetime.now.return_value.year = 2026

            with pytest.raises(RuntimeError, match="Unified audit failed"):
                monthly_unified_audit.crawl_monthly_unified_audit_job()

        assert (tmp_path / "pa_audit_2025.json").exists()
        assert (tmp_path / "team_audit_2025.json").exists()


class TestMonthlyUnifiedAuditOutput:
    def test_team_only_json_output_does_not_exit(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit", return_value=_team_result()) as audit:
            monthly_unified_audit._run_team_only(2025, json_output=True)

        audit.assert_called_once_with(2025)

    def test_team_only_failure_exits_nonzero(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit", return_value=_team_result(batting_ok=False)):
            with pytest.raises(SystemExit) as exc:
                monthly_unified_audit._run_team_only(2025, json_output=False)

        assert exc.value.code == 1

    def test_unified_output_logs_json_without_team_mismatches(self):
        monthly_unified_audit._emit_unified_cli_output(
            {"ok": True, "violation_count": 0},
            _team_result(),
            pa_only=False,
            json_output=True,
        )

    def test_unified_output_logs_mismatches_and_exits_nonzero(self):
        mismatch = {"team_id": "LG", "issue": "hits", "diffs": ["hits: 1", "runs: 2"]}
        with pytest.raises(SystemExit) as exc:
            monthly_unified_audit._emit_unified_cli_output(
                {"ok": True, "violation_count": 0},
                _team_result(batting_ok=False, mismatches=[mismatch]),
                pa_only=False,
                json_output=False,
            )

        assert exc.value.code == 1

    def test_pa_only_failure_exits_nonzero(self):
        with pytest.raises(SystemExit) as exc:
            monthly_unified_audit._emit_unified_cli_output(
                {"ok": False, "violation_count": 3},
                None,
                pa_only=True,
                json_output=False,
            )

        assert exc.value.code == 1
