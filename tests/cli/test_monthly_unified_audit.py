from argparse import Namespace
from unittest.mock import patch

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
            assert result is None

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
            assert result is None

    def test_pa_only(self):
        with (
            patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix,
            patch("src.cli.monthly_unified_audit.run_pa_audit") as mock_pa,
        ):
            mock_fix.return_value = {"ok": True, "fixed_rows": 0}
            mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
            result = self._run_main(pa_only=True)
            assert result is None

    def test_team_only(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team:
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = self._run_main(team_only=True)
            assert result is None
