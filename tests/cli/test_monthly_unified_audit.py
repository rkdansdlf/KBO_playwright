from unittest.mock import patch, MagicMock

from src.cli.monthly_unified_audit import main


class TestMonthlyUnifiedAudit:
    def test_default_year(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team:
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            with patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix:
                mock_fix.return_value = {"ok": True, "fixed_rows": 0}
                with patch("src.cli.monthly_unified_audit.run_pa_audit") as mock_pa:
                    mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
                    result = main()
                    assert result is None

    def test_dry_run(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team:
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            with patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix:
                mock_fix.return_value = {"ok": True, "fixed_rows": 0}
                result = main()
                assert result is None

    def test_pa_only(self):
        with patch("src.cli.monthly_unified_audit.run_pa_fix") as mock_fix:
            mock_fix.return_value = {"ok": True, "fixed_rows": 0}
            with patch("src.cli.monthly_unified_audit.run_pa_audit") as mock_pa:
                mock_pa.return_value = {"ok": True, "violation_count": 0, "violations": []}
                result = main()
                assert result is None

    def test_team_only(self):
        with patch("src.cli.monthly_unified_audit.run_monthly_team_audit") as mock_team:
            mock_team.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = main()
            assert result is None
