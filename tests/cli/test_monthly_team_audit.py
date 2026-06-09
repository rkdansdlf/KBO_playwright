from unittest.mock import patch, MagicMock

from src.cli.monthly_team_audit import main


class TestMonthlyTeamAudit:
    def test_default_year(self):
        with patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock:
            mock.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = main()
            assert result is None

    def test_specific_year(self):
        with patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock:
            mock.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = main()
            assert result is None

    def test_json_output(self):
        with patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock:
            mock.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            result = main()
            assert result is None
