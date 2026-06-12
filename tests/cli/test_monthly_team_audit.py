from argparse import Namespace
from unittest.mock import patch

from src.cli.monthly_team_audit import main


class TestMonthlyTeamAudit:
    def _run_main(self, parse_return):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_audit,
        ):
            mock_parse.return_value = parse_return
            mock_audit.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            return main()

    def test_default_year(self):
        result = self._run_main(Namespace(year=None, json=False))
        assert result is None

    def test_specific_year(self):
        result = self._run_main(Namespace(year=2025, json=False))
        assert result is None

    def test_json_output(self):
        result = self._run_main(Namespace(year=None, json=True))
        assert result is None
