from unittest.mock import patch, MagicMock

from src.cli.quality_gate_check import main


class TestQualityGateCheck:
    def test_default_year(self):
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.quality_gate_check.run_quality_gate") as mock_gate:
                mock_gate.return_value = {
                    "ok": True,
                    "batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pa_formula": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                }
                result = main([])
                assert result == 0

    def test_specific_year(self):
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.quality_gate_check.run_quality_gate") as mock_gate:
                mock_gate.return_value = {
                    "ok": True,
                    "batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pa_formula": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                }
                result = main(["--year", "2025"])
                assert result == 0

    def test_json_output(self):
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.quality_gate_check.run_quality_gate") as mock_gate:
                mock_gate.return_value = {
                    "ok": True,
                    "batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pa_formula": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                }
                result = main(["--json"])
                assert result == 0

    def test_failure_exit_code(self):
        with patch("src.cli.quality_gate_check.SessionLocal") as mock_sf:
            mock_session = MagicMock()
            mock_sf.return_value.__enter__.return_value = mock_session
            with patch("src.cli.quality_gate_check.run_quality_gate") as mock_gate:
                mock_gate.return_value = {
                    "ok": False,
                    "batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                    "pa_formula": {"ok": True, "checked_players": 0, "mismatches": []},
                    "team_batting": {"ok": False, "checked_players": 0, "mismatches": [{"team_id": "LG", "issue": "test"}]},
                    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                }
                result = main([])
                assert result == 1
