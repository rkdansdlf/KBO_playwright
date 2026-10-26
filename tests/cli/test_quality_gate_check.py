from unittest.mock import MagicMock, patch

from src.cli.quality_gate_check import _print_category, main


class TestPrintCategory:
    def test_passed_no_mismatches(self, caplog):
        import logging

        with caplog.at_level(logging.INFO):
            _print_category("batting", {"ok": True, "checked_players": 42, "mismatches": []})
        assert "Batting" in caplog.text
        assert "PASSED" in caplog.text
        assert "Checked Players: 42" in caplog.text

    def test_failed_label(self, caplog):
        import logging

        with caplog.at_level(logging.INFO):
            _print_category("pitching", {"ok": False, "checked_players": 10})
        assert "FAILED" in caplog.text

    def test_team_category_uses_teams_label(self, caplog):
        import logging

        with caplog.at_level(logging.INFO):
            _print_category("team_batting", {"ok": True, "checked_players": 5})
        assert "Checked Teams: 5" in caplog.text

    def test_unknown_category_uses_capitalized(self, caplog):
        import logging

        with caplog.at_level(logging.INFO):
            _print_category("custom_metric", {"ok": True})
        assert "Custom_metric" in caplog.text

    def test_error_field_logged(self, caplog):
        import logging

        with caplog.at_level(logging.ERROR):
            _print_category("batting", {"ok": False, "error": "DB timeout"})
        assert "Error: DB timeout" in caplog.text

    def test_mismatch_with_issue(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _print_category(
                "batting",
                {
                    "ok": False,
                    "mismatches": [{"player_id": 123, "issue": "PA mismatch"}],
                },
            )
        assert "Mismatches: 1" in caplog.text
        assert "PA mismatch" in caplog.text

    def test_mismatch_with_diffs(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _print_category(
                "pa_formula",
                {
                    "ok": False,
                    "mismatches": [
                        {"player_id": 456, "issue": "formula", "diffs": ["d1", "d2", "d3", "d4", "d5", "d6"]},
                    ],
                },
            )
        assert "d1" in caplog.text
        assert "... and 3 more diff entries" in caplog.text

    def test_mismatch_with_expected_actual_keys(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _print_category(
                "batting",
                {
                    "ok": False,
                    "mismatches": [
                        {"player_id": 789, "issue": "x", "expected_pa": 100, "actual_pa": 95, "difference": 5},
                    ],
                },
            )
        assert "expected_pa: 100" in caplog.text
        assert "actual_pa: 95" in caplog.text
        assert "difference: 5" in caplog.text

    def test_more_than_five_mismatches_truncated(self, caplog):
        import logging

        mismatches = [{"player_id": i, "issue": f"issue_{i}"} for i in range(8)]
        with caplog.at_level(logging.WARNING):
            _print_category("batting", {"ok": False, "mismatches": mismatches})
        assert "... and 3 more" in caplog.text

    def test_team_mismatch_uses_team_id(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _print_category(
                "team_pitching",
                {
                    "ok": False,
                    "mismatches": [{"team_id": "LG", "issue": "team issue"}],
                },
            )
        assert "LG" in caplog.text


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
                    "futures_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "futures_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
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
                    "futures_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "futures_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
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
                    "futures_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "futures_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
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
                    "team_batting": {
                        "ok": False,
                        "checked_players": 0,
                        "mismatches": [{"team_id": "LG", "issue": "test"}],
                    },
                    "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                    "futures_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                    "futures_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
                }
                result = main([])
                assert result == 1


def _ok_result() -> dict:
    categories = [
        "batting",
        "pitching",
        "pa_formula",
        "team_batting",
        "team_pitching",
        "futures_batting",
        "futures_pitching",
    ]
    return {
        "ok": True,
        **{name: {"ok": True, "checked_players": 1, "mismatches": []} for name in categories},
    }


class TestTheVerdictReachesTheOperator:
    """A passing gate used to print nothing at all.

    The report goes through ``logger.info`` and a CLI process configures no
    logging, so the whole verdict was dropped: zero bytes on stdout made a
    healthy gate indistinguishable from a command that never ran. The gate was
    reported as broken for as long as nobody could see it pass.
    """

    def test_the_cli_configures_logging_before_it_reports(self) -> None:
        calls: list[dict] = []
        with (
            patch(
                "src.cli.reports.quality_gate_check.logging.basicConfig",
                side_effect=lambda **kwargs: calls.append(kwargs),
            ),
            patch("src.cli.reports.quality_gate_check.SessionLocal") as session,
            patch("src.cli.reports.quality_gate_check.run_quality_gate", return_value=_ok_result()),
        ):
            session.return_value.__enter__.return_value = MagicMock()
            exit_code = main(["--year", "2026"])

        assert calls, "a CLI that reports through a logger has to configure one"
        assert exit_code == 0

    def test_a_failing_gate_returns_one(self) -> None:
        result = _ok_result()
        result["ok"] = False
        with (
            patch("src.cli.reports.quality_gate_check.logging.basicConfig"),
            patch("src.cli.reports.quality_gate_check.SessionLocal") as session,
            patch("src.cli.reports.quality_gate_check.run_quality_gate", return_value=result),
        ):
            session.return_value.__enter__.return_value = MagicMock()
            assert main(["--year", "2026"]) == 1
