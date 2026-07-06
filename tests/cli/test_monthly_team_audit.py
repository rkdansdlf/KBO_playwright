from __future__ import annotations

import json
import logging
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.cli.monthly_team_audit import (
    crawl_monthly_team_audit_job,
    main,
    run_monthly_team_audit,
)


class TestRunMonthlyTeamAudit:
    def test_returns_structured_result(self):
        mock_session = MagicMock()
        mock_gate_result = {
            "team_batting": {"ok": True, "checked_players": 10, "mismatches": []},
            "team_pitching": {"ok": True, "checked_players": 10, "mismatches": []},
        }
        with (
            patch("src.cli.monthly_team_audit.SessionLocal") as mock_session_factory,
            patch("src.cli.monthly_team_audit.run_quality_gate") as mock_gate,
        ):
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_gate.return_value = mock_gate_result

            result = run_monthly_team_audit(2025)
            assert result["year"] == 2025
            assert result["batting"]["ok"] is True
            assert result["batting"]["checked_teams"] == 10
            assert result["pitching"]["ok"] is True

    def test_handles_missing_keys(self):
        with (
            patch("src.cli.monthly_team_audit.SessionLocal") as mock_session_factory,
            patch("src.cli.monthly_team_audit.run_quality_gate") as mock_gate,
        ):
            mock_session_factory.return_value.__enter__ = lambda s: mock_session_factory
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_gate.return_value = {}

            result = run_monthly_team_audit(2025)
            assert result["batting"]["ok"] is True
            assert result["batting"]["checked_teams"] == 0
            assert result["pitching"]["mismatches"] == []

    def test_passes_year_to_gate(self):
        mock_session = MagicMock()
        with (
            patch("src.cli.monthly_team_audit.SessionLocal") as mock_session_factory,
            patch("src.cli.monthly_team_audit.run_quality_gate") as mock_gate,
        ):
            mock_session_factory.return_value.__enter__ = lambda s: mock_session
            mock_session_factory.return_value.__exit__ = lambda s, *a: False
            mock_gate.return_value = {
                "team_batting": {"ok": True, "checked_players": 0, "mismatches": []},
                "team_pitching": {"ok": True, "checked_players": 0, "mismatches": []},
            }

            run_monthly_team_audit(2023)
            mock_gate.assert_called_once_with(mock_session, 2023)


class TestCrawlMonthlyTeamAuditJob:
    def test_uses_previous_year(self, caplog):
        mock_result = {
            "year": 2025,
            "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
            "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
        }
        with (
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_run,
            patch("src.cli.monthly_team_audit.Path") as mock_path,
        ):
            mock_dt.now.return_value = datetime(2026, 1, 1)
            mock_run.return_value = mock_result
            mock_path.return_value.mkdir.return_value = None
            mock_path.return_value.__truediv__ = lambda self, other: Path(f"/tmp/{other}")
            mock_path.return_value.open = MagicMock()

            with caplog.at_level(logging.INFO):
                crawl_monthly_team_audit_job()

            mock_run.assert_called_once_with(2025)
            assert "Starting monthly team stats audit" in caplog.text

    def test_skips_before_2020(self, caplog):
        with (
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.Path"),
        ):
            mock_dt.now.return_value = datetime(2020, 1, 1)

            with caplog.at_level(logging.INFO):
                crawl_monthly_team_audit_job()

            assert "Skipping team audit" in caplog.text

    def test_raises_on_mismatch(self):
        mock_result = {
            "year": 2025,
            "batting": {
                "ok": False,
                "checked_teams": 10,
                "mismatches": [{"team_id": 1, "issue": "AB diff"}],
            },
            "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
        }
        with (
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_run,
            patch("src.cli.monthly_team_audit.Path") as mock_path,
        ):
            mock_dt.now.return_value = datetime(2026, 1, 1)
            mock_run.return_value = mock_result
            mock_path.return_value.mkdir.return_value = None
            mock_path.return_value.__truediv__ = lambda self, other: Path(f"/tmp/{other}")
            mock_path.return_value.open = MagicMock()

            with pytest.raises(RuntimeError, match="batting=1 mismatches"):
                crawl_monthly_team_audit_job()

    def test_writes_report_file(self, tmp_path):
        mock_result = {
            "year": 2025,
            "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
            "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
        }
        with (
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_run,
            patch("src.cli.monthly_team_audit.Path") as mock_path,
        ):
            mock_dt.now.return_value = datetime(2026, 1, 1)
            mock_run.return_value = mock_result
            mock_path.return_value.mkdir.return_value = None
            mock_path.return_value.__truediv__ = lambda self, other: tmp_path / other

            crawl_monthly_team_audit_job()
            report_files = list(tmp_path.glob("team_audit_*.json"))
            assert len(report_files) == 1
            data = json.loads(report_files[0].read_text())
            assert data["year"] == 2025


class TestMonthlyTeamAuditCLI:
    def _run_main(self, parse_return):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_audit,
        ):
            mock_parse.return_value = parse_return
            mock_dt.now.return_value = datetime(2026, 1, 1)
            mock_audit.return_value = {
                "year": 2025,
                "batting": {"ok": True, "checked_teams": 10, "mismatches": []},
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            return main()

    def test_default_year(self):
        result = self._run_main(Namespace(year=None, json=False))
        assert result == 0

    def test_specific_year(self):
        result = self._run_main(Namespace(year=2025, json=False))
        assert result == 0

    def test_json_output(self):
        result = self._run_main(Namespace(year=None, json=True))
        assert result == 0

    def test_year_before_2020_skipped(self, caplog):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
        ):
            mock_parse.return_value = Namespace(year=2019, json=False)
            mock_dt.now.return_value = datetime(2026, 1, 1)

            with caplog.at_level(logging.INFO):
                result = main()

            assert result == 0
            assert "Skipping team audit" in caplog.text

    def test_failure_exits_1(self):
        with (
            patch("argparse.ArgumentParser.parse_args") as mock_parse,
            patch("src.cli.monthly_team_audit.datetime") as mock_dt,
            patch("src.cli.monthly_team_audit.run_monthly_team_audit") as mock_audit,
        ):
            mock_parse.return_value = Namespace(year=2025, json=False)
            mock_dt.now.return_value = datetime(2026, 1, 1)
            mock_audit.return_value = {
                "year": 2025,
                "batting": {
                    "ok": False,
                    "checked_teams": 10,
                    "mismatches": [{"team_id": 1, "issue": "AB mismatch"}],
                },
                "pitching": {"ok": True, "checked_teams": 10, "mismatches": []},
            }
            with pytest.raises(SystemExit):
                main()
